use crate::majutsu_daemon::{DaemonServiceConfig, DaemonServiceScope, render_daemon_service};
use crate::majutsu_restore::RestoreQueueItem;
use anyhow::{Result, bail};
use chrono::Utc;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
#[cfg(not(windows))]
use std::process::Stdio;

use crate::cli::DaemonCommand;
use crate::config::{Paths, read_config, validate_watch_mode};
#[cfg(not(windows))]
use crate::platform_runtime::configure_background_command;
use crate::process_runtime::{
    ProcessLock, acquire_process_lock, pid_alive, process_identity, process_identity_matches,
    read_pid, read_process_identity, write_process_identity,
};
use crate::queue_runtime::{event_journal_records, upload_queue_stats};
use crate::root_state::roots;
use crate::snapshot_state::current_snapshot;
use crate::watch_runtime::default_watch_max_rss_mib;
use crate::watch_runtime::normalize_watch_backend;
use crate::{open_db, resolve_paths};

const DAEMON_ATTRIBUTION_ENV_KEYS: &[&str] = &[
    "MAJUTSU_SESSION_ID",
    "MAJUTSU_SESSION_LABEL",
    "MAJUTSU_AGENT_NAME",
    "CODEX_THREAD_ID",
    "CLAUDE_SESSION_ID",
    "CURSOR_SESSION_ID",
    "TERM_SESSION_ID",
];

const DAEMON_IDENTITY_FILE: &str = "daemon.identity";

pub(crate) fn child_process_exe() -> Result<PathBuf> {
    if let Some(path) = env::var_os("MAJUTSU_CHILD_EXE").map(PathBuf::from)
        && executable_candidate_exists(&path)
    {
        return Ok(path);
    }

    let current = env::current_exe();
    if let Ok(path) = &current
        && executable_candidate_exists(path)
    {
        return Ok(path.clone());
    }

    // daemon 稼働中に cargo install などで実行ファイルが置換されると、
    // current_exe が古い inode 由来の削除済みパスを返すことがある。
    // daemon の argv[0] はインストール済み mj の実体パスなので、子プロセス起動では
    // こちらへフォールバックする。
    if let Some(path) = env::args_os().next().map(PathBuf::from)
        && executable_candidate_exists(&path)
    {
        return Ok(path);
    }

    current.map_err(Into::into)
}

fn executable_candidate_exists(path: &Path) -> bool {
    path.exists() && !path.as_os_str().to_string_lossy().contains(" (deleted)")
}

struct DaemonStats {
    pid: u32,
    rss_kib: u64,
    vm_size_kib: u64,
    roots: usize,
    current: String,
    journal_events: usize,
    processed_journal_events: usize,
    pending_journal_event_count: usize,
    pending_journal_events: bool,
    pending_journal_state: String,
    pending_journal_oldest_age_secs: u64,
    queued_uploads: usize,
    queued_uploads_retrying: usize,
    queued_uploads_delayed: usize,
    queued_upload_next_retry_after: String,
    queued_upload_attempts: u64,
    queued_upload_max_attempts: u32,
    upload_queue_backpressure: bool,
    restore_jobs: usize,
    root_statuses: BTreeMap<String, usize>,
    restore_statuses: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DaemonHealthState {
    Running,
    Stopped,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DaemonHealth {
    pub(crate) state: DaemonHealthState,
    pub(crate) pid: Option<u32>,
    pub(crate) ipc_available: bool,
}

impl DaemonHealth {
    pub(crate) fn label(&self) -> &'static str {
        match self.state {
            DaemonHealthState::Running => {
                if self.ipc_available {
                    "running"
                } else {
                    "running, ipc unavailable"
                }
            }
            DaemonHealthState::Stopped => "stopped",
            DaemonHealthState::Stale => "stale pid",
        }
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.state == DaemonHealthState::Running && self.ipc_available
    }
}

pub(crate) fn daemon_cmd(paths: &Paths, command: DaemonCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    let config = read_config(paths)?;
    match command {
        DaemonCommand::Start {
            backend,
            mode,
            interval_secs,
            debounce_ms,
            settle_ms,
            buffer_max_ms,
            buffer_max_events,
            periodic_rescan_secs,
            max_rss_mib,
        } => {
            let configured_backend = backend.unwrap_or_else(|| config.watch.backend.clone());
            let backend = normalize_watch_backend(&configured_backend)?;
            let mode = mode.unwrap_or_else(|| config.watch.mode.clone());
            validate_watch_mode(&mode)?;
            let pid = start_watch_daemon(
                paths,
                WatchDaemonLaunchConfig {
                    backend,
                    mode,
                    interval_secs: interval_secs.unwrap_or(config.watch.interval),
                    debounce_ms: debounce_ms.unwrap_or(config.watch.debounce),
                    settle_ms: settle_ms.unwrap_or(config.watch.settle),
                    buffer_max_ms: buffer_max_ms.unwrap_or(config.watch.buffer_max),
                    buffer_max_events: buffer_max_events.unwrap_or(config.watch.buffer_max_events),
                    periodic_rescan_secs: periodic_rescan_secs
                        .unwrap_or(config.watch.periodic_rescan),
                    max_rss_mib: max_rss_mib.unwrap_or_else(default_watch_max_rss_mib),
                },
            )?;
            println!("started daemon pid {pid}");
        }
        DaemonCommand::Restart { backend, mode } => {
            let health = daemon_health(paths)?;
            match health.state {
                DaemonHealthState::Running => {
                    let pid = health.pid.unwrap_or_default();
                    stop_daemon_process(pid)?;
                    cleanup_daemon_runtime(paths);
                    println!("stopped daemon pid {pid}");
                }
                DaemonHealthState::Stale => {
                    cleanup_daemon_runtime(paths);
                    println!("cleaned stale daemon runtime");
                }
                DaemonHealthState::Stopped => {}
            }
            let configured_backend = backend.unwrap_or_else(|| config.watch.backend.clone());
            let backend = normalize_watch_backend(&configured_backend)?;
            let mode = mode.unwrap_or_else(|| config.watch.mode.clone());
            validate_watch_mode(&mode)?;
            let pid = start_watch_daemon(
                paths,
                WatchDaemonLaunchConfig {
                    backend,
                    mode,
                    interval_secs: config.watch.interval,
                    debounce_ms: config.watch.debounce,
                    settle_ms: config.watch.settle,
                    buffer_max_ms: config.watch.buffer_max,
                    buffer_max_events: config.watch.buffer_max_events,
                    periodic_rescan_secs: config.watch.periodic_rescan,
                    max_rss_mib: default_watch_max_rss_mib(),
                },
            )?;
            println!("started daemon pid {pid}");
        }
        DaemonCommand::Doctor => {
            daemon_doctor(paths)?;
        }
        DaemonCommand::Service {
            provider,
            style,
            scope,
        } => {
            let exe = env::current_exe()?;
            let scope = match scope.as_str() {
                "user" => DaemonServiceScope::User,
                "system" => DaemonServiceScope::System,
                other => bail!("unsupported daemon service scope: {other}"),
            };
            let backend = daemon_service_backend(&config.watch.backend, scope)?;
            let service = render_daemon_service(DaemonServiceConfig {
                provider: &provider,
                style: &style,
                scope,
                exe: &exe,
                home: &paths.home,
                backend,
                mode: &config.watch.mode,
                interval_secs: config.watch.interval,
                debounce_ms: config.watch.debounce,
                settle_ms: config.watch.settle,
                buffer_max_ms: config.watch.buffer_max,
                buffer_max_events: config.watch.buffer_max_events,
                periodic_rescan_secs: config.watch.periodic_rescan,
                max_rss_mib: default_watch_max_rss_mib(),
            })
            .map_err(anyhow::Error::msg)?;
            print!("{service}");
        }
        DaemonCommand::Stop => {
            let pid_file_pid = read_pid(&paths.daemon_pid)?;
            let pid = match pid_file_pid {
                Some(pid) if daemon_process_matches(paths, pid)? => Some(pid),
                _ => discover_running_watch_daemon(paths)?,
            };
            let Some(pid) = pid else {
                if pid_file_pid.is_some() {
                    cleanup_daemon_runtime(paths);
                    println!("cleaned stale daemon runtime");
                    return Ok(());
                }
                bail!("daemon pid file not found");
            };
            stop_daemon_process(pid)?;
            if let Some(extra_pid) = discover_running_watch_daemon(paths)?
                && extra_pid != pid
            {
                stop_daemon_process(extra_pid)?;
                println!("stopped daemon pid {extra_pid}");
            }
            cleanup_daemon_runtime(paths);
            println!("stopped daemon pid {pid}");
        }
        DaemonCommand::Status => {
            let health = daemon_health(paths)?;
            match health.state {
                DaemonHealthState::Running => {
                    let pid = health.pid.unwrap_or_default();
                    if let Ok(reply) = daemon_ipc_request(paths, "status") {
                        print!("{}", format_daemon_status_reply(&reply));
                    } else {
                        println!("running pid {pid}");
                        println!("ipc unavailable");
                    }
                }
                DaemonHealthState::Stale => {
                    println!("stale pid {}", health.pid.unwrap_or_default());
                }
                DaemonHealthState::Stopped => {
                    println!("stopped");
                }
            }
        }
        DaemonCommand::Metrics => match daemon_health(paths)? {
            DaemonHealth {
                state: DaemonHealthState::Running,
                ..
            } => {
                if let Ok(reply) = daemon_ipc_request(paths, "metrics") {
                    println!("{reply}");
                } else {
                    println!("majutsu_daemon_up 1");
                    println!("majutsu_daemon_ipc_up 0");
                }
            }
            DaemonHealth {
                state: DaemonHealthState::Stale,
                pid: Some(pid),
                ..
            } => {
                println!("majutsu_daemon_up 0");
                println!("majutsu_daemon_stale_pid {pid}");
            }
            DaemonHealth {
                state: DaemonHealthState::Stopped,
                ..
            }
            | DaemonHealth { pid: None, .. } => {
                println!("majutsu_daemon_up 0");
            }
        },
    }
    Ok(())
}

fn daemon_service_backend(
    configured_backend: &str,
    scope: DaemonServiceScope,
) -> Result<&'static str> {
    if cfg!(target_os = "linux") && matches!(scope, DaemonServiceScope::System) {
        return Ok("fanotify");
    }
    normalize_watch_backend(configured_backend)
}

fn format_daemon_status_reply(reply: &str) -> String {
    let mut values = BTreeMap::new();
    let mut root_statuses = Vec::new();
    let mut restore_statuses = Vec::new();
    for line in reply.lines() {
        let Some((key, value)) = line.split_once(' ') else {
            continue;
        };
        match key {
            "root_status" => root_statuses.push(value.to_string()),
            "restore_status" => restore_statuses.push(value.to_string()),
            _ => {
                values.insert(key.to_string(), value.to_string());
            }
        }
    }
    let mut out = String::new();
    out.push_str("Daemon\n");
    write_status_kv(
        &mut out,
        "State",
        &format!(
            "running pid {}",
            values
                .get("pid")
                .or_else(|| { values.get("running").and_then(|_| values.get("pid")) })
                .cloned()
                .unwrap_or_else(|| {
                    reply
                        .lines()
                        .find_map(|line| line.strip_prefix("running pid "))
                        .unwrap_or("?")
                        .to_string()
                })
        ),
    );
    write_status_kv(
        &mut out,
        "IPC",
        if reply.lines().any(|line| line == "ipc ok") {
            "ok"
        } else {
            values.get("ipc").map(String::as_str).unwrap_or("unknown")
        },
    );
    if let Some(current) = values.get("current") {
        write_status_kv(&mut out, "Current", current);
    }
    if let Some(roots) = values.get("roots") {
        write_status_kv(&mut out, "Roots", roots);
    }
    out.push('\n');

    out.push_str("Queues\n");
    for (label, key) in [
        ("Uploads", "queued_uploads"),
        ("Retrying", "queued_uploads_retrying"),
        ("Delayed", "queued_uploads_delayed"),
        ("Next retry", "queued_upload_next_retry_after"),
        ("Attempts", "queued_upload_attempts"),
        ("Max attempts", "queued_upload_max_attempts"),
        ("Backpressure", "upload_queue_backpressure"),
        ("Restore jobs", "restore_jobs"),
    ] {
        if let Some(value) = values.get(key) {
            write_status_kv(&mut out, label, value);
        }
    }
    out.push('\n');

    out.push_str("Journal\n");
    for (label, key) in [
        ("Records", "journal_events"),
        ("Processed", "processed_journal_events"),
        ("Pending", "pending_journal_event_count"),
        ("Pending state", "pending_journal_state"),
        ("Oldest pending", "pending_journal_oldest_age_secs"),
    ] {
        if let Some(value) = values.get(key) {
            write_status_kv(&mut out, label, value);
        }
    }
    out.push('\n');

    out.push_str("Memory\n");
    if let Some(value) = values.get("rss_kib") {
        write_status_kv(&mut out, "RSS", &format!("{value} KiB"));
    }
    if let Some(value) = values.get("vm_size_kib") {
        write_status_kv(&mut out, "VM size", &format!("{value} KiB"));
    }

    if !root_statuses.is_empty() {
        out.push('\n');
        out.push_str("Root Status\n");
        for status in root_statuses {
            write_status_parts(&mut out, &status);
        }
    }
    if !restore_statuses.is_empty() {
        out.push('\n');
        out.push_str("Restore Status\n");
        for status in restore_statuses {
            write_status_parts(&mut out, &status);
        }
    }
    out
}

fn write_status_kv(out: &mut String, key: &str, value: &str) {
    let _ = writeln!(out, "  {key:<18} {value}");
}

fn write_status_parts(out: &mut String, value: &str) {
    let mut parts = value.split_whitespace();
    let status = parts.next().unwrap_or("-");
    let count = parts.next().unwrap_or("0");
    let _ = writeln!(out, "  {status:<18} {count}");
}

pub(crate) fn daemon_health(paths: &Paths) -> Result<DaemonHealth> {
    let Some(pid) = read_pid(&paths.daemon_pid)? else {
        return Ok(DaemonHealth {
            state: DaemonHealthState::Stopped,
            pid: None,
            ipc_available: false,
        });
    };
    if daemon_process_matches(paths, pid)? {
        return Ok(DaemonHealth {
            state: DaemonHealthState::Running,
            pid: Some(pid),
            ipc_available: daemon_ipc_request(paths, "status").is_ok(),
        });
    }
    if let Some(live_pid) = discover_running_watch_daemon(paths)? {
        adopt_daemon_pid(paths, live_pid)?;
        return Ok(DaemonHealth {
            state: DaemonHealthState::Running,
            pid: Some(live_pid),
            ipc_available: daemon_ipc_request(paths, "status").is_ok(),
        });
    }
    Ok(DaemonHealth {
        state: DaemonHealthState::Stale,
        pid: Some(pid),
        ipc_available: false,
    })
}

#[cfg(unix)]
fn discover_running_watch_daemon(paths: &Paths) -> Result<Option<u32>> {
    let output = ProcessCommand::new("ps")
        .args(["-eo", "pid=,command="])
        .output()?;
    if !output.status.success() {
        return Ok(None);
    }
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let trimmed = line.trim_start();
        let Some((pid_s, command)) = trimmed.split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(pid) = pid_s.trim().parse::<u32>() else {
            continue;
        };
        if !pid_alive(pid) {
            continue;
        }
        if watch_command_matches(paths, command) {
            return Ok(Some(pid));
        }
    }
    Ok(None)
}

#[cfg(not(unix))]
fn discover_running_watch_daemon(_paths: &Paths) -> Result<Option<u32>> {
    Ok(None)
}

fn daemon_identity_path(paths: &Paths) -> PathBuf {
    paths.runtime.join(DAEMON_IDENTITY_FILE)
}

fn adopt_daemon_pid(paths: &Paths, pid: u32) -> Result<()> {
    fs::write(&paths.daemon_pid, pid.to_string())?;
    write_process_identity(&daemon_identity_path(paths), &process_identity(pid))?;
    Ok(())
}

fn daemon_process_matches(paths: &Paths, pid: u32) -> Result<bool> {
    if !pid_alive(pid) {
        return Ok(false);
    }

    let identity_path = daemon_identity_path(paths);
    if let Some(expected) = read_process_identity(&identity_path)? {
        if !process_identity_matches(&expected, &process_identity(pid)) {
            return Ok(false);
        }
        // Unixではcommand lineも確認し、壊れたidentityや過度に広い記録だけで
        // 無関係なプロセスを信頼しない。
        #[cfg(unix)]
        if let Some(command) = process_command_line(pid) {
            return Ok(watch_command_matches(paths, &command));
        }
        return Ok(true);
    }

    // 旧版はPIDだけを保存していた。Unixではcommand lineで再利用PIDを区別し、
    // Windowsでは強い検証に新identityが必要なので、ここでは実行ファイル一致
    // のみを受け入れる。
    #[cfg(unix)]
    {
        Ok(process_command_line(pid)
            .as_deref()
            .is_some_and(|command| watch_command_matches(paths, command)))
    }
    #[cfg(windows)]
    {
        let current = process_identity(std::process::id());
        let actual = process_identity(pid);
        return Ok(current.executable.is_some() && current.executable == actual.executable);
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (paths, pid);
        Ok(false)
    }
}

#[cfg(unix)]
fn process_command_line(pid: u32) -> Option<String> {
    let output = ProcessCommand::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let command = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!command.is_empty()).then_some(command)
}

#[cfg(unix)]
fn watch_command_matches(paths: &Paths, command: &str) -> bool {
    let exe_name = env::current_exe()
        .ok()
        .and_then(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().to_string())
        })
        .unwrap_or_else(|| "mj".into());
    let padded = format!(" {command} ");
    command.contains(&exe_name)
        && command.contains("--home")
        && command.contains(&paths.home.to_string_lossy().to_string())
        && padded.contains(" watch ")
}

pub(crate) fn acquire_daemon_lock(paths: &Paths) -> Result<ProcessLock> {
    if paths.daemon_lock.exists() {
        let owner = fs::read_to_string(&paths.daemon_lock)
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok());
        let stale = match owner {
            Some(pid) => !daemon_process_matches(paths, pid)?,
            None => true,
        };
        if stale {
            let _ = fs::remove_file(&paths.daemon_lock);
        }
    }
    acquire_process_lock(&paths.daemon_lock, "daemon")
}

pub(crate) fn ensure_daemon_running(paths: &Paths) -> Result<Option<u32>> {
    if !auto_daemon_enabled() {
        return Ok(None);
    }
    let conn = open_db(paths)?;
    let has_active_roots = roots(&conn)?.iter().any(|root| root.status == "active");
    if !has_active_roots {
        return Ok(None);
    }
    let health = daemon_health(paths)?;
    if health.state == DaemonHealthState::Running {
        return Ok(None);
    }
    if health.state == DaemonHealthState::Stale {
        if discover_running_watch_daemon(paths)?.is_some() {
            return Ok(None);
        }
        cleanup_daemon_runtime(paths);
    }
    let config = read_config(paths)?;
    let backend = normalize_watch_backend(&config.watch.backend)?;
    validate_watch_mode(&config.watch.mode)?;
    start_watch_daemon(
        paths,
        WatchDaemonLaunchConfig {
            backend,
            mode: config.watch.mode.clone(),
            interval_secs: config.watch.interval,
            debounce_ms: config.watch.debounce,
            settle_ms: config.watch.settle,
            buffer_max_ms: config.watch.buffer_max,
            buffer_max_events: config.watch.buffer_max_events,
            periodic_rescan_secs: config.watch.periodic_rescan,
            max_rss_mib: default_watch_max_rss_mib(),
        },
    )
    .map(Some)
}

pub(crate) fn apply_env_files(paths: &Paths) -> Result<()> {
    for (key, value) in daemon_env(paths)? {
        if env::var_os(&key).is_none() {
            // 起動直後の単一スレッド領域で、子プロセスや remote backend が参照する環境を補完する。
            unsafe {
                env::set_var(key, value);
            }
        }
    }
    Ok(())
}

fn auto_daemon_enabled() -> bool {
    std::env::var("MAJUTSU_AUTO_DAEMON")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "no" | "NO"))
        .unwrap_or(true)
}

pub(crate) struct WatchDaemonLaunchConfig {
    pub(crate) backend: &'static str,
    pub(crate) mode: String,
    pub(crate) interval_secs: u64,
    pub(crate) debounce_ms: u64,
    pub(crate) settle_ms: u64,
    pub(crate) buffer_max_ms: u64,
    pub(crate) buffer_max_events: usize,
    pub(crate) periodic_rescan_secs: u64,
    pub(crate) max_rss_mib: u64,
}

pub(crate) fn start_watch_daemon(paths: &Paths, config: WatchDaemonLaunchConfig) -> Result<u32> {
    if let Some(pid) = read_pid(&paths.daemon_pid)? {
        if daemon_process_matches(paths, pid)? {
            bail!("daemon already running with pid {pid}");
        }
        if let Some(live_pid) = discover_running_watch_daemon(paths)? {
            adopt_daemon_pid(paths, live_pid)?;
            return Ok(live_pid);
        }
        cleanup_daemon_runtime(paths);
    }
    fs::create_dir_all(&paths.runtime)?;
    fs::create_dir_all(&paths.logs)?;

    #[cfg(windows)]
    {
        let task_name = start_watch_daemon_windows(paths, &config)?;
        let pid = wait_for_daemon_registered_pid(paths, 0)?;
        disable_windows_daemon_launch_task(&task_name);
        append_daemon_launch_log(paths, pid, &config);
        Ok(pid)
    }

    #[cfg(not(windows))]
    {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(daemon_log_path(paths))?;
        let mut command = ProcessCommand::new(env::current_exe()?);
        command
            .arg("--home")
            .arg(&paths.home)
            .arg("watch")
            .arg("--foreground")
            .arg("true")
            .arg("--backend")
            .arg(config.backend)
            .arg("--mode")
            .arg(&config.mode)
            .arg("--interval-secs")
            .arg(config.interval_secs.to_string())
            .arg("--debounce-ms")
            .arg(config.debounce_ms.to_string())
            .arg("--settle-ms")
            .arg(config.settle_ms.to_string())
            .arg("--buffer-max-ms")
            .arg(config.buffer_max_ms.to_string())
            .arg("--buffer-max-events")
            .arg(config.buffer_max_events.to_string())
            .arg("--periodic-rescan-secs")
            .arg(config.periodic_rescan_secs.to_string())
            .arg("--max-rss-mib")
            .arg(config.max_rss_mib.to_string())
            .stdin(Stdio::null());
        command
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        for (key, value) in daemon_env(paths)? {
            command.env(key, value);
        }
        configure_daemon_operation_attribution(&mut command);
        configure_background_command(&mut command);
        detach_daemon_process(&mut command);
        let child = command.spawn()?;
        let pid = wait_for_daemon_registered_pid(paths, child.id())?;
        append_daemon_launch_log(paths, pid, &config);
        Ok(pid)
    }
}

fn wait_for_daemon_registered_pid(paths: &Paths, fallback_pid: u32) -> Result<u32> {
    #[cfg(windows)]
    let attempts = 300;
    #[cfg(not(windows))]
    let attempts = 50;
    for _ in 0..attempts {
        if let Some(pid) = read_pid(&paths.daemon_pid)?
            && daemon_process_matches(paths, pid)?
        {
            let _ = write_process_identity(&daemon_identity_path(paths), &process_identity(pid));
            return Ok(pid);
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    if fallback_pid == 0 {
        bail!("daemon did not register a live pid before startup timeout");
    }
    adopt_daemon_pid(paths, fallback_pid)?;
    Ok(fallback_pid)
}

fn append_daemon_launch_log(paths: &Paths, pid: u32, config: &WatchDaemonLaunchConfig) {
    append_daemon_log(
        paths,
        &format!(
            "daemon-launch pid={pid} backend={} mode={} debounce_ms={} settle_ms={} buffer_max_ms={} buffer_max_events={} periodic_rescan_secs={} max_rss_mib={}",
            config.backend,
            config.mode,
            config.debounce_ms,
            config.settle_ms,
            config.buffer_max_ms,
            config.buffer_max_events,
            config.periodic_rescan_secs,
            config.max_rss_mib
        ),
    );
}

#[cfg(windows)]
fn start_watch_daemon_windows(paths: &Paths, config: &WatchDaemonLaunchConfig) -> Result<String> {
    let exe = env::current_exe()?;
    let stdout_log = daemon_log_path(paths);
    let stderr_log = paths.logs.join("majutsu.err.log");
    let args = vec![
        "--home".to_string(),
        paths.home.display().to_string(),
        "watch".to_string(),
        "--foreground".to_string(),
        "true".to_string(),
        "--backend".to_string(),
        config.backend.to_string(),
        "--mode".to_string(),
        config.mode.clone(),
        "--interval-secs".to_string(),
        config.interval_secs.to_string(),
        "--debounce-ms".to_string(),
        config.debounce_ms.to_string(),
        "--settle-ms".to_string(),
        config.settle_ms.to_string(),
        "--buffer-max-ms".to_string(),
        config.buffer_max_ms.to_string(),
        "--buffer-max-events".to_string(),
        config.buffer_max_events.to_string(),
        "--periodic-rescan-secs".to_string(),
        config.periodic_rescan_secs.to_string(),
        "--max-rss-mib".to_string(),
        config.max_rss_mib.to_string(),
    ];
    let quoted_args = args
        .iter()
        .map(|arg| format!("'{}'", powershell_single_quote(arg)))
        .collect::<Vec<_>>()
        .join(", ");

    let mut runner = String::from("$ErrorActionPreference = 'Stop'\n");
    for key in DAEMON_ATTRIBUTION_ENV_KEYS {
        let _ = writeln!(runner, "$env:{key} = $null");
    }
    runner.push_str("$env:MAJUTSU_DAEMON = '1'\n");
    for (key, value) in daemon_env(paths)? {
        let _ = writeln!(runner, "$env:{key} = '{}'", powershell_single_quote(&value));
    }
    let _ = writeln!(runner, "$daemonArgs = @({quoted_args})");
    let _ = writeln!(
        runner,
        "& '{}' @daemonArgs >> '{}' 2>> '{}'",
        powershell_single_quote(&exe.display().to_string()),
        powershell_single_quote(&stdout_log.display().to_string()),
        powershell_single_quote(&stderr_log.display().to_string())
    );

    let runner_path = paths.runtime.join("run-daemon.ps1");
    fs::write(&runner_path, runner)?;

    let task_name = windows_daemon_launch_task_name(paths);
    let task_command = format!(
        "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"{}\"",
        runner_path.display()
    );
    let create = create_windows_daemon_launch_task(&task_name, &task_command, true)
        .or_else(|_| create_windows_daemon_launch_task(&task_name, &task_command, false))?;
    if !create.status.success() {
        bail!(
            "failed to create Windows daemon launch task\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&create.stdout),
            String::from_utf8_lossy(&create.stderr)
        );
    }
    let run = ProcessCommand::new("schtasks.exe")
        .args(["/Run", "/TN", &task_name])
        .output()?;
    if !run.status.success() {
        disable_windows_daemon_launch_task(&task_name);
        bail!(
            "failed to run Windows daemon launch task\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&run.stdout),
            String::from_utf8_lossy(&run.stderr)
        );
    }
    Ok(task_name)
}

#[cfg(windows)]
fn create_windows_daemon_launch_task(
    task_name: &str,
    task_command: &str,
    system: bool,
) -> Result<std::process::Output> {
    let mut command = ProcessCommand::new("schtasks.exe");
    command.args([
        "/Create",
        "/TN",
        task_name,
        "/SC",
        "ONCE",
        "/ST",
        "23:59",
        "/TR",
        task_command,
        "/F",
    ]);
    if system {
        command.args(["/RU", "SYSTEM", "/RL", "HIGHEST"]);
    }
    Ok(command.output()?)
}

#[cfg(windows)]
fn disable_windows_daemon_launch_task(task_name: &str) {
    let _ = ProcessCommand::new("schtasks.exe")
        .args(["/Change", "/TN", task_name, "/DISABLE"])
        .output();
}

#[cfg(windows)]
fn windows_daemon_launch_task_name(paths: &Paths) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in paths.home.as_os_str().to_string_lossy().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("MajutsuWatchLaunch-{hash:016x}")
}

#[cfg(windows)]
fn powershell_single_quote(value: &str) -> String {
    value.replace('\'', "''")
}

fn daemon_doctor(paths: &Paths) -> Result<()> {
    let health = daemon_health(paths)?;
    println!("daemon {}", health.label());
    if let Some(pid) = health.pid {
        println!("pid {pid}");
    }
    println!("pid_file {}", paths.daemon_pid.display());
    println!("socket {}", paths.runtime.join("daemon.sock").display());
    println!("log {}", daemon_log_path(paths).display());
    match health.state {
        DaemonHealthState::Running if health.ipc_available => {
            println!("action none");
        }
        DaemonHealthState::Running => {
            println!("action mj daemon restart");
            println!("reason process is alive but IPC is unavailable");
        }
        DaemonHealthState::Stale => {
            println!("action mj daemon restart");
            println!("reason pid file points to a dead process");
        }
        DaemonHealthState::Stopped => {
            println!("action mj daemon start");
        }
    }
    warn_stale_systemd_units(paths);
    if let Ok(tail) = daemon_log_tail(paths, 20)
        && !tail.is_empty()
    {
        println!("recent_log");
        print!("{tail}");
    }
    Ok(())
}

fn warn_stale_systemd_units(paths: &Paths) {
    for path in systemd_unit_candidates() {
        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        if !content.contains("majutsu") && !content.contains(&paths.home.display().to_string()) {
            continue;
        }
        let mut reasons = Vec::new();
        if content.contains("Type=forking")
            || content.contains(" daemon start")
            || content.contains("\"daemon\" \"start\"")
            || content.contains("'daemon' 'start'")
        {
            reasons.push("legacy forking daemon style");
        }
        if !content.contains("MemoryMax=") {
            reasons.push("missing MemoryMax");
        }
        if !content.contains("OOMPolicy=stop") {
            reasons.push("missing OOMPolicy=stop");
        }
        if reasons.is_empty() {
            continue;
        }
        println!("systemd_unit_stale {}", path.display());
        println!("reason {}", reasons.join(", "));
        println!("action regenerate with `mj daemon service --provider systemd`");
    }
}

fn systemd_unit_candidates() -> Vec<std::path::PathBuf> {
    let mut candidates = Vec::new();
    if let Some(home) = std::env::var_os("HOME") {
        candidates
            .push(std::path::PathBuf::from(home).join(".config/systemd/user/majutsu.service"));
    }
    candidates.push(std::path::PathBuf::from(
        "/etc/systemd/system/majutsu.service",
    ));
    candidates
}

fn daemon_log_path(paths: &Paths) -> std::path::PathBuf {
    paths.logs.join("majutsu.log")
}

fn append_daemon_log(paths: &Paths, line: &str) {
    let _ = fs::create_dir_all(&paths.logs);
    let timestamp = Utc::now().to_rfc3339();
    if let Ok(mut file) = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(daemon_log_path(paths))
    {
        let _ = writeln!(file, "{timestamp} {line}");
    }
}

fn daemon_log_tail(paths: &Paths, lines: usize) -> Result<String> {
    let content = fs::read_to_string(daemon_log_path(paths))?;
    let mut selected = content.lines().rev().take(lines).collect::<Vec<_>>();
    selected.reverse();
    if selected.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!("{}\n", selected.join("\n")))
    }
}

#[cfg(not(windows))]
fn configure_daemon_operation_attribution(command: &mut ProcessCommand) {
    for key in DAEMON_ATTRIBUTION_ENV_KEYS {
        command.env_remove(key);
    }
    command.env("MAJUTSU_DAEMON", "1");
}

#[cfg(unix)]
fn detach_daemon_process(command: &mut ProcessCommand) {
    use std::os::unix::process::CommandExt;

    // daemon 子プロセスを呼び出し元の端末・セッションから切り離し、親終了時の SIGHUP に巻き込まれないようにする。
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(all(not(unix), not(windows)))]
fn detach_daemon_process(_: &mut ProcessCommand) {}

fn daemon_env(paths: &Paths) -> Result<BTreeMap<String, String>> {
    let mut envs = BTreeMap::new();
    let mut files = Vec::new();
    if let Ok(value) = env::var("MAJUTSU_DAEMON_ENV_FILE") {
        files.extend(
            value
                .split(':')
                .filter(|path| !path.is_empty())
                .map(std::path::PathBuf::from),
        );
    }
    files.push(paths.home.join("daemon.env"));
    files.push(paths.home.join("s3.env"));
    for file in files {
        if !file.exists() {
            continue;
        }
        let content = fs::read_to_string(&file)?;
        for (key, value) in parse_daemon_env_file(&content) {
            envs.insert(key, value);
        }
    }
    Ok(envs)
}

fn parse_daemon_env_file(content: &str) -> BTreeMap<String, String> {
    let mut envs = BTreeMap::new();
    for line in content.lines() {
        let mut line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("export ") {
            line = rest.trim_start();
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty()
            || !key
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            continue;
        }
        let value = unquote_env_value(value.trim());
        envs.insert(key.to_string(), value);
    }
    envs
}

fn unquote_env_value(value: &str) -> String {
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

fn stop_daemon_process(pid: u32) -> Result<()> {
    crate::platform_runtime::terminate_process(pid, std::time::Duration::from_secs(5))
}

fn cleanup_daemon_runtime(paths: &Paths) {
    let _ = fs::remove_file(&paths.daemon_pid);
    let _ = fs::remove_file(daemon_identity_path(paths));
    let _ = fs::remove_file(&paths.daemon_lock);
    let _ = fs::remove_file(paths.runtime.join("daemon.sock"));
    let _ = fs::remove_file(paths.runtime.join("watch-backend"));
}

pub(crate) struct ForegroundDaemonRuntime {
    paths: Paths,
}

impl ForegroundDaemonRuntime {
    pub(crate) fn register(paths: &Paths) -> Result<Self> {
        fs::create_dir_all(&paths.runtime)?;
        let pid = std::process::id();
        fs::write(&paths.daemon_pid, pid.to_string())?;
        write_process_identity(&daemon_identity_path(paths), &process_identity(pid))?;
        Ok(Self {
            paths: paths.clone(),
        })
    }
}

impl Drop for ForegroundDaemonRuntime {
    fn drop(&mut self) {
        cleanup_daemon_runtime(&self.paths);
    }
}

#[cfg(any(unix, windows))]
pub(crate) fn start_daemon_ipc(paths: &Paths) -> Result<()> {
    use crate::platform_runtime::UnixListener;

    fs::create_dir_all(&paths.runtime)?;
    let sock = paths.runtime.join("daemon.sock");
    let _ = fs::remove_file(&sock);
    let listener = UnixListener::bind(&sock)?;
    #[cfg(unix)]
    fs::set_permissions(&sock, fs::Permissions::from_mode(0o666))?;
    let home = paths.home.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    let _ = handle_daemon_ipc(&home, &mut stream);
                }
                Err(_) => break,
            }
        }
    });
    Ok(())
}

#[cfg(any(unix, windows))]
pub(crate) fn ensure_daemon_ipc(paths: &Paths) -> Result<()> {
    let sock = paths.runtime.join("daemon.sock");
    if sock.exists() && daemon_ipc_request(paths, "status").is_ok() {
        return Ok(());
    }
    start_daemon_ipc(paths)
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn ensure_daemon_ipc(_: &Paths) -> Result<()> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn start_daemon_ipc(_: &Paths) -> Result<()> {
    Ok(())
}

#[cfg(any(unix, windows))]
fn handle_daemon_ipc(home: &Path, stream: &mut crate::platform_runtime::UnixStream) -> Result<()> {
    let mut command = String::new();
    stream.read_to_string(&mut command)?;
    let paths = resolve_paths(Some(home.to_path_buf()))?;
    match command.trim() {
        "status" => {
            let stats = daemon_stats(&paths)?;
            writeln!(stream, "running pid {}", stats.pid)?;
            writeln!(stream, "ipc ok")?;
            writeln!(stream, "rss_kib {}", stats.rss_kib)?;
            writeln!(stream, "vm_size_kib {}", stats.vm_size_kib)?;
            writeln!(stream, "roots {}", stats.roots)?;
            writeln!(stream, "current {}", stats.current)?;
            writeln!(stream, "journal_events {}", stats.journal_events)?;
            writeln!(
                stream,
                "processed_journal_events {}",
                stats.processed_journal_events
            )?;
            writeln!(
                stream,
                "pending_journal_event_count {}",
                stats.pending_journal_event_count
            )?;
            writeln!(
                stream,
                "pending_journal_events {}",
                stats.pending_journal_events
            )?;
            writeln!(
                stream,
                "pending_journal_state {}",
                stats.pending_journal_state
            )?;
            writeln!(
                stream,
                "pending_journal_oldest_age_secs {}",
                stats.pending_journal_oldest_age_secs
            )?;
            writeln!(stream, "queued_uploads {}", stats.queued_uploads)?;
            writeln!(
                stream,
                "queued_uploads_retrying {}",
                stats.queued_uploads_retrying
            )?;
            writeln!(
                stream,
                "queued_uploads_delayed {}",
                stats.queued_uploads_delayed
            )?;
            writeln!(
                stream,
                "queued_upload_next_retry_after {}",
                stats.queued_upload_next_retry_after
            )?;
            writeln!(
                stream,
                "queued_upload_attempts {}",
                stats.queued_upload_attempts
            )?;
            writeln!(
                stream,
                "queued_upload_max_attempts {}",
                stats.queued_upload_max_attempts
            )?;
            writeln!(
                stream,
                "upload_queue_backpressure {}",
                stats.upload_queue_backpressure
            )?;
            writeln!(stream, "restore_jobs {}", stats.restore_jobs)?;
            for (status, count) in stats.root_statuses {
                writeln!(stream, "root_status {status} {count}")?;
            }
            for (status, count) in stats.restore_statuses {
                writeln!(stream, "restore_status {status} {count}")?;
            }
        }
        "metrics" => {
            let stats = daemon_stats(&paths)?;
            writeln!(stream, "# TYPE majutsu_daemon_up gauge")?;
            writeln!(stream, "majutsu_daemon_up 1")?;
            writeln!(stream, "# TYPE majutsu_daemon_ipc_up gauge")?;
            writeln!(stream, "majutsu_daemon_ipc_up 1")?;
            writeln!(stream, "# TYPE majutsu_daemon_rss_kib gauge")?;
            writeln!(stream, "majutsu_daemon_rss_kib {}", stats.rss_kib)?;
            writeln!(stream, "# TYPE majutsu_daemon_vm_size_kib gauge")?;
            writeln!(stream, "majutsu_daemon_vm_size_kib {}", stats.vm_size_kib)?;
            writeln!(stream, "# TYPE majutsu_daemon_roots gauge")?;
            writeln!(stream, "majutsu_daemon_roots {}", stats.roots)?;
            writeln!(stream, "# TYPE majutsu_daemon_journal_events gauge")?;
            writeln!(
                stream,
                "majutsu_daemon_journal_events {}",
                stats.journal_events
            )?;
            writeln!(
                stream,
                "majutsu_daemon_processed_journal_events {}",
                stats.processed_journal_events
            )?;
            writeln!(
                stream,
                "majutsu_daemon_pending_journal_event_count {}",
                stats.pending_journal_event_count
            )?;
            writeln!(
                stream,
                "majutsu_daemon_pending_journal_events {}",
                bool_metric(stats.pending_journal_events)
            )?;
            writeln!(
                stream,
                "majutsu_daemon_pending_journal_oldest_age_secs {}",
                stats.pending_journal_oldest_age_secs
            )?;
            writeln!(stream, "# TYPE majutsu_daemon_queued_uploads gauge")?;
            writeln!(
                stream,
                "majutsu_daemon_queued_uploads {}",
                stats.queued_uploads
            )?;
            writeln!(
                stream,
                "majutsu_daemon_queued_uploads_retrying {}",
                stats.queued_uploads_retrying
            )?;
            writeln!(
                stream,
                "majutsu_daemon_queued_uploads_delayed {}",
                stats.queued_uploads_delayed
            )?;
            writeln!(
                stream,
                "majutsu_daemon_queued_upload_attempts {}",
                stats.queued_upload_attempts
            )?;
            writeln!(
                stream,
                "majutsu_daemon_queued_upload_max_attempts {}",
                stats.queued_upload_max_attempts
            )?;
            writeln!(
                stream,
                "majutsu_daemon_upload_queue_backpressure {}",
                bool_metric(stats.upload_queue_backpressure)
            )?;
            writeln!(stream, "# TYPE majutsu_daemon_restore_jobs gauge")?;
            writeln!(stream, "majutsu_daemon_restore_jobs {}", stats.restore_jobs)?;
            for (status, count) in stats.root_statuses {
                writeln!(
                    stream,
                    "majutsu_daemon_root_status{{status=\"{}\"}} {}",
                    escape_metric_label(&status),
                    count
                )?;
            }
            for (status, count) in stats.restore_statuses {
                writeln!(
                    stream,
                    "majutsu_daemon_restore_status{{status=\"{}\"}} {}",
                    escape_metric_label(&status),
                    count
                )?;
            }
        }
        other => {
            writeln!(stream, "error unknown command {other}")?;
        }
    }
    Ok(())
}

fn daemon_stats(paths: &Paths) -> Result<DaemonStats> {
    let conn = open_db(paths)?;
    let roots = roots(&conn)?;
    let mut root_statuses = BTreeMap::new();
    for root in &roots {
        *root_statuses.entry(root.status.clone()).or_insert(0usize) += 1;
    }
    let current = current_snapshot(&conn)?.unwrap_or_else(|| "(none)".into());
    let journal_records = event_journal_records(paths)?;
    let last_snapshot_finish = journal_records
        .iter()
        .filter(|event| event.is_snapshot_finish())
        .map(|event| event.observed_at)
        .max();
    let pending_records = journal_records
        .iter()
        .filter(|event| {
            event.is_pending_trigger()
                && last_snapshot_finish
                    .map(|finished_at| event.observed_at > finished_at)
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    let pending_journal_event_count = pending_records.len();
    let pending_journal_oldest_age_secs = pending_records
        .iter()
        .map(|event| Utc::now().signed_duration_since(event.observed_at))
        .filter_map(|duration| u64::try_from(duration.num_seconds().max(0)).ok())
        .max()
        .unwrap_or(0);
    let upload_stats = upload_queue_stats(paths)?;
    let pending_journal_state = pending_journal_state(
        pending_journal_event_count,
        pending_journal_oldest_age_secs,
        upload_stats.total,
    );
    let pending_journal_events = pending_journal_event_count > 0;
    let processed_journal_events = journal_records
        .len()
        .saturating_sub(pending_journal_event_count);
    let restore_statuses = restore_queue_status_counts(paths)?;
    let restore_jobs = restore_statuses
        .iter()
        .filter(|(status, _)| status.as_str() != "done")
        .map(|(_, count)| *count)
        .sum::<usize>();

    Ok(DaemonStats {
        pid: std::process::id(),
        rss_kib: self_proc_status_kib("VmRSS").unwrap_or(0),
        vm_size_kib: self_proc_status_kib("VmSize").unwrap_or(0),
        roots: roots.len(),
        current,
        journal_events: journal_records.len(),
        processed_journal_events,
        pending_journal_event_count,
        pending_journal_events,
        pending_journal_state,
        pending_journal_oldest_age_secs,
        queued_uploads: upload_stats.total,
        queued_uploads_retrying: upload_stats.retrying,
        queued_uploads_delayed: upload_stats.delayed,
        queued_upload_next_retry_after: upload_stats
            .next_retry_after
            .map(|retry_after| retry_after.to_rfc3339())
            .unwrap_or_else(|| "(none)".into()),
        queued_upload_attempts: upload_stats.attempts,
        queued_upload_max_attempts: upload_stats.max_attempts,
        upload_queue_backpressure: upload_stats.has_backpressure(),
        restore_jobs,
        root_statuses,
        restore_statuses,
    })
}

fn pending_journal_state(pending: usize, oldest_age_secs: u64, queued_uploads: usize) -> String {
    if pending == 0 {
        "clear".into()
    } else if queued_uploads > 0 {
        "syncing".into()
    } else if oldest_age_secs >= 300 {
        "stalled".into()
    } else {
        "processing".into()
    }
}

fn self_proc_status_kib(name: &str) -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        let (key, rest) = line.split_once(':')?;
        if key != name {
            continue;
        }
        return rest
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok());
    }
    None
}

fn bool_metric(value: bool) -> u8 {
    u8::from(value)
}

fn escape_metric_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn restore_queue_status_counts(paths: &Paths) -> Result<BTreeMap<String, usize>> {
    let dir = paths.home.join("queue/restores");
    let mut counts = BTreeMap::new();
    if !dir.exists() {
        return Ok(counts);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file()
            || entry.path().extension().and_then(|ext| ext.to_str()) != Some("json")
        {
            continue;
        }
        let job: RestoreQueueItem = serde_json::from_slice(&fs::read(entry.path())?)?;
        *counts.entry(job.status).or_insert(0) += 1;
    }
    Ok(counts)
}

#[cfg(any(unix, windows))]
pub(crate) fn daemon_ipc_request(paths: &Paths, command: &str) -> Result<String> {
    use crate::platform_runtime::UnixStream;

    let mut stream = UnixStream::connect(paths.runtime.join("daemon.sock"))?;
    stream.write_all(command.as_bytes())?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut reply = String::new();
    stream.read_to_string(&mut reply)?;
    let reply = reply.trim_end().to_string();
    if reply.is_empty() {
        bail!("empty daemon IPC reply");
    }
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::{parse_daemon_env_file, watch_command_matches};
    use crate::config::resolve_paths;
    use std::path::PathBuf;

    #[test]
    fn parses_daemon_env_files() {
        let envs = parse_daemon_env_file(
            r#"
# comment
export AWS_ACCESS_KEY_ID=example
AWS_SECRET_ACCESS_KEY='secret value'
AWS_ENDPOINT_URL="http://127.0.0.1:9000"
invalid line
BAD-NAME=ignored
"#,
        );

        assert_eq!(envs.get("AWS_ACCESS_KEY_ID").unwrap(), "example");
        assert_eq!(envs.get("AWS_SECRET_ACCESS_KEY").unwrap(), "secret value");
        assert_eq!(
            envs.get("AWS_ENDPOINT_URL").unwrap(),
            "http://127.0.0.1:9000"
        );
        assert!(!envs.contains_key("BAD-NAME"));
    }

    #[cfg(unix)]
    #[test]
    fn watch_command_matching_requires_the_configured_home() {
        let home = PathBuf::from("/tmp/majutsu-test-home");
        let paths = resolve_paths(Some(home.clone())).unwrap();
        let exe = std::env::current_exe().unwrap();
        let valid = format!(
            "{} --home {} watch --foreground true --backend fanotify",
            exe.display(),
            home.display()
        );
        let other_home = format!(
            "{} --home /tmp/another-home watch --foreground true --backend fanotify",
            exe.display()
        );
        assert!(watch_command_matches(&paths, &valid));
        assert!(!watch_command_matches(&paths, &other_home));
        assert!(!watch_command_matches(
            &paths,
            &format!("{} --home {} daemon status", exe.display(), home.display())
        ));
    }
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn daemon_ipc_request(_: &Paths, _: &str) -> Result<String> {
    bail!("daemon IPC is not supported on this platform")
}
