use crate::majutsu_daemon::{DaemonServiceConfig, DaemonServiceScope, render_daemon_service};
use crate::majutsu_restore::RestoreQueueItem;
use anyhow::{Result, anyhow, bail};
use chrono::Utc;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
#[cfg(not(windows))]
use std::process::Stdio;

use crate::cli::DaemonCommand;
use crate::config::{Paths, read_config, validate_watch_mode};
#[cfg(not(windows))]
use crate::platform_runtime::configure_background_command;
use crate::process_runtime::{pid_alive, read_pid};
use crate::queue_runtime::{event_journal_records, upload_queue_stats};
use crate::root_state::roots;
use crate::snapshot_state::current_snapshot;
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
            let backend = normalize_watch_backend(&config.watch.backend)?;
            let scope = match scope.as_str() {
                "user" => DaemonServiceScope::User,
                "system" => DaemonServiceScope::System,
                other => bail!("unsupported daemon service scope: {other}"),
            };
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
            })
            .map_err(anyhow::Error::msg)?;
            print!("{service}");
        }
        DaemonCommand::Stop => {
            let pid =
                read_pid(&paths.daemon_pid)?.ok_or_else(|| anyhow!("daemon pid file not found"))?;
            stop_daemon_process(pid)?;
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
        DaemonCommand::Metrics => {
            if let Some(pid) = read_pid(&paths.daemon_pid)? {
                if pid_alive(pid) {
                    if let Ok(reply) = daemon_ipc_request(paths, "metrics") {
                        println!("{reply}");
                    } else {
                        println!("majutsu_daemon_up 1");
                        println!("majutsu_daemon_ipc_up 0");
                    }
                } else {
                    println!("majutsu_daemon_up 0");
                    println!("majutsu_daemon_stale_pid {}", pid);
                }
            } else {
                println!("majutsu_daemon_up 0");
            }
        }
    }
    Ok(())
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
    if !pid_alive(pid) {
        return Ok(DaemonHealth {
            state: DaemonHealthState::Stale,
            pid: Some(pid),
            ipc_available: false,
        });
    }
    Ok(DaemonHealth {
        state: DaemonHealthState::Running,
        pid: Some(pid),
        ipc_available: daemon_ipc_request(paths, "status").is_ok(),
    })
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
}

pub(crate) fn start_watch_daemon(paths: &Paths, config: WatchDaemonLaunchConfig) -> Result<u32> {
    if let Some(pid) = read_pid(&paths.daemon_pid)? {
        if pid_alive(pid) {
            bail!("daemon already running with pid {pid}");
        }
        cleanup_daemon_runtime(paths);
    }
    fs::create_dir_all(&paths.runtime)?;
    fs::create_dir_all(&paths.logs)?;

    #[cfg(windows)]
    {
        let pid = start_watch_daemon_windows(paths, &config)?;
        write_daemon_pid_and_log(paths, pid, &config)?;
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
        let pid = child.id();
        write_daemon_pid_and_log(paths, pid, &config)?;
        Ok(pid)
    }
}

fn write_daemon_pid_and_log(
    paths: &Paths,
    pid: u32,
    config: &WatchDaemonLaunchConfig,
) -> Result<()> {
    fs::write(&paths.daemon_pid, pid.to_string())?;
    append_daemon_log(
        paths,
        &format!(
            "daemon-launch pid={pid} backend={} mode={} debounce_ms={} settle_ms={} buffer_max_ms={} buffer_max_events={} periodic_rescan_secs={}",
            config.backend,
            config.mode,
            config.debounce_ms,
            config.settle_ms,
            config.buffer_max_ms,
            config.buffer_max_events,
            config.periodic_rescan_secs
        ),
    );
    Ok(())
}

#[cfg(windows)]
fn start_watch_daemon_windows(paths: &Paths, config: &WatchDaemonLaunchConfig) -> Result<u32> {
    let exe = env::current_exe()?;
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
    ];
    let quoted_args = args
        .iter()
        .map(|arg| format!("'{}'", powershell_single_quote(arg)))
        .collect::<Vec<_>>()
        .join(", ");

    let mut script = String::from("$ErrorActionPreference = 'Stop'\n");
    for key in DAEMON_ATTRIBUTION_ENV_KEYS {
        let _ = writeln!(script, "$env:{key} = $null");
    }
    script.push_str("$env:MAJUTSU_DAEMON = '1'\n");
    for (key, value) in daemon_env(paths)? {
        let _ = writeln!(script, "$env:{key} = '{}'", powershell_single_quote(&value));
    }
    let _ = writeln!(
        script,
        "$p = Start-Process -FilePath '{}' -ArgumentList @({quoted_args}) -WindowStyle Hidden -PassThru",
        powershell_single_quote(&exe.display().to_string())
    );
    script.push_str("[Console]::Out.WriteLine($p.Id)\n");

    let script_path = paths.runtime.join("launch-daemon.ps1");
    fs::write(&script_path, script)?;
    let output = ProcessCommand::new("powershell.exe")
        .arg("-NoProfile")
        .arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-File")
        .arg(&script_path)
        .output()?;
    if !output.status.success() {
        bail!(
            "failed to launch Windows daemon\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u32>()
        .map_err(|err| anyhow!("failed to parse Windows daemon pid: {err}"))
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
    if let Ok(tail) = daemon_log_tail(paths, 20)
        && !tail.is_empty()
    {
        println!("recent_log");
        print!("{tail}");
    }
    Ok(())
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
    let _ = fs::remove_file(paths.runtime.join("daemon.sock"));
}

#[cfg(any(unix, windows))]
pub(crate) fn start_daemon_ipc(paths: &Paths) -> Result<()> {
    use crate::platform_runtime::UnixListener;

    fs::create_dir_all(&paths.runtime)?;
    let sock = paths.runtime.join("daemon.sock");
    let _ = fs::remove_file(&sock);
    let listener = UnixListener::bind(&sock)?;
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
    use super::parse_daemon_env_file;

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
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn daemon_ipc_request(_: &Paths, _: &str) -> Result<String> {
    bail!("daemon IPC is not supported on this platform")
}
