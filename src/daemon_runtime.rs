use anyhow::{Result, anyhow, bail};
use majutsu_daemon::{DaemonServiceConfig, render_daemon_service};
use majutsu_restore::RestoreQueueItem;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command as ProcessCommand, Stdio};
use std::thread;
use std::time::Duration;

use crate::cli::DaemonCommand;
use crate::config::{Paths, read_config, validate_watch_mode};
use crate::process_runtime::{pid_alive, read_pid};
use crate::queue_runtime::{event_journal_records, upload_queue_stats};
use crate::root_state::roots;
use crate::snapshot_state::current_snapshot;
use crate::watch_runtime::normalize_watch_backend;
use crate::{open_db, resolve_paths};

struct DaemonStats {
    pid: u32,
    roots: usize,
    current: String,
    journal_events: usize,
    pending_journal_events: bool,
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
            periodic_rescan_secs,
        } => {
            let configured_backend = backend.unwrap_or_else(|| config.watch.backend.clone());
            let backend = normalize_watch_backend(&configured_backend)?;
            let mode = mode.unwrap_or_else(|| config.watch.mode.clone());
            validate_watch_mode(&mode)?;
            let pid = start_watch_daemon(
                paths,
                backend,
                &mode,
                interval_secs.unwrap_or(config.watch.interval),
                debounce_ms.unwrap_or(config.watch.debounce),
                settle_ms.unwrap_or(config.watch.settle),
                periodic_rescan_secs.unwrap_or(config.watch.periodic_rescan),
            )?;
            println!("started daemon pid {pid}");
        }
        DaemonCommand::Service { provider } => {
            let exe = env::current_exe()?;
            let backend = normalize_watch_backend(&config.watch.backend)?;
            let service = render_daemon_service(DaemonServiceConfig {
                provider: &provider,
                exe: &exe,
                home: &paths.home,
                backend,
                mode: &config.watch.mode,
                interval_secs: config.watch.interval,
                debounce_ms: config.watch.debounce,
                settle_ms: config.watch.settle,
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
                        println!("{reply}");
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
        backend,
        &config.watch.mode,
        config.watch.interval,
        config.watch.debounce,
        config.watch.settle,
        config.watch.periodic_rescan,
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

pub(crate) fn start_watch_daemon(
    paths: &Paths,
    backend: &str,
    mode: &str,
    interval_secs: u64,
    debounce_ms: u64,
    settle_ms: u64,
    periodic_rescan_secs: u64,
) -> Result<u32> {
    if let Some(pid) = read_pid(&paths.daemon_pid)? {
        if pid_alive(pid) {
            bail!("daemon already running with pid {pid}");
        }
    }
    fs::create_dir_all(&paths.runtime)?;
    fs::create_dir_all(&paths.logs)?;
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(paths.logs.join("majutsu.log"))?;
    let mut command = ProcessCommand::new(env::current_exe()?);
    command
        .arg("--home")
        .arg(&paths.home)
        .arg("watch")
        .arg("--foreground")
        .arg("true")
        .arg("--backend")
        .arg(backend)
        .arg("--mode")
        .arg(mode)
        .arg("--interval-secs")
        .arg(interval_secs.to_string())
        .arg("--debounce-ms")
        .arg(debounce_ms.to_string())
        .arg("--settle-ms")
        .arg(settle_ms.to_string())
        .arg("--periodic-rescan-secs")
        .arg(periodic_rescan_secs.to_string())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log));
    for (key, value) in daemon_env(paths)? {
        command.env(key, value);
    }
    detach_daemon_process(&mut command);
    let child = command.spawn()?;
    let pid = child.id();
    fs::write(&paths.daemon_pid, pid.to_string())?;
    Ok(pid)
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

#[cfg(not(unix))]
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
    if !pid_alive(pid) {
        return Ok(());
    }
    let status = ProcessCommand::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()?;
    if !status.success() {
        bail!("failed to stop daemon pid {pid}");
    }
    if wait_for_pid_exit(pid, 50, Duration::from_millis(100)) {
        return Ok(());
    }
    let status = ProcessCommand::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .status()?;
    if !status.success() {
        bail!("failed to force stop daemon pid {pid}");
    }
    if !wait_for_pid_exit(pid, 50, Duration::from_millis(100)) {
        bail!("daemon pid {pid} did not exit after stop signal");
    }
    Ok(())
}

fn wait_for_pid_exit(pid: u32, attempts: usize, delay: Duration) -> bool {
    for _ in 0..attempts {
        if !pid_alive(pid) {
            return true;
        }
        thread::sleep(delay);
    }
    !pid_alive(pid)
}

fn cleanup_daemon_runtime(paths: &Paths) {
    let _ = fs::remove_file(&paths.daemon_pid);
    let _ = fs::remove_file(paths.runtime.join("daemon.sock"));
}

#[cfg(unix)]
pub(crate) fn start_daemon_ipc(paths: &Paths) -> Result<()> {
    use std::os::unix::net::UnixListener;

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

#[cfg(not(unix))]
pub(crate) fn start_daemon_ipc(_: &Paths) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn handle_daemon_ipc(home: &Path, stream: &mut std::os::unix::net::UnixStream) -> Result<()> {
    let mut command = String::new();
    stream.read_to_string(&mut command)?;
    let paths = resolve_paths(Some(home.to_path_buf()))?;
    match command.trim() {
        "status" => {
            let stats = daemon_stats(&paths)?;
            writeln!(stream, "running pid {}", stats.pid)?;
            writeln!(stream, "ipc ok")?;
            writeln!(stream, "roots {}", stats.roots)?;
            writeln!(stream, "current {}", stats.current)?;
            writeln!(stream, "journal_events {}", stats.journal_events)?;
            writeln!(
                stream,
                "pending_journal_events {}",
                stats.pending_journal_events
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
                "majutsu_daemon_pending_journal_events {}",
                bool_metric(stats.pending_journal_events)
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
    let pending_journal_events = majutsu_db::has_pending_journal_events(&journal_records);
    let upload_stats = upload_queue_stats(paths)?;
    let restore_statuses = restore_queue_status_counts(paths)?;
    let restore_jobs = restore_statuses
        .iter()
        .filter(|(status, _)| status.as_str() != "done")
        .map(|(_, count)| *count)
        .sum::<usize>();

    Ok(DaemonStats {
        pid: std::process::id(),
        roots: roots.len(),
        current,
        journal_events: journal_records.len(),
        pending_journal_events,
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

#[cfg(unix)]
pub(crate) fn daemon_ipc_request(paths: &Paths, command: &str) -> Result<String> {
    use std::os::unix::net::UnixStream;

    let mut stream = UnixStream::connect(paths.runtime.join("daemon.sock"))?;
    stream.write_all(command.as_bytes())?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut reply = String::new();
    stream.read_to_string(&mut reply)?;
    Ok(reply.trim_end().to_string())
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

#[cfg(not(unix))]
pub(crate) fn daemon_ipc_request(_: &Paths, _: &str) -> Result<String> {
    bail!("daemon IPC is not supported on this platform")
}
