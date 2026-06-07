use anyhow::{Result, bail};
use majutsu_restore::RestoreQueueItem;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command as ProcessCommand, Stdio};

use crate::process_runtime::{pid_alive, read_pid};
use crate::queue_runtime::{event_journal_records, upload_queue_stats};
use crate::root_state::roots;
use crate::snapshot_state::current_snapshot;
use crate::{Paths, open_db, resolve_paths};

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
    let child = ProcessCommand::new(env::current_exe()?)
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
        .stderr(Stdio::from(log))
        .spawn()?;
    let pid = child.id();
    fs::write(&paths.daemon_pid, pid.to_string())?;
    Ok(pid)
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
            let conn = open_db(&paths)?;
            let roots = roots(&conn)?;
            let mut root_statuses = BTreeMap::new();
            for root in &roots {
                *root_statuses.entry(root.status.as_str()).or_insert(0usize) += 1;
            }
            let current = current_snapshot(&conn)?.unwrap_or_else(|| "(none)".into());
            let journal_records = event_journal_records(&paths)?;
            let pending_journal_events = majutsu_db::has_pending_journal_events(&journal_records);
            let upload_stats = upload_queue_stats(&paths)?;
            let restore_jobs = restore_queue_status_counts(&paths)?;
            let active_restore_jobs = restore_jobs
                .iter()
                .filter(|(status, _)| status.as_str() != "done")
                .map(|(_, count)| *count)
                .sum::<usize>();
            let pid = std::process::id();
            writeln!(stream, "running pid {pid}")?;
            writeln!(stream, "ipc ok")?;
            writeln!(stream, "roots {}", roots.len())?;
            writeln!(stream, "current {current}")?;
            writeln!(stream, "journal_events {}", journal_records.len())?;
            writeln!(stream, "pending_journal_events {pending_journal_events}")?;
            writeln!(stream, "queued_uploads {}", upload_stats.total)?;
            writeln!(stream, "queued_uploads_retrying {}", upload_stats.retrying)?;
            writeln!(stream, "queued_uploads_delayed {}", upload_stats.delayed)?;
            writeln!(
                stream,
                "queued_upload_next_retry_after {}",
                upload_stats
                    .next_retry_after
                    .map(|retry_after| retry_after.to_rfc3339())
                    .unwrap_or_else(|| "(none)".into())
            )?;
            writeln!(stream, "queued_upload_attempts {}", upload_stats.attempts)?;
            writeln!(
                stream,
                "queued_upload_max_attempts {}",
                upload_stats.max_attempts
            )?;
            writeln!(
                stream,
                "upload_queue_backpressure {}",
                upload_stats.has_backpressure()
            )?;
            writeln!(stream, "restore_jobs {active_restore_jobs}")?;
            for (status, count) in root_statuses {
                writeln!(stream, "root_status {status} {count}")?;
            }
            for (status, count) in restore_jobs {
                writeln!(stream, "restore_status {status} {count}")?;
            }
        }
        other => {
            writeln!(stream, "error unknown command {other}")?;
        }
    }
    Ok(())
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

#[cfg(not(unix))]
pub(crate) fn daemon_ipc_request(_: &Paths, _: &str) -> Result<String> {
    bail!("daemon IPC is not supported on this platform")
}
