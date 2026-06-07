use anyhow::{Result, bail};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command as ProcessCommand, Stdio};

use crate::process_runtime::{pid_alive, read_pid};
use crate::snapshot_state::current_snapshot;
use crate::{Paths, open_db, resolve_paths, roots};

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
            let roots = roots(&conn)?.len();
            let current = current_snapshot(&conn)?.unwrap_or_else(|| "(none)".into());
            let pid = std::process::id();
            writeln!(stream, "running pid {pid}")?;
            writeln!(stream, "ipc ok")?;
            writeln!(stream, "roots {roots}")?;
            writeln!(stream, "current {current}")?;
        }
        other => {
            writeln!(stream, "error unknown command {other}")?;
        }
    }
    Ok(())
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
