use anyhow::{Context, Result, bail};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};

pub(crate) fn read_pid(path: &Path) -> Result<Option<u32>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    Ok(Some(text.trim().parse()?))
}

pub(crate) struct ProcessLock {
    path: PathBuf,
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) fn acquire_process_lock(path: &Path, name: &str) -> Result<ProcessLock> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => {
            writeln!(file, "{}", std::process::id())?;
            Ok(ProcessLock {
                path: path.to_path_buf(),
            })
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let owner = fs::read_to_string(path)
                .ok()
                .and_then(|value| value.trim().parse::<u32>().ok());
            if let Some(pid) = owner {
                if pid_alive(pid) {
                    bail!("{name} already running with pid {pid}");
                }
            }
            let _ = fs::remove_file(path);
            let mut file = File::create(path)?;
            writeln!(file, "{}", std::process::id())?;
            Ok(ProcessLock {
                path: path.to_path_buf(),
            })
        }
        Err(err) => Err(err).with_context(|| format!("acquire {name} lock")),
    }
}

pub(crate) fn pid_alive(pid: u32) -> bool {
    ProcessCommand::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}
