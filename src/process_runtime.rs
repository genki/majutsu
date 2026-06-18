use anyhow::{Context, Result, bail};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(crate) fn read_pid(path: &Path) -> Result<Option<u32>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(fs::read_to_string(path)?.trim().parse()?))
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
            Ok(ProcessLock { path: path.into() })
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let owner = fs::read_to_string(path)
                .ok()
                .and_then(|value| value.trim().parse::<u32>().ok());
            if let Some(pid) = owner
                && pid_alive(pid)
            {
                bail!("{name} already running with pid {pid}");
            }
            let _ = fs::remove_file(path);
            let mut file = File::create(path)?;
            writeln!(file, "{}", std::process::id())?;
            Ok(ProcessLock { path: path.into() })
        }
        Err(error) => Err(error).with_context(|| format!("acquire {name} lock")),
    }
}

pub(crate) fn process_lock_owner(path: &Path) -> Result<Option<u32>> {
    if !path.exists() {
        return Ok(None);
    }
    let owner = fs::read_to_string(path)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok());
    if let Some(pid) = owner
        && pid_alive(pid)
    {
        return Ok(Some(pid));
    }
    let _ = fs::remove_file(path);
    Ok(None)
}

pub(crate) fn pid_alive(pid: u32) -> bool {
    crate::platform_runtime::pid_alive(pid)
}
