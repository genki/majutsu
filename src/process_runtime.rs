use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ProcessIdentity {
    pub(crate) pid: u32,
    pub(crate) start_token: Option<String>,
    pub(crate) executable: Option<String>,
}

pub(crate) fn process_identity(pid: u32) -> ProcessIdentity {
    ProcessIdentity {
        pid,
        start_token: crate::platform_runtime::process_start_token(pid),
        executable: executable_path(pid),
    }
}

fn executable_path(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        fs::read_link(format!("/proc/{pid}/exe"))
            .ok()
            .map(|path| path.to_string_lossy().into_owned())
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, QueryFullProcessImageNameW,
        };

        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return None;
        }
        let mut buffer = [0u16; 32_768];
        let mut length = buffer.len() as u32;
        let ok = unsafe { QueryFullProcessImageNameW(handle, 0, buffer.as_mut_ptr(), &mut length) };
        unsafe { CloseHandle(handle) };
        if ok == 0 {
            return None;
        }
        return String::from_utf16(&buffer[..length as usize]).ok();
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let _ = pid;
        None
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        None
    }
}

pub(crate) fn read_process_identity(path: &Path) -> Result<Option<ProcessIdentity>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    // 途中まで書かれたidentityファイルや旧形式を理由に生存PIDを信頼しては
    // ならない。呼び出し側でプラットフォーム固有の検証へフォールバックする。
    Ok(serde_json::from_str(&text).ok())
}

pub(crate) fn write_process_identity(path: &Path, identity: &ProcessIdentity) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    crate::atomic_io::write_atomic(path, &serde_json::to_vec(identity)?)?;
    Ok(())
}

pub(crate) fn process_identity_matches(
    expected: &ProcessIdentity,
    actual: &ProcessIdentity,
) -> bool {
    if expected.pid != actual.pid {
        return false;
    }
    let mut checked = false;
    if let Some(expected_token) = expected.start_token.as_deref() {
        checked = true;
        if actual.start_token.as_deref() != Some(expected_token) {
            return false;
        }
    }
    if let Some(expected_executable) = expected.executable.as_deref() {
        checked = true;
        if actual.executable.as_deref() != Some(expected_executable) {
            return false;
        }
    }
    checked
}

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
