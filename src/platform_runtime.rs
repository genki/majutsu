//! Cross-platform helpers used by the CLI, daemon and restore paths.

use anyhow::{Context, Result};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

#[cfg(unix)]
pub(crate) use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(windows)]
pub(crate) use windows_ipc::{UnixListener, UnixStream};

/// Populate Unix-style variables expected by existing configuration code.
/// Called before clap parsing and before any worker threads are created.
pub(crate) fn initialize_process_environment() {
    #[cfg(windows)]
    {
        let home = std::env::var_os("USERPROFILE").or_else(|| {
            let drive = std::env::var_os("HOMEDRIVE")?;
            let path = std::env::var_os("HOMEPATH")?;
            let mut joined = drive;
            joined.push(path);
            Some(joined)
        });
        unsafe {
            if std::env::var_os("HOME").is_none()
                && let Some(home) = home
            {
                std::env::set_var("HOME", home);
            }
            if std::env::var_os("XDG_CONFIG_HOME").is_none()
                && let Some(app_data) = std::env::var_os("APPDATA")
            {
                std::env::set_var("XDG_CONFIG_HOME", app_data);
            }
            if std::env::var_os("HOSTNAME").is_none()
                && let Some(name) = std::env::var_os("COMPUTERNAME")
            {
                std::env::set_var("HOSTNAME", name);
            }
        }
    }
}

pub(crate) fn system_state_home() -> PathBuf {
    #[cfg(windows)]
    {
        std::env::var_os("PROGRAMDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
            .join("Majutsu")
            .join("state")
    }
    #[cfg(not(windows))]
    {
        PathBuf::from("/var/lib/majutsu")
    }
}

pub(crate) fn system_config_path() -> PathBuf {
    #[cfg(windows)]
    {
        std::env::var_os("PROGRAMDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
            .join("Majutsu")
            .join("config.toml")
    }
    #[cfg(not(windows))]
    {
        PathBuf::from("/etc/majutsu/config.toml")
    }
}

#[cfg(windows)]
pub(crate) fn configured_system_state_home() -> PathBuf {
    let config_path = system_config_path();
    let Ok(text) = std::fs::read_to_string(config_path) else {
        return system_state_home();
    };
    let Ok(value) = text.parse::<toml::Value>() else {
        return system_state_home();
    };
    value
        .get("state")
        .and_then(|state| state.get("home"))
        .and_then(toml::Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(system_state_home)
}

pub(crate) fn shell_command(script: impl AsRef<OsStr>) -> Command {
    #[cfg(windows)]
    {
        let mut command = Command::new("cmd.exe");
        command.args(["/D", "/S", "/C"]).arg(script);
        command
    }
    #[cfg(not(windows))]
    {
        let mut command = Command::new("sh");
        command.arg("-c").arg(script);
        command
    }
}

#[allow(dead_code)]
pub(crate) fn configure_background_command(command: &mut Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::{
            CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, DETACHED_PROCESS,
        };
        command.creation_flags(
            CREATE_NEW_PROCESS_GROUP
                | CREATE_NO_WINDOW
                | DETACHED_PROCESS
                | CREATE_BREAKAWAY_FROM_JOB,
        );
    }
    #[cfg(not(windows))]
    {
        let _ = command;
    }
}

pub(crate) fn pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if rc == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{CloseHandle, ERROR_ACCESS_DENIED};
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, WaitForSingleObject,
        };
        const SYNCHRONIZE: u32 = 0x0010_0000;
        let handle =
            unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, 0, pid) };
        if handle.is_null() {
            return std::io::Error::last_os_error().raw_os_error()
                == Some(ERROR_ACCESS_DENIED as i32);
        }
        let wait = unsafe { WaitForSingleObject(handle, 0) };
        unsafe { CloseHandle(handle) };
        wait == 0x0000_0102 // WAIT_TIMEOUT
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

/// プロセスの生存中は変化しない起動トークンを返す。
///
/// トークンの形式は意図的に非公開とする。Linuxでは`/proc/<pid>/stat`の
/// 起動tick、他のUnixでは`ps`の起動時刻、Windowsではプロセス生成時刻の
/// FILETIMEを使い、PIDの再利用を検出する。
pub(crate) fn process_start_token(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let mut fields = stat.rsplit_once(')')?.1.split_whitespace();
        // commの後の最初の値がfield 3 (state)であり、starttimeはfield 22なので、
        // このsuffixではindex 19にあたる。
        fields.nth(19).map(str::to_owned)
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let output = Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "lstart="])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        (!value.is_empty()).then_some(value)
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{CloseHandle, FILETIME};
        use windows_sys::Win32::System::Threading::{
            GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };

        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return None;
        }
        let mut creation = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut exit = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut kernel = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut user = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let ok =
            unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
        unsafe { CloseHandle(handle) };
        if ok == 0 {
            return None;
        }
        let value = (u64::from(creation.dwHighDateTime) << 32) | u64::from(creation.dwLowDateTime);
        return Some(value.to_string());
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        None
    }
}

pub(crate) fn terminate_process(pid: u32, timeout: Duration) -> Result<()> {
    if !pid_alive(pid) {
        return Ok(());
    }
    #[cfg(unix)]
    {
        let native = pid as libc::pid_t;
        if unsafe { libc::kill(native, libc::SIGTERM) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error).context("send SIGTERM to daemon");
            }
            return Ok(());
        }
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if !pid_alive(pid) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        if unsafe { libc::kill(native, libc::SIGKILL) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error).context("send SIGKILL to daemon");
            }
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_TERMINATE, TerminateProcess, WaitForSingleObject,
        };
        const SYNCHRONIZE: u32 = 0x0010_0000;
        let handle = unsafe { OpenProcess(PROCESS_TERMINATE | SYNCHRONIZE, 0, pid) };
        if handle.is_null() {
            if !pid_alive(pid) {
                return Ok(());
            }
            return Err(std::io::Error::last_os_error()).context("open daemon process");
        }
        if unsafe { TerminateProcess(handle, 1) } == 0 {
            let error = std::io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(error).context("terminate daemon process");
        }
        let wait = unsafe {
            WaitForSingleObject(handle, timeout.as_millis().min(u32::MAX as u128) as u32)
        };
        unsafe { CloseHandle(handle) };
        if wait == 0 {
            return Ok(());
        }
        anyhow::bail!("timed out waiting for Windows daemon process {pid}");
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (pid, timeout);
        anyhow::bail!("process termination is unsupported on this platform")
    }
}

pub(crate) fn replace_file_atomic(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
        };
        let source_wide = source
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let destination_wide = destination
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let result = unsafe {
            MoveFileExW(
                source_wide.as_ptr(),
                destination_wide.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| {
                format!(
                    "atomically replace {} with {}",
                    destination.display(),
                    source.display()
                )
            });
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(source, destination)?;
        Ok(())
    }
}

pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        // MoveFileExW is invoked with MOVEFILE_WRITE_THROUGH on Windows.
        let _ = path;
        Ok(())
    }
}

pub(crate) fn create_symlink(target: &str, destination: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, destination)?;
        Ok(())
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::{symlink_dir, symlink_file};
        let target_path = Path::new(target);
        let resolved = if target_path.is_absolute() {
            target_path.to_path_buf()
        } else {
            destination
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(target_path)
        };
        let directory_hint = resolved.is_dir() || target.ends_with('/') || target.ends_with('\\');
        let result = if directory_hint {
            symlink_dir(target_path, destination)
        } else {
            symlink_file(target_path, destination)
        };
        result.with_context(|| {
            format!(
                "create Windows symlink {} -> {}; enable Developer Mode or grant the Create symbolic links privilege",
                destination.display(),
                target
            )
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (target, destination);
        bail!("symlink creation is unsupported on this platform")
    }
}

#[cfg(windows)]
pub(crate) fn is_windows_mount_point(path: &Path) -> bool {
    use std::ffi::OsString;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use windows_sys::Win32::Storage::FileSystem::GetVolumePathNameW;
    let absolute = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let wide = absolute
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut volume = vec![0_u16; 32_768];
    let ok = unsafe {
        GetVolumePathNameW(
            wide.as_ptr(),
            volume.as_mut_ptr(),
            u32::try_from(volume.len()).expect("volume buffer fits u32"),
        )
    };
    if ok == 0 {
        return false;
    }
    let length = volume.iter().position(|value| *value == 0).unwrap_or(0);
    let volume = PathBuf::from(OsString::from_wide(&volume[..length]));
    normalize_windows_path(&absolute) == normalize_windows_path(&volume)
}

#[cfg(windows)]
fn normalize_windows_path(path: &Path) -> String {
    let mut value = path.to_string_lossy().replace('/', "\\");
    if let Some(rest) = value.strip_prefix(r"\\?\UNC\") {
        value = format!(r"\\{rest}");
    } else if let Some(rest) = value.strip_prefix(r"\\?\") {
        value = rest.to_string();
    }
    value.trim_end_matches('\\').to_lowercase()
}

/// Windows fallback for the Unix-socket-shaped daemon API. The endpoint file
/// contains a loopback address and a random bearer token. The token is consumed
/// by the wrapper before the existing daemon request protocol sees the stream.
#[cfg(windows)]
mod windows_ipc {
    use std::fs;
    use std::io::{self, Read, Write};
    use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use uuid::Uuid;

    const VERSION: &str = "majutsu-loopback-v1";

    pub(crate) struct UnixListener {
        inner: TcpListener,
        endpoint: PathBuf,
        token: String,
    }

    impl UnixListener {
        pub(crate) fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
            let path = path.as_ref();
            if path.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("daemon endpoint exists: {}", path.display()),
                ));
            }
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let inner = TcpListener::bind(("127.0.0.1", 0))?;
            let token = Uuid::new_v4().simple().to_string();
            let content = format!("{VERSION}\n{}\n{token}\n", inner.local_addr()?);
            let temporary = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
            fs::write(&temporary, content)?;
            if let Err(error) = fs::rename(&temporary, path) {
                let _ = fs::remove_file(&temporary);
                return Err(error);
            }
            Ok(Self {
                inner,
                endpoint: path.to_path_buf(),
                token,
            })
        }

        pub(crate) fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
            loop {
                let (mut stream, address) = self.inner.accept()?;
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                let presented = read_line(&mut stream, 256)?;
                stream.set_read_timeout(None)?;
                if constant_time_equal(presented.as_bytes(), self.token.as_bytes()) {
                    return Ok((UnixStream { inner: stream }, address));
                }
                let _ = stream.shutdown(Shutdown::Both);
            }
        }

        pub(crate) fn incoming(&self) -> Incoming<'_> {
            Incoming { listener: self }
        }

        #[allow(dead_code)]
        pub(crate) fn set_nonblocking(&self, value: bool) -> io::Result<()> {
            self.inner.set_nonblocking(value)
        }
    }

    impl Drop for UnixListener {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.endpoint);
        }
    }

    pub(crate) struct Incoming<'a> {
        listener: &'a UnixListener,
    }

    impl Iterator for Incoming<'_> {
        type Item = io::Result<UnixStream>;
        fn next(&mut self) -> Option<Self::Item> {
            Some(self.listener.accept().map(|(stream, _)| stream))
        }
    }

    pub(crate) struct UnixStream {
        inner: TcpStream,
    }

    impl UnixStream {
        pub(crate) fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
            let endpoint = fs::read_to_string(path)?;
            let mut lines = endpoint.lines();
            if lines.next() != Some(VERSION) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported daemon endpoint",
                ));
            }
            let address = lines.next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing daemon address")
            })?;
            let token = lines.next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing daemon token")
            })?;
            let mut inner = TcpStream::connect(address)?;
            inner.write_all(token.as_bytes())?;
            inner.write_all(b"\n")?;
            inner.flush()?;
            Ok(Self { inner })
        }

        #[allow(dead_code)]
        pub(crate) fn try_clone(&self) -> io::Result<Self> {
            self.inner.try_clone().map(|inner| Self { inner })
        }
        #[allow(dead_code)]
        pub(crate) fn set_read_timeout(&self, value: Option<Duration>) -> io::Result<()> {
            self.inner.set_read_timeout(value)
        }
        #[allow(dead_code)]
        pub(crate) fn set_write_timeout(&self, value: Option<Duration>) -> io::Result<()> {
            self.inner.set_write_timeout(value)
        }
        #[allow(dead_code)]
        pub(crate) fn set_nonblocking(&self, value: bool) -> io::Result<()> {
            self.inner.set_nonblocking(value)
        }
        pub(crate) fn shutdown(&self, how: Shutdown) -> io::Result<()> {
            self.inner.shutdown(how)
        }
    }

    impl Read for UnixStream {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.inner.read(buffer)
        }
    }
    impl Write for UnixStream {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.inner.write(buffer)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }

    fn read_line(stream: &mut TcpStream, limit: usize) -> io::Result<String> {
        let mut bytes = Vec::new();
        for _ in 0..limit {
            let mut byte = [0_u8; 1];
            stream.read_exact(&mut byte)?;
            if byte[0] == b'\n' {
                return String::from_utf8(bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
            }
            bytes.push(byte[0]);
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "daemon IPC handshake is too long",
        ))
    }

    fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
                == 0
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::thread;

        #[test]
        fn loopback_round_trip() {
            let temp = tempfile::tempdir().unwrap();
            let endpoint = temp.path().join("daemon.endpoint");
            let listener = UnixListener::bind(&endpoint).unwrap();
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = String::new();
                stream.read_to_string(&mut request).unwrap();
                assert_eq!(request, "status");
                stream.write_all(b"ok").unwrap();
            });
            let mut stream = UnixStream::connect(&endpoint).unwrap();
            stream.write_all(b"status").unwrap();
            stream.shutdown(Shutdown::Write).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            assert_eq!(response, "ok");
            server.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_process_is_alive() {
        assert!(pid_alive(std::process::id()));
    }
}
