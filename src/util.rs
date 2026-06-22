use crate::majutsu_restore::{
    parse_db_time as restore_parse_db_time, parse_duration_ago as restore_parse_duration_ago,
    parse_restore_time_rfc3339,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path};
use uuid::Uuid;

pub(crate) fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

pub(crate) const REMOTE_HEAD_DECODE_LIMIT: usize = 8 * 1024 * 1024;
pub(crate) const REMOTE_METADATA_DECODE_LIMIT: usize = 128 * 1024 * 1024;

pub(crate) fn zstd_decode_all_limited(bytes: &[u8], limit: usize, label: &str) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::Decoder::new(bytes)
        .with_context(|| format!("open zstd stream for {label}"))?;
    let mut limited = Vec::new();
    let max = limit
        .checked_add(1)
        .ok_or_else(|| anyhow!("invalid zstd decoded-size limit for {label}"))?;
    decoder
        .by_ref()
        .take(max as u64)
        .read_to_end(&mut limited)
        .with_context(|| format!("decode zstd stream for {label}"))?;
    if limited.len() > limit {
        bail!(
            "decoded {label} exceeds limit: {} > {} bytes",
            limited.len(),
            limit
        );
    }
    Ok(limited)
}

pub(crate) fn new_id(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4())
}

pub(crate) fn path_to_slash(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

pub(crate) fn modified_secs(meta: &fs::Metadata) -> Option<i64> {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
}

pub(crate) fn parse_time(input: &str) -> Result<String> {
    parse_restore_time_rfc3339(input, Utc::now())
}

pub(crate) fn parse_db_time(input: &str) -> Result<DateTime<Utc>> {
    restore_parse_db_time(input)
}

pub(crate) fn parse_duration_ago(input: &str) -> Result<DateTime<Utc>> {
    restore_parse_duration_ago(input, Utc::now())
}

pub(crate) fn media_type_for_path(path: &Path) -> Option<String> {
    let name = path
        .file_name()
        .and_then(OsStr::to_str)?
        .to_ascii_lowercase();
    let media_type = if name.ends_with(".tar.zst") {
        "application/zstd"
    } else {
        match path
            .extension()
            .and_then(OsStr::to_str)
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref()
        {
            Some("blend") => "application/x-blender",
            Some("db") | Some("sqlite") => "application/vnd.sqlite3",
            Some("gz") => "application/gzip",
            Some("heic") => "image/heic",
            Some("iso") => "application/x-iso9660-image",
            Some("jpeg") | Some("jpg") => "image/jpeg",
            Some("json") => "application/json",
            Some("log") | Some("txt") => "text/plain",
            Some("md") => "text/markdown",
            Some("mkv") => "video/x-matroska",
            Some("mov") => "video/quicktime",
            Some("mp4") => "video/mp4",
            Some("parquet") => "application/vnd.apache.parquet",
            Some("png") => "image/png",
            Some("psd") => "image/vnd.adobe.photoshop",
            Some("qcow2") => "application/x-qcow2",
            Some("tar") => "application/x-tar",
            Some("toml") => "application/toml",
            Some("vmdk") => "application/x-vmdk",
            Some("yaml") | Some("yml") => "application/yaml",
            Some("zip") => "application/zip",
            Some("zst") => "application/zstd",
            _ => return None,
        }
    };
    Some(media_type.to_string())
}

pub(crate) fn stable_read(path: &Path, mode: &str) -> Result<Vec<u8>> {
    let attempts = if mode == "strict" { 8 } else { 3 };
    let mut last_error = None;
    for attempt in 0..attempts {
        let before = fs::metadata(path)?;
        let bytes = fs::read(path)?;
        let after = fs::metadata(path)?;
        if stable_metadata_matches(&before, &after) {
            return Ok(bytes);
        }
        last_error = Some(anyhow!("file changed while reading: {}", path.display()));
        std::thread::sleep(std::time::Duration::from_millis(25 * (attempt + 1) as u64));
    }
    Err(last_error.unwrap_or_else(|| anyhow!("file did not become stable: {}", path.display())))
}

pub(crate) fn stable_read_in_root(root: &Path, rel: &Path, mode: &str) -> Result<Vec<u8>> {
    validate_relative_components(rel)?;
    stable_read_in_root_platform(root, rel, mode)
}

#[cfg(target_os = "linux")]
pub(crate) fn stable_open_file_in_root(root: &Path, rel: &Path) -> Result<(File, fs::Metadata)> {
    validate_relative_components(rel)?;
    let file = open_regular_file_in_root_linux(root, rel)?;
    let before = file.metadata()?;
    if !before.is_file() {
        bail!(
            "snapshot path is not a regular file: {}",
            root.join(rel).display()
        );
    }
    Ok((file, before))
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn stable_open_file_in_root(root: &Path, rel: &Path) -> Result<(File, fs::Metadata)> {
    validate_relative_components(rel)?;
    let file = open_regular_file_in_root_openat(root, rel)?;
    let before = file.metadata()?;
    if !before.is_file() {
        bail!(
            "snapshot path is not a regular file: {}",
            root.join(rel).display()
        );
    }
    Ok((file, before))
}

#[cfg(not(unix))]
pub(crate) fn stable_open_file_in_root(root: &Path, rel: &Path) -> Result<(File, fs::Metadata)> {
    validate_relative_components(rel)?;
    let file = File::open(root.join(rel))
        .with_context(|| format!("open snapshot path {}", root.join(rel).display()))?;
    let before = file.metadata()?;
    if !before.is_file() {
        bail!(
            "snapshot path is not a regular file: {}",
            root.join(rel).display()
        );
    }
    Ok((file, before))
}

fn validate_relative_components(path: &Path) -> Result<()> {
    let mut has_component = false;
    for component in path.components() {
        match component {
            Component::Normal(_) => has_component = true,
            _ => bail!(
                "path must be relative and must not contain '.', '..', or prefixes: {}",
                path.display()
            ),
        }
    }
    if !has_component {
        bail!("path must not be empty");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn stable_read_in_root_platform(root: &Path, rel: &Path, mode: &str) -> Result<Vec<u8>> {
    let attempts = if mode == "strict" { 8 } else { 3 };
    let mut last_error = None;
    for attempt in 0..attempts {
        match read_in_root_openat2(root, rel) {
            Ok((bytes, before, after)) if stable_metadata_matches(&before, &after) => {
                return Ok(bytes);
            }
            Ok(_) => {
                last_error = Some(anyhow!("file changed while reading: {}", rel.display()));
            }
            Err(err) => {
                last_error = Some(err);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(25 * (attempt + 1) as u64));
    }
    Err(last_error.unwrap_or_else(|| anyhow!("file did not become stable: {}", rel.display())))
}

#[cfg(target_os = "linux")]
fn read_in_root_openat2(root: &Path, rel: &Path) -> Result<(Vec<u8>, fs::Metadata, fs::Metadata)> {
    let mut file = open_regular_file_in_root_linux(root, rel)?;
    let before = file.metadata()?;
    if !before.is_file() {
        bail!(
            "snapshot path is not a regular file: {}",
            root.join(rel).display()
        );
    }
    let mut bytes = Vec::with_capacity(before.len().min(1024 * 1024) as usize);
    file.read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    Ok((bytes, before, after))
}

#[cfg(target_os = "linux")]
fn open_regular_file_in_root_linux(root: &Path, rel: &Path) -> Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;

    let root_file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(root)
        .with_context(|| format!("open snapshot root {}", root.display()))?;
    let raw_rel = CString::new(rel.as_os_str().as_bytes())
        .with_context(|| format!("invalid snapshot path {}", rel.display()))?;
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = (libc::O_RDONLY | libc::O_CLOEXEC) as u64;
    how.mode = 0;
    how.resolve = libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS;
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root_file.as_raw_fd(),
            raw_rel.as_ptr(),
            &how,
            std::mem::size_of::<libc::open_how>(),
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("secure-open snapshot path {}", root.join(rel).display()));
    }
    Ok(unsafe { File::from_raw_fd(fd as std::os::fd::RawFd) })
}

#[cfg(all(unix, not(target_os = "linux")))]
fn stable_read_in_root_platform(root: &Path, rel: &Path, mode: &str) -> Result<Vec<u8>> {
    let attempts = if mode == "strict" { 8 } else { 3 };
    let mut last_error = None;
    for attempt in 0..attempts {
        match stable_open_file_in_root(root, rel) {
            Ok((mut file, before)) => {
                let mut bytes = Vec::with_capacity(before.len().min(1024 * 1024) as usize);
                file.read_to_end(&mut bytes)?;
                let after = file.metadata()?;
                if stable_metadata_matches(&before, &after) {
                    return Ok(bytes);
                }
                last_error = Some(anyhow!("file changed while reading: {}", rel.display()));
            }
            Err(err) => {
                last_error = Some(err);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(25 * (attempt + 1) as u64));
    }
    Err(last_error.unwrap_or_else(|| anyhow!("file did not become stable: {}", rel.display())))
}

#[cfg(not(unix))]
fn stable_read_in_root_platform(root: &Path, rel: &Path, mode: &str) -> Result<Vec<u8>> {
    stable_read(&root.join(rel), mode)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn open_regular_file_in_root_openat(root: &Path, rel: &Path) -> Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;

    let root_file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(root)
        .with_context(|| format!("open snapshot root {}", root.display()))?;
    let mut current_fd = root_file.as_raw_fd();
    let mut owned_dirs: Vec<File> = Vec::new();
    let mut components = rel.components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(part) = component else {
            bail!("invalid snapshot path {}", rel.display());
        };
        let raw = CString::new(part.as_bytes())
            .with_context(|| format!("invalid snapshot path {}", rel.display()))?;
        let is_leaf = components.peek().is_none();
        let flags = if is_leaf {
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW
        } else {
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW
        };
        let fd = unsafe { libc::openat(current_fd, raw.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| {
                format!("secure-open snapshot path {}", root.join(rel).display())
            });
        }
        let file = unsafe { File::from_raw_fd(fd) };
        if is_leaf {
            return Ok(file);
        }
        current_fd = file.as_raw_fd();
        owned_dirs.push(file);
    }
    bail!("path must not be empty")
}

pub(crate) fn stable_metadata_matches(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return false;
    }
    stable_file_id(before) == stable_file_id(after)
}

#[cfg(unix)]
fn stable_file_id(meta: &fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(meta.ino())
}

#[cfg(windows)]
fn stable_file_id(meta: &fs::Metadata) -> Option<u64> {
    use std::os::windows::fs::MetadataExt;
    Some(meta.creation_time() ^ ((meta.file_attributes() as u64) << 32))
}

#[cfg(not(any(unix, windows)))]
fn stable_file_id(_: &fs::Metadata) -> Option<u64> {
    None
}
