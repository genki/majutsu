use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use majutsu_restore::{
    parse_db_time as restore_parse_db_time, parse_duration_ago as restore_parse_duration_ago,
    parse_restore_time_rfc3339,
};
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use uuid::Uuid;

pub(crate) fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
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

#[cfg(not(unix))]
fn stable_file_id(_: &fs::Metadata) -> Option<u64> {
    None
}
