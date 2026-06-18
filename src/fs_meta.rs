use anyhow::{Context, Result};
use base64::Engine;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[cfg(windows)]
mod windows_ea;

#[cfg(unix)]
pub(crate) fn file_mode(meta: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode()
}

#[cfg(not(unix))]
pub(crate) fn file_mode(_: &fs::Metadata) -> u32 {
    0
}

#[cfg(unix)]
pub(crate) fn file_uid(meta: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    Some(meta.uid())
}

#[cfg(not(unix))]
pub(crate) fn file_uid(_: &fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
pub(crate) fn file_gid(meta: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    Some(meta.gid())
}

#[cfg(not(unix))]
pub(crate) fn file_gid(_: &fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
pub(crate) fn special_file_kind(meta: &fs::Metadata) -> Option<String> {
    use std::os::unix::fs::FileTypeExt;
    let file_type = meta.file_type();
    if file_type.is_fifo() {
        Some("fifo".into())
    } else if file_type.is_socket() {
        Some("socket".into())
    } else if file_type.is_block_device() {
        Some("block-device".into())
    } else if file_type.is_char_device() {
        Some("char-device".into())
    } else {
        None
    }
}

#[cfg(not(unix))]
pub(crate) fn special_file_kind(_: &fs::Metadata) -> Option<String> {
    None
}

#[cfg(unix)]
pub(crate) fn read_xattrs(path: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(names) = xattr::list(path) else {
        return out;
    };
    for name in names {
        let name_s = name.to_string_lossy().to_string();
        let Ok(Some(value)) = xattr::get(path, &name) else {
            continue;
        };
        out.insert(
            name_s,
            base64::engine::general_purpose::STANDARD.encode(value),
        );
    }
    out
}

#[cfg(windows)]
pub(crate) fn read_xattrs(path: &Path) -> BTreeMap<String, String> {
    let Ok(entries) = windows_ea::list(path) else {
        // FAT/exFAT, some network filesystems, and policy-restricted paths do
        // not support native EAs. Match the existing Unix best-effort policy.
        return BTreeMap::new();
    };
    entries
        .into_iter()
        .map(|(name, value)| {
            (
                name,
                base64::engine::general_purpose::STANDARD.encode(value),
            )
        })
        .collect()
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn read_xattrs(_path: &Path) -> BTreeMap<String, String> {
    BTreeMap::new()
}

#[cfg(unix)]
pub(crate) fn apply_xattrs(path: &Path, xattrs: &BTreeMap<String, String>) -> Result<()> {
    for (name, encoded) in xattrs {
        let value = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .with_context(|| format!("decode xattr {name}"))?;
        if xattr::set(path, name, &value).is_err() {
            continue;
        }
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn apply_xattrs(path: &Path, xattrs: &BTreeMap<String, String>) -> Result<()> {
    for (name, encoded) in xattrs {
        let value = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .with_context(|| format!("decode Windows EA {name}"))?;
        // Cross-platform snapshots may contain macOS/Linux names or values
        // that Windows cannot represent. Preserve restore progress and apply
        // every native-compatible EA that can be written.
        if windows_ea::set(path, name, &value).is_err() {
            continue;
        }
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn apply_xattrs(_path: &Path, xattrs: &BTreeMap<String, String>) -> Result<()> {
    // Validate the serialized values even on a platform without an EA API.
    for (name, encoded) in xattrs {
        let _ = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .with_context(|| format!("decode xattr {name}"))?;
    }
    Ok(())
}

pub(crate) fn is_mount_point(path: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        let Ok(target) = fs::canonicalize(path) else {
            return false;
        };
        let Ok(mountinfo) = fs::read_to_string("/proc/self/mountinfo") else {
            return false;
        };
        for line in mountinfo.lines() {
            let Some(before_sep) = line.split(" - ").next() else {
                continue;
            };
            let mut fields = before_sep.split_whitespace();
            let mount_point = fields.nth(4);
            if let Some(mount_point) = mount_point
                && unescape_mountinfo_path(mount_point) == target
            {
                return true;
            }
        }
        false
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        use std::os::unix::fs::MetadataExt;

        let Ok(target) = fs::canonicalize(path) else {
            return false;
        };
        let Ok(metadata) = fs::metadata(&target) else {
            return false;
        };
        let parent = target.parent().unwrap_or(&target);
        let Ok(parent_metadata) = fs::metadata(parent) else {
            return false;
        };

        // A Unix mount root either crosses a device boundary or is the root
        // directory itself (same inode as its parent). This covers macOS
        // volumes mounted below /Volumes as well as Unix root.
        metadata.dev() != parent_metadata.dev()
            || (metadata.dev() == parent_metadata.dev() && metadata.ino() == parent_metadata.ino())
    }
    #[cfg(windows)]
    {
        crate::platform_runtime::is_windows_mount_point(path)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        false
    }
}

#[cfg(target_os = "linux")]
fn unescape_mountinfo_path(input: &str) -> String {
    let mut out = String::new();
    let bytes = input.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\\'
            && i + 3 < bytes.len()
            && bytes[i + 1].is_ascii_digit()
            && bytes[i + 2].is_ascii_digit()
            && bytes[i + 3].is_ascii_digit()
            && let Ok(value) = u8::from_str_radix(&input[i + 1..i + 4], 8)
        {
            out.push(value as char);
            i += 4;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}
