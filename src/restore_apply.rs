use crate::majutsu_core::FileRecord;
use anyhow::{Context, Result, bail};
use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::fs_meta::{apply_xattrs, special_file_kind};

pub(crate) fn restore_symlink(dest: &Path, target: &str, force: bool) -> Result<()> {
    if let Ok(meta) = fs::symlink_metadata(dest) {
        if !force {
            bail!("symlink restore target exists: {}", dest.display());
        }
        if meta.file_type().is_dir() {
            bail!("symlink restore target is a directory: {}", dest.display());
        }
        fs::remove_file(dest)?;
    }
    crate::platform_runtime::create_symlink(target, dest)
}

pub(crate) fn validate_restore_relative_path(path: &str) -> Result<()> {
    let rel = Path::new(path);
    let mut has_component = false;
    for component in rel.components() {
        match component {
            Component::Normal(_) => has_component = true,
            _ => bail!("restore path must stay inside its root: {path}"),
        }
    }
    if !has_component {
        bail!("restore path must not be empty");
    }
    Ok(())
}

pub(crate) fn ensure_restore_parent_beneath(base: &Path, dest: &Path) -> Result<()> {
    let Some(parent) = dest.parent() else {
        return Ok(());
    };
    ensure_directory_path_without_symlinks(base)?;
    if parent == base {
        return Ok(());
    }
    let rel = parent
        .strip_prefix(base)
        .with_context(|| format!("restore destination escapes base {}", base.display()))?;
    let mut current = base.to_path_buf();
    for component in rel.components() {
        let Component::Normal(part) = component else {
            bail!("restore parent escapes base: {}", parent.display());
        };
        current.push(part);
        ensure_directory_path_leaf_without_symlinks(&current)?;
    }
    Ok(())
}

pub(crate) fn ensure_restore_target_not_symlink(dest: &Path) -> Result<()> {
    if fs::symlink_metadata(dest)
        .map(|meta| meta.file_type().is_symlink())
        .unwrap_or(false)
    {
        bail!("restore target is a symlink: {}", dest.display());
    }
    Ok(())
}

fn ensure_directory_path_without_symlinks(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if matches!(component, Component::RootDir | Component::Prefix(_)) {
            continue;
        }
        ensure_directory_path_leaf_without_symlinks(&current)?;
    }
    Ok(())
}

fn ensure_directory_path_leaf_without_symlinks(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                bail!("restore path component is a symlink: {}", path.display());
            }
            if !meta.is_dir() {
                bail!(
                    "restore path component is not a directory: {}",
                    path.display()
                );
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path)
                .with_context(|| format!("create restore directory {}", path.display()))?;
            let meta = fs::symlink_metadata(path)
                .with_context(|| format!("inspect created restore directory {}", path.display()))?;
            if meta.file_type().is_symlink() || !meta.is_dir() {
                bail!(
                    "created restore path is not a directory: {}",
                    path.display()
                );
            }
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("inspect restore directory {}", path.display()));
        }
    }
    Ok(())
}

pub(crate) fn prepare_file_restore_destination(dest: &Path, force: bool) -> Result<()> {
    if fs::symlink_metadata(dest)
        .map(|meta| meta.file_type().is_dir())
        .unwrap_or(false)
    {
        if !force {
            bail!("restore target is a directory: {}", dest.display());
        }
        fs::remove_dir(dest)
            .with_context(|| format!("remove empty restore target directory {}", dest.display()))?;
    }
    Ok(())
}

pub(crate) fn prepare_directory_restore_destination(dest: &Path, force: bool) -> Result<()> {
    let Ok(meta) = fs::symlink_metadata(dest) else {
        return Ok(());
    };
    if meta.file_type().is_dir() {
        return Ok(());
    }
    if !force {
        bail!("directory restore target exists: {}", dest.display());
    }
    fs::remove_file(dest)?;
    Ok(())
}

pub(crate) fn apply_file_metadata(dest: &Path, record: &FileRecord) -> Result<()> {
    apply_xattrs(dest, &record.xattrs)?;
    apply_file_owner(dest, record)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if record.mode != 0 {
            fs::set_permissions(dest, fs::Permissions::from_mode(record.mode & 0o7777))?;
        }
    }
    if let Some(seconds) = record.modified {
        set_path_mtime(dest, seconds)?;
    }
    Ok(())
}

#[cfg(unix)]
fn apply_file_owner(dest: &Path, record: &FileRecord) -> Result<()> {
    let Some(uid) = record.uid else {
        return Ok(());
    };
    let Some(gid) = record.gid else {
        return Ok(());
    };
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let raw_path = CString::new(dest.as_os_str().as_bytes())
        .with_context(|| format!("invalid owner path {}", dest.display()))?;
    let rc = unsafe {
        libc::fchownat(
            libc::AT_FDCWD,
            raw_path.as_ptr(),
            uid as libc::uid_t,
            gid as libc::gid_t,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if matches!(
            err.kind(),
            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::Unsupported
        ) {
            return Ok(());
        }
        return Err(err).with_context(|| format!("set owner {}", dest.display()));
    }
    Ok(())
}

#[cfg(not(unix))]
fn apply_file_owner(_dest: &Path, _record: &FileRecord) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_path_mtime(path: &Path, seconds: i64) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let raw_path = CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("invalid mtime path {}", path.display()))?;
    let times = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
        libc::timespec {
            tv_sec: seconds as libc::time_t,
            tv_nsec: 0,
        },
    ];
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, raw_path.as_ptr(), times.as_ptr(), 0) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("set mtime {}", path.display()));
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_path_mtime(path: &Path, seconds: i64) -> Result<()> {
    filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(seconds, 0))?;
    Ok(())
}

#[cfg(unix)]
pub(crate) fn restore_special_file(
    dest: &Path,
    record: &FileRecord,
    special_kind: &str,
    force: bool,
) -> Result<()> {
    if special_kind != "fifo" {
        bail!(
            "restore of special file kind {special_kind} is not supported: {}",
            dest.display()
        );
    }
    if let Ok(meta) = fs::symlink_metadata(dest) {
        if restore_special_matches(&meta, special_kind)? {
            apply_file_metadata(dest, record)?;
            return Ok(());
        }
        if force {
            fs::remove_file(dest)?;
        } else {
            bail!("special file restore target exists: {}", dest.display());
        }
    }
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let raw_path = CString::new(dest.as_os_str().as_bytes())
        .with_context(|| format!("invalid fifo path {}", dest.display()))?;
    let mode = if record.mode == 0 {
        0o666
    } else {
        record.mode & 0o7777
    };
    let rc = unsafe { libc::mkfifo(raw_path.as_ptr(), mode as libc::mode_t) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("create fifo {}", dest.display()));
    }
    apply_file_metadata(dest, record)
}

#[cfg(not(unix))]
pub(crate) fn restore_special_file(
    dest: &Path,
    _record: &FileRecord,
    special_kind: &str,
    _force: bool,
) -> Result<()> {
    bail!(
        "restore of special file kind {special_kind} is not supported on this platform: {}",
        dest.display()
    )
}

pub(crate) fn restore_special_matches(meta: &fs::Metadata, special_kind: &str) -> Result<bool> {
    Ok(special_file_kind(meta).as_deref() == Some(special_kind))
}
