use anyhow::{Context, Result, bail};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

pub(crate) fn write_atomic(dest: &Path, bytes: &[u8]) -> Result<()> {
    write_atomic_with(dest, |file| {
        file.write_all(bytes)?;
        Ok(())
    })
}

pub(crate) fn write_atomic_with<F>(dest: &Path, write_contents: F) -> Result<()>
where
    F: FnOnce(&mut File) -> Result<()>,
{
    if fs::symlink_metadata(dest)
        .map(|meta| meta.file_type().is_dir())
        .unwrap_or(false)
    {
        bail!("restore target is a directory: {}", dest.display());
    }
    let (tmp, mut file) = create_atomic_temp(dest)?;
    let result = (|| -> Result<()> {
        write_contents(&mut file)?;
        file.sync_all()?;
        drop(file);
        crate::platform_runtime::replace_file_atomic(&tmp, dest)?;
        fsync_parent_dir(dest)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn create_atomic_temp(dest: &Path) -> Result<(PathBuf, File)> {
    let parent = dest.parent().unwrap_or_else(|| Path::new("."));
    for _ in 0..16 {
        let tmp = atomic_temp_path(dest);
        let file = match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err).with_context(|| format!("create {}", tmp.display())),
        };
        return Ok((tmp, file));
    }
    bail!(
        "failed to allocate temporary restore file in {}",
        parent.display()
    )
}

fn atomic_temp_path(dest: &Path) -> PathBuf {
    let parent = dest.parent().unwrap_or_else(|| Path::new("."));
    let file_name = dest
        .file_name()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("restore"));
    let mut tmp_name = OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(".mjtmp-");
    tmp_name.push(Uuid::new_v4().to_string());
    parent.join(tmp_name)
}

pub(crate) fn fsync_parent_dir(path: &Path) -> Result<()> {
    crate::platform_runtime::sync_parent_dir(path)
}

/// 異常終了したatomic writeが残した古い一時ファイルだけを回収する。
pub(crate) fn cleanup_stale_atomic_temps(dir: &Path, min_age: Duration) -> Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut removed = 0usize;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() || !is_atomic_temp_name(&entry.file_name()) {
            continue;
        }
        let old_enough = entry
            .metadata()?
            .modified()
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age >= min_age);
        if !old_enough {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed += 1,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| format!("remove {}", entry.path().display()));
            }
        }
    }
    Ok(removed)
}

fn is_atomic_temp_name(name: &std::ffi::OsStr) -> bool {
    let name = name.to_string_lossy();
    name.starts_with('.') && name.contains(".mjtmp-")
}

#[cfg(test)]
mod tests {
    use super::cleanup_stale_atomic_temps;
    use std::fs;
    use std::time::Duration;

    #[test]
    fn stale_atomic_temp_cleanup_is_narrowly_scoped() {
        let dir = tempfile::tempdir().unwrap();
        let stale = dir.path().join(".event.json.mjtmp-dead");
        let ordinary_hidden = dir.path().join(".keep");
        let user_file = dir.path().join("note.mjtmp-user");
        fs::write(&stale, b"stale").unwrap();
        fs::write(&ordinary_hidden, b"keep").unwrap();
        fs::write(&user_file, b"keep").unwrap();

        assert_eq!(
            cleanup_stale_atomic_temps(dir.path(), Duration::ZERO).unwrap(),
            1
        );
        assert!(!stale.exists());
        assert!(ordinary_hidden.exists());
        assert!(user_file.exists());
    }
}
