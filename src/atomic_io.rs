use anyhow::{Context, Result, bail};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
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
