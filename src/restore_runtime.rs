use anyhow::{Result, anyhow, bail};
use majutsu_core::FileRecord;
use majutsu_restore::RestoreQueueItem;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::{Paths, RestoreDelete, RestorePlan};

pub(crate) fn write_restore_job(paths: &Paths, job: &RestoreQueueItem) -> Result<()> {
    let dir = paths.home.join("queue/restores");
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join(format!("{}.json", job.id)),
        serde_json::to_vec_pretty(job)?,
    )?;
    Ok(())
}

pub(crate) fn read_restore_job(paths: &Paths, job_id: &str) -> Result<RestoreQueueItem> {
    let path = paths
        .home
        .join("queue/restores")
        .join(format!("{job_id}.json"));
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

pub(crate) fn ensure_restore_job_resumable(job: &RestoreQueueItem) -> Result<()> {
    if let Some(message) = job.non_resumable_message() {
        bail!("{message}");
    }
    Ok(())
}

pub(crate) fn ensure_restore_job_not_blocked(job: &RestoreQueueItem) -> Result<()> {
    if let Some(message) = job.blocking_resume_message() {
        bail!("{message}");
    }
    Ok(())
}

pub(crate) fn ensure_restore_job_has_no_missing_objects(job: &RestoreQueueItem) -> Result<()> {
    if let Some(message) = job.missing_objects_message() {
        bail!("{message}");
    }
    Ok(())
}

pub(crate) fn restore_destination(plan: &RestorePlan, record: &FileRecord) -> Result<PathBuf> {
    if let Some(to) = &plan.to {
        return Ok(to.join(&record.root_id).join(&record.path));
    }
    let root = plan.root_paths.get(&record.root_id).ok_or_else(|| {
        anyhow!(
            "snapshot root is not configured locally: {}",
            record.root_id
        )
    })?;
    Ok(root.join(&record.path))
}

pub(crate) fn restore_root_base(
    to: Option<&PathBuf>,
    root_paths: &BTreeMap<String, PathBuf>,
    root_id: &str,
) -> Result<PathBuf> {
    if let Some(to) = to {
        return Ok(to.join(root_id));
    }
    root_paths
        .get(root_id)
        .cloned()
        .ok_or_else(|| anyhow!("snapshot root is not configured locally: {root_id}"))
}

pub(crate) fn restore_delete_destination(
    plan: &RestorePlan,
    delete: &RestoreDelete,
) -> Result<PathBuf> {
    if let Some(to) = &plan.to {
        return Ok(to.join(&delete.root_id).join(&delete.path));
    }
    let root = plan.root_paths.get(&delete.root_id).ok_or_else(|| {
        anyhow!(
            "snapshot root is not configured locally: {}",
            delete.root_id
        )
    })?;
    Ok(root.join(&delete.path))
}

pub(crate) fn restore_target_label(plan: &RestorePlan) -> String {
    plan.to
        .as_ref()
        .map(|to| to.display().to_string())
        .unwrap_or_else(|| "original-roots".into())
}

pub(crate) fn remove_empty_restore_parents(
    plan: &RestorePlan,
    delete: &RestoreDelete,
    path: &Path,
) -> Result<()> {
    let Some(mut current) = path.parent().map(Path::to_path_buf) else {
        return Ok(());
    };
    let stop = if let Some(to) = &plan.to {
        to.join(&delete.root_id)
    } else {
        plan.root_paths
            .get(&delete.root_id)
            .cloned()
            .unwrap_or_else(|| PathBuf::from("/"))
    };
    while current.starts_with(&stop) && current != stop {
        if fs::remove_dir(&current).is_err() {
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }
    Ok(())
}

pub(crate) fn mark_restore_job_done(paths: &Paths, job_id: &str) -> Result<()> {
    let mut job = read_restore_job(paths, job_id)?;
    job.mark_done();
    write_restore_job(paths, &job)
}

pub(crate) fn print_restore_conflicts(conflicts: &[String]) {
    println!("conflicts {}", conflicts.len());
    for conflict in conflicts.iter().take(20) {
        println!("conflict\t{conflict}");
    }
    if conflicts.len() > 20 {
        println!("conflict\t... {} more", conflicts.len() - 20);
    }
}

pub(crate) fn print_restore_deletes(plan: &RestorePlan) {
    println!("deletes {}", plan.deletes.len());
    for delete in plan.deletes.iter().take(20) {
        println!("delete\t{}\t{}", delete.root_id, delete.path);
    }
    if plan.deletes.len() > 20 {
        println!("delete\t... {} more", plan.deletes.len() - 20);
    }
}
