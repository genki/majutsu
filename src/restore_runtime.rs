use anyhow::{Result, bail};
use majutsu_restore::RestoreQueueItem;
use std::fs;

use crate::{Paths, RestorePlan};

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
