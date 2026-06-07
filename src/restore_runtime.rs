use anyhow::{Context, Result, anyhow, bail};
use majutsu_core::{
    FileRecord, LargeManifest, SnapshotManifest, payload_blob_ref, payload_large_ref,
};
use majutsu_pack::PackExport;
use majutsu_restore::RestoreQueueItem;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};

use crate::config::{Paths, read_config};
use crate::remote_store::open_remote;
use crate::{
    decode_object, download_local_object_from_remote, query_blobs, read_object,
    remote_object_available,
};

#[derive(Debug)]
pub(crate) struct RestorePlan {
    pub(crate) snapshot: SnapshotManifest,
    pub(crate) to: Option<PathBuf>,
    pub(crate) root_paths: BTreeMap<String, PathBuf>,
    pub(crate) files: Vec<FileRecord>,
    pub(crate) deletes: Vec<RestoreDelete>,
}

#[derive(Debug)]
pub(crate) struct RestoreDelete {
    pub(crate) root_id: String,
    pub(crate) path: String,
}

pub(crate) struct RestoreObjectStats {
    pub(crate) required_objects: usize,
    pub(crate) required_chunks: usize,
    pub(crate) local_objects: usize,
    pub(crate) remote_objects: usize,
    pub(crate) archived_objects: usize,
    pub(crate) missing_objects: usize,
    pub(crate) archive_or_missing_objects: usize,
}

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

pub(crate) fn restore_object_stats(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<RestoreObjectStats> {
    let required_objects = required_object_keys_for_plan(paths, conn, plan)?;
    let required_chunks = required_chunk_count_for_plan(paths, plan)?;
    let local_objects = required_objects
        .iter()
        .filter(|key| paths.home.join(key).exists())
        .count();
    let remote = read_config(paths)
        .ok()
        .and_then(|config| config.remote.and_then(|remote| open_remote(&remote).ok()));
    let mut remote_objects = 0usize;
    let mut archived_objects = 0usize;
    let mut missing_objects = 0usize;
    for key in &required_objects {
        if paths.home.join(key).exists() {
            continue;
        }
        let available_remote = remote
            .as_ref()
            .map(|remote| remote_object_available(remote, key))
            .transpose()?
            .unwrap_or(false);
        if available_remote {
            remote_objects += 1;
            archived_objects += 1;
        } else {
            missing_objects += 1;
        }
    }
    if let Some(remote) = remote.as_ref() {
        for key in required_objects
            .iter()
            .filter(|key| paths.home.join(key).exists())
        {
            if remote_object_available(remote, key)? {
                remote_objects += 1;
            }
        }
    }
    let archive_or_missing_objects = archived_objects + missing_objects;
    Ok(RestoreObjectStats {
        required_objects: required_objects.len(),
        required_chunks,
        local_objects,
        remote_objects,
        archived_objects,
        missing_objects,
        archive_or_missing_objects,
    })
}

pub(crate) fn required_chunk_count_for_plan(paths: &Paths, plan: &RestorePlan) -> Result<usize> {
    let mut chunks = 0usize;
    for record in &plan.files {
        if let Some((_, manifest_key, chunk_count)) = payload_large_ref(&record.payload) {
            let manifest = read_large_manifest_for_restore(paths, manifest_key)
                .with_context(|| format!("read large manifest {manifest_key}"))?;
            if manifest.chunks.len() != chunk_count {
                bail!(
                    "large manifest chunk count mismatch for {manifest_key}: payload={chunk_count} manifest={}",
                    manifest.chunks.len()
                );
            }
            chunks += manifest.chunks.len();
        }
    }
    Ok(chunks)
}

pub(crate) fn required_object_keys_for_plan(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for record in &plan.files {
        if let Some((oid, object_key)) = payload_blob_ref(&record.payload) {
            let blob = query_blobs(conn)?
                .into_iter()
                .find(|blob| blob.oid == oid)
                .ok_or_else(|| anyhow!("missing blob metadata for {oid}"))?;
            if let Some(pack_id) = blob.pack_id {
                let pack: PackExport = conn.query_row(
                    "select pack_id, pack_key, index_key, object_count, size from packs where pack_id=?1",
                    params![pack_id],
                    |row| {
                        Ok(PackExport {
                            pack_id: row.get(0)?,
                            pack_key: row.get(1)?,
                            index_key: row.get(2)?,
                            object_count: row.get(3)?,
                            size: row.get(4)?,
                        })
                    },
                )?;
                keys.push(pack.pack_key);
                keys.push(pack.index_key);
            } else {
                keys.push(object_key.to_string());
            }
        } else if let Some((_, manifest_key, chunk_count)) = payload_large_ref(&record.payload) {
            keys.push(manifest_key.to_string());
            let manifest = read_large_manifest_for_restore(paths, manifest_key)
                .with_context(|| format!("read large manifest {manifest_key}"))?;
            if manifest.chunks.len() != chunk_count {
                bail!(
                    "large manifest chunk count mismatch for {manifest_key}: payload={chunk_count} manifest={}",
                    manifest.chunks.len()
                );
            }
            for chunk in manifest.chunks {
                keys.push(chunk.object_key);
            }
        }
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

pub(crate) fn read_large_manifest_for_restore(
    paths: &Paths,
    manifest_key: &str,
) -> Result<LargeManifest> {
    match read_object(paths, manifest_key) {
        Ok(bytes) => return serde_json::from_slice(&bytes).map_err(Into::into),
        Err(local_err) => {
            let config = read_config(paths).with_context(|| {
                format!(
                    "read config after local large manifest {manifest_key} was unavailable: {local_err}"
                )
            })?;
            let Some(remote_config) = config.remote.as_ref() else {
                return Err(local_err)
                    .with_context(|| format!("read local large manifest {manifest_key}"));
            };
            let remote = open_remote(remote_config)?;
            let bytes = download_local_object_from_remote(paths, &remote, manifest_key)
                .with_context(|| format!("download large manifest {manifest_key}"))?;
            let decoded = decode_object(paths, &bytes)
                .with_context(|| format!("decode large manifest {manifest_key}"))?;
            serde_json::from_slice(&decoded).map_err(Into::into)
        }
    }
}
