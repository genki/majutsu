use crate::majutsu_core::{
    FileRecord, LargeManifest, Payload, SnapshotManifest, payload_blob_ref, payload_large_ref,
};
use crate::majutsu_pack::PackExport;
use crate::majutsu_restore::{
    RestoreChangeStats, RestorePathState, RestoreQueueItem, count_restore_changes,
};
use anyhow::{Context, Result, anyhow, bail};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use rusqlite::{Connection, params};

use crate::atomic_io::write_atomic;
use crate::cli::{RestoreArgs, RestoreCommand, RestoreTopArgs};
use crate::config::{Paths, read_config};
use crate::operation_log::record_op;
use crate::remote_store::open_remote;
use crate::restore_apply::{
    apply_file_metadata, prepare_directory_restore_destination, prepare_file_restore_destination,
    restore_special_file, restore_special_matches, restore_symlink,
};
use crate::util::blake3_hex;
use crate::{HydrateStats, pack_entry_payload};
use crate::{
    build_restore_job, decode_object, download_local_object_from_remote,
    hydrate_local_objects_from_remote, hydrate_restore_job_objects, query_blobs, read_blob_payload,
    read_large_chunk, read_object, remote_available_key, remote_object_available,
    request_archive_restore_for_job, write_large_chunks_atomic,
};

#[cfg(unix)]
use std::ffi::OsStr;

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
    pub(crate) local_chunks: usize,
    pub(crate) remote_chunks: usize,
    pub(crate) archived_chunks: usize,
    pub(crate) missing_chunks: usize,
    pub(crate) local_objects: usize,
    pub(crate) remote_objects: usize,
    pub(crate) archived_objects: usize,
    pub(crate) missing_objects: usize,
    pub(crate) archive_or_missing_objects: usize,
}

pub(crate) fn restore_cmd(paths: &Paths, top_args: RestoreTopArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    let command = top_args
        .command
        .unwrap_or(RestoreCommand::Apply(top_args.args));
    match command {
        RestoreCommand::Plan(args) => {
            let plan = crate::build_restore_plan(paths, &conn, &args)?;
            print_restore_plan(paths, &conn, &plan)?;
            if args.check_conflicts {
                let conflicts = restore_conflicts(paths, &conn, &plan)?;
                print_restore_conflicts(&conflicts);
            }
        }
        RestoreCommand::Apply(args) => {
            let trace = RestoreTrace::new();
            let plan = crate::build_restore_plan(paths, &conn, &args)?;
            trace.mark("build plan");
            apply_restore_plan(paths, &plan, args.force, args.check_conflicts)?;
            trace.mark("apply plan");
            let after = plan.snapshot.snapshot_id.as_str();
            record_op(
                &conn,
                "restore",
                None,
                Some(after),
                Some(&format!("to {}", restore_target_label(&plan))),
            )?;
            trace.mark("record operation");
            print_restore_plan_with_stats_mode(paths, &conn, &plan, RestoreStatsMode::LocalOnly)?;
            trace.mark("print plan");
            println!("restored to {}", restore_target_label(&plan));
            trace.mark("finish");
        }
        RestoreCommand::Prepare(args) => {
            let plan = crate::build_restore_plan(paths, &conn, &args)?;
            let stats = restore_object_stats(paths, &conn, &plan)?;
            let mut job = build_restore_job(paths, &plan, &args)?;
            request_archive_restore_for_job(paths, &mut job)?;
            write_restore_job(paths, &job)?;
            record_op(
                &conn,
                "restore-prepare",
                None,
                Some(&plan.snapshot.snapshot_id),
                Some(&job.id),
            )?;
            println!("restore_job {}", job.id);
            println!("snapshot {}", job.snapshot_id);
            println!("required_objects {}", job.required_objects.len());
            println!("required_chunks {}", stats.required_chunks);
            println!("local_chunks {}", stats.local_chunks);
            println!("remote_chunks {}", stats.remote_chunks);
            println!("archived_chunks {}", stats.archived_chunks);
            println!("missing_chunks {}", stats.missing_chunks);
            println!("archived_objects {}", job.archived_objects.len());
            println!("missing_objects {}", job.missing_objects.len());
            println!(
                "archive_requested_objects {}",
                job.archive_requested_objects.len()
            );
        }
        RestoreCommand::Resume { job_id } => {
            let mut job = read_restore_job(paths, &job_id)?;
            ensure_restore_job_resumable(&job)?;
            ensure_restore_job_has_no_missing_objects(&job)?;
            hydrate_restore_job_snapshot_manifest(paths, &job)?;
            hydrate_restore_job_objects(paths, &mut job)?;
            ensure_restore_job_not_blocked(&job)?;
            let args = RestoreArgs {
                snapshot: Some(job.snapshot_id.clone()),
                at: None,
                root: job.root.clone(),
                path: job.path.as_ref().map(PathBuf::from),
                to: if job.target == "original-roots" {
                    None
                } else {
                    Some(PathBuf::from(&job.target))
                },
                force: job.force,
                check_conflicts: job.check_conflicts,
            };
            let plan = crate::build_restore_plan(paths, &conn, &args)?;
            apply_restore_plan(paths, &plan, job.force, job.check_conflicts)?;
            mark_restore_job_done(paths, &job.id)?;
            record_op(
                &conn,
                "restore-resume",
                None,
                Some(&plan.snapshot.snapshot_id),
                Some(&job.id),
            )?;
            println!("resumed {}", job.id);
            println!("restored to {}", restore_target_label(&plan));
        }
    }
    Ok(())
}

struct RestoreTrace {
    enabled: bool,
    start: Instant,
}

impl RestoreTrace {
    fn new() -> Self {
        Self {
            enabled: env::var("MAJUTSU_TRACE_RESTORE")
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
                .unwrap_or(false),
            start: Instant::now(),
        }
    }

    fn mark(&self, label: &str) {
        if self.enabled {
            eprintln!(
                "restore_trace elapsed_ms={} stage={label}",
                self.start.elapsed().as_millis()
            );
        }
    }

    fn mark_stats(&self, label: &str, stats: HydrateStats) {
        if self.enabled {
            eprintln!(
                "restore_trace elapsed_ms={} stage={label} hydrated={} downloaded_bytes={} download_ms_sum={} write_ms_sum={}",
                self.start.elapsed().as_millis(),
                stats.hydrated,
                stats.downloaded_bytes,
                stats.download_ms_sum,
                stats.write_ms_sum
            );
        }
    }
}

fn hydrate_restore_job_snapshot_manifest(paths: &Paths, job: &RestoreQueueItem) -> Result<()> {
    let conn = crate::open_db(paths)?;
    let manifest_key: String = conn.query_row(
        "select manifest_key from snapshots where id=?1",
        params![job.snapshot_id],
        |row| row.get(0),
    )?;
    let dest = paths.home.join(&manifest_key);
    if dest.exists() {
        return Ok(());
    }
    let config = read_config(paths)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(());
    };
    let remote = open_remote(remote_config)?;
    if !remote_object_available(&remote, &manifest_key)? {
        return Ok(());
    }
    let bytes = download_local_object_from_remote(paths, &remote, &manifest_key)?;
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&dest, bytes).with_context(|| format!("hydrate snapshot manifest {manifest_key}"))?;
    Ok(())
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

pub(crate) fn print_restore_plan(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<()> {
    print_restore_plan_with_stats_mode(paths, conn, plan, RestoreStatsMode::Full)
}

#[derive(Debug, Clone, Copy)]
enum RestoreStatsMode {
    Full,
    LocalOnly,
}

fn print_restore_plan_with_stats_mode(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
    stats_mode: RestoreStatsMode,
) -> Result<()> {
    let large = plan
        .files
        .iter()
        .filter(|r| payload_large_ref(&r.payload).is_some())
        .count();
    let bytes: u64 = plan.files.iter().map(|r| r.size).sum();
    let changes = restore_change_stats(paths, conn, plan)?;
    println!("snapshot {}", plan.snapshot.snapshot_id);
    if let Some(to) = &plan.to {
        println!("target {}", to.display());
    } else {
        println!("target original-roots");
    }
    println!(
        "restore {} files, {} bytes, {} large files",
        plan.files.len(),
        bytes,
        large
    );
    println!("delete {} files", plan.deletes.len());
    println!("restore_files {}", changes.restore_files);
    println!("modify_files {}", changes.modify_files);
    println!("keep_files {}", changes.keep_files);
    println!("delete_files {}", changes.delete_files);
    let stats = restore_object_stats_with_mode(paths, conn, plan, stats_mode)?;
    println!("large_files {large}");
    println!("required_objects {}", stats.required_objects);
    println!("required_chunks {}", stats.required_chunks);
    println!("local_chunks {}", stats.local_chunks);
    println!("remote_chunks {}", stats.remote_chunks);
    println!("archived_chunks {}", stats.archived_chunks);
    println!("missing_chunks {}", stats.missing_chunks);
    println!("local_objects {}", stats.local_objects);
    println!("remote_objects {}", stats.remote_objects);
    println!("archived_objects {}", stats.archived_objects);
    println!("missing_objects {}", stats.missing_objects);
    println!(
        "archive_or_missing_objects {}",
        stats.archive_or_missing_objects
    );
    if matches!(stats_mode, RestoreStatsMode::LocalOnly) {
        println!("remote_stats skipped");
    }
    Ok(())
}

fn restore_change_stats(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<RestoreChangeStats> {
    count_restore_changes(&plan.files, plan.deletes.len(), |record| {
        let dest = restore_destination(plan, record)?;
        if !dest.try_exists()? {
            Ok(RestorePathState::Missing)
        } else if restore_record_matches_path(paths, conn, record, &dest).unwrap_or(false) {
            Ok(RestorePathState::Matches)
        } else {
            Ok(RestorePathState::Differs)
        }
    })
}

pub(crate) fn restore_object_stats(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<RestoreObjectStats> {
    restore_object_stats_with_mode(paths, conn, plan, RestoreStatsMode::Full)
}

fn restore_object_stats_with_mode(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
    stats_mode: RestoreStatsMode,
) -> Result<RestoreObjectStats> {
    let required_objects = required_object_keys_for_plan(paths, conn, plan)?;
    let required_chunk_keys = required_large_chunk_keys_for_plan(paths, plan)?;
    let required_chunks = required_chunk_keys.len();
    let local_objects = required_objects
        .iter()
        .filter(|key| paths.home.join(key).exists())
        .count();
    if matches!(stats_mode, RestoreStatsMode::LocalOnly) {
        let local_chunks = required_chunk_keys
            .iter()
            .filter(|key| paths.home.join(key).exists())
            .count();
        let missing_objects = required_objects.len().saturating_sub(local_objects);
        let missing_chunks = required_chunk_keys.len().saturating_sub(local_chunks);
        return Ok(RestoreObjectStats {
            required_objects: required_objects.len(),
            required_chunks,
            local_chunks,
            remote_chunks: 0,
            archived_chunks: 0,
            missing_chunks,
            local_objects,
            remote_objects: 0,
            archived_objects: 0,
            missing_objects,
            archive_or_missing_objects: missing_objects,
        });
    }
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
    let mut local_chunks = 0usize;
    let mut remote_chunks = 0usize;
    let mut archived_chunks = 0usize;
    let mut missing_chunks = 0usize;
    for key in &required_chunk_keys {
        if paths.home.join(key).exists() {
            local_chunks += 1;
            if let Some(remote) = remote.as_ref()
                && remote_object_available(remote, key)?
            {
                remote_chunks += 1;
            }
            continue;
        }
        let available_remote = remote
            .as_ref()
            .map(|remote| remote_object_available(remote, key))
            .transpose()?
            .unwrap_or(false);
        if available_remote {
            remote_chunks += 1;
            archived_chunks += 1;
        } else {
            missing_chunks += 1;
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
        local_chunks,
        remote_chunks,
        archived_chunks,
        missing_chunks,
        local_objects,
        remote_objects,
        archived_objects,
        missing_objects,
        archive_or_missing_objects,
    })
}

fn required_large_chunk_keys_for_plan(paths: &Paths, plan: &RestorePlan) -> Result<Vec<String>> {
    let mut keys = Vec::new();
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
            keys.extend(manifest.chunks.into_iter().map(|chunk| chunk.object_key));
        }
    }
    Ok(keys)
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
    match fs::read(paths.home.join(manifest_key))
        .and_then(|bytes| decode_object(paths, &bytes).map_err(std::io::Error::other))
    {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(Into::into),
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

pub(crate) fn apply_restore_plan(
    paths: &Paths,
    plan: &RestorePlan,
    force: bool,
    check_conflicts: bool,
) -> Result<()> {
    let trace = RestoreTrace::new();
    let conn = crate::open_db(paths)?;
    let packed_stats = prefetch_packed_blob_ranges_for_plan(paths, &conn, plan)?;
    trace.mark_stats("prefetch packed blob ranges", packed_stats);
    let required_objects = required_object_keys_for_plan(paths, &conn, plan)?
        .into_iter()
        .filter(|key| restore_apply_prefetches_object(key))
        .collect();
    let object_stats = hydrate_local_objects_from_remote(paths, required_objects)?;
    trace.mark_stats("prefetch objects", object_stats);
    if check_conflicts && !force {
        let conflicts = restore_conflicts(paths, &conn, plan)?;
        if !conflicts.is_empty() {
            print_restore_conflicts(&conflicts);
            bail!("restore has conflicts; rerun with --force to overwrite");
        }
        if !plan.deletes.is_empty() {
            print_restore_deletes(plan);
            bail!("restore would delete extra files; rerun with --force to delete them");
        }
    }
    trace.mark("check conflicts");
    for delete in &plan.deletes {
        let dest = restore_delete_destination(plan, delete)?;
        if fs::symlink_metadata(&dest).is_ok() {
            fs::remove_file(&dest)?;
            remove_empty_restore_parents(plan, delete, &dest)?;
        }
    }
    trace.mark("delete files");
    let mut directory_metadata = Vec::new();
    for record in &plan.files {
        let dest = restore_destination(plan, record)?;
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        match &record.payload {
            Payload::Directory => {
                prepare_directory_restore_destination(&dest, force)?;
                fs::create_dir_all(&dest)?;
                directory_metadata.push((dest, record));
            }
            Payload::Special { special_kind } => {
                restore_special_file(&dest, record, special_kind, force)?;
            }
            Payload::Symlink { target } => {
                restore_symlink(&dest, target, force)?;
            }
            payload => {
                if let Some((oid, object_key)) = payload_blob_ref(payload) {
                    prepare_file_restore_destination(&dest, force)?;
                    write_atomic(&dest, &read_blob_payload(paths, &conn, oid, object_key)?)?;
                    apply_file_metadata(&dest, record)?;
                } else if let Some((_, manifest_key, _)) = payload_large_ref(payload) {
                    prepare_file_restore_destination(&dest, force)?;
                    let manifest: LargeManifest =
                        serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                    write_large_chunks_atomic(paths, &dest, &manifest)?;
                    apply_file_metadata(&dest, record)?;
                }
            }
        }
    }
    trace.mark("restore files");
    for (dest, record) in directory_metadata {
        apply_file_metadata(&dest, record)?;
    }
    trace.mark("directory metadata");
    Ok(())
}

fn restore_apply_prefetches_object(key: &str) -> bool {
    !key.starts_with("objects/packs/") && !key.starts_with("objects/indexes/pack/")
}

#[derive(Debug, Clone)]
struct PackedBlobRangePrefetch {
    oid: String,
    object_key: String,
    pack_key: String,
    remote_pack_key: String,
    offset: u64,
    len: u64,
    size: u64,
}

fn prefetch_packed_blob_ranges_for_plan(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<HydrateStats> {
    let blobs = query_blobs(conn)?;
    let blobs_by_oid = blobs
        .iter()
        .map(|blob| (blob.oid.as_str(), blob))
        .collect::<HashMap<_, _>>();
    let mut pack_cache = HashMap::<String, PackExport>::new();
    let mut tasks = Vec::new();
    for record in &plan.files {
        let Some((oid, _)) = payload_blob_ref(&record.payload) else {
            continue;
        };
        let blob = blobs_by_oid
            .get(oid)
            .copied()
            .ok_or_else(|| anyhow!("missing blob metadata for {oid}"))?;
        let Some(pack_id) = blob.pack_id.as_deref() else {
            continue;
        };
        if paths.home.join(&blob.object_key).exists() {
            continue;
        }
        let pack = if let Some(pack) = pack_cache.get(pack_id) {
            pack.clone()
        } else {
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
            pack_cache.insert(pack_id.to_string(), pack.clone());
            pack
        };
        if paths.home.join(&pack.pack_key).exists() {
            continue;
        }
        let offset = blob
            .pack_offset
            .ok_or_else(|| anyhow!("missing pack offset for {}", blob.oid))?;
        let len = blob
            .pack_len
            .ok_or_else(|| anyhow!("missing pack len for {}", blob.oid))?;
        tasks.push(PackedBlobRangePrefetch {
            oid: blob.oid.clone(),
            object_key: blob.object_key.clone(),
            pack_key: pack.pack_key,
            remote_pack_key: String::new(),
            offset,
            len,
            size: blob.size,
        });
    }
    tasks.sort_by(|left, right| left.object_key.cmp(&right.object_key));
    tasks.dedup_by(|left, right| left.object_key == right.object_key);
    if tasks.is_empty() {
        return Ok(HydrateStats::default());
    }
    let Ok(config) = read_config(paths) else {
        return Ok(HydrateStats::default());
    };
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(HydrateStats::default());
    };
    let remote = open_remote(remote_config)?;
    let mut remote_pack_keys = HashMap::<String, String>::new();
    for task in &mut tasks {
        let remote_pack_key = if let Some(key) = remote_pack_keys.get(&task.pack_key) {
            key.clone()
        } else {
            let key = remote_available_key(&remote, &task.pack_key)?;
            remote_pack_keys.insert(task.pack_key.clone(), key.clone());
            key
        };
        task.remote_pack_key = remote_pack_key;
    }
    prefetch_packed_blob_ranges_parallel(paths, remote, tasks)
}

fn prefetch_packed_blob_ranges_parallel(
    paths: &Paths,
    remote: crate::remote_store::RemoteStore,
    tasks: Vec<PackedBlobRangePrefetch>,
) -> Result<HydrateStats> {
    let workers = restore_packed_range_parallelism().min(tasks.len());
    let paths = Arc::new(paths.clone());
    let remote = Arc::new(remote);
    let tasks = Arc::new(Mutex::new(tasks.into_iter()));
    let (result_tx, result_rx) = mpsc::channel::<Result<HydrateStats>>();
    let mut handles = Vec::new();
    for _ in 0..workers {
        let paths = Arc::clone(&paths);
        let remote = Arc::clone(&remote);
        let tasks = Arc::clone(&tasks);
        let result_tx = result_tx.clone();
        handles.push(std::thread::spawn(move || {
            let mut stats = HydrateStats::default();
            loop {
                let task = {
                    let mut tasks = match tasks.lock() {
                        Ok(tasks) => tasks,
                        Err(err) => {
                            let _ = result_tx
                                .send(Err(anyhow!("restore packed range queue poisoned: {err}")));
                            return;
                        }
                    };
                    tasks.next()
                };
                let Some(task) = task else {
                    break;
                };
                match prefetch_one_packed_blob_range(&paths, &remote, &task) {
                    Ok(item) => stats.add(item),
                    Err(err) => {
                        let _ = result_tx.send(Err(err));
                        return;
                    }
                }
            }
            let _ = result_tx.send(Ok(stats));
        }));
    }
    drop(result_tx);
    let mut stats = HydrateStats::default();
    let mut first_error = None;
    for result in result_rx {
        match result {
            Ok(item) => stats.add(item),
            Err(err) => {
                first_error.get_or_insert(err);
            }
        }
    }
    for handle in handles {
        if let Err(err) = handle.join() {
            first_error
                .get_or_insert_with(|| anyhow!("restore packed range worker panicked: {err:?}"));
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(stats)
}

fn prefetch_one_packed_blob_range(
    paths: &Paths,
    remote: &crate::remote_store::RemoteStore,
    task: &PackedBlobRangePrefetch,
) -> Result<HydrateStats> {
    let dest = paths.home.join(&task.object_key);
    if dest.exists() {
        return Ok(HydrateStats::default());
    }
    let download_started = Instant::now();
    let entry = remote
        .get_range(&task.remote_pack_key, task.offset, task.len)
        .with_context(|| format!("download packed blob {} from {}", task.oid, task.pack_key))?;
    let encoded = pack_entry_payload(&task.oid, &entry)?;
    let decoded = decode_object(paths, encoded)
        .with_context(|| format!("decode packed blob {}", task.oid))?;
    if decoded.len() as u64 != task.size || blake3_hex(&decoded) != task.oid {
        bail!("packed blob range hash mismatch {}", task.oid);
    }
    let download_ms = download_started.elapsed().as_millis();
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = dest.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let write_started = Instant::now();
    fs::write(&tmp, encoded)?;
    fs::rename(&tmp, &dest)?;
    Ok(HydrateStats {
        hydrated: 1,
        downloaded_bytes: task.len,
        download_ms_sum: download_ms,
        write_ms_sum: write_started.elapsed().as_millis(),
    })
}

fn restore_packed_range_parallelism() -> usize {
    env::var("MAJUTSU_RESTORE_PACKED_RANGE_PARALLELISM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16)
}

pub(crate) fn restore_conflicts(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<Vec<String>> {
    let mut conflicts = Vec::new();
    for record in &plan.files {
        let dest = restore_destination(plan, record)?;
        if !dest.try_exists()? {
            continue;
        }
        if !restore_record_matches_path(paths, conn, record, &dest)? {
            conflicts.push(format!("{}\t{}", record.root_id, record.path));
        }
    }
    Ok(conflicts)
}

pub(crate) fn restore_record_matches_path(
    paths: &Paths,
    conn: &Connection,
    record: &FileRecord,
    dest: &Path,
) -> Result<bool> {
    let meta = fs::symlink_metadata(dest)?;
    match &record.payload {
        Payload::Directory => Ok(meta.file_type().is_dir()),
        Payload::Special { special_kind } => restore_special_matches(&meta, special_kind),
        Payload::Symlink { target } => {
            #[cfg(unix)]
            {
                if !meta.file_type().is_symlink() {
                    return Ok(false);
                }
                Ok(fs::read_link(dest)?.as_os_str() == OsStr::new(target))
            }
            #[cfg(not(unix))]
            {
                if !meta.file_type().is_file() {
                    return Ok(false);
                }
                Ok(fs::read_to_string(dest)? == *target)
            }
        }
        payload => {
            if let Some((oid, object_key)) = payload_blob_ref(payload) {
                if !meta.file_type().is_file() {
                    return Ok(false);
                }
                Ok(fs::read(dest)? == read_blob_payload(paths, conn, oid, object_key)?)
            } else if let Some((_, manifest_key, _)) = payload_large_ref(payload) {
                if !meta.file_type().is_file() || meta.len() != record.size {
                    return Ok(false);
                }
                let manifest: LargeManifest =
                    serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                let mut current = File::open(dest)?;
                for chunk in manifest.chunks {
                    let expected = read_large_chunk(paths, &chunk)?;
                    let mut actual = vec![0u8; expected.len()];
                    current.read_exact(&mut actual)?;
                    if actual != expected {
                        return Ok(false);
                    }
                }
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}
