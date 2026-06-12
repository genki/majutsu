use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use majutsu_core::{
    LargeManifest, OperationLogEntry as OperationExport, SnapshotManifest, TreeManifest,
};
use majutsu_pack::PackIndex;
use majutsu_store::{
    REMOTE_CHUNK_INDEX_SHARD_KEY, REMOTE_HOST_INDEX_KEY, RemoteChunkIndexEntry as ChunkIndexEntry,
    RemoteChunkIndexShard as ChunkIndexShard, RemoteGcMark as GcMarkExport,
    RemoteGcTombstone as GcTombstoneExport, RemoteHostIndex, RemoteHostSummary,
    canonical_remote_alias, canonical_remote_aliases, host_current_ref_key,
    host_last_synced_ref_key, host_legacy_current_key, host_metadata_key,
    host_operation_canonical_key, host_operation_key, host_oplog_canonical_key, host_oplog_key,
    host_ops_prefix, host_snapshot_canonical_key, host_snapshot_key, host_snapshots_prefix,
    is_content_addressed_remote_key, remote_gc_mark_key, remote_gc_tombstone_key,
};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::time::{Duration, Instant, SystemTime};

use crate::cli::{PackArgs, SyncArgs, SyncCommand};
use crate::config::{Config, MetadataExport, Paths, read_config};
use crate::db_refs::{
    persist_export_remote_refs, ref_value, restore_ref_value, set_ref_value, set_remote_ref_value,
};
use crate::object_paths::{
    canonical_alias_for_legacy_key, local_object_keys, prefer_s3_canonical_remote_only,
    remote_live_object_keys_for_local, s3_remote_live_object_keys_for_local,
};
use crate::operation_log::{record_op_with_details, update_operation_result};
use crate::pack_runtime::pack_cmd;
use crate::process_runtime::{acquire_process_lock, process_lock_owner};
use crate::queue_runtime::{
    drain_upload_queue, enqueue_file_upload, enqueue_file_upload_overwrite, enqueue_inline_upload,
    enqueue_inline_upload_overwrite, upload_queue_stats,
};
use crate::remote_runtime::read_remote_host_index;
use crate::remote_store::{RemoteStore, open_remote_with_upload_policy};
use crate::snapshot_state::current_snapshot;
use crate::util::{blake3_hex, new_id, parse_db_time};
use crate::{
    encode_object, ensure_ready, export_metadata, open_db, read_object, remote_object_available,
    remote_ref,
};

const REMOTE_SYNC_STATE_VERSION: &str = "remote-metadata-v7";
const CLONE_BOOTSTRAP_KEY: &str = "metadata/bootstrap.json.zst";
const CLONE_BOOTSTRAP_VERSION: u32 = 2;

struct SyncTrace {
    enabled: bool,
    start: Instant,
}

impl SyncTrace {
    fn new() -> Self {
        Self {
            enabled: env::var("MAJUTSU_TRACE_SYNC")
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
                .unwrap_or(false),
            start: Instant::now(),
        }
    }

    fn mark(&self, label: &str) {
        if self.enabled {
            eprintln!(
                "sync_trace elapsed_ms={} stage={label}",
                self.start.elapsed().as_millis()
            );
        }
    }
}

pub(crate) fn sync_cmd(paths: &Paths, args: SyncArgs) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let remote = open_remote_with_upload_policy(
        config
            .remote
            .as_ref()
            .ok_or_else(|| anyhow!("remote is not configured; run `mj init --remote ...`"))?,
        config.large.multipart,
        config.large.max_parallel_uploads,
    )?;
    if let Some(SyncCommand::Status(status_args)) = args.command {
        if status_args.deep {
            return sync_status(paths, &conn, &remote);
        }
        return sync_status_quick(paths, &conn, &remote);
    }
    if let Some(pid) = process_lock_owner(&paths.sync_lock)? {
        if args.wait {
            let target_current = current_snapshot(&conn)?.unwrap_or_else(|| "(none)".into());
            println!("sync already running pid {pid}; waiting");
            return wait_for_sync_catchup(
                paths,
                &conn,
                &config,
                &remote,
                &target_current,
                args.timeout_secs,
            );
        }
        println!("sync already running pid {pid}");
        sync_status_quick(paths, &conn, &remote)?;
        bail!("sync already running with pid {pid}; use `mj sync --wait` to wait for completion");
    }
    sync_configured_remote(paths, &conn, &config, &remote)?;
    if args.wait {
        let target_current = current_snapshot(&conn)?.unwrap_or_else(|| "(none)".into());
        wait_for_sync_catchup(
            paths,
            &conn,
            &config,
            &remote,
            &target_current,
            args.timeout_secs,
        )?;
    }
    Ok(())
}

pub(crate) enum AutoSyncResult {
    NoRemote,
    Synced,
    Deferred {
        delayed: usize,
        next_retry_after: Option<String>,
    },
}

pub(crate) fn sync_current_if_remote(paths: &Paths) -> Result<AutoSyncResult> {
    let config = read_config(paths)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(AutoSyncResult::NoRemote);
    };
    let upload_stats = upload_queue_stats(paths)?;
    if upload_stats.delayed > 0 {
        return Ok(AutoSyncResult::Deferred {
            delayed: upload_stats.delayed,
            next_retry_after: upload_stats
                .next_retry_after
                .map(|retry_after| retry_after.to_rfc3339()),
        });
    }
    let remote = open_remote_with_upload_policy(
        remote_config,
        config.large.multipart,
        config.large.max_parallel_uploads,
    )?;
    let conn = open_db(paths)?;
    sync_configured_remote(paths, &conn, &config, &remote)?;
    Ok(AutoSyncResult::Synced)
}

fn sync_configured_remote(
    paths: &Paths,
    conn: &Connection,
    config: &Config,
    remote: &RemoteStore,
) -> Result<()> {
    let trace = SyncTrace::new();
    let _lock = acquire_process_lock(&paths.sync_lock, "sync")?;
    trace.mark("lock");
    auto_pack_before_sync(paths, conn)?;
    trace.mark("auto pack");
    let current = current_snapshot(conn)?;
    let export = export_metadata(paths, conn, config)?;
    trace.mark("export metadata");
    let sync_cache = read_remote_sync_cache(paths, remote)?;
    trace.mark("read sync cache");
    let remote_export = metadata_export_for_remote(paths, remote, export)?;
    trace.mark("prepare remote metadata");
    let state_fingerprint = remote_sync_state_cache_key(paths, config, &remote_export)?;
    trace.mark("state fingerprint");
    let upload_stats = upload_queue_stats(paths)?;
    trace.mark("upload queue stats");
    if upload_stats.total == 0
        && sync_cache.state_fingerprint.as_deref() == Some(state_fingerprint.as_str())
        && !sync_remote_prune_enabled()
    {
        let pruned_local_objects =
            prune_local_packed_blob_objects(paths, &remote_export, local_prune_cutoff_time())?;
        trace.mark("local prune");
        println!("synced 0 objects to {}", remote.describe());
        println!("pruned_remote_exports 0");
        println!("pruned_remote_objects 0");
        println!("pruned_local_objects {}", pruned_local_objects);
        return Ok(());
    }
    let previous_last_synced = ref_value(conn, "last-synced")?;
    let synced_at = Utc::now().to_rfc3339();
    set_ref_value(conn, "last-synced", &synced_at)?;
    trace.mark("record last synced");
    let sync_op = new_id("op");
    record_op_with_details(
        conn,
        &sync_op,
        "remote-sync",
        current.as_deref(),
        current.as_deref(),
        "running",
        Some("pushed metadata and objects"),
        None,
        Some("queued"),
    )?;
    trace.mark("record sync op");
    let result = enqueue_and_drain_sync(paths, conn, config, remote, &trace);
    match result {
        Ok(()) => {
            update_operation_result(conn, &sync_op, "done", None, Some("synced"))?;
            Ok(())
        }
        Err(err) => {
            restore_ref_value(conn, "last-synced", previous_last_synced.as_deref())?;
            update_operation_result(
                conn,
                &sync_op,
                "failed",
                Some(&format!("{err:#}")),
                Some("failed"),
            )?;
            Err(err)
        }
    }
}

fn sync_status_quick(paths: &Paths, conn: &Connection, remote: &RemoteStore) -> Result<()> {
    let status = sync_wait_status(paths, conn, remote)?;
    print_sync_wait_status(&status);
    println!("status_mode quick");
    println!("local_objects skipped");
    println!("missing_remote_objects skipped");
    println!("hint run `mj sync status --deep` to verify every referenced object");
    Ok(())
}

#[derive(Debug)]
struct SyncWaitStatus {
    remote: String,
    local_current: String,
    remote_current: String,
    remote_last_synced: String,
    queued_uploads: usize,
    queued_uploads_retrying: usize,
    queued_uploads_delayed: usize,
    queued_upload_next_retry_after: String,
    queued_upload_attempts: u64,
    queued_upload_max_attempts: u32,
    upload_queue_backpressure: bool,
    sync_lock_pid: Option<u32>,
}

impl SyncWaitStatus {
    fn is_caught_up(&self) -> bool {
        self.local_current == self.remote_current && self.queued_uploads == 0
    }
}

fn sync_wait_status(
    paths: &Paths,
    conn: &Connection,
    remote: &RemoteStore,
) -> Result<SyncWaitStatus> {
    let local_current = current_snapshot(conn)?.unwrap_or_else(|| "(none)".into());
    let config = read_config(paths)?;
    let canonical_current = host_current_ref_key(&config.host.id);
    let canonical_last_synced = host_last_synced_ref_key(&config.host.id);
    let mut remote_current = remote_ref(remote, &canonical_current)?;
    if remote_current.is_none() {
        remote_current = remote_ref(remote, "hosts/current")?;
    }
    let remote_last_synced = remote_ref(remote, &canonical_last_synced)?;
    let upload_stats = upload_queue_stats(paths)?;
    let sync_lock_pid = process_lock_owner(&paths.sync_lock).ok().flatten();
    Ok(SyncWaitStatus {
        remote: remote.describe(),
        local_current,
        remote_current: remote_current.unwrap_or_else(|| "(none)".into()),
        remote_last_synced: remote_last_synced.unwrap_or_else(|| "(none)".into()),
        queued_uploads: upload_stats.total,
        queued_uploads_retrying: upload_stats.retrying,
        queued_uploads_delayed: upload_stats.delayed,
        queued_upload_next_retry_after: upload_stats
            .next_retry_after
            .map(|retry_after| retry_after.to_rfc3339())
            .unwrap_or_else(|| "(none)".into()),
        queued_upload_attempts: upload_stats.attempts,
        queued_upload_max_attempts: upload_stats.max_attempts,
        upload_queue_backpressure: upload_stats.has_backpressure(),
        sync_lock_pid,
    })
}

fn print_sync_wait_status(status: &SyncWaitStatus) {
    println!("remote {}", status.remote);
    println!("local_current {}", status.local_current);
    println!("remote_current {}", status.remote_current);
    println!("remote_last_synced {}", status.remote_last_synced);
    println!("queued_uploads {}", status.queued_uploads);
    println!("queued_uploads_retrying {}", status.queued_uploads_retrying);
    println!("queued_uploads_delayed {}", status.queued_uploads_delayed);
    println!(
        "queued_upload_next_retry_after {}",
        status.queued_upload_next_retry_after
    );
    println!("queued_upload_attempts {}", status.queued_upload_attempts);
    println!(
        "queued_upload_max_attempts {}",
        status.queued_upload_max_attempts
    );
    println!(
        "upload_queue_backpressure {}",
        status.upload_queue_backpressure
    );
    println!(
        "sync_lock_pid {}",
        status
            .sync_lock_pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "(none)".into())
    );
}

fn wait_for_sync_catchup(
    paths: &Paths,
    conn: &Connection,
    config: &Config,
    remote: &RemoteStore,
    target_current: &str,
    timeout_secs: u64,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let mut target_current = target_current.to_string();
    loop {
        if let Some(latest_current) = current_snapshot(conn)? {
            if latest_current != target_current {
                println!(
                    "wait_target_updated {} -> {}",
                    target_current, latest_current
                );
                target_current = latest_current;
            }
        }
        let status = sync_wait_status(paths, conn, remote)?;
        if status.is_caught_up() {
            print_sync_wait_status(&status);
            println!("wait_target_current {}", target_current);
            println!("status_mode quick");
            return Ok(());
        }
        if status.sync_lock_pid.is_none()
            && status.queued_uploads == 0
            && status.local_current != status.remote_current
        {
            println!("wait_resync {}", status.local_current);
            sync_configured_remote(paths, conn, config, remote)?;
            continue;
        }
        if Instant::now() >= deadline {
            print_sync_wait_status(&status);
            bail!(
                "timed out waiting for sync target {} after {}s; local_current={} remote_current={} queued_uploads={} delayed={} lock_pid={}",
                target_current,
                timeout_secs,
                status.local_current,
                status.remote_current,
                status.queued_uploads,
                status.queued_uploads_delayed,
                status
                    .sync_lock_pid
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "(none)".into())
            );
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn auto_pack_before_sync(paths: &Paths, conn: &Connection) -> Result<()> {
    if env::var("MAJUTSU_SYNC_AUTO_PACK").as_deref() == Ok("0") {
        return Ok(());
    }
    let threshold = env::var("MAJUTSU_SYNC_AUTO_PACK_MIN_BLOBS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(128);
    let unpacked_small_blobs: i64 = conn.query_row(
        "select count(*) from blobs where pack_id is null and size <= 131072",
        [],
        |row| row.get(0),
    )?;
    if unpacked_small_blobs >= threshold {
        println!("auto_pack unpacked_small_blobs {unpacked_small_blobs}");
        pack_cmd(paths, PackArgs { compact: false })?;
    }
    Ok(())
}

fn enqueue_and_drain_sync(
    paths: &Paths,
    conn: &Connection,
    config: &Config,
    remote: &RemoteStore,
    trace: &SyncTrace,
) -> Result<()> {
    let local_prune_cutoff = local_prune_cutoff_time();
    let content_export = export_metadata(paths, conn, config)?;
    trace.mark("content export");
    let remote_export =
        metadata_export_for_remote(paths, remote, export_metadata(paths, conn, config)?)?;
    trace.mark("remote export");
    let sync_cache = read_remote_sync_cache(paths, remote)?;
    trace.mark("read sync cache 2");
    let mut sync_fingerprints = build_remote_sync_fingerprints(&config.host.id, &remote_export)?;
    trace.mark("metadata fingerprints");
    let state_fingerprint = remote_sync_state_cache_key(paths, config, &remote_export)?;
    trace.mark("state fingerprint 2");
    let remote_metadata_json = metadata_export_json_for_remote(remote, &remote_export)?;
    trace.mark("metadata json");
    let legacy_metadata = "metadata/export.json";
    let host_metadata_plain = host_metadata_key(&config.host.id);
    let host_metadata = enqueue_metadata_uploads(
        paths,
        remote,
        legacy_metadata,
        &host_metadata_plain,
        &remote_metadata_json,
    )?;
    for snapshot in &remote_export.snapshots {
        enqueue_snapshot_uploads_if_needed(paths, remote, &sync_cache, &config.host.id, snapshot)?;
    }
    for operation in &remote_export.operations {
        enqueue_operation_uploads_if_needed(
            paths,
            remote,
            &sync_cache,
            &config.host.id,
            operation,
        )?;
    }
    if matches!(remote, RemoteStore::File(_)) {
        enqueue_inline_upload(
            paths,
            &host_oplog_key(&config.host.id),
            majutsu_core::encode_operation_log(&remote_export.operations)?,
        )?;
    }
    enqueue_inline_upload(
        paths,
        &host_oplog_canonical_key(&config.host.id),
        encode_canonical_remote_oplog(paths, &remote_export.operations)?,
    )?;
    enqueue_cached_inline_upload(
        paths,
        &sync_cache,
        &mut sync_fingerprints,
        "config.toml",
        toml::to_string_pretty(config)?.into_bytes(),
    )?;
    enqueue_cached_inline_upload(
        paths,
        &sync_cache,
        &mut sync_fingerprints,
        "host.toml",
        toml::to_string_pretty(&config.host)?.into_bytes(),
    )?;
    if let Some(current) = remote_export.refs.get("current") {
        enqueue_inline_upload(paths, "hosts/current", current.as_bytes().to_vec())?;
        enqueue_inline_upload(
            paths,
            &host_legacy_current_key(&config.host.id),
            current.as_bytes().to_vec(),
        )?;
        enqueue_inline_upload(
            paths,
            &host_current_ref_key(&config.host.id),
            current.as_bytes().to_vec(),
        )?;
    }
    if let Some(last_synced) = remote_export.refs.get("last-synced") {
        enqueue_inline_upload(
            paths,
            &host_last_synced_ref_key(&config.host.id),
            last_synced.as_bytes().to_vec(),
        )?;
    }
    let host_index = update_remote_host_index(remote, config, &remote_export, &host_metadata)?;
    trace.mark("host index");
    enqueue_inline_upload(
        paths,
        REMOTE_HOST_INDEX_KEY,
        serde_json::to_vec_pretty(&host_index)?,
    )?;
    enqueue_clone_bootstrap_if_supported(paths, remote, host_index)?;
    trace.mark("clone bootstrap");
    let recipients = paths.home.join("keys/recipients.toml");
    if recipients.exists() {
        enqueue_cached_inline_upload(
            paths,
            &sync_cache,
            &mut sync_fingerprints,
            "keys/recipients.toml",
            fs::read(&recipients)?,
        )?;
    }
    if !remote_export.chunks.is_empty() {
        enqueue_inline_upload(
            paths,
            REMOTE_CHUNK_INDEX_SHARD_KEY,
            encode_canonical_remote_export(paths, &build_chunk_index_shard(&remote_export))?,
        )?;
    }
    trace.mark("metadata queue");

    let content_keys = sync_content_local_object_keys(paths, remote, &content_export, &sync_cache)?;
    let snapshot_manifest_keys = content_keys
        .iter()
        .filter(|key| {
            content_export
                .snapshots
                .iter()
                .any(|snapshot| snapshot.manifest_key == **key)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let all_snapshot_manifest_keys = content_export
        .snapshots
        .iter()
        .map(|snapshot| snapshot.manifest_key.as_str())
        .collect::<BTreeSet<_>>();
    let existing_canonical_aliases =
        if remote_prefers_canonical_only(remote) && sync_verify_cached_remote_aliases() {
            Some(list_remote_canonical_content_aliases(remote)?)
        } else {
            None
        };
    for key in content_keys {
        let local = paths.home.join(&key);
        if local.exists() {
            let canonical_only =
                remote_prefers_canonical_only(remote) && s3_prefers_canonical_remote_only(&key);
            if !canonical_only {
                let force = snapshot_manifest_keys.contains(key.as_str());
                enqueue_file_upload_if_needed(paths, remote, &key, &local, force)?;
            }
            for alias in canonical_remote_aliases(&key) {
                let snapshot_manifest = snapshot_manifest_keys.contains(&key);
                let mut structured_bytes = None;
                let fingerprint = if snapshot_manifest {
                    payload_fingerprint(&fs::read(&local)?)
                } else {
                    content_object_fingerprint(&alias)
                };
                sync_fingerprints.insert(alias.clone(), fingerprint.clone());
                let alias_exists = existing_canonical_aliases
                    .as_ref()
                    .is_some_and(|keys| keys.contains(&alias));
                if cache_matches(&sync_cache, &alias, &fingerprint)
                    && (existing_canonical_aliases.is_none() || alias_exists)
                {
                    continue;
                }
                if structured_bytes.is_none() && canonical_alias_uses_structured_encoding(&key) {
                    structured_bytes = Some(encode_canonical_local_object(
                        paths,
                        &key,
                        &all_snapshot_manifest_keys,
                    )?);
                }
                if let Some(bytes) = structured_bytes {
                    enqueue_inline_upload_overwrite(paths, &alias, bytes)?;
                } else {
                    enqueue_file_upload(paths, &alias, &local)?;
                }
            }
        }
    }
    trace.mark("content queue");
    enqueue_inline_upload(
        paths,
        &remote_gc_mark_key(&config.host.id),
        serde_json::to_vec_pretty(&build_gc_mark_export(
            paths,
            config,
            remote,
            &content_export,
        )?)?,
    )?;
    trace.mark("gc mark");
    let upload_drain = drain_upload_queue(paths, remote, config.large.max_parallel_uploads)?;
    trace.mark("drain queue");
    write_remote_sync_cache(paths, remote, sync_fingerprints, state_fingerprint)?;
    trace.mark("write sync cache");
    let (pruned_remote_exports, pruned_remote_objects) = if sync_remote_prune_enabled() {
        let pruned_remote_exports =
            prune_remote_host_exports(remote, &config.host.id, &remote_export)?;
        let pruned_remote_objects =
            prune_remote_packed_blob_objects(
                remote,
                &config.host.id,
                &remote_export,
                config.large.max_parallel_uploads,
            )? + prune_remote_legacy_canonical_objects(remote, config.large.max_parallel_uploads)?;
        (pruned_remote_exports, pruned_remote_objects)
    } else {
        (0, 0)
    };
    let pruned_local_objects =
        prune_local_packed_blob_objects(paths, &remote_export, local_prune_cutoff)?;
    trace.mark("local prune");
    persist_export_remote_refs(
        conn,
        &remote.describe(),
        &config.host.id,
        &remote_export.refs,
    )?;
    trace.mark("persist remote refs");
    println!(
        "synced {} objects to {}",
        upload_drain.uploaded,
        remote.describe()
    );
    println!("synced_bytes {}", upload_drain.uploaded_bytes);
    println!("skipped_uploads {}", upload_drain.skipped);
    println!("pruned_remote_exports {}", pruned_remote_exports);
    println!("pruned_remote_objects {}", pruned_remote_objects);
    println!("pruned_local_objects {}", pruned_local_objects);
    Ok(())
}

fn remote_prefers_canonical_only(remote: &RemoteStore) -> bool {
    matches!(remote, RemoteStore::S3(_))
}

fn s3_prefers_canonical_remote_only(key: &str) -> bool {
    prefer_s3_canonical_remote_only(key)
}

fn content_object_fingerprint(key: &str) -> String {
    format!("content-v2:{key}")
}

fn sync_content_local_object_keys(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
    cache: &RemoteSyncCache,
) -> Result<Vec<String>> {
    if !remote_prefers_canonical_only(remote)
        || cache.entries.is_empty()
        || sync_verify_cached_remote_aliases()
    {
        return local_object_keys(paths, export);
    }
    let mut keys = Vec::new();
    if let Some(current) = export.refs.get("current")
        && let Some(snapshot) = export
            .snapshots
            .iter()
            .find(|snapshot| snapshot.id == *current)
    {
        keys.push(snapshot.manifest_key.clone());
        if let Ok(manifest) = current_snapshot_manifest_for_object_keys(paths, snapshot) {
            for root_tree in manifest.root_trees.values() {
                keys.push(root_tree.tree_key.clone());
            }
        }
    }
    for blob in &export.blobs {
        if blob.pack_id.is_none() {
            keys.push(blob.object_key.clone());
        }
    }
    for pack in &export.packs {
        keys.push(pack.pack_key.clone());
        keys.push(pack.index_key.clone());
    }
    for large in &export.large_objects {
        keys.push(large.manifest_key.clone());
    }
    for chunk in &export.chunks {
        keys.push(chunk.object_key.clone());
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn current_snapshot_manifest_for_object_keys(
    paths: &Paths,
    snapshot: &majutsu_core::SnapshotExport,
) -> Result<SnapshotManifest> {
    if !snapshot.manifest_json.trim().is_empty() {
        return Ok(serde_json::from_str(&snapshot.manifest_json)?);
    }
    let bytes = fs::read(paths.home.join(&snapshot.manifest_key))?;
    Ok(serde_json::from_slice(&crate::decode_object(
        paths, &bytes,
    )?)?)
}

fn sync_verify_cached_remote_aliases() -> bool {
    env::var("MAJUTSU_SYNC_VERIFY_CACHED_REMOTE_ALIASES")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn sync_remote_prune_enabled() -> bool {
    std::env::var("MAJUTSU_SYNC_REMOTE_PRUNE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn metadata_export_for_remote(
    paths: &Paths,
    remote: &RemoteStore,
    export: MetadataExport,
) -> Result<MetadataExport> {
    if remote_prefers_canonical_only(remote) {
        return compact_remote_metadata_export(paths, export);
    }
    Ok(export)
}

fn compact_remote_metadata_export(
    paths: &Paths,
    mut export: MetadataExport,
) -> Result<MetadataExport> {
    for snapshot in &mut export.snapshots {
        snapshot.manifest_json = compact_snapshot_manifest_json_for_metadata(
            paths,
            &snapshot.id,
            &snapshot.manifest_key,
            &snapshot.manifest_json,
        )?;
    }
    let skipped_parents = export
        .operations
        .iter()
        .filter(|operation| operation.kind == "remote-sync")
        .map(|operation| (operation.id.clone(), operation.parent_op.clone()))
        .collect::<BTreeMap<_, _>>();
    for operation in &mut export.operations {
        let mut parent = operation.parent_op.clone();
        let mut seen = BTreeSet::new();
        while let Some(parent_id) = parent.clone() {
            if !seen.insert(parent_id.clone()) {
                parent = None;
                break;
            }
            match skipped_parents.get(&parent_id) {
                Some(next_parent) => parent = next_parent.clone(),
                None => break,
            }
        }
        operation.parent_op = parent;
    }
    export
        .operations
        .retain(|operation| operation.kind != "remote-sync");
    Ok(export)
}

fn compact_snapshot_manifest_json_for_metadata(
    paths: &Paths,
    snapshot_id: &str,
    manifest_key: &str,
    manifest_json: &str,
) -> Result<String> {
    let mut value = if manifest_json.trim().is_empty() {
        let bytes = fs::read(paths.home.join(manifest_key)).with_context(|| {
            format!("read snapshot manifest object {snapshot_id} {manifest_key}")
        })?;
        serde_json::to_value(serde_json::from_slice::<SnapshotManifest>(
            &crate::decode_object(paths, &bytes)?,
        )?)?
    } else {
        serde_json::from_str(manifest_json)?
    };
    compact_snapshot_manifest_value(&mut value);
    serde_json::to_string(&value).map_err(Into::into)
}

fn compact_snapshot_manifest_value(value: &mut serde_json::Value) {
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "roots".into(),
            serde_json::Value::Object(serde_json::Map::new()),
        );
        object.insert("roots_omitted".into(), serde_json::Value::Bool(true));
    }
}

fn enqueue_metadata_uploads(
    paths: &Paths,
    remote: &RemoteStore,
    legacy_key: &str,
    host_key: &str,
    metadata_json: &[u8],
) -> Result<String> {
    if !matches!(remote, RemoteStore::S3(_)) {
        enqueue_inline_upload(paths, legacy_key, metadata_json.to_vec())?;
        enqueue_inline_upload(paths, host_key, metadata_json.to_vec())?;
        return Ok(host_key.to_string());
    }
    let compressed = zstd::stream::encode_all(metadata_json, 3)?;
    enqueue_inline_upload(
        paths,
        &compressed_metadata_key(legacy_key),
        compressed.clone(),
    )?;
    enqueue_inline_upload(paths, &compressed_metadata_key(host_key), compressed)?;
    Ok(compressed_metadata_key(host_key))
}

fn compressed_metadata_key(key: &str) -> String {
    format!("{key}.zst")
}

#[derive(Debug, Serialize)]
struct CloneBootstrapExport {
    version: u32,
    host_index: RemoteHostIndex,
}

fn enqueue_clone_bootstrap_if_supported(
    paths: &Paths,
    remote: &RemoteStore,
    host_index: RemoteHostIndex,
) -> Result<()> {
    if !matches!(remote, RemoteStore::S3(_)) {
        return Ok(());
    }
    let bootstrap = CloneBootstrapExport {
        version: CLONE_BOOTSTRAP_VERSION,
        host_index,
    };
    let json = serde_json::to_vec(&bootstrap)?;
    let compressed = zstd::stream::encode_all(json.as_slice(), 3)?;
    enqueue_inline_upload(paths, CLONE_BOOTSTRAP_KEY, compressed)
}

fn enqueue_cached_inline_upload(
    paths: &Paths,
    cache: &RemoteSyncCache,
    fingerprints: &mut BTreeMap<String, String>,
    key: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    let fingerprint = payload_fingerprint(&bytes);
    fingerprints.insert(key.to_string(), fingerprint.clone());
    if cache_matches(cache, key, &fingerprint) {
        return Ok(());
    }
    enqueue_inline_upload(paths, key, bytes)
}

fn enqueue_snapshot_uploads_if_needed(
    paths: &Paths,
    remote: &RemoteStore,
    cache: &RemoteSyncCache,
    host_id: &str,
    snapshot: &majutsu_core::SnapshotExport,
) -> Result<()> {
    let canonical_key = host_snapshot_canonical_key(host_id, &snapshot.id);
    let fingerprint = payload_fingerprint(&serde_json::to_vec(snapshot)?);
    let canonical_bytes = encode_snapshot_export_for_remote(paths, remote, snapshot)?;
    if matches!(remote, RemoteStore::File(_)) {
        let legacy_key = host_snapshot_key(host_id, &snapshot.id);
        enqueue_inline_upload(
            paths,
            &legacy_key,
            snapshot_export_json_for_remote(remote, snapshot)?,
        )?;
        enqueue_inline_upload(paths, &canonical_key, canonical_bytes)?;
        return Ok(());
    }
    if cache_matches(cache, &canonical_key, &fingerprint) {
        return Ok(());
    }
    if remote_key_exists(remote, &canonical_key) {
        return Ok(());
    }
    enqueue_inline_upload(paths, &canonical_key, canonical_bytes)
}

fn enqueue_operation_uploads_if_needed(
    paths: &Paths,
    remote: &RemoteStore,
    cache: &RemoteSyncCache,
    host_id: &str,
    operation: &OperationExport,
) -> Result<()> {
    let canonical_key = host_operation_canonical_key(host_id, &operation.id);
    let fingerprint = payload_fingerprint(&serde_json::to_vec(operation)?);
    let canonical_bytes = encode_canonical_remote_export(paths, operation)?;
    if matches!(remote, RemoteStore::File(_)) {
        let legacy_key = host_operation_key(host_id, &operation.id);
        enqueue_inline_upload(paths, &legacy_key, serde_json::to_vec_pretty(operation)?)?;
        enqueue_inline_upload(paths, &canonical_key, canonical_bytes)?;
        return Ok(());
    }
    if cache_matches(cache, &canonical_key, &fingerprint) {
        return Ok(());
    }
    if remote_key_exists(remote, &canonical_key) {
        return Ok(());
    }
    enqueue_inline_upload(paths, &canonical_key, canonical_bytes)
}

fn enqueue_file_upload_if_needed(
    paths: &Paths,
    remote: &RemoteStore,
    key: &str,
    source: &std::path::Path,
    force: bool,
) -> Result<()> {
    if !force && remote_key_can_be_skipped(remote, key) {
        return Ok(());
    }
    if force {
        enqueue_file_upload_overwrite(paths, key, source)
    } else {
        enqueue_file_upload(paths, key, source)
    }
}

fn remote_key_can_be_skipped(remote: &RemoteStore, key: &str) -> bool {
    if !is_content_addressed_remote_key(key) {
        return false;
    }
    remote_key_exists(remote, key)
}

fn remote_key_exists(remote: &RemoteStore, key: &str) -> bool {
    remote.exists(key).unwrap_or(false)
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RemoteSyncCache {
    version: u32,
    remote: String,
    #[serde(default)]
    state_fingerprint: Option<String>,
    entries: BTreeMap<String, String>,
}

fn remote_sync_cache_path(paths: &Paths) -> std::path::PathBuf {
    paths.home.join("cache/remote-sync.json")
}

fn read_remote_sync_cache(paths: &Paths, remote: &RemoteStore) -> Result<RemoteSyncCache> {
    let path = remote_sync_cache_path(paths);
    if !path.exists() {
        return Ok(empty_remote_sync_cache(remote));
    }
    let cache: RemoteSyncCache = serde_json::from_slice(&fs::read(path)?)?;
    if cache.version == 1 && cache.remote == remote.describe() {
        Ok(cache)
    } else {
        Ok(empty_remote_sync_cache(remote))
    }
}

fn empty_remote_sync_cache(remote: &RemoteStore) -> RemoteSyncCache {
    RemoteSyncCache {
        version: 1,
        remote: remote.describe(),
        state_fingerprint: None,
        entries: BTreeMap::new(),
    }
}

fn write_remote_sync_cache(
    paths: &Paths,
    remote: &RemoteStore,
    entries: BTreeMap<String, String>,
    state_fingerprint: String,
) -> Result<()> {
    let path = remote_sync_cache_path(paths);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    let cache = RemoteSyncCache {
        version: 1,
        remote: remote.describe(),
        state_fingerprint: Some(state_fingerprint),
        entries,
    };
    fs::write(&tmp, serde_json::to_vec_pretty(&cache)?)?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn build_remote_sync_fingerprints(
    host_id: &str,
    export: &MetadataExport,
) -> Result<BTreeMap<String, String>> {
    let mut entries = BTreeMap::new();
    for snapshot in &export.snapshots {
        let fingerprint = payload_fingerprint(&serde_json::to_vec(snapshot)?);
        entries.insert(
            host_snapshot_canonical_key(host_id, &snapshot.id),
            fingerprint,
        );
    }
    for operation in &export.operations {
        let fingerprint = payload_fingerprint(&serde_json::to_vec(operation)?);
        entries.insert(
            host_operation_canonical_key(host_id, &operation.id),
            fingerprint,
        );
    }
    Ok(entries)
}

fn remote_sync_state_fingerprint(
    paths: &Paths,
    config: &Config,
    export: &MetadataExport,
) -> Result<String> {
    let mut export_value = serde_json::to_value(export)?;
    if let Some(object) = export_value.as_object_mut() {
        object.remove("exported_at");
        object.remove("operations");
        if let Some(refs) = object
            .get_mut("refs")
            .and_then(serde_json::Value::as_object_mut)
        {
            refs.remove("last-synced");
        }
    }
    let mut snapshot_manifest_fingerprints = BTreeMap::new();
    for snapshot in &export.snapshots {
        let path = paths.home.join(&snapshot.manifest_key);
        if path.exists() {
            snapshot_manifest_fingerprints.insert(
                snapshot.manifest_key.clone(),
                payload_fingerprint(&fs::read(path)?),
            );
        }
    }
    let value = serde_json::json!({
        "config": config,
        "export": export_value,
        "snapshot_manifest_fingerprints": snapshot_manifest_fingerprints,
    });
    Ok(payload_fingerprint(&serde_json::to_vec(&value)?))
}

fn remote_sync_state_cache_key(
    paths: &Paths,
    config: &Config,
    export: &MetadataExport,
) -> Result<String> {
    Ok(format!(
        "{}:{}",
        REMOTE_SYNC_STATE_VERSION,
        remote_sync_state_fingerprint(paths, config, export)?
    ))
}

fn cache_matches(cache: &RemoteSyncCache, key: &str, fingerprint: &str) -> bool {
    cache
        .entries
        .get(key)
        .is_some_and(|cached| cached == fingerprint)
}

fn payload_fingerprint(bytes: &[u8]) -> String {
    blake3_hex(bytes)
}

fn sync_status(paths: &Paths, conn: &Connection, remote: &RemoteStore) -> Result<()> {
    let status = sync_status_snapshot(paths, conn, remote)?;
    print_sync_status(&status);
    Ok(())
}

struct SyncStatusSnapshot {
    remote: String,
    local_current: String,
    remote_current: String,
    remote_last_synced: String,
    local_objects: usize,
    missing_remote_objects: usize,
    queued_uploads: usize,
    queued_uploads_retrying: usize,
    queued_uploads_delayed: usize,
    queued_upload_next_retry_after: String,
    queued_upload_attempts: u64,
    queued_upload_max_attempts: u32,
    upload_queue_backpressure: bool,
}

fn sync_status_snapshot(
    paths: &Paths,
    conn: &Connection,
    remote: &RemoteStore,
) -> Result<SyncStatusSnapshot> {
    let local_current = current_snapshot(conn)?;
    let config = read_config(paths)?;
    let canonical_current = host_current_ref_key(&config.host.id);
    let canonical_last_synced = host_last_synced_ref_key(&config.host.id);
    let mut remote_current = remote_ref(remote, &canonical_current)?;
    if remote_current.is_none() {
        remote_current = remote_ref(remote, "hosts/current")?;
    }
    let remote_last_synced = remote_ref(remote, &canonical_last_synced)?;
    if let Some(value) = remote_current.as_deref() {
        set_remote_ref_value(conn, &remote.describe(), &canonical_current, value)?;
    }
    if let Some(value) = remote_last_synced.as_deref() {
        set_remote_ref_value(conn, &remote.describe(), &canonical_last_synced, value)?;
    }
    let export = export_metadata(paths, conn, &read_config(paths)?)?;
    let local_keys = local_object_keys(paths, &export)?;
    let mut missing_remote = 0usize;
    for key in &local_keys {
        if !remote_object_available(remote, key)? {
            missing_remote += 1;
        }
    }
    let upload_stats = upload_queue_stats(paths)?;
    Ok(SyncStatusSnapshot {
        remote: remote.describe(),
        local_current: local_current.unwrap_or_else(|| "(none)".into()),
        remote_current: remote_current.unwrap_or_else(|| "(none)".into()),
        remote_last_synced: remote_last_synced.unwrap_or_else(|| "(none)".into()),
        local_objects: local_keys.len(),
        missing_remote_objects: missing_remote,
        queued_uploads: upload_stats.total,
        queued_uploads_retrying: upload_stats.retrying,
        queued_uploads_delayed: upload_stats.delayed,
        queued_upload_next_retry_after: upload_stats
            .next_retry_after
            .map(|retry_after| retry_after.to_rfc3339())
            .unwrap_or_else(|| "(none)".into()),
        queued_upload_attempts: upload_stats.attempts,
        queued_upload_max_attempts: upload_stats.max_attempts,
        upload_queue_backpressure: upload_stats.has_backpressure(),
    })
}

fn print_sync_status(status: &SyncStatusSnapshot) {
    println!("remote {}", status.remote);
    println!("local_current {}", status.local_current);
    println!("remote_current {}", status.remote_current);
    println!("remote_last_synced {}", status.remote_last_synced);
    println!("local_objects {}", status.local_objects);
    println!("missing_remote_objects {}", status.missing_remote_objects);
    println!("queued_uploads {}", status.queued_uploads);
    println!("queued_uploads_retrying {}", status.queued_uploads_retrying);
    println!("queued_uploads_delayed {}", status.queued_uploads_delayed);
    println!(
        "queued_upload_next_retry_after {}",
        status.queued_upload_next_retry_after
    );
    println!("queued_upload_attempts {}", status.queued_upload_attempts);
    println!(
        "queued_upload_max_attempts {}",
        status.queued_upload_max_attempts
    );
    println!(
        "upload_queue_backpressure {}",
        status.upload_queue_backpressure
    );
}

fn metadata_export_json_for_remote(
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<Vec<u8>> {
    if remote_prefers_canonical_only(remote) {
        let mut value = serde_json::to_value(export)?;
        compact_manifest_json_fields(&mut value);
        return serde_json::to_vec_pretty(&value).map_err(Into::into);
    }
    serde_json::to_vec_pretty(export).map_err(Into::into)
}

fn snapshot_export_json_for_remote<T: Serialize>(
    remote: &RemoteStore,
    snapshot: &T,
) -> Result<Vec<u8>> {
    if remote_prefers_canonical_only(remote) {
        let mut value = serde_json::to_value(snapshot)?;
        compact_manifest_json_fields(&mut value);
        return serde_json::to_vec_pretty(&value).map_err(Into::into);
    }
    serde_json::to_vec_pretty(snapshot).map_err(Into::into)
}

fn encode_snapshot_export_for_remote<T: Serialize>(
    paths: &Paths,
    remote: &RemoteStore,
    snapshot: &T,
) -> Result<Vec<u8>> {
    if remote_prefers_canonical_only(remote) {
        let mut value = serde_json::to_value(snapshot)?;
        compact_manifest_json_fields(&mut value);
        return encode_canonical_remote_export(paths, &value);
    }
    encode_canonical_remote_export(paths, snapshot)
}

fn compact_manifest_json_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(serde_json::Value::String(manifest_json)) = object.get_mut("manifest_json")
                && !manifest_json.trim().is_empty()
                && let Ok(mut manifest) = serde_json::from_str::<serde_json::Value>(manifest_json)
            {
                compact_snapshot_manifest_value(&mut manifest);
                if let Ok(compacted) = serde_json::to_string(&manifest) {
                    *manifest_json = compacted;
                }
                object.insert(
                    "manifest_json_omitted".into(),
                    serde_json::Value::Bool(true),
                );
            } else if object.contains_key("manifest_json") {
                object.insert(
                    "manifest_json_omitted".into(),
                    serde_json::Value::Bool(true),
                );
            }
            if let Some(serde_json::Value::Array(snapshots)) = object.get_mut("snapshots") {
                for snapshot in snapshots {
                    compact_manifest_json_fields(snapshot);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                compact_manifest_json_fields(value);
            }
        }
        _ => {}
    }
}

fn encode_canonical_remote_export<T: Serialize>(paths: &Paths, value: &T) -> Result<Vec<u8>> {
    let cbor = serde_cbor::to_vec(value)?;
    let compressed = zstd::stream::encode_all(cbor.as_slice(), 3)?;
    encode_object(paths, &compressed)
}

fn encode_canonical_remote_oplog(paths: &Paths, operations: &[OperationExport]) -> Result<Vec<u8>> {
    let cborl = majutsu_core::encode_operation_log(operations)?;
    let compressed = zstd::stream::encode_all(cborl.as_slice(), 3)?;
    encode_object(paths, &compressed)
}

fn build_chunk_index_shard(export: &MetadataExport) -> ChunkIndexShard {
    let chunks = export
        .chunks
        .iter()
        .map(|chunk| {
            ChunkIndexEntry::new(
                chunk.oid.clone(),
                chunk.size,
                chunk.object_key.clone(),
                canonical_remote_alias(&chunk.object_key),
            )
        })
        .collect();
    ChunkIndexShard::new(Utc::now(), chunks)
}

fn build_gc_mark_export(
    paths: &Paths,
    config: &Config,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<GcMarkExport> {
    let object_keys = if remote_prefers_canonical_only(remote) {
        s3_remote_live_object_keys_for_local(paths, export)?
    } else {
        remote_live_object_keys_for_local(paths, export)?
    };
    Ok(GcMarkExport::new(
        config.host.id.clone(),
        Utc::now(),
        export.refs.get("current").cloned(),
        object_keys,
    ))
}

fn encode_canonical_local_object(
    paths: &Paths,
    key: &str,
    snapshot_manifest_keys: &BTreeSet<&str>,
) -> Result<Vec<u8>> {
    let bytes = read_object(paths, key)?;
    if snapshot_manifest_keys.contains(key) {
        let manifest: SnapshotManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode snapshot manifest {key}"))?;
        return crate::encode_compact_snapshot_manifest_for_remote(paths, &manifest);
    }
    if key.starts_with("objects/trees/") {
        let manifest: TreeManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode tree manifest {key}"))?;
        encode_canonical_remote_export(paths, &manifest)
    } else if key.starts_with("objects/indexes/pack/") {
        let index: PackIndex =
            serde_json::from_slice(&bytes).with_context(|| format!("decode pack index {key}"))?;
        encode_canonical_remote_export(paths, &index)
    } else if key.starts_with("objects/large/manifests/") {
        let manifest: LargeManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode large manifest {key}"))?;
        encode_canonical_remote_export(paths, &manifest)
    } else {
        encode_object(paths, &bytes)
    }
}

fn canonical_alias_uses_structured_encoding(key: &str) -> bool {
    key.starts_with("objects/blobs/")
        || key.starts_with("objects/trees/")
        || key.starts_with("objects/indexes/pack/")
        || key.starts_with("objects/large/manifests/")
}

fn update_remote_host_index(
    remote: &RemoteStore,
    config: &Config,
    export: &MetadataExport,
    metadata_key: &str,
) -> Result<RemoteHostIndex> {
    let mut index = read_remote_host_index(remote)?;
    let last_synced_at = export
        .refs
        .get("last-synced")
        .map(|value| parse_db_time(value))
        .transpose()?
        .unwrap_or(export.exported_at);
    let summary = RemoteHostSummary {
        id: config.host.id.clone(),
        name: config.host.name.clone(),
        last_synced_at,
        current_snapshot: export.refs.get("current").cloned(),
        metadata_key: metadata_key.to_string(),
    };
    index.upsert_host(summary, Utc::now());
    Ok(index)
}

fn prune_remote_host_exports(
    remote: &RemoteStore,
    host_id: &str,
    export: &MetadataExport,
) -> Result<usize> {
    let live_snapshots = export
        .snapshots
        .iter()
        .flat_map(|snapshot| {
            [
                host_snapshot_key(host_id, &snapshot.id),
                host_snapshot_canonical_key(host_id, &snapshot.id),
            ]
        })
        .collect::<BTreeSet<_>>();
    let mut live_ops = export
        .operations
        .iter()
        .flat_map(|operation| {
            [
                host_operation_key(host_id, &operation.id),
                host_operation_canonical_key(host_id, &operation.id),
            ]
        })
        .collect::<BTreeSet<_>>();
    live_ops.insert(host_oplog_key(host_id));
    live_ops.insert(host_oplog_canonical_key(host_id));
    let mut removed = 0usize;
    for key in remote.list(&host_snapshots_prefix(host_id))? {
        if (key.ends_with(".json") || key.ends_with(".cbor.zst.enc"))
            && !live_snapshots.contains(&key)
        {
            write_remote_gc_tombstone(remote, host_id, &key)?;
            remote.delete(&key)?;
            removed += 1;
        }
    }
    for key in remote.list(&host_ops_prefix(host_id))? {
        if (key.ends_with(".json") || key.ends_with(".cbor.zst.enc")) && !live_ops.contains(&key) {
            write_remote_gc_tombstone(remote, host_id, &key)?;
            remote.delete(&key)?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn prune_remote_packed_blob_objects(
    remote: &RemoteStore,
    _host_id: &str,
    export: &MetadataExport,
    max_parallel_deletes: usize,
) -> Result<usize> {
    if env::var("MAJUTSU_SYNC_REMOTE_OBJECT_PRUNE").as_deref() == Ok("0") {
        return Ok(0);
    }
    let protected = remote_gc_mark_live_keys(remote)?;
    let candidates = export
        .blobs
        .iter()
        .filter(|blob| blob.pack_id.is_some())
        .flat_map(|blob| {
            let mut keys = vec![blob.object_key.clone()];
            keys.extend(canonical_remote_aliases(&blob.object_key));
            keys
        })
        .filter(|key| !protected.contains(key))
        .collect::<BTreeSet<_>>();
    let remote_blob_keys = remote
        .list("objects/blobs/")?
        .into_iter()
        .chain(remote.list("blobs/loose/")?)
        .collect::<BTreeSet<_>>();
    let delete_keys = candidates
        .into_iter()
        .filter(|key| remote_blob_keys.contains(key))
        .collect::<Vec<_>>();
    delete_remote_keys(remote, &delete_keys, max_parallel_deletes)?;
    Ok(delete_keys.len())
}

fn prune_remote_legacy_canonical_objects(
    remote: &RemoteStore,
    max_parallel_deletes: usize,
) -> Result<usize> {
    if !remote_prefers_canonical_only(remote) {
        return Ok(0);
    }
    if env::var("MAJUTSU_SYNC_REMOTE_LEGACY_OBJECT_PRUNE").as_deref() == Ok("0") {
        return Ok(0);
    }
    let protected = remote_gc_mark_live_keys(remote)?;
    let canonical_keys = list_remote_canonical_content_aliases(remote)?;
    let mut candidates = BTreeSet::new();
    for prefix in [
        "objects/blobs/",
        "objects/trees/",
        "objects/packs/",
        "objects/indexes/pack/",
        "objects/large/manifests/",
        "objects/large/chunks/fixed/",
        "objects/large/chunks/fastcdc/",
    ] {
        for key in remote.list(prefix)? {
            if protected.contains(&key) {
                continue;
            }
            let Some(alias) = canonical_alias_for_legacy_key(&key) else {
                continue;
            };
            if canonical_keys.contains(&alias) {
                candidates.insert(key);
            }
        }
    }
    let delete_keys = candidates.into_iter().collect::<Vec<_>>();
    delete_remote_keys(remote, &delete_keys, max_parallel_deletes)?;
    Ok(delete_keys.len())
}

fn list_remote_canonical_content_aliases(remote: &RemoteStore) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for prefix in [
        "blobs/loose/",
        "trees/",
        "packs/small/",
        "packs/normal/",
        "indexes/pack-index/",
        "large/manifests/",
        "large/chunks/fixed-8m/",
        "large/chunks/fastcdc/",
    ] {
        keys.extend(remote.list(prefix)?);
    }
    Ok(keys)
}

fn prune_local_packed_blob_objects(
    paths: &Paths,
    export: &MetadataExport,
    cutoff: SystemTime,
) -> Result<usize> {
    if env::var("MAJUTSU_SYNC_LOCAL_OBJECT_PRUNE").as_deref() == Ok("0") {
        return Ok(0);
    }
    let all_local_packs_present = export.packs.iter().all(|pack| {
        paths.home.join(&pack.pack_key).exists() && paths.home.join(&pack.index_key).exists()
    });
    if !all_local_packs_present {
        return Ok(0);
    }
    let referenced_blob_dir_keys = local_object_keys(paths, export)?
        .into_iter()
        .filter(|key| key.starts_with("objects/blobs/"))
        .collect::<BTreeSet<_>>();
    let mut removed = 0usize;
    let blob_root = paths.home.join("objects/blobs");
    if !blob_root.exists() {
        return Ok(0);
    }
    for entry in walkdir::WalkDir::new(&blob_root).sort_by_file_name() {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let key = crate::util::path_to_slash(entry.path().strip_prefix(&paths.home)?);
        if referenced_blob_dir_keys.contains(key.as_str()) {
            continue;
        }
        if entry
            .metadata()?
            .modified()
            .is_ok_and(|modified| modified > cutoff)
        {
            continue;
        }
        fs::remove_file(entry.path()).with_context(|| format!("remove {key}"))?;
        removed += 1;
    }
    Ok(removed)
}

fn local_prune_cutoff_time() -> SystemTime {
    let local_prune_min_age_secs = env::var("MAJUTSU_SYNC_LOCAL_PRUNE_MIN_AGE_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60);
    SystemTime::now()
        .checked_sub(Duration::from_secs(local_prune_min_age_secs))
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

fn delete_remote_keys(
    remote: &RemoteStore,
    keys: &[String],
    max_parallel_deletes: usize,
) -> Result<()> {
    let parallelism = max_parallel_deletes.max(1);
    for batch in keys.chunks(parallelism) {
        std::thread::scope(|scope| {
            let handles = batch
                .iter()
                .map(|key| scope.spawn(move || remote.delete(key).with_context(|| key.clone())))
                .collect::<Vec<_>>();
            for handle in handles {
                handle
                    .join()
                    .map_err(|_| anyhow!("remote delete worker panicked"))??;
            }
            Ok::<_, anyhow::Error>(())
        })?;
    }
    Ok(())
}

fn remote_gc_mark_live_keys(remote: &RemoteStore) -> Result<BTreeSet<String>> {
    let mut live = BTreeSet::new();
    for key in remote.list("gc/marks/")? {
        if !key.ends_with(".json") {
            continue;
        }
        let mark: GcMarkExport = serde_json::from_slice(&remote.get(&key)?)
            .with_context(|| format!("decode remote gc mark {key}"))?;
        live.extend(mark.object_keys);
    }
    Ok(live)
}

fn write_remote_gc_tombstone(remote: &RemoteStore, host_id: &str, key: &str) -> Result<()> {
    let tombstone = GcTombstoneExport::new(host_id.to_string(), Utc::now(), key.to_string());
    remote.put(
        &remote_gc_tombstone_key(host_id, &new_id("tombstone")),
        &serde_json::to_vec_pretty(&tombstone)?,
    )
}
