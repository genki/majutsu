use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use majutsu_core::{LargeManifest, OperationLogEntry as OperationExport, TreeManifest};
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
use std::time::{Duration, Instant};

use crate::cli::{PackArgs, SyncArgs, SyncCommand};
use crate::config::{Config, MetadataExport, Paths, read_config};
use crate::db_refs::{
    persist_export_remote_refs, ref_value, restore_ref_value, set_ref_value, set_remote_ref_value,
};
use crate::object_paths::{
    local_object_keys, prefer_canonical_remote_only, remote_live_object_keys,
};
use crate::operation_log::{record_op_with_details, update_operation_result};
use crate::pack_runtime::pack_cmd;
use crate::process_runtime::{acquire_process_lock, process_lock_owner};
use crate::queue_runtime::{
    drain_upload_queue, enqueue_file_upload, enqueue_inline_upload, upload_queue_stats,
};
use crate::remote_runtime::read_remote_host_index;
use crate::remote_store::{RemoteStore, open_remote_with_upload_policy};
use crate::snapshot_state::current_snapshot;
use crate::util::{blake3_hex, new_id, parse_db_time};
use crate::{
    encode_object, ensure_ready, export_metadata, open_db, read_object, remote_object_available,
    remote_ref,
};

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
    if let Some(SyncCommand::Status) = args.command {
        return sync_status(paths, &conn, &remote);
    }
    if let Some(pid) = process_lock_owner(&paths.sync_lock)? {
        if args.wait {
            let target_current = current_snapshot(&conn)?.unwrap_or_else(|| "(none)".into());
            println!("sync already running pid {pid}; waiting");
            return wait_for_sync_catchup(
                paths,
                &conn,
                &remote,
                &target_current,
                args.timeout_secs,
            );
        }
        println!("sync already running pid {pid}");
        sync_status(paths, &conn, &remote)?;
        bail!("sync already running with pid {pid}; use `mj sync --wait` to wait for completion");
    }
    sync_configured_remote(paths, &conn, &config, &remote)?;
    if args.wait {
        let target_current = current_snapshot(&conn)?.unwrap_or_else(|| "(none)".into());
        wait_for_sync_catchup(paths, &conn, &remote, &target_current, args.timeout_secs)?;
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
    let _lock = acquire_process_lock(&paths.sync_lock, "sync")?;
    auto_pack_before_sync(paths, conn)?;
    let current = current_snapshot(conn)?;
    let export = export_metadata(conn, config)?;
    let sync_cache = read_remote_sync_cache(paths, remote)?;
    let state_fingerprint = remote_sync_state_fingerprint(config, &export)?;
    let upload_stats = upload_queue_stats(paths)?;
    if upload_stats.total == 0
        && sync_cache.state_fingerprint.as_deref() == Some(state_fingerprint.as_str())
    {
        println!("synced 0 objects to {}", remote.describe());
        println!("pruned_remote_exports 0");
        println!("pruned_remote_objects 0");
        println!("pruned_local_objects 0");
        return Ok(());
    }
    let previous_last_synced = ref_value(conn, "last-synced")?;
    let synced_at = Utc::now().to_rfc3339();
    set_ref_value(conn, "last-synced", &synced_at)?;
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
    let result = enqueue_and_drain_sync(paths, conn, config, remote);
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

fn wait_for_sync_catchup(
    paths: &Paths,
    conn: &Connection,
    remote: &RemoteStore,
    target_current: &str,
    timeout_secs: u64,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let status = sync_status_snapshot(paths, conn, remote)?;
        if status.is_caught_up_to(target_current) {
            print_sync_status(&status);
            if status.local_current != target_current {
                println!("wait_target_current {target_current}");
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            print_sync_status(&status);
            bail!("timed out waiting for sync target {target_current} after {timeout_secs}s");
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
) -> Result<()> {
    let export = export_metadata(conn, config)?;
    let sync_cache = read_remote_sync_cache(paths, remote)?;
    let sync_fingerprints = build_remote_sync_fingerprints(&config.host.id, &export)?;
    let state_fingerprint = remote_sync_state_fingerprint(config, &export)?;
    enqueue_inline_upload(
        paths,
        "metadata/export.json",
        serde_json::to_vec_pretty(&export)?,
    )?;
    let host_metadata = host_metadata_key(&config.host.id);
    enqueue_inline_upload(paths, &host_metadata, serde_json::to_vec_pretty(&export)?)?;
    for snapshot in &export.snapshots {
        enqueue_snapshot_uploads_if_needed(paths, remote, &sync_cache, &config.host.id, snapshot)?;
    }
    for operation in &export.operations {
        enqueue_operation_uploads_if_needed(
            paths,
            remote,
            &sync_cache,
            &config.host.id,
            operation,
        )?;
    }
    enqueue_inline_upload(
        paths,
        &host_oplog_key(&config.host.id),
        majutsu_core::encode_operation_log(&export.operations)?,
    )?;
    enqueue_inline_upload(
        paths,
        &host_oplog_canonical_key(&config.host.id),
        encode_canonical_remote_oplog(paths, &export.operations)?,
    )?;
    enqueue_inline_upload(
        paths,
        "config.toml",
        toml::to_string_pretty(config)?.into_bytes(),
    )?;
    enqueue_inline_upload(
        paths,
        "host.toml",
        toml::to_string_pretty(&config.host)?.into_bytes(),
    )?;
    if let Some(current) = export.refs.get("current") {
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
    if let Some(last_synced) = export.refs.get("last-synced") {
        enqueue_inline_upload(
            paths,
            &host_last_synced_ref_key(&config.host.id),
            last_synced.as_bytes().to_vec(),
        )?;
    }
    let host_index = update_remote_host_index(remote, config, &export, &host_metadata)?;
    enqueue_inline_upload(
        paths,
        REMOTE_HOST_INDEX_KEY,
        serde_json::to_vec_pretty(&host_index)?,
    )?;
    enqueue_inline_upload(
        paths,
        &remote_gc_mark_key(&config.host.id),
        serde_json::to_vec_pretty(&build_gc_mark_export(config, &export))?,
    )?;
    let recipients = paths.home.join("keys/recipients.toml");
    if recipients.exists() {
        enqueue_file_upload(paths, "keys/recipients.toml", &recipients)?;
    }
    enqueue_inline_upload(
        paths,
        REMOTE_CHUNK_INDEX_SHARD_KEY,
        encode_canonical_remote_export(paths, &build_chunk_index_shard(&export))?,
    )?;

    for key in local_object_keys(&export) {
        let local = paths.home.join(&key);
        if local.exists() {
            if !prefer_canonical_remote_only(&key) {
                enqueue_file_upload_if_needed(paths, remote, &key, &local)?;
            }
            for alias in canonical_remote_aliases(&key) {
                if canonical_alias_uses_structured_encoding(&key) {
                    enqueue_inline_upload_if_needed(paths, remote, &alias, || {
                        encode_canonical_local_object(paths, &key)
                    })?;
                } else {
                    enqueue_file_upload_if_needed(paths, remote, &alias, &local)?;
                }
            }
        }
    }
    let uploaded = drain_upload_queue(paths, remote)?;
    write_remote_sync_cache(paths, remote, sync_fingerprints, state_fingerprint)?;
    let pruned_remote_exports = prune_remote_host_exports(remote, &config.host.id, &export)?;
    let pruned_remote_objects = prune_remote_packed_blob_objects(
        remote,
        &config.host.id,
        &export,
        config.large.max_parallel_uploads,
    )?;
    let pruned_local_objects = prune_local_packed_blob_objects(paths, &export)?;
    persist_export_remote_refs(conn, &remote.describe(), &config.host.id, &export.refs)?;
    println!("synced {} objects to {}", uploaded, remote.describe());
    println!("pruned_remote_exports {}", pruned_remote_exports);
    println!("pruned_remote_objects {}", pruned_remote_objects);
    println!("pruned_local_objects {}", pruned_local_objects);
    Ok(())
}

fn enqueue_inline_upload_if_needed<F>(
    paths: &Paths,
    remote: &RemoteStore,
    key: &str,
    payload: F,
) -> Result<()>
where
    F: FnOnce() -> Result<Vec<u8>>,
{
    if remote_key_can_be_skipped(remote, key) {
        return Ok(());
    }
    enqueue_inline_upload(paths, key, payload()?)
}

fn enqueue_snapshot_uploads_if_needed(
    paths: &Paths,
    remote: &RemoteStore,
    cache: &RemoteSyncCache,
    host_id: &str,
    snapshot: &majutsu_core::SnapshotExport,
) -> Result<()> {
    let legacy_key = host_snapshot_key(host_id, &snapshot.id);
    let canonical_key = host_snapshot_canonical_key(host_id, &snapshot.id);
    let legacy_bytes = serde_json::to_vec_pretty(snapshot)?;
    let fingerprint = payload_fingerprint(&legacy_bytes);
    if cache_matches(cache, &legacy_key, &fingerprint)
        && cache_matches(cache, &canonical_key, &fingerprint)
    {
        return Ok(());
    }
    if remote_bytes_match(remote, &legacy_key, &legacy_bytes)
        && remote_key_exists(remote, &canonical_key)
    {
        return Ok(());
    }
    enqueue_inline_upload(paths, &legacy_key, legacy_bytes)?;
    enqueue_inline_upload(
        paths,
        &canonical_key,
        encode_canonical_remote_export(paths, snapshot)?,
    )
}

fn enqueue_operation_uploads_if_needed(
    paths: &Paths,
    remote: &RemoteStore,
    cache: &RemoteSyncCache,
    host_id: &str,
    operation: &OperationExport,
) -> Result<()> {
    let legacy_key = host_operation_key(host_id, &operation.id);
    let canonical_key = host_operation_canonical_key(host_id, &operation.id);
    let legacy_bytes = serde_json::to_vec_pretty(operation)?;
    let fingerprint = payload_fingerprint(&legacy_bytes);
    if cache_matches(cache, &legacy_key, &fingerprint)
        && cache_matches(cache, &canonical_key, &fingerprint)
    {
        return Ok(());
    }
    if remote_bytes_match(remote, &legacy_key, &legacy_bytes)
        && remote_key_exists(remote, &canonical_key)
    {
        return Ok(());
    }
    enqueue_inline_upload(paths, &legacy_key, legacy_bytes)?;
    enqueue_inline_upload(
        paths,
        &canonical_key,
        encode_canonical_remote_export(paths, operation)?,
    )
}

fn enqueue_file_upload_if_needed(
    paths: &Paths,
    remote: &RemoteStore,
    key: &str,
    source: &std::path::Path,
) -> Result<()> {
    if remote_key_can_be_skipped(remote, key) {
        return Ok(());
    }
    enqueue_file_upload(paths, key, source)
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

fn remote_bytes_match(remote: &RemoteStore, key: &str, expected: &[u8]) -> bool {
    if !remote_key_exists(remote, key) {
        return false;
    }
    remote
        .get(key)
        .map(|actual| actual == expected)
        .unwrap_or(false)
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
        let fingerprint = payload_fingerprint(&serde_json::to_vec_pretty(snapshot)?);
        entries.insert(
            host_snapshot_key(host_id, &snapshot.id),
            fingerprint.clone(),
        );
        entries.insert(
            host_snapshot_canonical_key(host_id, &snapshot.id),
            fingerprint,
        );
    }
    for operation in &export.operations {
        let fingerprint = payload_fingerprint(&serde_json::to_vec_pretty(operation)?);
        entries.insert(
            host_operation_key(host_id, &operation.id),
            fingerprint.clone(),
        );
        entries.insert(
            host_operation_canonical_key(host_id, &operation.id),
            fingerprint,
        );
    }
    Ok(entries)
}

fn remote_sync_state_fingerprint(config: &Config, export: &MetadataExport) -> Result<String> {
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
    let value = serde_json::json!({
        "config": config,
        "export": export_value,
    });
    Ok(payload_fingerprint(&serde_json::to_vec(&value)?))
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

impl SyncStatusSnapshot {
    fn is_caught_up_to(&self, target_current: &str) -> bool {
        if target_current == "(none)" {
            return self.local_current == self.remote_current
                && self.missing_remote_objects == 0
                && self.queued_uploads == 0;
        }
        if self.remote_current != target_current {
            return false;
        }
        if self.local_current == target_current {
            return self.missing_remote_objects == 0 && self.queued_uploads == 0;
        }
        true
    }
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
    let export = export_metadata(conn, &read_config(paths)?)?;
    let local_keys = local_object_keys(&export);
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

fn build_gc_mark_export(config: &Config, export: &MetadataExport) -> GcMarkExport {
    GcMarkExport::new(
        config.host.id.clone(),
        Utc::now(),
        export.refs.get("current").cloned(),
        remote_live_object_keys(export),
    )
}

fn encode_canonical_local_object(paths: &Paths, key: &str) -> Result<Vec<u8>> {
    let bytes = read_object(paths, key)?;
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
    key.starts_with("objects/trees/")
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

fn prune_local_packed_blob_objects(paths: &Paths, export: &MetadataExport) -> Result<usize> {
    if env::var("MAJUTSU_SYNC_LOCAL_OBJECT_PRUNE").as_deref() == Ok("0") {
        return Ok(0);
    }
    let local_pack_keys = export
        .packs
        .iter()
        .filter(|pack| {
            paths.home.join(&pack.pack_key).exists() && paths.home.join(&pack.index_key).exists()
        })
        .map(|pack| pack.pack_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut removed = 0usize;
    for blob in &export.blobs {
        let Some(pack_id) = blob.pack_id.as_deref() else {
            continue;
        };
        if !local_pack_keys.contains(pack_id) {
            continue;
        }
        let local = paths.home.join(&blob.object_key);
        if local.exists() {
            fs::remove_file(&local).with_context(|| format!("remove {}", blob.object_key))?;
            removed += 1;
        }
    }
    Ok(removed)
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
