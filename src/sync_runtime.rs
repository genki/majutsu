use crate::majutsu_core::{
    FileRecord, LargeManifest, OperationLogEntry as OperationExport, SnapshotManifest,
    TreeManifest, TreeNodeManifest,
};
use crate::majutsu_pack::PackIndex;
use crate::majutsu_store::{
    REMOTE_CHUNK_INDEX_SHARD_KEY, RemoteChunkIndexEntry as ChunkIndexEntry,
    RemoteChunkIndexShard as ChunkIndexShard, RemoteGcMark as GcMarkExport,
    RemoteGcTombstone as GcTombstoneExport, RemoteHostIndex, canonical_remote_alias,
    canonical_remote_aliases, host_current_ref_key, host_last_synced_ref_key, host_metadata_key,
    host_operation_canonical_key, host_operation_key, host_oplog_canonical_key, host_oplog_key,
    host_ops_prefix, host_remote_key, host_root_ack_ref_key, host_snapshot_canonical_key,
    host_snapshot_key, host_snapshots_prefix, is_content_addressed_remote_key, remote_gc_mark_key,
    remote_gc_tombstone_key, remote_host_label,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::time::{Duration, Instant, SystemTime};

use crate::cache_runtime::{
    prune_synced_metadata_cache, prune_synced_payload_cache, remote_payload_index_contains,
    remote_payload_key_index_for_host,
};
use crate::cli::{PackArgs, SyncArgs, SyncCommand};
use crate::config::{Config, MetadataExport, Paths, read_config};
use crate::db_refs::{
    persist_export_remote_refs, ref_value, restore_ref_value, set_ref_value, set_remote_ref_value,
};
use crate::object_paths::{
    canonical_alias_for_legacy_key, local_object_keys, prefer_s3_canonical_remote_only,
    remote_live_object_keys_for_local, s3_remote_live_object_keys_for_local,
};
use crate::operation_log::{OperationDetails, record_op_with_details, update_operation_result};
use crate::pack_runtime::pack_cmd;
use crate::process_runtime::{acquire_process_lock, process_lock_owner};
use crate::queue_runtime::{
    acknowledge_remote_event_journal_uploads, drain_upload_queue, enqueue_file_upload,
    enqueue_file_upload_overwrite, enqueue_inline_upload, enqueue_inline_upload_overwrite,
    enqueue_live_diff_event_journals, enqueue_remote_event_journal_uploads, event_journal_records,
    event_journal_stats, remote_event_journal_stats, upload_queue_contains_key, upload_queue_stats,
};
use crate::remote_runtime::repair_missing_referenced_objects;
use crate::remote_store::{RemoteStore, RemoteTrafficTraceGuard, open_remote_with_upload_policy};
use crate::root_size_summary::{
    RootSizeSummary, build_root_size_summary, encode_root_size_summary, root_size_summary_key,
    write_cached_root_size_summary,
};
use crate::root_state::roots;
use crate::snapshot_state::{current_snapshot, load_snapshot_by_id};
use crate::util::{REMOTE_HEAD_DECODE_LIMIT, blake3_hex, new_id, zstd_decode_all_limited};
use crate::{
    decode_object, encode_object, ensure_ready, export_metadata, open_db, read_object,
    remote_object_available_for_paths, remote_ref,
};

const REMOTE_SYNC_CACHE_VERSION: u32 = 2;
const REMOTE_SYNC_STATE_VERSION: &str = "remote-metadata-v9-host-scoped";
const REMOTE_HEAD_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteHeadExport {
    version: u32,
    host_id: String,
    host_name: String,
    current_snapshot: Option<String>,
    last_synced: Option<String>,
    #[serde(default)]
    root_acks: BTreeMap<String, RemoteRootAck>,
    metadata_key: String,
    host_index_key: String,
    gc_mark_key: String,
    latest_snapshot_key: Option<String>,
    latest_operation_key: Option<String>,
    updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteRootAck {
    snapshot_id: String,
    tree_id: String,
    tree_key: String,
    file_count: usize,
    synced_at: Option<String>,
}

fn remote_head_key(host_id: &str) -> String {
    format!("{host_id}/head.cbor.zst.enc")
}

fn build_remote_head_export(
    paths: &Paths,
    config: &Config,
    host_label: &str,
    export: &MetadataExport,
    metadata_key: &str,
) -> Result<RemoteHeadExport> {
    let current_snapshot = export.refs.get("current").cloned();
    let latest_snapshot_key = current_snapshot
        .as_ref()
        .map(|snapshot_id| host_snapshot_canonical_key(host_label, snapshot_id));
    let latest_operation_key = export
        .operations
        .last()
        .map(|operation| host_operation_canonical_key(host_label, &operation.id));
    let root_acks = build_remote_root_acks(paths, export)?;
    Ok(RemoteHeadExport {
        version: REMOTE_HEAD_VERSION,
        host_id: config.host.id.clone(),
        host_name: config.host.name.clone(),
        current_snapshot,
        last_synced: export.refs.get("last-synced").cloned(),
        root_acks,
        metadata_key: metadata_key.to_string(),
        host_index_key: host_metadata_key(host_label),
        gc_mark_key: remote_gc_mark_key(host_label),
        latest_snapshot_key,
        latest_operation_key,
        updated_at: Utc::now().to_rfc3339(),
    })
}

fn build_remote_root_acks(
    paths: &Paths,
    export: &MetadataExport,
) -> Result<BTreeMap<String, RemoteRootAck>> {
    let Some(current) = export.refs.get("current") else {
        return Ok(BTreeMap::new());
    };
    let Some(snapshot) = export
        .snapshots
        .iter()
        .find(|snapshot| snapshot.id == *current)
    else {
        return Ok(BTreeMap::new());
    };
    let manifest = current_snapshot_manifest_for_object_keys(paths, snapshot)?;
    let synced_at = export.refs.get("last-synced").cloned();
    Ok(manifest
        .root_trees
        .into_iter()
        .map(|(root_id, root)| {
            (
                root_id,
                RemoteRootAck {
                    snapshot_id: current.clone(),
                    tree_id: root.tree_id,
                    tree_key: root.tree_key,
                    file_count: root.file_count,
                    synced_at: synced_at.clone(),
                },
            )
        })
        .collect())
}

fn persist_remote_root_acks(
    conn: &Connection,
    remote: &str,
    host_id: &str,
    root_acks: &BTreeMap<String, RemoteRootAck>,
) -> Result<()> {
    for (root_id, ack) in root_acks {
        set_remote_ref_value(
            conn,
            remote,
            &host_root_ack_ref_key(host_id, root_id),
            &serde_json::to_string(ack)?,
        )?;
    }
    Ok(())
}

fn decode_remote_head(paths: &Paths, bytes: &[u8]) -> Result<RemoteHeadExport> {
    let decoded = decode_object(paths, bytes)?;
    let decompressed =
        zstd_decode_all_limited(decoded.as_slice(), REMOTE_HEAD_DECODE_LIMIT, "remote head")?;
    Ok(serde_cbor::from_slice(&decompressed)?)
}

fn read_remote_head(
    paths: &Paths,
    remote: &RemoteStore,
    host_id: &str,
) -> Result<Option<RemoteHeadExport>> {
    let Some(bytes) = remote.get_optional(&remote_head_key(host_id))? else {
        return Ok(None);
    };
    Ok(Some(decode_remote_head(paths, &bytes)?))
}

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
            return sync_status(paths, &conn, &remote, &status_args);
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
                None,
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
            Some(std::process::id()),
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
    let _remote_trace = RemoteTrafficTraceGuard::new("sync");
    let trace = SyncTrace::new();
    let _lock = acquire_process_lock(&paths.sync_lock, "sync")?;
    trace.mark("lock");
    auto_pack_before_sync(paths, conn)?;
    trace.mark("auto pack");
    let current = current_snapshot(conn)?;
    let current_manifest = current
        .as_deref()
        .map(|id| load_snapshot_by_id(paths, conn, id))
        .transpose()?;
    let configured_roots = roots(conn)?;
    let enqueued_live_diff =
        enqueue_live_diff_event_journals(paths, current_manifest.as_ref(), &configured_roots)?;
    trace.mark("enqueue live diff journal");
    let export = export_metadata(paths, conn, config)?;
    trace.mark("export metadata");
    let sync_cache = read_remote_sync_cache(paths, remote)?;
    trace.mark("read sync cache");
    let remote_export = metadata_export_for_remote(paths, remote, export)?;
    trace.mark("prepare remote metadata");
    let state_fingerprint = remote_sync_state_cache_key(paths, config, &remote_export)?;
    let host_label = remote_host_label(&config.host.name);
    trace.mark("state fingerprint");
    let enqueued_remote_journal = enqueue_remote_event_journal_uploads(
        paths,
        config,
        &host_label,
        &configured_roots,
        current_manifest.as_ref(),
    )?;
    trace.mark("enqueue remote journal");
    let upload_stats = upload_queue_stats(paths)?;
    trace.mark("upload queue stats");
    let should_prune_remote =
        sync_remote_prune_enabled() && remote_prune_due(paths, remote, &state_fingerprint)?;
    if upload_stats.total == 0
        && sync_cache.state_fingerprint.as_deref() == Some(state_fingerprint.as_str())
        && !should_prune_remote
    {
        let pruned_local_objects =
            prune_local_packed_blob_objects(paths, &remote_export, local_prune_cutoff_time())?;
        let pruned_payload_cache = if sync_local_payload_cache_prune_enabled() {
            prune_synced_payload_cache(paths, remote, &remote_export)?
        } else {
            Default::default()
        };
        let pruned_metadata_cache = if sync_local_metadata_cache_prune_enabled() {
            prune_synced_metadata_cache(paths, remote, &remote_export)?
        } else {
            Default::default()
        };
        trace.mark("local prune");
        println!("synced 0 objects to {}", remote.describe());
        println!("live_diff_journal_enqueued {}", enqueued_live_diff);
        println!("durable_journal_enqueued 0");
        println!("durable_journal_acknowledged 0");
        println!("pruned_remote_exports 0");
        println!("pruned_remote_objects 0");
        println!("pruned_remote_journals 0");
        println!("pruned_local_objects {}", pruned_local_objects);
        println!(
            "pruned_payload_cache_objects {}",
            pruned_payload_cache.removed
        );
        println!(
            "pruned_payload_cache_bytes {}",
            pruned_payload_cache.removed_bytes
        );
        println!(
            "pruned_metadata_cache_objects {}",
            pruned_metadata_cache.metadata_removed
        );
        println!(
            "pruned_metadata_cache_bytes {}",
            pruned_metadata_cache.metadata_removed_bytes
        );
        return Ok(());
    }
    let previous_last_synced = ref_value(conn, "last-synced")?;
    let synced_at = Utc::now().to_rfc3339();
    set_ref_value(conn, "last-synced", &synced_at)?;
    trace.mark("record last synced");
    let sync_op = new_id("op");
    record_op_with_details(
        conn,
        OperationDetails {
            id: &sync_op,
            kind: "remote-sync",
            before: current.as_deref(),
            after: current.as_deref(),
            status: "running",
            message: Some("pushed metadata and objects"),
            error: None,
            remote_sync_state: Some("queued"),
            origin: None,
        },
    )?;
    trace.mark("record sync op");
    let result = enqueue_and_drain_sync(
        paths,
        conn,
        config,
        remote,
        &trace,
        enqueued_live_diff,
        enqueued_remote_journal,
    );
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
    event_journal_pending: usize,
    durable_journal_pending: usize,
    sync_lock_pid: Option<u32>,
}

impl SyncWaitStatus {
    fn is_caught_up(&self) -> bool {
        self.local_current == self.remote_current
            && self.queued_uploads == 0
            && self.durable_journal_pending == 0
            && self.sync_lock_pid.is_none()
    }
}

fn sync_wait_status(
    paths: &Paths,
    conn: &Connection,
    remote: &RemoteStore,
) -> Result<SyncWaitStatus> {
    let local_current = current_snapshot(conn)?.unwrap_or_else(|| "(none)".into());
    let config = read_config(paths)?;
    let host_label = remote_host_label(&config.host.name);
    let (remote_current, remote_last_synced) = match read_remote_head(paths, remote, &host_label)? {
        Some(head) => (head.current_snapshot, head.last_synced),
        None => {
            let canonical_current = host_current_ref_key(&host_label);
            let canonical_last_synced = host_last_synced_ref_key(&host_label);
            let remote_current = remote_ref(remote, &canonical_current)?;
            let remote_last_synced = remote_ref(remote, &canonical_last_synced)?;
            (remote_current, remote_last_synced)
        }
    };
    let upload_stats = upload_queue_stats(paths)?;
    let event_stats = event_journal_stats(paths)?;
    let remote_journal_stats = remote_event_journal_stats(paths)?;
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
        event_journal_pending: event_stats.pending,
        durable_journal_pending: remote_journal_stats.pending,
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
    println!("event_journal_pending {}", status.event_journal_pending);
    println!("durable_journal_pending {}", status.durable_journal_pending);
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
    allowed_lock_pid: Option<u32>,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let mut target_current = target_current.to_string();
    loop {
        if let Some(latest_current) = current_snapshot(conn)?
            && latest_current != target_current
        {
            println!(
                "wait_target_updated {} -> {}",
                target_current, latest_current
            );
            target_current = latest_current;
        }
        let status = sync_wait_status(paths, conn, remote)?;
        if status.is_caught_up() {
            if sync_wait_deep_repair_enabled() {
                let repair = repair_missing_referenced_objects(paths, remote)?;
                if repair.missing > 0 || repair.repaired > 0 {
                    println!("wait_remote_repair_checked {}", repair.checked);
                    println!("wait_remote_repair_total {}", repair.total);
                    println!("wait_remote_repair_missing {}", repair.missing);
                    println!("wait_remote_repair_repaired {}", repair.repaired);
                    println!("wait_remote_repair_missing_local {}", repair.missing_local);
                }
                if repair.missing_local > 0 {
                    bail!(
                        "remote is caught up but {} referenced local object(s) are unavailable for repair",
                        repair.missing_local
                    );
                }
            }
            print_sync_wait_status(&status);
            println!("wait_target_current {}", target_current);
            println!("status_mode quick");
            return Ok(());
        }
        let allowed_lock_pid = allowed_lock_pid.or(Some(std::process::id()));
        let lock_is_allowed = status
            .sync_lock_pid
            .zip(allowed_lock_pid)
            .is_some_and(|(lock_pid, allowed_pid)| lock_pid == allowed_pid);
        if (status.sync_lock_pid.is_none() || lock_is_allowed)
            && status.queued_uploads == 0
            && status.local_current != status.remote_current
        {
            println!("wait_resync {}", status.local_current);
            if lock_is_allowed {
                let _ = fs::remove_file(&paths.sync_lock);
            }
            sync_configured_remote(paths, conn, config, remote)?;
            continue;
        }
        if Instant::now() >= deadline {
            print_sync_wait_status(&status);
            println!("status_mode quick");
            bail!(
                "timed out waiting for sync target {} after {}s; local_current={} remote_current={} queued_uploads={} delayed={} event_journal_pending={} durable_journal_pending={} lock_pid={}",
                target_current,
                timeout_secs,
                status.local_current,
                status.remote_current,
                status.queued_uploads,
                status.queued_uploads_delayed,
                status.event_journal_pending,
                status.durable_journal_pending,
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
    enqueued_live_diff: usize,
    enqueued_remote_journal: usize,
) -> Result<()> {
    let local_prune_cutoff = local_prune_cutoff_time();
    let content_export = export_metadata(paths, conn, config)?;
    trace.mark("content export");
    let remote_export =
        metadata_export_for_remote(paths, remote, export_metadata(paths, conn, config)?)?;
    trace.mark("remote export");
    let sync_cache = read_remote_sync_cache(paths, remote)?;
    trace.mark("read sync cache 2");
    let host_label = remote_host_label(&config.host.name);
    let mut sync_fingerprints = build_remote_sync_fingerprints(&host_label, &remote_export)?;
    trace.mark("metadata fingerprints");
    let state_fingerprint = remote_sync_state_cache_key(paths, config, &remote_export)?;
    trace.mark("state fingerprint 2");
    let should_prune_remote =
        sync_remote_prune_enabled() && remote_prune_due(paths, remote, &state_fingerprint)?;
    let remote_metadata_json = metadata_export_json_for_remote(remote, &remote_export)?;
    trace.mark("metadata json");
    let host_metadata_plain = host_metadata_key(&host_label);
    let host_metadata =
        enqueue_metadata_uploads(paths, remote, &host_metadata_plain, &remote_metadata_json)?;
    for snapshot in &remote_export.snapshots {
        enqueue_snapshot_uploads_if_needed(paths, remote, &sync_cache, &host_label, snapshot)?;
    }
    for operation in &remote_export.operations {
        enqueue_operation_uploads_if_needed(paths, remote, &sync_cache, &host_label, operation)?;
    }
    if matches!(remote, RemoteStore::File(_)) {
        enqueue_inline_upload(
            paths,
            &host_oplog_key(&host_label),
            crate::majutsu_core::encode_operation_log(&remote_export.operations)?,
        )?;
    }
    if should_upload_full_remote_oplog(remote) {
        enqueue_inline_upload(
            paths,
            &host_oplog_canonical_key(&host_label),
            encode_canonical_remote_oplog(paths, &remote_export.operations)?,
        )?;
    }
    enqueue_cached_inline_upload(
        paths,
        &sync_cache,
        &mut sync_fingerprints,
        &host_remote_key(&host_label, "config.toml"),
        toml::to_string_pretty(config)?.into_bytes(),
    )?;
    enqueue_cached_inline_upload(
        paths,
        &sync_cache,
        &mut sync_fingerprints,
        &host_remote_key(&host_label, "host.toml"),
        toml::to_string_pretty(&config.host)?.into_bytes(),
    )?;
    if should_upload_remote_ref_objects(remote) {
        if let Some(current) = remote_export.refs.get("current") {
            enqueue_inline_upload(
                paths,
                &host_current_ref_key(&host_label),
                current.as_bytes().to_vec(),
            )?;
        }
        if let Some(last_synced) = remote_export.refs.get("last-synced") {
            enqueue_inline_upload(
                paths,
                &host_last_synced_ref_key(&host_label),
                last_synced.as_bytes().to_vec(),
            )?;
        }
    }
    let (host_index, host_index_changed) = (RemoteHostIndex::empty(Utc::now()), false);
    trace.mark("host metadata discovery");
    enqueue_remote_head_if_supported(
        paths,
        remote,
        config,
        &host_label,
        &remote_export,
        &host_metadata,
    )?;
    enqueue_clone_bootstrap_if_supported(
        paths,
        remote,
        &sync_cache,
        &mut sync_fingerprints,
        host_index,
        host_index_changed,
    )?;
    trace.mark("clone bootstrap");
    let recipients = paths.home.join("keys/recipients.toml");
    if recipients.exists() {
        enqueue_cached_inline_upload(
            paths,
            &sync_cache,
            &mut sync_fingerprints,
            &host_remote_key(&host_label, "keys/recipients.toml"),
            fs::read(&recipients)?,
        )?;
    }
    if !remote_export.chunks.is_empty() {
        enqueue_inline_upload(
            paths,
            &host_remote_key(&host_label, REMOTE_CHUNK_INDEX_SHARD_KEY),
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
            Some(list_remote_canonical_content_aliases(remote, &host_label)?)
        } else {
            None
        };
    for key in content_keys {
        let local = paths.home.join(&key);
        if !local.exists() && !crate::hydrate_local_object_from_remote(paths, &key)? {
            bail!(
                "cannot sync referenced object {key}: local object is missing and remote hydration failed"
            );
        }
        if local.exists() {
            let canonical_only =
                remote_prefers_canonical_only(remote) && s3_prefers_canonical_remote_only(&key);
            if !canonical_only {
                let force = snapshot_manifest_keys.contains(key.as_str());
                let upload_key = remote_upload_key(remote, &host_label, &key);
                enqueue_file_upload_if_needed(paths, remote, &upload_key, &local, force)?;
            }
            for alias in canonical_remote_aliases(&key) {
                let upload_alias = remote_upload_key(remote, &host_label, &alias);
                let snapshot_manifest = snapshot_manifest_keys.contains(&key);
                let mut structured_bytes = None;
                let fingerprint = if snapshot_manifest {
                    payload_fingerprint(&fs::read(&local)?)
                } else {
                    content_object_fingerprint(&alias)
                };
                sync_fingerprints.insert(upload_alias.clone(), fingerprint.clone());
                let alias_exists = existing_canonical_aliases
                    .as_ref()
                    .is_some_and(|keys| keys.contains(&upload_alias));
                if cache_matches(&sync_cache, &upload_alias, &fingerprint)
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
                    enqueue_inline_upload_overwrite(paths, &upload_alias, bytes)?;
                } else {
                    enqueue_file_upload(paths, &upload_alias, &local)?;
                }
            }
        }
    }
    trace.mark("content queue");
    enqueue_gc_mark_if_needed(paths, remote, &host_label, config, &content_export)?;
    trace.mark("gc mark");
    let mut root_size_summary_cache = None::<RootSizeSummary>;
    match build_root_size_summary(paths, config, &remote_export) {
        Ok(Some(summary)) => {
            let key = root_size_summary_key(&host_label);
            let fingerprint = payload_fingerprint(&serde_json::to_vec(&summary)?);
            sync_fingerprints.insert(key.clone(), fingerprint.clone());
            if !cache_matches(&sync_cache, &key, &fingerprint) {
                let bytes = encode_root_size_summary(paths, &summary)?;
                enqueue_inline_upload_overwrite(paths, &key, bytes)?;
            }
            root_size_summary_cache = Some(summary);
            trace.mark("root size summary");
        }
        Ok(None) => {}
        Err(err) => {
            eprintln!("warning: skipped root size summary publish: {err:#}");
        }
    }
    let upload_drain = drain_upload_queue(paths, remote, config.large.max_parallel_uploads)?;
    let acknowledged_remote_journal =
        acknowledge_remote_event_journal_uploads(paths, remote, &host_label)?;
    let compacted_event_journal = crate::queue_runtime::compact_event_journal_force(paths)?;
    if let Some(summary) = &root_size_summary_cache
        && let Err(err) = write_cached_root_size_summary(paths, summary)
    {
        eprintln!("warning: failed to update local root size summary cache: {err:#}");
    }
    trace.mark("drain queue");
    write_remote_sync_cache(paths, remote, sync_fingerprints, state_fingerprint.clone())?;
    trace.mark("write sync cache");
    let (pruned_remote_exports, pruned_remote_objects, pruned_remote_journals) =
        if should_prune_remote {
            let pruned_remote_exports =
                prune_remote_host_exports(remote, &host_label, &remote_export)?;
            let pruned_remote_journals = prune_remote_stale_event_journals(
                paths,
                remote,
                &host_label,
                config.large.max_parallel_uploads,
            )?;
            let pruned_remote_objects = prune_remote_unreferenced_content_objects(
                paths,
                remote,
                &host_label,
                &content_export,
                config.large.max_parallel_uploads,
            )? + prune_remote_legacy_canonical_objects(
                remote,
                &host_label,
                config.large.max_parallel_uploads,
            )?;
            write_remote_prune_state(paths, remote, &state_fingerprint)?;
            (
                pruned_remote_exports,
                pruned_remote_objects,
                pruned_remote_journals,
            )
        } else {
            (0, 0, 0)
        };
    let pruned_local_objects =
        prune_local_packed_blob_objects(paths, &remote_export, local_prune_cutoff)?;
    let pruned_payload_cache = if sync_local_payload_cache_prune_enabled() {
        prune_synced_payload_cache(paths, remote, &content_export)?
    } else {
        Default::default()
    };
    let pruned_metadata_cache = if sync_local_metadata_cache_prune_enabled() {
        prune_synced_metadata_cache(paths, remote, &content_export)?
    } else {
        Default::default()
    };
    if pruned_remote_objects > 0 {
        invalidate_root_size_remote_object_cache(paths)?;
    }
    trace.mark("local prune");
    persist_export_remote_refs(conn, &remote.describe(), &host_label, &remote_export.refs)?;
    if matches!(remote, RemoteStore::S3(_)) {
        persist_remote_root_acks(
            conn,
            &remote.describe(),
            &config.host.id,
            &build_remote_root_acks(paths, &remote_export)?,
        )?;
    }
    trace.mark("persist remote refs");
    println!(
        "synced {} objects to {}",
        upload_drain.uploaded,
        remote.describe()
    );
    println!("synced_bytes {}", upload_drain.uploaded_bytes);
    println!("skipped_uploads {}", upload_drain.skipped);
    println!("live_diff_journal_enqueued {}", enqueued_live_diff);
    println!("durable_journal_enqueued {}", enqueued_remote_journal);
    println!(
        "durable_journal_acknowledged {}",
        acknowledged_remote_journal
    );
    println!("event_journal_compacted {}", compacted_event_journal);
    println!("pruned_remote_exports {}", pruned_remote_exports);
    println!("pruned_remote_objects {}", pruned_remote_objects);
    println!("pruned_remote_journals {}", pruned_remote_journals);
    println!("pruned_local_objects {}", pruned_local_objects);
    println!(
        "pruned_payload_cache_objects {}",
        pruned_payload_cache.removed
    );
    println!(
        "pruned_payload_cache_bytes {}",
        pruned_payload_cache.removed_bytes
    );
    println!(
        "pruned_metadata_cache_objects {}",
        pruned_metadata_cache.metadata_removed
    );
    println!(
        "pruned_metadata_cache_bytes {}",
        pruned_metadata_cache.metadata_removed_bytes
    );
    Ok(())
}

fn remote_prefers_canonical_only(remote: &RemoteStore) -> bool {
    matches!(remote, RemoteStore::S3(_))
}

fn remote_upload_key(remote: &RemoteStore, host_id: &str, key: &str) -> String {
    if matches!(remote, RemoteStore::S3(_)) {
        host_remote_key(host_id, key)
    } else {
        key.to_string()
    }
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
    let mut referenced_blob_oids = BTreeSet::new();
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
                if let Ok(tree) = tree_manifest_for_object_keys(paths, &root_tree.tree_key) {
                    if let Some(root_node) = &tree.root_node {
                        keys.push(root_node.node_key.clone());
                        push_child_tree_node_keys(paths, &mut keys, &root_node.node_key)?;
                    }
                    for node in tree.subtree_nodes.values() {
                        keys.push(node.node_key.clone());
                        push_child_tree_node_keys(paths, &mut keys, &node.node_key)?;
                    }
                    for record in tree_entries_for_object_keys(paths, &tree)?.values() {
                        if let Some((oid, _object_key)) =
                            crate::majutsu_core::payload_blob_ref(&record.payload)
                        {
                            referenced_blob_oids.insert(oid.to_string());
                        }
                        if let Some((_, manifest_key, _)) =
                            crate::majutsu_core::payload_large_ref(&record.payload)
                        {
                            keys.push(manifest_key.to_string());
                            if let Ok(large) = large_manifest_for_object_keys(paths, manifest_key) {
                                for chunk in large.chunks {
                                    keys.push(chunk.object_key);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    for blob in export
        .blobs
        .iter()
        .filter(|blob| referenced_blob_oids.contains(blob.oid.as_str()))
    {
        if blob.pack_id.is_none() {
            keys.push(blob.object_key.clone());
        }
    }
    let live_pack_ids = export
        .blobs
        .iter()
        .filter(|blob| referenced_blob_oids.contains(blob.oid.as_str()))
        .filter_map(|blob| blob.pack_id.as_ref())
        .collect::<BTreeSet<_>>();
    for pack in export
        .packs
        .iter()
        .filter(|pack| live_pack_ids.contains(&pack.pack_id))
    {
        keys.push(pack.pack_key.clone());
        keys.push(pack.index_key.clone());
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn current_snapshot_manifest_for_object_keys(
    paths: &Paths,
    snapshot: &crate::majutsu_core::SnapshotExport,
) -> Result<SnapshotManifest> {
    if !snapshot.manifest_json.trim().is_empty() {
        return Ok(serde_json::from_str(&snapshot.manifest_json)?);
    }
    let bytes = fs::read(paths.home.join(&snapshot.manifest_key))?;
    Ok(serde_json::from_slice(&crate::decode_object(
        paths, &bytes,
    )?)?)
}

fn tree_manifest_for_object_keys(paths: &Paths, tree_key: &str) -> Result<TreeManifest> {
    let bytes = fs::read(paths.home.join(tree_key))?;
    Ok(serde_json::from_slice(&crate::decode_object(
        paths, &bytes,
    )?)?)
}

fn large_manifest_for_object_keys(paths: &Paths, manifest_key: &str) -> Result<LargeManifest> {
    let bytes = fs::read(paths.home.join(manifest_key))?;
    Ok(serde_json::from_slice(&crate::decode_object(
        paths, &bytes,
    )?)?)
}

fn tree_entries_for_object_keys(
    paths: &Paths,
    tree: &TreeManifest,
) -> Result<BTreeMap<String, FileRecord>> {
    if !tree.entries.is_empty() || tree.root_node.is_none() {
        return Ok(tree.entries.clone());
    }
    let root_node = tree.root_node.as_ref().expect("checked above");
    let bytes = fs::read(paths.home.join(&root_node.node_key))?;
    let node: TreeNodeManifest = serde_json::from_slice(&crate::decode_object(paths, &bytes)?)?;
    tree_entries_from_node_for_object_keys(paths, node)
}

fn tree_entries_from_node_for_object_keys(
    paths: &Paths,
    node: TreeNodeManifest,
) -> Result<BTreeMap<String, FileRecord>> {
    let mut entries = node.entries;
    for child in node.child_nodes.values() {
        let bytes = fs::read(paths.home.join(&child.node_key))?;
        let child_node: TreeNodeManifest =
            serde_json::from_slice(&crate::decode_object(paths, &bytes)?)?;
        entries.extend(tree_entries_from_node_for_object_keys(paths, child_node)?);
    }
    Ok(entries)
}

fn push_child_tree_node_keys(paths: &Paths, keys: &mut Vec<String>, node_key: &str) -> Result<()> {
    let bytes = fs::read(paths.home.join(node_key))?;
    let node: TreeNodeManifest = serde_json::from_slice(&crate::decode_object(paths, &bytes)?)?;
    for child in node.child_nodes.values() {
        keys.push(child.node_key.clone());
        push_child_tree_node_keys(paths, keys, &child.node_key)?;
    }
    Ok(())
}

fn sync_verify_cached_remote_aliases() -> bool {
    env::var("MAJUTSU_SYNC_VERIFY_CACHED_REMOTE_ALIASES")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn sync_remote_prune_enabled() -> bool {
    std::env::var("MAJUTSU_SYNC_REMOTE_PRUNE")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "no" | "NO"))
        .unwrap_or(true)
}

fn sync_wait_deep_repair_enabled() -> bool {
    std::env::var("MAJUTSU_SYNC_WAIT_DEEP_REPAIR")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

#[derive(Serialize, Deserialize)]
struct RemotePruneState {
    version: u32,
    remote: String,
    state_fingerprint: String,
    pruned_at_unix_secs: u64,
}

fn remote_prune_due(paths: &Paths, remote: &RemoteStore, state_fingerprint: &str) -> Result<bool> {
    if env::var("MAJUTSU_SYNC_REMOTE_PRUNE_FORCE").as_deref() == Ok("1") {
        return Ok(true);
    }
    let Some(state) = read_remote_prune_state(paths)? else {
        return Ok(true);
    };
    if state.version != 1
        || state.remote != remote.describe()
        || state.state_fingerprint != state_fingerprint
    {
        return Ok(true);
    }
    let elapsed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH + Duration::from_secs(state.pruned_at_unix_secs))
        .unwrap_or(Duration::MAX);
    Ok(elapsed >= remote_prune_interval())
}

fn remote_prune_interval() -> Duration {
    env::var("MAJUTSU_SYNC_REMOTE_PRUNE_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(3600))
}

fn remote_prune_state_path(paths: &Paths) -> std::path::PathBuf {
    paths.home.join("cache/remote-prune-state.json")
}

fn read_remote_prune_state(paths: &Paths) -> Result<Option<RemotePruneState>> {
    let path = remote_prune_state_path(paths);
    if !path.exists() {
        return Ok(None);
    }
    let state = match serde_json::from_slice(&fs::read(path)?) {
        Ok(state) => state,
        Err(_) => return Ok(None),
    };
    Ok(Some(state))
}

fn write_remote_prune_state(
    paths: &Paths,
    remote: &RemoteStore,
    state_fingerprint: &str,
) -> Result<()> {
    let path = remote_prune_state_path(paths);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    let pruned_at_unix_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let state = RemotePruneState {
        version: 1,
        remote: remote.describe(),
        state_fingerprint: state_fingerprint.to_string(),
        pruned_at_unix_secs,
    };
    fs::write(&tmp, serde_json::to_vec(&state)?)?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn invalidate_root_size_remote_object_cache(paths: &Paths) -> Result<()> {
    let path = paths.home.join("cache/root-size-remote-objects.json");
    if path.exists() {
        fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
    }
    Ok(())
}

fn sync_local_payload_cache_prune_enabled() -> bool {
    std::env::var("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn sync_local_metadata_cache_prune_enabled() -> bool {
    std::env::var("MAJUTSU_SYNC_LOCAL_METADATA_CACHE_PRUNE")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "no" | "NO"))
        .unwrap_or_else(|_| sync_local_payload_cache_prune_enabled())
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
    host_key: &str,
    metadata_json: &[u8],
) -> Result<String> {
    if !matches!(remote, RemoteStore::S3(_)) {
        enqueue_inline_upload(paths, host_key, metadata_json.to_vec())?;
        return Ok(host_key.to_string());
    }
    let compressed = zstd::stream::encode_all(metadata_json, 3)?;
    enqueue_inline_upload(paths, &compressed_metadata_key(host_key), compressed)?;
    Ok(compressed_metadata_key(host_key))
}

fn compressed_metadata_key(key: &str) -> String {
    format!("{key}.zst")
}

fn should_upload_full_remote_oplog(remote: &RemoteStore) -> bool {
    if env::var("MAJUTSU_SYNC_FULL_REMOTE_OPLOG").as_deref() == Ok("1") {
        return true;
    }
    !matches!(remote, RemoteStore::S3(_))
}

fn should_upload_remote_ref_objects(remote: &RemoteStore) -> bool {
    if env::var("MAJUTSU_SYNC_REMOTE_REF_OBJECTS").as_deref() == Ok("1") {
        return true;
    }
    !matches!(remote, RemoteStore::S3(_))
}

fn enqueue_remote_head_if_supported(
    paths: &Paths,
    remote: &RemoteStore,
    config: &Config,
    host_label: &str,
    export: &MetadataExport,
    metadata_key: &str,
) -> Result<()> {
    if !matches!(remote, RemoteStore::S3(_)) {
        return Ok(());
    }
    let key = remote_head_key(host_label);
    let head = build_remote_head_export(paths, config, host_label, export, metadata_key)?;
    enqueue_inline_upload(paths, &key, encode_canonical_remote_export(paths, &head)?)
}

fn enqueue_clone_bootstrap_if_supported(
    paths: &Paths,
    remote: &RemoteStore,
    cache: &RemoteSyncCache,
    fingerprints: &mut BTreeMap<String, String>,
    host_index: RemoteHostIndex,
    force: bool,
) -> Result<()> {
    let _ = (paths, remote, cache, fingerprints, host_index, force);
    Ok(())
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
    snapshot: &crate::majutsu_core::SnapshotExport,
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
    if upload_queue_contains_key(paths, &canonical_key) || remote_key_exists(remote, &canonical_key)
    {
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
    if upload_queue_contains_key(paths, &canonical_key) || remote_key_exists(remote, &canonical_key)
    {
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
    if !force && remote_key_can_be_skipped(paths, remote, key) {
        return Ok(());
    }
    if force {
        enqueue_file_upload_overwrite(paths, key, source)
    } else {
        enqueue_file_upload(paths, key, source)
    }
}

fn remote_key_can_be_skipped(paths: &Paths, remote: &RemoteStore, key: &str) -> bool {
    if !is_content_addressed_remote_key(key) {
        return false;
    }
    if upload_queue_contains_key(paths, key) {
        return true;
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
    if cache.version == REMOTE_SYNC_CACHE_VERSION && cache.remote == remote.describe() {
        Ok(cache)
    } else {
        Ok(empty_remote_sync_cache(remote))
    }
}

fn empty_remote_sync_cache(remote: &RemoteStore) -> RemoteSyncCache {
    RemoteSyncCache {
        version: REMOTE_SYNC_CACHE_VERSION,
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
        version: REMOTE_SYNC_CACHE_VERSION,
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

fn sync_status(
    paths: &Paths,
    conn: &Connection,
    remote: &RemoteStore,
    args: &crate::cli::SyncStatusArgs,
) -> Result<()> {
    let options = SyncStatusDeepOptions::from_args(args);
    let status = sync_status_snapshot(paths, conn, remote, &options)?;
    print_sync_status(&status);
    Ok(())
}

struct SyncStatusDeepOptions {
    sample: Option<usize>,
    deadline: Option<Instant>,
    progress: bool,
    started: Instant,
    current_only: bool,
}

impl SyncStatusDeepOptions {
    fn from_args(args: &crate::cli::SyncStatusArgs) -> Self {
        Self {
            sample: args.sample,
            deadline: args
                .timeout_secs
                .map(|timeout| Instant::now() + Duration::from_secs(timeout)),
            progress: args.progress,
            started: Instant::now(),
            current_only: false,
        }
    }

    fn timed_out(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    fn prefer_head_checks(&self) -> bool {
        self.sample.is_some() || self.deadline.is_some()
    }

    fn maybe_progress(&self, checked: usize, total: usize, source: &str) {
        if self.progress && (checked == total || checked > 0 && checked.is_multiple_of(500)) {
            eprintln!(
                "sync status progress phase=remote-objects source={source} checked={checked}/{total} elapsed_secs={}",
                self.started.elapsed().as_secs()
            );
        }
    }
}

struct SyncStatusSnapshot {
    remote: String,
    local_current: String,
    remote_current: String,
    remote_last_synced: String,
    local_objects: usize,
    remote_objects_checked: usize,
    missing_remote_objects: usize,
    missing_remote_objects_limited: bool,
    remote_object_check_source: String,
    queued_uploads: usize,
    queued_uploads_retrying: usize,
    queued_uploads_delayed: usize,
    queued_upload_next_retry_after: String,
    queued_upload_attempts: u64,
    queued_upload_max_attempts: u32,
    upload_queue_backpressure: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SyncDeepHealthSummary {
    pub(crate) local_objects: usize,
    pub(crate) checked: usize,
    pub(crate) missing: usize,
    pub(crate) limited: bool,
    pub(crate) source: String,
    pub(crate) scope: &'static str,
}

pub(crate) fn sync_deep_health_summary(
    paths: &Paths,
    conn: &Connection,
    remote: &RemoteStore,
    sample: Option<usize>,
    timeout_secs: Option<u64>,
    current_only: bool,
) -> Result<SyncDeepHealthSummary> {
    let options = SyncStatusDeepOptions {
        sample,
        deadline: timeout_secs.map(|timeout| Instant::now() + Duration::from_secs(timeout)),
        progress: false,
        started: Instant::now(),
        current_only,
    };
    let status = sync_status_snapshot(paths, conn, remote, &options)?;
    Ok(SyncDeepHealthSummary {
        local_objects: status.local_objects,
        checked: status.remote_objects_checked,
        missing: status.missing_remote_objects,
        limited: status.missing_remote_objects_limited,
        source: status.remote_object_check_source,
        scope: if current_only { "current" } else { "history" },
    })
}

fn sync_status_snapshot(
    paths: &Paths,
    conn: &Connection,
    remote: &RemoteStore,
    options: &SyncStatusDeepOptions,
) -> Result<SyncStatusSnapshot> {
    let local_current = current_snapshot(conn)?;
    let config = read_config(paths)?;
    let host_label = remote_host_label(&config.host.name);
    let canonical_current = host_current_ref_key(&host_label);
    let canonical_last_synced = host_last_synced_ref_key(&host_label);
    let (remote_current, remote_last_synced, remote_root_acks) =
        match read_remote_head(paths, remote, &host_label)? {
            Some(head) => (head.current_snapshot, head.last_synced, head.root_acks),
            None => {
                let remote_current = remote_ref(remote, &canonical_current)?;
                let remote_last_synced = remote_ref(remote, &canonical_last_synced)?;
                (remote_current, remote_last_synced, BTreeMap::new())
            }
        };
    if let Some(value) = remote_current.as_deref() {
        set_remote_ref_value(conn, &remote.describe(), &canonical_current, value)?;
    }
    if let Some(value) = remote_last_synced.as_deref() {
        set_remote_ref_value(conn, &remote.describe(), &canonical_last_synced, value)?;
    }
    persist_remote_root_acks(conn, &remote.describe(), &config.host.id, &remote_root_acks)?;
    let mut export = export_metadata(paths, conn, &read_config(paths)?)?;
    if options.current_only
        && let Some(current) = local_current.as_deref()
    {
        export.snapshots.retain(|snapshot| snapshot.id == current);
    }
    let local_keys = local_object_keys(paths, &export)?;
    let total_local_objects = local_keys.len();
    let check_limit = options
        .sample
        .unwrap_or(total_local_objects)
        .min(total_local_objects);
    let keys_to_check = local_keys.iter().take(check_limit).collect::<Vec<_>>();
    let mut remote_object_check_source = if options.prefer_head_checks() {
        "head".to_string()
    } else {
        "list".to_string()
    };
    let mut missing_remote = 0usize;
    let mut checked = 0usize;
    let remote_keys = if options.prefer_head_checks() {
        None
    } else {
        match remote_payload_key_index_for_host(remote, &host_label) {
            Ok(keys) => Some(keys),
            Err(err) => {
                eprintln!(
                    "sync status warning: remote object index unavailable; falling back to HEAD checks: {err}"
                );
                remote_object_check_source = "head".into();
                None
            }
        }
    };
    let mut limited = check_limit < total_local_objects;
    for key in keys_to_check {
        if options.timed_out() {
            limited = true;
            break;
        }
        checked += 1;
        let available = if let Some(remote_keys) = remote_keys.as_ref() {
            remote_payload_index_contains(remote_keys, key)
        } else {
            remote_object_available_for_paths(paths, remote, key)?
        };
        if !available {
            missing_remote += 1;
        }
        options.maybe_progress(checked, check_limit, &remote_object_check_source);
    }
    if options.timed_out() {
        limited = true;
    }
    let upload_stats = upload_queue_stats(paths)?;
    Ok(SyncStatusSnapshot {
        remote: remote.describe(),
        local_current: local_current.unwrap_or_else(|| "(none)".into()),
        remote_current: remote_current.unwrap_or_else(|| "(none)".into()),
        remote_last_synced: remote_last_synced.unwrap_or_else(|| "(none)".into()),
        local_objects: total_local_objects,
        remote_objects_checked: checked,
        missing_remote_objects: missing_remote,
        missing_remote_objects_limited: limited,
        remote_object_check_source,
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
    println!("remote_objects_checked {}", status.remote_objects_checked);
    println!("missing_remote_objects {}", status.missing_remote_objects);
    println!(
        "missing_remote_objects_limited {}",
        status.missing_remote_objects_limited
    );
    println!(
        "remote_object_check_source {}",
        status.remote_object_check_source
    );
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
    if status.missing_remote_objects > 0 {
        println!(
            "hint missing remote objects may be repairable from local cache; run `mj remote repair --dry-run` then `mj remote repair`"
        );
    }
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
    let cborl = crate::majutsu_core::encode_operation_log(operations)?;
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
    host_label: &str,
    config: &Config,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<GcMarkExport> {
    let mut object_keys = if remote_prefers_canonical_only(remote) {
        s3_remote_live_object_keys_for_local(paths, export)?
    } else {
        remote_live_object_keys_for_local(paths, export)?
    };
    object_keys.extend(durable_journal_live_keys(paths)?);
    if matches!(remote, RemoteStore::S3(_)) {
        object_keys = object_keys
            .into_iter()
            .map(|key| host_remote_key(host_label, &key))
            .collect();
    }
    Ok(GcMarkExport::new(
        config.host.id.clone(),
        Utc::now(),
        export.refs.get("current").cloned(),
        object_keys,
    ))
}

fn enqueue_gc_mark_if_needed(
    paths: &Paths,
    remote: &RemoteStore,
    host_label: &str,
    config: &Config,
    export: &MetadataExport,
) -> Result<()> {
    let key = remote_gc_mark_key(host_label);
    enqueue_inline_upload(
        paths,
        &key,
        serde_json::to_vec_pretty(&build_gc_mark_export(
            paths, host_label, config, remote, export,
        )?)?,
    )
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
    if key.starts_with("objects/trees/nodes/") {
        let manifest: TreeNodeManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode tree node manifest {key}"))?;
        encode_canonical_remote_export(paths, &manifest)
    } else if key.starts_with("objects/trees/") {
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
    if should_upload_full_remote_oplog(remote) {
        live_ops.insert(host_oplog_key(host_id));
        live_ops.insert(host_oplog_canonical_key(host_id));
    }
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

fn prune_remote_unreferenced_content_objects(
    paths: &Paths,
    remote: &RemoteStore,
    host_id: &str,
    export: &MetadataExport,
    max_parallel_deletes: usize,
) -> Result<usize> {
    if env::var("MAJUTSU_SYNC_REMOTE_OBJECT_PRUNE").as_deref() == Ok("0") {
        return Ok(0);
    }
    let mut live = if matches!(remote, RemoteStore::S3(_)) {
        s3_remote_live_object_keys_for_local(paths, export)?
    } else {
        remote_live_object_keys_for_local(paths, export)?
    }
    .into_iter()
    .collect::<BTreeSet<_>>();
    live.extend(durable_journal_live_keys(paths)?);
    if matches!(remote, RemoteStore::S3(_)) {
        live = live
            .into_iter()
            .map(|key| host_remote_key(host_id, &key))
            .collect();
    }
    live.extend(remote_gc_mark_live_keys(remote, host_id)?);
    let delete_keys = remote_content_object_keys(remote, host_id)?
        .into_iter()
        .filter(|key| !live.contains(key))
        .collect::<Vec<_>>();
    delete_remote_keys(remote, &delete_keys, max_parallel_deletes)?;
    Ok(delete_keys.len())
}

fn prune_remote_stale_event_journals(
    paths: &Paths,
    remote: &RemoteStore,
    host_id: &str,
    max_parallel_deletes: usize,
) -> Result<usize> {
    if env::var("MAJUTSU_SYNC_REMOTE_JOURNAL_PRUNE").as_deref() == Ok("0") {
        return Ok(0);
    }
    let live = event_journal_records(paths)?
        .into_iter()
        .filter_map(|record| record.remote_journal_key)
        .collect::<BTreeSet<_>>();
    let prefix = format!("{host_id}/journal/");
    let delete_keys = remote
        .list(&prefix)?
        .into_iter()
        .filter(|key| key.ends_with(".json") && !live.contains(key))
        .collect::<Vec<_>>();
    delete_remote_keys(remote, &delete_keys, max_parallel_deletes)?;
    Ok(delete_keys.len())
}

fn durable_journal_live_keys(paths: &Paths) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for record in event_journal_records(paths)? {
        if record.remote_journal_synced_at.is_none() && record.remote_journal_key.is_none() {
            continue;
        }
        if let Some(manifest_key) = record.durable_large_manifest_key.as_deref() {
            keys.insert(manifest_key.to_string());
            keys.extend(canonical_remote_aliases(manifest_key));
            if let Ok(manifest) =
                serde_json::from_slice::<LargeManifest>(&read_object(paths, manifest_key)?)
            {
                for chunk in manifest.chunks {
                    keys.insert(chunk.object_key.clone());
                    keys.extend(canonical_remote_aliases(&chunk.object_key));
                }
            }
        }
        let Some(key) = record.durable_payload_key else {
            continue;
        };
        keys.insert(key.clone());
        keys.extend(canonical_remote_aliases(&key));
    }
    Ok(keys)
}

fn remote_content_object_keys(remote: &RemoteStore, host_id: &str) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for prefix in [
        "blobs/loose/",
        "trees/",
        "packs/small/",
        "packs/normal/",
        "indexes/pack-index/",
        "indexes/chunk-index/",
        "large/manifests/",
        "large/chunks/fixed-8m/",
        "large/chunks/fastcdc/",
        "objects/blobs/",
        "objects/trees/",
        "objects/packs/",
        "objects/indexes/pack/",
        "objects/large/manifests/",
        "objects/large/chunks/fixed/",
        "objects/large/chunks/fastcdc/",
    ] {
        let prefix = if matches!(remote, RemoteStore::S3(_)) {
            host_remote_key(host_id, prefix)
        } else {
            prefix.to_string()
        };
        keys.extend(remote.list(&prefix)?);
    }
    Ok(keys)
}

fn prune_remote_legacy_canonical_objects(
    remote: &RemoteStore,
    host_id: &str,
    max_parallel_deletes: usize,
) -> Result<usize> {
    if !remote_prefers_canonical_only(remote) {
        return Ok(0);
    }
    if env::var("MAJUTSU_SYNC_REMOTE_LEGACY_OBJECT_PRUNE").as_deref() == Ok("0") {
        return Ok(0);
    }
    let protected = remote_gc_mark_live_keys(remote, host_id)?;
    let canonical_keys = list_remote_canonical_content_aliases(remote, host_id)?;
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

fn list_remote_canonical_content_aliases(
    remote: &RemoteStore,
    host_id: &str,
) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for prefix in [
        "blobs/loose/",
        "trees/",
        "packs/small/",
        "packs/normal/",
        "indexes/pack-index/",
        "indexes/chunk-index/",
        "large/manifests/",
        "large/chunks/fixed-8m/",
        "large/chunks/fastcdc/",
    ] {
        keys.extend(remote.list(&host_remote_key(host_id, prefix))?);
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

fn remote_gc_mark_live_keys(remote: &RemoteStore, host_id: &str) -> Result<BTreeSet<String>> {
    let mut live = BTreeSet::new();
    let key = remote_gc_mark_key(host_id);
    if remote.exists(&key)? {
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
