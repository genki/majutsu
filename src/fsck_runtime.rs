use anyhow::{Context, Result, bail};
use majutsu_core::{
    ConfigRootIssue, HistoryGraphIssue, HostFileIssue, LargeManifest, LiveMetadataReferences,
    MetadataReferenceIssue, OperationLogComparisonIssue, OperationLogEntry as OperationExport,
    OperationLogEntryIssue, SnapshotExport, SnapshotManifest, TreeManifest,
    config_root_consistency_issues, decode_operation_log, history_graph_issues, host_file_issues,
    metadata_reference_issues, operation_log_comparison_issues, operation_log_entry_matches,
    payload_blob_ref, payload_large_ref, snapshot_export_matches, snapshot_manifest_matches,
    tree_manifest_issues,
};
use majutsu_db::{
    EventJournalRecord, EventJournalRecordIssue, RemoteObjectKeyIssue, UploadQueueItem,
    UploadQueueItemIssue, local_ref_issues, remote_ref_issues,
};
use majutsu_large::{
    LargePinIssue, large_chunk_hash_matches, large_manifest_issues, large_pin_issues,
};
use majutsu_pack::{
    PackIndex, PackIndexIssue, PackObjectIssue, missing_pack_metadata_ids, pack_index_issues,
    pack_object_issues,
};
use majutsu_restore::{RestoreQueueItem, validate_relative_filter_path};
use majutsu_store::{
    BlobExport, LEGACY_METADATA_EXPORT_KEY, REMOTE_CHUNK_INDEX_SHARD_KEY, REMOTE_HOST_INDEX_KEY,
    RemoteChunkIndexEntry as ChunkIndexEntry, RemoteChunkIndexIssue,
    RemoteChunkIndexShard as ChunkIndexShard, RemoteGcMark as GcMarkExport,
    RemoteGcTombstone as GcTombstoneExport, RemoteHostIndexIssue, RemoteObjectAvailabilityIssue,
    canonical_remote_alias, canonical_remote_aliases, host_current_ref_key,
    host_last_synced_ref_key, host_legacy_current_key, host_metadata_key,
    host_operation_canonical_key, host_operation_key, host_oplog_canonical_key, host_oplog_key,
    host_ops_prefix, host_snapshot_canonical_key, host_snapshot_key, host_snapshots_prefix,
    remote_gc_mark_key, remote_gc_tombstone_prefix, remote_object_availability_issues,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::cache_runtime::{
    payload_cache_key_set, prune_synced_metadata_cache, prune_synced_payload_cache,
    remote_payload_index_contains, remote_payload_key_index,
};
use crate::cli::FsckArgs;
use crate::config::{
    Config, HostConfig, METADATA_EXPORT_VERSION, Paths, RootConfig, read_config, validate_config,
};
use crate::object_paths::{
    local_object_keys, remote_live_object_keys_for_local, s3_remote_live_object_keys_for_local,
};
use crate::operation_log::{local_oplog_path, query_operations, record_op};
use crate::remote_runtime::read_remote_host_index;
use crate::remote_store::{RemoteStore, open_remote};
use crate::root_state::roots;
use crate::snapshot_state::current_snapshot;
use crate::util::{blake3_hex, parse_db_time, parse_time};
use crate::{
    decode_canonical_remote_export, decode_canonical_remote_oplog, decode_large_chunk_stored_bytes,
    decode_object, export_metadata, open_db, packed_blob_metadata, read_blob_payload,
    read_large_chunk, read_object, remote_local_object_variants, remote_object_available,
    remote_ref,
};

#[derive(Debug, serde::Deserialize)]
struct RemoteHeadExport {
    version: u32,
    host_id: String,
    host_name: String,
    current_snapshot: Option<String>,
    last_synced: Option<String>,
    metadata_key: String,
}

fn remote_head_key(host_id: &str) -> String {
    format!("hosts/{host_id}/head.cbor.zst.enc")
}

fn read_remote_head(
    paths: &Paths,
    remote: &RemoteStore,
    host_id: &str,
) -> Result<Option<RemoteHeadExport>> {
    let Some(bytes) = remote.get_optional(&remote_head_key(host_id))? else {
        return Ok(None);
    };
    let decoded = decode_object(paths, &bytes)?;
    let decompressed = zstd::stream::decode_all(decoded.as_slice())?;
    Ok(Some(serde_cbor::from_slice(&decompressed)?))
}

pub(crate) struct FsckOptions {
    quick: bool,
    progress: bool,
    sample: Option<usize>,
    since: Option<String>,
    backfill_index: bool,
    hydrate_index_objects: bool,
    deadline: Option<Instant>,
    started: Instant,
}

impl FsckOptions {
    fn from_args(args: FsckArgs) -> Result<Self> {
        let quick = args.quick && !args.deep;
        Ok(Self {
            quick,
            progress: args.progress,
            sample: args.sample,
            since: args.since.map(|since| parse_time(&since)).transpose()?,
            backfill_index: args.backfill_index,
            hydrate_index_objects: args.hydrate_index_objects,
            deadline: args
                .timeout_secs
                .map(|timeout| Instant::now() + Duration::from_secs(timeout)),
            started: Instant::now(),
        })
    }

    fn phase(&self, name: &str) -> Result<()> {
        self.check_timeout()?;
        if self.progress {
            eprintln!(
                "fsck progress phase={name} elapsed_secs={}",
                self.started.elapsed().as_secs()
            );
        }
        Ok(())
    }

    fn tick(&self, name: &str, checked: usize, total: Option<usize>) -> Result<()> {
        self.check_timeout()?;
        if self.progress && checked > 0 && checked.is_multiple_of(500) {
            match total {
                Some(total) => eprintln!(
                    "fsck progress phase={name} checked={checked}/{total} elapsed_secs={}",
                    self.started.elapsed().as_secs()
                ),
                None => eprintln!(
                    "fsck progress phase={name} checked={checked} elapsed_secs={}",
                    self.started.elapsed().as_secs()
                ),
            }
        }
        Ok(())
    }

    fn within_sample(&self, name: &str, index: usize, total: Option<usize>) -> Result<bool> {
        self.check_timeout()?;
        let Some(sample) = self.sample else {
            return Ok(true);
        };
        if index < sample {
            return Ok(true);
        }
        if self.progress {
            match total {
                Some(total) => eprintln!(
                    "fsck progress phase={name} sampled={sample}/{total} elapsed_secs={}",
                    self.started.elapsed().as_secs()
                ),
                None => eprintln!(
                    "fsck progress phase={name} sampled={sample} elapsed_secs={}",
                    self.started.elapsed().as_secs()
                ),
            }
        }
        Ok(false)
    }

    fn check_timeout(&self) -> Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            bail!(
                "fsck stopped after {} second(s); rerun with a larger --timeout-secs or use --quick",
                self.started.elapsed().as_secs()
            );
        }
        Ok(())
    }
}

#[derive(Default)]
struct FsckScope {
    snapshot_ids: BTreeSet<String>,
    blob_oids: BTreeSet<String>,
    large_oids: BTreeSet<String>,
    chunk_oids: BTreeSet<String>,
    pack_ids: BTreeSet<String>,
}

impl FsckScope {
    fn includes_snapshot(&self, id: &str) -> bool {
        self.snapshot_ids.contains(id)
    }

    fn includes_blob(&self, oid: &str) -> bool {
        self.blob_oids.contains(oid)
    }

    fn includes_large(&self, oid: &str) -> bool {
        self.large_oids.contains(oid)
    }

    fn includes_chunk(&self, oid: &str) -> bool {
        self.chunk_oids.contains(oid)
    }

    fn includes_pack(&self, pack_id: &str) -> bool {
        self.pack_ids.contains(pack_id)
    }

    fn is_empty(&self) -> bool {
        self.snapshot_ids.is_empty()
    }
}

struct PayloadValidationContext<'a> {
    remote_payload_keys: Option<&'a BTreeSet<String>>,
    remote: Option<&'a RemoteStore>,
    payload_cache_keys: &'a BTreeSet<String>,
    scope: Option<&'a FsckScope>,
    options: &'a FsckOptions,
}

pub(crate) fn fsck(paths: &Paths, args: FsckArgs) -> Result<()> {
    let options = FsckOptions::from_args(args)?;
    crate::ensure_ready(paths)?;
    options.phase("open-db")?;
    let conn = open_db(paths)?;
    let mut missing = 0usize;
    options.phase("read-config")?;
    let config = read_config(paths)?;
    options.phase("export-metadata")?;
    let export = export_metadata(paths, &conn, &config)?;
    if options.backfill_index {
        backfill_snapshot_payload_indexes(paths, &conn, &export, &options)?;
        return Ok(());
    }
    let payload_cache_keys = payload_cache_key_set(&export);
    let scope = if !options.quick {
        build_fsck_scope(paths, &conn, &export, &options)?
    } else {
        None
    };
    let remote = config
        .remote
        .as_ref()
        .and_then(|remote_config| open_remote(remote_config).ok());
    let use_remote_payload_index =
        remote.is_some() && !options.quick && options.sample.is_none() && options.since.is_none();
    if use_remote_payload_index {
        options.phase("remote-payload-index")?;
    }
    let remote_payload_keys = if use_remote_payload_index {
        Some(remote_payload_key_index(
            remote.as_ref().expect("remote exists"),
        )?)
    } else {
        None
    };
    if let Some(remote_payload_keys) = remote_payload_keys.as_ref() {
        options.tick(
            "remote-payload-index",
            remote_payload_keys.len(),
            Some(remote_payload_keys.len()),
        )?;
    }
    let pack_key_by_id = export
        .packs
        .iter()
        .map(|pack| (pack.pack_id.as_str(), pack.pack_key.as_str()))
        .collect::<BTreeMap<_, _>>();
    let payload_context = PayloadValidationContext {
        remote_payload_keys: remote_payload_keys.as_ref(),
        remote: remote.as_ref(),
        payload_cache_keys: &payload_cache_keys,
        scope: scope.as_ref(),
        options: &options,
    };
    if !options.quick && scope.is_none() {
        options.phase("local-object-payloads")?;
        let local_keys = local_object_keys(paths, &export)?;
        for (index, key) in local_keys.iter().enumerate() {
            if !options.within_sample("local-object-payloads", index, Some(local_keys.len()))? {
                break;
            }
            options.tick("local-object-payloads", index + 1, Some(local_keys.len()))?;
            let full = paths.home.join(key);
            if !full.exists() {
                if !payload_cache_available_remotely(&payload_context, key)? {
                    missing += 1;
                    eprintln!("missing object {key}");
                }
            } else if let Err(err) = read_object(paths, key) {
                missing += 1;
                eprintln!("unreadable object {key}: {err}");
            }
        }
    }
    options.phase("blob-records")?;
    if !options.quick {
        validate_blob_payloads(
            paths,
            &conn,
            &pack_key_by_id,
            &payload_context,
            &mut missing,
        )?;
        validate_chunk_payloads(paths, &conn, &payload_context, &mut missing)?;
        validate_large_payloads(paths, &conn, &payload_context, &mut missing)?;
    }
    for issue in large_pin_issues(&export.large_pins, &export.large_objects) {
        missing += 1;
        match issue {
            LargePinIssue::Dangling { oid, pinned_at } => {
                eprintln!("dangling large pin {oid} pinned_at={pinned_at}");
            }
            LargePinIssue::InvalidTimestamp { oid, pinned_at } => {
                eprintln!("invalid large pin timestamp {oid} pinned_at={pinned_at}");
            }
        }
    }
    options.phase("host-and-refs")?;
    validate_host_file(paths, &config, &mut missing)?;
    validate_config_roots(paths, &conn, &config, &mut missing)?;
    validate_local_refs(&conn, &mut missing)?;
    validate_remote_refs(&conn, &config, &mut missing)?;
    validate_local_history_graph(&export, &mut missing)?;
    if !options.quick {
        if scope.is_some() && options.sample.is_some() {
            if options.progress {
                eprintln!(
                    "fsck progress phase=object-manifests skipped=scoped-sample elapsed_secs={}",
                    options.started.elapsed().as_secs()
                );
            }
        } else {
            options.phase("snapshot-manifests")?;
            validate_local_snapshot_objects(
                paths,
                &export,
                scope.as_ref(),
                &options,
                &mut missing,
            )?;
            options.phase("large-manifests")?;
            validate_local_large_manifest_objects(
                paths,
                &export,
                scope.as_ref(),
                &options,
                &mut missing,
            )?;
            options.phase("pack-objects")?;
            validate_local_pack_objects(paths, &export, &payload_context, &mut missing)?;
        }
        options.phase("metadata-references")?;
        if scope.is_none() && options.sample.is_none() {
            validate_local_metadata_references(paths, &export, &options, &mut missing)?;
        } else if options.progress {
            let reason = if scope.is_some() {
                "since-scope"
            } else {
                "sample"
            };
            eprintln!(
                "fsck progress phase=metadata-references skipped={reason} elapsed_secs={}",
                options.started.elapsed().as_secs()
            );
        }
    }
    options.phase("queues")?;
    validate_local_oplog(&conn, &mut missing)?;
    validate_upload_queue(paths, &mut missing)?;
    validate_event_queue(paths, &mut missing)?;
    validate_restore_queue(paths, &conn, &mut missing)?;
    if missing > 0 {
        bail!("fsck found {missing} problems");
    }
    let current = current_snapshot(&conn)?;
    record_op(
        &conn,
        "fsck",
        current.as_deref(),
        current.as_deref(),
        Some(if options.quick {
            "checked local state quick"
        } else {
            "checked local state"
        }),
    )?;
    if options.progress {
        eprintln!(
            "fsck progress phase=done elapsed_secs={}",
            options.started.elapsed().as_secs()
        );
    }
    if !options.quick
        && fsck_payload_cache_prune_enabled()
        && let Some(remote) = remote.as_ref()
    {
        let pruned = prune_synced_payload_cache(paths, remote, &export)?;
        if pruned.payload_removed > 0 || options.progress {
            eprintln!(
                "fsck progress phase=payload-cache-prune removed={} bytes={} elapsed_secs={}",
                pruned.payload_removed,
                pruned.payload_removed_bytes,
                options.started.elapsed().as_secs()
            );
        }
    }
    if !options.quick
        && fsck_metadata_cache_prune_enabled()
        && let Some(remote) = remote.as_ref()
    {
        let pruned = prune_synced_metadata_cache(paths, remote, &export)?;
        if pruned.metadata_removed > 0 || options.progress {
            eprintln!(
                "fsck progress phase=metadata-cache-prune removed={} bytes={} elapsed_secs={}",
                pruned.metadata_removed,
                pruned.metadata_removed_bytes,
                options.started.elapsed().as_secs()
            );
        }
    }
    println!("fsck ok");
    Ok(())
}

fn fsck_payload_cache_prune_enabled() -> bool {
    std::env::var("MAJUTSU_FSCK_PRUNE_PAYLOAD_CACHE")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "no" | "NO"))
        .unwrap_or(true)
}

fn fsck_metadata_cache_prune_enabled() -> bool {
    std::env::var("MAJUTSU_FSCK_PRUNE_METADATA_CACHE")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "no" | "NO"))
        .unwrap_or(true)
}

#[derive(Default)]
struct PayloadIndexBackfillStats {
    selected_snapshots: usize,
    indexed_snapshots: usize,
    skipped_indexed_snapshots: usize,
    skipped_missing_local_snapshots: usize,
    indexed_large_manifests: usize,
    skipped_large_manifests: usize,
    skipped_missing_local_large_manifests: usize,
}

fn backfill_snapshot_payload_indexes(
    paths: &Paths,
    conn: &Connection,
    export: &crate::MetadataExport,
    options: &FsckOptions,
) -> Result<()> {
    options.phase("backfill-index")?;
    let since = options.since.as_deref().map(parse_db_time).transpose()?;
    let mut snapshots = Vec::new();
    for snapshot in &export.snapshots {
        let created_at = parse_db_time(&snapshot.created_at)?;
        if since.as_ref().is_none_or(|since| created_at >= *since) {
            snapshots.push((created_at, snapshot));
        }
    }
    snapshots.sort_by_key(|snapshot| Reverse(snapshot.0));

    let mut stats = PayloadIndexBackfillStats::default();
    let mut large_oids = BTreeSet::new();
    let mut indexed_stmt =
        conn.prepare("select 1 from snapshot_payload_index where snapshot_id=?1")?;
    let mut payload_stmt =
        conn.prepare("select oid from snapshot_payloads where snapshot_id=?1 and kind='large'")?;

    for (_, snapshot) in snapshots {
        if indexed_stmt
            .query_row(params![snapshot.id], |_| Ok(()))
            .optional()?
            .is_some()
        {
            stats.skipped_indexed_snapshots += 1;
            let rows =
                payload_stmt.query_map(params![snapshot.id], |row| row.get::<_, String>(0))?;
            for row in rows {
                large_oids.insert(row?);
            }
            continue;
        }

        if !options.within_sample("backfill-index", stats.selected_snapshots, None)? {
            break;
        }
        stats.selected_snapshots += 1;
        options.tick("backfill-index", stats.selected_snapshots, None)?;

        let Some(manifest) =
            snapshot_manifest_for_index_backfill(paths, snapshot, options.hydrate_index_objects)?
        else {
            stats.skipped_missing_local_snapshots += 1;
            continue;
        };
        persist_snapshot_payload_index(conn, &manifest)?;
        stats.indexed_snapshots += 1;
        for record in manifest.roots.values().flatten() {
            if let Some((oid, _, _)) = payload_large_ref(&record.payload) {
                large_oids.insert(oid.to_string());
            }
        }
    }

    let mut chunk_count_stmt =
        conn.prepare("select count(*) from large_object_chunks where large_oid=?1")?;
    for large in &export.large_objects {
        if !large_oids.contains(&large.oid) {
            continue;
        }
        options.check_timeout()?;
        let indexed_chunks = chunk_count_stmt
            .query_row(params![large.oid], |row| row.get::<_, usize>(0))
            .optional()?
            .unwrap_or(0);
        if indexed_chunks == large.chunk_count {
            stats.skipped_large_manifests += 1;
            continue;
        }
        let manifest = if options.hydrate_index_objects {
            Some(
                read_object(paths, &large.manifest_key)
                    .and_then(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))?,
            )
        } else {
            read_local_object_if_present(paths, &large.manifest_key)?
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()?
        };
        let Some(manifest) = manifest else {
            stats.skipped_missing_local_large_manifests += 1;
            continue;
        };
        persist_large_object_chunk_index(conn, &manifest)?;
        stats.indexed_large_manifests += 1;
    }

    println!(
        "backfill_index_selected_snapshots {}",
        stats.selected_snapshots
    );
    println!("backfill_indexed_snapshots {}", stats.indexed_snapshots);
    println!(
        "backfill_skipped_indexed_snapshots {}",
        stats.skipped_indexed_snapshots
    );
    println!(
        "backfill_skipped_missing_local_snapshots {}",
        stats.skipped_missing_local_snapshots
    );
    println!(
        "backfill_indexed_large_manifests {}",
        stats.indexed_large_manifests
    );
    println!(
        "backfill_skipped_large_manifests {}",
        stats.skipped_large_manifests
    );
    println!(
        "backfill_skipped_missing_local_large_manifests {}",
        stats.skipped_missing_local_large_manifests
    );
    println!("backfill index ok");
    Ok(())
}

fn snapshot_manifest_for_index_backfill(
    paths: &Paths,
    snapshot: &SnapshotExport,
    hydrate_missing: bool,
) -> Result<Option<SnapshotManifest>> {
    if hydrate_missing {
        return crate::snapshot_state::snapshot_manifest_from_parts(
            paths,
            &snapshot.id,
            &snapshot.manifest_key,
            &snapshot.manifest_json,
        )
        .map(Some);
    }
    let manifest = if !snapshot.manifest_json.trim().is_empty() {
        serde_json::from_str::<SnapshotManifest>(&snapshot.manifest_json)
            .with_context(|| format!("parse snapshot manifest metadata {}", snapshot.id))?
    } else {
        let Some(bytes) = read_local_object_if_present(paths, &snapshot.manifest_key)? else {
            return Ok(None);
        };
        serde_json::from_slice::<SnapshotManifest>(&bytes)
            .with_context(|| format!("parse snapshot manifest object {}", snapshot.manifest_key))?
    };
    if !manifest.roots.is_empty() || manifest.root_trees.is_empty() {
        return Ok(Some(manifest));
    }
    Ok(None)
}

fn read_local_object_if_present(paths: &Paths, key: &str) -> Result<Option<Vec<u8>>> {
    let path = paths.home.join(key);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    decode_object(paths, &bytes).map(Some)
}

fn build_fsck_scope(
    paths: &Paths,
    conn: &Connection,
    export: &crate::MetadataExport,
    options: &FsckOptions,
) -> Result<Option<FsckScope>> {
    let Some(since) = &options.since else {
        return Ok(None);
    };
    options.phase("since-scope")?;
    let cutoff = parse_db_time(since)?;
    let mut scope = FsckScope::default();
    let mut matching_snapshots = Vec::new();
    for snapshot in &export.snapshots {
        let created_at = parse_db_time(&snapshot.created_at)?;
        if created_at >= cutoff {
            matching_snapshots.push((created_at, snapshot));
        }
    }
    matching_snapshots.sort_by_key(|snapshot| Reverse(snapshot.0));
    let mut checked = 0usize;
    for (_, snapshot) in &matching_snapshots {
        if !options.within_sample("since-scope", checked, None)? {
            break;
        }
        checked += 1;
        options.tick("since-scope", checked, None)?;
        scope.snapshot_ids.insert(snapshot.id.clone());
    }
    if let Some(indexed_scope) = build_fsck_scope_from_index(conn, &scope)? {
        if options.progress {
            eprintln!(
                "fsck progress phase=since-scope source=index snapshots={} blobs={} large={} chunks={} packs={} since={since}",
                indexed_scope.snapshot_ids.len(),
                indexed_scope.blob_oids.len(),
                indexed_scope.large_oids.len(),
                indexed_scope.chunk_oids.len(),
                indexed_scope.pack_ids.len()
            );
        }
        if indexed_scope.is_empty() {
            eprintln!("fsck since scope is empty; no snapshots at or after {since}");
        }
        return Ok(Some(indexed_scope));
    }
    if options.sample.is_some() {
        let (indexed_scope, skipped_unindexed) = build_fsck_scope_from_partial_index(conn, &scope)?;
        if options.progress {
            eprintln!(
                "fsck progress phase=since-scope source=index-partial snapshots={} skipped_unindexed_snapshots={} blobs={} large={} chunks={} packs={} since={since}",
                indexed_scope.snapshot_ids.len(),
                skipped_unindexed,
                indexed_scope.blob_oids.len(),
                indexed_scope.large_oids.len(),
                indexed_scope.chunk_oids.len(),
                indexed_scope.pack_ids.len()
            );
        }
        if skipped_unindexed > 0 {
            eprintln!(
                "fsck since sample skipped {skipped_unindexed} unindexed snapshot(s); run without --sample to backfill full scope"
            );
        }
        if indexed_scope.is_empty() {
            eprintln!(
                "fsck since indexed sample scope is empty; no indexed snapshots at or after {since}"
            );
        }
        return Ok(Some(indexed_scope));
    }
    scope.blob_oids.clear();
    scope.large_oids.clear();
    scope.chunk_oids.clear();
    scope.pack_ids.clear();
    for snapshot in &export.snapshots {
        if !scope.includes_snapshot(&snapshot.id) {
            continue;
        }
        let manifest = crate::snapshot_state::snapshot_manifest_from_parts(
            paths,
            &snapshot.id,
            &snapshot.manifest_key,
            &snapshot.manifest_json,
        )?;
        persist_snapshot_payload_index(conn, &manifest)?;
        for root_tree in manifest.root_trees.values() {
            let tree: TreeManifest = read_object(paths, &root_tree.tree_key)
                .and_then(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))?;
            for record in tree.entries.values() {
                if let Some((oid, _)) = payload_blob_ref(&record.payload) {
                    scope.blob_oids.insert(oid.to_string());
                }
                if let Some((oid, _, _)) = payload_large_ref(&record.payload) {
                    scope.large_oids.insert(oid.to_string());
                }
            }
        }
    }
    for blob in &export.blobs {
        if scope.includes_blob(&blob.oid)
            && let Some(pack_id) = &blob.pack_id
        {
            scope.pack_ids.insert(pack_id.clone());
        }
    }
    for large in &export.large_objects {
        if !scope.includes_large(&large.oid) {
            continue;
        }
        let manifest: LargeManifest = read_object(paths, &large.manifest_key)
            .and_then(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))?;
        persist_large_object_chunk_index(conn, &manifest)?;
        for chunk in manifest.chunks {
            scope.chunk_oids.insert(chunk.oid);
        }
    }
    if options.progress {
        eprintln!(
            "fsck progress phase=since-scope source=manifest snapshots={} blobs={} large={} chunks={} packs={} since={since}",
            scope.snapshot_ids.len(),
            scope.blob_oids.len(),
            scope.large_oids.len(),
            scope.chunk_oids.len(),
            scope.pack_ids.len()
        );
    }
    if scope.is_empty() {
        eprintln!("fsck since scope is empty; no snapshots at or after {since}");
    }
    Ok(Some(scope))
}

fn persist_snapshot_payload_index(conn: &Connection, manifest: &SnapshotManifest) -> Result<()> {
    conn.execute(
        "delete from snapshot_payloads where snapshot_id=?1",
        params![manifest.snapshot_id],
    )?;
    for record in manifest.roots.values().flatten() {
        if let Some((oid, _)) = payload_blob_ref(&record.payload) {
            conn.execute(
                "insert or ignore into snapshot_payloads(snapshot_id, kind, oid) values (?1, 'blob', ?2)",
                params![manifest.snapshot_id, oid],
            )?;
        } else if let Some((oid, _, _)) = payload_large_ref(&record.payload) {
            conn.execute(
                "insert or ignore into snapshot_payloads(snapshot_id, kind, oid) values (?1, 'large', ?2)",
                params![manifest.snapshot_id, oid],
            )?;
        }
    }
    conn.execute(
        "insert or replace into snapshot_payload_index(snapshot_id, indexed_at) values (?1, ?2)",
        params![manifest.snapshot_id, chrono::Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn persist_large_object_chunk_index(conn: &Connection, manifest: &LargeManifest) -> Result<()> {
    for chunk in &manifest.chunks {
        conn.execute(
            "insert or ignore into large_object_chunks(large_oid, chunk_oid) values (?1, ?2)",
            params![manifest.oid, chunk.oid],
        )?;
    }
    Ok(())
}

fn build_fsck_scope_from_index(
    conn: &Connection,
    selected: &FsckScope,
) -> Result<Option<FsckScope>> {
    if selected.snapshot_ids.is_empty() {
        return Ok(Some(FsckScope::default()));
    }
    let indexed_snapshot_ids = indexed_snapshot_ids(conn, &selected.snapshot_ids)?;
    if indexed_snapshot_ids.len() != selected.snapshot_ids.len() {
        return Ok(None);
    }
    build_fsck_scope_from_indexed_snapshot_ids(conn, indexed_snapshot_ids)
}

fn build_fsck_scope_from_partial_index(
    conn: &Connection,
    selected: &FsckScope,
) -> Result<(FsckScope, usize)> {
    let indexed_snapshot_ids = indexed_snapshot_ids(conn, &selected.snapshot_ids)?;
    let skipped_unindexed = selected
        .snapshot_ids
        .len()
        .saturating_sub(indexed_snapshot_ids.len());
    Ok((
        build_fsck_scope_from_indexed_snapshot_ids(conn, indexed_snapshot_ids)?
            .unwrap_or_else(FsckScope::default),
        skipped_unindexed,
    ))
}

fn indexed_snapshot_ids(
    conn: &Connection,
    snapshot_ids: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    let mut indexed = BTreeSet::new();
    let mut indexed_stmt =
        conn.prepare("select 1 from snapshot_payload_index where snapshot_id=?1")?;
    for snapshot_id in snapshot_ids {
        if indexed_stmt
            .query_row(params![snapshot_id], |_| Ok(()))
            .optional()?
            .is_some()
        {
            indexed.insert(snapshot_id.clone());
        }
    }
    Ok(indexed)
}

fn build_fsck_scope_from_indexed_snapshot_ids(
    conn: &Connection,
    snapshot_ids: BTreeSet<String>,
) -> Result<Option<FsckScope>> {
    let mut scope = FsckScope {
        snapshot_ids,
        ..FsckScope::default()
    };
    let mut payload_stmt =
        conn.prepare("select kind, oid from snapshot_payloads where snapshot_id=?1")?;
    for snapshot_id in &scope.snapshot_ids {
        let rows = payload_stmt.query_map(params![snapshot_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (kind, oid) = row?;
            match kind.as_str() {
                "blob" => {
                    scope.blob_oids.insert(oid);
                }
                "large" => {
                    scope.large_oids.insert(oid);
                }
                _ => return Ok(None),
            }
        }
    }
    let mut large_stmt = conn.prepare("select chunk_count from large_objects where oid=?1")?;
    let mut chunk_stmt =
        conn.prepare("select chunk_oid from large_object_chunks where large_oid=?1")?;
    for large_oid in &scope.large_oids {
        let chunk_count = large_stmt
            .query_row(params![large_oid], |row| row.get::<_, usize>(0))
            .optional()?
            .unwrap_or(0);
        let rows = chunk_stmt.query_map(params![large_oid], |row| row.get::<_, String>(0))?;
        let mut chunks = Vec::new();
        for row in rows {
            chunks.push(row?);
        }
        if chunks.len() != chunk_count {
            return Ok(None);
        }
        scope.chunk_oids.extend(chunks);
    }
    let mut blob_stmt = conn.prepare("select pack_id from blobs where oid=?1")?;
    for blob_oid in &scope.blob_oids {
        if let Some(pack_id) = blob_stmt
            .query_row(params![blob_oid], |row| row.get::<_, Option<String>>(0))
            .optional()?
            .flatten()
        {
            scope.pack_ids.insert(pack_id);
        }
    }
    Ok(Some(scope))
}

fn validate_blob_payloads(
    paths: &Paths,
    conn: &Connection,
    pack_key_by_id: &BTreeMap<&str, &str>,
    context: &PayloadValidationContext<'_>,
    missing: &mut usize,
) -> Result<()> {
    let mut stmt = conn.prepare("select oid, object_key, pack_id from blobs")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    let mut checked = 0usize;
    for row in rows {
        let (oid, key, pack_id) = row?;
        if context
            .scope
            .is_some_and(|scope| !scope.includes_blob(&oid))
        {
            continue;
        }
        if !context
            .options
            .within_sample("blob-records", checked, None)?
        {
            break;
        }
        checked += 1;
        context.options.tick("blob-records", checked, None)?;
        if let Some(pack_id) = pack_id.as_deref() {
            if let Some(pack_key) = pack_key_by_id.get(pack_id)
                && !paths.home.join(pack_key).exists()
                && payload_cache_available_remotely(context, pack_key)?
            {
                continue;
            }
            if let Err(err) = read_blob_payload(paths, conn, &oid, &key) {
                *missing += 1;
                eprintln!("unreadable packed blob {oid}: {err}");
            }
        } else if !paths.home.join(&key).exists() {
            if !payload_cache_available_remotely(context, &key)? {
                *missing += 1;
                eprintln!("missing blob {oid} {key}");
            }
        } else if let Err(err) = read_object(paths, &key) {
            *missing += 1;
            eprintln!("unreadable blob {oid} {key}: {err}");
        }
    }
    Ok(())
}

fn validate_chunk_payloads(
    paths: &Paths,
    conn: &Connection,
    context: &PayloadValidationContext<'_>,
    missing: &mut usize,
) -> Result<()> {
    let mut stmt = conn.prepare("select oid, object_key from chunks")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut checked = 0usize;
    for row in rows {
        let (oid, key) = row?;
        if context
            .scope
            .is_some_and(|scope| !scope.includes_chunk(&oid))
        {
            continue;
        }
        if !context
            .options
            .within_sample("chunk-records", checked, None)?
        {
            break;
        }
        checked += 1;
        context.options.tick("chunk-records", checked, None)?;
        if !paths.home.join(&key).exists() {
            if !payload_cache_available_remotely(context, &key)? {
                *missing += 1;
                eprintln!("missing chunk {oid} {key}");
            }
        } else if let Err(err) = read_object(paths, &key) {
            *missing += 1;
            eprintln!("unreadable chunk {oid} {key}: {err}");
        }
    }
    Ok(())
}

fn validate_large_payloads(
    paths: &Paths,
    conn: &Connection,
    context: &PayloadValidationContext<'_>,
    missing: &mut usize,
) -> Result<()> {
    let mut stmt = conn.prepare("select oid, manifest_key from large_objects")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut checked = 0usize;
    let mut checked_chunks = 0usize;
    let mut chunk_sample_exhausted = false;
    for row in rows {
        if chunk_sample_exhausted {
            break;
        }
        let (oid, manifest_key) = row?;
        if context
            .scope
            .is_some_and(|scope| !scope.includes_large(&oid))
        {
            continue;
        }
        if !context
            .options
            .within_sample("large-payloads", checked, None)?
        {
            break;
        }
        checked += 1;
        context.options.tick("large-payloads", checked, None)?;
        match read_object(paths, &manifest_key)
            .and_then(|bytes| serde_json::from_slice::<LargeManifest>(&bytes).map_err(Into::into))
        {
            Ok(manifest) => {
                for chunk in &manifest.chunks {
                    context.options.check_timeout()?;
                    if context
                        .options
                        .sample
                        .is_some_and(|sample| checked_chunks >= sample)
                    {
                        chunk_sample_exhausted = true;
                        if context.options.progress {
                            eprintln!(
                                "fsck progress phase=large-chunks sampled={} elapsed_secs={}",
                                checked_chunks,
                                context.options.started.elapsed().as_secs()
                            );
                        }
                        break;
                    }
                    checked_chunks += 1;
                    context.options.tick("large-chunks", checked_chunks, None)?;
                    if !paths.home.join(&chunk.object_key).exists()
                        && payload_cache_available_remotely(context, &chunk.object_key)?
                    {
                        continue;
                    }
                    match read_large_chunk(paths, chunk) {
                        Ok(bytes) if large_chunk_hash_matches(chunk, &bytes) => {}
                        Ok(_) => {
                            *missing += 1;
                            eprintln!("large chunk hash mismatch {} {}", oid, chunk.object_key);
                        }
                        Err(err) => {
                            *missing += 1;
                            eprintln!("unreadable large chunk {} {}: {err}", oid, chunk.object_key);
                        }
                    }
                }
            }
            Err(err) => {
                *missing += 1;
                eprintln!("unreadable large manifest {oid} {manifest_key}: {err}");
            }
        }
    }
    Ok(())
}

fn payload_cache_available_remotely(
    context: &PayloadValidationContext<'_>,
    key: &str,
) -> Result<bool> {
    if !context.payload_cache_keys.contains(key) {
        return Ok(false);
    }
    if let Some(remote_payload_keys) = context.remote_payload_keys {
        return Ok(remote_payload_index_contains(remote_payload_keys, key));
    }
    let Some(remote) = context.remote else {
        return Ok(false);
    };
    context.options.check_timeout()?;
    remote_object_available(remote, key)
}

fn validate_local_history_graph(export: &crate::MetadataExport, missing: &mut usize) -> Result<()> {
    for operation in &export.operations {
        validate_operation_entry(operation, missing);
    }
    for issue in history_graph_issues(&export.snapshots, &export.operations) {
        *missing += 1;
        match issue {
            HistoryGraphIssue::SnapshotSelfParent { snapshot_id } => {
                eprintln!("snapshot {snapshot_id} references itself as parent");
            }
            HistoryGraphIssue::SnapshotMissingOperation {
                snapshot_id,
                operation_id,
            } => {
                eprintln!("snapshot {snapshot_id} references missing operation {operation_id}");
            }
            HistoryGraphIssue::OperationMissingParent {
                operation_id,
                parent_id,
            } => {
                eprintln!(
                    "operation {operation_id} references missing parent operation {parent_id}"
                );
            }
            HistoryGraphIssue::OperationSelfParent { operation_id } => {
                eprintln!("operation {operation_id} references itself as parent");
            }
        }
    }
    Ok(())
}

fn validate_operation_entry(operation: &OperationExport, missing: &mut usize) {
    for issue in operation.validation_issues() {
        *missing += 1;
        match issue {
            OperationLogEntryIssue::InvalidId => {
                eprintln!("operation {} has invalid id", operation.id);
            }
            OperationLogEntryIssue::InvalidKind(kind) => {
                eprintln!("operation {} has invalid kind {kind}", operation.id);
            }
            OperationLogEntryIssue::InvalidStatus(status) => {
                eprintln!("operation {} has invalid status {status}", operation.id);
            }
            OperationLogEntryIssue::InvalidRemoteSyncState(state) => {
                eprintln!(
                    "operation {} has invalid remote_sync_state {state}",
                    operation.id
                );
            }
            OperationLogEntryIssue::FailedWithoutError => {
                eprintln!("operation {} is failed without error detail", operation.id);
            }
            OperationLogEntryIssue::RemoteSyncMissingState => {
                eprintln!(
                    "operation {} is remote-sync without remote_sync_state",
                    operation.id
                );
            }
            OperationLogEntryIssue::RemoteSyncStateMismatch {
                status,
                remote_sync_state,
            } => {
                eprintln!(
                    "operation {} remote-sync status {status} does not match remote_sync_state {remote_sync_state}",
                    operation.id
                );
            }
            OperationLogEntryIssue::EmptyActor => {
                eprintln!("operation {} has empty actor", operation.id);
            }
            OperationLogEntryIssue::InvalidCreatedAt { value, error } => {
                eprintln!(
                    "operation {} has invalid created_at {value}: {error}",
                    operation.id
                );
            }
        }
    }
}

fn validate_remote_refs(conn: &Connection, config: &Config, missing: &mut usize) -> Result<()> {
    let snapshot_ids = {
        let mut stmt = conn.prepare("select id from snapshots")?;
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<BTreeSet<_>>>()?
    };
    let mut stmt = conn.prepare(
        "select remote, name, value, observed_at from remote_refs order by remote, name",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let refs = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    for issue in remote_ref_issues(refs, &config.host.id, &snapshot_ids) {
        *missing += 1;
        eprintln!("{}", issue.message());
    }
    Ok(())
}

fn validate_local_refs(conn: &Connection, missing: &mut usize) -> Result<()> {
    let snapshot_ids = {
        let mut stmt = conn.prepare("select id from snapshots")?;
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<BTreeSet<_>>>()?
    };
    let mut stmt = conn.prepare("select name, value from refs order by name")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let refs = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    for issue in local_ref_issues(refs, &snapshot_ids) {
        *missing += 1;
        eprintln!("{}", issue.message());
    }
    Ok(())
}

fn validate_host_file(paths: &Paths, config: &Config, missing: &mut usize) -> Result<()> {
    if !paths.host.exists() {
        *missing += 1;
        eprintln!("missing host file {}", paths.host.display());
        return Ok(());
    }
    let host: HostConfig = match toml::from_str(&fs::read_to_string(&paths.host)?) {
        Ok(host) => host,
        Err(err) => {
            *missing += 1;
            eprintln!("invalid host file {}: {err}", paths.host.display());
            return Ok(());
        }
    };
    for issue in host_file_issues(&host.id, &host.name, &config.host.id, &config.host.name) {
        *missing += 1;
        match issue {
            HostFileIssue::IdMismatch {
                host_id,
                config_host_id,
            } => {
                eprintln!("host file id {host_id} does not match config host id {config_host_id}");
            }
            HostFileIssue::NameMismatch {
                host_name,
                config_host_name,
            } => {
                eprintln!(
                    "host file name {host_name} does not match config host name {config_host_name}"
                );
            }
        }
    }
    Ok(())
}

fn validate_config_roots(
    paths: &Paths,
    conn: &Connection,
    config: &Config,
    missing: &mut usize,
) -> Result<()> {
    let db_roots = roots(conn)?
        .into_iter()
        .map(|root| (root.id.clone(), root))
        .collect::<BTreeMap<_, _>>();
    let mut config_roots = BTreeMap::new();
    for config_root in &config.roots {
        if config_roots.contains_key(&config_root.id) {
            continue;
        }
        let existing = db_roots.get(&config_root.id);
        let root = match config_root.to_root_config(paths, existing) {
            Ok(root) => root,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid config root {}: {err}", config_root.id);
                continue;
            }
        };
        config_roots.insert(config_root.id.clone(), root);
    }
    let mismatched_root_ids = db_roots
        .iter()
        .filter_map(|(id, db_root)| {
            let config_root = config_roots.get(id)?;
            (!root_configs_match(db_root, config_root)).then_some(id.as_str())
        })
        .collect::<Vec<_>>();
    for issue in config_root_consistency_issues(
        config.roots.iter().map(|root| root.id.as_str()),
        config_roots.keys().map(String::as_str),
        db_roots.keys().map(String::as_str),
        mismatched_root_ids,
    ) {
        *missing += 1;
        match issue {
            ConfigRootIssue::DuplicateRootId { id } => {
                eprintln!("duplicate root id in config {id}");
            }
            ConfigRootIssue::DatabaseMissingConfig { id } => {
                eprintln!("root exists in database but not config {id}");
            }
            ConfigRootIssue::ConfigMissingDatabase { id } => {
                eprintln!("root exists in config but not database {id}");
            }
            ConfigRootIssue::ConfigMismatch { id } => {
                eprintln!("root config does not match database {id}");
            }
        }
    }
    Ok(())
}

fn root_configs_match(left: &RootConfig, right: &RootConfig) -> bool {
    left.id == right.id
        && left.name == right.name
        && left.path == right.path
        && left.include == right.include
        && left.exclude == right.exclude
        && left.follow_symlinks == right.follow_symlinks
        && left.require_mount == right.require_mount
        && left.status == right.status
        && left.snapshot_mode == right.snapshot_mode
        && left.pre_snapshot == right.pre_snapshot
        && left.post_snapshot == right.post_snapshot
        && left.snapshot_source == right.snapshot_source
        && left.application_plugin == right.application_plugin
        && left.large == right.large
}

fn validate_local_metadata_references(
    paths: &Paths,
    export: &crate::MetadataExport,
    options: &FsckOptions,
    missing: &mut usize,
) -> Result<()> {
    let mut live = LiveMetadataReferences::default();
    let mut checked = 0usize;
    for snapshot in &export.snapshots {
        if !options.within_sample("metadata-references", checked, None)? {
            break;
        }
        checked += 1;
        options.tick("metadata-references", checked, None)?;
        let manifest = match crate::snapshot_state::snapshot_manifest_from_parts(
            paths,
            &snapshot.id,
            &snapshot.manifest_key,
            &snapshot.manifest_json,
        ) {
            Ok(manifest) => manifest,
            Err(_) => continue,
        };
        let mut large_manifest_keys = live.add_snapshot_manifest(&manifest);
        for root_tree in manifest.root_trees.values() {
            let Ok(bytes) = read_object(paths, &root_tree.tree_key) else {
                continue;
            };
            let Ok(tree) = serde_json::from_slice::<TreeManifest>(&bytes) else {
                continue;
            };
            for record in tree.entries.values() {
                if let Some((oid, _)) = payload_blob_ref(&record.payload) {
                    live.blobs.insert(oid.to_string());
                } else if let Some((oid, manifest_key, _)) = payload_large_ref(&record.payload) {
                    live.large_objects.insert(oid.to_string());
                    large_manifest_keys.push(manifest_key.to_string());
                }
            }
        }
        for manifest_key in large_manifest_keys {
            let Ok(bytes) = read_object(paths, &manifest_key) else {
                continue;
            };
            let Ok(large_manifest) = serde_json::from_slice::<LargeManifest>(&bytes) else {
                continue;
            };
            live.add_large_manifest(large_manifest);
        }
    }
    let blob_oids = export
        .blobs
        .iter()
        .map(|blob| blob.oid.clone())
        .collect::<Vec<_>>();
    let large_oids = export
        .large_objects
        .iter()
        .map(|large| large.oid.clone())
        .collect::<Vec<_>>();
    let chunk_oids = export
        .chunks
        .iter()
        .map(|chunk| chunk.oid.clone())
        .collect::<Vec<_>>();
    for issue in metadata_reference_issues(
        &blob_oids,
        &large_oids,
        &chunk_oids,
        &live.blobs,
        &live.large_objects,
        &live.chunks,
    ) {
        *missing += 1;
        match issue {
            MetadataReferenceIssue::DanglingBlob { oid } => {
                eprintln!("dangling blob metadata {oid}");
            }
            MetadataReferenceIssue::DanglingLargeObject { oid } => {
                eprintln!("dangling large object metadata {oid}");
            }
            MetadataReferenceIssue::DanglingChunk { oid } => {
                eprintln!("dangling chunk metadata {oid}");
            }
        }
    }
    Ok(())
}

fn validate_local_snapshot_objects(
    paths: &Paths,
    export: &crate::MetadataExport,
    scope: Option<&FsckScope>,
    options: &FsckOptions,
    missing: &mut usize,
) -> Result<()> {
    let mut checked = 0usize;
    for snapshot in &export.snapshots {
        if scope.is_some_and(|scope| !scope.includes_snapshot(&snapshot.id)) {
            continue;
        }
        if !options.within_sample("snapshot-manifests", checked, None)? {
            break;
        }
        checked += 1;
        options.tick("snapshot-manifests", checked, None)?;
        let metadata_manifest = match crate::snapshot_state::snapshot_manifest_from_parts(
            paths,
            &snapshot.id,
            &snapshot.manifest_key,
            &snapshot.manifest_json,
        ) {
            Ok(manifest) => manifest,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid snapshot manifest metadata {}: {err}", snapshot.id);
                continue;
            }
        };
        match read_object(paths, &snapshot.manifest_key).and_then(|bytes| {
            serde_json::from_slice::<SnapshotManifest>(&bytes).map_err(Into::into)
        }) {
            Ok(local_manifest) => {
                if !snapshot_manifest_matches(&local_manifest, &metadata_manifest) {
                    *missing += 1;
                    eprintln!(
                        "snapshot manifest object does not match metadata {} {}",
                        snapshot.id, snapshot.manifest_key
                    );
                }
            }
            Err(err) => {
                *missing += 1;
                eprintln!(
                    "unreadable snapshot manifest {} {}: {err}",
                    snapshot.id, snapshot.manifest_key
                );
            }
        }
        for (root_id, root_tree) in &metadata_manifest.root_trees {
            match read_object(paths, &root_tree.tree_key).and_then(|bytes| {
                serde_json::from_slice::<TreeManifest>(&bytes).map_err(Into::into)
            }) {
                Ok(tree) => {
                    if !tree_manifest_issues(&tree, root_id, root_tree).is_empty() {
                        *missing += 1;
                        eprintln!(
                            "tree manifest object does not match snapshot metadata {} {}",
                            snapshot.id, root_tree.tree_key
                        );
                    }
                }
                Err(err) => {
                    *missing += 1;
                    eprintln!(
                        "unreadable tree manifest {} {}: {err}",
                        snapshot.id, root_tree.tree_key
                    );
                }
            }
        }
    }
    Ok(())
}

fn validate_local_large_manifest_objects(
    paths: &Paths,
    export: &crate::MetadataExport,
    scope: Option<&FsckScope>,
    options: &FsckOptions,
    missing: &mut usize,
) -> Result<()> {
    let mut checked = 0usize;
    for large in &export.large_objects {
        if scope.is_some_and(|scope| !scope.includes_large(&large.oid)) {
            continue;
        }
        if !options.within_sample("large-manifests", checked, None)? {
            break;
        }
        checked += 1;
        options.tick("large-manifests", checked, None)?;
        match read_object(paths, &large.manifest_key)
            .and_then(|bytes| serde_json::from_slice::<LargeManifest>(&bytes).map_err(Into::into))
        {
            Ok(manifest) => {
                if !large_manifest_issues(&manifest, large).is_empty() {
                    *missing += 1;
                    eprintln!(
                        "large manifest object does not match metadata {} {}",
                        large.oid, large.manifest_key
                    );
                }
            }
            Err(err) => {
                *missing += 1;
                eprintln!(
                    "unreadable large manifest {} {}: {err}",
                    large.oid, large.manifest_key
                );
            }
        }
    }
    Ok(())
}

fn validate_local_pack_objects(
    paths: &Paths,
    export: &crate::MetadataExport,
    context: &PayloadValidationContext<'_>,
    missing: &mut usize,
) -> Result<()> {
    let mut blobs_by_pack: BTreeMap<&str, BTreeMap<&str, &BlobExport>> = BTreeMap::new();
    for blob in &export.blobs {
        if let Some(pack_id) = blob.pack_id.as_deref() {
            blobs_by_pack
                .entry(pack_id)
                .or_default()
                .insert(blob.oid.as_str(), blob);
        }
    }
    let mut checked = 0usize;
    for pack in &export.packs {
        if context
            .scope
            .is_some_and(|scope| !scope.includes_pack(&pack.pack_id))
        {
            continue;
        }
        if !context
            .options
            .within_sample("pack-objects", checked, None)?
        {
            break;
        }
        checked += 1;
        context.options.tick("pack-objects", checked, None)?;
        let expected_blobs = blobs_by_pack
            .get(pack.pack_id.as_str())
            .cloned()
            .unwrap_or_default();
        let expected_blob_metadata = packed_blob_metadata(&expected_blobs);
        match read_object(paths, &pack.index_key)
            .and_then(|bytes| serde_json::from_slice::<PackIndex>(&bytes).map_err(Into::into))
        {
            Ok(index) => {
                for issue in pack_index_issues(pack, &index, &expected_blob_metadata) {
                    *missing += 1;
                    match issue {
                        PackIndexIssue::PackMetadataMismatch => {
                            eprintln!("pack index does not match metadata {}", pack.pack_id);
                        }
                        PackIndexIssue::EntryWithoutBlobMetadata { oid } => {
                            eprintln!(
                                "pack index entry has no matching blob metadata {} {}",
                                pack.pack_id, oid
                            );
                        }
                        PackIndexIssue::EntryOffsetMismatch { oid } => {
                            eprintln!(
                                "pack index entry does not match blob offset metadata {} {}",
                                pack.pack_id, oid
                            );
                        }
                        PackIndexIssue::MissingBlobEntry { oid } => {
                            eprintln!(
                                "packed blob missing from pack index {} {}",
                                pack.pack_id, oid
                            );
                        }
                    }
                }
            }
            Err(err) => {
                *missing += 1;
                eprintln!(
                    "unreadable pack index {} {}: {err}",
                    pack.pack_id, pack.index_key
                );
            }
        }
        let pack_path = paths.home.join(&pack.pack_key);
        if !pack_path.exists() && payload_cache_available_remotely(context, &pack.pack_key)? {
            continue;
        }
        match fs::read(&pack_path) {
            Ok(bytes) => {
                for issue in pack_object_issues(pack, bytes.len() as u64, &expected_blob_metadata) {
                    *missing += 1;
                    match issue {
                        PackObjectIssue::SizeMismatch => {
                            eprintln!("pack object size does not match metadata {}", pack.pack_id);
                        }
                        PackObjectIssue::MissingBlobOffset { oid } => {
                            eprintln!(
                                "packed blob missing offset metadata {} {}",
                                pack.pack_id, oid
                            );
                        }
                        PackObjectIssue::MissingBlobLength { oid } => {
                            eprintln!(
                                "packed blob missing length metadata {} {}",
                                pack.pack_id, oid
                            );
                        }
                        PackObjectIssue::BlobRangeOutOfBounds { oid } => {
                            eprintln!(
                                "packed blob range out of pack bounds {} {}",
                                pack.pack_id, oid
                            );
                        }
                    }
                }
            }
            Err(err) => {
                *missing += 1;
                eprintln!(
                    "unreadable pack object {} {}: {err}",
                    pack.pack_id, pack.pack_key
                );
            }
        }
    }
    for pack_id in missing_pack_metadata_ids(
        blobs_by_pack.keys().copied(),
        export.packs.iter().map(|pack| pack.pack_id.as_str()),
    ) {
        if context
            .scope
            .is_some_and(|scope| !scope.includes_pack(&pack_id))
        {
            continue;
        }
        *missing += 1;
        eprintln!("packed blob references missing pack metadata {pack_id}");
    }
    Ok(())
}

fn validate_event_queue(paths: &Paths, missing: &mut usize) -> Result<()> {
    let dir = &paths.event_queue;
    if !dir.exists() {
        return Ok(());
    }
    let mut seen = BTreeSet::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file()
            || entry.path().extension().and_then(OsStr::to_str) != Some("json")
        {
            continue;
        }
        let path = entry.path();
        let event: EventJournalRecord = match serde_json::from_slice(&fs::read(&path)?) {
            Ok(event) => event,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid event journal item {}: {err}", path.display());
                continue;
            }
        };
        if path.file_stem().and_then(OsStr::to_str) != Some(event.event_id.as_str()) {
            *missing += 1;
            eprintln!(
                "event journal filename does not match event id {}",
                path.display()
            );
        }
        for issue in event.validation_issues() {
            *missing += 1;
            match issue {
                EventJournalRecordIssue::EmptyEventId => {
                    eprintln!("event journal item has empty event id {}", path.display());
                }
                EventJournalRecordIssue::EmptyKind => {
                    eprintln!("event journal item has empty kind {}", event.event_id);
                }
                EventJournalRecordIssue::EmptyDetail => {
                    eprintln!("event journal item has empty detail {}", event.event_id);
                }
            }
        }
        if !seen.insert(event.event_id.clone()) {
            *missing += 1;
            eprintln!("duplicate event journal id {}", event.event_id);
        }
    }
    Ok(())
}

fn validate_upload_queue(paths: &Paths, missing: &mut usize) -> Result<()> {
    let dir = &paths.upload_queue;
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file()
            || entry.path().extension().and_then(OsStr::to_str) != Some("json")
        {
            continue;
        }
        let path = entry.path();
        let item: UploadQueueItem = match serde_json::from_slice(&fs::read(&path)?) {
            Ok(item) => item,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid upload queue item {}: {err}", path.display());
                continue;
            }
        };
        if path.file_stem().and_then(OsStr::to_str) != Some(item.id.as_str()) {
            *missing += 1;
            eprintln!(
                "upload queue filename does not match item id {}",
                path.display()
            );
        }
        let mut missing_source = None;
        if let (Some(source), None) = (&item.source, &item.inline)
            && !Path::new(source).exists()
        {
            missing_source = Some(source.as_str());
        }
        for issue in item.validation_issues() {
            *missing += 1;
            match issue {
                UploadQueueItemIssue::IdDoesNotMatchKey { expected } => {
                    eprintln!(
                        "upload queue item id does not match key {} expected {}",
                        item.id, expected
                    );
                }
                UploadQueueItemIssue::InvalidKey { reason } => {
                    let reason = match reason {
                        RemoteObjectKeyIssue::NotRelativeSlashPath => {
                            "remote object key must be a relative slash path"
                        }
                        RemoteObjectKeyIssue::UnsafeComponent => {
                            "remote object key must not contain empty, '.', or '..' components"
                        }
                    };
                    eprintln!("upload queue item has invalid key {}: {reason}", item.id);
                }
                UploadQueueItemIssue::BothSourceAndInline => {
                    eprintln!(
                        "upload queue item has both source and inline payload {}",
                        item.id
                    );
                }
                UploadQueueItemIssue::MissingPayload => {
                    eprintln!(
                        "upload queue item has neither source nor inline payload {}",
                        item.id
                    );
                }
            }
        }
        if let Some(source) = missing_source {
            *missing += 1;
            eprintln!("upload queue item source is missing {} {source}", item.id);
        }
    }
    Ok(())
}

fn validate_restore_queue(paths: &Paths, conn: &Connection, missing: &mut usize) -> Result<()> {
    let dir = paths.home.join("queue/restores");
    if !dir.exists() {
        return Ok(());
    }
    let mut stmt = conn.prepare("select id from snapshots")?;
    let snapshot_ids = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<BTreeSet<_>>>()?;
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file()
            || entry.path().extension().and_then(OsStr::to_str) != Some("json")
        {
            continue;
        }
        let path = entry.path();
        let job: RestoreQueueItem = match serde_json::from_slice(&fs::read(&path)?) {
            Ok(job) => job,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid restore queue item {}: {err}", path.display());
                continue;
            }
        };
        if path.file_stem().and_then(OsStr::to_str) != Some(job.id.as_str()) {
            *missing += 1;
            eprintln!(
                "restore queue filename does not match job id {}",
                path.display()
            );
        }
        if !snapshot_ids.contains(&job.snapshot_id) {
            *missing += 1;
            eprintln!(
                "restore queue item references missing snapshot {} {}",
                job.id, job.snapshot_id
            );
        }
        if !job.has_valid_status() {
            *missing += 1;
            eprintln!(
                "restore queue item has invalid status {} {}",
                job.id, job.status
            );
        }
        if let Some(path_filter) = job.path.as_deref()
            && let Err(err) =
                validate_relative_filter_path(Path::new(path_filter), "restore job path")
        {
            *missing += 1;
            eprintln!("restore queue item has invalid path {}: {err}", job.id);
        }
        if job.has_duplicate_required_objects() {
            *missing += 1;
            eprintln!(
                "restore queue item has duplicate required objects {}",
                job.id
            );
        }
        for key in job.pending_objects_outside_required() {
            *missing += 1;
            eprintln!(
                "restore queue item references object outside required set {} {}",
                job.id, key
            );
        }
        if job.done_with_pending_objects() {
            *missing += 1;
            eprintln!(
                "completed restore queue item still has pending objects {}",
                job.id
            );
        }
    }
    Ok(())
}

fn validate_local_oplog(conn: &Connection, missing: &mut usize) -> Result<()> {
    let expected = query_operations(conn)?;
    let Some(path) = local_oplog_path(conn)? else {
        if !expected.is_empty() {
            *missing += 1;
            eprintln!("missing local operation log path");
        }
        return Ok(());
    };
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) => {
            *missing += 1;
            eprintln!("unreadable local operation log {}: {err}", path.display());
            return Ok(());
        }
    };
    let actual = match decode_operation_log(&bytes) {
        Ok(actual) => actual,
        Err(err) => {
            *missing += 1;
            eprintln!("invalid local operation log {}: {err}", path.display());
            return Ok(());
        }
    };
    for issue in operation_log_comparison_issues(&actual, &expected) {
        *missing += 1;
        match issue {
            OperationLogComparisonIssue::CountMismatch { expected, actual } => {
                eprintln!(
                    "local operation log count mismatch {} expected={} actual={}",
                    path.display(),
                    expected,
                    actual
                );
                return Ok(());
            }
            OperationLogComparisonIssue::EntryMismatch { index, id } => {
                eprintln!("local operation log entry does not match metadata {id} index={index}");
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_remote_gc_records(
    paths: &Paths,
    remote: &RemoteStore,
    host_id: &str,
    export: &crate::MetadataExport,
    missing: &mut usize,
) -> Result<()> {
    let mark_key = remote_gc_mark_key(host_id);
    let compact_head_authoritative =
        matches!(remote, RemoteStore::S3(_)) && read_remote_head(paths, remote, host_id)?.is_some();
    if !remote.exists(&mark_key)? {
        if !compact_head_authoritative {
            *missing += 1;
            eprintln!("missing remote gc mark {mark_key}");
        }
    } else {
        let mark: GcMarkExport = match serde_json::from_slice(&remote.get(&mark_key)?) {
            Ok(mark) => mark,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid remote gc mark {mark_key}: {err}");
                return validate_remote_gc_tombstones(remote, host_id, missing);
            }
        };
        if compact_head_authoritative {
            validate_compact_remote_gc_mark(&mark_key, host_id, &mark, missing);
        } else {
            let expected = expected_gc_mark_object_keys(paths, remote, export)?;
            for issue in mark.validation_issues(host_id, export.refs.get("current"), &expected) {
                *missing += 1;
                eprintln!("{}", issue.message(&mark_key, host_id));
            }
        }
    }
    validate_remote_gc_tombstones(remote, host_id, missing)
}

fn validate_compact_remote_gc_mark(
    mark_key: &str,
    host_id: &str,
    mark: &GcMarkExport,
    missing: &mut usize,
) {
    if mark.version != 1 {
        *missing += 1;
        eprintln!("unsupported remote gc mark version {mark_key}");
    }
    if mark.host_id != host_id {
        *missing += 1;
        eprintln!(
            "remote gc mark host id {} does not match {}",
            mark.host_id, host_id
        );
    }
    if mark.has_duplicate_object_keys() {
        *missing += 1;
        eprintln!("remote gc mark contains duplicate object keys {mark_key}");
    }
}

fn expected_gc_mark_object_keys(
    paths: &Paths,
    remote: &RemoteStore,
    export: &crate::MetadataExport,
) -> Result<BTreeSet<String>> {
    let export = export_with_hydrated_remote_snapshot_manifests(paths, remote, export)?;
    if matches!(remote, RemoteStore::S3(_)) {
        return Ok(s3_remote_live_object_keys_for_local(paths, &export)?
            .into_iter()
            .collect());
    }
    Ok(remote_live_object_keys_for_local(paths, &export)?
        .into_iter()
        .collect())
}

fn export_with_hydrated_remote_snapshot_manifests(
    paths: &Paths,
    remote: &RemoteStore,
    export: &crate::MetadataExport,
) -> Result<crate::MetadataExport> {
    let mut export: crate::MetadataExport = serde_json::from_value(serde_json::to_value(export)?)?;
    for snapshot in &mut export.snapshots {
        if !snapshot.manifest_json.trim().is_empty() {
            continue;
        }
        let bytes = crate::download_local_object_from_remote(paths, remote, &snapshot.manifest_key)
            .with_context(|| {
                format!(
                    "download remote snapshot manifest {}",
                    snapshot.manifest_key
                )
            })?;
        let manifest: SnapshotManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse remote snapshot manifest {}", snapshot.manifest_key))?;
        snapshot.manifest_json = serde_json::to_string(&manifest)?;
    }
    Ok(export)
}

fn validate_remote_gc_tombstones(
    remote: &RemoteStore,
    host_id: &str,
    missing: &mut usize,
) -> Result<()> {
    let prefix = remote_gc_tombstone_prefix(host_id);
    let mut seen_keys = BTreeSet::new();
    for key in remote.list(&prefix)? {
        if !key.ends_with(".json") {
            continue;
        }
        let tombstone: GcTombstoneExport = match serde_json::from_slice(&remote.get(&key)?) {
            Ok(tombstone) => tombstone,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid remote gc tombstone {key}: {err}");
                continue;
            }
        };
        for issue in tombstone.validation_issues(host_id) {
            *missing += 1;
            eprintln!("{}", issue.message(&key, host_id));
        }
        if remote.exists(&tombstone.key)? {
            *missing += 1;
            eprintln!(
                "remote gc tombstone points to existing object {} {}",
                key, tombstone.key
            );
        }
        if !seen_keys.insert(tombstone.key.clone()) {
            *missing += 1;
            eprintln!(
                "duplicate remote gc tombstone deleted key {}",
                tombstone.key
            );
        }
    }
    Ok(())
}

pub(crate) fn validate_remote_oplog(
    paths: &Paths,
    remote: &RemoteStore,
    host_id: &str,
    expected: &[OperationExport],
    missing: &mut usize,
) -> Result<()> {
    let canonical_key = host_oplog_canonical_key(host_id);
    if !remote.exists(&canonical_key)? {
        if !matches!(remote, RemoteStore::S3(_)) {
            *missing += 1;
            eprintln!("missing canonical host operation log {canonical_key}");
        }
    } else {
        let operations = decode_canonical_remote_oplog(paths, &remote.get(&canonical_key)?)
            .map_err(|err| anyhow::anyhow!("parse remote operation log {canonical_key}: {err}"))?;
        validate_remote_oplog_entries(&canonical_key, &operations, expected, missing);
    }

    let legacy_key = host_oplog_key(host_id);
    if remote.exists(&legacy_key)? {
        let operations = decode_operation_log(&remote.get(&legacy_key)?)
            .map_err(|err| anyhow::anyhow!("parse remote operation log {legacy_key}: {err}"))?;
        validate_remote_oplog_entries(&legacy_key, &operations, expected, missing);
    }
    Ok(())
}

fn validate_remote_oplog_entries(
    key: &str,
    actual: &[OperationExport],
    expected: &[OperationExport],
    missing: &mut usize,
) {
    for issue in operation_log_comparison_issues(actual, expected) {
        *missing += 1;
        match issue {
            OperationLogComparisonIssue::CountMismatch { expected, actual } => {
                eprintln!(
                    "remote operation log count mismatch {key} expected={} actual={}",
                    expected, actual
                );
                return;
            }
            OperationLogComparisonIssue::EntryMismatch { index, id } => {
                eprintln!(
                    "remote operation log entry does not match metadata {key} {id} index={index}"
                );
            }
        }
    }
}

pub(crate) fn validate_remote_chunk_index(
    paths: &Paths,
    remote: &RemoteStore,
    export: &crate::MetadataExport,
    missing: &mut usize,
) -> Result<()> {
    if export.chunks.is_empty() {
        return Ok(());
    }
    for key in remote.list("indexes/chunk-index/")? {
        if key != REMOTE_CHUNK_INDEX_SHARD_KEY {
            *missing += 1;
            eprintln!("unexpected remote chunk index shard {key}");
        }
    }
    if !remote.exists(REMOTE_CHUNK_INDEX_SHARD_KEY)? {
        *missing += 1;
        eprintln!("missing remote chunk index shard {REMOTE_CHUNK_INDEX_SHARD_KEY}");
        return Ok(());
    }
    let shard: ChunkIndexShard = decode_canonical_remote_export(
        paths,
        &remote.get(REMOTE_CHUNK_INDEX_SHARD_KEY)?,
    )
    .map_err(|err| {
        anyhow::anyhow!("parse remote chunk index shard {REMOTE_CHUNK_INDEX_SHARD_KEY}: {err}")
    })?;
    let expected = export
        .chunks
        .iter()
        .map(|chunk| {
            let expected_canonical = canonical_remote_alias(&chunk.object_key)
                .unwrap_or_else(|| chunk.object_key.clone());
            ChunkIndexEntry::new(
                chunk.oid.clone(),
                chunk.size,
                chunk.object_key.clone(),
                Some(expected_canonical),
            )
        })
        .collect::<Vec<_>>();
    for issue in shard.validation_issues(&expected) {
        *missing += 1;
        match issue {
            RemoteChunkIndexIssue::ShardMetadataMismatch => {
                eprintln!("remote chunk index shard metadata does not match export");
                return Ok(());
            }
            RemoteChunkIndexIssue::DuplicateShardOids => {
                eprintln!("remote chunk index shard contains duplicate chunk oids");
            }
            RemoteChunkIndexIssue::UnexpectedEntry(oid) => {
                eprintln!("remote chunk index entry has no matching chunk {oid}");
            }
            RemoteChunkIndexIssue::DuplicateEntry(oid) => {
                eprintln!("duplicate remote chunk index entry {oid}");
            }
            RemoteChunkIndexIssue::EntryMismatch(oid) => {
                eprintln!("remote chunk index entry does not match metadata {oid}");
            }
            RemoteChunkIndexIssue::MissingEntry(oid) => {
                eprintln!("chunk missing from remote chunk index {oid}");
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_remote_large_manifest_objects(
    paths: &Paths,
    remote: &RemoteStore,
    export: &crate::MetadataExport,
    budget: &mut RemotePayloadFsckBudget,
    missing: &mut usize,
) -> Result<()> {
    for large in &export.large_objects {
        for bytes in remote_local_object_variants(paths, remote, &large.manifest_key)? {
            if !budget.try_take() {
                return Ok(());
            }
            let manifest: LargeManifest =
                serde_json::from_slice(&decode_object(paths, &bytes.bytes)?).with_context(
                    || {
                        format!(
                            "parse large manifest {} from {}",
                            large.manifest_key, bytes.remote_key
                        )
                    },
                )?;
            if !large_manifest_issues(&manifest, large).is_empty() {
                *missing += 1;
                eprintln!(
                    "large manifest object does not match metadata {} {}",
                    large.oid, bytes.remote_key
                );
                continue;
            }
            for chunk in &manifest.chunks {
                validate_remote_large_chunk_object(paths, remote, chunk, budget, missing)?;
            }
        }
    }
    Ok(())
}

fn validate_remote_large_chunk_object(
    paths: &Paths,
    remote: &RemoteStore,
    chunk: &majutsu_core::LargeChunk,
    budget: &mut RemotePayloadFsckBudget,
    missing: &mut usize,
) -> Result<()> {
    for variant in remote_local_object_variants(paths, remote, &chunk.object_key)? {
        if !budget.try_take() {
            return Ok(());
        }
        let stored = decode_object(paths, &variant.bytes).with_context(|| {
            format!(
                "decode large chunk object {} from {}",
                chunk.object_key, variant.remote_key
            )
        })?;
        if chunk
            .stored_len
            .is_some_and(|stored_len| stored.len() as u64 != stored_len)
        {
            *missing += 1;
            eprintln!(
                "large chunk object stored size does not match metadata {} {}",
                chunk.oid, variant.remote_key
            );
            continue;
        }
        let payload = decode_large_chunk_stored_bytes(chunk, &stored).with_context(|| {
            format!(
                "decode large chunk payload {} from {}",
                chunk.object_key, variant.remote_key
            )
        })?;
        if payload.len() as u64 != chunk.len {
            *missing += 1;
            eprintln!(
                "large chunk object size does not match metadata {} {}",
                chunk.oid, variant.remote_key
            );
            continue;
        }
        if blake3_hex(&payload) != chunk.oid {
            *missing += 1;
            eprintln!(
                "large chunk object hash does not match metadata {} {}",
                chunk.oid, variant.remote_key
            );
        }
    }
    Ok(())
}

pub(crate) fn validate_remote_blob_objects(
    paths: &Paths,
    remote: &RemoteStore,
    export: &crate::MetadataExport,
    budget: &mut RemotePayloadFsckBudget,
    missing: &mut usize,
) -> Result<()> {
    for blob in &export.blobs {
        if blob.pack_id.is_some() {
            continue;
        }
        for variant in remote_local_object_variants(paths, remote, &blob.object_key)? {
            if !budget.try_take() {
                return Ok(());
            }
            let payload = decode_object(paths, &variant.bytes).with_context(|| {
                format!(
                    "decode blob object {} from {}",
                    blob.object_key, variant.remote_key
                )
            })?;
            if payload.len() as u64 != blob.size {
                *missing += 1;
                eprintln!(
                    "blob object size does not match metadata {} {}",
                    blob.oid, variant.remote_key
                );
                continue;
            }
            if blake3_hex(&payload) != blob.oid {
                *missing += 1;
                eprintln!(
                    "blob object hash does not match metadata {} {}",
                    blob.oid, variant.remote_key
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_remote_metadata_references(
    paths: &Paths,
    remote: &RemoteStore,
    export: &crate::MetadataExport,
    budget: &mut RemotePayloadFsckBudget,
    missing: &mut usize,
) -> Result<()> {
    let mut live = LiveMetadataReferences::default();
    for snapshot in &export.snapshots {
        let manifest = match crate::snapshot_state::snapshot_manifest_from_parts(
            paths,
            &snapshot.id,
            &snapshot.manifest_key,
            &snapshot.manifest_json,
        ) {
            Ok(manifest) => manifest,
            Err(_) => continue,
        };
        for manifest_key in live.add_snapshot_manifest(&manifest) {
            for bytes in remote_local_object_variants(paths, remote, &manifest_key)? {
                if !budget.try_take() {
                    return Ok(());
                }
                let large_manifest: LargeManifest =
                    match serde_json::from_slice(&decode_object(paths, &bytes.bytes)?) {
                        Ok(manifest) => manifest,
                        Err(_) => continue,
                    };
                live.add_large_manifest(large_manifest);
            }
        }
    }
    for issue in metadata_reference_issues(
        export.blobs.iter().map(|blob| blob.oid.as_str()),
        export.large_objects.iter().map(|large| large.oid.as_str()),
        export.chunks.iter().map(|chunk| chunk.oid.as_str()),
        &live.blobs,
        &live.large_objects,
        &live.chunks,
    ) {
        *missing += 1;
        match issue {
            MetadataReferenceIssue::DanglingBlob { oid } => {
                eprintln!("dangling remote blob metadata {oid}");
            }
            MetadataReferenceIssue::DanglingLargeObject { oid } => {
                eprintln!("dangling remote large object metadata {oid}");
            }
            MetadataReferenceIssue::DanglingChunk { oid } => {
                eprintln!("dangling remote chunk metadata {oid}");
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_remote_snapshot_objects(
    paths: &Paths,
    remote: &RemoteStore,
    export: &crate::MetadataExport,
    budget: &mut RemotePayloadFsckBudget,
    missing: &mut usize,
) -> Result<()> {
    for snapshot in &export.snapshots {
        let metadata_manifest = match crate::snapshot_state::snapshot_manifest_from_parts(
            paths,
            &snapshot.id,
            &snapshot.manifest_key,
            &snapshot.manifest_json,
        ) {
            Ok(manifest) => manifest,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid snapshot manifest metadata {}: {err}", snapshot.id);
                continue;
            }
        };
        for bytes in remote_local_object_variants(paths, remote, &snapshot.manifest_key)? {
            if !budget.try_take() {
                return Ok(());
            }
            let payload = crate::hydrate_compact_snapshot_manifest_payload(
                paths,
                &snapshot.manifest_key,
                decode_object(paths, &bytes.bytes)?,
            )?;
            let remote_manifest: SnapshotManifest =
                serde_json::from_slice(&payload).with_context(|| {
                    format!(
                        "parse snapshot manifest {} from {}",
                        snapshot.manifest_key, bytes.remote_key
                    )
                })?;
            if !snapshot_manifest_matches(&remote_manifest, &metadata_manifest) {
                *missing += 1;
                eprintln!(
                    "snapshot manifest object does not match metadata {} {}",
                    snapshot.id, bytes.remote_key
                );
            }
        }
        for (root_id, root_tree) in &metadata_manifest.root_trees {
            for bytes in remote_local_object_variants(paths, remote, &root_tree.tree_key)? {
                if !budget.try_take() {
                    return Ok(());
                }
                let tree: TreeManifest =
                    serde_json::from_slice(&decode_object(paths, &bytes.bytes)?).with_context(
                        || {
                            format!(
                                "parse tree manifest {} from {}",
                                root_tree.tree_key, bytes.remote_key
                            )
                        },
                    )?;
                if !tree_manifest_issues(&tree, root_id, root_tree).is_empty() {
                    *missing += 1;
                    eprintln!(
                        "tree manifest object does not match snapshot metadata {} {}",
                        snapshot.id, bytes.remote_key
                    );
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_remote_pack_objects(
    paths: &Paths,
    remote: &RemoteStore,
    export: &crate::MetadataExport,
    budget: &mut RemotePayloadFsckBudget,
    missing: &mut usize,
) -> Result<()> {
    let expected_pack_index_keys = expected_canonical_pack_index_keys(export);
    for key in remote.list("indexes/pack-index/")? {
        if !expected_pack_index_keys.contains(&key) {
            *missing += 1;
            eprintln!("unexpected remote pack index object {key}");
        }
    }
    let expected_pack_object_keys = expected_canonical_pack_object_keys(export);
    for key in remote.list("packs/")? {
        if !expected_pack_object_keys.contains(&key) {
            *missing += 1;
            eprintln!("unexpected remote pack object {key}");
        }
    }
    let mut blobs_by_pack: BTreeMap<&str, BTreeMap<&str, &BlobExport>> = BTreeMap::new();
    for blob in &export.blobs {
        if let Some(pack_id) = blob.pack_id.as_deref() {
            blobs_by_pack
                .entry(pack_id)
                .or_default()
                .insert(blob.oid.as_str(), blob);
        }
    }
    for pack in &export.packs {
        let expected_blobs = blobs_by_pack
            .get(pack.pack_id.as_str())
            .cloned()
            .unwrap_or_default();
        let expected_blob_metadata = packed_blob_metadata(&expected_blobs);
        for bytes in remote_local_object_variants(paths, remote, &pack.index_key)? {
            if !budget.try_take() {
                return Ok(());
            }
            let index: PackIndex = serde_json::from_slice(&bytes.bytes).with_context(|| {
                format!(
                    "parse pack index {} from {}",
                    pack.index_key, bytes.remote_key
                )
            })?;
            for issue in pack_index_issues(pack, &index, &expected_blob_metadata) {
                *missing += 1;
                let stop_after_issue = matches!(issue, PackIndexIssue::PackMetadataMismatch);
                match issue {
                    PackIndexIssue::PackMetadataMismatch => {
                        eprintln!(
                            "pack index object does not match metadata {} {}",
                            pack.pack_id, bytes.remote_key
                        );
                    }
                    PackIndexIssue::EntryWithoutBlobMetadata { oid } => {
                        eprintln!(
                            "pack index entry has no matching blob metadata {} {}",
                            pack.pack_id, oid
                        );
                    }
                    PackIndexIssue::EntryOffsetMismatch { oid } => {
                        eprintln!(
                            "pack index entry does not match blob offset metadata {} {}",
                            pack.pack_id, oid
                        );
                    }
                    PackIndexIssue::MissingBlobEntry { oid } => {
                        eprintln!(
                            "packed blob missing from pack index {} {}",
                            pack.pack_id, oid
                        );
                    }
                }
                if stop_after_issue {
                    break;
                }
            }
        }
        for bytes in remote_local_object_variants(paths, remote, &pack.pack_key)? {
            if !budget.try_take() {
                return Ok(());
            }
            for issue in pack_object_issues(pack, bytes.bytes.len() as u64, &expected_blob_metadata)
            {
                *missing += 1;
                match issue {
                    PackObjectIssue::SizeMismatch => {
                        eprintln!(
                            "pack object size does not match metadata {} {}",
                            pack.pack_id, bytes.remote_key
                        );
                    }
                    PackObjectIssue::MissingBlobOffset { oid } => {
                        eprintln!(
                            "packed blob missing offset metadata {} {}",
                            pack.pack_id, oid
                        );
                    }
                    PackObjectIssue::MissingBlobLength { oid } => {
                        eprintln!(
                            "packed blob missing length metadata {} {}",
                            pack.pack_id, oid
                        );
                    }
                    PackObjectIssue::BlobRangeOutOfBounds { oid } => {
                        eprintln!(
                            "packed blob range out of pack bounds {} {}",
                            pack.pack_id, oid
                        );
                    }
                }
            }
        }
    }
    for pack_id in missing_pack_metadata_ids(
        blobs_by_pack.keys().copied(),
        export.packs.iter().map(|pack| pack.pack_id.as_str()),
    ) {
        *missing += 1;
        eprintln!("packed blob references missing pack metadata {pack_id}");
    }
    Ok(())
}

fn expected_canonical_pack_index_keys(export: &crate::MetadataExport) -> BTreeSet<String> {
    export
        .packs
        .iter()
        .flat_map(|pack| canonical_remote_aliases(&pack.index_key))
        .filter(|key| key.starts_with("indexes/pack-index/"))
        .collect()
}

fn expected_canonical_pack_object_keys(export: &crate::MetadataExport) -> BTreeSet<String> {
    export
        .packs
        .iter()
        .flat_map(|pack| canonical_remote_aliases(&pack.pack_key))
        .filter(|key| key.starts_with("packs/"))
        .collect()
}

pub(crate) struct RemotePayloadFsckBudget {
    sample: Option<usize>,
    deadline: Option<Instant>,
    checked: usize,
    stopped: bool,
    timed_out: bool,
}

impl RemotePayloadFsckBudget {
    fn new(sample: Option<usize>, timeout: Option<Duration>) -> Self {
        Self {
            sample,
            deadline: timeout.map(|timeout| Instant::now() + timeout),
            checked: 0,
            stopped: false,
            timed_out: false,
        }
    }

    fn try_take(&mut self) -> bool {
        if self.stopped {
            return false;
        }
        if self.sample.is_some_and(|sample| self.checked >= sample) {
            self.stopped = true;
            return false;
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.stopped = true;
            self.timed_out = true;
            return false;
        }
        self.checked += 1;
        true
    }

    fn limited(&self) -> bool {
        self.stopped
    }

    fn timed_out(&self) -> bool {
        self.timed_out
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RemoteFsckOptions {
    pub(crate) metadata: bool,
    pub(crate) payload: bool,
    pub(crate) payload_sample: Option<usize>,
    pub(crate) timeout: Option<Duration>,
}

pub(crate) fn remote_fsck_with_options(
    paths: &Paths,
    remote: &RemoteStore,
    options: RemoteFsckOptions,
) -> Result<()> {
    let mut missing = 0usize;
    let mut verified_hosts = 0usize;
    let mut payload_budget = RemotePayloadFsckBudget::new(options.payload_sample, options.timeout);
    if !options.metadata {
        remote_payload_fsck_current_host(paths, remote, options, &mut payload_budget)?;
        println!("remote fsck payload ok");
        println!("hosts 1");
        println!("payload_objects_checked {}", payload_budget.checked);
        println!("payload_limited {}", payload_budget.limited());
        return Ok(());
    }
    let has_legacy_export = remote.exists(LEGACY_METADATA_EXPORT_KEY)?;
    let has_host_index = remote.exists(REMOTE_HOST_INDEX_KEY)?;

    if has_legacy_export {
        let export = remote_fsck_export(
            paths,
            remote,
            LEGACY_METADATA_EXPORT_KEY,
            None,
            options,
            &mut payload_budget,
            &mut missing,
        )?;
        if let Some(current) = export.refs.get("current") {
            let legacy_current = remote_ref(remote, "hosts/current")?;
            if legacy_current.as_deref() != Some(current.as_str()) {
                missing += 1;
                eprintln!("legacy hosts/current does not match metadata current ref");
            }
        }
    }

    if has_host_index {
        let index = read_remote_host_index(remote)?;
        let indexed_host_ids = index
            .hosts
            .iter()
            .map(|host| host.id.clone())
            .collect::<BTreeSet<_>>();
        for issue in index.duplicate_issues() {
            missing += 1;
            match issue {
                RemoteHostIndexIssue::DuplicateHostId(id) => {
                    eprintln!("duplicate host id in hosts/index.json: {id}");
                }
                RemoteHostIndexIssue::DuplicateMetadataKey(key) => {
                    eprintln!("duplicate host metadata_key in hosts/index.json: {key}");
                }
            }
        }
        for host in &index.hosts {
            verified_hosts += 1;
            let expected_metadata_key = host_metadata_key(&host.id);
            let expected_compressed_metadata_key = compressed_metadata_key(&expected_metadata_key);
            if host.metadata_key != expected_metadata_key
                && host.metadata_key != expected_compressed_metadata_key
            {
                missing += 1;
                eprintln!(
                    "host index metadata_key {} does not match canonical key {}",
                    host.metadata_key, expected_metadata_key
                );
            }
            if !remote.exists(&host.metadata_key)? {
                missing += 1;
                eprintln!("missing host metadata {} {}", host.id, host.metadata_key);
                continue;
            }
            let export = remote_fsck_export(
                paths,
                remote,
                &host.metadata_key,
                Some(&host.id),
                options,
                &mut payload_budget,
                &mut missing,
            )?;
            if export.config.host.id != host.id {
                missing += 1;
                eprintln!(
                    "host index id {} does not match metadata host id {}",
                    host.id, export.config.host.id
                );
            }
            if export.config.host.name != host.name {
                missing += 1;
                eprintln!(
                    "host index name {} does not match metadata host name {}",
                    host.name, export.config.host.name
                );
            }
            let current = export.refs.get("current");
            if host.current_snapshot.as_ref() != current {
                missing += 1;
                eprintln!(
                    "host index current snapshot does not match metadata for {}",
                    host.id
                );
            }
            let head = read_remote_head(paths, remote, &host.id)?;
            if let Some(head) = head.as_ref() {
                if head.version != 1 {
                    missing += 1;
                    eprintln!(
                        "unsupported remote head version {}",
                        remote_head_key(&host.id)
                    );
                }
                if head.host_id != host.id {
                    missing += 1;
                    eprintln!(
                        "remote head host id {} does not match {}",
                        head.host_id, host.id
                    );
                }
                if head.host_name != export.config.host.name {
                    missing += 1;
                    eprintln!(
                        "remote head host name does not match metadata for {}",
                        host.id
                    );
                }
                if head.metadata_key != host.metadata_key {
                    missing += 1;
                    eprintln!(
                        "remote head metadata key does not match host index for {}",
                        host.id
                    );
                }
                if head.current_snapshot.as_ref() != current {
                    missing += 1;
                    eprintln!(
                        "remote head current snapshot does not match metadata for {}",
                        host.id
                    );
                }
            }
            let current_ref_key = host_current_ref_key(&host.id);
            if let Some(current) = current {
                match remote_ref(remote, &current_ref_key)? {
                    Some(remote_current) if remote_current == *current => {}
                    Some(remote_current) => {
                        if !matches!(remote, RemoteStore::S3(_)) || head.is_none() {
                            missing += 1;
                            eprintln!(
                                "{current_ref_key} points to {remote_current}, expected {current}"
                            );
                        }
                    }
                    None => {
                        if !matches!(remote, RemoteStore::S3(_)) || head.is_none() {
                            missing += 1;
                            eprintln!("missing remote ref {current_ref_key}");
                        }
                    }
                }
                let legacy_current_key = host_legacy_current_key(&host.id);
                if let Some(legacy_current) = remote_ref(remote, &legacy_current_key)?
                    && legacy_current != *current
                {
                    missing += 1;
                    eprintln!(
                        "{legacy_current_key} points to {legacy_current}, expected {current}"
                    );
                }
            }
            if let Some(last_synced) = export.refs.get("last-synced") {
                match parse_db_time(last_synced) {
                    Ok(metadata_last_synced) if host.last_synced_at == metadata_last_synced => {}
                    Ok(metadata_last_synced) => {
                        missing += 1;
                        eprintln!(
                            "host index last_synced_at {} does not match metadata last-synced {} for {}",
                            host.last_synced_at.to_rfc3339(),
                            metadata_last_synced.to_rfc3339(),
                            host.id
                        );
                    }
                    Err(err) => {
                        missing += 1;
                        eprintln!("invalid metadata last-synced for {}: {err}", host.id);
                    }
                }
                let last_synced_ref_key = host_last_synced_ref_key(&host.id);
                if let Some(head) = head.as_ref()
                    && head.last_synced.as_ref() != Some(last_synced)
                {
                    missing += 1;
                    eprintln!(
                        "remote head last-synced does not match metadata for {}",
                        host.id
                    );
                }
                match remote_ref(remote, &last_synced_ref_key)? {
                    Some(remote_last_synced) if remote_last_synced == *last_synced => {}
                    Some(remote_last_synced) => {
                        if !matches!(remote, RemoteStore::S3(_)) || head.is_none() {
                            missing += 1;
                            eprintln!(
                                "{last_synced_ref_key} points to {remote_last_synced}, expected {last_synced}"
                            );
                        }
                    }
                    None => {
                        if !matches!(remote, RemoteStore::S3(_)) || head.is_none() {
                            missing += 1;
                            eprintln!("missing remote ref {last_synced_ref_key}");
                        }
                    }
                }
            }
            validate_remote_host_ref_prefix(remote, &host.id, &mut missing)?;
            for snapshot in &export.snapshots {
                let key = host_snapshot_canonical_key(&host.id, &snapshot.id);
                if !remote.exists(&key)? {
                    missing += 1;
                    eprintln!("missing canonical host snapshot export {key}");
                }
            }
            for operation in &export.operations {
                let key = host_operation_canonical_key(&host.id, &operation.id);
                if !remote.exists(&key)? {
                    missing += 1;
                    eprintln!("missing canonical host operation export {key}");
                }
            }
            validate_remote_gc_records(paths, remote, &host.id, &export, &mut missing)?;
        }
        validate_remote_host_prefix_hosts(remote, &indexed_host_ids, &mut missing)?;
        validate_remote_gc_prefix_hosts(remote, &indexed_host_ids, &mut missing)?;
    }
    validate_remote_lifecycle_artifacts(remote, &mut missing)?;

    if !has_legacy_export && !has_host_index {
        bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found");
    }
    if has_host_index && verified_hosts == 0 {
        missing += 1;
        eprintln!("hosts/index.json contains no hosts");
    }
    if missing > 0 {
        bail!("remote fsck found {missing} issue(s)");
    }
    if options.payload && payload_budget.timed_out() {
        bail!(
            "remote fsck payload verification stopped after checking {} object(s)",
            payload_budget.checked
        );
    }
    println!("remote fsck ok");
    println!("hosts {}", verified_hosts);
    if options.payload {
        println!("payload_objects_checked {}", payload_budget.checked);
        println!("payload_limited {}", payload_budget.limited());
    }
    if has_legacy_export {
        println!("legacy_metadata ok");
    }
    Ok(())
}

fn remote_payload_fsck_current_host(
    paths: &Paths,
    remote: &RemoteStore,
    options: RemoteFsckOptions,
    payload_budget: &mut RemotePayloadFsckBudget,
) -> Result<()> {
    let config = read_config(paths)?;
    let metadata_key = if remote.exists(REMOTE_HOST_INDEX_KEY)? {
        let index = read_remote_host_index(remote)?;
        index
            .hosts
            .iter()
            .find(|host| host.id == config.host.id)
            .map(|host| host.metadata_key.clone())
            .with_context(|| {
                format!(
                    "host {} is not present in remote host index",
                    config.host.id
                )
            })?
    } else if remote.exists(LEGACY_METADATA_EXPORT_KEY)? {
        LEGACY_METADATA_EXPORT_KEY.into()
    } else {
        bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found");
    };
    let metadata_bytes = remote_metadata_bytes(remote, &metadata_key)?;
    let export: crate::MetadataExport = serde_json::from_slice(&metadata_bytes)
        .with_context(|| format!("parse remote metadata {metadata_key}"))?;
    let mut missing = 0usize;
    validate_remote_snapshot_objects(paths, remote, &export, payload_budget, &mut missing)?;
    validate_remote_blob_objects(paths, remote, &export, payload_budget, &mut missing)?;
    validate_remote_large_manifest_objects(paths, remote, &export, payload_budget, &mut missing)?;
    validate_remote_pack_objects(paths, remote, &export, payload_budget, &mut missing)?;
    if missing > 0 {
        bail!("remote fsck payload found {missing} issue(s)");
    }
    if options.timeout.is_some() && payload_budget.timed_out() {
        bail!(
            "remote fsck payload verification stopped after checking {} object(s)",
            payload_budget.checked
        );
    }
    Ok(())
}

fn remote_fsck_export(
    paths: &Paths,
    remote: &RemoteStore,
    metadata_key: &str,
    host_id: Option<&str>,
    options: RemoteFsckOptions,
    payload_budget: &mut RemotePayloadFsckBudget,
    missing: &mut usize,
) -> Result<crate::MetadataExport> {
    let metadata_bytes = remote_metadata_bytes(remote, metadata_key)?;
    let export: crate::MetadataExport = serde_json::from_slice(&metadata_bytes)
        .with_context(|| format!("parse remote metadata {metadata_key}"))?;
    if export.version != METADATA_EXPORT_VERSION {
        *missing += 1;
        eprintln!(
            "unsupported remote metadata version {} in {metadata_key}",
            export.version
        );
    }
    if let Err(err) = validate_config(&export.config) {
        *missing += 1;
        eprintln!("invalid remote config in {metadata_key}: {err}");
    }
    let snapshot_ids = export
        .snapshots
        .iter()
        .map(|snapshot| snapshot.id.clone())
        .collect::<BTreeSet<_>>();
    for issue in local_ref_issues(
        export
            .refs
            .iter()
            .map(|(name, value)| (name.clone(), value.clone())),
        &snapshot_ids,
    ) {
        *missing += 1;
        eprintln!("remote metadata {metadata_key}: {}", issue.message());
    }
    for issue in large_pin_issues(&export.large_pins, &export.large_objects) {
        *missing += 1;
        match issue {
            LargePinIssue::Dangling { oid, .. } => {
                eprintln!("dangling remote large pin {oid} in {metadata_key}");
            }
            LargePinIssue::InvalidTimestamp { oid, .. } => {
                eprintln!("invalid remote large pin timestamp {oid} in {metadata_key}");
            }
        }
    }
    validate_remote_chunk_index(paths, remote, &export, missing)?;
    for key in local_object_keys(paths, &export)? {
        let legacy_exists = remote.exists(&key)?;
        let aliases = canonical_remote_aliases(&key);
        let alias_exists = aliases
            .iter()
            .map(|alias| remote.exists(alias))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .any(|exists| exists);
        for issue in remote_object_availability_issues(legacy_exists, &aliases, alias_exists) {
            *missing += 1;
            match issue {
                RemoteObjectAvailabilityIssue::MissingObjectOrAlias => {
                    eprintln!("missing remote object {key} or canonical alias");
                }
                RemoteObjectAvailabilityIssue::MissingCanonicalAlias => {
                    eprintln!("missing canonical remote object alias for {key}");
                }
            }
        }
    }
    if options.payload {
        validate_remote_snapshot_objects(paths, remote, &export, payload_budget, missing)?;
        validate_remote_blob_objects(paths, remote, &export, payload_budget, missing)?;
        validate_remote_large_manifest_objects(paths, remote, &export, payload_budget, missing)?;
        validate_remote_pack_objects(paths, remote, &export, payload_budget, missing)?;
        validate_remote_metadata_references(paths, remote, &export, payload_budget, missing)?;
    }
    if let Some(host_id) = host_id {
        validate_remote_oplog(paths, remote, host_id, &export.operations, missing)?;
        validate_remote_timeline_prefixes(remote, host_id, &export, missing)?;
        for snapshot in &export.snapshots {
            let key = host_snapshot_key(host_id, &snapshot.id);
            if remote.exists(&key)? {
                let remote_snapshot: SnapshotExport = serde_json::from_slice(&remote.get(&key)?)
                    .with_context(|| format!("parse remote snapshot export {key}"))?;
                if !snapshot_export_matches(&remote_snapshot, snapshot) {
                    *missing += 1;
                    eprintln!("host snapshot export does not match metadata {key}");
                }
            }
            let canonical_key = host_snapshot_canonical_key(host_id, &snapshot.id);
            if remote.exists(&canonical_key)? {
                let remote_snapshot: SnapshotExport =
                    decode_canonical_remote_export(paths, &remote.get(&canonical_key)?)
                        .with_context(|| format!("parse remote snapshot export {canonical_key}"))?;
                if !snapshot_export_matches(&remote_snapshot, snapshot) {
                    *missing += 1;
                    eprintln!("host snapshot export does not match metadata {canonical_key}");
                }
            }
        }
        for operation in &export.operations {
            let key = host_operation_key(host_id, &operation.id);
            if remote.exists(&key)? {
                let remote_operation: OperationExport = serde_json::from_slice(&remote.get(&key)?)
                    .with_context(|| format!("parse remote operation export {key}"))?;
                if !operation_log_entry_matches(&remote_operation, operation) {
                    *missing += 1;
                    eprintln!("host operation export does not match metadata {key}");
                }
            }
            let canonical_key = host_operation_canonical_key(host_id, &operation.id);
            if remote.exists(&canonical_key)? {
                let remote_operation: OperationExport =
                    decode_canonical_remote_export(paths, &remote.get(&canonical_key)?)
                        .with_context(|| {
                            format!("parse remote operation export {canonical_key}")
                        })?;
                if !operation_log_entry_matches(&remote_operation, operation) {
                    *missing += 1;
                    eprintln!("host operation export does not match metadata {canonical_key}");
                }
            }
        }
    }
    Ok(export)
}

fn compressed_metadata_key(key: &str) -> String {
    format!("{key}.zst")
}

fn remote_metadata_bytes(remote: &RemoteStore, metadata_key: &str) -> Result<Vec<u8>> {
    let bytes = remote.get(metadata_key)?;
    if metadata_key.ends_with(".zst") {
        return zstd::stream::decode_all(bytes.as_slice())
            .with_context(|| format!("decode compressed metadata {metadata_key}"));
    }
    Ok(bytes)
}

fn validate_remote_gc_prefix_hosts(
    remote: &RemoteStore,
    indexed_host_ids: &BTreeSet<String>,
    missing: &mut usize,
) -> Result<()> {
    for key in remote.list("gc/marks/")? {
        let Some(host_id) = key
            .strip_prefix("gc/marks/")
            .and_then(|rest| rest.strip_suffix(".json"))
        else {
            *missing += 1;
            eprintln!("unexpected remote gc mark key {key}");
            continue;
        };
        if host_id.is_empty() || host_id.contains('/') {
            *missing += 1;
            eprintln!("unexpected remote gc mark key {key}");
        } else if !indexed_host_ids.contains(host_id) {
            *missing += 1;
            eprintln!("remote gc mark references unknown host {key}");
        }
    }

    let mut reported_tombstone_hosts = BTreeSet::new();
    for key in remote.list("gc/tombstones/")? {
        let Some(rest) = key.strip_prefix("gc/tombstones/") else {
            *missing += 1;
            eprintln!("unexpected remote gc tombstone key {key}");
            continue;
        };
        let Some((host_id, _)) = rest.split_once('/') else {
            *missing += 1;
            eprintln!("unexpected remote gc tombstone key {key}");
            continue;
        };
        if host_id.is_empty() {
            *missing += 1;
            eprintln!("unexpected remote gc tombstone key {key}");
        } else if !indexed_host_ids.contains(host_id)
            && reported_tombstone_hosts.insert(host_id.to_string())
        {
            *missing += 1;
            eprintln!("remote gc tombstones reference unknown host {host_id}");
        }
    }
    Ok(())
}

fn validate_remote_host_prefix_hosts(
    remote: &RemoteStore,
    indexed_host_ids: &BTreeSet<String>,
    missing: &mut usize,
) -> Result<()> {
    let mut reported_hosts = BTreeSet::new();
    for key in remote.list("hosts/")? {
        let Some(rest) = key.strip_prefix("hosts/") else {
            *missing += 1;
            eprintln!("unexpected remote host key {key}");
            continue;
        };
        if matches!(rest, "index.json" | "current") {
            continue;
        }
        let Some((host_id, _)) = rest.split_once('/') else {
            *missing += 1;
            eprintln!("unexpected remote host key {key}");
            continue;
        };
        if host_id.is_empty() {
            *missing += 1;
            eprintln!("unexpected remote host key {key}");
        } else if !indexed_host_ids.contains(host_id) && reported_hosts.insert(host_id.to_string())
        {
            *missing += 1;
            eprintln!("remote hosts prefix references unknown host {host_id}");
        }
    }
    Ok(())
}

fn validate_remote_host_ref_prefix(
    remote: &RemoteStore,
    host_id: &str,
    missing: &mut usize,
) -> Result<()> {
    let expected_ref_keys = [
        host_current_ref_key(host_id),
        host_last_synced_ref_key(host_id),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    for key in remote.list(&format!("hosts/{host_id}/refs/"))? {
        if !expected_ref_keys.contains(&key) {
            *missing += 1;
            eprintln!("unexpected remote host ref {key}");
        }
    }
    Ok(())
}

fn validate_remote_lifecycle_artifacts(remote: &RemoteStore, missing: &mut usize) -> Result<()> {
    let keys = remote.list("lifecycle/")?;
    if keys.is_empty() {
        return Ok(());
    }
    let allowed_keys = [
        "lifecycle/status.json".to_string(),
        "lifecycle/policy-s3.json".to_string(),
        "lifecycle/policy-gcs.json".to_string(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    for key in &keys {
        if !allowed_keys.contains(key) {
            *missing += 1;
            eprintln!("unexpected lifecycle artifact {key}");
        }
    }
    let has_policy = keys
        .iter()
        .any(|key| key == "lifecycle/policy-s3.json" || key == "lifecycle/policy-gcs.json");
    if has_policy && !remote.exists("lifecycle/status.json")? {
        *missing += 1;
        eprintln!("missing lifecycle status artifact lifecycle/status.json");
    }
    if remote.exists("lifecycle/status.json")? {
        validate_remote_lifecycle_status(remote, missing)?;
    }
    for key in ["lifecycle/policy-s3.json", "lifecycle/policy-gcs.json"] {
        if remote.exists(key)? {
            validate_remote_lifecycle_policy(remote, key, missing)?;
        }
    }
    Ok(())
}

fn validate_remote_lifecycle_status(remote: &RemoteStore, missing: &mut usize) -> Result<()> {
    let status_key = "lifecycle/status.json";
    let status: serde_json::Value = match serde_json::from_slice(&remote.get(status_key)?) {
        Ok(status) => status,
        Err(err) => {
            *missing += 1;
            eprintln!("invalid lifecycle status {status_key}: {err}");
            return Ok(());
        }
    };
    let provider = match status.get("provider").and_then(|value| value.as_str()) {
        Some("s3" | "gcs") => status["provider"].as_str().unwrap(),
        Some(provider) => {
            *missing += 1;
            eprintln!("lifecycle status has unsupported provider {provider}");
            return Ok(());
        }
        None => {
            *missing += 1;
            eprintln!("lifecycle status is missing provider");
            return Ok(());
        }
    };
    let Some(policy_key) = status.get("policy_key").and_then(|value| value.as_str()) else {
        *missing += 1;
        eprintln!("lifecycle status is missing policy_key");
        return Ok(());
    };
    let expected_policy_key = format!("lifecycle/policy-{provider}.json");
    if policy_key != expected_policy_key {
        *missing += 1;
        eprintln!("lifecycle status policy_key {policy_key} does not match {expected_policy_key}");
    }
    if !remote.exists(policy_key)? {
        *missing += 1;
        eprintln!("lifecycle status points to missing policy {policy_key}");
    }
    if !status
        .get("provider_applied")
        .is_some_and(|value| value.is_boolean())
    {
        *missing += 1;
        eprintln!("lifecycle status is missing boolean provider_applied");
    }
    match status.get("applied_at").and_then(|value| value.as_str()) {
        Some(applied_at) => {
            if let Err(err) = parse_db_time(applied_at) {
                *missing += 1;
                eprintln!("lifecycle status has invalid applied_at {applied_at}: {err}");
            }
        }
        None => {
            *missing += 1;
            eprintln!("lifecycle status is missing applied_at");
        }
    }
    Ok(())
}

fn validate_remote_lifecycle_policy(
    remote: &RemoteStore,
    key: &str,
    missing: &mut usize,
) -> Result<()> {
    let policy: serde_json::Value = match serde_json::from_slice(&remote.get(key)?) {
        Ok(policy) => policy,
        Err(err) => {
            *missing += 1;
            eprintln!("invalid lifecycle policy {key}: {err}");
            return Ok(());
        }
    };
    if key == "lifecycle/policy-s3.json" {
        if let Err(err) = majutsu_policy::s3_lifecycle_configuration_xml(&policy) {
            *missing += 1;
            eprintln!("invalid S3 lifecycle policy {key}: {err}");
        }
    } else if !policy.get("rule").is_some_and(|rule| rule.is_array()) {
        *missing += 1;
        eprintln!("invalid GCS lifecycle policy {key}: missing rule array");
    }
    Ok(())
}

fn validate_remote_timeline_prefixes(
    remote: &RemoteStore,
    host_id: &str,
    export: &crate::MetadataExport,
    missing: &mut usize,
) -> Result<()> {
    let expected_snapshot_keys = export
        .snapshots
        .iter()
        .flat_map(|snapshot| {
            [
                host_snapshot_key(host_id, &snapshot.id),
                host_snapshot_canonical_key(host_id, &snapshot.id),
            ]
        })
        .collect::<BTreeSet<_>>();
    for key in remote.list(&host_snapshots_prefix(host_id))? {
        if !expected_snapshot_keys.contains(&key) {
            *missing += 1;
            eprintln!("unexpected host snapshot export {key}");
        }
    }

    let expected_operation_keys = export
        .operations
        .iter()
        .flat_map(|operation| {
            [
                host_operation_key(host_id, &operation.id),
                host_operation_canonical_key(host_id, &operation.id),
            ]
        })
        .chain(if matches!(remote, RemoteStore::S3(_)) {
            Vec::new()
        } else {
            vec![host_oplog_key(host_id), host_oplog_canonical_key(host_id)]
        })
        .collect::<BTreeSet<_>>();
    for key in remote.list(&host_ops_prefix(host_id))? {
        if !expected_operation_keys.contains(&key) {
            *missing += 1;
            eprintln!("unexpected host operation export {key}");
        }
    }
    Ok(())
}
