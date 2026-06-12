use anyhow::{Context, Result, bail};
use majutsu_core::{
    ConfigRootIssue, HistoryGraphIssue, HostFileIssue, LargeManifest, LiveMetadataReferences,
    MetadataReferenceIssue, OperationLogComparisonIssue, OperationLogEntry as OperationExport,
    OperationLogEntryIssue, SnapshotExport, SnapshotManifest, TreeManifest,
    config_root_consistency_issues, decode_operation_log, history_graph_issues, host_file_issues,
    metadata_reference_issues, operation_log_comparison_issues, operation_log_entry_matches,
    snapshot_export_matches, snapshot_manifest_matches, tree_manifest_issues,
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
use rusqlite::Connection;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::path::Path;

use crate::config::{
    Config, HostConfig, METADATA_EXPORT_VERSION, Paths, RootConfig, read_config, validate_config,
};
use crate::object_paths::{
    local_object_keys, remote_live_object_keys_for_local, s3_remote_live_object_keys_for_local,
};
use crate::operation_log::{local_oplog_path, query_operations, record_op};
use crate::remote_runtime::read_remote_host_index;
use crate::remote_store::RemoteStore;
use crate::root_state::roots;
use crate::snapshot_state::current_snapshot;
use crate::util::{blake3_hex, parse_db_time};
use crate::{
    decode_canonical_remote_export, decode_canonical_remote_oplog, decode_large_chunk_stored_bytes,
    decode_object, export_metadata, open_db, packed_blob_metadata, read_blob_payload,
    read_large_chunk, read_object, remote_local_object_variants, remote_ref,
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

pub(crate) fn fsck(paths: &Paths) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let mut missing = 0usize;
    let config = read_config(paths)?;
    let export = export_metadata(paths, &conn, &config)?;
    for key in local_object_keys(paths, &export)? {
        let full = paths.home.join(&key);
        if !full.exists() {
            missing += 1;
            eprintln!("missing object {key}");
        } else if let Err(err) = read_object(paths, &key) {
            missing += 1;
            eprintln!("unreadable object {key}: {err}");
        }
    }
    let mut stmt = conn.prepare("select oid, object_key, pack_id from blobs")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    for row in rows {
        let (oid, key, pack_id) = row?;
        if pack_id.is_some() {
            if let Err(err) = read_blob_payload(paths, &conn, &oid, &key) {
                missing += 1;
                eprintln!("unreadable packed blob {oid}: {err}");
            }
        } else if !paths.home.join(&key).exists() {
            missing += 1;
            eprintln!("missing blob {oid} {key}");
        } else if let Err(err) = read_object(paths, &key) {
            missing += 1;
            eprintln!("unreadable blob {oid} {key}: {err}");
        }
    }
    let mut stmt = conn.prepare("select oid, object_key from chunks")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (oid, key) = row?;
        if !paths.home.join(&key).exists() {
            missing += 1;
            eprintln!("missing chunk {oid} {key}");
        } else if let Err(err) = read_object(paths, &key) {
            missing += 1;
            eprintln!("unreadable chunk {oid} {key}: {err}");
        }
    }
    let mut stmt = conn.prepare("select oid, manifest_key from large_objects")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (oid, manifest_key) = row?;
        match read_object(paths, &manifest_key)
            .and_then(|bytes| serde_json::from_slice::<LargeManifest>(&bytes).map_err(Into::into))
        {
            Ok(manifest) => {
                for chunk in &manifest.chunks {
                    match read_large_chunk(paths, chunk) {
                        Ok(bytes) if large_chunk_hash_matches(chunk, &bytes) => {}
                        Ok(_) => {
                            missing += 1;
                            eprintln!("large chunk hash mismatch {} {}", oid, chunk.object_key);
                        }
                        Err(err) => {
                            missing += 1;
                            eprintln!("unreadable large chunk {} {}: {err}", oid, chunk.object_key);
                        }
                    }
                }
            }
            Err(err) => {
                missing += 1;
                eprintln!("unreadable large manifest {oid} {manifest_key}: {err}");
            }
        }
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
    validate_host_file(paths, &config, &mut missing)?;
    validate_config_roots(paths, &conn, &config, &mut missing)?;
    validate_local_refs(&conn, &mut missing)?;
    validate_remote_refs(&conn, &config, &mut missing)?;
    validate_local_history_graph(&export, &mut missing)?;
    validate_local_snapshot_objects(paths, &export, &mut missing)?;
    validate_local_large_manifest_objects(paths, &export, &mut missing)?;
    validate_local_pack_objects(paths, &export, &mut missing)?;
    validate_local_metadata_references(paths, &export, &mut missing)?;
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
        Some("checked local state"),
    )?;
    println!("fsck ok");
    Ok(())
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
            let Ok(bytes) = read_object(paths, &manifest_key) else {
                continue;
            };
            let Ok(large_manifest) = serde_json::from_slice::<LargeManifest>(&bytes) else {
                continue;
            };
            live.add_large_manifest(large_manifest);
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
    missing: &mut usize,
) -> Result<()> {
    for large in &export.large_objects {
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
    for pack in &export.packs {
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
        if !pack_path.exists() {
            let _ = crate::hydrate_local_object_from_remote(paths, &pack.pack_key);
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
    if !remote.exists(&mark_key)? {
        *missing += 1;
        eprintln!("missing remote gc mark {mark_key}");
    } else {
        let mark: GcMarkExport = match serde_json::from_slice(&remote.get(&mark_key)?) {
            Ok(mark) => mark,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid remote gc mark {mark_key}: {err}");
                return validate_remote_gc_tombstones(remote, host_id, missing);
            }
        };
        let expected = expected_gc_mark_object_keys(paths, remote, export)?;
        for issue in mark.validation_issues(host_id, export.refs.get("current"), &expected) {
            *missing += 1;
            eprintln!("{}", issue.message(&mark_key, host_id));
        }
    }
    validate_remote_gc_tombstones(remote, host_id, missing)
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
    missing: &mut usize,
) -> Result<()> {
    for large in &export.large_objects {
        for bytes in remote_local_object_variants(paths, remote, &large.manifest_key)? {
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
                validate_remote_large_chunk_object(paths, remote, chunk, missing)?;
            }
        }
    }
    Ok(())
}

fn validate_remote_large_chunk_object(
    paths: &Paths,
    remote: &RemoteStore,
    chunk: &majutsu_core::LargeChunk,
    missing: &mut usize,
) -> Result<()> {
    for variant in remote_local_object_variants(paths, remote, &chunk.object_key)? {
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
    missing: &mut usize,
) -> Result<()> {
    for blob in &export.blobs {
        if blob.pack_id.is_some() {
            continue;
        }
        for variant in remote_local_object_variants(paths, remote, &blob.object_key)? {
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

pub(crate) fn remote_fsck(paths: &Paths, remote: &RemoteStore) -> Result<()> {
    let mut missing = 0usize;
    let mut verified_hosts = 0usize;
    let has_legacy_export = remote.exists(LEGACY_METADATA_EXPORT_KEY)?;
    let has_host_index = remote.exists(REMOTE_HOST_INDEX_KEY)?;

    if has_legacy_export {
        let export = remote_fsck_export(
            paths,
            remote,
            LEGACY_METADATA_EXPORT_KEY,
            None,
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
                        missing += 1;
                        eprintln!(
                            "{current_ref_key} points to {remote_current}, expected {current}"
                        );
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
                if let Some(head) = head.as_ref() {
                    if head.last_synced.as_ref() != Some(last_synced) {
                        missing += 1;
                        eprintln!(
                            "remote head last-synced does not match metadata for {}",
                            host.id
                        );
                    }
                }
                match remote_ref(remote, &last_synced_ref_key)? {
                    Some(remote_last_synced) if remote_last_synced == *last_synced => {}
                    Some(remote_last_synced) => {
                        missing += 1;
                        eprintln!(
                            "{last_synced_ref_key} points to {remote_last_synced}, expected {last_synced}"
                        );
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
    println!("remote fsck ok");
    println!("hosts {}", verified_hosts);
    if has_legacy_export {
        println!("legacy_metadata ok");
    }
    Ok(())
}

fn remote_fsck_export(
    paths: &Paths,
    remote: &RemoteStore,
    metadata_key: &str,
    host_id: Option<&str>,
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
    validate_remote_snapshot_objects(paths, remote, &export, missing)?;
    validate_remote_blob_objects(paths, remote, &export, missing)?;
    validate_remote_large_manifest_objects(paths, remote, &export, missing)?;
    validate_remote_pack_objects(paths, remote, &export, missing)?;
    validate_remote_metadata_references(paths, remote, &export, missing)?;
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
