use anyhow::{Context, Result, bail};
use majutsu_core::{
    ConfigRootIssue, HistoryGraphIssue, HostFileIssue, LargeManifest, OperationLogComparisonIssue,
    OperationLogEntry as OperationExport, OperationLogEntryIssue, config_root_consistency_issues,
    decode_operation_log, history_graph_issues, host_file_issues, operation_log_comparison_issues,
};
use majutsu_db::{local_ref_issues, remote_ref_issues};
use majutsu_large::{
    LargePinIssue, large_chunk_hash_matches, large_manifest_issues, large_pin_issues,
};
use majutsu_store::{
    REMOTE_CHUNK_INDEX_SHARD_KEY, RemoteChunkIndexEntry as ChunkIndexEntry, RemoteChunkIndexIssue,
    RemoteChunkIndexShard as ChunkIndexShard, RemoteGcMark as GcMarkExport,
    RemoteGcTombstone as GcTombstoneExport, canonical_remote_alias, canonical_remote_aliases,
    host_oplog_canonical_key, host_oplog_key, remote_gc_mark_key, remote_gc_tombstone_prefix,
};
use rusqlite::Connection;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use crate::config::{Config, HostConfig, Paths, RootConfig, read_config};
use crate::object_paths::local_object_keys;
use crate::operation_log::record_op;
use crate::remote_store::RemoteStore;
use crate::root_state::roots;
use crate::snapshot_state::current_snapshot;
use crate::{
    decode_canonical_remote_export, decode_canonical_remote_oplog, decode_object, export_metadata,
    open_db, read_blob_payload, read_large_chunk, read_object, remote_local_object_variants,
    validate_event_queue, validate_local_large_manifest_objects,
    validate_local_metadata_references, validate_local_oplog, validate_local_pack_objects,
    validate_local_snapshot_objects, validate_restore_queue, validate_upload_queue,
};

pub(crate) fn fsck(paths: &Paths) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let mut missing = 0usize;
    let config = read_config(paths)?;
    let export = export_metadata(&conn, &config)?;
    for key in local_object_keys(&export) {
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

pub(crate) fn validate_remote_gc_records(
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
        let expected = expected_gc_mark_object_keys(export);
        for issue in mark.validation_issues(host_id, export.refs.get("current"), &expected) {
            *missing += 1;
            eprintln!("{}", issue.message(&mark_key, host_id));
        }
    }
    validate_remote_gc_tombstones(remote, host_id, missing)
}

fn expected_gc_mark_object_keys(export: &crate::MetadataExport) -> BTreeSet<String> {
    let mut object_keys = local_object_keys(export);
    for key in object_keys.clone() {
        object_keys.extend(canonical_remote_aliases(&key));
    }
    object_keys.into_iter().collect()
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
        *missing += 1;
        eprintln!("missing canonical host operation log {canonical_key}");
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
            }
        }
    }
    Ok(())
}
