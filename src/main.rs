use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use clap::Parser;
use hmac::{Hmac, Mac};
use majutsu_core::{
    FileRecord, HistoryGraphIssue, LargeChunk, LargeManifest, LiveMetadataReferences,
    MetadataReferenceIssue, OperationLogEntry as OperationExport, OperationLogEntryIssue, Payload,
    RootSnapshot, SnapshotExport, SnapshotManifest, TreeManifest, decode_operation_log,
    history_graph_issues, metadata_reference_issues, operation_log_comparison_issues,
    operation_log_entry_matches, payload_large_ref, snapshot_export_matches,
    snapshot_manifest_matches, tree_manifest_issues,
};
use majutsu_crypto::EncryptionMode;
use majutsu_large::{
    ChunkExport, LargeObjectExport, LargePinExport, LargePinIssue, large_manifest_issues,
    large_pin_issues,
};
use majutsu_pack::{
    PackExport, PackIndex, PackIndexIssue, PackObjectIssue, PackedBlobMetadata,
    missing_pack_metadata_ids, pack_index_issues, pack_object_issues,
};
use majutsu_restore::{
    RestoreQueueItem, classify_restore_object_availability, validate_relative_filter_path,
};
use majutsu_store::{
    BlobExport, REMOTE_CHUNK_INDEX_SHARD_KEY, RemoteChunkIndexShard as ChunkIndexShard,
    RemoteGcMark as GcMarkExport, RemoteGcTombstone as GcTombstoneExport, RemoteHostSummary,
    canonical_remote_alias, canonical_remote_aliases, host_current_ref_key,
    host_last_synced_ref_key, host_legacy_current_key, host_metadata_key,
    host_operation_canonical_key, host_operation_key, host_oplog_canonical_key, host_oplog_key,
    host_ops_prefix, host_snapshot_canonical_key, host_snapshot_key, host_snapshots_prefix,
    remote_gc_mark_key, remote_gc_tombstone_prefix,
};
#[cfg(test)]
use reqwest::blocking::Client;
use rusqlite::{Connection, params};
use serde::Deserialize;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
#[cfg(test)]
use std::sync::mpsc;
use uuid::Uuid;
use walkdir::WalkDir;

mod atomic_io;
mod cli;
mod clone_runtime;
mod config;
mod daemon_runtime;
mod db_refs;
mod fs_meta;
mod fsck_runtime;
mod fuse_mount;
mod history_runtime;
mod key_runtime;
mod large_runtime;
mod lifecycle_runtime;
mod mount_runtime;
mod object_paths;
mod operation_log;
mod pack_runtime;
mod process_runtime;
mod prune_runtime;
mod queue_runtime;
mod remote_runtime;
mod remote_store;
mod restore_apply;
mod restore_runtime;
mod root_runtime;
mod root_state;
mod snapshot_rules;
mod snapshot_state;
mod sync_runtime;
mod util;
mod watch_runtime;

use atomic_io::write_atomic_with;
#[cfg(test)]
use cli::PackArgs;
use cli::{Cli, Command, InitArgs, RestoreArgs, SnapshotArgs};
use clone_runtime::clone_cmd;
use config::{
    Config, ConfigRoot, HostConfig, LargeCompressionConfig, LargeConfig, METADATA_EXPORT_VERSION,
    MetadataExport, PackConfig, Paths, RemoteConfig, RestoreConfig, RootConfig, SecurityConfig,
    TieringConfig, WatchConfig, default_chunk_size, default_large_binary_min_size,
    default_large_chunking, default_large_max_parallel_uploads, default_large_min_size,
    default_security_hash, default_security_key_id, encryption_enabled, encryption_mode,
    read_config, resolve_paths, validate_restore_archive_config, write_config,
};
use daemon_runtime::daemon_cmd;
use fs_meta::{file_gid, file_mode, file_uid, is_mount_point, read_xattrs, special_file_kind};
use fsck_runtime::fsck;
use history_runtime::{diff_cmd, log_cmd, op_cmd, status_cmd};
use key_runtime::key_cmd;
use large_runtime::large_cmd;
use lifecycle_runtime::lifecycle_cmd;
use mount_runtime::{hydrate_cmd, mount_cmd, unmount_cmd};
use object_paths::{large_chunk_base, local_object_keys};
use operation_log::{
    query_operations, record_op, record_op_with_details, record_op_with_id, rewrite_local_oplog,
};
use pack_runtime::pack_cmd;
use process_runtime::acquire_process_lock;
use prune_runtime::{gc_cmd, prune_cmd};
use queue_runtime::{has_pending_journal_events, record_event};
use remote_runtime::remote_cmd;
#[cfg(test)]
use remote_store::{
    DEFAULT_MULTIPART_THRESHOLD, FileRemote, MIN_MULTIPART_PART_SIZE, S3Remote, s3_object_class,
};
use remote_store::{RemoteStore, open_remote};
use restore_runtime::{
    RestoreDelete, RestorePlan, required_object_keys_for_plan, restore_cmd, restore_root_base,
    write_restore_job,
};
use root_runtime::root_cmd;
use root_state::{roots, sync_config_roots, sync_roots_to_config, update_root_status};
use snapshot_rules::{
    build_ignore, classify_large, effective_large_config, is_ignored, is_included,
    large_pointer_compression, looks_binary,
};
use snapshot_state::{
    carry_forward_root_snapshot, current_snapshot, load_snapshot, load_snapshot_by_id,
};
use sync_runtime::sync_cmd;
use util::{
    blake3_hex, media_type_for_path, modified_secs, new_id, parse_db_time, path_to_slash,
    stable_metadata_matches, stable_read,
};
use watch_runtime::watch_cmd;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths = resolve_paths(cli.home)?;
    match cli.command {
        Command::Init(args) => init(&paths, args),
        Command::Root { command } => root_cmd(&paths, command),
        Command::Snapshot(args) => snapshot(&paths, args),
        Command::Status => status_cmd(&paths),
        Command::Log(args) => log_cmd(&paths, args),
        Command::Op { command } => op_cmd(&paths, command),
        Command::Diff(args) => diff_cmd(&paths, args),
        Command::Restore(args) => restore_cmd(&paths, args),
        Command::Mount(args) => mount_cmd(&paths, args),
        Command::Unmount(args) => unmount_cmd(&paths, args),
        Command::Hydrate(args) => hydrate_cmd(&paths, args),
        Command::Large { command } => large_cmd(&paths, command),
        Command::Sync { command } => sync_cmd(&paths, command),
        Command::Remote { command } => remote_cmd(&paths, command),
        Command::Lifecycle { command } => lifecycle_cmd(&paths, command),
        Command::Clone(args) => clone_cmd(&paths, args),
        Command::Watch(args) => watch_cmd(&paths, args),
        Command::Daemon { command } => daemon_cmd(&paths, command),
        Command::Key { command } => key_cmd(&paths, command),
        Command::Pack(args) => pack_cmd(&paths, args),
        Command::Prune(args) => prune_cmd(&paths, args),
        Command::Gc => gc_cmd(&paths),
        Command::Fsck => fsck(&paths),
    }
}

fn init(paths: &Paths, args: InitArgs) -> Result<()> {
    create_layout(paths)?;
    let host_name = args
        .host_name
        .or_else(|| hostname_from_env().ok())
        .unwrap_or_else(|| "unknown-host".to_string());
    let config = if paths.config.exists() {
        read_config(paths)?
    } else {
        Config {
            host: HostConfig {
                id: Uuid::new_v4().to_string(),
                name: host_name,
            },
            remote: args.remote.map(RemoteConfig::from_url),
            roots: Vec::new(),
            large: LargeConfig {
                enabled: true,
                min_size: default_large_min_size(),
                binary_min_size: default_large_binary_min_size(),
                default_chunking: default_large_chunking(),
                chunk_size: default_chunk_size(),
                max_parallel_uploads: default_large_max_parallel_uploads(),
                multipart: true,
                always: majutsu_large::default_large_always_patterns(),
                never: majutsu_large::default_large_never_patterns(),
                compression: LargeCompressionConfig::default(),
            },
            pack: PackConfig::default(),
            watch: WatchConfig::default(),
            security: SecurityConfig {
                encryption: if args.encrypt {
                    "age".into()
                } else {
                    "none".into()
                },
                key_id: default_security_key_id(),
                hash: default_security_hash(),
            },
            tiering: TieringConfig::default(),
            restore: RestoreConfig::default(),
        }
    };
    write_config(paths, &config)?;
    fs::write(&paths.host, toml::to_string_pretty(&config.host)?)?;
    if encryption_enabled(&config.security)? && !paths.master_key.exists() {
        write_master_key(paths, &random_key_hex()?)?;
    }
    if config.security.encryption == "age" {
        majutsu_crypto::ensure_age_keyring(&recipients_path(paths))?;
    }
    let conn = open_db(paths)?;
    migrate(&conn)?;
    record_op(&conn, "init", None, None, Some("initialized majutsu home"))?;
    println!("initialized {}", paths.home.display());
    println!("host {} {}", config.host.name, config.host.id);
    Ok(())
}

fn snapshot(paths: &Paths, args: SnapshotArgs) -> Result<()> {
    ensure_ready(paths)?;
    let _lock = acquire_process_lock(&paths.snapshot_lock, "snapshot")?;
    record_event(
        paths,
        "snapshot-start",
        args.message.as_deref().unwrap_or("manual"),
    )?;
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let parent = current_snapshot(&conn)?;
    let parent_manifest = parent
        .as_deref()
        .map(|id| load_snapshot_by_id(&conn, id))
        .transpose()?;
    let op_id = new_id("op");
    let snapshot_id = new_id("snap");
    let mut by_root = BTreeMap::new();
    let mut root_trees = BTreeMap::new();
    let mut total_files = 0usize;
    let mut large_files = 0usize;
    for root in roots(&conn)? {
        if root.status != "active" {
            eprintln!("root {}, skipped: status={}", root.id, root.status);
            record_event(
                paths,
                "root-skipped",
                &format!("{} status={}", root.id, root.status),
            )?;
            if root.status != "deleted" {
                carry_forward_root_snapshot(
                    parent_manifest.as_ref(),
                    &root.id,
                    &mut root_trees,
                    &mut by_root,
                );
            }
            continue;
        }
        if !root.path.exists() {
            update_root_status(&conn, &root.id, "missing")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(
                &conn,
                "root-missing",
                parent.as_deref(),
                parent.as_deref(),
                Some(&root.id),
            )?;
            eprintln!("root missing, skipped: {} {}", root.id, root.path.display());
            record_event(
                paths,
                "root-missing",
                &format!("{} {}", root.id, root.path.display()),
            )?;
            carry_forward_root_snapshot(
                parent_manifest.as_ref(),
                &root.id,
                &mut root_trees,
                &mut by_root,
            );
            continue;
        }
        if root.require_mount && !is_mount_point(&root.path) {
            update_root_status(&conn, &root.id, "unmounted")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(
                &conn,
                "root-unmounted",
                parent.as_deref(),
                parent.as_deref(),
                Some(&root.id),
            )?;
            eprintln!(
                "root unmounted, skipped: {} {}",
                root.id,
                root.path.display()
            );
            record_event(
                paths,
                "root-unmounted",
                &format!("{} {}", root.id, root.path.display()),
            )?;
            carry_forward_root_snapshot(
                parent_manifest.as_ref(),
                &root.id,
                &mut root_trees,
                &mut by_root,
            );
            continue;
        }
        if let Err(err) = run_pre_snapshot_hook(paths, &root) {
            record_snapshot_failure(
                &conn,
                &op_id,
                snapshot_operation_kind(args.message.as_deref(), parent.as_deref()),
                parent.as_deref(),
                &root.id,
                &err,
            )?;
            return Err(err);
        }
        let scan_root_config = snapshot_scan_root(paths, &root)?;
        let records_result = scan_root(paths, &config, &scan_root_config);
        let post_result = run_post_snapshot_hook(paths, &root);
        let records = match records_result {
            Ok(records) => records,
            Err(err) if is_permission_denied_error(&err) => {
                update_root_status(&conn, &root.id, "permission-denied")?;
                sync_roots_to_config(paths, &conn)?;
                record_op(
                    &conn,
                    "root-permission-denied",
                    parent.as_deref(),
                    parent.as_deref(),
                    Some(&root.id),
                )?;
                eprintln!(
                    "root permission-denied, skipped: {} {}",
                    root.id,
                    root.path.display()
                );
                record_event(
                    paths,
                    "root-permission-denied",
                    &format!("{} {}", root.id, root.path.display()),
                )?;
                carry_forward_root_snapshot(
                    parent_manifest.as_ref(),
                    &root.id,
                    &mut root_trees,
                    &mut by_root,
                );
                continue;
            }
            Err(err) => {
                record_snapshot_failure(
                    &conn,
                    &op_id,
                    snapshot_operation_kind(args.message.as_deref(), parent.as_deref()),
                    parent.as_deref(),
                    &root.id,
                    &err,
                )?;
                return Err(err);
            }
        };
        if let Err(err) = post_result {
            record_snapshot_failure(
                &conn,
                &op_id,
                snapshot_operation_kind(args.message.as_deref(), parent.as_deref()),
                parent.as_deref(),
                &root.id,
                &err,
            )?;
            return Err(err);
        }
        large_files += records
            .iter()
            .filter(|r| payload_large_ref(&r.payload).is_some())
            .count();
        total_files += records
            .iter()
            .filter(|r| !matches!(r.payload, Payload::Directory))
            .count();
        let tree = build_tree_manifest(&root.id, records)?;
        let root_snapshot = if let Some(previous) = parent_manifest
            .as_ref()
            .and_then(|parent| parent.root_trees.get(&root.id))
            .filter(|previous| previous.tree_id == tree.tree_id)
        {
            previous.clone()
        } else {
            let tree_json = serde_json::to_vec_pretty(&tree)?;
            let tree_oid = blake3_hex(&tree_json);
            let tree_key = store_bytes(paths, &paths.trees, &tree_oid, &tree_json)?;
            RootSnapshot {
                tree_id: tree.tree_id.clone(),
                tree_key,
                file_count: tree.entries.len(),
            }
        };
        root_trees.insert(root.id.clone(), root_snapshot);
        by_root.insert(root.id, tree.entries.into_values().collect());
    }
    let manifest = SnapshotManifest {
        snapshot_id: snapshot_id.clone(),
        parent: parent.clone(),
        op_id: op_id.clone(),
        timestamp: Utc::now(),
        root_trees,
        roots: by_root,
    };
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let manifest_oid = blake3_hex(&manifest_json);
    let manifest_key = store_bytes(paths, &paths.objects, &manifest_oid, &manifest_json)?;
    conn.execute(
        "insert into snapshots(id, parent_id, op_id, created_at, manifest_key, manifest_json)
         values (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            snapshot_id,
            parent,
            op_id,
            manifest.timestamp.to_rfc3339(),
            manifest_key,
            String::from_utf8(manifest_json)?
        ],
    )?;
    conn.execute(
        "insert into refs(name, value) values ('current', ?1)
         on conflict(name) do update set value=excluded.value",
        params![manifest.snapshot_id],
    )?;
    record_op_with_id(
        &conn,
        &op_id,
        snapshot_operation_kind(args.message.as_deref(), manifest.parent.as_deref()),
        manifest.parent.as_deref(),
        Some(&manifest.snapshot_id),
        args.message.as_deref(),
    )?;
    println!("snapshot {}", manifest.snapshot_id);
    println!("files {total_files}, large {large_files}");
    record_event(paths, "snapshot-finish", &manifest.snapshot_id)?;
    Ok(())
}

fn snapshot_operation_kind(message: Option<&str>, parent: Option<&str>) -> &'static str {
    if message
        .map(|message| message.starts_with("watch "))
        .unwrap_or(false)
    {
        "file-events-batch"
    } else if parent.is_none() {
        "initial-scan"
    } else {
        "manual-snapshot"
    }
}

fn record_snapshot_failure(
    conn: &Connection,
    op_id: &str,
    kind: &str,
    parent: Option<&str>,
    root_id: &str,
    err: &anyhow::Error,
) -> Result<()> {
    let message = format!("snapshot failed for root {root_id}: {err:#}");
    record_op_with_details(
        conn,
        op_id,
        kind,
        parent,
        parent,
        "failed",
        Some(&message),
        Some(&message),
        None,
    )
}

pub(crate) fn remote_ref(remote: &RemoteStore, key: &str) -> Result<Option<String>> {
    if remote.exists(key)? {
        return Ok(Some(
            String::from_utf8(remote.get(key)?)?.trim().to_string(),
        ));
    }
    Ok(None)
}

pub(crate) fn decode_canonical_remote_export<T: for<'de> Deserialize<'de>>(
    paths: &Paths,
    bytes: &[u8],
) -> Result<T> {
    let compressed = decode_object(paths, bytes)?;
    let cbor = zstd::stream::decode_all(compressed.as_slice())?;
    Ok(serde_cbor::from_slice(&cbor)?)
}

pub(crate) fn decode_canonical_remote_oplog(
    paths: &Paths,
    bytes: &[u8],
) -> Result<Vec<OperationExport>> {
    let compressed = decode_object(paths, bytes)?;
    let cborl = zstd::stream::decode_all(compressed.as_slice())?;
    decode_operation_log(&cborl)
}

fn replay_pending_journal_events(paths: &Paths) -> Result<bool> {
    if !has_pending_journal_events(paths)? {
        return Ok(false);
    }
    record_event(
        paths,
        "event-journal-replay",
        "pending filesystem events after last snapshot-finish",
    )?;
    snapshot(
        paths,
        SnapshotArgs {
            message: Some("watch journal replay snapshot".into()),
        },
    )?;
    Ok(true)
}

pub(crate) fn validate_clone_remote_blob_objects(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<()> {
    for blob in &export.blobs {
        if blob.pack_id.is_some() {
            continue;
        }
        for variant in remote_local_object_variants(paths, remote, &blob.object_key)? {
            let payload = decode_object(paths, &variant.bytes).with_context(|| {
                format!(
                    "decode remote blob object {} from {}",
                    blob.object_key, variant.remote_key
                )
            })?;
            if payload.len() as u64 != blob.size {
                bail!(
                    "remote blob object size does not match metadata {} {}",
                    blob.oid,
                    variant.remote_key
                );
            }
            if blake3_hex(&payload) != blob.oid {
                bail!(
                    "remote blob object hash does not match metadata {} {}",
                    blob.oid,
                    variant.remote_key
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_clone_remote_snapshot_objects(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<()> {
    for snapshot in &export.snapshots {
        let metadata_manifest: SnapshotManifest = serde_json::from_str(&snapshot.manifest_json)
            .with_context(|| format!("parse snapshot manifest metadata {}", snapshot.id))?;
        for variant in remote_local_object_variants(paths, remote, &snapshot.manifest_key)? {
            let remote_manifest: SnapshotManifest =
                serde_json::from_slice(&decode_object(paths, &variant.bytes)?).with_context(
                    || {
                        format!(
                            "parse remote snapshot manifest {} from {}",
                            snapshot.manifest_key, variant.remote_key
                        )
                    },
                )?;
            if !snapshot_manifest_matches(&remote_manifest, &metadata_manifest) {
                bail!(
                    "remote snapshot manifest object does not match metadata {} {}",
                    snapshot.id,
                    variant.remote_key
                );
            }
        }
        for (root_id, root_tree) in &metadata_manifest.root_trees {
            for variant in remote_local_object_variants(paths, remote, &root_tree.tree_key)? {
                let tree: TreeManifest =
                    serde_json::from_slice(&decode_object(paths, &variant.bytes)?).with_context(
                        || {
                            format!(
                                "parse remote tree manifest {} from {}",
                                root_tree.tree_key, variant.remote_key
                            )
                        },
                    )?;
                if !tree_manifest_issues(&tree, root_id, root_tree).is_empty() {
                    bail!(
                        "remote tree manifest object does not match snapshot metadata {} {}",
                        snapshot.id,
                        variant.remote_key
                    );
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_clone_remote_large_objects(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<()> {
    for large in &export.large_objects {
        for variant in remote_local_object_variants(paths, remote, &large.manifest_key)? {
            let manifest: LargeManifest =
                serde_json::from_slice(&decode_object(paths, &variant.bytes)?).with_context(
                    || {
                        format!(
                            "parse remote large manifest {} from {}",
                            large.manifest_key, variant.remote_key
                        )
                    },
                )?;
            if !large_manifest_issues(&manifest, large).is_empty() {
                bail!(
                    "remote large manifest object does not match metadata {} {}",
                    large.oid,
                    variant.remote_key
                );
            }
            for chunk in &manifest.chunks {
                validate_clone_remote_large_chunk_object(paths, remote, chunk)?;
            }
        }
    }
    Ok(())
}

fn validate_clone_remote_large_chunk_object(
    paths: &Paths,
    remote: &RemoteStore,
    chunk: &LargeChunk,
) -> Result<()> {
    for variant in remote_local_object_variants(paths, remote, &chunk.object_key)? {
        let stored = decode_object(paths, &variant.bytes).with_context(|| {
            format!(
                "decode remote large chunk {} from {}",
                chunk.object_key, variant.remote_key
            )
        })?;
        if chunk
            .stored_len
            .is_some_and(|stored_len| stored.len() as u64 != stored_len)
        {
            bail!(
                "remote large chunk stored size does not match metadata {} {}",
                chunk.oid,
                variant.remote_key
            );
        }
        let payload = decode_large_chunk_stored_bytes(chunk, &stored).with_context(|| {
            format!(
                "decode remote large chunk payload {} from {}",
                chunk.object_key, variant.remote_key
            )
        })?;
        if payload.len() as u64 != chunk.len {
            bail!(
                "remote large chunk size does not match metadata {} {}",
                chunk.oid,
                variant.remote_key
            );
        }
        if blake3_hex(&payload) != chunk.oid {
            bail!(
                "remote large chunk hash does not match metadata {} {}",
                chunk.oid,
                variant.remote_key
            );
        }
    }
    Ok(())
}

pub(crate) fn validate_clone_remote_pack_objects(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<()> {
    let expected_pack_index_keys = expected_canonical_pack_index_keys(export);
    for key in remote.list("indexes/pack-index/")? {
        if !expected_pack_index_keys.contains(&key) {
            bail!("remote contains unexpected pack index object {key}");
        }
    }
    let expected_pack_object_keys = expected_canonical_pack_object_keys(export);
    for key in remote.list("packs/")? {
        if !expected_pack_object_keys.contains(&key) {
            bail!("remote contains unexpected pack object {key}");
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
        for variant in remote_local_object_variants(paths, remote, &pack.index_key)? {
            let index: PackIndex = serde_json::from_slice(&variant.bytes).with_context(|| {
                format!(
                    "parse remote pack index {} from {}",
                    pack.index_key, variant.remote_key
                )
            })?;
            for issue in pack_index_issues(pack, &index, &expected_blob_metadata) {
                match issue {
                    PackIndexIssue::PackMetadataMismatch => {
                        bail!(
                            "remote pack index does not match metadata {} {}",
                            pack.pack_id,
                            variant.remote_key
                        );
                    }
                    PackIndexIssue::EntryWithoutBlobMetadata { oid } => {
                        bail!(
                            "remote pack index entry has no matching blob metadata {} {}",
                            pack.pack_id,
                            oid
                        );
                    }
                    PackIndexIssue::EntryOffsetMismatch { oid } => {
                        bail!(
                            "remote pack index entry does not match blob offset metadata {} {}",
                            pack.pack_id,
                            oid
                        );
                    }
                    PackIndexIssue::MissingBlobEntry { oid } => {
                        bail!(
                            "packed blob missing from remote pack index {} {}",
                            pack.pack_id,
                            oid
                        );
                    }
                }
            }
        }
        for variant in remote_local_object_variants(paths, remote, &pack.pack_key)? {
            for issue in
                pack_object_issues(pack, variant.bytes.len() as u64, &expected_blob_metadata)
            {
                match issue {
                    PackObjectIssue::SizeMismatch => {
                        bail!(
                            "remote pack object size does not match metadata {} {}",
                            pack.pack_id,
                            variant.remote_key
                        );
                    }
                    PackObjectIssue::MissingBlobOffset { oid } => {
                        bail!(
                            "packed blob missing offset metadata {} {}",
                            pack.pack_id,
                            oid
                        );
                    }
                    PackObjectIssue::MissingBlobLength { oid } => {
                        bail!(
                            "packed blob missing length metadata {} {}",
                            pack.pack_id,
                            oid
                        );
                    }
                    PackObjectIssue::BlobRangeOutOfBounds { oid } => {
                        bail!(
                            "packed blob range out of remote pack bounds {} {}",
                            pack.pack_id,
                            oid
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
        bail!("packed blob references missing remote pack metadata {pack_id}");
    }
    Ok(())
}

fn expected_canonical_pack_index_keys(export: &MetadataExport) -> BTreeSet<String> {
    export
        .packs
        .iter()
        .flat_map(|pack| canonical_remote_aliases(&pack.index_key))
        .filter(|key| key.starts_with("indexes/pack-index/"))
        .collect()
}

fn expected_canonical_pack_object_keys(export: &MetadataExport) -> BTreeSet<String> {
    export
        .packs
        .iter()
        .flat_map(|pack| canonical_remote_aliases(&pack.pack_key))
        .filter(|key| key.starts_with("packs/"))
        .collect()
}

pub(crate) fn validate_clone_remote_chunk_index(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<()> {
    if export.chunks.is_empty() {
        return Ok(());
    }
    for key in remote.list("indexes/chunk-index/")? {
        if key != REMOTE_CHUNK_INDEX_SHARD_KEY {
            bail!("remote contains unexpected chunk index shard {key}");
        }
    }
    if !remote.exists(REMOTE_CHUNK_INDEX_SHARD_KEY)? {
        bail!("remote is missing chunk index shard {REMOTE_CHUNK_INDEX_SHARD_KEY}");
    }
    let shard: ChunkIndexShard =
        decode_canonical_remote_export(paths, &remote.get(REMOTE_CHUNK_INDEX_SHARD_KEY)?)
            .with_context(|| {
                format!("parse remote chunk index shard {REMOTE_CHUNK_INDEX_SHARD_KEY}")
            })?;
    if shard.version != 1 || shard.shard != majutsu_store::DEFAULT_CHUNK_INDEX_SHARD {
        bail!("remote chunk index shard metadata does not match export");
    }
    if shard.chunks.len() != export.chunks.len() {
        bail!("remote chunk index shard metadata does not match export");
    }
    if shard.has_duplicate_oids() {
        bail!("remote chunk index shard contains duplicate chunk oids");
    }
    let expected = export
        .chunks
        .iter()
        .map(|chunk| (chunk.oid.as_str(), chunk))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    for entry in &shard.chunks {
        let Some(chunk) = expected.get(entry.oid.as_str()) else {
            bail!(
                "remote chunk index entry has no matching chunk {}",
                entry.oid
            );
        };
        if !seen.insert(entry.oid.as_str()) {
            bail!("duplicate remote chunk index entry {}", entry.oid);
        }
        if entry.size != chunk.size || entry.object_key != chunk.object_key {
            bail!(
                "remote chunk index entry does not match metadata {}: index={} size={} metadata={} size={}",
                entry.oid,
                entry.object_key,
                entry.size,
                chunk.object_key,
                chunk.size
            );
        }
    }
    for oid in expected.keys() {
        if !seen.contains(oid) {
            bail!("chunk missing from remote chunk index {oid}");
        }
    }
    Ok(())
}

pub(crate) fn validate_clone_remote_timeline_exports(
    paths: &Paths,
    remote: &RemoteStore,
    host: Option<&RemoteHostSummary>,
    snapshots: &[SnapshotExport],
    operations: &[OperationExport],
) -> Result<()> {
    let Some(host) = host else {
        return Ok(());
    };
    let expected_snapshot_keys = snapshots
        .iter()
        .flat_map(|snapshot| {
            [
                host_snapshot_key(&host.id, &snapshot.id),
                host_snapshot_canonical_key(&host.id, &snapshot.id),
            ]
        })
        .collect::<BTreeSet<_>>();
    for key in remote.list(&host_snapshots_prefix(&host.id))? {
        if !expected_snapshot_keys.contains(&key) {
            bail!("remote has unexpected host snapshot export {key}");
        }
    }
    for snapshot in snapshots {
        let key = host_snapshot_canonical_key(&host.id, &snapshot.id);
        if !remote.exists(&key)? {
            bail!("remote is missing canonical host snapshot export {key}");
        }
        let actual: SnapshotExport = decode_canonical_remote_export(paths, &remote.get(&key)?)
            .with_context(|| format!("parse remote snapshot export {key}"))?;
        if !snapshot_export_matches(&actual, snapshot) {
            bail!("remote snapshot export does not match metadata {key}");
        }
    }
    let expected_operation_keys = operations
        .iter()
        .flat_map(|operation| {
            [
                host_operation_key(&host.id, &operation.id),
                host_operation_canonical_key(&host.id, &operation.id),
            ]
        })
        .chain([host_oplog_key(&host.id), host_oplog_canonical_key(&host.id)])
        .collect::<BTreeSet<_>>();
    for key in remote.list(&host_ops_prefix(&host.id))? {
        if !expected_operation_keys.contains(&key) {
            bail!("remote has unexpected host operation export {key}");
        }
    }
    for operation in operations {
        let key = host_operation_canonical_key(&host.id, &operation.id);
        if !remote.exists(&key)? {
            bail!("remote is missing canonical host operation export {key}");
        }
        let actual: OperationExport = decode_canonical_remote_export(paths, &remote.get(&key)?)
            .with_context(|| format!("parse remote operation export {key}"))?;
        if !operation_log_entry_matches(&actual, operation) {
            bail!("remote operation export does not match metadata {key}");
        }
    }
    Ok(())
}

pub(crate) fn validate_clone_remote_oplog(
    paths: &Paths,
    remote: &RemoteStore,
    host: Option<&RemoteHostSummary>,
    expected: &[OperationExport],
) -> Result<()> {
    let Some(host) = host else {
        return Ok(());
    };
    let key = host_oplog_canonical_key(&host.id);
    if !remote.exists(&key)? {
        bail!("remote is missing canonical host operation log {key}");
    }
    let actual = decode_canonical_remote_oplog(paths, &remote.get(&key)?)
        .with_context(|| format!("parse remote operation log {key}"))?;
    for issue in operation_log_comparison_issues(&actual, expected) {
        match issue {
            majutsu_core::OperationLogComparisonIssue::CountMismatch { expected, actual } => {
                bail!(
                    "remote operation log count mismatch {key} expected={expected} actual={actual}"
                );
            }
            majutsu_core::OperationLogComparisonIssue::EntryMismatch { index, id } => {
                bail!(
                    "remote operation log entry does not match metadata {key} {id} index={index}"
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_clone_host_summary(
    host: &Option<RemoteHostSummary>,
    host_index: bool,
    export: &MetadataExport,
) -> Result<()> {
    let Some(host) = host else {
        return Ok(());
    };
    if host_index {
        let expected_metadata_key = host_metadata_key(&host.id);
        if host.metadata_key != expected_metadata_key {
            bail!(
                "remote host index metadata_key {} does not match canonical key {}",
                host.metadata_key,
                expected_metadata_key
            );
        }
    }
    if host.id != export.config.host.id {
        bail!(
            "remote host index id {} does not match metadata host id {}",
            host.id,
            export.config.host.id
        );
    }
    if host.name != export.config.host.name {
        bail!(
            "remote host index name {} does not match metadata host name {}",
            host.name,
            export.config.host.name
        );
    }
    if host.current_snapshot.as_ref() != export.refs.get("current") {
        bail!("remote host index current snapshot does not match metadata");
    }
    if let Some(last_synced) = export.refs.get("last-synced") {
        let metadata_last_synced = parse_db_time(last_synced)?;
        if host.last_synced_at != metadata_last_synced {
            bail!(
                "remote host index last_synced_at {} does not match metadata last-synced {}",
                host.last_synced_at.to_rfc3339(),
                metadata_last_synced.to_rfc3339()
            );
        }
    }
    Ok(())
}

pub(crate) fn validate_clone_remote_refs(
    remote: &RemoteStore,
    host: Option<&RemoteHostSummary>,
    export: &MetadataExport,
) -> Result<()> {
    let Some(host) = host else {
        return Ok(());
    };
    let expected_ref_keys = [
        host_current_ref_key(&host.id),
        host_last_synced_ref_key(&host.id),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    for key in remote.list(&format!("hosts/{}/refs/", host.id))? {
        if !expected_ref_keys.contains(&key) {
            bail!("remote has unexpected host ref {key}");
        }
    }
    if let Some(current) = export.refs.get("current") {
        let key = host_current_ref_key(&host.id);
        match remote_ref(remote, &key)? {
            Some(remote_current) if remote_current == *current => {}
            Some(remote_current) => bail!(
                "remote ref {key} points to {remote_current}, expected metadata current {current}"
            ),
            None => bail!("remote is missing canonical host current ref {key}"),
        }
        let legacy_key = host_legacy_current_key(&host.id);
        if let Some(legacy_current) = remote_ref(remote, &legacy_key)?
            && legacy_current != *current
        {
            bail!(
                "remote ref {legacy_key} points to {legacy_current}, expected metadata current {current}"
            );
        }
    }
    if let Some(last_synced) = export.refs.get("last-synced") {
        let key = host_last_synced_ref_key(&host.id);
        match remote_ref(remote, &key)? {
            Some(remote_last_synced) if remote_last_synced == *last_synced => {}
            Some(remote_last_synced) => bail!(
                "remote ref {key} points to {remote_last_synced}, expected metadata last-synced {last_synced}"
            ),
            None => bail!("remote is missing canonical host last-synced ref {key}"),
        }
    }
    Ok(())
}

pub(crate) fn validate_clone_remote_gc_mark(
    remote: &RemoteStore,
    host: Option<&RemoteHostSummary>,
    export: &MetadataExport,
) -> Result<()> {
    let Some(host) = host else {
        return Ok(());
    };
    let key = remote_gc_mark_key(&host.id);
    if !remote.exists(&key)? {
        bail!("remote is missing gc mark {key}");
    }
    let mark: GcMarkExport = serde_json::from_slice(&remote.get(&key)?)
        .with_context(|| format!("parse remote gc mark {key}"))?;
    let mut expected = local_object_keys(export);
    for object_key in expected.clone() {
        expected.extend(canonical_remote_aliases(&object_key));
    }
    let expected = expected.into_iter().collect::<BTreeSet<_>>();
    for issue in mark.validation_issues(&host.id, export.refs.get("current"), &expected) {
        bail!("{}", issue.message(&key, &host.id));
    }
    validate_clone_remote_gc_tombstones(remote, &host.id)?;
    Ok(())
}

fn validate_clone_remote_gc_tombstones(remote: &RemoteStore, host_id: &str) -> Result<()> {
    let prefix = remote_gc_tombstone_prefix(host_id);
    let mut seen_keys = BTreeSet::new();
    for key in remote.list(&prefix)? {
        if !key.ends_with(".json") {
            continue;
        }
        let tombstone: GcTombstoneExport = serde_json::from_slice(&remote.get(&key)?)
            .with_context(|| format!("parse remote gc tombstone {key}"))?;
        let issues = tombstone.validation_issues(host_id);
        if let Some(issue) = issues.into_iter().next() {
            bail!("{}", issue.message(&key, host_id));
        }
        if remote.exists(&tombstone.key)? {
            bail!(
                "remote gc tombstone points to existing object {} {}",
                key,
                tombstone.key
            );
        }
        if !seen_keys.insert(tombstone.key.clone()) {
            bail!(
                "duplicate remote gc tombstone deleted key {}",
                tombstone.key
            );
        }
    }
    Ok(())
}

pub(crate) fn validate_clone_remote_lifecycle_artifacts(remote: &RemoteStore) -> Result<()> {
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
            bail!("remote has unexpected lifecycle artifact {key}");
        }
    }
    let has_policy = keys
        .iter()
        .any(|key| key == "lifecycle/policy-s3.json" || key == "lifecycle/policy-gcs.json");
    if has_policy && !remote.exists("lifecycle/status.json")? {
        bail!("remote lifecycle policy exists without lifecycle/status.json");
    }
    if remote.exists("lifecycle/status.json")? {
        validate_clone_remote_lifecycle_status(remote)?;
    }
    for key in ["lifecycle/policy-s3.json", "lifecycle/policy-gcs.json"] {
        if remote.exists(key)? {
            validate_clone_remote_lifecycle_policy(remote, key)?;
        }
    }
    Ok(())
}

fn validate_clone_remote_lifecycle_status(remote: &RemoteStore) -> Result<()> {
    let status_key = "lifecycle/status.json";
    let status: serde_json::Value = serde_json::from_slice(&remote.get(status_key)?)
        .with_context(|| format!("parse lifecycle status {status_key}"))?;
    let provider = match status.get("provider").and_then(|value| value.as_str()) {
        Some("s3" | "gcs") => status["provider"].as_str().unwrap(),
        Some(provider) => bail!("lifecycle status has unsupported provider {provider}"),
        None => bail!("lifecycle status is missing provider"),
    };
    let Some(policy_key) = status.get("policy_key").and_then(|value| value.as_str()) else {
        bail!("lifecycle status is missing policy_key");
    };
    let expected_policy_key = format!("lifecycle/policy-{provider}.json");
    if policy_key != expected_policy_key {
        bail!("lifecycle status policy_key {policy_key} does not match {expected_policy_key}");
    }
    if !remote.exists(policy_key)? {
        bail!("lifecycle status points to missing policy {policy_key}");
    }
    if !status
        .get("provider_applied")
        .is_some_and(|value| value.is_boolean())
    {
        bail!("lifecycle status is missing boolean provider_applied");
    }
    let Some(applied_at) = status.get("applied_at").and_then(|value| value.as_str()) else {
        bail!("lifecycle status is missing applied_at");
    };
    parse_db_time(applied_at)
        .with_context(|| format!("lifecycle status has invalid applied_at {applied_at}"))?;
    Ok(())
}

fn validate_clone_remote_lifecycle_policy(remote: &RemoteStore, key: &str) -> Result<()> {
    let policy: serde_json::Value = serde_json::from_slice(&remote.get(key)?)
        .with_context(|| format!("parse lifecycle policy {key}"))?;
    if key == "lifecycle/policy-s3.json" {
        majutsu_policy::s3_lifecycle_configuration_xml(&policy)
            .with_context(|| format!("validate S3 lifecycle policy {key}"))?;
    } else if !policy.get("rule").is_some_and(|rule| rule.is_array()) {
        bail!("invalid GCS lifecycle policy {key}: missing rule array");
    }
    Ok(())
}

pub(crate) fn validate_clone_metadata(export: &MetadataExport) -> Result<()> {
    let mut issues = Vec::new();
    if export.version != METADATA_EXPORT_VERSION {
        issues.push(format!(
            "unsupported metadata export version {}",
            export.version
        ));
    }
    let snapshot_ids = export
        .snapshots
        .iter()
        .map(|snapshot| snapshot.id.clone())
        .collect::<BTreeSet<_>>();
    for issue in majutsu_db::local_ref_issues(
        export
            .refs
            .iter()
            .map(|(name, value)| (name.clone(), value.clone())),
        &snapshot_ids,
    ) {
        issues.push(issue.message());
    }
    for operation in &export.operations {
        for issue in operation.validation_issues() {
            issues.push(format_operation_entry_issue(operation, issue));
        }
    }
    for issue in history_graph_issues(&export.snapshots, &export.operations) {
        issues.push(format_history_graph_issue(issue));
    }
    validate_clone_metadata_references(export, &mut issues);
    for issue in large_pin_issues(&export.large_pins, &export.large_objects) {
        issues.push(format_large_pin_issue(issue));
    }
    if issues.is_empty() {
        return Ok(());
    }
    let sample = issues
        .iter()
        .take(5)
        .cloned()
        .collect::<Vec<_>>()
        .join("; ");
    let suffix = if issues.len() > 5 {
        format!("; ... {} more", issues.len() - 5)
    } else {
        String::new()
    };
    bail!("remote metadata is inconsistent and cannot be cloned: {sample}{suffix}")
}

fn validate_clone_metadata_references(export: &MetadataExport, issues: &mut Vec<String>) {
    let mut live = LiveMetadataReferences::default();
    for snapshot in &export.snapshots {
        match serde_json::from_str::<SnapshotManifest>(&snapshot.manifest_json) {
            Ok(manifest) => {
                live.add_snapshot_manifest(&manifest);
            }
            Err(err) => {
                issues.push(format!(
                    "snapshot {} has invalid manifest metadata: {err}",
                    snapshot.id
                ));
            }
        }
    }
    let live_chunks = BTreeSet::new();
    for issue in metadata_reference_issues(
        export.blobs.iter().map(|blob| blob.oid.as_str()),
        export.large_objects.iter().map(|large| large.oid.as_str()),
        std::iter::empty::<&str>(),
        &live.blobs,
        &live.large_objects,
        &live_chunks,
    ) {
        issues.push(format_metadata_reference_issue(issue));
    }
}

fn format_metadata_reference_issue(issue: MetadataReferenceIssue) -> String {
    match issue {
        MetadataReferenceIssue::DanglingBlob { oid } => {
            format!("dangling blob metadata {oid}")
        }
        MetadataReferenceIssue::DanglingLargeObject { oid } => {
            format!("dangling large object metadata {oid}")
        }
        MetadataReferenceIssue::DanglingChunk { oid } => {
            format!("dangling chunk metadata {oid}")
        }
    }
}

fn format_large_pin_issue(issue: LargePinIssue) -> String {
    match issue {
        LargePinIssue::Dangling { oid, pinned_at } => {
            format!("dangling large pin {oid} pinned_at={pinned_at}")
        }
        LargePinIssue::InvalidTimestamp { oid, pinned_at } => {
            format!("invalid large pin timestamp {oid} pinned_at={pinned_at}")
        }
    }
}

fn format_operation_entry_issue(
    operation: &OperationExport,
    issue: OperationLogEntryIssue,
) -> String {
    match issue {
        OperationLogEntryIssue::InvalidId => format!("operation {} has invalid id", operation.id),
        OperationLogEntryIssue::InvalidKind(kind) => {
            format!("operation {} has invalid kind {kind}", operation.id)
        }
        OperationLogEntryIssue::InvalidStatus(status) => {
            format!("operation {} has invalid status {status}", operation.id)
        }
        OperationLogEntryIssue::InvalidRemoteSyncState(state) => {
            format!(
                "operation {} has invalid remote_sync_state {state}",
                operation.id
            )
        }
        OperationLogEntryIssue::FailedWithoutError => {
            format!("operation {} is failed without error detail", operation.id)
        }
        OperationLogEntryIssue::RemoteSyncMissingState => {
            format!(
                "operation {} is remote-sync without remote_sync_state",
                operation.id
            )
        }
        OperationLogEntryIssue::RemoteSyncStateMismatch {
            status,
            remote_sync_state,
        } => format!(
            "operation {} remote-sync status {status} does not match remote_sync_state {remote_sync_state}",
            operation.id
        ),
        OperationLogEntryIssue::EmptyActor => {
            format!("operation {} has empty actor", operation.id)
        }
        OperationLogEntryIssue::InvalidCreatedAt { value, error } => {
            format!(
                "operation {} has invalid created_at {value}: {error}",
                operation.id
            )
        }
    }
}

fn format_history_graph_issue(issue: HistoryGraphIssue) -> String {
    match issue {
        HistoryGraphIssue::SnapshotSelfParent { snapshot_id } => {
            format!("snapshot {snapshot_id} references itself as parent")
        }
        HistoryGraphIssue::SnapshotMissingOperation {
            snapshot_id,
            operation_id,
        } => {
            format!("snapshot {snapshot_id} references missing operation {operation_id}")
        }
        HistoryGraphIssue::OperationMissingParent {
            operation_id,
            parent_id,
        } => {
            format!("operation {operation_id} references missing parent operation {parent_id}")
        }
        HistoryGraphIssue::OperationSelfParent { operation_id } => {
            format!("operation {operation_id} references itself as parent")
        }
    }
}

pub(crate) fn download_local_object_from_remote(
    paths: &Paths,
    remote: &RemoteStore,
    key: &str,
) -> Result<Vec<u8>> {
    if remote.exists(key)? {
        return remote.get(key).with_context(|| format!("download {key}"));
    }
    let Some(alias) = canonical_remote_alias(key) else {
        return remote.get(key).with_context(|| format!("download {key}"));
    };
    let bytes = remote
        .get(&alias)
        .with_context(|| format!("download {key} via canonical alias {alias}"))?;
    canonical_remote_object_to_local_bytes(paths, key, &bytes)
}

fn canonical_remote_object_to_local_bytes(
    paths: &Paths,
    key: &str,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    if key.starts_with("objects/trees/") {
        let manifest: TreeManifest = decode_canonical_remote_export(paths, bytes)?;
        return Ok(encode_object(
            paths,
            &serde_json::to_vec_pretty(&manifest)?,
        )?);
    }
    if key.starts_with("objects/indexes/pack/") {
        let index: PackIndex = decode_canonical_remote_export(paths, bytes)?;
        return Ok(serde_json::to_vec_pretty(&index)?);
    }
    if key.starts_with("objects/large/manifests/") {
        let manifest: LargeManifest = decode_canonical_remote_export(paths, bytes)?;
        return Ok(encode_object(
            paths,
            &serde_json::to_vec_pretty(&manifest)?,
        )?);
    }
    Ok(bytes.to_vec())
}

pub(crate) fn remote_object_available(remote: &RemoteStore, key: &str) -> Result<bool> {
    if remote.exists(key)? {
        return Ok(true);
    }
    let Some(alias) = canonical_remote_alias(key) else {
        return Ok(false);
    };
    remote.exists(&alias)
}

fn remote_available_key(remote: &RemoteStore, key: &str) -> Result<String> {
    if remote.exists(key)? {
        return Ok(key.to_string());
    }
    if let Some(alias) = canonical_remote_alias(key) {
        if remote.exists(&alias)? {
            return Ok(alias);
        }
    }
    Ok(key.to_string())
}

pub(crate) fn packed_blob_metadata(blobs: &BTreeMap<&str, &BlobExport>) -> Vec<PackedBlobMetadata> {
    blobs
        .values()
        .map(|blob| PackedBlobMetadata {
            oid: blob.oid.clone(),
            pack_offset: blob.pack_offset,
            pack_len: blob.pack_len,
        })
        .collect()
}

pub(crate) struct RemoteObjectVariant {
    pub(crate) remote_key: String,
    pub(crate) bytes: Vec<u8>,
}

pub(crate) fn remote_local_object_variants(
    paths: &Paths,
    remote: &RemoteStore,
    key: &str,
) -> Result<Vec<RemoteObjectVariant>> {
    let mut variants = Vec::new();
    if remote.exists(key)? {
        variants.push(RemoteObjectVariant {
            remote_key: key.to_string(),
            bytes: remote.get(key).with_context(|| format!("download {key}"))?,
        });
    }
    for alias in canonical_remote_aliases(key) {
        if remote.exists(&alias)? {
            let bytes = remote
                .get(&alias)
                .with_context(|| format!("download {key} via canonical alias {alias}"))?;
            variants.push(RemoteObjectVariant {
                remote_key: alias,
                bytes: canonical_remote_object_to_local_bytes(paths, key, &bytes)?,
            });
        }
    }
    Ok(variants)
}

fn build_restore_plan(
    _paths: &Paths,
    conn: &Connection,
    args: &RestoreArgs,
) -> Result<RestorePlan> {
    if let Some(path) = &args.path {
        validate_relative_filter_path(path, "restore --path")?;
    }
    let snapshot = load_snapshot(conn, args)?;
    let root_paths = roots(conn)?
        .into_iter()
        .map(|root| (root.id, root.path))
        .collect::<BTreeMap<_, _>>();
    let mut files = Vec::new();
    let mut plan_roots = Vec::new();
    for (root_id, records) in &snapshot.roots {
        if let Some(filter_root) = &args.root {
            if filter_root != root_id {
                continue;
            }
        }
        plan_roots.push(root_id.clone());
        for record in records {
            if let Some(path_filter) = &args.path {
                if !Path::new(&record.path).starts_with(path_filter) {
                    continue;
                }
            }
            files.push(FileRecord {
                root_id: record.root_id.clone(),
                path: record.path.clone(),
                kind: record.kind.clone(),
                size: record.size,
                mode: record.mode,
                modified: record.modified,
                uid: record.uid,
                gid: record.gid,
                xattrs: record.xattrs.clone(),
                payload: match &record.payload {
                    Payload::Directory => Payload::Directory,
                    Payload::InlineSmall { oid, object_key } => Payload::InlineSmall {
                        oid: oid.clone(),
                        object_key: object_key.clone(),
                    },
                    Payload::NormalBlob { oid, object_key } => Payload::NormalBlob {
                        oid: oid.clone(),
                        object_key: object_key.clone(),
                    },
                    Payload::ChunkedBlob {
                        oid,
                        manifest_key,
                        chunk_count,
                    } => Payload::ChunkedBlob {
                        oid: oid.clone(),
                        manifest_key: manifest_key.clone(),
                        chunk_count: *chunk_count,
                    },
                    Payload::LargeObject {
                        oid,
                        manifest_key,
                        chunk_count,
                        media_type,
                        binary,
                        chunking,
                        compression,
                        encryption,
                        storage_tier_hint,
                        hydrate_policy,
                    } => Payload::LargeObject {
                        oid: oid.clone(),
                        manifest_key: manifest_key.clone(),
                        chunk_count: *chunk_count,
                        media_type: media_type.clone(),
                        binary: *binary,
                        chunking: chunking.clone(),
                        compression: compression.clone(),
                        encryption: encryption.clone(),
                        storage_tier_hint: storage_tier_hint.clone(),
                        hydrate_policy: hydrate_policy.clone(),
                    },
                    Payload::Blob { oid, object_key } => Payload::Blob {
                        oid: oid.clone(),
                        object_key: object_key.clone(),
                    },
                    Payload::Large {
                        oid,
                        manifest_key,
                        chunk_count,
                    } => Payload::Large {
                        oid: oid.clone(),
                        manifest_key: manifest_key.clone(),
                        chunk_count: *chunk_count,
                    },
                    Payload::Symlink { target } => Payload::Symlink {
                        target: target.clone(),
                    },
                    Payload::Special { special_kind } => Payload::Special {
                        special_kind: special_kind.clone(),
                    },
                },
            });
        }
    }
    let deletes = build_restore_deletes(args, &root_paths, &plan_roots, &files)?;
    Ok(RestorePlan {
        snapshot,
        to: args.to.clone(),
        root_paths,
        files,
        deletes,
    })
}

fn build_restore_deletes(
    args: &RestoreArgs,
    root_paths: &BTreeMap<String, PathBuf>,
    root_ids: &[String],
    files: &[FileRecord],
) -> Result<Vec<RestoreDelete>> {
    let mut snapshot_paths: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for record in files {
        snapshot_paths
            .entry(record.root_id.clone())
            .or_default()
            .insert(record.path.clone());
    }
    let mut deletes = Vec::new();
    for root_id in root_ids {
        if let Some(filter_root) = &args.root {
            if filter_root != root_id {
                continue;
            }
        }
        let base = restore_root_base(args.to.as_ref(), root_paths, root_id)?;
        let scan_base = args
            .path
            .as_ref()
            .map(|path| base.join(path))
            .unwrap_or_else(|| base.clone());
        if !scan_base.try_exists()? {
            continue;
        }
        for entry in WalkDir::new(&scan_base).follow_links(false) {
            let entry = entry?;
            if entry.file_type().is_dir() {
                continue;
            }
            let rel = entry.path().strip_prefix(&base)?.to_path_buf();
            let rel_s = path_to_slash(&rel);
            if !snapshot_paths
                .get(root_id)
                .map(|paths| paths.contains(&rel_s))
                .unwrap_or(false)
            {
                deletes.push(RestoreDelete {
                    root_id: root_id.clone(),
                    path: rel_s,
                });
            }
        }
    }
    deletes.sort_by(|a, b| {
        a.root_id
            .cmp(&b.root_id)
            .then_with(|| b.path.len().cmp(&a.path.len()))
            .then_with(|| a.path.cmp(&b.path))
    });
    Ok(deletes)
}

fn build_restore_job(
    paths: &Paths,
    plan: &RestorePlan,
    args: &RestoreArgs,
) -> Result<RestoreQueueItem> {
    let conn = open_db(paths)?;
    let required_objects = required_object_keys_for_plan(paths, &conn, plan)?;
    let remote = read_config(paths)
        .ok()
        .and_then(|config| config.remote.and_then(|remote| open_remote(&remote).ok()));
    let availability = classify_restore_object_availability(
        required_objects,
        |key| -> Result<bool> { Ok(paths.home.join(key).exists()) },
        |key| -> Result<bool> {
            Ok(remote
                .as_ref()
                .and_then(|remote| remote_object_available(remote, key).ok())
                .unwrap_or(false))
        },
    )?;
    Ok(RestoreQueueItem {
        id: new_id("restore"),
        snapshot_id: plan.snapshot.snapshot_id.clone(),
        root: args.root.clone(),
        path: args.path.as_ref().map(|path| path_to_slash(path)),
        target: args
            .to
            .as_ref()
            .map(|to| to.display().to_string())
            .unwrap_or_else(|| "original-roots".into()),
        required_objects: availability.required_objects,
        archived_objects: availability.archived_objects,
        missing_objects: availability.missing_objects,
        archive_requested_objects: Vec::new(),
        force: args.force,
        check_conflicts: args.check_conflicts,
        created_at: Utc::now(),
        status: "prepared".into(),
    })
}

fn request_archive_restore_for_job(paths: &Paths, job: &mut RestoreQueueItem) -> Result<()> {
    if job.archived_objects.is_empty() {
        return Ok(());
    }
    let config = read_config(paths)?;
    validate_restore_archive_config(&config.restore.archive)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(());
    };
    let remote = open_remote(remote_config)?;
    let mut requested = Vec::new();
    for key in &job.archived_objects {
        let restore_key = remote_available_key(&remote, key)?;
        if remote.restore_archive(
            &restore_key,
            config.restore.archive.days,
            &config.restore.archive.tier,
        )? {
            requested.push(key.clone());
        }
    }
    job.mark_archive_requested(requested);
    Ok(())
}

fn hydrate_restore_job_objects(paths: &Paths, job: &mut RestoreQueueItem) -> Result<()> {
    if job.archived_objects.is_empty() {
        return Ok(());
    }
    let config = read_config(paths)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(());
    };
    let remote = open_remote(remote_config)?;
    let mut still_pending = Vec::new();
    let mut hydrated = Vec::new();
    for key in &job.archived_objects {
        let dest = paths.home.join(key);
        if dest.exists() {
            hydrated.push(key.clone());
            continue;
        }
        if hydrate_packed_blobs_from_remote(paths, &remote, key)? {
            hydrated.push(key.clone());
            continue;
        }
        if !remote_object_available(&remote, key)? {
            still_pending.push(key.clone());
            continue;
        }
        match download_local_object_from_remote(paths, &remote, key) {
            Ok(bytes) => {
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&dest, bytes).with_context(|| format!("hydrate restore object {key}"))?;
                hydrated.push(key.clone());
            }
            Err(_) => still_pending.push(key.clone()),
        }
    }
    job.archived_objects = still_pending;
    job.mark_ready_if_archives_hydrated();
    write_restore_job(paths, job)?;
    if !hydrated.is_empty() {
        record_event(
            paths,
            "restore-hydrate",
            &format!("{} hydrated_objects={}", job.id, hydrated.len()),
        )?;
    }
    Ok(())
}

fn hydrate_packed_blobs_from_remote(
    paths: &Paths,
    remote: &RemoteStore,
    pack_key: &str,
) -> Result<bool> {
    let conn = open_db(paths)?;
    let Some(pack) = query_packs(&conn)?
        .into_iter()
        .find(|pack| pack.pack_key == pack_key)
    else {
        return Ok(false);
    };
    if paths.home.join(&pack.pack_key).exists() {
        return Ok(false);
    }
    let remote_pack_key = remote_available_key(remote, &pack.pack_key)?;
    if !remote_object_available(remote, &pack.pack_key)? {
        return Ok(false);
    }
    let blobs = query_blobs(&conn)?
        .into_iter()
        .filter(|blob| blob.pack_id.as_deref() == Some(pack.pack_id.as_str()))
        .collect::<Vec<_>>();
    for blob in blobs {
        if paths.home.join(&blob.object_key).exists() {
            continue;
        }
        let offset = blob
            .pack_offset
            .ok_or_else(|| anyhow!("missing pack offset for {}", blob.oid))?;
        let len = blob
            .pack_len
            .ok_or_else(|| anyhow!("missing pack len for {}", blob.oid))?;
        let entry = remote
            .get_range(&remote_pack_key, offset, len)
            .with_context(|| format!("download packed blob {} from {}", blob.oid, pack.pack_key))?;
        let encoded = pack_entry_payload(&blob.oid, &entry)?;
        let decoded = decode_object(paths, encoded)
            .with_context(|| format!("decode packed blob {}", blob.oid))?;
        if decoded.len() as u64 != blob.size || blake3_hex(&decoded) != blob.oid {
            bail!("packed blob range hash mismatch {}", blob.oid);
        }
        let dest = paths.home.join(&blob.object_key);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(dest, encoded)?;
    }
    Ok(true)
}

fn pack_entry_payload<'a>(oid: &str, entry: &'a [u8]) -> Result<&'a [u8]> {
    if entry.len() < 8 {
        bail!("pack entry too short for {oid}");
    }
    let mut len_bytes = [0u8; 8];
    len_bytes.copy_from_slice(&entry[..8]);
    let stored_len = u64::from_le_bytes(len_bytes) as usize;
    if stored_len != entry.len() - 8 {
        bail!("pack entry length mismatch for {oid}");
    }
    Ok(&entry[8..])
}

fn scan_root(paths: &Paths, config: &Config, root: &RootConfig) -> Result<Vec<FileRecord>> {
    let ignore = build_ignore(root)?;
    let mut records = Vec::new();
    let walker = WalkDir::new(&root.path)
        .follow_links(root.follow_symlinks)
        .sort_by_file_name();
    for entry in walker {
        let entry = entry?;
        if entry.path() == root.path {
            continue;
        }
        let rel = entry.path().strip_prefix(&root.path)?.to_path_buf();
        if !is_included(&root.include, &rel) {
            continue;
        }
        if is_ignored(&ignore, &rel, entry.file_type().is_dir()) {
            if entry.file_type().is_dir() {
                continue;
            }
            continue;
        }
        let rel_s = path_to_slash(&rel);
        if entry.file_type().is_dir() {
            let meta = fs::symlink_metadata(entry.path())?;
            records.push(FileRecord {
                root_id: root.id.clone(),
                path: rel_s,
                kind: "directory".into(),
                size: 0,
                mode: file_mode(&meta),
                modified: modified_secs(&meta),
                uid: file_uid(&meta),
                gid: file_gid(&meta),
                xattrs: read_xattrs(entry.path()),
                payload: Payload::Directory,
            });
            continue;
        }
        let link_meta = fs::symlink_metadata(entry.path())?;
        if link_meta.file_type().is_symlink() && !root.follow_symlinks {
            let target = fs::read_link(entry.path())?.to_string_lossy().to_string();
            records.push(FileRecord {
                root_id: root.id.clone(),
                path: rel_s,
                kind: "symlink".into(),
                size: 0,
                mode: file_mode(&link_meta),
                modified: modified_secs(&link_meta),
                uid: file_uid(&link_meta),
                gid: file_gid(&link_meta),
                xattrs: BTreeMap::new(),
                payload: Payload::Symlink { target },
            });
            continue;
        }
        if let Some(special_kind) = special_file_kind(&link_meta) {
            records.push(FileRecord {
                root_id: root.id.clone(),
                path: rel_s,
                kind: "special".into(),
                size: 0,
                mode: file_mode(&link_meta),
                modified: modified_secs(&link_meta),
                uid: file_uid(&link_meta),
                gid: file_gid(&link_meta),
                xattrs: read_xattrs(entry.path()),
                payload: Payload::Special { special_kind },
            });
            continue;
        }
        let meta = if link_meta.file_type().is_symlink() {
            fs::metadata(entry.path())?
        } else {
            link_meta
        };
        if !meta.is_file() {
            continue;
        }
        let large_config = effective_large_config(config, root);
        let binary = looks_binary(entry.path()).unwrap_or(false);
        let large = classify_large(&large_config, &rel, meta.len(), binary);
        let payload = if large {
            let (oid, manifest_key, chunk_count) =
                store_large_file(paths, entry.path(), &rel, &large_config, binary)?;
            Payload::LargeObject {
                oid,
                manifest_key,
                chunk_count,
                media_type: media_type_for_path(&rel),
                binary,
                chunking: large_config.default_chunking.clone(),
                compression: large_pointer_compression(&large_config),
                encryption: config.security.encryption.clone(),
                storage_tier_hint: "hot-manifest-cold-chunks".into(),
                hydrate_policy: "on-demand".into(),
            }
        } else if large_config.enabled && meta.len() >= large_config.binary_min_size {
            let (oid, manifest_key, chunk_count) =
                store_large_file(paths, entry.path(), &rel, &large_config, binary)?;
            Payload::ChunkedBlob {
                oid,
                manifest_key,
                chunk_count,
            }
        } else {
            let bytes = stable_read(entry.path(), root.snapshot_mode.as_str())?;
            let oid = blake3_hex(&bytes);
            let object_key = store_bytes(paths, &paths.objects, &oid, &bytes)?;
            let conn = open_db(paths)?;
            conn.execute(
                "insert or ignore into blobs(oid, size, object_key) values (?1, ?2, ?3)",
                params![oid, bytes.len() as u64, object_key],
            )?;
            if bytes.len() as u64 <= majutsu_pack::SMALL_BLOB_MAX_SIZE {
                Payload::InlineSmall { oid, object_key }
            } else {
                Payload::NormalBlob { oid, object_key }
            }
        };
        records.push(FileRecord {
            root_id: root.id.clone(),
            path: rel_s,
            kind: "file".into(),
            size: meta.len(),
            mode: file_mode(&meta),
            modified: modified_secs(&meta),
            uid: file_uid(&meta),
            gid: file_gid(&meta),
            xattrs: read_xattrs(entry.path()),
            payload,
        });
    }
    Ok(records)
}

fn is_permission_denied_error(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
        {
            return true;
        }
        if cause
            .downcast_ref::<walkdir::Error>()
            .and_then(|walkdir| walkdir.io_error())
            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
        {
            return true;
        }
    }
    false
}

pub(crate) fn build_tree_manifest(root_id: &str, records: Vec<FileRecord>) -> Result<TreeManifest> {
    let mut entries = BTreeMap::new();
    for record in records {
        entries.insert(record.path.clone(), record);
    }
    let identity = serde_json::to_vec(&entries)?;
    Ok(TreeManifest {
        version: 1,
        tree_id: format!("tree-{}", blake3_hex(&identity)),
        root_id: root_id.to_string(),
        created_at: Utc::now(),
        entries,
    })
}

fn store_large_file(
    paths: &Paths,
    path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    if config.default_chunking == "fixed" {
        return store_large_file_fixed_streaming(paths, path, rel, config, binary);
    }
    store_large_file_buffered(paths, path, rel, config, binary)
}

fn store_large_file_buffered(
    paths: &Paths,
    path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    let bytes = stable_read(path, "strict")?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes);
    let mut chunks = Vec::new();
    let ranges =
        majutsu_large::chunk_ranges_for_bytes(&config.default_chunking, config.chunk_size, &bytes);
    for (index, (start, end)) in ranges.into_iter().enumerate() {
        let chunk = &bytes[start..end];
        let chunk_oid = blake3_hex(chunk);
        let stored = compress_large_chunk(config, rel, chunk)?;
        let object_key = store_bytes(
            paths,
            &large_chunk_base(paths, &config.default_chunking),
            &chunk_oid,
            &stored.bytes,
        )?;
        chunks.push(LargeChunk {
            index,
            offset: start as u64,
            len: chunk.len() as u64,
            stored_len: Some(stored.bytes.len() as u64),
            compression: stored.compression,
            oid: chunk_oid.clone(),
            object_key: object_key.clone(),
        });
        let conn = open_db(paths)?;
        conn.execute(
            "insert or ignore into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk_oid, chunk.len() as u64, object_key],
        )?;
    }
    let oid = hasher.finalize().to_hex().to_string();
    let manifest = LargeManifest {
        version: 1,
        oid: oid.clone(),
        size: bytes.len() as u64,
        media_type: media_type_for_path(rel),
        binary,
        chunking: config.default_chunking.clone(),
        chunk_size: config.chunk_size,
        chunks,
    };
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let manifest_oid = blake3_hex(&manifest_json);
    let manifest_key = store_bytes(paths, &paths.large_manifests, &manifest_oid, &manifest_json)?;
    let conn = open_db(paths)?;
    conn.execute(
        "insert or ignore into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
        params![oid, bytes.len() as u64, manifest.chunks.len(), manifest_key],
    )?;
    Ok((oid, manifest_key, manifest.chunks.len()))
}

fn store_large_file_fixed_streaming(
    paths: &Paths,
    path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    let attempts = 8;
    let mut last_error = None;
    for _ in 0..attempts {
        match store_large_file_fixed_streaming_once(paths, path, rel, config, binary) {
            Ok(result) => return Ok(result),
            Err(err) if is_file_changed_error(&err) => {
                last_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("file changed while reading: {}", path.display())))
}

fn store_large_file_fixed_streaming_once(
    paths: &Paths,
    path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    let before = fs::metadata(path)?;
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut chunks = Vec::new();
    let mut buffer = vec![0u8; config.chunk_size.max(1)];
    let mut offset = 0u64;
    let mut index = 0usize;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        let chunk = &buffer[..n];
        hasher.update(chunk);
        let chunk_oid = blake3_hex(chunk);
        let stored = compress_large_chunk(config, rel, chunk)?;
        let object_key = store_bytes(
            paths,
            &large_chunk_base(paths, &config.default_chunking),
            &chunk_oid,
            &stored.bytes,
        )?;
        chunks.push(LargeChunk {
            index,
            offset,
            len: n as u64,
            stored_len: Some(stored.bytes.len() as u64),
            compression: stored.compression,
            oid: chunk_oid,
            object_key,
        });
        offset += n as u64;
        index += 1;
    }
    let after = fs::metadata(path)?;
    if !stable_metadata_matches(&before, &after) {
        bail!("file changed while reading: {}", path.display());
    }
    let oid = hasher.finalize().to_hex().to_string();
    let manifest = LargeManifest {
        version: 1,
        oid: oid.clone(),
        size: offset,
        media_type: media_type_for_path(rel),
        binary,
        chunking: config.default_chunking.clone(),
        chunk_size: config.chunk_size,
        chunks,
    };
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let manifest_oid = blake3_hex(&manifest_json);
    let manifest_key = store_bytes(paths, &paths.large_manifests, &manifest_oid, &manifest_json)?;
    let conn = open_db(paths)?;
    for chunk in &manifest.chunks {
        conn.execute(
            "insert or ignore into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk.oid, chunk.len, chunk.object_key],
        )?;
    }
    conn.execute(
        "insert or ignore into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
        params![oid, manifest.size, manifest.chunks.len(), manifest_key],
    )?;
    Ok((oid, manifest_key, manifest.chunks.len()))
}

fn compress_large_chunk(
    config: &LargeConfig,
    rel: &Path,
    bytes: &[u8],
) -> Result<majutsu_large::StoredLargeChunk> {
    let name = rel.file_name().and_then(OsStr::to_str).unwrap_or_default();
    Ok(majutsu_large::compress_chunk_if_useful(
        bytes,
        config.compression.enabled,
        &config.compression.algorithm,
        config.compression.level,
        config.compression.sample_bytes,
        config.compression.min_gain_ratio,
        &config.compression.skip_extensions,
        name,
    )?)
}

fn read_large_chunk(paths: &Paths, chunk: &LargeChunk) -> Result<Vec<u8>> {
    let bytes = read_object(paths, &chunk.object_key)?;
    decode_large_chunk_stored_bytes(chunk, &bytes)
}

pub(crate) fn decode_large_chunk_stored_bytes(chunk: &LargeChunk, bytes: &[u8]) -> Result<Vec<u8>> {
    match chunk.compression.as_str() {
        "none" => Ok(bytes.to_vec()),
        "zstd" => Ok(zstd::stream::decode_all(bytes)?),
        other => bail!("unsupported large chunk compression: {other}"),
    }
}

pub(crate) fn create_layout(paths: &Paths) -> Result<()> {
    fs::create_dir_all(paths.db.parent().unwrap())?;
    fs::create_dir_all(&paths.objects)?;
    fs::create_dir_all(&paths.trees)?;
    fs::create_dir_all(&paths.large_chunks)?;
    fs::create_dir_all(paths.home.join("objects/large/chunks/fastcdc"))?;
    fs::create_dir_all(&paths.large_manifests)?;
    fs::create_dir_all(&paths.packs)?;
    fs::create_dir_all(paths.home.join("objects/packs/small"))?;
    fs::create_dir_all(&paths.pack_indexes)?;
    fs::create_dir_all(&paths.logs)?;
    for dir in [
        "ops",
        "queue/events",
        "queue/uploads",
        "queue/restores",
        "cache",
        "cache/blobs",
        "cache/large",
        "cache/packs",
        "cache/indexes",
        "keys",
        "locks",
        "runtime",
    ] {
        fs::create_dir_all(paths.home.join(dir))?;
    }
    let recipients = paths.home.join("keys/recipients.toml");
    if !recipients.exists() {
        fs::write(recipients, "recipients = []\n")?;
    }
    let log = paths.home.join("logs/majutsu.log");
    if !log.exists() {
        File::create(log)?;
    }
    Ok(())
}

fn ensure_ready(paths: &Paths) -> Result<()> {
    if !paths.config.exists() || !paths.db.exists() {
        bail!("majutsu home is not initialized: run `mj init`");
    }
    Ok(())
}

fn open_db(paths: &Paths) -> Result<Connection> {
    if let Some(parent) = paths.db.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(&paths.db)?;
    migrate(&conn)?;
    if paths.config.exists() {
        let config = read_config(paths)?;
        sync_config_roots(paths, &conn, &config)?;
    }
    Ok(conn)
}

fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(majutsu_db::schema_sql())?;
    for migration in majutsu_db::compat_migrations() {
        let _ = conn.execute(migration, []);
    }
    Ok(())
}

pub(crate) fn export_metadata(conn: &Connection, config: &Config) -> Result<MetadataExport> {
    let roots = roots(conn)?;
    let config_roots = roots.iter().map(ConfigRoot::from).collect();
    let mut snapshots = Vec::new();
    let mut stmt = conn.prepare(
        "select id, parent_id, op_id, created_at, manifest_key, manifest_json from snapshots order by created_at",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(SnapshotExport {
            id: row.get(0)?,
            parent_id: row.get(1)?,
            op_id: row.get(2)?,
            created_at: row.get(3)?,
            manifest_key: row.get(4)?,
            manifest_json: row.get(5)?,
        })
    })?;
    for row in rows {
        snapshots.push(row?);
    }

    let operations = query_operations(conn)?;

    let mut refs = BTreeMap::new();
    let mut stmt = conn.prepare("select name, value from refs order by name")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (name, value) = row?;
        refs.insert(name, value);
    }

    Ok(MetadataExport {
        version: METADATA_EXPORT_VERSION,
        exported_at: Utc::now(),
        config: Config {
            host: HostConfig {
                id: config.host.id.clone(),
                name: config.host.name.clone(),
            },
            remote: config.remote.clone(),
            roots: config_roots,
            large: LargeConfig {
                enabled: config.large.enabled,
                min_size: config.large.min_size,
                binary_min_size: config.large.binary_min_size,
                default_chunking: config.large.default_chunking.clone(),
                chunk_size: config.large.chunk_size,
                max_parallel_uploads: config.large.max_parallel_uploads,
                multipart: config.large.multipart,
                always: config.large.always.clone(),
                never: config.large.never.clone(),
                compression: config.large.compression.clone(),
            },
            pack: config.pack.clone(),
            watch: config.watch.clone(),
            security: config.security.clone(),
            tiering: config.tiering.clone(),
            restore: config.restore.clone(),
        },
        roots,
        snapshots,
        operations,
        refs,
        blobs: query_blobs(conn)?,
        large_objects: query_large_objects(conn)?,
        chunks: query_chunks(conn)?,
        packs: query_packs(conn)?,
        large_pins: query_large_pins(conn)?,
    })
}

fn import_metadata(conn: &Connection, export: &MetadataExport) -> Result<()> {
    for root in &export.roots {
        conn.execute(
            "insert or replace into roots(id, data_json) values (?1, ?2)",
            params![root.id, serde_json::to_string(root)?],
        )?;
    }
    for snapshot in &export.snapshots {
        conn.execute(
            "insert or replace into snapshots(id, parent_id, op_id, created_at, manifest_key, manifest_json)
             values (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                snapshot.id,
                snapshot.parent_id,
                snapshot.op_id,
                snapshot.created_at,
                snapshot.manifest_key,
                snapshot.manifest_json
            ],
        )?;
    }
    for op in &export.operations {
        conn.execute(
            "insert or replace into operations(id, parent_op, kind, actor, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state)
             values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                op.id,
                op.parent_op,
                op.kind,
                op.actor,
                op.status,
                op.before_snapshot,
                op.after_snapshot,
                op.created_at,
                op.message,
                op.error,
                op.remote_sync_state
            ],
        )?;
    }
    for (name, value) in &export.refs {
        conn.execute(
            "insert or replace into refs(name, value) values (?1, ?2)",
            params![name, value],
        )?;
    }
    for blob in &export.blobs {
        conn.execute(
            "insert or replace into blobs(oid, size, object_key, pack_id, pack_offset, pack_len) values (?1, ?2, ?3, ?4, ?5, ?6)",
            params![blob.oid, blob.size, blob.object_key, blob.pack_id, blob.pack_offset, blob.pack_len],
        )?;
    }
    for pack in &export.packs {
        conn.execute(
            "insert or replace into packs(pack_id, pack_key, index_key, object_count, size) values (?1, ?2, ?3, ?4, ?5)",
            params![pack.pack_id, pack.pack_key, pack.index_key, pack.object_count, pack.size],
        )?;
    }
    for large in &export.large_objects {
        conn.execute(
            "insert or replace into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
            params![large.oid, large.size, large.chunk_count, large.manifest_key],
        )?;
    }
    for chunk in &export.chunks {
        conn.execute(
            "insert or replace into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk.oid, chunk.size, chunk.object_key],
        )?;
    }
    for pin in &export.large_pins {
        conn.execute(
            "insert or replace into large_pins(oid, pinned_at, reason) values (?1, ?2, ?3)",
            params![pin.oid, pin.pinned_at, pin.reason],
        )?;
    }
    rewrite_local_oplog(conn)?;
    Ok(())
}

pub(crate) fn query_blobs(conn: &Connection) -> Result<Vec<BlobExport>> {
    let mut stmt = conn.prepare(
        "select oid, size, object_key, pack_id, pack_offset, pack_len from blobs order by oid",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(BlobExport {
            oid: row.get(0)?,
            size: row.get(1)?,
            object_key: row.get(2)?,
            pack_id: row.get(3)?,
            pack_offset: row.get(4)?,
            pack_len: row.get(5)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_packs(conn: &Connection) -> Result<Vec<PackExport>> {
    let mut stmt = conn.prepare(
        "select pack_id, pack_key, index_key, object_count, size from packs order by pack_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(PackExport {
            pack_id: row.get(0)?,
            pack_key: row.get(1)?,
            index_key: row.get(2)?,
            object_count: row.get(3)?,
            size: row.get(4)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(crate) fn query_large_objects(conn: &Connection) -> Result<Vec<LargeObjectExport>> {
    let mut stmt = conn
        .prepare("select oid, size, chunk_count, manifest_key from large_objects order by oid")?;
    let rows = stmt.query_map([], |row| {
        Ok(LargeObjectExport {
            oid: row.get(0)?,
            size: row.get(1)?,
            chunk_count: row.get(2)?,
            manifest_key: row.get(3)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(crate) fn query_chunks(conn: &Connection) -> Result<Vec<ChunkExport>> {
    let mut stmt = conn.prepare("select oid, size, object_key from chunks order by oid")?;
    let rows = stmt.query_map([], |row| {
        Ok(ChunkExport {
            oid: row.get(0)?,
            size: row.get(1)?,
            object_key: row.get(2)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_large_pins(conn: &Connection) -> Result<Vec<LargePinExport>> {
    let mut stmt = conn.prepare("select oid, pinned_at, reason from large_pins order by oid")?;
    let rows = stmt.query_map([], |row| {
        Ok(LargePinExport {
            oid: row.get(0)?,
            pinned_at: row.get(1)?,
            reason: row.get(2)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn run_pre_snapshot_hook(paths: &Paths, root: &RootConfig) -> Result<()> {
    if root.snapshot_mode == "transactional" {
        run_application_plugin(paths, root, "pre")?;
        if let Some(command) = &root.pre_snapshot {
            record_event(paths, "pre-snapshot", &format!("{} {}", root.id, command))?;
            run_hook(command, &root.path)?;
        }
    }
    Ok(())
}

fn run_post_snapshot_hook(paths: &Paths, root: &RootConfig) -> Result<()> {
    if root.snapshot_mode == "transactional" {
        if let Some(command) = &root.post_snapshot {
            record_event(paths, "post-snapshot", &format!("{} {}", root.id, command))?;
            run_hook(command, &root.path)?;
        }
        run_application_plugin(paths, root, "post")?;
    }
    Ok(())
}

fn run_application_plugin(paths: &Paths, root: &RootConfig, phase: &str) -> Result<()> {
    let Some(command) = &root.application_plugin else {
        return Ok(());
    };
    record_event(
        paths,
        &format!("application-plugin-{phase}"),
        &format!("{} {}", root.id, command),
    )?;
    let mut process = ProcessCommand::new("sh");
    process
        .arg("-c")
        .arg(command)
        .current_dir(&root.path)
        .env("MAJUTSU_HOME", &paths.home)
        .env("MAJUTSU_PLUGIN_PHASE", phase)
        .env("MAJUTSU_ROOT_ID", &root.id)
        .env("MAJUTSU_ROOT_NAME", &root.name)
        .env("MAJUTSU_ROOT_PATH", &root.path);
    if let Some(source) = &root.snapshot_source {
        process.env("MAJUTSU_SNAPSHOT_SOURCE", source);
    }
    let status = process.status()?;
    if !status.success() {
        bail!("application plugin failed during {phase}: {command}");
    }
    Ok(())
}

fn snapshot_scan_root(paths: &Paths, root: &RootConfig) -> Result<RootConfig> {
    let Some(source) = &root.snapshot_source else {
        return Ok(root.clone());
    };
    if root.snapshot_mode != "transactional" {
        bail!(
            "snapshot source requires transactional snapshot mode for root {}",
            root.id
        );
    }
    if !source.exists() {
        bail!(
            "snapshot source does not exist for root {}: {}",
            root.id,
            source.display()
        );
    }
    if !source.is_dir() {
        bail!(
            "snapshot source is not a directory for root {}: {}",
            root.id,
            source.display()
        );
    }
    record_event(
        paths,
        "snapshot-source",
        &format!("{} {}", root.id, source.display()),
    )?;
    let mut scan_root = root.clone();
    scan_root.path = source.clone();
    Ok(scan_root)
}

fn run_hook(command: &str, cwd: &Path) -> Result<()> {
    let status = ProcessCommand::new("sh")
        .arg("-c")
        .arg(command)
        .current_dir(cwd)
        .status()?;
    if !status.success() {
        bail!("snapshot hook failed: {command}");
    }
    Ok(())
}

fn is_file_changed_error(err: &anyhow::Error) -> bool {
    err.to_string().starts_with("file changed while reading:")
}

pub(crate) fn store_bytes(paths: &Paths, base: &Path, oid: &str, bytes: &[u8]) -> Result<String> {
    let storage_id = object_storage_id(paths, oid)?;
    let (a, b) = storage_id.split_at(2);
    let dir = base.join(a);
    fs::create_dir_all(&dir)?;
    let path = dir.join(b);
    if !path.exists() {
        let tmp = path.with_extension("tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(&encode_object(paths, bytes)?)?;
        f.sync_all()?;
        fs::rename(tmp, &path)?;
    }
    let rel = path.strip_prefix(&paths.home).unwrap_or(&path);
    Ok(path_to_slash(rel))
}

fn object_storage_id(paths: &Paths, oid: &str) -> Result<String> {
    if !object_keys_are_hmac(paths)? {
        return Ok(oid.to_string());
    }
    let key_hex = read_master_key(paths)?;
    let key_bytes = hex::decode(key_hex.trim())?;
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&key_bytes)?;
    mac.update(b"majutsu-object-key-v1\0");
    mac.update(oid.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn object_keys_are_hmac(paths: &Paths) -> Result<bool> {
    if !paths.config.exists() {
        return Ok(false);
    }
    encryption_enabled(&read_config(paths)?.security)
}

fn write_large_chunks_atomic(paths: &Paths, dest: &Path, manifest: &LargeManifest) -> Result<()> {
    write_atomic_with(dest, |file| {
        for chunk in &manifest.chunks {
            file.write_all(&read_large_chunk(paths, chunk)?)?;
        }
        Ok(())
    })
}

pub(crate) fn encode_object(paths: &Paths, bytes: &[u8]) -> Result<Vec<u8>> {
    let config = if paths.config.exists() {
        Some(read_config(paths)?)
    } else {
        None
    };
    if config
        .as_ref()
        .map(|config| encryption_enabled(&config.security))
        .transpose()?
        .unwrap_or(false)
    {
        let mode = config
            .as_ref()
            .map(|config| encryption_mode(&config.security))
            .transpose()?
            .unwrap_or(EncryptionMode::None);
        majutsu_crypto::encode_object(bytes, mode, &paths.master_key, &recipients_path(paths))
    } else {
        Ok(bytes.to_vec())
    }
}

pub(crate) fn read_object(paths: &Paths, key: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(paths.home.join(key))?;
    decode_object(paths, &bytes)
}

pub(crate) fn read_blob_payload(
    paths: &Paths,
    conn: &Connection,
    oid: &str,
    fallback_key: &str,
) -> Result<Vec<u8>> {
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
        let offset = blob
            .pack_offset
            .ok_or_else(|| anyhow!("missing pack offset for {oid}"))? as usize;
        let len = blob
            .pack_len
            .ok_or_else(|| anyhow!("missing pack len for {oid}"))? as usize;
        if !paths.home.join(&pack.pack_key).exists() && paths.home.join(fallback_key).exists() {
            return read_object(paths, fallback_key);
        }
        let bytes = fs::read(paths.home.join(pack.pack_key))?;
        let slice = bytes
            .get(offset..offset + len)
            .ok_or_else(|| anyhow!("pack entry out of range for {oid}"))?;
        decode_object(paths, pack_entry_payload(oid, slice)?)
    } else {
        read_object(paths, fallback_key)
    }
}

pub(crate) fn decode_object(paths: &Paths, bytes: &[u8]) -> Result<Vec<u8>> {
    majutsu_crypto::decode_object(bytes, &paths.master_key, &recipients_path(paths))
}

pub(crate) fn recipients_path(paths: &Paths) -> PathBuf {
    paths.home.join("keys/recipients.toml")
}

pub(crate) fn random_key_hex() -> Result<String> {
    majutsu_crypto::random_key_hex()
}

pub(crate) fn validate_key_hex(hex_key: &str) -> Result<()> {
    majutsu_crypto::validate_key_hex(hex_key)
}

pub(crate) fn read_master_key(paths: &Paths) -> Result<String> {
    majutsu_crypto::read_master_key(&paths.master_key)
}

pub(crate) fn write_master_key(paths: &Paths, hex_key: &str) -> Result<()> {
    majutsu_crypto::write_master_key(&paths.master_key, hex_key)
}

fn hostname_from_env() -> Result<String> {
    if let Ok(hostname) = env::var("HOSTNAME") {
        if !hostname.is_empty() {
            return Ok(hostname);
        }
    }
    Ok(fs::read_to_string("/etc/hostname")?.trim().to_string())
}

fn absolutize(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_s3_remote() -> S3Remote {
        S3Remote {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            endpoint: "https://storage.googleapis.com".to_string(),
            region: "auto".to_string(),
            signature_version: "s3v4".to_string(),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            storage_class: Some("STANDARD_IA".to_string()),
            object_tags: vec![("purpose".to_string(), "backup data".to_string())],
            multipart_enabled: true,
            max_parallel_uploads: 8,
            client: Client::new(),
        }
    }

    #[test]
    fn s3_put_headers_include_storage_class_and_encoded_tags() {
        let remote = test_s3_remote();
        let headers = remote
            .put_object_headers("objects/large/chunks/fixed/chunk-1")
            .unwrap();
        assert!(headers.contains(&("x-amz-storage-class".to_string(), "STANDARD_IA".to_string())));
        assert!(headers.contains(&(
            "x-amz-tagging".to_string(),
            "majutsu-class=large&purpose=backup%20data".to_string()
        )));
    }

    #[test]
    fn s3_capabilities_honor_multipart_policy() {
        let mut remote = test_s3_remote();
        remote.multipart_enabled = false;
        let store = RemoteStore::S3(remote);
        assert!(!store.capabilities().multipart_upload);
    }

    #[test]
    fn s3_multipart_threshold_requires_enabled_policy() {
        let mut remote = test_s3_remote();
        remote.multipart_enabled = false;
        assert!(!remote.should_use_multipart(DEFAULT_MULTIPART_THRESHOLD));
        remote.multipart_enabled = true;
        assert!(remote.should_use_multipart(DEFAULT_MULTIPART_THRESHOLD));
    }

    #[test]
    fn s3_multipart_initiate_headers_use_local_object_class() {
        let remote = test_s3_remote();
        let remote_key = remote.remote_key("large/chunks/fixed-8m/chunk-1");
        assert_eq!(s3_object_class(&remote_key), "object");

        let headers = remote
            .multipart_initiate_headers("large/chunks/fixed-8m/chunk-1")
            .unwrap();
        assert!(headers.contains(&(
            "x-amz-tagging".to_string(),
            "majutsu-class=large&purpose=backup%20data".to_string()
        )));
    }

    #[test]
    fn s3_multipart_upload_sends_parts_and_complete_manifest() {
        fn read_http_request(stream: &mut std::net::TcpStream) -> (String, Vec<u8>) {
            let mut bytes = Vec::new();
            let mut buf = [0u8; 8192];
            let header_end = loop {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0, "client closed before sending headers");
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]).to_string();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim())
                })
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            while bytes.len() < header_end + content_length {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0, "client closed before sending body");
                bytes.extend_from_slice(&buf[..n]);
            }
            (
                headers.lines().next().unwrap_or_default().to_string(),
                bytes[header_end..header_end + content_length].to_vec(),
            )
        }

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let mut observed = Vec::new();
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().unwrap();
                let (request_line, body) = read_http_request(&mut stream);
                if request_line.contains("?uploads=") {
                    let body = "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>";
                    stream
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                                body.len(),
                                body
                            )
                            .as_bytes(),
                        )
                        .unwrap();
                } else if request_line.contains("partNumber=") {
                    let etag = if request_line.contains("partNumber=1") {
                        "etag-1"
                    } else {
                        "etag-2"
                    };
                    stream
                        .write_all(
                            format!("HTTP/1.1 200 OK\r\nETag: {etag}\r\nContent-Length: 0\r\n\r\n")
                                .as_bytes(),
                        )
                        .unwrap();
                } else if request_line.contains("?uploadId=upload-1") {
                    stream
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .unwrap();
                } else {
                    panic!("unexpected multipart request: {request_line}");
                }
                observed.push((request_line, body));
            }
            tx.send(observed).unwrap();
        });

        let mut remote = test_s3_remote();
        remote.endpoint = endpoint;
        remote.prefix = "majutsu/v1".into();
        remote.max_parallel_uploads = 2;
        let mut payload = vec![1u8; MIN_MULTIPART_PART_SIZE];
        payload.extend(vec![2u8; 3]);
        remote
            .put_multipart("large/chunks/fixed-8m/chunk-1", &payload)
            .unwrap();
        server.join().unwrap();

        let observed = rx.recv().unwrap();
        assert!(
            observed[0]
                .0
                .starts_with("POST /bucket/majutsu/v1/large/chunks/fixed-8m/chunk-1?uploads=")
        );
        let part_bodies = observed
            .iter()
            .filter(|(line, _)| line.contains("partNumber="))
            .map(|(line, body)| (line.clone(), body.clone()))
            .collect::<Vec<_>>();
        assert_eq!(part_bodies.len(), 2);
        assert!(part_bodies.iter().any(|(line, body)| {
            line.contains("partNumber=1") && body == &vec![1u8; MIN_MULTIPART_PART_SIZE]
        }));
        assert!(
            part_bodies
                .iter()
                .any(|(line, body)| line.contains("partNumber=2") && body == &vec![2u8; 3])
        );
        let complete_body = observed
            .iter()
            .find(|(line, _)| line.starts_with("POST ") && line.contains("?uploadId=upload-1"))
            .map(|(_, body)| String::from_utf8_lossy(body).to_string())
            .unwrap();
        assert!(complete_body.contains("<PartNumber>1</PartNumber><ETag>etag-1</ETag>"));
        assert!(complete_body.contains("<PartNumber>2</PartNumber><ETag>etag-2</ETag>"));
    }

    #[test]
    fn s3_sigv4_signs_put_attribute_headers() {
        let remote = test_s3_remote();
        let headers = remote
            .put_object_headers("objects/packs/normal/pack-1")
            .unwrap();
        let auth = remote
            .auth_v4(
                "PUT",
                "prefix/objects/packs/normal/pack-1",
                "",
                "hash",
                &headers,
            )
            .unwrap();
        assert!(auth.authorization.contains(
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-storage-class;x-amz-tagging"
        ));
    }

    #[test]
    fn s3_sigv4_signs_conditional_put_header() {
        let remote = test_s3_remote();
        let mut headers = remote
            .put_object_headers("objects/blobs/loose/blob-1")
            .unwrap();
        headers.push(("if-none-match".to_string(), "*".to_string()));
        let auth = remote
            .auth_v4(
                "PUT",
                "prefix/objects/blobs/loose/blob-1",
                "",
                "hash",
                &headers,
            )
            .unwrap();
        assert!(auth.authorization.contains("SignedHeaders=host;if-none-match;x-amz-content-sha256;x-amz-date;x-amz-storage-class;x-amz-tagging"));
    }

    #[test]
    fn s3_restore_archive_posts_restore_request_xml() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0u8; 1024];
            let header_end = loop {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0, "client closed before sending headers");
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]).to_string();
            let content_length = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .or_else(|| {
                    headers
                        .lines()
                        .find_map(|line| line.strip_prefix("Content-Length: "))
                })
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            while bytes.len() < header_end + content_length {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0, "client closed before sending body");
                bytes.extend_from_slice(&buf[..n]);
            }
            stream
                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
            tx.send(bytes).unwrap();
        });

        let mut remote = test_s3_remote();
        remote.endpoint = endpoint;
        remote.prefix = "majutsu/v1".into();
        assert!(
            remote
                .restore_archive("objects/blobs/aa/bb", 5, "Bulk")
                .unwrap()
        );
        server.join().unwrap();
        let request = rx.recv().unwrap();
        let text = String::from_utf8_lossy(&request);
        let lower = text.to_ascii_lowercase();
        assert!(text.starts_with("POST /bucket/majutsu/v1/objects/blobs/aa/bb?restore="));
        assert!(lower.contains("authorization: aws4-hmac-sha256"));
        assert!(lower.contains("content-type: application/xml"));
        assert!(text.contains("<RestoreRequest><Days>5</Days>"));
        assert!(text.contains("<Tier>Bulk</Tier>"));
    }

    #[test]
    fn file_remote_put_if_absent_does_not_overwrite_existing_object() {
        let tmp = tempfile::tempdir().unwrap();
        let remote = RemoteStore::File(FileRemote {
            root: tmp.path().to_path_buf(),
        });

        assert!(remote.put_if_absent("objects/test", b"first").unwrap());
        assert!(!remote.put_if_absent("objects/test", b"second").unwrap());
        assert_eq!(remote.get("objects/test").unwrap(), b"first");
    }

    #[test]
    fn restore_archive_config_defaults_and_validates() {
        let legacy = r#"
[host]
id = "host-1"
name = "test-host"

[large]
enabled = true
always = []
never = []
"#;
        let config: Config = toml::from_str(legacy).unwrap();
        assert_eq!(config.restore.archive.days, 7);
        assert_eq!(config.restore.archive.tier, "Standard");
        validate_restore_archive_config(&config.restore.archive).unwrap();

        let custom = r#"
[host]
id = "host-1"
name = "test-host"

[large]
enabled = true
always = []
never = []

[restore.archive]
days = 3
tier = "Bulk"
"#;
        let config: Config = toml::from_str(custom).unwrap();
        assert_eq!(config.restore.archive.days, 3);
        assert_eq!(config.restore.archive.tier, "Bulk");
        validate_restore_archive_config(&config.restore.archive).unwrap();

        assert!(
            validate_restore_archive_config(&config::RestoreArchiveConfig {
                days: 0,
                tier: "Standard".into(),
            })
            .unwrap_err()
            .to_string()
            .contains("restore archive days must be greater than zero")
        );
        assert!(
            validate_restore_archive_config(&config::RestoreArchiveConfig {
                days: 1,
                tier: " ".into(),
            })
            .unwrap_err()
            .to_string()
            .contains("restore archive tier must not be empty")
        );
    }

    #[test]
    fn fuse_read_file_can_read_packed_blob_without_loose_object() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = resolve_paths(Some(tmp.path().join("state"))).unwrap();
        init(
            &paths,
            InitArgs {
                remote: None,
                host_name: Some("test-host".into()),
                encrypt: false,
            },
        )
        .unwrap();
        let payload = b"alpha packed payload\n";
        let oid = blake3_hex(payload);
        let object_key = store_bytes(&paths, &paths.objects, &oid, payload).unwrap();
        let conn = open_db(&paths).unwrap();
        conn.execute(
            "insert into blobs(oid, size, object_key) values (?1, ?2, ?3)",
            params![oid, payload.len() as u64, object_key],
        )
        .unwrap();
        drop(conn);
        pack_cmd(&paths, PackArgs { compact: false }).unwrap();
        fs::remove_file(paths.home.join(&object_key)).unwrap();

        let fs = crate::fuse_mount::MajutsuFuseFs::for_test(paths);
        let record = FileRecord {
            root_id: "sample".into(),
            path: "alpha.txt".into(),
            kind: "file".into(),
            size: payload.len() as u64,
            mode: 0o100644,
            modified: None,
            uid: None,
            gid: None,
            xattrs: BTreeMap::new(),
            payload: Payload::NormalBlob { oid, object_key },
        };

        assert_eq!(fs.read_file(&record, 6, 6).unwrap(), b"packed".to_vec());
    }

    #[test]
    fn fuse_large_read_only_requires_overlapping_chunks() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = resolve_paths(Some(tmp.path().join("state"))).unwrap();
        init(
            &paths,
            InitArgs {
                remote: None,
                host_name: Some("test-host".into()),
                encrypt: false,
            },
        )
        .unwrap();
        let chunks = [(0u64, b"abcd".as_slice()), (4, b"efgh"), (8, b"ijkl")];
        let mut manifest_chunks = Vec::new();
        let mut first_chunk_key = String::new();
        for (index, (offset, bytes)) in chunks.iter().enumerate() {
            let oid = blake3_hex(bytes);
            let object_key = store_bytes(&paths, &paths.large_chunks, &oid, bytes).unwrap();
            if index == 0 {
                first_chunk_key = object_key.clone();
            }
            manifest_chunks.push(LargeChunk {
                index,
                offset: *offset,
                len: bytes.len() as u64,
                stored_len: Some(bytes.len() as u64),
                compression: "none".into(),
                oid,
                object_key,
            });
        }
        let manifest = LargeManifest {
            version: 1,
            oid: blake3_hex(b"abcdefghijkl"),
            size: 12,
            media_type: Some("application/octet-stream".into()),
            binary: true,
            chunking: "fixed".into(),
            chunk_size: 4,
            chunks: manifest_chunks,
        };
        let manifest_json = serde_json::to_vec_pretty(&manifest).unwrap();
        let manifest_oid = blake3_hex(&manifest_json);
        let manifest_key = store_bytes(
            &paths,
            &paths.large_manifests,
            &manifest_oid,
            &manifest_json,
        )
        .unwrap();
        fs::remove_file(paths.home.join(&first_chunk_key)).unwrap();

        let fs = crate::fuse_mount::MajutsuFuseFs::for_test(paths);
        let record = FileRecord {
            root_id: "sample".into(),
            path: "payload.bin".into(),
            kind: "file".into(),
            size: 12,
            mode: 0o100644,
            modified: None,
            uid: None,
            gid: None,
            xattrs: BTreeMap::new(),
            payload: Payload::LargeObject {
                oid: manifest.oid,
                manifest_key,
                chunk_count: 3,
                media_type: Some("application/octet-stream".into()),
                binary: true,
                chunking: "fixed".into(),
                compression: "none".into(),
                encryption: "none".into(),
                storage_tier_hint: "hot-manifest-cold-chunks".into(),
                hydrate_policy: "on-demand".into(),
            },
        };

        assert_eq!(fs.read_file(&record, 5, 2).unwrap(), b"fg".to_vec());
        assert!(fs.read_file(&record, 1, 2).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn stable_metadata_detects_same_size_file_replacement() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("data.txt");
        let replacement = tmp.path().join("replacement.txt");
        fs::write(&path, b"same-size-a").unwrap();
        fs::write(&replacement, b"same-size-b").unwrap();
        let before = fs::metadata(&path).unwrap();
        fs::rename(&replacement, &path).unwrap();
        filetime::set_file_mtime(
            &path,
            filetime::FileTime::from_system_time(before.modified().unwrap()),
        )
        .unwrap();
        let after = fs::metadata(&path).unwrap();

        assert_eq!(before.len(), after.len());
        assert_eq!(before.modified().unwrap(), after.modified().unwrap());
        assert!(!stable_metadata_matches(&before, &after));
    }
}
