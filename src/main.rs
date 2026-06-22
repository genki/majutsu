#[allow(dead_code)]
#[path = "internal/majutsu_cli.rs"]
mod majutsu_cli;
#[allow(dead_code)]
#[path = "internal/majutsu_core.rs"]
mod majutsu_core;
#[allow(dead_code)]
#[path = "internal/majutsu_crypto.rs"]
mod majutsu_crypto;
#[allow(dead_code)]
#[path = "internal/majutsu_daemon.rs"]
mod majutsu_daemon;
#[allow(dead_code)]
#[path = "internal/majutsu_db.rs"]
mod majutsu_db;
#[allow(dead_code)]
#[path = "internal/majutsu_large.rs"]
mod majutsu_large;
#[allow(dead_code)]
#[path = "internal/majutsu_pack.rs"]
mod majutsu_pack;
#[allow(dead_code)]
#[path = "internal/majutsu_policy.rs"]
mod majutsu_policy;
#[allow(dead_code)]
#[path = "internal/majutsu_restore.rs"]
mod majutsu_restore;
#[allow(dead_code)]
#[path = "internal/majutsu_store.rs"]
mod majutsu_store;
#[allow(dead_code)]
#[path = "internal/majutsu_watch.rs"]
mod majutsu_watch;

use crate::majutsu_core::{
    FileRecord, HistoryGraphIssue, LargeChunk, LargeManifest, LiveMetadataReferences,
    MetadataReferenceIssue, OperationLogEntry as OperationExport, OperationLogEntryIssue, Payload,
    RootSnapshot, SnapshotExport, SnapshotManifest, TreeManifest, TreeNodeManifest, TreeNodeRef,
    decode_operation_log, history_graph_issues, metadata_reference_issues,
    operation_log_comparison_issues, operation_log_entry_matches, payload_blob_ref,
    payload_large_ref, snapshot_export_matches, snapshot_manifest_matches, tree_manifest_issues,
};
use crate::majutsu_crypto::EncryptionMode;
use crate::majutsu_large::{
    ChunkExport, LargeObjectExport, LargePinExport, LargePinIssue, large_manifest_issues,
    large_pin_issues,
};
use crate::majutsu_pack::{
    PackExport, PackIndex, PackIndexIssue, PackObjectIssue, PackedBlobMetadata,
    missing_pack_metadata_ids, pack_index_issues, pack_object_issues,
};
use crate::majutsu_restore::{
    RestoreQueueItem, classify_restore_object_availability, validate_relative_filter_path,
};
use crate::majutsu_store::{
    BlobExport, REMOTE_CHUNK_INDEX_SHARD_KEY, RemoteChunkIndexShard as ChunkIndexShard,
    RemoteGcMark as GcMarkExport, RemoteGcTombstone as GcTombstoneExport, RemoteHostSummary,
    canonical_remote_alias, canonical_remote_aliases, host_current_ref_key,
    host_last_synced_ref_key, host_legacy_current_key, host_metadata_key,
    host_operation_canonical_key, host_operation_key, host_oplog_canonical_key, host_oplog_key,
    host_ops_prefix, host_snapshot_canonical_key, host_snapshot_key, host_snapshots_prefix,
    remote_gc_mark_key, remote_gc_tombstone_prefix,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use hmac::{Hmac, Mac};
use rusqlite::{Connection, params};
use serde::Deserialize;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use uuid::Uuid;
use walkdir::WalkDir;

mod atomic_io;
mod branch_runtime;
mod cache_runtime;
mod cli;
mod clone_runtime;
mod config;
mod daemon_runtime;
mod db_refs;
mod fs_meta;
mod fsck_runtime;
#[cfg(target_os = "linux")]
mod fuse_mount;
#[cfg(not(target_os = "linux"))]
mod fuse_mount {
    use anyhow::{Result, bail};
    use rusqlite::Connection;
    use std::fs;
    use std::path::Path;

    use crate::config::Paths;
    use crate::restore_runtime::RestorePlan;

    pub(crate) fn mount_fuse_cmd(
        _paths: &Paths,
        _conn: &Connection,
        _plan: &RestorePlan,
    ) -> Result<()> {
        bail!("fuse mount backend is only supported on Linux")
    }

    pub(crate) fn prepare_mountpoint(mountpoint: &Path) -> Result<()> {
        if !mountpoint.exists() {
            fs::create_dir_all(mountpoint)?;
            return Ok(());
        }
        let meta = fs::symlink_metadata(mountpoint)?;
        if !meta.file_type().is_dir() {
            bail!("mountpoint is not a directory: {}", mountpoint.display());
        }
        if fs::read_dir(mountpoint)?.next().is_some() {
            bail!("mountpoint is not empty: {}", mountpoint.display());
        }
        Ok(())
    }

    pub(crate) fn is_mountpoint(_path: &Path) -> Result<bool> {
        Ok(false)
    }

    pub(crate) fn unmount_fuse(path: &Path) -> Result<()> {
        bail!(
            "fuse unmount is only supported on Linux: {}",
            path.display()
        )
    }
}
mod history_runtime;
mod key_runtime;
mod large_runtime;
mod lifecycle_runtime;
mod mount_runtime;
mod object_paths;
mod operation_log;
mod pack_runtime;
mod platform_runtime;
mod process_runtime;
mod prune_runtime;
mod queue_runtime;
mod remote_runtime;
mod remote_store;
mod restore_apply;
mod restore_runtime;
mod root_runtime;
mod root_size_summary;
mod root_state;
mod snapshot_rules;
mod snapshot_state;
mod sync_runtime;
mod util;
mod watch_runtime;

use atomic_io::write_atomic_with;
use branch_runtime::branch_cmd;
use cache_runtime::{cache_cmd, prune_synced_metadata_cache, prune_synced_payload_cache};
#[cfg(test)]
use cli::PackArgs;
use cli::{Command, InitArgs, RestoreArgs, SnapshotArgs, parse_cli};
use clone_runtime::clone_cmd;
use config::{
    Config, ConfigRoot, HostConfig, LargeCompressionConfig, LargeConfig, METADATA_EXPORT_VERSION,
    MetadataExport, PackConfig, Paths, RemoteConfig, RestoreConfig, RootConfig, RootDegraded,
    SecurityConfig, TieringConfig, WatchConfig, default_chunk_size, default_large_binary_min_size,
    default_large_chunked_chunk_size, default_large_chunked_min_size, default_large_chunking,
    default_large_max_parallel_uploads, default_large_min_size, default_security_hash,
    default_security_key_id, encryption_enabled, encryption_mode, read_config, resolve_paths,
    resolve_paths_with_scope, validate_restore_archive_config, write_config,
};
use daemon_runtime::{apply_env_files, daemon_cmd};
use fs_meta::{file_gid, file_mode, file_uid, is_mount_point, read_xattrs, special_file_kind};
use fsck_runtime::fsck;
use history_runtime::{diff_cmd, health_cmd, log_cmd, op_cmd, state_cmd, status_cmd};
use key_runtime::key_cmd;
use large_runtime::large_cmd;
use lifecycle_runtime::lifecycle_cmd;
use mount_runtime::{hydrate_cmd, mount_cmd, unmount_cmd};
use object_paths::{
    large_chunk_base, remote_live_object_keys_for_local, s3_remote_live_object_keys_for_local,
};
use operation_log::{
    OperationDetails, query_operations, record_op, record_op_with_details, rewrite_local_oplog,
};
use pack_runtime::pack_cmd;
use process_runtime::acquire_process_lock;
use prune_runtime::{gc_cmd, prune_cmd};
use queue_runtime::{
    compact_event_journal_force, event_cmd, event_journal_records, has_pending_journal_events,
    record_event,
};
use remote_runtime::remote_cmd;
#[cfg(test)]
use remote_store::{
    DEFAULT_LOCAL_MULTIPART_PART_SIZE, DEFAULT_MULTIPART_THRESHOLD, FileRemote, S3Remote,
    s3_object_class,
};
use remote_store::{RemoteStore, open_remote};
use restore_runtime::{
    RestoreDelete, RestorePlan, required_object_keys_for_plan, restore_cmd, restore_root_base,
    write_restore_job,
};
use root_runtime::root_cmd;
use root_state::{
    roots, sync_config_roots, sync_roots_to_config, update_root_degraded, update_root_status,
};
use snapshot_rules::{
    build_ignore, classify_large, effective_large_config, is_ignored, is_included,
    large_pointer_compression, looks_binary,
};
use snapshot_state::{
    carry_forward_root_snapshot, current_snapshot, load_snapshot_by_id, load_snapshot_header,
};
use sync_runtime::sync_cmd;
use util::{
    REMOTE_METADATA_DECODE_LIMIT, blake3_hex, media_type_for_path, modified_secs, new_id,
    parse_db_time, parse_time, path_to_slash, stable_metadata_matches, stable_open_file_in_root,
    stable_read, stable_read_in_root, zstd_decode_all_limited,
};
use watch_runtime::watch_cmd;

fn main() -> Result<()> {
    platform_runtime::initialize_process_environment();
    let cli = parse_cli();
    let paths = resolve_paths_with_scope(cli.home, cli.system)?;
    apply_env_files(&paths)?;
    match cli.command {
        Command::Init(args) => init(&paths, args),
        Command::Root { command } => root_cmd(&paths, command),
        Command::Branch { command } => branch_cmd(&paths, command),
        Command::Switch(args) => branch_cmd(&paths, cli::BranchCommand::Switch(args)),
        Command::Snapshot(args) => snapshot(&paths, args),
        Command::Status(args) => status_cmd(&paths, args),
        Command::Health(args) => health_cmd(&paths, args),
        Command::State(args) => state_cmd(&paths, args),
        Command::Log(args) => log_cmd(&paths, args),
        Command::Op { command } => op_cmd(&paths, command),
        Command::Diff(args) => diff_cmd(&paths, args),
        Command::Restore(args) => restore_cmd(&paths, args),
        Command::Mount(args) => mount_cmd(&paths, args),
        Command::Unmount(args) => unmount_cmd(&paths, args),
        Command::Hydrate(args) => hydrate_cmd(&paths, args),
        Command::Large { command } => large_cmd(&paths, command),
        Command::Cache { command } => cache_cmd(&paths, command),
        Command::Event { command } => event_cmd(&paths, command),
        Command::Sync(args) => sync_cmd(&paths, args),
        Command::Remote { command } => remote_cmd(&paths, command),
        Command::Lifecycle { command } => lifecycle_cmd(&paths, command),
        Command::Clone(args) => clone_cmd(&paths, args),
        Command::Watch(args) => watch_cmd(&paths, args),
        Command::Daemon { command } => daemon_cmd(&paths, command),
        Command::Key { command } => key_cmd(&paths, command),
        Command::Pack(args) => pack_cmd(&paths, args),
        Command::Prune(args) => prune_cmd(&paths, args),
        Command::Gc => gc_cmd(&paths),
        Command::Fsck(args) => fsck(&paths, args),
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
                chunked_min_size: default_large_chunked_min_size(),
                chunked_chunk_size: default_large_chunked_chunk_size(),
                default_chunking: default_large_chunking(),
                chunk_size: default_chunk_size(),
                max_parallel_uploads: default_large_max_parallel_uploads(),
                multipart: true,
                always: crate::majutsu_large::default_large_always_patterns(),
                never: crate::majutsu_large::default_large_never_patterns(),
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
        crate::majutsu_crypto::ensure_age_keyring(&recipients_path(paths))?;
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
    let mut conn = open_db(paths)?;
    let parent = current_snapshot(&conn)?;
    let parent_manifest = parent
        .as_deref()
        .map(|id| load_snapshot_by_id(paths, &conn, id))
        .transpose()?;
    let op_id = new_id("op");
    let snapshot_id = new_id("snap");
    let mut by_root = BTreeMap::new();
    let mut root_trees = BTreeMap::new();
    let mut total_files = 0usize;
    let mut large_files = 0usize;
    let mut pending_blob_inserts = Vec::new();
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
        let scan_result = scan_root(paths, &conn, &config, &scan_root_config);
        let post_result = run_post_snapshot_hook(paths, &root);
        let scanned = match scan_result {
            Ok(scanned) => scanned,
            Err(err) if is_permission_denied_error(&err) => {
                let detail = format!("{err:#}");
                update_root_degraded(
                    &conn,
                    &root.id,
                    "permission-denied",
                    RootDegraded {
                        kind: "permission-denied".into(),
                        at: Utc::now(),
                        message: detail.clone(),
                    },
                )?;
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
                    &format!("{} {}: {detail}", root.id, root.path.display()),
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
        let records = scanned.records;
        pending_blob_inserts.extend(scanned.blobs);
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
        let mut tree = build_tree_manifest(&root.id, records)?;
        let tree_entries = tree.entries.clone();
        let tree_file_count = tree_entries.len();
        prepare_tree_manifest_for_storage(paths, &mut tree)?;
        let root_snapshot = if let Some(previous) = parent_manifest
            .as_ref()
            .and_then(|parent| parent.root_trees.get(&root.id))
            .filter(|previous| previous.tree_id == tree.tree_id)
        {
            previous.clone()
        } else {
            let tree_json = serde_json::to_vec(&tree)?;
            let tree_oid = blake3_hex(&tree_json);
            let tree_key = store_bytes(paths, &paths.trees, &tree_oid, &tree_json)?;
            RootSnapshot {
                tree_id: tree.tree_id.clone(),
                tree_key,
                file_count: tree_file_count,
            }
        };
        root_trees.insert(root.id.clone(), root_snapshot);
        by_root.insert(root.id, tree_entries.into_values().collect());
    }
    let manifest = SnapshotManifest {
        snapshot_id: snapshot_id.clone(),
        parent: parent.clone(),
        op_id: op_id.clone(),
        timestamp: Utc::now(),
        root_trees,
        roots: by_root,
    };
    if snapshot_is_noop(parent_manifest.as_ref(), &manifest) && !snapshot_allows_noop() {
        let current_id = parent.as_deref().unwrap_or("(none)");
        record_event(paths, "snapshot-noop", current_id)?;
        record_event(
            paths,
            "snapshot-finish",
            &format!("noop current={current_id}"),
        )?;
        let _ = compact_event_journal_force(paths);
        prune_noop_snapshot_cache(paths, &conn, &config);
        println!("snapshot unchanged {current_id}");
        println!("files {total_files}, large {large_files}");
        return Ok(());
    }
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let manifest_oid = blake3_hex(&manifest_json);
    let manifest_key = store_encoded_object_bytes(
        paths,
        &paths.objects,
        &manifest_oid,
        &encode_compact_snapshot_manifest_for_local(paths, &manifest)?,
    )?;
    let tx = conn.transaction()?;
    for blob in &pending_blob_inserts {
        tx.execute(
            "insert or ignore into blobs(oid, size, object_key) values (?1, ?2, ?3)",
            params![blob.oid.as_str(), blob.size, blob.object_key.as_str()],
        )?;
    }
    tx.execute(
        "insert into snapshots(id, parent_id, op_id, created_at, manifest_key, manifest_json)
         values (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            snapshot_id,
            parent,
            op_id,
            manifest.timestamp.to_rfc3339(),
            manifest_key,
            ""
        ],
    )?;
    tx.execute(
        "insert into refs(name, value) values ('current', ?1)
         on conflict(name) do update set value=excluded.value",
        params![manifest.snapshot_id],
    )?;
    record_op_with_details(
        &tx,
        OperationDetails {
            id: &op_id,
            kind: snapshot_operation_kind(args.message.as_deref(), manifest.parent.as_deref()),
            before: manifest.parent.as_deref(),
            after: Some(&manifest.snapshot_id),
            status: "done",
            message: args.message.as_deref(),
            error: None,
            remote_sync_state: None,
            origin: args.origin.clone(),
        },
    )?;
    insert_snapshot_payload_index(&tx, &manifest)?;
    branch_runtime::update_active_branch_head(&tx, &snapshot_id)?;
    tx.commit()?;
    println!("snapshot {}", manifest.snapshot_id);
    println!("files {total_files}, large {large_files}");
    record_event(paths, "snapshot-finish", &manifest.snapshot_id)?;
    let _ = compact_event_journal_force(paths);
    Ok(())
}

fn snapshot_is_noop(parent: Option<&SnapshotManifest>, manifest: &SnapshotManifest) -> bool {
    parent
        .map(|parent| parent.root_trees == manifest.root_trees)
        .unwrap_or(false)
}

fn snapshot_allows_noop() -> bool {
    env::var("MAJUTSU_SNAPSHOT_ALLOW_NOOP")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn prune_noop_snapshot_cache(paths: &Paths, conn: &Connection, config: &Config) {
    let Some(remote_config) = config.remote.as_ref() else {
        return;
    };
    let result = (|| -> Result<()> {
        let remote = open_remote(remote_config)?;
        let export = export_metadata(paths, conn, config)?;
        prune_synced_payload_cache(paths, &remote, &export)?;
        prune_synced_metadata_cache(paths, &remote, &export)?;
        Ok(())
    })();
    if let Err(err) = result {
        eprintln!("snapshot noop cache prune skipped: {err:#}");
    }
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
        OperationDetails {
            id: op_id,
            kind,
            before: parent,
            after: parent,
            status: "failed",
            message: Some(&message),
            error: Some(&message),
            remote_sync_state: None,
            origin: None,
        },
    )
}

pub(crate) fn remote_ref(remote: &RemoteStore, key: &str) -> Result<Option<String>> {
    remote
        .get_optional(key)?
        .map(|bytes| String::from_utf8(bytes).map(|value| value.trim().to_string()))
        .transpose()
        .map_err(Into::into)
}

pub(crate) fn decode_canonical_remote_export<T: for<'de> Deserialize<'de>>(
    paths: &Paths,
    bytes: &[u8],
) -> Result<T> {
    let compressed = decode_object(paths, bytes)?;
    let cbor = zstd_decode_all_limited(
        compressed.as_slice(),
        REMOTE_METADATA_DECODE_LIMIT,
        "canonical remote export",
    )?;
    Ok(serde_cbor::from_slice(&cbor)?)
}

pub(crate) fn decode_canonical_remote_oplog(
    paths: &Paths,
    bytes: &[u8],
) -> Result<Vec<OperationExport>> {
    let compressed = decode_object(paths, bytes)?;
    let cborl = zstd_decode_all_limited(
        compressed.as_slice(),
        REMOTE_METADATA_DECODE_LIMIT,
        "canonical remote oplog",
    )?;
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
    match snapshot(
        paths,
        SnapshotArgs {
            message: Some("watch journal replay snapshot".into()),
            origin: None,
        },
    ) {
        Ok(()) => {}
        Err(err) if snapshot_lock_error(&err) => {
            record_event(paths, "watch-snapshot-deferred", &format!("{err:#}"))?;
            return Ok(false);
        }
        Err(err) => return Err(err),
    }
    Ok(true)
}

fn snapshot_lock_error(err: &anyhow::Error) -> bool {
    err.to_string()
        .contains("snapshot already running with pid")
        || format!("{err:#}").contains("snapshot already running with pid")
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
        let metadata_manifest = raw_snapshot_manifest_from_export(paths, snapshot)?;
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

fn raw_snapshot_manifest_from_export(
    paths: &Paths,
    snapshot: &SnapshotExport,
) -> Result<SnapshotManifest> {
    if !snapshot.manifest_json.trim().is_empty() {
        return Ok(serde_json::from_str(&snapshot.manifest_json)?);
    }
    let bytes = fs::read(paths.home.join(&snapshot.manifest_key))?;
    Ok(serde_json::from_slice(&decode_object(paths, &bytes)?)?)
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
            if let Some(issue) = pack_index_issues(pack, &index, &expected_blob_metadata)
                .into_iter()
                .next()
            {
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
            if let Some(issue) =
                pack_object_issues(pack, variant.bytes.len() as u64, &expected_blob_metadata)
                    .into_iter()
                    .next()
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
    if let Some(pack_id) = missing_pack_metadata_ids(
        blobs_by_pack.keys().copied(),
        export.packs.iter().map(|pack| pack.pack_id.as_str()),
    )
    .into_iter()
    .next()
    {
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
    if shard.version != 1 || shard.shard != crate::majutsu_store::DEFAULT_CHUNK_INDEX_SHARD {
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
    if let Some(issue) = operation_log_comparison_issues(&actual, expected)
        .into_iter()
        .next()
    {
        match issue {
            crate::majutsu_core::OperationLogComparisonIssue::CountMismatch {
                expected,
                actual,
            } => {
                bail!(
                    "remote operation log count mismatch {key} expected={expected} actual={actual}"
                );
            }
            crate::majutsu_core::OperationLogComparisonIssue::EntryMismatch { index, id } => {
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
        let expected_compressed_metadata_key = format!("{expected_metadata_key}.zst");
        if host.metadata_key != expected_metadata_key
            && host.metadata_key != expected_compressed_metadata_key
        {
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
    if matches!(remote, RemoteStore::S3(_)) && !clone_validates_s3_remote_refs() {
        return Ok(());
    }
    let expected_ref_keys = [
        host_current_ref_key(&host.id),
        host_last_synced_ref_key(&host.id),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if !matches!(remote, RemoteStore::S3(_)) {
        for key in remote.list(&format!("hosts/{}/refs/", host.id))? {
            if !expected_ref_keys.contains(&key) {
                bail!("remote has unexpected host ref {key}");
            }
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
        if !matches!(remote, RemoteStore::S3(_)) {
            let legacy_key = host_legacy_current_key(&host.id);
            if let Some(legacy_current) = remote_ref(remote, &legacy_key)?
                && legacy_current != *current
            {
                bail!(
                    "remote ref {legacy_key} points to {legacy_current}, expected metadata current {current}"
                );
            }
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

fn clone_validates_s3_remote_refs() -> bool {
    env::var("MAJUTSU_CLONE_VALIDATE_REMOTE_REFS")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

pub(crate) fn validate_clone_remote_gc_mark(
    paths: &Paths,
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
    let mut expected = if matches!(remote, RemoteStore::S3(_)) {
        s3_remote_live_object_keys_for_local(paths, export)?
    } else {
        remote_live_object_keys_for_local(paths, export)?
    }
    .into_iter()
    .collect::<BTreeSet<_>>();
    for key in &mark.object_keys {
        if key.starts_with("objects/trees/nodes/") || key.starts_with("trees/nodes/") {
            expected.insert(key.clone());
        }
    }
    if let Some(issue) = mark
        .validation_issues(&host.id, export.refs.get("current"), &expected)
        .into_iter()
        .next()
    {
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
        crate::majutsu_policy::s3_lifecycle_configuration_xml(&policy)
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
    for issue in crate::majutsu_db::local_ref_issues(
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
    validate_clone_large_pin_metadata_into(export, &mut issues);
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

pub(crate) fn validate_clone_large_pin_metadata(export: &MetadataExport) -> Result<()> {
    let mut issues = Vec::new();
    validate_clone_large_pin_metadata_into(export, &mut issues);
    if issues.is_empty() {
        return Ok(());
    }
    bail!("{}", issues.remove(0))
}

fn validate_clone_large_pin_metadata_into(export: &MetadataExport, issues: &mut Vec<String>) {
    for issue in large_pin_issues(&export.large_pins, &export.large_objects) {
        issues.push(format_large_pin_issue(issue));
    }
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
    if matches!(remote, RemoteStore::S3(_))
        && let Some(alias) = canonical_remote_alias(key)
        && alias != key
    {
        match remote.get(&alias) {
            Ok(bytes) => return canonical_remote_object_to_local_bytes(paths, key, &bytes),
            Err(alias_err) => {
                let bytes = remote.get(key).with_context(|| {
                    format!("download {key} after canonical alias {alias} failed: {alias_err}")
                })?;
                return Ok(bytes);
            }
        }
    }
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

pub(crate) fn hydrate_local_object_from_remote(paths: &Paths, key: &str) -> Result<bool> {
    let dest = paths.home.join(key);
    if dest.exists() {
        return Ok(true);
    }
    let Ok(config) = read_config(paths) else {
        return Ok(false);
    };
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(false);
    };
    let remote = open_remote(remote_config)?;
    let bytes = match download_local_object_from_remote(paths, &remote, key) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(false),
    };
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = dest.with_extension(format!("{}.tmp", Uuid::new_v4()));
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, &dest)?;
    Ok(true)
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct HydrateStats {
    pub(crate) hydrated: usize,
    pub(crate) downloaded_bytes: u64,
    pub(crate) download_ms_sum: u128,
    pub(crate) write_ms_sum: u128,
}

impl HydrateStats {
    pub(crate) fn add(&mut self, other: HydrateStats) {
        self.hydrated += other.hydrated;
        self.downloaded_bytes += other.downloaded_bytes;
        self.download_ms_sum += other.download_ms_sum;
        self.write_ms_sum += other.write_ms_sum;
    }
}

pub(crate) fn hydrate_local_objects_from_remote(
    paths: &Paths,
    keys: Vec<String>,
) -> Result<HydrateStats> {
    let mut keys = keys
        .into_iter()
        .filter(|key| !paths.home.join(key).exists())
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    if keys.is_empty() {
        return Ok(HydrateStats::default());
    }
    let Ok(config) = read_config(paths) else {
        return Ok(HydrateStats::default());
    };
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(HydrateStats::default());
    };
    let remote = open_remote(remote_config)?;
    if !matches!(remote, RemoteStore::S3(_)) {
        let mut stats = HydrateStats::default();
        for key in keys {
            stats.add(hydrate_one_local_object_from_remote(paths, &remote, &key)?);
        }
        return Ok(stats);
    }
    hydrate_local_objects_from_remote_parallel(paths, remote, keys)
}

fn hydrate_local_objects_from_remote_parallel(
    paths: &Paths,
    remote: RemoteStore,
    keys: Vec<String>,
) -> Result<HydrateStats> {
    let workers = restore_prefetch_parallelism().min(keys.len());
    let paths = Arc::new(paths.clone());
    let remote = Arc::new(remote);
    let keys = Arc::new(Mutex::new(keys.into_iter()));
    let (result_tx, result_rx) = mpsc::channel::<Result<HydrateStats>>();
    let mut handles = Vec::new();
    for _ in 0..workers {
        let paths = Arc::clone(&paths);
        let remote = Arc::clone(&remote);
        let keys = Arc::clone(&keys);
        let result_tx = result_tx.clone();
        handles.push(std::thread::spawn(move || {
            let mut stats = HydrateStats::default();
            loop {
                let key = {
                    let mut keys = match keys.lock() {
                        Ok(keys) => keys,
                        Err(err) => {
                            let _ = result_tx
                                .send(Err(anyhow!("restore prefetch queue poisoned: {err}")));
                            return;
                        }
                    };
                    keys.next()
                };
                let Some(key) = key else {
                    break;
                };
                match hydrate_one_local_object_from_remote(&paths, &remote, &key) {
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
            first_error.get_or_insert_with(|| anyhow!("restore prefetch worker panicked: {err:?}"));
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(stats)
}

fn hydrate_one_local_object_from_remote(
    paths: &Paths,
    remote: &RemoteStore,
    key: &str,
) -> Result<HydrateStats> {
    let dest = paths.home.join(key);
    if dest.exists() {
        return Ok(HydrateStats::default());
    }
    let download_started = std::time::Instant::now();
    let bytes = download_local_object_from_remote(paths, remote, key)
        .with_context(|| format!("prefetch restore object {key}"))?;
    let download_ms = download_started.elapsed().as_millis();
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = dest.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let write_started = std::time::Instant::now();
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, &dest)?;
    let write_ms = write_started.elapsed().as_millis();
    Ok(HydrateStats {
        hydrated: 1,
        downloaded_bytes: fs::metadata(&dest)?.len(),
        download_ms_sum: download_ms,
        write_ms_sum: write_ms,
    })
}

fn restore_prefetch_parallelism() -> usize {
    env::var("MAJUTSU_RESTORE_PREFETCH_PARALLELISM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(32)
}

fn canonical_remote_object_to_local_bytes(
    paths: &Paths,
    key: &str,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    if key.starts_with("objects/blobs/")
        && let Some(bytes) = compact_snapshot_manifest_to_local_bytes(paths, bytes)?
    {
        return Ok(bytes);
    }
    if key.starts_with("objects/trees/nodes/") {
        let manifest: TreeNodeManifest = decode_canonical_remote_export(paths, bytes)?;
        return encode_object(paths, &serde_json::to_vec(&manifest)?);
    }
    if key.starts_with("objects/trees/") {
        let manifest: TreeManifest = decode_canonical_remote_export(paths, bytes)?;
        return encode_object(paths, &serde_json::to_vec(&manifest)?);
    }
    if key.starts_with("objects/indexes/pack/") {
        let index: PackIndex = decode_canonical_remote_export(paths, bytes)?;
        return Ok(serde_json::to_vec_pretty(&index)?);
    }
    if key.starts_with("objects/large/manifests/") {
        let manifest: LargeManifest = decode_canonical_remote_export(paths, bytes)?;
        return encode_object(paths, &serde_json::to_vec_pretty(&manifest)?);
    }
    Ok(bytes.to_vec())
}

pub(crate) fn encode_compact_snapshot_manifest_for_remote(
    paths: &Paths,
    manifest: &SnapshotManifest,
) -> Result<Vec<u8>> {
    let value = compact_snapshot_manifest_value(manifest)?;
    let cbor = serde_cbor::to_vec(&value)?;
    let compressed = zstd::stream::encode_all(cbor.as_slice(), 3)?;
    encode_object(paths, &compressed)
}

pub(crate) fn encode_compact_snapshot_manifest_for_local(
    paths: &Paths,
    manifest: &SnapshotManifest,
) -> Result<Vec<u8>> {
    encode_object(
        paths,
        &serde_json::to_vec_pretty(&compact_snapshot_manifest_value(manifest)?)?,
    )
}

fn compact_snapshot_manifest_value(manifest: &SnapshotManifest) -> Result<serde_json::Value> {
    let mut value = serde_json::to_value(manifest)?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "roots".into(),
            serde_json::Value::Object(serde_json::Map::new()),
        );
        object.insert("roots_omitted".into(), serde_json::Value::Bool(true));
    }
    Ok(value)
}

fn compact_snapshot_manifest_to_local_bytes(
    paths: &Paths,
    bytes: &[u8],
) -> Result<Option<Vec<u8>>> {
    let Ok(manifest) = decode_canonical_remote_export::<SnapshotManifest>(paths, bytes) else {
        return Ok(None);
    };
    if !manifest.roots.is_empty() || manifest.root_trees.is_empty() {
        return Ok(None);
    }
    Ok(Some(encode_compact_snapshot_manifest_for_local(
        paths, &manifest,
    )?))
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

pub(crate) fn remote_available_key(remote: &RemoteStore, key: &str) -> Result<String> {
    if remote.exists(key)? {
        return Ok(key.to_string());
    }
    if let Some(alias) = canonical_remote_alias(key)
        && remote.exists(&alias)?
    {
        return Ok(alias);
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

fn build_restore_plan(paths: &Paths, conn: &Connection, args: &RestoreArgs) -> Result<RestorePlan> {
    let trace_start = std::time::Instant::now();
    if let Some(path) = &args.path {
        validate_relative_filter_path(path, "restore --path")?;
    }
    restore_trace_mark(trace_start, "build_plan validate args");
    let snapshot = load_snapshot_header(paths, conn, args)?;
    restore_trace_mark(trace_start, "build_plan load snapshot");
    let root_paths = roots(conn)?
        .into_iter()
        .map(|root| (root.id, root.path))
        .collect::<BTreeMap<_, _>>();
    restore_trace_mark(trace_start, "build_plan load roots");
    let mut files = Vec::new();
    let mut plan_roots = Vec::new();
    for root_id in restore_plan_root_ids(&snapshot) {
        if let Some(filter_root) = &args.root
            && filter_root != &root_id
        {
            continue;
        }
        plan_roots.push(root_id.clone());
        collect_restore_plan_records(paths, &snapshot, &root_id, args.path.as_deref(), &mut files)?;
    }
    overlay_durable_journal_records(paths, args, &snapshot, &mut files)?;
    restore_trace_mark(trace_start, "build_plan filter files");
    let deletes = build_restore_deletes(args, &root_paths, &plan_roots, &files)?;
    restore_trace_mark(trace_start, "build_plan deletes");
    Ok(RestorePlan {
        snapshot,
        to: args.to.clone(),
        root_paths,
        files,
        deletes,
    })
}

fn restore_plan_root_ids(snapshot: &SnapshotManifest) -> Vec<String> {
    let mut roots = snapshot
        .root_trees
        .keys()
        .cloned()
        .chain(snapshot.roots.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    roots.sort();
    roots
}

fn collect_restore_plan_records(
    paths: &Paths,
    snapshot: &SnapshotManifest,
    root_id: &str,
    path_filter: Option<&Path>,
    files: &mut Vec<FileRecord>,
) -> Result<()> {
    let mut push_record = |record: &FileRecord| -> Result<()> {
        if path_filter.is_some_and(|filter| !Path::new(&record.path).starts_with(filter)) {
            return Ok(());
        }
        files.push(record.clone());
        Ok(())
    };
    if let Some(records) = snapshot.roots.get(root_id) {
        for record in records {
            push_record(record)?;
        }
        return Ok(());
    }
    if let Some(root_tree) = snapshot.root_trees.get(root_id) {
        crate::snapshot_state::visit_tree_records(paths, root_tree, push_record)?;
    }
    Ok(())
}

fn overlay_durable_journal_records(
    paths: &Paths,
    args: &RestoreArgs,
    snapshot: &SnapshotManifest,
    files: &mut Vec<FileRecord>,
) -> Result<()> {
    if args.snapshot.is_some() {
        return Ok(());
    }
    let target_time = if let Some(at) = &args.at {
        parse_db_time(&parse_time(at)?)?
    } else {
        Utc::now()
    };
    let mut by_path = files
        .drain(..)
        .map(|record| ((record.root_id.clone(), record.path.clone()), record))
        .collect::<BTreeMap<_, _>>();
    for event in event_journal_records(paths)? {
        if event.remote_journal_synced_at.is_none() {
            continue;
        }
        if event.observed_at <= snapshot.timestamp || event.observed_at > target_time {
            continue;
        }
        let (Some(root_id), Some(path)) = (event.root_id.as_deref(), event.path.as_deref()) else {
            continue;
        };
        if let Some(filter_root) = &args.root
            && filter_root != root_id
        {
            continue;
        }
        if args
            .path
            .as_ref()
            .is_some_and(|filter| !Path::new(path).starts_with(filter))
        {
            continue;
        }
        let key = (root_id.to_string(), path.to_string());
        if event.durable_tombstone == Some(true) {
            by_path.remove(&key);
            continue;
        }
        if event.durable_entry_kind.as_deref() == Some("symlink") {
            let Some(target) = event.durable_symlink_target.as_deref() else {
                continue;
            };
            by_path.insert(
                key,
                FileRecord {
                    root_id: root_id.to_string(),
                    path: path.to_string(),
                    kind: "symlink".into(),
                    size: 0,
                    mode: event.durable_mode.unwrap_or(0),
                    modified: event
                        .durable_modified
                        .or_else(|| Some(event.observed_at.timestamp())),
                    uid: event.durable_uid,
                    gid: event.durable_gid,
                    xattrs: BTreeMap::new(),
                    payload: Payload::Symlink {
                        target: target.to_string(),
                    },
                },
            );
            continue;
        }
        if event.durable_entry_kind.as_deref() == Some("special") {
            let Some(special_kind) = event.durable_special_kind.as_deref() else {
                continue;
            };
            by_path.insert(
                key,
                FileRecord {
                    root_id: root_id.to_string(),
                    path: path.to_string(),
                    kind: "special".into(),
                    size: 0,
                    mode: event.durable_mode.unwrap_or(0),
                    modified: event
                        .durable_modified
                        .or_else(|| Some(event.observed_at.timestamp())),
                    uid: event.durable_uid,
                    gid: event.durable_gid,
                    xattrs: BTreeMap::new(),
                    payload: Payload::Special {
                        special_kind: special_kind.to_string(),
                    },
                },
            );
            continue;
        }
        if let (Some(oid), Some(manifest_key), Some(chunk_count)) = (
            event.durable_payload_oid.as_deref(),
            event.durable_large_manifest_key.as_deref(),
            event.durable_large_chunk_count,
        ) {
            by_path.insert(
                key,
                FileRecord {
                    root_id: root_id.to_string(),
                    path: path.to_string(),
                    kind: "file".into(),
                    size: event.durable_payload_size.unwrap_or(0),
                    mode: event.durable_mode.unwrap_or(0),
                    modified: event
                        .durable_modified
                        .or_else(|| Some(event.observed_at.timestamp())),
                    uid: event.durable_uid,
                    gid: event.durable_gid,
                    xattrs: BTreeMap::new(),
                    payload: Payload::ChunkedBlob {
                        oid: oid.to_string(),
                        manifest_key: manifest_key.to_string(),
                        chunk_count,
                    },
                },
            );
            continue;
        }
        let (Some(oid), Some(object_key)) = (
            event.durable_payload_oid.as_deref(),
            event.durable_payload_key.as_deref(),
        ) else {
            continue;
        };
        by_path.insert(
            key,
            FileRecord {
                root_id: root_id.to_string(),
                path: path.to_string(),
                kind: "file".into(),
                size: event.durable_payload_size.unwrap_or(0),
                mode: event.durable_mode.unwrap_or(0),
                modified: event
                    .durable_modified
                    .or_else(|| Some(event.observed_at.timestamp())),
                uid: event.durable_uid,
                gid: event.durable_gid,
                xattrs: BTreeMap::new(),
                payload: Payload::NormalBlob {
                    oid: oid.to_string(),
                    object_key: object_key.to_string(),
                },
            },
        );
    }
    *files = by_path.into_values().collect();
    Ok(())
}

fn restore_trace_mark(start: std::time::Instant, label: &str) {
    if restore_trace_enabled() {
        eprintln!(
            "restore_trace elapsed_ms={} stage={label}",
            start.elapsed().as_millis()
        );
    }
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
        if let Some(filter_root) = &args.root
            && filter_root != root_id
        {
            continue;
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

pub(crate) fn pack_entry_payload<'a>(oid: &str, entry: &'a [u8]) -> Result<&'a [u8]> {
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

struct ScannedRoot {
    records: Vec<FileRecord>,
    blobs: Vec<BlobInsert>,
}

struct BlobInsert {
    oid: String,
    size: u64,
    object_key: String,
}

fn scan_root(
    paths: &Paths,
    conn: &Connection,
    config: &Config,
    root: &RootConfig,
) -> Result<ScannedRoot> {
    let ignore = build_ignore(root)?;
    let mut records = Vec::new();
    let mut blobs = Vec::new();
    let packed_blob_keys = packed_blob_object_keys(conn)?;
    let walker = WalkDir::new(&root.path)
        .follow_links(root.follow_symlinks)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| {
            if entry.path() == root.path {
                return true;
            }
            let Ok(rel) = entry.path().strip_prefix(&root.path) else {
                return true;
            };
            !is_ignored(&ignore, rel, entry.file_type().is_dir())
        });
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
            let (oid, manifest_key, chunk_count) = if root.follow_symlinks {
                store_large_file(paths, entry.path(), &rel, &large_config, binary)?
            } else {
                store_large_file_in_root(paths, &root.path, &rel, &large_config, binary)?
            };
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
        } else if large_config.enabled && meta.len() >= large_config.chunked_min_size {
            let mut chunked_config = large_config.clone();
            chunked_config.chunk_size = large_config.chunked_chunk_size;
            let (oid, manifest_key, chunk_count) = if root.follow_symlinks {
                store_large_file(paths, entry.path(), &rel, &chunked_config, binary)?
            } else {
                store_large_file_in_root(paths, &root.path, &rel, &chunked_config, binary)?
            };
            Payload::ChunkedBlob {
                oid,
                manifest_key,
                chunk_count,
            }
        } else {
            let bytes = if root.follow_symlinks {
                stable_read(entry.path(), root.snapshot_mode.as_str())?
            } else {
                stable_read_in_root(&root.path, &rel, root.snapshot_mode.as_str())?
            };
            let oid = blake3_hex(&bytes);
            let object_key = if let Some(object_key) = packed_blob_keys.get(oid.as_str()) {
                object_key.clone()
            } else {
                store_bytes(paths, &paths.objects, &oid, &bytes)?
            };
            blobs.push(BlobInsert {
                oid: oid.clone(),
                size: bytes.len() as u64,
                object_key: object_key.clone(),
            });
            if bytes.len() as u64 <= crate::majutsu_pack::SMALL_BLOB_MAX_SIZE {
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
    Ok(ScannedRoot { records, blobs })
}

fn packed_blob_object_keys(conn: &Connection) -> Result<BTreeMap<String, String>> {
    let mut stmt = conn.prepare("select oid, object_key from blobs where pack_id is not null")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut keys = BTreeMap::new();
    for row in rows {
        let (oid, object_key) = row?;
        keys.insert(oid, object_key);
    }
    Ok(keys)
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
        root_node: None,
        subtree_nodes: BTreeMap::new(),
        entries,
    })
}

fn attach_subtree_node_index(paths: &Paths, tree: &mut TreeManifest) -> Result<()> {
    if !tree_subtree_nodes_enabled() {
        return Ok(());
    }
    let threshold = env::var("MAJUTSU_TREE_SUBTREE_NODE_MIN_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(128);
    attach_subtree_node_index_with_threshold(paths, tree, threshold)
}

pub(crate) fn prepare_tree_manifest_for_storage(
    paths: &Paths,
    tree: &mut TreeManifest,
) -> Result<()> {
    if tree_format_v2_enabled() {
        attach_subtree_node_index_with_threshold(paths, tree, 0)?;
        tree.version = 2;
        tree.entries.clear();
    } else {
        attach_subtree_node_index(paths, tree)?;
    }
    Ok(())
}

fn tree_subtree_nodes_enabled() -> bool {
    env::var("MAJUTSU_TREE_SUBTREE_NODES")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn tree_format_v2_enabled() -> bool {
    env::var("MAJUTSU_TREE_FORMAT")
        .map(|value| value.eq_ignore_ascii_case("v2"))
        .unwrap_or(false)
}

fn attach_subtree_node_index_with_threshold(
    paths: &Paths,
    tree: &mut TreeManifest,
    threshold: usize,
) -> Result<()> {
    if tree.entries.len() < threshold {
        return Ok(());
    }

    let nodes_dir = paths.home.join("objects/trees/nodes");
    let trie = TreeNodeBuild::from_entries(&tree.entries);
    let root_node = store_tree_node(paths, &nodes_dir, &tree.root_id, "", &trie, threshold)?;
    tree.root_node = Some(root_node);
    tree.subtree_nodes.clear();
    if let Some(root_node) = &tree.root_node {
        let node_bytes = read_object(paths, &root_node.node_key)?;
        let node: TreeNodeManifest = serde_json::from_slice(&node_bytes)?;
        for (child_path, child) in node.child_nodes {
            if child.file_count >= threshold {
                tree.subtree_nodes.insert(child_path, child);
            }
        }
    }
    Ok(())
}

fn store_tree_node(
    paths: &Paths,
    nodes_dir: &Path,
    root_id: &str,
    path: &str,
    node: &TreeNodeBuild,
    threshold: usize,
) -> Result<TreeNodeRef> {
    let mut child_nodes = BTreeMap::new();
    for (child_name, child_node) in &node.children {
        if child_node.file_count < threshold {
            continue;
        }
        let child_path = join_tree_path(path, child_name);
        let child = store_tree_node(
            paths,
            nodes_dir,
            root_id,
            &child_path,
            child_node,
            threshold,
        )?;
        child_nodes.insert(child_path, child);
    }
    let file_count = node.entries.len()
        + child_nodes
            .values()
            .map(|child| child.file_count)
            .sum::<usize>();
    let total_size = node.entries.values().fold(0_u64, |sum, record| {
        if matches!(record.payload, Payload::Directory) {
            sum
        } else {
            sum.saturating_add(record.size)
        }
    }) + child_nodes
        .values()
        .map(|child| child.total_size)
        .sum::<u64>();
    let identity = serde_json::to_vec(&(root_id, path, &node.entries, &child_nodes))?;
    let node_id = format!("node-{}", blake3_hex(&identity));
    let manifest = TreeNodeManifest {
        version: 2,
        node_id: node_id.clone(),
        root_id: root_id.to_string(),
        path: path.to_string(),
        created_at: chrono::DateTime::<Utc>::UNIX_EPOCH,
        file_count,
        total_size,
        child_nodes,
        entries: node.entries.clone(),
    };
    let node_json = serde_json::to_vec(&manifest)?;
    let node_oid = blake3_hex(&node_json);
    let node_key = store_bytes(paths, nodes_dir, &node_oid, &node_json)?;
    Ok(TreeNodeRef {
        node_id,
        node_key,
        file_count,
        total_size,
    })
}

#[derive(Default)]
struct TreeNodeBuild {
    entries: BTreeMap<String, FileRecord>,
    children: BTreeMap<String, TreeNodeBuild>,
    file_count: usize,
}

impl TreeNodeBuild {
    fn from_entries(entries: &BTreeMap<String, FileRecord>) -> Self {
        let mut root = Self::default();
        for (path, record) in entries {
            root.insert(path, record.clone());
        }
        root.recount();
        root
    }

    fn insert(&mut self, path: &str, record: FileRecord) {
        let parent = parent_path(path);
        let mut node = self;
        if !parent.is_empty() {
            for component in parent.split('/') {
                node = node.children.entry(component.to_string()).or_default();
            }
        }
        node.entries.insert(path.to_string(), record);
    }

    fn recount(&mut self) -> usize {
        let mut count = self.entries.len();
        for child in self.children.values_mut() {
            count += child.recount();
        }
        self.file_count = count;
        count
    }
}

fn parent_path(path: &str) -> &str {
    path.rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or("")
}

fn join_tree_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{parent}/{child}")
    }
}

pub(crate) fn store_large_file(
    paths: &Paths,
    path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    if config.default_chunking == "fixed" {
        return store_large_file_fixed_streaming(paths, path, rel, config, binary);
    }
    let bytes = stable_read(path, "strict")?;
    store_large_bytes_buffered(paths, rel, config, binary, &bytes)
}

pub(crate) fn store_large_file_in_root(
    paths: &Paths,
    root: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    if config.default_chunking == "fixed" {
        return store_large_file_fixed_streaming_in_root(paths, root, rel, config, binary);
    }
    let bytes = stable_read_in_root(root, rel, "strict")?;
    store_large_bytes_buffered(paths, rel, config, binary, &bytes)
}

fn store_large_bytes_buffered(
    paths: &Paths,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
    bytes: &[u8],
) -> Result<(String, String, usize)> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(bytes);
    let oid = hasher.finalize().to_hex().to_string();
    let conn = open_db(paths)?;
    if let Some((manifest_key, chunk_count)) =
        existing_large_object_manifest(paths, &conn, &oid, config)?
    {
        return Ok((oid, manifest_key, chunk_count));
    }
    let mut chunks = Vec::new();
    let ranges = crate::majutsu_large::chunk_ranges_for_bytes(
        &config.default_chunking,
        config.chunk_size,
        bytes,
    );
    for (index, (start, end)) in ranges.into_iter().enumerate() {
        let chunk = &bytes[start..end];
        let chunk_oid = blake3_hex(chunk);
        let stored = compress_large_chunk(config, rel, chunk)?;
        let object_key = store_large_chunk_bytes(paths, &conn, config, &chunk_oid, &stored.bytes)?;
        chunks.push(LargeChunk {
            index,
            offset: start as u64,
            len: chunk.len() as u64,
            stored_len: Some(stored.bytes.len() as u64),
            compression: stored.compression,
            oid: chunk_oid.clone(),
            object_key: object_key.clone(),
        });
        conn.execute(
            "insert or replace into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk_oid, chunk.len() as u64, object_key],
        )?;
    }
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
    conn.execute(
        "insert or replace into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
        params![oid, bytes.len() as u64, manifest.chunks.len(), manifest_key],
    )?;
    insert_large_object_chunk_index(&conn, &manifest)?;
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

fn store_large_file_fixed_streaming_in_root(
    paths: &Paths,
    root: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    let attempts = 8;
    let mut last_error = None;
    for _ in 0..attempts {
        match store_large_file_fixed_streaming_in_root_once(paths, root, rel, config, binary) {
            Ok(result) => return Ok(result),
            Err(err) if is_file_changed_error(&err) => {
                last_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_error
        .unwrap_or_else(|| anyhow!("file changed while reading: {}", root.join(rel).display())))
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
    let conn = open_db(paths)?;
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
        let object_key = store_large_chunk_bytes(paths, &conn, config, &chunk_oid, &stored.bytes)?;
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
    if let Some((manifest_key, chunk_count)) =
        existing_large_object_manifest(paths, &conn, &oid, config)?
    {
        return Ok((oid, manifest_key, chunk_count));
    }
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
    for chunk in &manifest.chunks {
        conn.execute(
            "insert or replace into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk.oid, chunk.len, chunk.object_key],
        )?;
    }
    conn.execute(
        "insert or replace into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
        params![oid, manifest.size, manifest.chunks.len(), manifest_key],
    )?;
    insert_large_object_chunk_index(&conn, &manifest)?;
    Ok((oid, manifest_key, manifest.chunks.len()))
}

fn store_large_file_fixed_streaming_in_root_once(
    paths: &Paths,
    root: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    let (file, before) = stable_open_file_in_root(root, rel)?;
    store_large_file_fixed_streaming_from_file(
        paths,
        file,
        before,
        &root.join(rel),
        rel,
        config,
        binary,
    )
}

fn store_large_file_fixed_streaming_from_file(
    paths: &Paths,
    mut file: File,
    before: fs::Metadata,
    display_path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    let conn = open_db(paths)?;
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
        let object_key = store_large_chunk_bytes(paths, &conn, config, &chunk_oid, &stored.bytes)?;
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
    let after = file.metadata()?;
    if !stable_metadata_matches(&before, &after) {
        bail!("file changed while reading: {}", display_path.display());
    }
    let oid = hasher.finalize().to_hex().to_string();
    if let Some((manifest_key, chunk_count)) =
        existing_large_object_manifest(paths, &conn, &oid, config)?
    {
        return Ok((oid, manifest_key, chunk_count));
    }
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
    for chunk in &manifest.chunks {
        conn.execute(
            "insert or replace into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk.oid, chunk.len, chunk.object_key],
        )?;
    }
    conn.execute(
        "insert or replace into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
        params![oid, manifest.size, manifest.chunks.len(), manifest_key],
    )?;
    insert_large_object_chunk_index(&conn, &manifest)?;
    Ok((oid, manifest_key, manifest.chunks.len()))
}

fn store_large_chunk_bytes(
    paths: &Paths,
    conn: &Connection,
    config: &LargeConfig,
    chunk_oid: &str,
    bytes: &[u8],
) -> Result<String> {
    if let Some(object_key) = existing_chunk_object_key(paths, conn, chunk_oid)? {
        return Ok(object_key);
    }
    store_bytes(
        paths,
        &large_chunk_base(paths, &config.default_chunking),
        chunk_oid,
        bytes,
    )
}

fn existing_chunk_object_key(
    paths: &Paths,
    conn: &Connection,
    oid: &str,
) -> Result<Option<String>> {
    let mut stmt = conn.prepare("select object_key from chunks where oid=?1")?;
    let mut rows = stmt.query(params![oid])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let object_key: String = row.get(0)?;
    if paths.home.join(&object_key).exists() {
        Ok(Some(object_key))
    } else {
        Ok(None)
    }
}

fn existing_large_object_manifest(
    paths: &Paths,
    conn: &Connection,
    oid: &str,
    config: &LargeConfig,
) -> Result<Option<(String, usize)>> {
    let mut stmt =
        conn.prepare("select manifest_key, chunk_count from large_objects where oid=?1")?;
    let mut rows = stmt.query(params![oid])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let manifest_key: String = row.get(0)?;
    let chunk_count: usize = row.get(1)?;
    if !paths.home.join(&manifest_key).exists() {
        return Ok(None);
    }
    let manifest: LargeManifest = serde_json::from_slice(&read_object(paths, &manifest_key)?)?;
    if manifest.chunking == config.default_chunking && manifest.chunk_size == config.chunk_size {
        insert_large_object_chunk_index(conn, &manifest)?;
        Ok(Some((manifest_key, chunk_count)))
    } else {
        Ok(None)
    }
}

fn insert_large_object_chunk_index(conn: &Connection, manifest: &LargeManifest) -> Result<()> {
    for chunk in &manifest.chunks {
        conn.execute(
            "insert or ignore into large_object_chunks(large_oid, chunk_oid) values (?1, ?2)",
            params![manifest.oid, chunk.oid],
        )?;
    }
    Ok(())
}

pub(crate) fn insert_snapshot_payload_index(
    conn: &Connection,
    manifest: &SnapshotManifest,
) -> Result<()> {
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
        params![manifest.snapshot_id, Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn compress_large_chunk(
    config: &LargeConfig,
    rel: &Path,
    bytes: &[u8],
) -> Result<crate::majutsu_large::StoredLargeChunk> {
    let name = rel.file_name().and_then(OsStr::to_str).unwrap_or_default();
    crate::majutsu_large::compress_chunk_if_useful(crate::majutsu_large::CompressionInput {
        bytes,
        enabled: config.compression.enabled,
        algorithm: &config.compression.algorithm,
        level: config.compression.level,
        sample_bytes: config.compression.sample_bytes,
        min_gain_ratio: config.compression.min_gain_ratio,
        skip_extensions: &config.compression.skip_extensions,
        file_name: name,
    })
}

pub(crate) fn read_large_chunk(paths: &Paths, chunk: &LargeChunk) -> Result<Vec<u8>> {
    read_large_chunk_payload(paths, &chunk.object_key, &chunk.compression)
}

pub(crate) fn decode_large_chunk_stored_bytes(chunk: &LargeChunk, bytes: &[u8]) -> Result<Vec<u8>> {
    decode_large_chunk_payload(&chunk.compression, bytes)
}

fn read_large_chunk_payload(paths: &Paths, object_key: &str, compression: &str) -> Result<Vec<u8>> {
    let bytes = read_object(paths, object_key)?;
    decode_large_chunk_payload(compression, &bytes)
}

fn decode_large_chunk_payload(compression: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    match compression {
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
        "queue/upload-payloads",
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
    restrict_state_permissions(paths)?;
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

#[cfg(unix)]
fn restrict_state_permissions(paths: &Paths) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    for dir in [
        paths.home.clone(),
        paths.home.join("keys"),
        paths.home.join("runtime"),
        paths.home.join("locks"),
    ] {
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("set state directory permissions {}", dir.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn restrict_state_permissions(_: &Paths) -> Result<()> {
    Ok(())
}

fn ensure_ready(paths: &Paths) -> Result<()> {
    if !paths.config.exists() || !paths.db.exists() {
        bail!(
            "majutsu state home is not initialized at {}: run `mj init --home {}` or omit --home to use the default state home",
            paths.home.display(),
            paths.home.display()
        );
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
    conn.execute_batch(crate::majutsu_db::schema_sql())?;
    for migration in crate::majutsu_db::compat_migrations() {
        let _ = conn.execute(migration, []);
    }
    Ok(())
}

pub(crate) fn export_metadata(
    _paths: &Paths,
    conn: &Connection,
    config: &Config,
) -> Result<MetadataExport> {
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
                chunked_min_size: config.large.chunked_min_size,
                chunked_chunk_size: config.large.chunked_chunk_size,
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

fn import_metadata(conn: &mut Connection, export: &MetadataExport) -> Result<()> {
    let tx = conn.transaction()?;
    for root in &export.roots {
        tx.execute(
            "insert or replace into roots(id, data_json) values (?1, ?2)",
            params![root.id, serde_json::to_string(root)?],
        )?;
    }
    for snapshot in &export.snapshots {
        tx.execute(
            "insert or replace into snapshots(id, parent_id, op_id, created_at, manifest_key, manifest_json)
             values (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                snapshot.id,
                snapshot.parent_id,
                snapshot.op_id,
                snapshot.created_at,
                snapshot.manifest_key,
                ""
            ],
        )?;
    }
    for op in &export.operations {
        let process_path_json = op
            .process_path
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let origin_process_path_json = op
            .origin_process_path
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        tx.execute(
            "insert or replace into operations(id, parent_op, kind, actor, session_id, session_label, process_id, process_path, origin_label, origin_session_id, origin_process_id, origin_process_path, origin_exe, origin_confidence, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state)
             values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
            params![
                op.id,
                op.parent_op,
                op.kind,
                op.actor,
                op.session_id,
                op.session_label,
                op.process_id,
                process_path_json,
                op.origin_label,
                op.origin_session_id,
                op.origin_process_id,
                origin_process_path_json,
                op.origin_exe,
                op.origin_confidence,
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
        tx.execute(
            "insert or replace into refs(name, value) values (?1, ?2)",
            params![name, value],
        )?;
    }
    for blob in &export.blobs {
        tx.execute(
            "insert or replace into blobs(oid, size, object_key, pack_id, pack_offset, pack_len) values (?1, ?2, ?3, ?4, ?5, ?6)",
            params![blob.oid, blob.size, blob.object_key, blob.pack_id, blob.pack_offset, blob.pack_len],
        )?;
    }
    for pack in &export.packs {
        tx.execute(
            "insert or replace into packs(pack_id, pack_key, index_key, object_count, size) values (?1, ?2, ?3, ?4, ?5)",
            params![pack.pack_id, pack.pack_key, pack.index_key, pack.object_count, pack.size],
        )?;
    }
    for large in &export.large_objects {
        tx.execute(
            "insert or replace into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
            params![large.oid, large.size, large.chunk_count, large.manifest_key],
        )?;
    }
    for chunk in &export.chunks {
        tx.execute(
            "insert or replace into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk.oid, chunk.size, chunk.object_key],
        )?;
    }
    for pin in &export.large_pins {
        tx.execute(
            "insert or replace into large_pins(oid, pinned_at, reason) values (?1, ?2, ?3)",
            params![pin.oid, pin.pinned_at, pin.reason],
        )?;
    }
    rewrite_local_oplog(&tx)?;
    tx.commit()?;
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
    let mut process = crate::platform_runtime::shell_command(command);
    process
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
    let status = crate::platform_runtime::shell_command(command)
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
    store_encoded_object_bytes(paths, base, oid, &encode_object(paths, bytes)?)
}

pub(crate) fn store_encoded_object_bytes(
    paths: &Paths,
    base: &Path,
    oid: &str,
    bytes: &[u8],
) -> Result<String> {
    let storage_id = object_storage_id(paths, oid)?;
    let (a, b) = storage_id.split_at(2);
    let dir = base.join(a);
    fs::create_dir_all(&dir)?;
    let path = dir.join(b);
    if !path.exists() {
        let tmp = path.with_extension("tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        if fsync_objects_enabled() {
            f.sync_all()?;
        }
        fs::rename(tmp, &path)?;
    }
    let rel = path.strip_prefix(&paths.home).unwrap_or(&path);
    Ok(path_to_slash(rel))
}

fn fsync_objects_enabled() -> bool {
    env::var("MAJUTSU_FSYNC_OBJECTS")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
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
    let parallelism = restore_chunk_pipeline_parallelism().min(manifest.chunks.len());
    if parallelism > 1 {
        return write_large_chunks_atomic_pipelined(paths, dest, manifest, parallelism);
    }
    write_atomic_with(dest, |file| {
        for chunk in &manifest.chunks {
            file.write_all(&read_large_chunk(paths, chunk)?)?;
        }
        Ok(())
    })
}

#[derive(Debug, Clone)]
struct RestoreChunkRead {
    index: usize,
    object_key: String,
    compression: String,
}

fn write_large_chunks_atomic_pipelined(
    paths: &Paths,
    dest: &Path,
    manifest: &LargeManifest,
    parallelism: usize,
) -> Result<()> {
    let chunks = manifest
        .chunks
        .iter()
        .enumerate()
        .map(|(index, chunk)| RestoreChunkRead {
            index,
            object_key: chunk.object_key.clone(),
            compression: chunk.compression.clone(),
        })
        .collect::<Vec<_>>();
    let paths = Arc::new(paths.clone());
    let chunks = Arc::new(chunks);
    let next = Arc::new(Mutex::new(0usize));
    let (result_tx, result_rx) =
        mpsc::sync_channel::<Result<(usize, Vec<u8>, u128)>>(parallelism.saturating_mul(2).max(1));
    let mut handles = Vec::new();
    for _ in 0..parallelism {
        let paths = Arc::clone(&paths);
        let chunks = Arc::clone(&chunks);
        let next = Arc::clone(&next);
        let result_tx = result_tx.clone();
        handles.push(std::thread::spawn(move || {
            loop {
                let chunk = {
                    let mut next = match next.lock() {
                        Ok(next) => next,
                        Err(err) => {
                            let _ = result_tx
                                .send(Err(anyhow!("large restore chunk queue poisoned: {err}")));
                            return;
                        }
                    };
                    let Some(chunk) = chunks.get(*next).cloned() else {
                        break;
                    };
                    *next += 1;
                    chunk
                };
                let read_started = std::time::Instant::now();
                match read_large_chunk_payload(&paths, &chunk.object_key, &chunk.compression) {
                    Ok(bytes) => {
                        if result_tx
                            .send(Ok((chunk.index, bytes, read_started.elapsed().as_millis())))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(err) => {
                        let _ = result_tx.send(Err(err));
                        return;
                    }
                }
            }
        }));
    }
    drop(result_tx);

    let trace_enabled = restore_trace_enabled();
    let write_result = write_atomic_with(dest, |file| {
        let mut pending = BTreeMap::<usize, Vec<u8>>::new();
        let mut next_write = 0usize;
        let mut read_ms = 0u128;
        let mut write_ms = 0u128;
        for result in result_rx {
            let (index, bytes, chunk_read_ms) = result?;
            read_ms += chunk_read_ms;
            pending.insert(index, bytes);
            while let Some(bytes) = pending.remove(&next_write) {
                let write_started = std::time::Instant::now();
                file.write_all(&bytes)?;
                write_ms += write_started.elapsed().as_millis();
                next_write += 1;
            }
        }
        if next_write != chunks.len() {
            bail!(
                "large restore wrote {} of {} chunk(s)",
                next_write,
                chunks.len()
            );
        }
        if trace_enabled {
            eprintln!(
                "restore_trace large_chunks={} parallelism={} read_decode_ms_sum={} write_ms_sum={}",
                chunks.len(),
                parallelism,
                read_ms,
                write_ms
            );
        }
        Ok(())
    });

    let mut first_error = write_result.err();
    for handle in handles {
        if let Err(err) = handle.join() {
            first_error
                .get_or_insert_with(|| anyhow!("large restore chunk worker panicked: {err:?}"));
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(())
}

fn restore_chunk_pipeline_parallelism() -> usize {
    env::var("MAJUTSU_RESTORE_CHUNK_PIPELINE_PARALLELISM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2)
}

fn restore_trace_enabled() -> bool {
    env::var("MAJUTSU_TRACE_RESTORE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
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
        crate::majutsu_crypto::encode_object(
            bytes,
            mode,
            &paths.master_key,
            &recipients_path(paths),
        )
    } else {
        Ok(bytes.to_vec())
    }
}

pub(crate) fn read_object(paths: &Paths, key: &str) -> Result<Vec<u8>> {
    let path = paths.home.join(key);
    if !path.exists() {
        hydrate_local_object_from_remote(paths, key)
            .with_context(|| format!("hydrate missing local object {key}"))?;
    }
    let bytes = fs::read(paths.home.join(key))?;
    let decoded = decode_object(paths, &bytes)?;
    if key.starts_with("objects/blobs/") {
        return hydrate_compact_snapshot_manifest_payload(paths, key, decoded);
    }
    Ok(decoded)
}

pub(crate) fn hydrate_compact_snapshot_manifest_payload(
    paths: &Paths,
    key: &str,
    bytes: Vec<u8>,
) -> Result<Vec<u8>> {
    let Ok(mut manifest) = serde_json::from_slice::<SnapshotManifest>(&bytes) else {
        return Ok(bytes);
    };
    if !manifest.roots.is_empty() || manifest.root_trees.is_empty() {
        return Ok(bytes);
    }
    for (root_id, root_tree) in manifest.root_trees.clone() {
        let tree_bytes = read_object(paths, &root_tree.tree_key)
            .with_context(|| format!("hydrate compact snapshot manifest {key} root {root_id}"))?;
        let tree: TreeManifest = serde_json::from_slice(&tree_bytes)
            .with_context(|| format!("parse root tree {}", root_tree.tree_key))?;
        manifest.roots.insert(
            root_id,
            crate::snapshot_state::tree_entries_from_manifest(paths, tree)?
                .into_values()
                .collect(),
        );
    }
    Ok(serde_json::to_vec_pretty(&manifest)?)
}

pub(crate) fn read_blob_payload(
    paths: &Paths,
    conn: &Connection,
    oid: &str,
    fallback_key: &str,
) -> Result<Vec<u8>> {
    let Some(blob) = query_blobs(conn)?.into_iter().find(|blob| blob.oid == oid) else {
        if !fallback_key.trim().is_empty() {
            return read_object(paths, fallback_key)
                .with_context(|| format!("read fallback blob object {fallback_key} for {oid}"));
        }
        bail!("missing blob metadata for {oid}");
    };
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
        if !paths.home.join(&pack.pack_key).exists() {
            hydrate_local_object_from_remote(paths, &pack.pack_key)
                .with_context(|| format!("hydrate missing pack object {}", pack.pack_key))?;
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
    crate::majutsu_crypto::decode_object(bytes, &paths.master_key, &recipients_path(paths))
}

pub(crate) fn recipients_path(paths: &Paths) -> PathBuf {
    paths.home.join("keys/recipients.toml")
}

pub(crate) fn random_key_hex() -> Result<String> {
    crate::majutsu_crypto::random_key_hex()
}

pub(crate) fn validate_key_hex(hex_key: &str) -> Result<()> {
    crate::majutsu_crypto::validate_key_hex(hex_key)
}

pub(crate) fn read_master_key(paths: &Paths) -> Result<String> {
    crate::majutsu_crypto::read_master_key(&paths.master_key)
}

pub(crate) fn write_master_key(paths: &Paths, hex_key: &str) -> Result<()> {
    crate::majutsu_crypto::write_master_key(&paths.master_key, hex_key)
}

fn hostname_from_env() -> Result<String> {
    if let Ok(hostname) = env::var("HOSTNAME")
        && !hostname.is_empty()
    {
        return Ok(hostname);
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
    use crate::cli::{RootAddArgs, RootCommand};
    use crate::root_runtime::root_cmd;

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
            client: crate::remote_store::s3_http_client().unwrap(),
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
        let store = RemoteStore::S3(Box::new(remote));
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
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
                            format!("HTTP/1.1 200 OK\r\nETag: {etag}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                                .as_bytes(),
                        )
                        .unwrap();
                } else if request_line.contains("?uploadId=upload-1") {
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .unwrap();
                } else {
                    panic!("unexpected multipart request: {request_line}");
                }
                stream.flush().unwrap();
                let _ = stream.shutdown(std::net::Shutdown::Both);
                observed.push((request_line, body));
            }
            tx.send(observed).unwrap();
        });

        let mut remote = test_s3_remote();
        remote.endpoint = endpoint;
        remote.prefix = "majutsu/v1".into();
        remote.max_parallel_uploads = 2;
        let mut payload = vec![1u8; DEFAULT_LOCAL_MULTIPART_PART_SIZE];
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
            line.contains("partNumber=1") && body == &vec![1u8; DEFAULT_LOCAL_MULTIPART_PART_SIZE]
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
    fn subtree_node_index_reuses_unchanged_top_level_subtrees() {
        fn file_record(root_id: &str, path: String, oid: String, size: u64) -> FileRecord {
            FileRecord {
                root_id: root_id.to_string(),
                path,
                kind: "file".to_string(),
                size,
                mode: 0o100644,
                modified: None,
                uid: None,
                gid: None,
                xattrs: BTreeMap::new(),
                payload: Payload::InlineSmall {
                    oid: oid.clone(),
                    object_key: format!("objects/blobs/{oid}"),
                },
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let paths = crate::config::resolve_paths(Some(tmp.path().join("state"))).unwrap();
        create_layout(&paths).unwrap();

        let mut records = Vec::new();
        for dir in ["alpha", "beta"] {
            for index in 0..130 {
                let path = format!("{dir}/file-{index:03}.txt");
                let oid = format!("{dir}-{index:03}");
                records.push(file_record("sample", path, oid, 1));
            }
        }

        let mut first = build_tree_manifest("sample", records.clone()).unwrap();
        attach_subtree_node_index(&paths, &mut first).unwrap();
        assert!(first.root_node.is_none());
        attach_subtree_node_index_with_threshold(&paths, &mut first, 128).unwrap();
        let first_root_node = first.root_node.clone().expect("root node");
        let first_alpha = first
            .subtree_nodes
            .get("alpha")
            .cloned()
            .expect("alpha node");
        let first_beta = first.subtree_nodes.get("beta").cloned().expect("beta node");
        assert!(paths.home.join(&first_root_node.node_key).exists());
        assert!(paths.home.join(&first_alpha.node_key).exists());
        assert!(paths.home.join(&first_beta.node_key).exists());

        records.retain(|record| record.path != "beta/file-129.txt");
        records.push(file_record(
            "sample",
            "beta/file-129.txt".to_string(),
            "beta-edited".to_string(),
            2,
        ));
        let mut second = build_tree_manifest("sample", records).unwrap();
        attach_subtree_node_index_with_threshold(&paths, &mut second, 128).unwrap();

        assert_eq!(
            first_alpha.node_key,
            second.subtree_nodes.get("alpha").unwrap().node_key
        );
        assert_ne!(
            first_beta.node_key,
            second.subtree_nodes.get("beta").unwrap().node_key
        );
        assert_ne!(
            first_root_node.node_key,
            second.root_node.as_ref().unwrap().node_key
        );
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

    #[cfg(target_os = "linux")]
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
    fn snapshot_reuses_packed_blob_without_recreating_loose_object() {
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
        let root = tmp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        let payload = vec![b'x'; (crate::majutsu_pack::SMALL_BLOB_MAX_SIZE as usize) + 1024];
        fs::write(root.join("payload.bin"), &payload).unwrap();
        root_cmd(
            &paths,
            RootCommand::Add(RootAddArgs {
                id: "sample".into(),
                path: root.clone(),
                name: None,
                exclude: Vec::new(),
                presets: Vec::new(),
                no_default_excludes: false,
                include: Vec::new(),
                follow_symlinks: false,
                require_mount: false,
                snapshot_mode: "default".into(),
                pre_snapshot: None,
                post_snapshot: None,
                snapshot_source: None,
                application_plugin: None,
                large_min_size: None,
                large_binary_min_size: None,
                large_chunked_min_size: None,
                large_chunked_chunk_size: None,
                large_chunk_size: None,
                large_chunking: None,
                large_always: Vec::new(),
                large_never: Vec::new(),
            }),
        )
        .unwrap();
        snapshot(
            &paths,
            SnapshotArgs {
                message: Some("initial".into()),
                origin: None,
            },
        )
        .unwrap();
        let oid = blake3_hex(&payload);
        let conn = open_db(&paths).unwrap();
        let object_key: String = conn
            .query_row(
                "select object_key from blobs where oid=?1",
                params![oid.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(paths.home.join(&object_key).exists());
        drop(conn);

        pack_cmd(&paths, PackArgs { compact: false }).unwrap();
        fs::remove_file(paths.home.join(&object_key)).unwrap();
        assert!(!paths.home.join(&object_key).exists());
        snapshot(
            &paths,
            SnapshotArgs {
                message: Some("after-pack".into()),
                origin: None,
            },
        )
        .unwrap();

        assert!(
            !paths.home.join(&object_key).exists(),
            "snapshot should keep using the packed blob instead of recreating a loose object"
        );
    }

    #[cfg(target_os = "linux")]
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
