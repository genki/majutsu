use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::Parser;
use hmac::{Hmac, Mac};
use majutsu_core::{
    FileRecord, LargeChunk, LargeManifest, OperationLogEntry as OperationExport, Payload,
    RootSnapshot, SnapshotExport, SnapshotManifest, TreeManifest, decode_operation_log,
    encode_operation_log, payload_blob_ref, payload_blob_ref_mut, payload_large_ref,
    payload_large_ref_mut,
};
use majutsu_crypto::EncryptionMode;
use majutsu_daemon::render_daemon_service;
use majutsu_large::{ChunkExport, LargeObjectExport, LargePinExport};
use majutsu_pack::{PackExport, PackIndex, PackedBlobMetadata};
use majutsu_restore::{
    RestoreQueueItem, classify_restore_object_availability, validate_relative_filter_path,
};
use majutsu_store::{
    BlobExport, LEGACY_METADATA_EXPORT_KEY, REMOTE_CHUNK_INDEX_SHARD_KEY, REMOTE_HOST_INDEX_KEY,
    RemoteChunkIndexEntry as ChunkIndexEntry, RemoteChunkIndexShard as ChunkIndexShard,
    RemoteGcMark as GcMarkExport, RemoteGcTombstone as GcTombstoneExport, RemoteHostIndex,
    RemoteHostSummary, canonical_remote_alias, canonical_remote_aliases, host_current_ref_key,
    host_last_synced_ref_key, host_legacy_current_key, host_metadata_key,
    host_operation_canonical_key, host_operation_key, host_oplog_canonical_key, host_oplog_key,
    host_ops_prefix, host_snapshot_canonical_key, host_snapshot_key, host_snapshots_prefix,
    remote_gc_mark_key, remote_gc_tombstone_key, select_remote_host,
};
#[cfg(test)]
use reqwest::blocking::Client;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
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
mod config;
mod daemon_runtime;
mod db_refs;
mod fs_meta;
mod fsck_runtime;
mod fuse_mount;
mod object_paths;
mod operation_log;
mod pack_runtime;
mod process_runtime;
mod prune_runtime;
mod queue_runtime;
mod remote_store;
mod restore_apply;
mod restore_runtime;
mod root_state;
mod snapshot_rules;
mod snapshot_state;
mod util;
mod watch_runtime;

use atomic_io::{write_atomic, write_atomic_with};
#[cfg(test)]
use cli::PackArgs;
use cli::{
    Cli, CloneArgs, Command, DaemonCommand, DiffArgs, HydrateArgs, InitArgs, KeyCommand,
    LargeCommand, LifecycleCommand, LogArgs, MountArgs, OpCommand, RemoteCommand, RestoreArgs,
    RestoreCommand, RestoreTopArgs, RootCommand, SnapshotArgs, SyncCommand, UnmountArgs,
};
use config::{
    Config, ConfigRoot, HostConfig, LargeCompressionConfig, LargeConfig, LazyMountEntry,
    MetadataExport, MountViewMetadata, PackConfig, Paths, RemoteConfig, RestoreConfig, RootConfig,
    SecurityConfig, TieringConfig, WatchConfig, default_chunk_size, default_include,
    default_large_binary_min_size, default_large_chunking, default_large_max_parallel_uploads,
    default_large_min_size, default_security_hash, default_security_key_id, encryption_enabled,
    encryption_mode, policy_config, read_config, resolve_paths, validate_config,
    validate_large_chunking, validate_restore_archive_config, validate_snapshot_mode,
    validate_watch_mode, write_config,
};
use daemon_runtime::{daemon_ipc_request, start_watch_daemon};
use db_refs::{
    persist_export_remote_refs, ref_value, restore_ref_value, set_ref_value, set_remote_ref_value,
};
use fs_meta::{file_gid, file_mode, file_uid, is_mount_point, read_xattrs, special_file_kind};
use fsck_runtime::{fsck, remote_fsck};
use fuse_mount::{is_mountpoint, mount_fuse_cmd, prepare_mountpoint, unmount_fuse};
use object_paths::{large_chunk_base, large_chunk_base_for_key, local_object_keys};
use operation_log::{
    query_operation, query_operations, record_op, record_op_with_id, record_op_with_id_and_status,
    rewrite_local_oplog,
};
use pack_runtime::pack_cmd;
use process_runtime::{acquire_process_lock, pid_alive, read_pid};
use prune_runtime::{gc_cmd, prune_cmd};
use queue_runtime::{
    drain_upload_queue, enqueue_file_upload, enqueue_inline_upload, has_pending_journal_events,
    record_event, upload_queue_items,
};
#[cfg(test)]
use remote_store::{
    DEFAULT_MULTIPART_THRESHOLD, FileRemote, MIN_MULTIPART_PART_SIZE, S3Remote, s3_object_class,
};
use remote_store::{RemoteStore, open_remote, open_remote_with_upload_policy};
use restore_apply::{
    apply_file_metadata, prepare_directory_restore_destination, restore_special_file,
};
use restore_runtime::{
    RestoreDelete, RestorePlan, apply_restore_plan, ensure_restore_job_has_no_missing_objects,
    ensure_restore_job_not_blocked, ensure_restore_job_resumable, mark_restore_job_done,
    print_restore_conflicts, print_restore_plan, read_restore_job, required_object_keys_for_plan,
    restore_conflicts, restore_root_base, restore_target_label, write_restore_job,
};
use root_state::{
    root_by_id, root_by_id_optional, roots, save_root, sync_config_roots, sync_roots_to_config,
    update_root_status,
};
use snapshot_rules::{
    apply_root_large_set, build_ignore, classify_large, effective_large_config, is_ignored,
    is_included, large_pointer_compression, looks_binary, root_large_override,
};
use snapshot_state::{
    carry_forward_root_snapshot, current_snapshot, load_snapshot, load_snapshot_by_id,
    snapshot_contains_root, snapshot_file_map, snapshot_id_at,
};
use util::{
    blake3_hex, media_type_for_path, modified_secs, new_id, parse_db_time, parse_duration_ago,
    parse_time, path_to_slash, stable_metadata_matches, stable_read,
};
use watch_runtime::{normalize_watch_backend, watch_cmd};

fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths = resolve_paths(cli.home)?;
    match cli.command {
        Command::Init(args) => init(&paths, args),
        Command::Root { command } => root_cmd(&paths, command),
        Command::Snapshot(args) => snapshot(&paths, args),
        Command::Status => status(&paths),
        Command::Log(args) => log_ops(&paths, args),
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

fn root_cmd(paths: &Paths, command: RootCommand) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    match command {
        RootCommand::Add(args) => {
            let path = absolutize(&args.path)?;
            if !path.exists() {
                bail!("root path does not exist: {}", path.display());
            }
            if root_by_id_optional(&conn, &args.id)?.is_some() {
                bail!(
                    "root already exists: {}; use `mj root set` to change it",
                    args.id
                );
            }
            validate_snapshot_mode(&args.snapshot_mode)?;
            if let Some(chunking) = &args.large_chunking {
                validate_large_chunking(chunking)?;
            }
            let snapshot_source = args
                .snapshot_source
                .as_deref()
                .map(absolutize)
                .transpose()?;
            if snapshot_source.is_some() && args.snapshot_mode != "transactional" {
                bail!("--snapshot-source requires --snapshot-mode transactional");
            }
            let large = root_large_override(&args);
            let root = RootConfig {
                name: args.name.unwrap_or_else(|| args.id.clone()),
                id: args.id,
                path,
                include: if args.include.is_empty() {
                    default_include()
                } else {
                    args.include
                },
                exclude: args.exclude,
                follow_symlinks: args.follow_symlinks,
                require_mount: args.require_mount,
                status: "active".into(),
                snapshot_mode: args.snapshot_mode,
                pre_snapshot: args.pre_snapshot,
                post_snapshot: args.post_snapshot,
                snapshot_source,
                application_plugin: args.application_plugin,
                large,
            };
            conn.execute(
                "insert into roots(id, data_json) values (?1, ?2)",
                params![root.id, serde_json::to_string(&root)?],
            )?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-added", None, None, Some(&root.id))?;
            println!("added root {} -> {}", root.id, root.path.display());
        }
        RootCommand::Set(args) => {
            let mut root = root_by_id(&conn, &args.id)?;
            if let Some(path) = &args.path {
                let path = absolutize(path)?;
                if !path.exists() {
                    bail!("root path does not exist: {}", path.display());
                }
                root.path = path;
            }
            if let Some(name) = &args.name {
                root.name = name.clone();
            }
            if args.clear_include {
                root.include = default_include();
            }
            if !args.include.is_empty() {
                root.include = args.include.clone();
            }
            if args.clear_exclude {
                root.exclude.clear();
            }
            root.exclude.extend(args.exclude.clone());
            if args.follow_symlinks && args.no_follow_symlinks {
                bail!("use either --follow-symlinks or --no-follow-symlinks, not both");
            }
            if args.follow_symlinks {
                root.follow_symlinks = true;
            }
            if args.no_follow_symlinks {
                root.follow_symlinks = false;
            }
            if args.require_mount && args.no_require_mount {
                bail!("use either --require-mount or --no-require-mount, not both");
            }
            if args.require_mount {
                root.require_mount = true;
            }
            if args.no_require_mount {
                root.require_mount = false;
            }
            if let Some(mode) = &args.snapshot_mode {
                validate_snapshot_mode(&mode)?;
                root.snapshot_mode = mode.clone();
            }
            if args.clear_pre_snapshot {
                root.pre_snapshot = None;
            }
            if let Some(pre_snapshot) = &args.pre_snapshot {
                root.pre_snapshot = Some(pre_snapshot.clone());
            }
            if args.clear_post_snapshot {
                root.post_snapshot = None;
            }
            if let Some(post_snapshot) = &args.post_snapshot {
                root.post_snapshot = Some(post_snapshot.clone());
            }
            if args.clear_snapshot_source {
                root.snapshot_source = None;
            }
            if let Some(snapshot_source) = &args.snapshot_source {
                root.snapshot_source = Some(absolutize(snapshot_source)?);
            }
            if args.clear_application_plugin {
                root.application_plugin = None;
            }
            if let Some(application_plugin) = &args.application_plugin {
                root.application_plugin = Some(application_plugin.clone());
            }
            if root.snapshot_source.is_some() && root.snapshot_mode != "transactional" {
                bail!("--snapshot-source requires snapshot_mode transactional");
            }
            apply_root_large_set(&mut root, &args)?;
            save_root(&conn, &root)?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "config-change", None, None, Some(&root.id))?;
            println!("updated root {} -> {}", root.id, root.path.display());
        }
        RootCommand::List => {
            for root in roots(&conn)? {
                println!(
                    "{}\t{}\t{}\t{}",
                    root.id,
                    root.status,
                    root.name,
                    root.path.display()
                );
            }
        }
        RootCommand::Remove { id } => {
            let _ = root_by_id(&conn, &id)?;
            conn.execute("delete from roots where id=?1", params![id])?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-removed", None, None, Some(&id))?;
            println!("removed root {id}");
        }
        RootCommand::Pause { id } => {
            update_root_status(&conn, &id, "paused")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-paused", None, None, Some(&id))?;
            println!("paused root {id}");
        }
        RootCommand::Resume { id } => {
            update_root_status(&conn, &id, "active")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-resumed", None, None, Some(&id))?;
            println!("resumed root {id}");
        }
        RootCommand::MarkDeleted { id } => {
            update_root_status(&conn, &id, "deleted")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-mark-deleted", None, None, Some(&id))?;
            println!("marked root {id} deleted");
        }
    }
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
                snapshot_operation_kind(args.message.as_deref()),
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
                    snapshot_operation_kind(args.message.as_deref()),
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
                snapshot_operation_kind(args.message.as_deref()),
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
        snapshot_operation_kind(args.message.as_deref()),
        manifest.parent.as_deref(),
        Some(&manifest.snapshot_id),
        args.message.as_deref(),
    )?;
    println!("snapshot {}", manifest.snapshot_id);
    println!("files {total_files}, large {large_files}");
    record_event(paths, "snapshot-finish", &manifest.snapshot_id)?;
    Ok(())
}

fn snapshot_operation_kind(message: Option<&str>) -> &'static str {
    if message
        .map(|message| message.starts_with("watch "))
        .unwrap_or(false)
    {
        "file-events-batch"
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
    record_op_with_id_and_status(
        conn,
        op_id,
        kind,
        parent,
        parent,
        "failed",
        Some(&format!("snapshot failed for root {root_id}: {err:#}")),
    )
}

fn status(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let roots = roots(&conn)?;
    let current = current_snapshot(&conn)?;
    println!("home {}", paths.home.display());
    println!("roots {}", roots.len());
    for root in roots {
        let state = if root.status == "active" && !root.path.exists() {
            "missing"
        } else {
            root.status.as_str()
        };
        println!("  {}\t{}\t{}", root.id, state, root.path.display());
    }
    println!("current {}", current.unwrap_or_else(|| "(none)".into()));
    Ok(())
}

fn log_ops(paths: &Paths, args: LogArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    print_op_log(&conn, &args)
}

fn print_op_log(conn: &Connection, args: &LogArgs) -> Result<()> {
    let mut stmt = conn.prepare(
        "select id, kind, before_snapshot, after_snapshot, created_at, message
         from operations order by rowid desc",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;
    let mut printed = 0usize;
    for row in rows {
        let (id, kind, before, after, created, message) = row?;
        if let Some(root) = &args.root {
            let matches_root = message.as_deref() == Some(root)
                || before
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(&conn, snapshot, root).ok())
                    .unwrap_or(false)
                || after
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(&conn, snapshot, root).ok())
                    .unwrap_or(false);
            if !matches_root {
                continue;
            }
        }
        if printed >= args.limit {
            break;
        }
        println!(
            "{id}\t{created}\t{kind}\t{} -> {}\t{}",
            before.unwrap_or_default(),
            after.unwrap_or_default(),
            message.unwrap_or_default()
        );
        printed += 1;
    }
    Ok(())
}

fn op_cmd(paths: &Paths, command: OpCommand) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    match command {
        OpCommand::Log(args) => print_op_log(&conn, &args),
        OpCommand::Show { op_id } => {
            let op = query_operation(&conn, &op_id)?;
            println!("id {}", op.id);
            println!("parent {}", op.parent_op.unwrap_or_else(|| "(none)".into()));
            println!("kind {}", op.kind);
            println!("actor {}", op.actor);
            println!("status {}", op.status);
            println!(
                "before {}",
                op.before_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!(
                "after {}",
                op.after_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!("created_at {}", op.created_at);
            println!("message {}", op.message.unwrap_or_default());
            Ok(())
        }
        OpCommand::Restore { op_id } => {
            let op = query_operation(&conn, &op_id)?;
            let before = current_snapshot(&conn)?;
            let snapshot = op
                .after_snapshot
                .or(op.before_snapshot)
                .ok_or_else(|| anyhow!("operation has no snapshot to restore: {op_id}"))?;
            conn.execute(
                "insert into refs(name, value) values ('current', ?1)
                 on conflict(name) do update set value=excluded.value",
                params![snapshot],
            )?;
            record_op(
                &conn,
                "op-restore",
                before.as_deref(),
                Some(&snapshot),
                Some(&op_id),
            )?;
            println!("current {}", snapshot);
            Ok(())
        }
    }
}

fn lifecycle_cmd(paths: &Paths, command: LifecycleCommand) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    match command {
        LifecycleCommand::Policy { provider } => match provider.as_str() {
            "gcs" => {
                let policy = majutsu_policy::gcs_lifecycle_policy(&policy_config(&config.tiering))?;
                println!("{}", serde_json::to_string_pretty(&policy)?);
            }
            "s3" | "aws" => {
                let policy = majutsu_policy::s3_lifecycle_policy(&policy_config(&config.tiering))?;
                println!("{}", serde_json::to_string_pretty(&policy)?);
            }
            other => bail!("unsupported lifecycle provider: {other}"),
        },
    }
    Ok(())
}

fn diff_cmd(paths: &Paths, args: DiffArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    if args.at.is_some() && args.from.is_some() {
        bail!("use either a positional from snapshot or --at, not both");
    }
    let to_id = args
        .to
        .clone()
        .or_else(|| current_snapshot(&conn).ok().flatten())
        .ok_or_else(|| anyhow!("no target snapshot"))?;
    let to = load_snapshot_by_id(&conn, &to_id)?;
    let from_id = if let Some(at) = &args.at {
        Some(snapshot_id_at(&conn, at)?)
    } else {
        args.from.or_else(|| to.parent.clone())
    };
    let from = if let Some(from_id) = from_id {
        Some(load_snapshot_by_id(&conn, &from_id)?)
    } else {
        None
    };
    let from_files = from
        .as_ref()
        .map(snapshot_file_map)
        .transpose()?
        .unwrap_or_default();
    let to_files = snapshot_file_map(&to)?;
    let mut paths_all = from_files.keys().cloned().collect::<Vec<_>>();
    paths_all.extend(
        to_files
            .keys()
            .filter(|key| !from_files.contains_key(*key))
            .cloned(),
    );
    paths_all.sort();
    for key in paths_all {
        if let Some(root) = &args.root {
            if !key.starts_with(&format!("{root}/")) {
                continue;
            }
        }
        match (from_files.get(&key), to_files.get(&key)) {
            (None, Some(_)) => println!("A\t{key}"),
            (Some(_), None) => println!("D\t{key}"),
            (Some(a), Some(b)) if serde_json::to_value(a)? != serde_json::to_value(b)? => {
                println!("M\t{key}");
            }
            _ => {}
        }
    }
    Ok(())
}

fn restore_cmd(paths: &Paths, top_args: RestoreTopArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let command = top_args
        .command
        .unwrap_or_else(|| RestoreCommand::Apply(top_args.args));
    match command {
        RestoreCommand::Plan(args) => {
            let plan = build_restore_plan(paths, &conn, &args)?;
            print_restore_plan(paths, &conn, &plan)?;
            if args.check_conflicts {
                let conflicts = restore_conflicts(paths, &conn, &plan)?;
                print_restore_conflicts(&conflicts);
            }
        }
        RestoreCommand::Apply(args) => {
            let plan = build_restore_plan(paths, &conn, &args)?;
            apply_restore_plan(paths, &plan, args.force, args.check_conflicts)?;
            let after = plan.snapshot.snapshot_id.as_str();
            record_op(
                &conn,
                "restore",
                None,
                Some(after),
                Some(&format!("to {}", restore_target_label(&plan))),
            )?;
            print_restore_plan(paths, &conn, &plan)?;
            println!("restored to {}", restore_target_label(&plan));
        }
        RestoreCommand::Prepare(args) => {
            let plan = build_restore_plan(paths, &conn, &args)?;
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
            let plan = build_restore_plan(paths, &conn, &args)?;
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

fn mount_cmd(paths: &Paths, args: MountArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let restore_args = RestoreArgs {
        snapshot: args.snapshot.clone(),
        at: args.at.clone(),
        root: args.root.clone(),
        path: args.path.clone(),
        to: Some(args.mountpoint.clone()),
        force: true,
        check_conflicts: false,
    };
    let plan = build_restore_plan(paths, &conn, &restore_args)?;
    if args.backend == "fuse" {
        return mount_fuse_cmd(paths, &conn, &plan);
    }
    if args.backend != "materialized" {
        bail!("mount backend must be materialized or fuse");
    }
    let mountpoint = plan
        .to
        .as_ref()
        .ok_or_else(|| anyhow!("mount requires a target directory"))?;
    prepare_mountpoint(mountpoint)?;
    let lazy_root = mountpoint.join(".majutsu-lazy");
    let mut lazy_files = 0usize;
    let mut hydrated_large = 0usize;
    let mut directory_metadata = Vec::new();
    for record in &plan.files {
        let dest = mountpoint.join(&record.root_id).join(&record.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        match &record.payload {
            Payload::Directory => {
                prepare_directory_restore_destination(&dest, false)?;
                fs::create_dir_all(&dest)?;
                directory_metadata.push((dest, record));
            }
            Payload::Special { special_kind } => {
                restore_special_file(&dest, record, special_kind, true)?;
            }
            Payload::Symlink { target } => {
                #[cfg(unix)]
                std::os::unix::fs::symlink(target, &dest)?;
                #[cfg(not(unix))]
                fs::write(&dest, target)?;
            }
            payload => {
                if let Some((oid, object_key)) = payload_blob_ref(payload) {
                    write_atomic(&dest, &read_blob_payload(paths, &conn, oid, object_key)?)?;
                    apply_file_metadata(&dest, record)?;
                } else if let Some((_, manifest_key, chunk_count)) = payload_large_ref(payload) {
                    if args.hydrate_large {
                        let manifest: LargeManifest =
                            serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                        write_large_chunks_atomic(paths, &dest, &manifest)?;
                        apply_file_metadata(&dest, record)?;
                        hydrated_large += 1;
                    } else {
                        let file = File::create(&dest)?;
                        file.set_len(record.size)?;
                        apply_file_metadata(&dest, record)?;
                        let sidecar = lazy_root
                            .join(&record.root_id)
                            .join(format!("{}.json", record.path));
                        if let Some(parent) = sidecar.parent() {
                            fs::create_dir_all(parent)?;
                        }
                        let entry = LazyMountEntry {
                            version: 1,
                            snapshot_id: plan.snapshot.snapshot_id.clone(),
                            root_id: record.root_id.clone(),
                            path: record.path.clone(),
                            size: record.size,
                            manifest_key: manifest_key.to_string(),
                            chunk_count,
                        };
                        fs::write(sidecar, serde_json::to_vec_pretty(&entry)?)?;
                        lazy_files += 1;
                    }
                }
            }
        }
    }
    for (dest, record) in directory_metadata {
        apply_file_metadata(&dest, record)?;
    }
    record_op(
        &conn,
        "mount",
        None,
        Some(&plan.snapshot.snapshot_id),
        Some(&format!("at {}", mountpoint.display())),
    )?;
    let mount_metadata = MountViewMetadata {
        version: 1,
        snapshot_id: plan.snapshot.snapshot_id.clone(),
        created_at: Utc::now(),
        hydrate_large: args.hydrate_large,
        files: plan.files.len(),
        lazy_large_files: lazy_files,
        hydrated_large_files: hydrated_large,
    };
    fs::write(
        mountpoint.join(".majutsu-mount.json"),
        serde_json::to_vec_pretty(&mount_metadata)?,
    )?;
    println!("mounted snapshot {}", plan.snapshot.snapshot_id);
    println!("target {}", mountpoint.display());
    println!("files {}", plan.files.len());
    println!("lazy_large_files {lazy_files}");
    println!("hydrated_large_files {hydrated_large}");
    Ok(())
}

fn unmount_cmd(paths: &Paths, args: UnmountArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let marker = args.mountpoint.join(".majutsu-mount.json");
    if !marker.exists() && is_mountpoint(&args.mountpoint)? {
        unmount_fuse(&args.mountpoint)?;
        record_op(
            &conn,
            "unmount-fuse",
            None,
            None,
            Some(&format!("from {}", args.mountpoint.display())),
        )?;
        println!("unmounted {}", args.mountpoint.display());
        return Ok(());
    }
    if !marker.exists() {
        bail!(
            "{} is not a majutsu mount view; missing .majutsu-mount.json",
            args.mountpoint.display()
        );
    }
    let metadata: MountViewMetadata = serde_json::from_slice(&fs::read(&marker)?)
        .with_context(|| format!("read mount metadata {}", marker.display()))?;
    fs::remove_dir_all(&args.mountpoint)
        .with_context(|| format!("remove mount view {}", args.mountpoint.display()))?;
    record_op(
        &conn,
        "unmount",
        Some(&metadata.snapshot_id),
        None,
        Some(&format!("from {}", args.mountpoint.display())),
    )?;
    println!("unmounted {}", args.mountpoint.display());
    println!("snapshot {}", metadata.snapshot_id);
    Ok(())
}

fn hydrate_cmd(paths: &Paths, args: HydrateArgs) -> Result<()> {
    ensure_ready(paths)?;
    if let Some(path) = &args.path {
        validate_relative_filter_path(path, "hydrate --path")?;
    }
    let conn = open_db(paths)?;
    let lazy_root = args.view.join(".majutsu-lazy");
    if !lazy_root.exists() {
        bail!("lazy metadata not found: {}", lazy_root.display());
    }
    let requested_path = args.path.as_ref().map(|path| path_to_slash(path));
    let mut sidecars = Vec::new();
    for entry in WalkDir::new(&lazy_root).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() || entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let lazy: LazyMountEntry = serde_json::from_slice(&fs::read(entry.path())?)
            .with_context(|| format!("read lazy metadata {}", entry.path().display()))?;
        if args
            .root
            .as_deref()
            .is_some_and(|root| root != lazy.root_id)
        {
            continue;
        }
        if requested_path
            .as_deref()
            .is_some_and(|path| path != lazy.path)
        {
            continue;
        }
        sidecars.push((entry.path().to_path_buf(), lazy));
    }
    if sidecars.is_empty() {
        bail!("no lazy large files matched");
    }
    let mut hydrated = 0usize;
    for (sidecar, lazy) in sidecars {
        let manifest: LargeManifest =
            serde_json::from_slice(&read_object(paths, &lazy.manifest_key)?)
                .with_context(|| format!("read large manifest {}", lazy.manifest_key))?;
        let dest = args.view.join(&lazy.root_id).join(&lazy.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        write_large_chunks_atomic(paths, &dest, &manifest)?;
        fs::remove_file(sidecar)?;
        hydrated += 1;
    }
    record_op(
        &conn,
        "hydrate",
        None,
        None,
        Some(&format!("view {}", args.view.display())),
    )?;
    println!("hydrated_large_files {hydrated}");
    Ok(())
}

fn large_cmd(paths: &Paths, command: LargeCommand) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    match command {
        LargeCommand::List => {
            let mut stmt = conn.prepare(
                "select l.oid, l.size, l.chunk_count, l.manifest_key, p.oid is not null
                 from large_objects l left join large_pins p on p.oid = l.oid
                 order by l.rowid desc",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, usize>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            })?;
            for row in rows {
                let (oid, size, chunks, key, pinned) = row?;
                let pin = if pinned { "pinned" } else { "unpinned" };
                println!("{oid}\t{size}\t{chunks}\t{pin}\t{key}");
            }
        }
        LargeCommand::Stat => {
            let count: i64 =
                conn.query_row("select count(*) from large_objects", [], |r| r.get(0))?;
            let bytes: Option<u64> =
                conn.query_row("select sum(size) from large_objects", [], |r| r.get(0))?;
            let chunks: i64 = conn.query_row("select count(*) from chunks", [], |r| r.get(0))?;
            let pins: i64 = conn.query_row("select count(*) from large_pins", [], |r| r.get(0))?;
            println!("large_objects {count}");
            println!("logical_bytes {}", bytes.unwrap_or(0));
            println!("chunks {chunks}");
            println!("pinned {pins}");
        }
        LargeCommand::Verify => fsck(paths)?,
        LargeCommand::Pin(args) => {
            let snapshot =
                current_snapshot(&conn)?.ok_or_else(|| anyhow!("no current snapshot"))?;
            let manifests = large_pin_snapshots(&conn, args.since.as_deref(), &snapshot)?;
            let mut pinned = 0usize;
            let mut seen = BTreeSet::new();
            for manifest in manifests {
                for (root_id, records) in manifest.roots {
                    if args.root.as_deref().is_some_and(|filter| filter != root_id) {
                        continue;
                    }
                    for record in records {
                        if let Some((oid, _, _)) = payload_large_ref(&record.payload) {
                            let oid = oid.to_string();
                            if seen.insert(oid.clone()) {
                                conn.execute(
                                    "insert or replace into large_pins(oid, pinned_at, reason) values (?1, ?2, ?3)",
                                    params![
                                        oid,
                                        Utc::now().to_rfc3339(),
                                        args.since
                                            .as_ref()
                                            .map(|since| format!("pin since {since}"))
                                    ],
                                )?;
                                pinned += 1;
                            }
                        }
                    }
                }
            }
            record_op(
                &conn,
                "large-pin",
                Some(&snapshot),
                Some(&snapshot),
                Some(&format!("pinned {pinned} large objects")),
            )?;
            println!("pinned {pinned}");
        }
        LargeCommand::Unpin(args) => {
            let removed = if let Some(older_than) = args.older_than {
                let cutoff = parse_duration_ago(&older_than)?;
                conn.execute(
                    "delete from large_pins where pinned_at <= ?1",
                    params![cutoff.to_rfc3339()],
                )?
            } else {
                conn.execute("delete from large_pins", [])?
            };
            record_op(
                &conn,
                "large-unpin",
                current_snapshot(&conn)?.as_deref(),
                current_snapshot(&conn)?.as_deref(),
                Some(&format!("unpinned {removed} large objects")),
            )?;
            println!("unpinned {removed}");
        }
    }
    Ok(())
}

fn large_pin_snapshots(
    conn: &Connection,
    since: Option<&str>,
    current_snapshot_id: &str,
) -> Result<Vec<SnapshotManifest>> {
    let Some(since) = since else {
        return Ok(vec![load_snapshot_by_id(conn, current_snapshot_id)?]);
    };
    let cutoff = parse_pin_since(since)?;
    let mut stmt =
        conn.prepare("select manifest_json, created_at from snapshots order by created_at")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut manifests = Vec::new();
    for row in rows {
        let (json, created_at) = row?;
        if parse_db_time(&created_at)? >= cutoff {
            manifests.push(serde_json::from_str(&json)?);
        }
    }
    Ok(manifests)
}

fn parse_pin_since(input: &str) -> Result<DateTime<Utc>> {
    parse_duration_ago(input).or_else(|_| {
        let parsed = parse_time(input)?;
        parse_db_time(&parsed)
    })
}

fn sync_cmd(paths: &Paths, command: Option<SyncCommand>) -> Result<()> {
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
    if let Some(SyncCommand::Status) = command {
        return sync_status(paths, &conn, &remote);
    }
    sync_configured_remote(paths, &conn, &config, &remote)
}

pub(crate) fn sync_current_if_remote(paths: &Paths) -> Result<bool> {
    let config = read_config(paths)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(false);
    };
    let remote = open_remote_with_upload_policy(
        remote_config,
        config.large.multipart,
        config.large.max_parallel_uploads,
    )?;
    let conn = open_db(paths)?;
    sync_configured_remote(paths, &conn, &config, &remote)?;
    Ok(true)
}

fn sync_configured_remote(
    paths: &Paths,
    conn: &Connection,
    config: &Config,
    remote: &RemoteStore,
) -> Result<()> {
    let current = current_snapshot(&conn)?;
    let previous_last_synced = ref_value(&conn, "last-synced")?;
    let synced_at = Utc::now().to_rfc3339();
    set_ref_value(&conn, "last-synced", &synced_at)?;
    let sync_op = record_op(
        &conn,
        "remote-sync",
        current.as_deref(),
        current.as_deref(),
        Some("pushed metadata and objects"),
    )?;
    let result = enqueue_and_drain_sync(paths, &conn, &config, &remote);
    if result.is_err() {
        restore_ref_value(&conn, "last-synced", previous_last_synced.as_deref())?;
        delete_operation(&conn, &sync_op)?;
    }
    result
}

fn enqueue_and_drain_sync(
    paths: &Paths,
    conn: &Connection,
    config: &Config,
    remote: &RemoteStore,
) -> Result<()> {
    let export = export_metadata(conn, config)?;
    enqueue_inline_upload(
        paths,
        "metadata/export.json",
        serde_json::to_vec_pretty(&export)?,
    )?;
    let host_metadata = host_metadata_key(&config.host.id);
    enqueue_inline_upload(paths, &host_metadata, serde_json::to_vec_pretty(&export)?)?;
    for snapshot in &export.snapshots {
        enqueue_inline_upload(
            paths,
            &host_snapshot_key(&config.host.id, &snapshot.id),
            serde_json::to_vec_pretty(snapshot)?,
        )?;
        enqueue_inline_upload(
            paths,
            &host_snapshot_canonical_key(&config.host.id, &snapshot.id),
            encode_canonical_remote_export(paths, snapshot)?,
        )?;
    }
    for operation in &export.operations {
        enqueue_inline_upload(
            paths,
            &host_operation_key(&config.host.id, &operation.id),
            serde_json::to_vec_pretty(operation)?,
        )?;
        enqueue_inline_upload(
            paths,
            &host_operation_canonical_key(&config.host.id, &operation.id),
            encode_canonical_remote_export(paths, operation)?,
        )?;
    }
    enqueue_inline_upload(
        paths,
        &host_oplog_key(&config.host.id),
        encode_operation_log(&export.operations)?,
    )?;
    enqueue_inline_upload(
        paths,
        &host_oplog_canonical_key(&config.host.id),
        encode_canonical_remote_oplog(paths, &export.operations)?,
    )?;
    enqueue_inline_upload(
        paths,
        "config.toml",
        toml::to_string_pretty(&config)?.into_bytes(),
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
    let host_index = update_remote_host_index(&remote, &config, &export, &host_metadata)?;
    enqueue_inline_upload(
        paths,
        REMOTE_HOST_INDEX_KEY,
        serde_json::to_vec_pretty(&host_index)?,
    )?;
    enqueue_inline_upload(
        paths,
        &remote_gc_mark_key(&config.host.id),
        serde_json::to_vec_pretty(&build_gc_mark_export(&config, &export))?,
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
            enqueue_file_upload(paths, &key, &local)?;
            for alias in canonical_remote_aliases(&key) {
                if canonical_alias_uses_structured_encoding(&key) {
                    enqueue_inline_upload(
                        paths,
                        &alias,
                        encode_canonical_local_object(paths, &key)?,
                    )?;
                } else {
                    enqueue_file_upload(paths, &alias, &local)?;
                }
            }
        }
    }
    let uploaded = drain_upload_queue(paths, &remote)?;
    let pruned_remote_exports = prune_remote_host_exports(&remote, &config.host.id, &export)?;
    persist_export_remote_refs(conn, &remote.describe(), &config.host.id, &export.refs)?;
    println!("synced {} objects to {}", uploaded, remote.describe());
    println!("pruned_remote_exports {}", pruned_remote_exports);
    Ok(())
}

fn delete_operation(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("delete from operations where id=?1", params![id])?;
    rewrite_local_oplog(conn)?;
    Ok(())
}

fn sync_status(paths: &Paths, conn: &Connection, remote: &RemoteStore) -> Result<()> {
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
    println!("remote {}", remote.describe());
    println!(
        "local_current {}",
        local_current.unwrap_or_else(|| "(none)".into())
    );
    println!(
        "remote_current {}",
        remote_current.unwrap_or_else(|| "(none)".into())
    );
    println!(
        "remote_last_synced {}",
        remote_last_synced.unwrap_or_else(|| "(none)".into())
    );
    println!("local_objects {}", local_keys.len());
    println!("missing_remote_objects {}", missing_remote);
    println!("queued_uploads {}", upload_queue_items(paths)?.len());
    Ok(())
}

pub(crate) fn remote_ref(remote: &RemoteStore, key: &str) -> Result<Option<String>> {
    if remote.exists(key)? {
        return Ok(Some(
            String::from_utf8(remote.get(key)?)?.trim().to_string(),
        ));
    }
    Ok(None)
}

fn encode_canonical_remote_export<T: Serialize>(paths: &Paths, value: &T) -> Result<Vec<u8>> {
    let cbor = serde_cbor::to_vec(value)?;
    let compressed = zstd::stream::encode_all(cbor.as_slice(), 3)?;
    encode_object(paths, &compressed)
}

pub(crate) fn decode_canonical_remote_export<T: for<'de> Deserialize<'de>>(
    paths: &Paths,
    bytes: &[u8],
) -> Result<T> {
    let compressed = decode_object(paths, bytes)?;
    let cbor = zstd::stream::decode_all(compressed.as_slice())?;
    Ok(serde_cbor::from_slice(&cbor)?)
}

fn encode_canonical_remote_oplog(paths: &Paths, operations: &[OperationExport]) -> Result<Vec<u8>> {
    let cborl = encode_operation_log(operations)?;
    let compressed = zstd::stream::encode_all(cborl.as_slice(), 3)?;
    encode_object(paths, &compressed)
}

pub(crate) fn decode_canonical_remote_oplog(
    paths: &Paths,
    bytes: &[u8],
) -> Result<Vec<OperationExport>> {
    let compressed = decode_object(paths, bytes)?;
    let cborl = zstd::stream::decode_all(compressed.as_slice())?;
    decode_operation_log(&cborl)
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
    let mut object_keys = local_object_keys(export);
    for key in object_keys.clone() {
        object_keys.extend(canonical_remote_aliases(&key));
    }
    object_keys.sort();
    object_keys.dedup();
    GcMarkExport::new(
        config.host.id.clone(),
        Utc::now(),
        export.refs.get("current").cloned(),
        object_keys,
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

fn write_remote_gc_tombstone(remote: &RemoteStore, host_id: &str, key: &str) -> Result<()> {
    let tombstone = GcTombstoneExport::new(host_id.to_string(), Utc::now(), key.to_string());
    remote.put(
        &remote_gc_tombstone_key(host_id, &new_id("tombstone")),
        &serde_json::to_vec_pretty(&tombstone)?,
    )
}

pub(crate) fn read_remote_host_index(remote: &RemoteStore) -> Result<RemoteHostIndex> {
    if remote.exists(REMOTE_HOST_INDEX_KEY)? {
        let mut index: RemoteHostIndex =
            serde_json::from_slice(&remote.get(REMOTE_HOST_INDEX_KEY)?)?;
        index.sort_hosts();
        return Ok(index);
    }
    Ok(RemoteHostIndex::empty(Utc::now()))
}

fn remote_host_index_with_legacy(remote: &RemoteStore) -> Result<RemoteHostIndex> {
    let mut index = read_remote_host_index(remote)?;
    if index.hosts.is_empty() && remote.exists(LEGACY_METADATA_EXPORT_KEY)? {
        let export: MetadataExport =
            serde_json::from_slice(&remote.get(LEGACY_METADATA_EXPORT_KEY)?)?;
        index.hosts.push(RemoteHostSummary {
            id: export.config.host.id.clone(),
            name: export.config.host.name.clone(),
            last_synced_at: export.exported_at,
            current_snapshot: export.refs.get("current").cloned(),
            metadata_key: LEGACY_METADATA_EXPORT_KEY.into(),
        });
    }
    Ok(index)
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

fn remote_cmd(paths: &Paths, command: RemoteCommand) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let remote = open_remote_with_upload_policy(
        config
            .remote
            .as_ref()
            .ok_or_else(|| anyhow!("remote is not configured; run `mj init --remote ...`"))?,
        config.large.multipart,
        config.large.max_parallel_uploads,
    )?;
    match command {
        RemoteCommand::Check => {
            let keys = remote.list("")?;
            println!("remote {}", remote.describe());
            println!("objects {}", keys.len());
            let metadata_key = if remote.exists(REMOTE_HOST_INDEX_KEY)? {
                REMOTE_HOST_INDEX_KEY
            } else if remote.exists(LEGACY_METADATA_EXPORT_KEY)? {
                LEGACY_METADATA_EXPORT_KEY
            } else {
                bail!(
                    "remote metadata is missing: metadata/export.json and hosts/index.json not found"
                );
            };
            if remote.exists(metadata_key)? {
                println!("metadata ok");
                println!("metadata_key {metadata_key}");
                let first = remote.get_range(metadata_key, 0, 1)?;
                println!("range_get {}", first.len());
            }
        }
        RemoteCommand::Fsck => {
            remote_fsck(paths, &remote)?;
            let conn = open_db(paths)?;
            let current = current_snapshot(&conn)?;
            record_op(
                &conn,
                "fsck",
                current.as_deref(),
                current.as_deref(),
                Some("checked remote state"),
            )?;
        }
        RemoteCommand::Capabilities => {
            let capabilities = remote.capabilities();
            println!("remote {}", remote.describe());
            println!("lifecycle_rules {}", capabilities.lifecycle_rules);
            println!("object_tags {}", capabilities.object_tags);
            println!("storage_class_on_put {}", capabilities.storage_class_on_put);
            println!(
                "restore_archived_object {}",
                capabilities.restore_archived_object
            );
            println!("multipart_upload {}", capabilities.multipart_upload);
            println!("range_get {}", capabilities.range_get);
            println!("conditional_put {}", capabilities.conditional_put);
        }
        RemoteCommand::Hosts => {
            let index = remote_host_index_with_legacy(&remote)?;
            println!("remote {}", remote.describe());
            println!("hosts {}", index.hosts.len());
            for host in index.hosts {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    host.id,
                    host.name,
                    host.last_synced_at.to_rfc3339(),
                    host.current_snapshot.unwrap_or_else(|| "(none)".into()),
                    host.metadata_key
                );
            }
        }
        RemoteCommand::Host { id } => {
            let index = remote_host_index_with_legacy(&remote)?;
            let host = select_remote_host(index.hosts, &id)?;
            let export: MetadataExport = serde_json::from_slice(&remote.get(&host.metadata_key)?)?;
            println!("id {}", host.id);
            println!("name {}", host.name);
            println!("last_synced_at {}", host.last_synced_at.to_rfc3339());
            println!(
                "current_snapshot {}",
                host.current_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!("metadata_key {}", host.metadata_key);
            println!("roots {}", export.roots.len());
            println!("snapshots {}", export.snapshots.len());
            println!("operations {}", export.operations.len());
        }
    }
    Ok(())
}

fn clone_cmd(paths: &Paths, args: CloneArgs) -> Result<()> {
    if paths.home.exists() && paths.home.read_dir()?.next().is_some() {
        bail!("target majutsu home is not empty: {}", paths.home.display());
    }
    let remote_config = RemoteConfig::from_url(args.remote);
    let remote = open_remote(&remote_config)?;
    let metadata_key = clone_metadata_key(&remote, args.host.as_deref())?;
    let export_bytes = remote.get(&metadata_key)?;
    let mut export: MetadataExport = serde_json::from_slice(&export_bytes)?;
    export.config.remote = Some(remote_config);
    validate_config(&export.config)?;
    ensure_clone_objects_available(&remote, &export)?;
    let staging_home = clone_staging_home(&paths.home);
    let staging_paths = resolve_paths(Some(staging_home.clone()))?;
    let clone_result = (|| -> Result<()> {
        create_layout(&staging_paths)?;
        write_config(&staging_paths, &export.config)?;
        fs::write(
            &staging_paths.host,
            toml::to_string_pretty(&export.config.host)?,
        )?;
        if remote.exists("keys/recipients.toml")? {
            fs::write(
                staging_paths.home.join("keys/recipients.toml"),
                remote.get("keys/recipients.toml")?,
            )?;
        }
        if export.config.security.encryption != "none" {
            if let Ok(key) = env::var("MAJUTSU_MASTER_KEY") {
                write_master_key(&staging_paths, &key)?;
            }
        }
        for key in local_object_keys(&export) {
            let dest = staging_paths.home.join(&key);
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(
                dest,
                download_local_object_from_remote(&staging_paths, &remote, &key)?,
            )?;
        }
        let conn = open_db(&staging_paths)?;
        import_metadata(&conn, &export)?;
        persist_export_remote_refs(
            &conn,
            &remote.describe(),
            &export.config.host.id,
            &export.refs,
        )?;
        Ok(())
    })();
    if let Err(err) = clone_result {
        let _ = fs::remove_dir_all(&staging_home);
        return Err(err);
    }
    if paths.home.exists() {
        fs::remove_dir(&paths.home)
            .with_context(|| format!("remove empty clone target {}", paths.home.display()))?;
    }
    fs::rename(&staging_home, &paths.home).with_context(|| {
        format!(
            "move clone staging {} to {}",
            staging_home.display(),
            paths.home.display()
        )
    })?;
    println!("cloned {} into {}", remote.describe(), paths.home.display());
    println!("host {} {}", export.config.host.name, export.config.host.id);
    Ok(())
}

fn clone_staging_home(home: &Path) -> PathBuf {
    let parent = home.parent().unwrap_or_else(|| Path::new("."));
    let name = home
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "majutsu".into());
    parent.join(format!(".{name}.clone-{}", Uuid::new_v4()))
}

fn ensure_clone_objects_available(remote: &RemoteStore, export: &MetadataExport) -> Result<()> {
    let mut missing = Vec::new();
    for key in local_object_keys(export) {
        if !remote_object_available(remote, &key)? {
            missing.push(key);
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let sample = missing
        .iter()
        .take(5)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if missing.len() > 5 {
        format!(", ... {} more", missing.len() - 5)
    } else {
        String::new()
    };
    bail!(
        "remote is missing {} object(s) required for clone: {sample}{suffix}",
        missing.len()
    )
}

fn clone_metadata_key(remote: &RemoteStore, host: Option<&str>) -> Result<String> {
    if let Some(host_id) = host {
        let index = remote_host_index_with_legacy(remote)?;
        return Ok(select_remote_host(index.hosts, host_id)?.metadata_key);
    }
    let index = remote_host_index_with_legacy(remote)?;
    match index.hosts.as_slice() {
        [host] => Ok(host.metadata_key.clone()),
        [] if remote.exists(LEGACY_METADATA_EXPORT_KEY)? => Ok(LEGACY_METADATA_EXPORT_KEY.into()),
        [] => {
            bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found")
        }
        _ => bail!("remote contains multiple hosts; rerun clone with --host"),
    }
}

fn download_local_object_from_remote(
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

fn remote_object_available(remote: &RemoteStore, key: &str) -> Result<bool> {
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

fn daemon_cmd(paths: &Paths, command: DaemonCommand) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    match command {
        DaemonCommand::Start {
            backend,
            mode,
            interval_secs,
            settle_ms,
            periodic_rescan_secs,
        } => {
            let configured_backend = backend.unwrap_or_else(|| config.watch.backend.clone());
            let backend = normalize_watch_backend(&configured_backend)?;
            let mode = mode.unwrap_or_else(|| config.watch.mode.clone());
            validate_watch_mode(&mode)?;
            let pid = start_watch_daemon(
                paths,
                backend,
                &mode,
                interval_secs.unwrap_or(config.watch.interval),
                config.watch.debounce,
                settle_ms.unwrap_or(config.watch.settle),
                periodic_rescan_secs.unwrap_or(config.watch.periodic_rescan),
            )?;
            println!("started daemon pid {pid}");
        }
        DaemonCommand::Service { provider } => {
            let exe = env::current_exe()?;
            let backend = normalize_watch_backend(&config.watch.backend)?;
            let service = render_daemon_service(
                &provider,
                &exe,
                &paths.home,
                backend,
                &config.watch.mode,
                config.watch.interval,
                config.watch.debounce,
                config.watch.settle,
                config.watch.periodic_rescan,
            )
            .map_err(anyhow::Error::msg)?;
            print!("{service}");
        }
        DaemonCommand::Stop => {
            let pid =
                read_pid(&paths.daemon_pid)?.ok_or_else(|| anyhow!("daemon pid file not found"))?;
            if pid_alive(pid) {
                let status = ProcessCommand::new("kill").arg(pid.to_string()).status()?;
                if !status.success() {
                    bail!("failed to stop daemon pid {pid}");
                }
            }
            let _ = fs::remove_file(&paths.daemon_pid);
            let _ = fs::remove_file(paths.runtime.join("daemon.sock"));
            println!("stopped daemon pid {pid}");
        }
        DaemonCommand::Status => {
            if let Some(pid) = read_pid(&paths.daemon_pid)? {
                if pid_alive(pid) {
                    if let Ok(reply) = daemon_ipc_request(paths, "status") {
                        println!("{reply}");
                    } else {
                        println!("running pid {pid}");
                        println!("ipc unavailable");
                    }
                } else {
                    println!("stale pid {pid}");
                }
            } else {
                println!("stopped");
            }
        }
    }
    Ok(())
}

fn key_cmd(paths: &Paths, command: KeyCommand) -> Result<()> {
    match command {
        KeyCommand::Export => {
            ensure_ready(paths)?;
            let key = read_master_key(paths)?;
            println!("{key}");
        }
        KeyCommand::Import { hex } => {
            create_layout(paths)?;
            validate_key_hex(&hex)?;
            write_master_key(paths, &hex)?;
            println!("imported master key into {}", paths.master_key.display());
        }
        KeyCommand::Rotate { new_key } => {
            ensure_ready(paths)?;
            let rotated = rotate_master_key(paths, new_key)?;
            println!("rotated master key");
            println!("objects_rewritten {}", rotated.objects);
            println!("snapshots_rewritten {}", rotated.snapshots);
            println!("new_key {}", rotated.new_key);
        }
    }
    Ok(())
}

struct KeyRotationResult {
    objects: usize,
    snapshots: usize,
    new_key: String,
}

fn rotate_master_key(paths: &Paths, new_key: Option<String>) -> Result<KeyRotationResult> {
    let config = read_config(paths)?;
    if !encryption_enabled(&config.security)? {
        bail!("key rotation requires encrypted state");
    }
    let conn = open_db(paths)?;
    let old_key = read_master_key(paths)?;
    let new_key = new_key.unwrap_or(random_key_hex()?);
    validate_key_hex(&new_key)?;
    if old_key.trim() == new_key.trim() {
        bail!("new key must differ from current key");
    }

    let blobs = query_blobs(&conn)?;
    let chunks = query_chunks(&conn)?;
    let large_objects = query_large_objects(&conn)?;
    let mut blob_payloads = BTreeMap::new();
    for blob in &blobs {
        blob_payloads.insert(
            blob.oid.clone(),
            read_blob_payload(paths, &conn, &blob.oid, &blob.object_key)?,
        );
    }
    let mut chunk_payloads = BTreeMap::new();
    for chunk in &chunks {
        chunk_payloads.insert(chunk.oid.clone(), read_object(paths, &chunk.object_key)?);
    }
    let mut large_manifests = BTreeMap::new();
    for large in &large_objects {
        let manifest: LargeManifest =
            serde_json::from_slice(&read_object(paths, &large.manifest_key)?)?;
        large_manifests.insert(large.oid.clone(), manifest);
    }
    let mut snapshots = Vec::new();
    let mut stmt = conn.prepare("select id, manifest_json from snapshots order by created_at")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (id, json) = row?;
        snapshots.push((id, serde_json::from_str::<SnapshotManifest>(&json)?));
    }

    write_master_key(paths, &new_key)?;
    let mut objects = 0usize;
    let mut blob_keys = BTreeMap::new();
    for blob in &blobs {
        let key = store_bytes(paths, &paths.objects, &blob.oid, &blob_payloads[&blob.oid])?;
        conn.execute(
            "update blobs set object_key=?2, pack_id=null, pack_offset=null, pack_len=null where oid=?1",
            params![blob.oid, key],
        )?;
        blob_keys.insert(blob.oid.clone(), key);
        objects += 1;
    }
    conn.execute("delete from packs", [])?;
    let mut chunk_keys = BTreeMap::new();
    for chunk in &chunks {
        let key = store_bytes(
            paths,
            &large_chunk_base_for_key(paths, &chunk.object_key),
            &chunk.oid,
            &chunk_payloads[&chunk.oid],
        )?;
        conn.execute(
            "update chunks set object_key=?2 where oid=?1",
            params![chunk.oid, key],
        )?;
        chunk_keys.insert(chunk.oid.clone(), key);
        objects += 1;
    }
    let mut large_manifest_keys = BTreeMap::new();
    for large in &large_objects {
        let mut manifest = large_manifests
            .remove(&large.oid)
            .ok_or_else(|| anyhow!("missing loaded large manifest {}", large.oid))?;
        for chunk in &mut manifest.chunks {
            chunk.object_key = chunk_keys
                .get(&chunk.oid)
                .ok_or_else(|| anyhow!("missing rotated chunk key {}", chunk.oid))?
                .clone();
        }
        let bytes = serde_json::to_vec_pretty(&manifest)?;
        let manifest_oid = blake3_hex(&bytes);
        let key = store_bytes(paths, &paths.large_manifests, &manifest_oid, &bytes)?;
        conn.execute(
            "update large_objects set manifest_key=?2 where oid=?1",
            params![large.oid, key],
        )?;
        large_manifest_keys.insert(large.oid.clone(), key);
        objects += 1;
    }

    let mut snapshots_rewritten = 0usize;
    for (snapshot_id, mut manifest) in snapshots {
        rewrite_manifest_payload_keys(&mut manifest, &blob_keys, &large_manifest_keys)?;
        manifest.root_trees.clear();
        for (root_id, records) in &manifest.roots {
            let tree = build_tree_manifest(root_id, records.clone())?;
            let tree_json = serde_json::to_vec_pretty(&tree)?;
            let tree_oid = blake3_hex(&tree_json);
            let tree_key = store_bytes(paths, &paths.trees, &tree_oid, &tree_json)?;
            manifest.root_trees.insert(
                root_id.clone(),
                RootSnapshot {
                    tree_id: tree.tree_id,
                    tree_key,
                    file_count: tree.entries.len(),
                },
            );
            objects += 1;
        }
        let manifest_json = serde_json::to_vec_pretty(&manifest)?;
        let manifest_oid = blake3_hex(&manifest_json);
        let manifest_key = store_bytes(paths, &paths.objects, &manifest_oid, &manifest_json)?;
        conn.execute(
            "update snapshots set manifest_key=?2, manifest_json=?3 where id=?1",
            params![snapshot_id, manifest_key, String::from_utf8(manifest_json)?],
        )?;
        snapshots_rewritten += 1;
        objects += 1;
    }
    record_op(
        &conn,
        "key-rotation",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!("rewrote {objects} objects")),
    )?;
    Ok(KeyRotationResult {
        objects,
        snapshots: snapshots_rewritten,
        new_key,
    })
}

fn rewrite_manifest_payload_keys(
    manifest: &mut SnapshotManifest,
    blob_keys: &BTreeMap<String, String>,
    large_manifest_keys: &BTreeMap<String, String>,
) -> Result<()> {
    for records in manifest.roots.values_mut() {
        for record in records {
            if let Some((oid, object_key)) = payload_blob_ref_mut(&mut record.payload) {
                *object_key = blob_keys
                    .get(oid)
                    .ok_or_else(|| anyhow!("missing rotated blob key {oid}"))?
                    .clone();
            } else if let Some((oid, manifest_key)) = payload_large_ref_mut(&mut record.payload) {
                *manifest_key = large_manifest_keys
                    .get(oid)
                    .ok_or_else(|| anyhow!("missing rotated large manifest key {oid}"))?
                    .clone();
            }
        }
    }
    Ok(())
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

fn build_tree_manifest(root_id: &str, records: Vec<FileRecord>) -> Result<TreeManifest> {
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
    match chunk.compression.as_str() {
        "none" => Ok(bytes),
        "zstd" => Ok(zstd::stream::decode_all(bytes.as_slice())?),
        other => bail!("unsupported large chunk compression: {other}"),
    }
}

fn create_layout(paths: &Paths) -> Result<()> {
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

fn export_metadata(conn: &Connection, config: &Config) -> Result<MetadataExport> {
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
        version: 1,
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
            "insert or replace into operations(id, parent_op, kind, actor, status, before_snapshot, after_snapshot, created_at, message)
             values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                op.id,
                op.parent_op,
                op.kind,
                op.actor,
                op.status,
                op.before_snapshot,
                op.after_snapshot,
                op.created_at,
                op.message
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

fn query_blobs(conn: &Connection) -> Result<Vec<BlobExport>> {
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

fn query_large_objects(conn: &Connection) -> Result<Vec<LargeObjectExport>> {
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

fn query_chunks(conn: &Connection) -> Result<Vec<ChunkExport>> {
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

fn store_bytes(paths: &Paths, base: &Path, oid: &str, bytes: &[u8]) -> Result<String> {
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

fn encode_object(paths: &Paths, bytes: &[u8]) -> Result<Vec<u8>> {
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

fn read_object(paths: &Paths, key: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(paths.home.join(key))?;
    decode_object(paths, &bytes)
}

fn read_blob_payload(
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
        let bytes = fs::read(paths.home.join(pack.pack_key))?;
        let slice = bytes
            .get(offset..offset + len)
            .ok_or_else(|| anyhow!("pack entry out of range for {oid}"))?;
        if slice.len() < 8 {
            bail!("pack entry too short for {oid}");
        }
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&slice[..8]);
        let stored_len = u64::from_le_bytes(len_bytes) as usize;
        if stored_len != slice.len() - 8 {
            bail!("pack entry length mismatch for {oid}");
        }
        decode_object(paths, &slice[8..])
    } else {
        read_object(paths, fallback_key)
    }
}

pub(crate) fn decode_object(paths: &Paths, bytes: &[u8]) -> Result<Vec<u8>> {
    majutsu_crypto::decode_object(bytes, &paths.master_key, &recipients_path(paths))
}

fn recipients_path(paths: &Paths) -> PathBuf {
    paths.home.join("keys/recipients.toml")
}

fn random_key_hex() -> Result<String> {
    majutsu_crypto::random_key_hex()
}

fn validate_key_hex(hex_key: &str) -> Result<()> {
    majutsu_crypto::validate_key_hex(hex_key)
}

fn read_master_key(paths: &Paths) -> Result<String> {
    majutsu_crypto::read_master_key(&paths.master_key)
}

fn write_master_key(paths: &Paths, hex_key: &str) -> Result<()> {
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
