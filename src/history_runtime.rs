use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, params};
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

use crate::cli::{DiffArgs, LogArgs, OpCommand};
use crate::config::{Config, Paths, read_config};
use crate::operation_log::{query_operation, record_op};
use crate::queue_runtime::{event_journal_records, upload_queue_stats};
use crate::remote_store::open_remote;
use crate::root_state::roots;
use crate::snapshot_state::{
    current_snapshot, load_snapshot_by_id, snapshot_contains_root, snapshot_file_map,
    snapshot_id_at,
};

pub(crate) fn status_cmd(paths: &Paths) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    let config = read_config(paths)?;
    let roots = roots(&conn)?;
    let current = current_snapshot(&conn)?;
    let db_stats = read_status_db_stats(&conn)?;
    let storage = read_storage_stats(paths)?;
    let upload_stats = upload_queue_stats(paths)?;
    let event_count = event_journal_records(paths)?.len();
    let restore_queue_count = count_json_files(&paths.home.join("queue/restores"))?;

    println!("home {}", paths.home.display());
    println!("config {}", paths.config.display());
    println!("database {}", paths.db.display());
    println!("host_id {}", config.host.id);
    println!("host_name {}", config.host.name);
    print_remote_status(&config)?;
    println!("security_encryption {}", config.security.encryption);
    println!("security_hash {}", config.security.hash);
    println!("watch_backend {}", config.watch.backend);
    println!("watch_mode {}", config.watch.mode);
    println!("watch_debounce_ms {}", config.watch.debounce);
    println!("watch_settle_ms {}", config.watch.settle);
    println!("large_enabled {}", config.large.enabled);
    println!("large_min_size_bytes {}", config.large.min_size);
    println!(
        "large_binary_min_size_bytes {}",
        config.large.binary_min_size
    );
    println!("large_chunking {}", config.large.default_chunking);
    println!("large_chunk_size_bytes {}", config.large.chunk_size);
    println!("large_multipart {}", config.large.multipart);
    println!("pack_small_target_bytes {}", config.pack.small_pack_target);
    println!(
        "pack_normal_target_bytes {}",
        config.pack.normal_pack_target
    );
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
    println!("snapshots {}", db_stats.snapshots);
    println!("operations {}", db_stats.operations);
    println!("refs {}", db_stats.refs);
    println!("blobs {}", db_stats.blobs);
    println!("blob_bytes {}", db_stats.blob_bytes);
    println!("large_objects {}", db_stats.large_objects);
    println!("large_object_bytes {}", db_stats.large_object_bytes);
    println!("chunks {}", db_stats.chunks);
    println!("chunk_bytes {}", db_stats.chunk_bytes);
    println!("packs {}", db_stats.packs);
    println!("pack_bytes {}", db_stats.pack_bytes);
    println!("large_pins {}", db_stats.large_pins);
    println!("remote_refs {}", db_stats.remote_refs);
    println!("state_files {}", storage.state_files);
    println!("state_bytes {}", storage.state_bytes);
    println!("objects_files {}", storage.objects_files);
    println!("objects_bytes {}", storage.objects_bytes);
    println!("logs_files {}", storage.logs_files);
    println!("logs_bytes {}", storage.logs_bytes);
    println!("queue_files {}", storage.queue_files);
    println!("queue_bytes {}", storage.queue_bytes);
    println!("queued_uploads {}", upload_stats.total);
    println!("queued_uploads_retrying {}", upload_stats.retrying);
    println!("queued_uploads_delayed {}", upload_stats.delayed);
    if let Some(next_retry_after) = upload_stats.next_retry_after {
        println!("queued_upload_next_retry_after {}", next_retry_after);
    } else {
        println!("queued_upload_next_retry_after (none)");
    }
    println!("queued_upload_attempts {}", upload_stats.attempts);
    println!("queued_upload_max_attempts {}", upload_stats.max_attempts);
    println!(
        "upload_queue_backpressure {}",
        upload_stats.has_backpressure()
    );
    println!("event_journal_records {}", event_count);
    println!("restore_queue_items {}", restore_queue_count);
    Ok(())
}

#[derive(Default)]
struct StatusDbStats {
    snapshots: i64,
    operations: i64,
    refs: i64,
    blobs: i64,
    blob_bytes: i64,
    large_objects: i64,
    large_object_bytes: i64,
    chunks: i64,
    chunk_bytes: i64,
    packs: i64,
    pack_bytes: i64,
    large_pins: i64,
    remote_refs: i64,
}

fn read_status_db_stats(conn: &Connection) -> Result<StatusDbStats> {
    Ok(StatusDbStats {
        snapshots: count_table(conn, "snapshots")?,
        operations: count_table(conn, "operations")?,
        refs: count_table(conn, "refs")?,
        blobs: count_table(conn, "blobs")?,
        blob_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from blobs")?,
        large_objects: count_table(conn, "large_objects")?,
        large_object_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from large_objects")?,
        chunks: count_table(conn, "chunks")?,
        chunk_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from chunks")?,
        packs: count_table(conn, "packs")?,
        pack_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from packs")?,
        large_pins: count_table(conn, "large_pins")?,
        remote_refs: count_table(conn, "remote_refs")?,
    })
}

fn count_table(conn: &Connection, table: &str) -> Result<i64> {
    let sql = format!("select count(*) from {table}");
    sum_i64(conn, &sql)
}

fn sum_i64(conn: &Connection, sql: &str) -> Result<i64> {
    conn.query_row(sql, [], |row| row.get(0))
        .map_err(Into::into)
}

struct StorageStats {
    state_files: u64,
    state_bytes: u64,
    objects_files: u64,
    objects_bytes: u64,
    logs_files: u64,
    logs_bytes: u64,
    queue_files: u64,
    queue_bytes: u64,
}

fn read_storage_stats(paths: &Paths) -> Result<StorageStats> {
    let state = dir_stats(&paths.home)?;
    let objects = dir_stats(&paths.home.join("objects"))?;
    let logs = dir_stats(&paths.logs)?;
    let queue = dir_stats(&paths.home.join("queue"))?;
    Ok(StorageStats {
        state_files: state.files,
        state_bytes: state.bytes,
        objects_files: objects.files,
        objects_bytes: objects.bytes,
        logs_files: logs.files,
        logs_bytes: logs.bytes,
        queue_files: queue.files,
        queue_bytes: queue.bytes,
    })
}

#[derive(Default)]
struct DirStats {
    files: u64,
    bytes: u64,
}

fn dir_stats(path: &Path) -> Result<DirStats> {
    if !path.exists() {
        return Ok(DirStats::default());
    }
    let mut stats = DirStats::default();
    for entry in WalkDir::new(path).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_file() {
            stats.files += 1;
            stats.bytes += entry.metadata()?.len();
        }
    }
    Ok(stats)
}

fn count_json_files(path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(|ext| ext.to_str()) == Some("json")
        {
            count += 1;
        }
    }
    Ok(count)
}

fn print_remote_status(config: &Config) -> Result<()> {
    let Some(remote_config) = &config.remote else {
        println!("remote_configured false");
        println!("remote_backend none");
        return Ok(());
    };
    let remote_url = remote_config.url().context("resolve remote URL")?;
    let backend = if remote_url.starts_with("file://") {
        "file"
    } else if remote_url.starts_with("s3://") {
        "s3"
    } else {
        "unknown"
    };
    println!("remote_configured true");
    println!("remote_backend {backend}");
    println!("remote_url {remote_url}");
    match open_remote(remote_config) {
        Ok(remote) => {
            let capabilities = remote.capabilities();
            println!("remote_resolved {}", remote.describe());
            println!("remote_lifecycle_rules {}", capabilities.lifecycle_rules);
            println!("remote_object_tags {}", capabilities.object_tags);
            println!(
                "remote_storage_class_on_put {}",
                capabilities.storage_class_on_put
            );
            println!(
                "remote_restore_archived_object {}",
                capabilities.restore_archived_object
            );
            println!("remote_multipart_upload {}", capabilities.multipart_upload);
            println!("remote_range_get {}", capabilities.range_get);
            println!("remote_conditional_put {}", capabilities.conditional_put);
        }
        Err(err) => {
            println!("remote_open_error {err:#}");
        }
    }
    Ok(())
}

pub(crate) fn log_cmd(paths: &Paths, args: LogArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    print_op_log(&conn, &args)
}

fn print_op_log(conn: &Connection, args: &LogArgs) -> Result<()> {
    let mut stmt = conn.prepare(
        "select id, kind, before_snapshot, after_snapshot, created_at, message, status, remote_sync_state
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
            row.get::<_, String>(6)?,
            row.get::<_, Option<String>>(7)?,
        ))
    })?;
    let mut printed = 0usize;
    for row in rows {
        let (id, kind, before, after, created, message, status, remote_sync_state) = row?;
        if let Some(root) = &args.root {
            let matches_root = message.as_deref() == Some(root)
                || before
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(conn, snapshot, root).ok())
                    .unwrap_or(false)
                || after
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(conn, snapshot, root).ok())
                    .unwrap_or(false);
            if !matches_root {
                continue;
            }
        }
        if printed >= args.limit {
            break;
        }
        println!(
            "{id}\t{created}\t{kind}\t{status}\t{}\t{} -> {}\t{}",
            remote_sync_state.unwrap_or_else(|| "-".into()),
            before.unwrap_or_default(),
            after.unwrap_or_default(),
            message.unwrap_or_default()
        );
        printed += 1;
    }
    Ok(())
}

pub(crate) fn op_cmd(paths: &Paths, command: OpCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
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
            println!("error {}", op.error.unwrap_or_default());
            println!(
                "remote_sync_state {}",
                op.remote_sync_state.unwrap_or_default()
            );
            Ok(())
        }
        OpCommand::Restore { op_id } => {
            let op = query_operation(&conn, &op_id)?;
            let before = current_snapshot(&conn)?;
            let snapshot = op
                .before_snapshot
                .or(op.after_snapshot)
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

pub(crate) fn diff_cmd(paths: &Paths, args: DiffArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
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
