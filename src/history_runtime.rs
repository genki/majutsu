use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, params};
use std::fs;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::Path;
#[cfg(unix)]
use std::{io, mem};
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
    let current_label = current.as_deref().unwrap_or("(none)");
    let remote = read_remote_status(&config)?;
    let db_stats = read_status_db_stats(&conn)?;
    let storage = read_storage_stats(paths)?;
    let upload_stats = upload_queue_stats(paths)?;
    let event_count = event_journal_records(paths)?.len();
    let restore_queue_count = count_json_files(&paths.home.join("queue/restores"))?;
    let width = terminal_width();

    println!("Status");
    print_kv(width, "Current snapshot", current_label);
    print_kv(width, "Roots", &roots.len().to_string());
    print_kv(width, "Remote", remote.summary());
    print_kv(width, "Queued uploads", &upload_stats.total.to_string());
    print_kv(width, "State usage", &format_bytes(storage.state_bytes));
    println!();

    println!("Host");
    print_kv(width, "Name", &config.host.name);
    print_kv(width, "ID", &config.host.id);
    print_kv(width, "Home", &paths.home.display().to_string());
    print_kv(width, "Config", &paths.config.display().to_string());
    print_kv(width, "Database", &paths.db.display().to_string());
    println!();

    print_remote_section(width, &remote);
    println!();

    println!("Configuration");
    print_table(
        width,
        &["AREA", "SETTING", "VALUE"],
        &[
            [
                "security",
                "encryption",
                config.security.encryption.as_str(),
            ],
            ["security", "hash", config.security.hash.as_str()],
            ["watch", "backend", config.watch.backend.as_str()],
            ["watch", "mode", config.watch.mode.as_str()],
            [
                "watch",
                "debounce",
                &format!("{} ms", config.watch.debounce),
            ],
            ["watch", "settle", &format!("{} ms", config.watch.settle)],
            ["large", "enabled", &config.large.enabled.to_string()],
            ["large", "min-size", &format_bytes(config.large.min_size)],
            [
                "large",
                "binary-min-size",
                &format_bytes(config.large.binary_min_size),
            ],
            ["large", "chunking", config.large.default_chunking.as_str()],
            [
                "large",
                "chunk-size",
                &format_bytes(config.large.chunk_size as u64),
            ],
            ["large", "multipart", &config.large.multipart.to_string()],
            [
                "pack",
                "small-target",
                &format_bytes(config.pack.small_pack_target),
            ],
            [
                "pack",
                "normal-target",
                &format_bytes(config.pack.normal_pack_target),
            ],
        ],
    );
    println!();

    println!("Roots");
    if roots.is_empty() {
        println!("  (none)");
    } else {
        let (id_width, status_width) = root_table_widths(width);
        println!(
            "  {:<id_width$} {:<status_width$} PATH",
            "ID",
            "STATUS",
            id_width = id_width,
            status_width = status_width
        );
        println!(
            "  {:<id_width$} {:<status_width$} {}",
            "-".repeat(id_width),
            "-".repeat(status_width),
            "-".repeat(4),
            id_width = id_width,
            status_width = status_width
        );
    }
    for root in &roots {
        let state = if root.status == "active" && !root.path.exists() {
            "missing"
        } else {
            root.status.as_str()
        };
        print_root_row(width, &root.id, state, &root.path.display().to_string());
    }
    println!();

    println!("Metadata");
    print_table(
        width,
        &["ITEM", "COUNT", "LOGICAL SIZE"],
        &[
            ["snapshots", &db_stats.snapshots.to_string(), "-"],
            ["operations", &db_stats.operations.to_string(), "-"],
            ["refs", &db_stats.refs.to_string(), "-"],
            [
                "blobs",
                &db_stats.blobs.to_string(),
                &format_bytes(db_stats.blob_bytes as u64),
            ],
            [
                "large objects",
                &db_stats.large_objects.to_string(),
                &format_bytes(db_stats.large_object_bytes as u64),
            ],
            [
                "chunks",
                &db_stats.chunks.to_string(),
                &format_bytes(db_stats.chunk_bytes as u64),
            ],
            [
                "packs",
                &db_stats.packs.to_string(),
                &format_bytes(db_stats.pack_bytes as u64),
            ],
            ["large pins", &db_stats.large_pins.to_string(), "-"],
            ["remote refs", &db_stats.remote_refs.to_string(), "-"],
        ],
    );
    println!();

    println!("Storage");
    print_table(
        width,
        &["SCOPE", "FILES", "SIZE"],
        &[
            [
                "state",
                &storage.state_files.to_string(),
                &format_bytes(storage.state_bytes),
            ],
            [
                "objects",
                &storage.objects_files.to_string(),
                &format_bytes(storage.objects_bytes),
            ],
            [
                "logs",
                &storage.logs_files.to_string(),
                &format_bytes(storage.logs_bytes),
            ],
            [
                "queue",
                &storage.queue_files.to_string(),
                &format_bytes(storage.queue_bytes),
            ],
        ],
    );
    println!();

    println!("Queues");
    print_table(
        width,
        &["QUEUE", "ITEMS", "DETAILS"],
        &[
            [
                "uploads",
                &upload_stats.total.to_string(),
                &format!(
                    "retrying={}, delayed={}, attempts={}, max_attempts={}, next_retry={}, backpressure={}",
                    upload_stats.retrying,
                    upload_stats.delayed,
                    upload_stats.attempts,
                    upload_stats.max_attempts,
                    upload_stats
                        .next_retry_after
                        .map(|ts| ts.to_string())
                        .unwrap_or_else(|| "(none)".into()),
                    upload_stats.has_backpressure()
                ),
            ],
            [
                "event journal",
                &event_count.to_string(),
                "pending local observations",
            ],
            [
                "restore jobs",
                &restore_queue_count.to_string(),
                "prepared restore jobs",
            ],
        ],
    );
    println!();

    println!("Machine");
    println!("current {current_label}");
    Ok(())
}

fn print_kv(width: usize, key: &str, value: &str) {
    let prefix = format!("  {key:<18} ");
    print_wrapped(&prefix, value, width);
}

fn print_table<const N: usize>(width: usize, headers: &[&str; N], rows: &[[&str; N]]) {
    let mut widths = [0usize; N];
    for (i, column_width) in widths.iter_mut().enumerate() {
        *column_width = headers[i].len();
    }
    for row in rows {
        for (i, column_width) in widths.iter_mut().enumerate() {
            *column_width = (*column_width).max(row[i].len());
        }
    }
    if N > 1 {
        let fixed_width: usize = widths[..N - 1].iter().sum::<usize>() + ((N - 1) * 2) + 2;
        let max_last = width
            .saturating_sub(fixed_width)
            .max(headers[N - 1].len())
            .max(12);
        widths[N - 1] = widths[N - 1].min(max_last);
    }
    print!("  ");
    for (i, column_width) in widths.iter().enumerate() {
        if i > 0 {
            print!("  ");
        }
        print!("{:<width$}", headers[i], width = *column_width);
    }
    println!();
    print!("  ");
    for (i, column_width) in widths.iter().enumerate() {
        if i > 0 {
            print!("  ");
        }
        print!(
            "{:<width$}",
            "-".repeat(*column_width),
            width = *column_width
        );
    }
    println!();
    for row in rows {
        print_table_row(row, &widths, width);
    }
}

fn print_table_row<const N: usize>(row: &[&str; N], widths: &[usize; N], terminal_width: usize) {
    let mut line_prefix = String::from("  ");
    for i in 0..N.saturating_sub(1) {
        if i > 0 {
            line_prefix.push_str("  ");
        }
        line_prefix.push_str(&format!("{:<width$}", row[i], width = widths[i]));
    }
    if N > 1 {
        line_prefix.push_str("  ");
        print_wrapped(&line_prefix, row[N - 1], terminal_width);
    } else if let Some(value) = row.first() {
        print_wrapped(&line_prefix, value, terminal_width);
    }
}

fn print_root_row(width: usize, id: &str, state: &str, path: &str) {
    let (id_width, status_width) = root_table_widths(width);
    let prefix = format!(
        "  {id:<id_width$} {state:<status_width$} ",
        id_width = id_width,
        status_width = status_width
    );
    print_wrapped(&prefix, path, width);
}

fn root_table_widths(width: usize) -> (usize, usize) {
    if width < 60 {
        (18, 10)
    } else if width < 88 {
        (24, 18)
    } else {
        (32, 18)
    }
}

fn print_wrapped(prefix: &str, value: &str, width: usize) {
    let available = width.saturating_sub(prefix.len()).max(16);
    let lines = wrap_text(value, available);
    if let Some((first, rest)) = lines.split_first() {
        println!("{prefix}{first}");
        let continuation = " ".repeat(prefix.len());
        for line in rest {
            println!("{continuation}{line}");
        }
    } else {
        println!("{prefix}");
    }
}

fn wrap_text(value: &str, width: usize) -> Vec<String> {
    if value.len() <= width {
        return vec![value.to_string()];
    }
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in value.split_whitespace() {
        if line.is_empty() {
            line.push_str(word);
        } else if line.len() + 1 + word.len() <= width {
            line.push(' ');
            line.push_str(word);
        } else {
            lines.push(line);
            line = word.to_string();
        }
        while line.len() > width {
            let rest = line.split_off(width);
            lines.push(line);
            line = rest;
        }
    }
    if !line.is_empty() {
        lines.push(line);
    }
    if lines.is_empty() {
        vec![String::new()]
    } else {
        lines
    }
}

fn terminal_width() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|width| *width >= 40)
        .or_else(detect_terminal_width)
        .unwrap_or(100)
}

#[cfg(unix)]
fn detect_terminal_width() -> Option<usize> {
    #[repr(C)]
    struct Winsize {
        ws_row: libc::c_ushort,
        ws_col: libc::c_ushort,
        ws_xpixel: libc::c_ushort,
        ws_ypixel: libc::c_ushort,
    }

    let mut winsize: Winsize = unsafe { mem::zeroed() };
    let result = unsafe {
        libc::ioctl(
            io::stdout().as_raw_fd(),
            libc::TIOCGWINSZ,
            &mut winsize as *mut Winsize,
        )
    };
    if result == 0 && winsize.ws_col >= 40 {
        Some(winsize.ws_col as usize)
    } else {
        None
    }
}

#[cfg(not(unix))]
fn detect_terminal_width() -> Option<usize> {
    None
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {} ({bytes} B)", UNITS[unit])
    }
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

struct RemoteStatus {
    configured: bool,
    backend: String,
    url: Option<String>,
    resolved: Option<String>,
    open_error: Option<String>,
    lifecycle_rules: Option<bool>,
    object_tags: Option<bool>,
    storage_class_on_put: Option<bool>,
    restore_archived_object: Option<bool>,
    multipart_upload: Option<bool>,
    range_get: Option<bool>,
    conditional_put: Option<bool>,
}

impl RemoteStatus {
    fn summary(&self) -> &str {
        if !self.configured {
            return "not configured";
        }
        if self.open_error.is_some() {
            return "configured, unavailable";
        }
        "configured"
    }
}

fn read_remote_status(config: &Config) -> Result<RemoteStatus> {
    let Some(remote_config) = &config.remote else {
        return Ok(RemoteStatus {
            configured: false,
            backend: "none".into(),
            url: None,
            resolved: None,
            open_error: None,
            lifecycle_rules: None,
            object_tags: None,
            storage_class_on_put: None,
            restore_archived_object: None,
            multipart_upload: None,
            range_get: None,
            conditional_put: None,
        });
    };
    let remote_url = remote_config.url().context("resolve remote URL")?;
    let backend = if remote_url.starts_with("file://") {
        "file"
    } else if remote_url.starts_with("s3://") {
        "s3"
    } else {
        "unknown"
    };
    match open_remote(remote_config) {
        Ok(remote) => {
            let capabilities = remote.capabilities();
            Ok(RemoteStatus {
                configured: true,
                backend: backend.into(),
                url: Some(remote_url),
                resolved: Some(remote.describe()),
                open_error: None,
                lifecycle_rules: Some(capabilities.lifecycle_rules),
                object_tags: Some(capabilities.object_tags),
                storage_class_on_put: Some(capabilities.storage_class_on_put),
                restore_archived_object: Some(capabilities.restore_archived_object),
                multipart_upload: Some(capabilities.multipart_upload),
                range_get: Some(capabilities.range_get),
                conditional_put: Some(capabilities.conditional_put),
            })
        }
        Err(err) => Ok(RemoteStatus {
            configured: true,
            backend: backend.into(),
            url: Some(remote_url),
            resolved: None,
            open_error: Some(format!("{err:#}")),
            lifecycle_rules: None,
            object_tags: None,
            storage_class_on_put: None,
            restore_archived_object: None,
            multipart_upload: None,
            range_get: None,
            conditional_put: None,
        }),
    }
}

fn print_remote_section(width: usize, remote: &RemoteStatus) {
    println!("Remote");
    print_kv(width, "Configured", &remote.configured.to_string());
    print_kv(width, "Backend", &remote.backend);
    if let Some(url) = &remote.url {
        print_kv(width, "URL", url);
    }
    if let Some(resolved) = &remote.resolved {
        print_kv(width, "Resolved", resolved);
    }
    if let Some(error) = &remote.open_error {
        print_kv(width, "Open error", error);
    }
    if remote.lifecycle_rules.is_some() {
        print_table(
            width,
            &["CAPABILITY", "SUPPORTED"],
            &[
                [
                    "lifecycle rules",
                    &display_option_bool(remote.lifecycle_rules),
                ],
                ["object tags", &display_option_bool(remote.object_tags)],
                [
                    "storage class on put",
                    &display_option_bool(remote.storage_class_on_put),
                ],
                [
                    "restore archived object",
                    &display_option_bool(remote.restore_archived_object),
                ],
                [
                    "multipart upload",
                    &display_option_bool(remote.multipart_upload),
                ],
                ["range get", &display_option_bool(remote.range_get)],
                [
                    "conditional put",
                    &display_option_bool(remote.conditional_put),
                ],
            ],
        );
    }
}

fn display_option_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".into())
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
