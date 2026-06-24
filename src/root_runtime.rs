use crate::majutsu_core::{
    FileRecord, LargeManifest, Payload, SnapshotManifest, TreeManifest, TreeNodeManifest,
    payload_blob_ref, payload_large_ref,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration as StdDuration;

use crate::cli::{PathTrackArgs, RootCommand, RootListArgs, RootSizeArgs};
use crate::config::{
    Paths, RootConfig, default_include, read_config, validate_large_chunking,
    validate_snapshot_mode,
};
use crate::daemon_runtime::ensure_daemon_running;
use crate::operation_log::record_op;
use crate::remote_store::{RemoteObjectStat, RemoteStore, open_remote};
use crate::root_size_summary::{
    RootSizeSummary, RootSizeSummaryRow, RootSizeSummaryTotals, write_cached_root_size_summary,
};
use crate::root_state::{
    all_tracked_paths, mark_path_tracked, mark_path_untracked, root_by_id, root_by_id_optional,
    roots, save_root, sync_roots_to_config, update_root_status,
};
use crate::snapshot_rules::{
    apply_default_root_excludes, apply_root_large_set, apply_root_presets, apply_root_volatile_set,
    build_ignore, dedup_patterns, root_large_override, root_record_is_managed,
    root_volatile_override, warn_sensitive_root_defaults,
};
use crate::util::{REMOTE_METADATA_DECODE_LIMIT, zstd_decode_all_limited};

pub(crate) fn root_cmd(paths: &Paths, command: RootCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    match command {
        RootCommand::Add(args) => {
            let path = crate::absolutize(&args.path)?;
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
                .map(crate::absolutize)
                .transpose()?;
            if snapshot_source.is_some() && args.snapshot_mode != "transactional" {
                bail!("--snapshot-source requires --snapshot-mode transactional");
            }
            let mut exclude = Vec::new();
            if !args.no_default_excludes {
                apply_default_root_excludes(&mut exclude);
            }
            exclude.extend(args.exclude.clone());
            apply_root_presets(&mut exclude, &args.presets)?;
            warn_sensitive_root_defaults(&path, &exclude);
            let large = root_large_override(&args);
            let volatile = root_volatile_override(&args)?;
            let root = RootConfig {
                name: args.name.unwrap_or_else(|| args.id.clone()),
                id: args.id,
                path,
                include: if args.include.is_empty() {
                    default_include()
                } else {
                    args.include
                },
                exclude,
                explicit_track: Vec::new(),
                explicit_untrack: Vec::new(),
                follow_symlinks: args.follow_symlinks,
                require_mount: args.require_mount,
                status: "active".into(),
                degraded: None,
                snapshot_mode: args.snapshot_mode,
                pre_snapshot: args.pre_snapshot,
                post_snapshot: args.post_snapshot,
                snapshot_source,
                application_plugin: args.application_plugin,
                large,
                volatile,
            };
            conn.execute(
                "insert into roots(id, data_json) values (?1, ?2)",
                params![root.id, serde_json::to_string(&root)?],
            )?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-added", None, None, Some(&root.id))?;
            println!("added root {} -> {}", root.id, root.path.display());
            match ensure_daemon_running(paths) {
                Ok(Some(pid)) => println!("started daemon pid {pid}"),
                Ok(None) => {}
                Err(err) => eprintln!("warning: daemon auto-start failed: {err:#}"),
            }
        }
        RootCommand::Set(args) => {
            let mut root = root_by_id(&conn, &args.id)?;
            if let Some(path) = &args.path {
                let path = crate::absolutize(path)?;
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
            let mut exclude_additions = args.exclude.clone();
            apply_root_presets(&mut exclude_additions, &args.presets)?;
            root.exclude.extend(exclude_additions);
            dedup_patterns(&mut root.exclude);
            warn_sensitive_root_defaults(&root.path, &root.exclude);
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
                validate_snapshot_mode(mode)?;
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
                root.snapshot_source = Some(crate::absolutize(snapshot_source)?);
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
            apply_root_volatile_set(&mut root, &args)?;
            let forgotten = if args.skip_history_rewrite {
                ForgetUnmanagedStats::default()
            } else {
                with_sqlite_storage_context(paths, "apply root config cleanup", || {
                    forget_unmanaged_root_history(paths, &conn, &root)
                })?
            };
            save_root(&conn, &root)?;
            sync_roots_to_config(paths, &conn)?;
            if args.skip_history_rewrite {
                println!(
                    "history_rewrite skipped; run `mj untrack -r {} --excluded` or `mj root set {} --exclude ...` without --skip-history-rewrite after repairing history",
                    root.id, root.id
                );
            } else if forgotten.records > 0 {
                let removed = with_sqlite_storage_context(
                    paths,
                    "prune root config cleanup metadata",
                    || crate::prune_runtime::prune_unreferenced_metadata(paths, &conn),
                )?;
                println!("forgotten_unmanaged_records {}", forgotten.records);
                println!("rewritten_snapshots {}", forgotten.snapshots);
                println!("removed_blob_metadata {}", removed.blobs);
                println!("removed_large_metadata {}", removed.large_objects);
                println!("removed_chunk_metadata {}", removed.chunks);
                println!("removed_pack_metadata {}", removed.packs);
            }
            record_op(&conn, "config-change", None, None, Some(&root.id))?;
            println!("updated root {} -> {}", root.id, root.path.display());
        }
        RootCommand::List(args) => {
            root_list_cmd(&conn, &args)?;
        }
        RootCommand::Size(args) => {
            root_size_cmd(paths, &conn, &args)?;
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

pub(crate) fn track_cmd(paths: &Paths, args: PathTrackArgs) -> Result<()> {
    update_explicit_tracking(paths, args, true)
}

pub(crate) fn untrack_cmd(paths: &Paths, args: PathTrackArgs) -> Result<()> {
    update_explicit_tracking(paths, args, false)
}

fn update_explicit_tracking(paths: &Paths, args: PathTrackArgs, track: bool) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    let configured_roots = roots(&conn)?;
    if track && (args.excluded || args.dry_run || args.summary || args.quiet || args.json) {
        bail!("track does not support --excluded, --dry-run, --summary, --quiet, or --json");
    }
    if args.excluded && args.root.is_none() {
        bail!("--excluded requires -r/--root");
    }
    let input_paths = collect_path_tracking_inputs(&args, &configured_roots, &conn)?;
    if input_paths.is_empty() {
        bail!("no paths supplied");
    }
    let mut changed = BTreeMap::<String, RootConfig>::new();
    let mut summary = UntrackSummary {
        requested: input_paths.len(),
        ..Default::default()
    };
    let mut failures = Vec::new();
    for input in &input_paths {
        let resolved = resolve_path_tracking_target(&configured_roots, args.root.as_deref(), input);
        let (root, rel) = match resolved {
            Ok(value) => value,
            Err(err) => {
                summary.failed += 1;
                failures.push(format!("{}: {err:#}", input.display()));
                continue;
            }
        };
        let root = changed
            .entry(root.id.clone())
            .or_insert_with(|| root.clone());
        if track {
            remove_pattern(&mut root.explicit_untrack, &rel);
            push_pattern(&mut root.explicit_track, rel.clone());
            mark_path_tracked(&conn, &root.id, &rel)?;
            println!("tracked {}:{}", root.id, rel);
        } else {
            if args.dry_run {
                summary.would_untrack += 1;
                if !args.quiet && !args.summary && !args.json {
                    println!("would untrack {}:{}", root.id, rel);
                }
                continue;
            }
            remove_pattern(&mut root.explicit_track, &rel);
            push_pattern(&mut root.explicit_untrack, rel.clone());
            mark_path_untracked(&conn, &root.id, &rel)?;
            summary.untracked += 1;
            if !args.quiet && !args.summary && !args.json {
                println!("untracked {}:{}", root.id, rel);
            }
        }
    }
    if !failures.is_empty() && !args.json {
        for failure in &failures {
            eprintln!("untrack error: {failure}");
        }
    }
    for root in changed.values() {
        if !track {
            if args.dry_run {
                continue;
            }
            let forgotten = match with_sqlite_storage_context(
                paths,
                "forget unmanaged root history",
                || forget_unmanaged_root_history(paths, &conn, root),
            ) {
                Ok(stats) => stats,
                Err(err) if args.continue_on_history_error => {
                    eprintln!(
                        "warning: tracking metadata updated but historical cleanup failed: {err:#}"
                    );
                    eprintln!(
                        "next: run `mj fsck`; then rerun `mj untrack -r {} --excluded`",
                        root.id
                    );
                    ForgetUnmanagedStats::default()
                }
                Err(err) => return Err(err),
            };
            if forgotten.records > 0 {
                let removed =
                    with_sqlite_storage_context(paths, "prune unreferenced metadata", || {
                        crate::prune_runtime::prune_unreferenced_metadata(paths, &conn)
                    })?;
                if !args.quiet && !args.json {
                    println!("forgotten_unmanaged_records {}", forgotten.records);
                    println!("rewritten_snapshots {}", forgotten.snapshots);
                    println!("removed_blob_metadata {}", removed.blobs);
                    println!("removed_large_metadata {}", removed.large_objects);
                    println!("removed_chunk_metadata {}", removed.chunks);
                    println!("removed_pack_metadata {}", removed.packs);
                }
            }
        }
        if !args.dry_run {
            save_root(&conn, root)?;
        }
    }
    if !args.dry_run {
        sync_roots_to_config(paths, &conn)?;
        record_op(
            &conn,
            if track {
                "path-tracked"
            } else {
                "path-untracked"
            },
            None,
            None,
            args.root.as_deref(),
        )?;
    }
    if !track && (args.summary || args.quiet || args.json || args.dry_run) {
        if args.json {
            println!("{}", serde_json::to_string_pretty(&summary)?);
        } else if !args.quiet || args.summary {
            println!("requested {}", summary.requested);
            if args.dry_run {
                println!("would_untrack {}", summary.would_untrack);
            } else {
                println!("untracked {}", summary.untracked);
            }
            println!("failed {}", summary.failed);
            if !args.dry_run && summary.untracked > 0 {
                println!("hint run `mj snapshot` and `mj sync --wait` to publish cleanup metadata");
            }
        }
    }
    if summary.failed > 0 {
        bail!("{} path operation(s) failed", summary.failed);
    }
    Ok(())
}

#[derive(Default, Serialize)]
struct UntrackSummary {
    requested: usize,
    untracked: usize,
    would_untrack: usize,
    failed: usize,
}

fn collect_path_tracking_inputs(
    args: &PathTrackArgs,
    roots: &[RootConfig],
    conn: &Connection,
) -> Result<Vec<PathBuf>> {
    let mut inputs = args.paths.clone();
    if let Some(path_file) = &args.path_file {
        let content = fs::read_to_string(path_file)
            .with_context(|| format!("read path file {}", path_file.display()))?;
        inputs.extend(lines_to_paths(&content));
    }
    if args.stdin {
        let mut content = String::new();
        io::stdin().read_to_string(&mut content)?;
        inputs.extend(lines_to_paths(&content));
    }
    if args.excluded {
        let root_id = args.root.as_deref().expect("--excluded requires root");
        let root = roots
            .iter()
            .find(|root| root.id == root_id && root.status == "active")
            .ok_or_else(|| anyhow!("unknown active root: {root_id}"))?;
        let ignore = build_ignore(root)?;
        for rel in all_tracked_paths(conn, &root.id)? {
            let rel_path = PathBuf::from(&rel);
            let is_dir = root.path.join(&rel).is_dir();
            if !root_record_is_managed(root, &ignore, &rel_path, is_dir) {
                inputs.push(rel_path);
            }
        }
    }
    inputs.sort();
    inputs.dedup();
    Ok(inputs)
}

fn lines_to_paths(content: &str) -> Vec<PathBuf> {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(PathBuf::from)
        .collect()
}

fn with_sqlite_storage_context<T, F>(paths: &Paths, operation: &str, action: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    match action() {
        Ok(value) => Ok(value),
        Err(err) if looks_like_sqlite_full(&err) => {
            Err(err).with_context(|| sqlite_storage_diagnostics(paths, operation))
        }
        Err(err) => Err(err),
    }
}

fn looks_like_sqlite_full(err: &anyhow::Error) -> bool {
    let message = format!("{err:#}").to_ascii_lowercase();
    message.contains("database or disk is full")
        || message.contains("sqlite_full")
        || message.contains("error code 13")
}

fn sqlite_storage_diagnostics(paths: &Paths, operation: &str) -> String {
    let temp_dir = env::var_os("SQLITE_TMPDIR")
        .or_else(|| env::var_os("TMPDIR"))
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);
    let db_stats = sqlite_page_stats(paths).unwrap_or_else(|err| format!("unavailable: {err:#}"));
    format!(
        "SQLite storage diagnostic while {operation}\n\
         state_home: {}\n\
         state_db: {}\n\
         sqlite_temp_dir: {}\n\
         state_fs: {}\n\
         temp_fs: {}\n\
         sqlite_pages: {}\n\
         transaction: failed operation was rolled back by SQLite/command error handling\n\
         next: run `mj fsck`; if remote is configured, run `mj sync status --deep` and `mj remote fsck --objects`",
        paths.home.display(),
        paths.db.display(),
        temp_dir.display(),
        fs_capacity_summary(&paths.home),
        fs_capacity_summary(&temp_dir),
        db_stats
    )
}

fn sqlite_page_stats(paths: &Paths) -> Result<String> {
    let conn = Connection::open(&paths.db)?;
    let page_count: i64 = conn.pragma_query_value(None, "page_count", |row| row.get(0))?;
    let max_page_count: i64 = conn.pragma_query_value(None, "max_page_count", |row| row.get(0))?;
    let page_size: i64 = conn.pragma_query_value(None, "page_size", |row| row.get(0))?;
    Ok(format!(
        "page_count={page_count} max_page_count={max_page_count} page_size={page_size}"
    ))
}

#[cfg(unix)]
fn fs_capacity_summary(path: &Path) -> String {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let existing = nearest_existing_path(path);
    let Ok(c_path) = CString::new(existing.as_os_str().as_bytes()) else {
        return format!("{} unavailable: interior nul byte", existing.display());
    };
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: c_path is a valid nul-terminated path and stats points to writable memory.
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return format!(
            "{} unavailable: {}",
            existing.display(),
            io::Error::last_os_error()
        );
    }
    // SAFETY: statvfs returned success and initialized stats.
    let stats = unsafe { stats.assume_init() };
    let free_bytes = stats.f_bavail as u128 * stats.f_frsize as u128;
    format!(
        "{} free_bytes={} free_inodes={}",
        existing.display(),
        free_bytes,
        stats.f_favail
    )
}

#[cfg(not(unix))]
fn fs_capacity_summary(path: &Path) -> String {
    format!(
        "{} capacity details unavailable on this platform",
        nearest_existing_path(path).display()
    )
}

fn nearest_existing_path(path: &Path) -> PathBuf {
    let mut current = path;
    loop {
        if current.exists() {
            return current.to_path_buf();
        }
        let Some(parent) = current.parent() else {
            return PathBuf::from(".");
        };
        current = parent;
    }
}

fn resolve_path_tracking_target<'a>(
    roots: &'a [RootConfig],
    root_id: Option<&str>,
    input: &Path,
) -> Result<(&'a RootConfig, String)> {
    let candidates = roots
        .iter()
        .filter(|root| root.status == "active")
        .filter(|root| root_id.is_none_or(|id| id == root.id))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        bail!(
            "no active root matches {}",
            root_id.unwrap_or("(current path)")
        );
    }
    if let Some(id) = root_id {
        let root = candidates
            .into_iter()
            .find(|root| root.id == id)
            .ok_or_else(|| anyhow!("unknown root: {id}"))?;
        let rel = if input.is_absolute() {
            input
                .strip_prefix(&root.path)
                .with_context(|| {
                    format!(
                        "path {} is not inside root {} ({})",
                        input.display(),
                        root.id,
                        root.path.display()
                    )
                })?
                .to_path_buf()
        } else {
            input.to_path_buf()
        };
        return Ok((root, normalize_tracking_path(&rel)?));
    }
    let abs = crate::absolutize(input)?;
    let mut matches = candidates
        .into_iter()
        .filter_map(|root| {
            abs.strip_prefix(&root.path)
                .ok()
                .map(|rel| (root, rel.to_path_buf()))
        })
        .collect::<Vec<_>>();
    matches.sort_by_key(|(root, _)| std::cmp::Reverse(root.path.components().count()));
    let Some((root, rel)) = matches.into_iter().next() else {
        bail!(
            "path {} is not inside a configured root; use -r/--root for root-relative paths",
            input.display()
        );
    };
    Ok((root, normalize_tracking_path(&rel)?))
}

fn normalize_tracking_path(path: &Path) -> Result<String> {
    if path.as_os_str().is_empty() {
        bail!("cannot track or untrack a root directory itself");
    }
    let rel = crate::util::path_to_slash(path);
    let rel = rel.trim_matches('/').to_string();
    if rel.is_empty() || rel == "." || rel.split('/').any(|part| part == "..") {
        bail!("invalid root-relative path: {}", path.display());
    }
    Ok(rel)
}

fn push_pattern(patterns: &mut Vec<String>, path: String) {
    if !patterns.iter().any(|existing| existing == &path) {
        patterns.push(path);
    }
    dedup_patterns(patterns);
}

fn remove_pattern(patterns: &mut Vec<String>, path: &str) {
    patterns.retain(|existing| existing != path);
}

#[derive(Default)]
struct ForgetUnmanagedStats {
    snapshots: usize,
    records: usize,
}

fn forget_unmanaged_root_history(
    paths: &Paths,
    conn: &Connection,
    root: &RootConfig,
) -> Result<ForgetUnmanagedStats> {
    let ignore = build_ignore(root)?;
    let config = read_config(paths)?;
    let remote = config.remote.as_ref().map(open_remote).transpose()?;
    let mut stmt =
        conn.prepare("select id, manifest_key, manifest_json from snapshots order by created_at")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut stats = ForgetUnmanagedStats::default();
    for row in rows {
        let (snapshot_id, manifest_key, manifest_json) = row?;
        let mut manifest: SnapshotManifest = if manifest_json.trim().is_empty() {
            read_metadata_manifest(paths, remote.as_ref(), &manifest_key)
                .with_context(|| format!("read snapshot manifest {manifest_key}"))?
        } else {
            serde_json::from_str(&manifest_json)
                .with_context(|| format!("decode snapshot manifest {snapshot_id}"))?
        };
        let Some(root_snapshot) = manifest.root_trees.get(&root.id).cloned() else {
            continue;
        };
        let tree: TreeManifest =
            read_metadata_manifest(paths, remote.as_ref(), &root_snapshot.tree_key)
                .with_context(|| {
                    format!(
                        "read existing root tree {} for root {} while applying root set; run `mj fsck` and `mj remote fsck --objects`, then `mj remote repair` if referenced objects are missing",
                        root_snapshot.tree_key, root.id
                    )
                })?;
        let entries = root_size_tree_entries(paths, remote.as_ref(), tree).with_context(|| {
            format!(
                "expand existing root tree for root {} while applying root set; run `mj fsck` and `mj remote fsck --objects`, then `mj remote repair` if referenced objects are missing",
                root.id
            )
        })?;
        let before = entries.len();
        let kept = entries
            .into_values()
            .filter(|record| {
                root_record_is_managed(
                    root,
                    &ignore,
                    Path::new(&record.path),
                    record.kind == "directory",
                )
            })
            .collect::<Vec<_>>();
        let removed = before.saturating_sub(kept.len());
        if removed == 0 {
            continue;
        }
        stats.snapshots += 1;
        stats.records += removed;
        let mut tree = crate::build_tree_manifest(&root.id, kept.clone())?;
        let tree_entries = tree.entries.clone();
        let tree_file_count = tree_entries.len();
        crate::prepare_tree_manifest_for_storage(paths, &mut tree)?;
        let tree_json = serde_json::to_vec(&tree)?;
        let tree_oid = crate::util::blake3_hex(&tree_json);
        let tree_key = crate::store_bytes(paths, &paths.trees, &tree_oid, &tree_json)?;
        manifest.root_trees.insert(
            root.id.clone(),
            crate::majutsu_core::RootSnapshot {
                tree_id: tree.tree_id,
                tree_key,
                file_count: tree_file_count,
            },
        );
        manifest
            .roots
            .insert(root.id.clone(), tree_entries.into_values().collect());
        let manifest_json_bytes = serde_json::to_vec_pretty(&manifest)?;
        let manifest_oid = crate::util::blake3_hex(&manifest_json_bytes);
        let new_manifest_key = crate::store_encoded_object_bytes(
            paths,
            &paths.objects,
            &manifest_oid,
            &crate::encode_compact_snapshot_manifest_for_local(paths, &manifest)?,
        )?;
        conn.execute(
            "update snapshots set manifest_key=?2, manifest_json='' where id=?1",
            params![snapshot_id, new_manifest_key],
        )?;
        crate::insert_snapshot_payload_index(conn, &manifest)?;
    }
    Ok(stats)
}

#[derive(Serialize)]
struct RootListRow {
    id: String,
    status: String,
    name: String,
    path: String,
    include: Vec<String>,
    exclude: Vec<String>,
    volatile: Option<crate::config::RootVolatileConfig>,
}

#[derive(Serialize)]
struct RootListOutput {
    total: usize,
    active: usize,
    issues: usize,
    roots: Vec<RootListRow>,
}

fn root_list_cmd(conn: &Connection, args: &RootListArgs) -> Result<()> {
    let mut roots = roots(conn)?;
    roots.sort_by(|left, right| {
        root_status_rank(&left.status)
            .cmp(&root_status_rank(&right.status))
            .then_with(|| left.id.cmp(&right.id))
    });
    let total = roots.len();
    let active = roots.iter().filter(|root| root.status == "active").count();
    let problematic = roots.iter().filter(|root| root.status != "active").count();
    if args.json {
        let output = RootListOutput {
            total,
            active,
            issues: problematic,
            roots: roots
                .iter()
                .map(|root| RootListRow {
                    id: root.id.clone(),
                    status: root.status.clone(),
                    name: root.name.clone(),
                    path: root.path.display().to_string(),
                    include: root.include.clone(),
                    exclude: root.exclude.clone(),
                    volatile: root.volatile.clone(),
                })
                .collect(),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }

    let width = if args.no_truncate {
        usize::MAX
    } else {
        terminal_width()
    };
    let mut output = String::new();
    output.push_str("Roots\n");
    output.push_str(&format!(
        "  total {total}  active {active}  issues {problematic}\n\n"
    ));
    let rows = roots
        .iter()
        .map(|root| {
            [
                root.id.clone(),
                root.status.clone(),
                root.name.clone(),
                root.path.display().to_string(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&mut output, width, &["ID", "STATUS", "NAME", "PATH"], &rows);
    print!("{output}");
    Ok(())
}

fn root_status_rank(status: &str) -> u8 {
    match status {
        "active" => 1,
        "paused" => 2,
        "deleted" => 3,
        _ => 0,
    }
}

#[derive(Default)]
struct RootSizeStat {
    files: usize,
    dirs: usize,
    client_bytes: u64,
    payload_keys: BTreeSet<String>,
    metadata_keys: BTreeSet<String>,
    packed_payload_keys: BTreeSet<String>,
    packed_payload_oids: BTreeSet<String>,
    packed_slice_bytes: u64,
}

#[derive(Serialize)]
struct RootSizeRow {
    root: String,
    files: usize,
    dirs: usize,
    client_bytes: u64,
    used_bytes: u64,
    backend_bytes: u64,
    payload_bytes: u64,
    metadata_bytes: u64,
    backend_objects: usize,
    missing_objects: usize,
}

#[derive(Serialize)]
struct RootSizeReport<'a> {
    roots: &'a [RootSizeRow],
    totals: RootSizeTotals,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<&'a RootSizeHistoryReport>,
}

#[derive(Serialize)]
struct RootSizeHistoryReport {
    retained_bytes: u64,
    retained_payloads: usize,
    scanned_snapshots: usize,
    skipped_snapshots: usize,
    rows: Vec<RootSizeHistoryRow>,
    warnings: Vec<String>,
}

#[derive(Clone, Serialize)]
struct RootSizeHistoryRow {
    bytes: u64,
    kind: String,
    snapshots: usize,
    first_seen: String,
    last_seen: String,
    root: String,
    path: String,
    oid: String,
}

#[derive(Clone, Serialize)]
struct RootSizeTotals {
    billed_bytes: u64,
    billed_objects: usize,
    row_used_bytes: u64,
    unique_used_bytes: u64,
    current_backend_bytes: u64,
    payload_bytes: u64,
    metadata_bytes: u64,
    objects: usize,
    backend_prefix_bytes: u64,
    backend_prefix_objects: usize,
    backend_prefix_exact: bool,
    backend_prefix_scope: String,
}

#[derive(Serialize, Deserialize)]
struct RootSizeRemoteObjectCache {
    version: u32,
    remote: String,
    fetched_at: DateTime<Utc>,
    objects: Vec<RemoteObjectStat>,
}

struct RootSizeRemoteObjects {
    objects: Vec<RemoteObjectStat>,
    exact: bool,
    scope: String,
}

struct RootSizeTotalsInput<'a> {
    remote_objects: &'a RootSizeRemoteObjects,
    remote_size_map: &'a BTreeMap<String, u64>,
    unique_keys: &'a BTreeSet<String>,
    unique_payload_keys: &'a BTreeSet<String>,
    unique_metadata_keys: &'a BTreeSet<String>,
    unique_packed_payload_keys: &'a BTreeSet<String>,
    unique_packed_slice_bytes: u64,
    rows: &'a [RootSizeRow],
}

#[derive(Clone)]
struct PackedBlobSizeRef {
    pack_key: String,
    index_key: String,
    pack_len: u64,
}

fn root_size_cmd(paths: &Paths, conn: &Connection, args: &RootSizeArgs) -> Result<()> {
    let current: String = conn
        .query_row("select value from refs where name='current'", [], |row| {
            row.get(0)
        })
        .context("read current snapshot ref")?;
    let manifest_key: String = conn
        .query_row(
            "select manifest_key from snapshots where id=?1",
            params![current],
            |row| row.get(0),
        )
        .with_context(|| format!("read snapshot manifest key for {current}"))?;
    let snapshot_created_at: String = conn
        .query_row(
            "select created_at from snapshots where id=?1",
            params![current],
            |row| row.get(0),
        )
        .with_context(|| format!("read snapshot timestamp for {current}"))?;
    let config = read_config(paths)?;
    let remote = config.remote.as_ref().map(open_remote).transpose()?;
    let is_s3_remote = matches!(remote, Some(RemoteStore::S3(_)));
    let remote_sizes_task = remote.as_ref().map(|remote| {
        let paths = paths.clone();
        let remote = remote.clone();
        let use_cache = !args.no_remote_cache;
        thread::spawn(move || root_size_remote_objects(&paths, &remote, use_cache))
    });
    let packed_blobs = packed_blob_size_refs(conn)?;
    let manifest: SnapshotManifest = read_metadata_manifest(paths, remote.as_ref(), &manifest_key)
        .with_context(|| format!("read snapshot manifest {manifest_key}"))?;
    let mut stats = BTreeMap::<String, RootSizeStat>::new();
    for (root_id, root_snapshot) in &manifest.root_trees {
        let tree: TreeManifest =
            read_metadata_manifest(paths, remote.as_ref(), &root_snapshot.tree_key)
                .with_context(|| format!("read root tree {}", root_snapshot.tree_key))?;
        let stat = stats.entry(root_id.clone()).or_default();
        stat.metadata_keys.insert(root_snapshot.tree_key.clone());
        if let Some(root_node) = &tree.root_node {
            stat.metadata_keys.insert(root_node.node_key.clone());
        }
        for node in tree.subtree_nodes.values() {
            stat.metadata_keys.insert(node.node_key.clone());
        }
        let entries = root_size_tree_entries(paths, remote.as_ref(), tree)?;
        for record in entries.values() {
            match record.kind.as_str() {
                "directory" => stat.dirs += 1,
                _ => {
                    stat.files += 1;
                    stat.client_bytes = stat.client_bytes.saturating_add(record.size);
                }
            }
            add_payload_remote_keys(paths, remote.as_ref(), &packed_blobs, &record.payload, stat)?;
        }
    }

    let stream_pending = root_size_should_stream_pending(args, remote_sizes_task.as_ref());
    if stream_pending {
        print!("{}", root_size_pending_table(&stats));
        io::stdout().flush()?;
    }

    let remote_objects = match remote_sizes_task {
        Some(task) => task
            .join()
            .map_err(|err| anyhow!("root size remote listing worker panicked: {err:?}"))??,
        None => RootSizeRemoteObjects {
            objects: Vec::new(),
            exact: true,
            scope: "no-remote".into(),
        },
    };
    let remote_size_map = remote_objects
        .objects
        .iter()
        .map(|object| (object.key.clone(), object.size))
        .collect::<BTreeMap<_, _>>();

    let mut rows = Vec::new();
    let mut unique_keys = BTreeSet::new();
    let mut unique_payload_keys = BTreeSet::new();
    let mut unique_metadata_keys = BTreeSet::new();
    let mut unique_packed_payload_keys = BTreeSet::new();
    let mut unique_packed_payload_oids = BTreeSet::new();
    let mut unique_packed_slice_bytes = 0u64;
    for (root, stat) in stats {
        let payload_keys = resolve_remote_keys(&stat.payload_keys, &remote_size_map, is_s3_remote);
        let metadata_keys =
            resolve_remote_keys(&stat.metadata_keys, &remote_size_map, is_s3_remote);
        let all_keys = payload_keys
            .found
            .union(&metadata_keys.found)
            .cloned()
            .collect::<BTreeSet<_>>();
        unique_keys.extend(all_keys.iter().cloned());
        unique_payload_keys.extend(payload_keys.found.iter().cloned());
        unique_metadata_keys.extend(metadata_keys.found.iter().cloned());
        let payload_bytes = sum_remote_keys(&remote_size_map, &payload_keys.found);
        let metadata_bytes = sum_remote_keys(&remote_size_map, &metadata_keys.found);
        let packed_payload_keys =
            resolve_remote_keys(&stat.packed_payload_keys, &remote_size_map, is_s3_remote);
        unique_packed_payload_keys.extend(packed_payload_keys.found.iter().cloned());
        for oid in &stat.packed_payload_oids {
            if unique_packed_payload_oids.insert(oid.clone())
                && let Some(packed) = packed_blobs.get(oid)
            {
                unique_packed_slice_bytes =
                    unique_packed_slice_bytes.saturating_add(packed.pack_len);
            }
        }
        let packed_payload_bytes = sum_remote_keys(&remote_size_map, &packed_payload_keys.found);
        let backend_bytes = sum_remote_keys(&remote_size_map, &all_keys);
        let used_bytes = backend_bytes
            .saturating_sub(packed_payload_bytes)
            .saturating_add(stat.packed_slice_bytes);
        rows.push(RootSizeRow {
            root,
            files: stat.files,
            dirs: stat.dirs,
            client_bytes: stat.client_bytes,
            used_bytes,
            backend_bytes,
            payload_bytes,
            metadata_bytes,
            backend_objects: all_keys.len(),
            missing_objects: payload_keys.missing + metadata_keys.missing,
        });
    }
    let totals = root_size_totals(RootSizeTotalsInput {
        remote_objects: &remote_objects,
        remote_size_map: &remote_size_map,
        unique_keys: &unique_keys,
        unique_payload_keys: &unique_payload_keys,
        unique_metadata_keys: &unique_metadata_keys,
        unique_packed_payload_keys: &unique_packed_payload_keys,
        unique_packed_slice_bytes,
        rows: &rows,
    });
    let summary = root_size_summary_from_scan(
        &config.host.id,
        &current,
        &snapshot_created_at,
        &rows,
        &totals,
    );
    if let Err(err) = write_cached_root_size_summary(paths, &summary) {
        eprintln!("warning: failed to update local root size summary cache: {err:#}");
    }
    let history = if args.history {
        Some(root_size_history_report(
            paths,
            conn,
            remote.as_ref(),
            &current,
            args.history_limit,
        )?)
    } else {
        None
    };
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&RootSizeReport {
                roots: &rows,
                totals,
                history: history.as_ref(),
            })?
        );
    } else {
        if stream_pending && root_size_stdout_is_interactive() {
            print!("\x1b[2J\x1b[H");
        } else if stream_pending {
            println!();
            println!("--- remote object listing completed ---");
            println!();
        }
        print_root_size_table(&rows, &totals);
        if let Some(history) = &history {
            println!();
            print_root_size_history(history);
        }
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PayloadIdentity {
    oid: String,
}

#[derive(Clone)]
struct PayloadOccurrence {
    identity: PayloadIdentity,
    kind: String,
    bytes: u64,
    root: String,
    path: String,
    snapshot_id: String,
    seen_at: String,
}

#[derive(Default)]
struct PayloadHistoryAgg {
    bytes: u64,
    kinds: BTreeSet<String>,
    roots: BTreeSet<String>,
    paths: BTreeSet<String>,
    snapshots: BTreeSet<String>,
    first_seen: Option<String>,
    last_seen: Option<String>,
}

fn root_size_history_report(
    paths: &Paths,
    conn: &Connection,
    remote: Option<&RemoteStore>,
    current_snapshot: &str,
    limit: usize,
) -> Result<RootSizeHistoryReport> {
    let current_payloads = snapshot_payload_occurrences(paths, conn, remote, current_snapshot)?
        .into_iter()
        .map(|occurrence| occurrence.identity)
        .collect::<BTreeSet<_>>();

    let mut stmt = conn.prepare("select id from snapshots order by created_at asc")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut aggregate = BTreeMap::<PayloadIdentity, PayloadHistoryAgg>::new();
    let mut scanned_snapshots = 0usize;
    let mut warnings = Vec::new();
    for row in rows {
        let snapshot_id = row?;
        match snapshot_payload_occurrences(paths, conn, remote, &snapshot_id) {
            Ok(occurrences) => {
                scanned_snapshots += 1;
                for occurrence in occurrences {
                    if current_payloads.contains(&occurrence.identity) {
                        continue;
                    }
                    let entry = aggregate.entry(occurrence.identity).or_default();
                    entry.bytes = entry.bytes.max(occurrence.bytes);
                    entry.kinds.insert(occurrence.kind);
                    entry.roots.insert(occurrence.root);
                    entry.paths.insert(occurrence.path);
                    entry.snapshots.insert(occurrence.snapshot_id);
                    match &entry.first_seen {
                        Some(first_seen) if first_seen <= &occurrence.seen_at => {}
                        _ => entry.first_seen = Some(occurrence.seen_at.clone()),
                    }
                    match &entry.last_seen {
                        Some(last_seen) if last_seen >= &occurrence.seen_at => {}
                        _ => entry.last_seen = Some(occurrence.seen_at),
                    }
                }
            }
            Err(err) => warnings.push(format!("{snapshot_id}: {err:#}")),
        }
    }
    let retained_bytes = aggregate.values().map(|entry| entry.bytes).sum();
    let retained_payloads = aggregate.len();
    let skipped_snapshots = warnings.len();
    let mut rows = aggregate
        .into_iter()
        .map(|(identity, entry)| RootSizeHistoryRow {
            bytes: entry.bytes,
            kind: join_examples(&entry.kinds, 2),
            snapshots: entry.snapshots.len(),
            first_seen: entry.first_seen.unwrap_or_default(),
            last_seen: entry.last_seen.unwrap_or_default(),
            root: join_examples(&entry.roots, 2),
            path: join_examples(&entry.paths, 1),
            oid: short_oid(&identity.oid),
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .bytes
            .cmp(&left.bytes)
            .then_with(|| right.snapshots.cmp(&left.snapshots))
            .then_with(|| left.root.cmp(&right.root))
            .then_with(|| left.path.cmp(&right.path))
    });
    rows.truncate(limit);
    Ok(RootSizeHistoryReport {
        retained_bytes,
        retained_payloads,
        scanned_snapshots,
        skipped_snapshots,
        rows,
        warnings,
    })
}

fn snapshot_payload_occurrences(
    paths: &Paths,
    conn: &Connection,
    remote: Option<&RemoteStore>,
    snapshot_id: &str,
) -> Result<Vec<PayloadOccurrence>> {
    let (created_at, manifest_key, manifest_json): (String, String, String) = conn.query_row(
        "select created_at, manifest_key, manifest_json from snapshots where id=?1",
        params![snapshot_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let manifest: SnapshotManifest = if manifest_json.trim().is_empty() {
        read_metadata_manifest(paths, remote, &manifest_key)
            .with_context(|| format!("read snapshot manifest {manifest_key}"))?
    } else {
        serde_json::from_str(&manifest_json)
            .with_context(|| format!("decode snapshot manifest json for {snapshot_id}"))?
    };
    let mut occurrences = Vec::new();
    for (root_id, root_snapshot) in &manifest.root_trees {
        let tree: TreeManifest = read_metadata_manifest(paths, remote, &root_snapshot.tree_key)
            .with_context(|| format!("read root tree {}", root_snapshot.tree_key))?;
        let entries = root_size_tree_entries(paths, remote, tree)?;
        for (path, record) in entries {
            add_payload_occurrence(
                &mut occurrences,
                snapshot_id,
                &created_at,
                root_id,
                &path,
                &record,
            );
        }
    }
    for (root_id, records) in &manifest.roots {
        for record in records {
            add_payload_occurrence(
                &mut occurrences,
                snapshot_id,
                &created_at,
                root_id,
                &record.path,
                record,
            );
        }
    }
    Ok(occurrences)
}

fn add_payload_occurrence(
    occurrences: &mut Vec<PayloadOccurrence>,
    snapshot_id: &str,
    seen_at: &str,
    root_id: &str,
    path: &str,
    record: &FileRecord,
) {
    let Some(identity) = payload_identity(&record.payload) else {
        return;
    };
    occurrences.push(PayloadOccurrence {
        identity,
        kind: payload_kind(&record.payload).unwrap_or_else(|| "payload".into()),
        bytes: record.size,
        root: root_id.to_string(),
        path: path.to_string(),
        snapshot_id: snapshot_id.to_string(),
        seen_at: seen_at.to_string(),
    });
}

fn payload_identity(payload: &Payload) -> Option<PayloadIdentity> {
    if let Some((oid, _)) = payload_blob_ref(payload) {
        return Some(PayloadIdentity {
            oid: oid.to_string(),
        });
    }
    if let Some((oid, _, _)) = payload_large_ref(payload) {
        return Some(PayloadIdentity {
            oid: oid.to_string(),
        });
    }
    None
}

fn payload_kind(payload: &Payload) -> Option<String> {
    if payload_blob_ref(payload).is_some() {
        return Some("blob".into());
    }
    if payload_large_ref(payload).is_some() {
        return Some("large".into());
    }
    None
}

fn join_examples(values: &BTreeSet<String>, max_items: usize) -> String {
    let mut items = values.iter().take(max_items).cloned().collect::<Vec<_>>();
    if values.len() > max_items {
        items.push(format!("+{}", values.len() - max_items));
    }
    items.join(",")
}

fn short_oid(oid: &str) -> String {
    oid.chars().take(12).collect()
}

fn print_root_size_history(report: &RootSizeHistoryReport) {
    println!("履歴保持payload:");
    println!(
        "- current snapshotから外れている保持payload推定: {}",
        format_mib(report.retained_bytes)
    );
    println!("  - payloads: {}", format_count(report.retained_payloads));
    println!(
        "  - scanned snapshots: {}",
        format_count(report.scanned_snapshots)
    );
    if report.skipped_snapshots > 0 {
        println!(
            "  - skipped snapshots: {}",
            format_count(report.skipped_snapshots)
        );
    }
    if report.rows.is_empty() {
        println!("- top historical payloads: none");
    } else {
        println!();
        let rows = report
            .rows
            .iter()
            .map(|row| {
                vec![
                    format_mib(row.bytes),
                    row.kind.clone(),
                    format_count(row.snapshots),
                    row.first_seen.clone(),
                    row.last_seen.clone(),
                    row.root.clone(),
                    row.path.clone(),
                    row.oid.clone(),
                ]
            })
            .collect::<Vec<_>>();
        print_aligned_table(
            &[
                "size",
                "kind",
                "snapshots",
                "first",
                "last",
                "root",
                "path",
                "oid",
            ],
            &[true, false, true, false, false, false, false, false],
            &rows,
        );
    }
    if !report.warnings.is_empty() {
        println!();
        println!("警告:");
        for warning in report.warnings.iter().take(10) {
            println!("- {warning}");
        }
        if report.warnings.len() > 10 {
            println!("- ... and {} more", report.warnings.len() - 10);
        }
    }
}

fn root_size_summary_from_scan(
    host_id: &str,
    snapshot_id: &str,
    generated_at: &str,
    rows: &[RootSizeRow],
    totals: &RootSizeTotals,
) -> RootSizeSummary {
    RootSizeSummary {
        version: 1,
        host_id: host_id.to_string(),
        snapshot_id: snapshot_id.to_string(),
        generated_at: generated_at.to_string(),
        roots: rows
            .iter()
            .map(|row| RootSizeSummaryRow {
                root: row.root.clone(),
                files: row.files,
                dirs: row.dirs,
                client_bytes: row.client_bytes,
                used_bytes: row.used_bytes,
                backend_bytes: row.backend_bytes,
                payload_bytes: row.payload_bytes,
                metadata_bytes: row.metadata_bytes,
                backend_objects: row.backend_objects,
                missing_objects: row.missing_objects,
            })
            .collect(),
        totals: RootSizeSummaryTotals {
            billed_bytes: totals.billed_bytes,
            billed_objects: totals.billed_objects,
            row_used_bytes: totals.row_used_bytes,
            unique_used_bytes: totals.unique_used_bytes,
            current_backend_bytes: totals.current_backend_bytes,
            payload_bytes: totals.payload_bytes,
            metadata_bytes: totals.metadata_bytes,
            objects: totals.objects,
            backend_prefix_bytes: totals.backend_prefix_bytes,
            backend_prefix_objects: totals.backend_prefix_objects,
            backend_prefix_exact: totals.backend_prefix_exact,
            backend_prefix_scope: totals.backend_prefix_scope.clone(),
        },
    }
}

fn root_size_remote_objects(
    paths: &Paths,
    remote: &RemoteStore,
    use_cache: bool,
) -> Result<RootSizeRemoteObjects> {
    let cache_ttl = root_size_remote_cache_ttl();
    if use_cache
        && cache_ttl > Duration::zero()
        && let Some(cache) = read_root_size_remote_object_cache(paths, remote, cache_ttl)?
    {
        return Ok(RootSizeRemoteObjects {
            objects: cache.objects,
            exact: true,
            scope: format!("cached-prefix-list:{}", cache.fetched_at.to_rfc3339()),
        });
    }
    if use_cache
        && env::var("MAJUTSU_ROOT_SIZE_FORCE_SCAN").as_deref() != Ok("1")
        && let Some(cache) = read_root_size_remote_object_cache_any_age(paths, remote)?
    {
        return Ok(RootSizeRemoteObjects {
            objects: cache.objects,
            exact: false,
            scope: format!("stale-cached-prefix-list:{}", cache.fetched_at.to_rfc3339()),
        });
    }
    if let Some(delay) = root_size_remote_list_delay() {
        thread::sleep(delay);
    }
    let objects = remote.list_with_sizes("")?;
    if use_cache {
        write_root_size_remote_object_cache(paths, remote, &objects)?;
    }
    Ok(RootSizeRemoteObjects {
        objects,
        exact: true,
        scope: "full-prefix-scan".into(),
    })
}

fn root_size_remote_list_delay() -> Option<StdDuration> {
    env::var("MAJUTSU_ROOT_SIZE_REMOTE_LIST_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(StdDuration::from_millis)
}

fn root_size_remote_cache_ttl() -> Duration {
    env::var("MAJUTSU_ROOT_SIZE_REMOTE_CACHE_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .map(Duration::seconds)
        .unwrap_or_else(|| Duration::seconds(60))
}

fn root_size_remote_object_cache_path(paths: &Paths) -> std::path::PathBuf {
    paths.home.join("cache/root-size-remote-objects.json")
}

fn read_root_size_remote_object_cache(
    paths: &Paths,
    remote: &RemoteStore,
    ttl: Duration,
) -> Result<Option<RootSizeRemoteObjectCache>> {
    let Some(cache) = read_root_size_remote_object_cache_any_age(paths, remote)? else {
        return Ok(None);
    };
    if Utc::now().signed_duration_since(cache.fetched_at) > ttl {
        return Ok(None);
    }
    Ok(Some(cache))
}

fn read_root_size_remote_object_cache_any_age(
    paths: &Paths,
    remote: &RemoteStore,
) -> Result<Option<RootSizeRemoteObjectCache>> {
    let path = root_size_remote_object_cache_path(paths);
    if !path.exists() {
        return Ok(None);
    }
    let cache: RootSizeRemoteObjectCache = match serde_json::from_slice(&fs::read(path)?) {
        Ok(cache) => cache,
        Err(_) => return Ok(None),
    };
    if cache.version != 1 || cache.remote != remote.describe() {
        return Ok(None);
    }
    Ok(Some(cache))
}

fn write_root_size_remote_object_cache(
    paths: &Paths,
    remote: &RemoteStore,
    objects: &[RemoteObjectStat],
) -> Result<()> {
    let path = root_size_remote_object_cache_path(paths);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    let cache = RootSizeRemoteObjectCache {
        version: 1,
        remote: remote.describe(),
        fetched_at: Utc::now(),
        objects: objects.to_vec(),
    };
    fs::write(&tmp, serde_json::to_vec(&cache)?)?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn add_payload_remote_keys(
    paths: &Paths,
    remote: Option<&RemoteStore>,
    packed_blobs: &BTreeMap<String, PackedBlobSizeRef>,
    payload: &Payload,
    stat: &mut RootSizeStat,
) -> Result<()> {
    if let Some((oid, object_key)) = payload_blob_ref(payload) {
        if let Some(packed) = packed_blobs.get(oid) {
            stat.packed_payload_oids.insert(oid.to_string());
            stat.packed_payload_keys.insert(packed.pack_key.clone());
            stat.payload_keys.insert(packed.pack_key.clone());
            stat.metadata_keys.insert(packed.index_key.clone());
            stat.packed_slice_bytes = stat.packed_slice_bytes.saturating_add(packed.pack_len);
        } else {
            stat.payload_keys.insert(object_key.to_string());
        }
        return Ok(());
    }
    if let Some((manifest_key, chunk_count)) = payload_large_manifest(payload) {
        stat.metadata_keys.insert(manifest_key.to_string());
        let manifest: LargeManifest = read_metadata_manifest(paths, remote, manifest_key)
            .with_context(|| format!("read large manifest {manifest_key}"))?;
        if manifest.chunks.len() != chunk_count {
            bail!(
                "large manifest chunk count mismatch for {manifest_key}: payload={chunk_count} manifest={}",
                manifest.chunks.len()
            );
        }
        for chunk in manifest.chunks {
            stat.payload_keys.insert(chunk.object_key);
        }
    }
    Ok(())
}

fn payload_large_manifest(payload: &Payload) -> Option<(&str, usize)> {
    match payload {
        Payload::ChunkedBlob {
            manifest_key,
            chunk_count,
            ..
        }
        | Payload::LargeObject {
            manifest_key,
            chunk_count,
            ..
        }
        | Payload::Large {
            manifest_key,
            chunk_count,
            ..
        } => Some((manifest_key, *chunk_count)),
        _ => None,
    }
}

fn packed_blob_size_refs(conn: &Connection) -> Result<BTreeMap<String, PackedBlobSizeRef>> {
    let mut stmt = conn.prepare(
        "select b.oid, p.pack_key, p.index_key, coalesce(b.pack_len, 0) \
         from blobs b join packs p on b.pack_id=p.pack_id \
         where b.pack_id is not null",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            PackedBlobSizeRef {
                pack_key: row.get(1)?,
                index_key: row.get(2)?,
                pack_len: row.get(3)?,
            },
        ))
    })?;
    let mut packed = BTreeMap::new();
    for row in rows {
        let (oid, reference) = row?;
        packed.insert(oid, reference);
    }
    Ok(packed)
}

fn read_metadata_manifest<T: for<'de> serde::Deserialize<'de>>(
    paths: &Paths,
    remote: Option<&RemoteStore>,
    key: &str,
) -> Result<T> {
    if let Ok(bytes) = fs::read(paths.home.join(key)) {
        let decoded = crate::decode_object(paths, &bytes)?;
        if let Ok(value) = serde_json::from_slice(&decoded) {
            return Ok(value);
        }
        if let Ok(decompressed) =
            zstd_decode_all_limited(decoded.as_slice(), REMOTE_METADATA_DECODE_LIMIT, key)
            && let Ok(value) = serde_cbor::from_slice(&decompressed)
        {
            return Ok(value);
        }
    }
    let remote = remote.ok_or_else(|| anyhow!("metadata object is not cached locally: {key}"))?;
    let remote_key =
        crate::majutsu_store::canonical_remote_alias(key).unwrap_or_else(|| key.to_string());
    let bytes = remote.get(&remote_key)?;
    crate::decode_canonical_remote_export(paths, &bytes)
}

fn root_size_tree_entries(
    paths: &Paths,
    remote: Option<&RemoteStore>,
    tree: TreeManifest,
) -> Result<BTreeMap<String, FileRecord>> {
    if !tree.entries.is_empty() || tree.root_node.is_none() {
        return Ok(tree.entries);
    }
    let root_node = tree.root_node.expect("checked above");
    let node: TreeNodeManifest = read_metadata_manifest(paths, remote, &root_node.node_key)
        .with_context(|| format!("read root tree node {}", root_node.node_key))?;
    root_size_tree_entries_from_node(paths, remote, node)
}

fn root_size_tree_entries_from_node(
    paths: &Paths,
    remote: Option<&RemoteStore>,
    node: TreeNodeManifest,
) -> Result<BTreeMap<String, FileRecord>> {
    let mut entries = node.entries;
    for child in node.child_nodes.values() {
        let child_node: TreeNodeManifest =
            read_metadata_manifest(paths, remote, &child.node_key)
                .with_context(|| format!("read child tree node {}", child.node_key))?;
        entries.extend(root_size_tree_entries_from_node(paths, remote, child_node)?);
    }
    Ok(entries)
}

struct ResolvedRemoteKeys {
    found: BTreeSet<String>,
    missing: usize,
}

fn resolve_remote_keys(
    keys: &BTreeSet<String>,
    remote_size_map: &BTreeMap<String, u64>,
    is_s3_remote: bool,
) -> ResolvedRemoteKeys {
    let mut found = BTreeSet::new();
    let mut missing = 0usize;
    for key in keys {
        let candidates = remote_key_candidates(key, is_s3_remote);
        if let Some(candidate) = candidates
            .into_iter()
            .find(|candidate| remote_size_map.contains_key(candidate))
        {
            found.insert(candidate);
        } else {
            missing += 1;
        }
    }
    ResolvedRemoteKeys { found, missing }
}

fn remote_key_candidates(key: &str, is_s3_remote: bool) -> Vec<String> {
    let alias = crate::majutsu_store::canonical_remote_alias(key).filter(|alias| alias != key);
    match (is_s3_remote, alias) {
        (true, Some(alias)) => vec![alias, key.to_string()],
        (_, Some(alias)) => vec![key.to_string(), alias],
        (_, None) => vec![key.to_string()],
    }
}

fn sum_remote_keys(remote_size_map: &BTreeMap<String, u64>, keys: &BTreeSet<String>) -> u64 {
    keys.iter()
        .filter_map(|key| remote_size_map.get(key).copied())
        .sum()
}

fn root_size_totals(input: RootSizeTotalsInput<'_>) -> RootSizeTotals {
    let billed_bytes = input
        .remote_objects
        .objects
        .iter()
        .map(|object| object.size)
        .sum();
    let billed_objects = input.remote_objects.objects.len();
    let current_backend_bytes = sum_remote_keys(input.remote_size_map, input.unique_keys);
    let payload_bytes = sum_remote_keys(input.remote_size_map, input.unique_payload_keys);
    let metadata_bytes = sum_remote_keys(input.remote_size_map, input.unique_metadata_keys);
    let packed_payload_bytes =
        sum_remote_keys(input.remote_size_map, input.unique_packed_payload_keys);
    let row_used_bytes = input.rows.iter().map(|row| row.used_bytes).sum();
    let unique_used_bytes = current_backend_bytes
        .saturating_sub(packed_payload_bytes)
        .saturating_add(input.unique_packed_slice_bytes);
    RootSizeTotals {
        billed_bytes,
        billed_objects,
        row_used_bytes,
        unique_used_bytes,
        current_backend_bytes,
        payload_bytes,
        metadata_bytes,
        objects: input.unique_keys.len(),
        backend_prefix_bytes: billed_bytes,
        backend_prefix_objects: billed_objects,
        backend_prefix_exact: input.remote_objects.exact,
        backend_prefix_scope: input.remote_objects.scope.clone(),
    }
}

fn root_size_should_stream_pending(
    args: &RootSizeArgs,
    task: Option<&thread::JoinHandle<Result<RootSizeRemoteObjects>>>,
) -> bool {
    if args.json || args.history {
        return false;
    }
    if task.is_none_or(|task| task.is_finished()) {
        return false;
    }
    root_size_stdout_is_interactive()
        || env::var("MAJUTSU_ROOT_SIZE_FORCE_STREAM").as_deref() == Ok("1")
}

fn root_size_stdout_is_interactive() -> bool {
    io::stdout().is_terminal() && env::var("TERM").as_deref() != Ok("dumb")
}

fn root_size_pending_table(stats: &BTreeMap<String, RootSizeStat>) -> String {
    let table_rows = stats
        .iter()
        .map(|(root, stat)| {
            vec![
                root.clone(),
                format_count(stat.files),
                format_count(stat.dirs),
                format_mib(stat.client_bytes),
                "|".into(),
                "...".into(),
                "...".into(),
                "...".into(),
                "...".into(),
                "...".into(),
                "...".into(),
            ]
        })
        .collect::<Vec<_>>();
    let mut out = String::new();
    out.push_str(&aligned_table_text(
        &[
            "root", "files", "dirs", "client", "|", "backend", "used", "payload", "metadata",
            "objects", "missing",
        ],
        &[
            false, true, true, true, false, true, true, true, true, true, true,
        ],
        &table_rows,
    ));
    let client_bytes = stats
        .values()
        .fold(0_u64, |sum, stat| sum.saturating_add(stat.client_bytes));
    let files = stats.values().map(|stat| stat.files).sum::<usize>();
    let dirs = stats.values().map(|stat| stat.dirs).sum::<usize>();
    writeln!(out).ok();
    writeln!(
        out,
        "注: `|` より左はclient側、右はremote側。remote側はS3 object listing完了後に更新されます。"
    )
    .ok();
    writeln!(out).ok();
    writeln!(out, "全体:").ok();
    writeln!(
        out,
        "- local root scan: files {}  dirs {}  client {}",
        format_count(files),
        format_count(dirs),
        format_mib(client_bytes)
    )
    .ok();
    writeln!(out, "- remote集計: ...").ok();
    writeln!(out, "- S3上の実サイズbackend prefix全体: ...").ok();
    out
}

fn print_root_size_table(rows: &[RootSizeRow], totals: &RootSizeTotals) {
    let table_rows = rows
        .iter()
        .map(|row| {
            vec![
                row.root.clone(),
                format_count(row.files),
                format_count(row.dirs),
                format_mib(row.client_bytes),
                "|".into(),
                format_mib(row.backend_bytes),
                format_mib(row.used_bytes),
                format_mib(row.payload_bytes),
                format_mib(row.metadata_bytes),
                format_count(row.backend_objects),
                format_count(row.missing_objects),
            ]
        })
        .collect::<Vec<_>>();
    print_aligned_table(
        &[
            "root", "files", "dirs", "client", "|", "backend", "used", "payload", "metadata",
            "objects", "missing",
        ],
        &[
            false, true, true, true, false, true, true, true, true, true, true,
        ],
        &table_rows,
    );
    println!();
    println!(
        "注: `|` より左はclient側、右はremote側。backend は復元に必要なremote object全体、used はpack内slice換算の実利用量。"
    );
    println!();
    println!("全体:");
    println!(
        "- root別used集計合計: {}",
        format_mib(totals.row_used_bytes)
    );
    println!("  - 注: root間共有payloadは重複計上されます。");
    println!(
        "- current snapshotのユニークused推定: {}",
        format_mib(totals.unique_used_bytes)
    );
    println!("  - 注: pack内slice換算。S3課金対象サイズではありません。");
    println!(
        "- current snapshotが参照するremote object全体: {}",
        format_mib(totals.current_backend_bytes)
    );
    println!("  - 注: pack object全体サイズ。root別backend列は共有objectを含むため合計不可。");
    println!("  - payload: {}", format_mib(totals.payload_bytes));
    println!("  - metadata: {}", format_mib(totals.metadata_bytes));
    println!("  - objects: {}", format_count(totals.objects));
    println!(
        "- S3上の実サイズbackend prefix全体: {}",
        format_mib(totals.billed_bytes)
    );
    println!("  - objects: {}", format_count(totals.billed_objects));
    println!("  - exact: {}", totals.backend_prefix_exact);
}

fn terminal_width() -> usize {
    env::var("COLUMNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|width| *width >= 40)
        .unwrap_or(100)
}

fn print_table<const N: usize>(
    out: &mut String,
    width: usize,
    headers: &[&str; N],
    rows: &[[String; N]],
) {
    let mut widths = [0usize; N];
    for (index, column_width) in widths.iter_mut().enumerate() {
        *column_width = headers[index].chars().count();
    }
    for row in rows {
        for (index, column_width) in widths.iter_mut().enumerate() {
            *column_width = (*column_width).max(row[index].chars().count());
        }
    }
    let available_width = width.saturating_sub(2 + ((N.saturating_sub(1)) * 2));
    while widths.iter().sum::<usize>() > available_width {
        let Some((index, _)) = widths
            .iter()
            .enumerate()
            .filter(|(index, column_width)| **column_width > headers[*index].len().max(8))
            .max_by_key(|(_, column_width)| **column_width)
        else {
            break;
        };
        widths[index] = widths[index].saturating_sub(1);
    }
    write_table_row(out, headers, &widths);
    write_table_separator(out, &widths);
    for row in rows {
        write_table_row(out, row, &widths);
    }
}

fn write_table_separator<const N: usize>(out: &mut String, widths: &[usize; N]) {
    out.push_str("  ");
    for (index, width) in widths.iter().enumerate() {
        if index > 0 {
            out.push_str("  ");
        }
        out.push_str(&"-".repeat(*width));
    }
    out.push('\n');
}

fn write_table_row<const N: usize, S: AsRef<str>>(
    out: &mut String,
    row: &[S; N],
    widths: &[usize; N],
) {
    out.push_str("  ");
    for (index, width) in widths.iter().enumerate() {
        if index > 0 {
            out.push_str("  ");
        }
        let cell = truncate_cell(row[index].as_ref(), *width);
        let _ = write!(out, "{cell:<width$}");
    }
    out.push('\n');
}

fn truncate_cell(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.into();
    }
    if width <= 1 {
        return "…".into();
    }
    let mut out = value
        .chars()
        .take(width.saturating_sub(1))
        .collect::<String>();
    out.push('…');
    out
}

fn print_aligned_table(headers: &[&str], right_align: &[bool], rows: &[Vec<String>]) {
    print!("{}", aligned_table_text(headers, right_align, rows));
}

fn aligned_table_text(headers: &[&str], right_align: &[bool], rows: &[Vec<String>]) -> String {
    let widths = headers
        .iter()
        .enumerate()
        .map(|(index, header)| {
            rows.iter()
                .filter_map(|row| row.get(index))
                .map(|value| value.len())
                .max()
                .unwrap_or(0)
                .max(header.len())
        })
        .collect::<Vec<_>>();
    let mut out = String::new();
    write_table_line_text(&mut out, headers, &widths, right_align);
    let separator = widths
        .iter()
        .map(|width| "-".repeat(*width))
        .collect::<Vec<_>>()
        .join("  ");
    writeln!(out, "{separator}").ok();
    for row in rows {
        let cells = row.iter().map(String::as_str).collect::<Vec<_>>();
        write_table_line_text(&mut out, &cells, &widths, right_align);
    }
    out
}

fn write_table_line_text(out: &mut String, cells: &[&str], widths: &[usize], right_align: &[bool]) {
    let line = cells
        .iter()
        .enumerate()
        .map(|(index, cell)| {
            if right_align.get(index).copied().unwrap_or(false) {
                format!("{:>width$}", cell, width = widths[index])
            } else {
                format!("{:<width$}", cell, width = widths[index])
            }
        })
        .collect::<Vec<_>>()
        .join("  ");
    writeln!(out, "{line}").ok();
}

fn format_mib(bytes: u64) -> String {
    format!("{:.2} MiB", bytes as f64 / 1024.0 / 1024.0)
}

fn format_count(value: usize) -> String {
    let text = value.to_string();
    let mut grouped = String::new();
    for (index, ch) in text.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    grouped.chars().rev().collect()
}
