use crate::majutsu_core::{
    FileRecord, LargeManifest, Payload, SnapshotManifest, TreeManifest, TreeNodeManifest,
    payload_blob_ref,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt::Write as _;
use std::fs;

use crate::cli::{RootCommand, RootSizeArgs};
use crate::config::{
    Paths, RootConfig, default_include, read_config, validate_large_chunking,
    validate_snapshot_mode,
};
use crate::daemon_runtime::ensure_daemon_running;
use crate::operation_log::record_op;
use crate::remote_store::{RemoteObjectStat, RemoteStore, open_remote};
use crate::root_size_summary::{print_root_size_summary, read_root_size_summary};
use crate::root_state::{
    root_by_id, root_by_id_optional, roots, save_root, sync_roots_to_config, update_root_status,
};
use crate::snapshot_rules::{
    apply_root_large_set, apply_root_presets, dedup_patterns, root_large_override,
    warn_sensitive_root_defaults,
};

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
            let mut exclude = args.exclude.clone();
            apply_root_presets(&mut exclude, &args.presets)?;
            warn_sensitive_root_defaults(&path, &exclude);
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
                exclude,
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
            save_root(&conn, &root)?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "config-change", None, None, Some(&root.id))?;
            println!("updated root {} -> {}", root.id, root.path.display());
        }
        RootCommand::List => {
            root_list_cmd(&conn)?;
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

fn root_list_cmd(conn: &Connection) -> Result<()> {
    let mut roots = roots(conn)?;
    roots.sort_by(|left, right| {
        root_status_rank(&left.status)
            .cmp(&root_status_rank(&right.status))
            .then_with(|| left.id.cmp(&right.id))
    });
    let total = roots.len();
    let active = roots.iter().filter(|root| root.status == "active").count();
    let problematic = roots.iter().filter(|root| root.status != "active").count();
    let width = terminal_width();
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
}

#[derive(Clone, Copy, Serialize)]
struct RootSizeTotals {
    current_backend_bytes: u64,
    payload_bytes: u64,
    metadata_bytes: u64,
    objects: usize,
    backend_prefix_bytes: u64,
    backend_prefix_objects: usize,
}

#[derive(Serialize, Deserialize)]
struct RootSizeRemoteObjectCache {
    version: u32,
    remote: String,
    fetched_at: DateTime<Utc>,
    objects: Vec<RemoteObjectStat>,
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
    let config = read_config(paths)?;
    let remote = config.remote.as_ref().map(open_remote).transpose()?;
    if env::var("MAJUTSU_ROOT_SIZE_FORCE_SCAN").as_deref() != Ok("1") {
        match read_root_size_summary(paths, remote.as_ref(), &config.host.id, &current) {
            Ok(Some(summary)) => {
                print_root_size_summary(&summary, args.json)?;
                return Ok(());
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!("warning: ignoring invalid root size summary: {err:#}");
            }
        }
    }
    let remote_sizes = remote
        .as_ref()
        .map(|remote| root_size_remote_objects(paths, remote))
        .transpose()?
        .unwrap_or_default();
    let remote_size_map = remote_sizes
        .iter()
        .map(|object| (object.key.clone(), object.size))
        .collect::<BTreeMap<_, _>>();
    let is_s3_remote = matches!(remote, Some(RemoteStore::S3(_)));
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

    let mut rows = Vec::new();
    let mut unique_keys = BTreeSet::new();
    let mut unique_payload_keys = BTreeSet::new();
    let mut unique_metadata_keys = BTreeSet::new();
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
    let totals = root_size_totals(
        &remote_sizes,
        &remote_size_map,
        &unique_keys,
        &unique_payload_keys,
        &unique_metadata_keys,
    );
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&RootSizeReport {
                roots: &rows,
                totals,
            })?
        );
    } else {
        print_root_size_table(&rows, &totals);
    }
    Ok(())
}

fn root_size_remote_objects(paths: &Paths, remote: &RemoteStore) -> Result<Vec<RemoteObjectStat>> {
    let cache_ttl = root_size_remote_cache_ttl();
    if cache_ttl > Duration::zero()
        && let Some(objects) = read_root_size_remote_object_cache(paths, remote, cache_ttl)?
    {
        return Ok(objects);
    }
    let objects = remote.list_with_sizes("")?;
    write_root_size_remote_object_cache(paths, remote, &objects)?;
    Ok(objects)
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
) -> Result<Option<Vec<RemoteObjectStat>>> {
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
    if Utc::now().signed_duration_since(cache.fetched_at) > ttl {
        return Ok(None);
    }
    Ok(Some(cache.objects))
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
        if let Ok(decompressed) = zstd::stream::decode_all(decoded.as_slice())
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
    Ok(node.entries)
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

fn root_size_totals(
    remote_sizes: &[RemoteObjectStat],
    remote_size_map: &BTreeMap<String, u64>,
    unique_keys: &BTreeSet<String>,
    unique_payload_keys: &BTreeSet<String>,
    unique_metadata_keys: &BTreeSet<String>,
) -> RootSizeTotals {
    RootSizeTotals {
        current_backend_bytes: sum_remote_keys(remote_size_map, unique_keys),
        payload_bytes: sum_remote_keys(remote_size_map, unique_payload_keys),
        metadata_bytes: sum_remote_keys(remote_size_map, unique_metadata_keys),
        objects: unique_keys.len(),
        backend_prefix_bytes: remote_sizes.iter().map(|object| object.size).sum(),
        backend_prefix_objects: remote_sizes.len(),
    }
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
        "- current snapshotのユニークbackend復元単位: {}",
        format_mib(totals.current_backend_bytes)
    );
    println!("  - payload: {}", format_mib(totals.payload_bytes));
    println!("  - metadata: {}", format_mib(totals.metadata_bytes));
    println!("  - objects: {}", format_count(totals.objects));
    println!(
        "- GCS backend prefix全体: {}",
        format_mib(totals.backend_prefix_bytes)
    );
    println!(
        "  - objects: {}",
        format_count(totals.backend_prefix_objects)
    );
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
    print_table_line(headers, &widths, right_align);
    let separator = widths
        .iter()
        .map(|width| "-".repeat(*width))
        .collect::<Vec<_>>()
        .join("  ");
    println!("{separator}");
    for row in rows {
        let cells = row.iter().map(String::as_str).collect::<Vec<_>>();
        print_table_line(&cells, &widths, right_align);
    }
}

fn print_table_line(cells: &[&str], widths: &[usize], right_align: &[bool]) {
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
    println!("{line}");
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
