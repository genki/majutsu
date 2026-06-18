use crate::majutsu_core::{
    FileRecord, RootSnapshot, SnapshotManifest, TreeManifest, TreeNodeManifest,
};
use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;
use std::fs;

use crate::cli::RestoreArgs;
use crate::config::Paths;
use crate::util::parse_time;

pub(crate) fn current_snapshot(conn: &Connection) -> Result<Option<String>> {
    conn.query_row("select value from refs where name='current'", [], |row| {
        row.get(0)
    })
    .optional()
    .map_err(Into::into)
}

pub(crate) fn load_snapshot(
    paths: &Paths,
    conn: &Connection,
    args: &RestoreArgs,
) -> Result<SnapshotManifest> {
    let id = if let Some(id) = &args.snapshot {
        id.clone()
    } else if let Some(at) = &args.at {
        snapshot_id_at(conn, at)?
    } else {
        current_snapshot(conn)?.ok_or_else(|| anyhow!("no current snapshot"))?
    };
    if args.root.is_some() {
        return load_snapshot_by_id_for_root(paths, conn, &id, args.root.as_deref());
    }
    load_snapshot_by_id(paths, conn, &id)
}

pub(crate) fn snapshot_id_at(conn: &Connection, at: &str) -> Result<String> {
    conn.query_row(
        "select id from snapshots where created_at <= ?1 order by created_at desc limit 1",
        params![parse_time(at)?],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| anyhow!("no snapshot at or before {at}"))
}

pub(crate) fn load_snapshot_by_id(
    paths: &Paths,
    conn: &Connection,
    id: &str,
) -> Result<SnapshotManifest> {
    let (manifest_key, manifest_json): (String, String) = conn.query_row(
        "select manifest_key, manifest_json from snapshots where id=?1",
        params![id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    snapshot_manifest_from_parts(paths, id, &manifest_key, &manifest_json)
}

pub(crate) fn load_snapshot_header_by_id(
    paths: &Paths,
    conn: &Connection,
    id: &str,
) -> Result<SnapshotManifest> {
    let (manifest_key, manifest_json): (String, String) = conn.query_row(
        "select manifest_key, manifest_json from snapshots where id=?1",
        params![id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if !manifest_json.trim().is_empty() {
        return serde_json::from_str(&manifest_json)
            .with_context(|| format!("parse snapshot manifest metadata {id}"));
    }
    let bytes = read_snapshot_manifest_object_raw(paths, id, &manifest_key)?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse snapshot manifest object {id} {manifest_key}"))
}

pub(crate) fn snapshot_manifest_from_parts(
    paths: &Paths,
    id: &str,
    manifest_key: &str,
    manifest_json: &str,
) -> Result<SnapshotManifest> {
    if !manifest_json.trim().is_empty() {
        return serde_json::from_str(manifest_json)
            .with_context(|| format!("parse snapshot manifest metadata {id}"));
    }
    let bytes = match crate::read_object(paths, manifest_key) {
        Ok(bytes) => bytes,
        Err(local_err) => hydrate_snapshot_manifest_from_remote(paths, manifest_key)
            .and_then(|_| crate::read_object(paths, manifest_key))
            .with_context(|| {
                format!("read snapshot manifest object {id} {manifest_key}: {local_err}")
            })?,
    };
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse snapshot manifest object {id} {manifest_key}"))
}

pub(crate) fn load_snapshot_by_id_for_root(
    paths: &Paths,
    conn: &Connection,
    id: &str,
    root: Option<&str>,
) -> Result<SnapshotManifest> {
    let (manifest_key, manifest_json): (String, String) = conn.query_row(
        "select manifest_key, manifest_json from snapshots where id=?1",
        params![id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    snapshot_manifest_from_parts_for_root(paths, id, &manifest_key, &manifest_json, root)
}

fn snapshot_manifest_from_parts_for_root(
    paths: &Paths,
    id: &str,
    manifest_key: &str,
    manifest_json: &str,
    root: Option<&str>,
) -> Result<SnapshotManifest> {
    if !manifest_json.trim().is_empty() {
        return serde_json::from_str(manifest_json)
            .with_context(|| format!("parse snapshot manifest metadata {id}"));
    }
    let bytes = read_snapshot_manifest_object_raw(paths, id, manifest_key)?;
    let mut manifest: SnapshotManifest = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse snapshot manifest object {id} {manifest_key}"))?;
    if !manifest.roots.is_empty() || manifest.root_trees.is_empty() {
        return Ok(manifest);
    }
    let Some(root) = root else {
        return Ok(manifest);
    };
    if let Some(root_tree) = manifest.root_trees.get(root) {
        let tree_bytes = crate::read_object(paths, &root_tree.tree_key).with_context(|| {
            format!("hydrate compact snapshot manifest {manifest_key} root {root}")
        })?;
        let tree: TreeManifest = serde_json::from_slice(&tree_bytes)
            .with_context(|| format!("parse root tree {}", root_tree.tree_key))?;
        manifest.roots.insert(
            root.to_string(),
            tree_entries_from_manifest(paths, tree)?
                .into_values()
                .collect(),
        );
    }
    Ok(manifest)
}

fn read_snapshot_manifest_object_raw(
    paths: &Paths,
    id: &str,
    manifest_key: &str,
) -> Result<Vec<u8>> {
    let local = paths.home.join(manifest_key);
    let bytes = match fs::read(&local) {
        Ok(bytes) => bytes,
        Err(local_err) => {
            hydrate_snapshot_manifest_from_remote(paths, manifest_key).with_context(|| {
                format!("read snapshot manifest object {id} {manifest_key}: {local_err}")
            })?;
            fs::read(&local).with_context(|| {
                format!("read hydrated snapshot manifest object {id} {manifest_key}")
            })?
        }
    };
    crate::decode_object(paths, &bytes)
        .with_context(|| format!("decode snapshot manifest object {id} {manifest_key}"))
}

pub(crate) fn load_root_tree_entries(
    paths: &Paths,
    root_tree: &RootSnapshot,
) -> Result<BTreeMap<String, FileRecord>> {
    let tree_bytes = crate::read_object(paths, &root_tree.tree_key)
        .with_context(|| format!("read root tree {}", root_tree.tree_key))?;
    let tree: TreeManifest = serde_json::from_slice(&tree_bytes)
        .with_context(|| format!("parse root tree {}", root_tree.tree_key))?;
    tree_entries_from_manifest(paths, tree)
}

pub(crate) fn tree_entries_from_manifest(
    paths: &Paths,
    tree: TreeManifest,
) -> Result<BTreeMap<String, FileRecord>> {
    if !tree.entries.is_empty() || tree.root_node.is_none() {
        return Ok(tree.entries);
    }
    let root_node = tree.root_node.expect("checked above");
    let node_bytes = crate::read_object(paths, &root_node.node_key)
        .with_context(|| format!("read root tree node {}", root_node.node_key))?;
    let node: TreeNodeManifest = serde_json::from_slice(&node_bytes)
        .with_context(|| format!("parse root tree node {}", root_node.node_key))?;
    tree_entries_from_node(paths, node)
}

pub(crate) fn tree_entries_from_node(
    paths: &Paths,
    node: TreeNodeManifest,
) -> Result<BTreeMap<String, FileRecord>> {
    let mut entries = node.entries;
    for child in node.child_nodes.values() {
        let child_bytes = crate::read_object(paths, &child.node_key)
            .with_context(|| format!("read child tree node {}", child.node_key))?;
        let child_node: TreeNodeManifest = serde_json::from_slice(&child_bytes)
            .with_context(|| format!("parse child tree node {}", child.node_key))?;
        entries.extend(tree_entries_from_node(paths, child_node)?);
    }
    Ok(entries)
}

fn hydrate_snapshot_manifest_from_remote(paths: &Paths, manifest_key: &str) -> Result<()> {
    let config = crate::config::read_config(paths)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(());
    };
    let remote = crate::remote_store::open_remote(remote_config)?;
    if !crate::remote_object_available(&remote, manifest_key)? {
        return Ok(());
    }
    let bytes = crate::download_local_object_from_remote(paths, &remote, manifest_key)?;
    let dest = paths.home.join(manifest_key);
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(dest, bytes)?;
    Ok(())
}

pub(crate) fn snapshot_contains_root(
    paths: &Paths,
    conn: &Connection,
    snapshot_id: &str,
    root: &str,
) -> Result<bool> {
    Ok(load_snapshot_by_id(paths, conn, snapshot_id)?
        .roots
        .contains_key(root))
}

pub(crate) fn snapshot_file_map(
    snapshot: &SnapshotManifest,
) -> Result<BTreeMap<String, &FileRecord>> {
    let mut out = BTreeMap::new();
    for (root_id, records) in &snapshot.roots {
        for record in records {
            out.insert(format!("{}/{}", root_id, record.path), record);
        }
    }
    Ok(out)
}

pub(crate) fn carry_forward_root_snapshot(
    parent: Option<&SnapshotManifest>,
    root_id: &str,
    root_trees: &mut BTreeMap<String, RootSnapshot>,
    by_root: &mut BTreeMap<String, Vec<FileRecord>>,
) {
    let Some(parent) = parent else {
        return;
    };
    if let Some(root_tree) = parent.root_trees.get(root_id) {
        root_trees.insert(root_id.to_string(), root_tree.clone());
    }
    if let Some(records) = parent.roots.get(root_id) {
        by_root.insert(root_id.to_string(), records.clone());
    }
}
