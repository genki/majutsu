use anyhow::{Result, anyhow};
use majutsu_core::{FileRecord, RootSnapshot, SnapshotManifest};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;

use crate::cli::RestoreArgs;
use crate::util::parse_time;

pub(crate) fn current_snapshot(conn: &Connection) -> Result<Option<String>> {
    conn.query_row("select value from refs where name='current'", [], |row| {
        row.get(0)
    })
    .optional()
    .map_err(Into::into)
}

pub(crate) fn load_snapshot(conn: &Connection, args: &RestoreArgs) -> Result<SnapshotManifest> {
    let id = if let Some(id) = &args.snapshot {
        id.clone()
    } else if let Some(at) = &args.at {
        snapshot_id_at(conn, at)?
    } else {
        current_snapshot(conn)?.ok_or_else(|| anyhow!("no current snapshot"))?
    };
    let json: String = conn.query_row(
        "select manifest_json from snapshots where id=?1",
        params![id],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
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

pub(crate) fn load_snapshot_by_id(conn: &Connection, id: &str) -> Result<SnapshotManifest> {
    let json: String = conn.query_row(
        "select manifest_json from snapshots where id=?1",
        params![id],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

pub(crate) fn snapshot_contains_root(
    conn: &Connection,
    snapshot_id: &str,
    root: &str,
) -> Result<bool> {
    Ok(load_snapshot_by_id(conn, snapshot_id)?
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
