use anyhow::Result;
use majutsu_core::{LargeManifest, LiveMetadataReferences};
use majutsu_policy::{SnapshotPruneInput, SnapshotPrunePlan, build_snapshot_prune_plan};
use rusqlite::{Connection, params};
use std::collections::BTreeSet;
use std::fs;

use crate::cli::PruneArgs;
use crate::config::{Paths, read_config};
use crate::object_paths::{all_local_object_keys, local_object_keys_with_progress};
use crate::operation_log::record_op;
use crate::snapshot_state::current_snapshot;
use crate::util::parse_db_time;
use crate::{ensure_ready, export_metadata, open_db, read_object};

pub(crate) fn prune_cmd(paths: &Paths, args: PruneArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let plan = build_prune_plan(&conn, &args)?;
    let total = plan.keep.len() + plan.delete.len();
    println!("snapshots {total}");
    println!("keep_daily {}", args.keep_daily);
    println!("keep_monthly {}", args.keep_monthly);
    println!("keep_snapshots {}", plan.keep.len());
    println!("candidate_snapshots {}", plan.delete.len());
    if args.dry_run {
        println!("dry_run true");
    } else {
        let before = current_snapshot(&conn)?;
        for snapshot in &plan.delete {
            conn.execute("delete from snapshots where id=?1", params![snapshot])?;
        }
        let removed = prune_unreferenced_metadata(paths, &conn)?;
        record_op(
            &conn,
            "prune",
            before.as_deref(),
            before.as_deref(),
            Some(&format!("deleted {} snapshots", plan.delete.len())),
        )?;
        println!("dry_run false");
        println!("deleted_snapshots {}", plan.delete.len());
        println!("removed_blob_metadata {}", removed.blobs);
        println!("removed_large_metadata {}", removed.large_objects);
        println!("removed_chunk_metadata {}", removed.chunks);
        println!("removed_large_pins {}", removed.large_pins);
    }
    Ok(())
}

struct PrunedMetadata {
    blobs: usize,
    large_objects: usize,
    chunks: usize,
    large_pins: usize,
}

fn build_prune_plan(conn: &Connection, args: &PruneArgs) -> Result<SnapshotPrunePlan> {
    let current = current_snapshot(conn)?;
    let mut stmt = conn.prepare("select id, created_at from snapshots order by created_at desc")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let snapshots = rows
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .map(|(id, created)| {
            Ok(SnapshotPruneInput {
                id,
                created_at: parse_db_time(&created)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut plan = build_snapshot_prune_plan(
        &snapshots,
        current.as_deref(),
        args.keep_daily,
        args.keep_monthly,
    );
    let protected = protected_ref_snapshots(conn, &snapshots)?;
    let mut still_delete = Vec::new();
    for snapshot_id in plan.delete {
        if protected.contains(&snapshot_id) {
            plan.keep.push(snapshot_id);
        } else {
            still_delete.push(snapshot_id);
        }
    }
    plan.keep.sort();
    plan.keep.dedup();
    plan.delete = still_delete;
    Ok(plan)
}

fn protected_ref_snapshots(
    conn: &Connection,
    snapshots: &[SnapshotPruneInput],
) -> Result<BTreeSet<String>> {
    let snapshot_ids = snapshots
        .iter()
        .map(|snapshot| snapshot.id.clone())
        .collect::<BTreeSet<_>>();
    let mut protected = BTreeSet::new();
    let mut stmt = conn.prepare("select value from refs")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let value = row?;
        if snapshot_ids.contains(&value) {
            protected.insert(value);
        }
    }
    Ok(protected)
}

fn prune_unreferenced_metadata(paths: &Paths, conn: &Connection) -> Result<PrunedMetadata> {
    let mut live = LiveMetadataReferences::default();
    let mut stmt = conn.prepare("select id, manifest_key, manifest_json from snapshots")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (id, manifest_key, manifest_json) = row?;
        let manifest = crate::snapshot_state::snapshot_manifest_from_parts(
            paths,
            &id,
            &manifest_key,
            &manifest_json,
        )?;
        for manifest_key in live.add_snapshot_manifest(&manifest) {
            let large_manifest: LargeManifest =
                serde_json::from_slice(&read_object(paths, &manifest_key)?)?;
            live.add_large_manifest(large_manifest);
        }
    }
    let blobs = delete_rows_not_in(conn, "blobs", "oid", &live.blobs)?;
    let large_objects = delete_rows_not_in(conn, "large_objects", "oid", &live.large_objects)?;
    let chunks = delete_rows_not_in(conn, "chunks", "oid", &live.chunks)?;
    let large_pins = delete_rows_not_in(conn, "large_pins", "oid", &live.large_objects)?;
    Ok(PrunedMetadata {
        blobs,
        large_objects,
        chunks,
        large_pins,
    })
}

fn delete_rows_not_in(
    conn: &Connection,
    table: &str,
    column: &str,
    live: &BTreeSet<String>,
) -> Result<usize> {
    let mut stmt = conn.prepare(&format!("select {column} from {table}"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut removed = 0usize;
    for row in rows {
        let id = row?;
        if !live.contains(&id) {
            conn.execute(
                &format!("delete from {table} where {column}=?1"),
                params![id],
            )?;
            removed += 1;
        }
    }
    Ok(removed)
}

pub(crate) fn gc_cmd(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let config = read_config(paths)?;
    eprintln!("gc progress phase=metadata-export");
    let export = export_metadata(paths, &conn, &config)?;
    eprintln!(
        "gc progress phase=referenced-objects snapshots={}",
        export.snapshots.len()
    );
    let referenced = local_object_keys_with_progress(paths, &export, "referenced-objects")?
        .into_iter()
        .collect::<BTreeSet<_>>();
    eprintln!(
        "gc progress phase=local-object-scan referenced={}",
        referenced.len()
    );
    let mut removed = 0usize;
    for key in all_local_object_keys(paths)? {
        if !referenced.contains(&key) {
            fs::remove_file(paths.home.join(&key))?;
            removed += 1;
            if removed.is_multiple_of(500) {
                eprintln!("gc progress phase=remove-unreferenced removed={removed}");
            }
        }
    }
    eprintln!("gc progress phase=compact-snapshot-manifests");
    let (compacted_manifests, compacted_manifest_objects) =
        compact_snapshot_manifest_metadata(paths, &conn)?;
    record_op(
        &conn,
        "gc",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!(
            "removed {removed} unreferenced objects; compacted {compacted_manifests} manifests and {compacted_manifest_objects} manifest objects"
        )),
    )?;
    println!("removed_unreferenced_objects {removed}");
    println!("compacted_snapshot_manifests {compacted_manifests}");
    println!("compacted_snapshot_manifest_objects {compacted_manifest_objects}");
    Ok(())
}

fn compact_snapshot_manifest_metadata(paths: &Paths, conn: &Connection) -> Result<(usize, usize)> {
    let mut stmt =
        conn.prepare("select id, manifest_key, manifest_json from snapshots order by created_at")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut compacted_metadata = 0usize;
    let mut compacted_objects = 0usize;
    for row in rows {
        let (id, manifest_key, manifest_json) = row?;
        if manifest_json.is_empty()
            && local_snapshot_manifest_object_is_compact(paths, &manifest_key)?
        {
            continue;
        }
        let manifest = crate::snapshot_state::snapshot_manifest_from_parts(
            paths,
            &id,
            &manifest_key,
            &manifest_json,
        )?;
        fs::write(
            paths.home.join(&manifest_key),
            crate::encode_compact_snapshot_manifest_for_local(paths, &manifest)?,
        )?;
        compacted_objects += 1;
        conn.execute(
            "update snapshots set manifest_json='' where id=?1",
            params![id],
        )?;
        compacted_metadata += 1;
    }
    Ok((compacted_metadata, compacted_objects))
}

fn local_snapshot_manifest_object_is_compact(paths: &Paths, manifest_key: &str) -> Result<bool> {
    let bytes = fs::read(paths.home.join(manifest_key))?;
    let decoded = crate::decode_object(paths, &bytes)?;
    let value: serde_json::Value = serde_json::from_slice(&decoded)?;
    let roots_omitted = value
        .get("roots_omitted")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let roots_empty = value
        .get("roots")
        .and_then(serde_json::Value::as_object)
        .is_some_and(serde_json::Map::is_empty);
    Ok(roots_omitted && roots_empty)
}
