use crate::majutsu_core::{LargeManifest, LiveMetadataReferences, payload_large_ref};
use crate::majutsu_policy::{SnapshotPruneInput, SnapshotPrunePlan, build_snapshot_prune_plan};
use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::BTreeSet;
use std::fs;

use crate::cache_runtime::{remote_payload_index_contains, remote_payload_key_index};
use crate::cli::PruneArgs;
use crate::config::{Paths, read_config};
use crate::object_paths::{all_local_object_keys, local_object_keys_with_progress};
use crate::operation_log::record_op;
use crate::remote_store::RemoteStore;
use crate::snapshot_state::current_snapshot;
use crate::util::parse_db_time;
use crate::{ensure_ready, export_metadata, open_db, open_remote, read_object};

pub(crate) fn prune_cmd(paths: &Paths, args: PruneArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let plan = build_prune_plan(paths, &conn, &args)?;
    let total = plan.keep.len() + plan.delete.len();
    println!("snapshots {total}");
    println!("keep_daily {}", args.keep_daily);
    println!("keep_monthly {}", args.keep_monthly);
    println!("keep_snapshots {}", plan.keep.len());
    println!("candidate_snapshots {}", plan.delete.len());
    if args.drop_missing_remote_history {
        println!(
            "missing_remote_history_snapshots {}",
            plan.missing_remote_history.len()
        );
        for (snapshot, missing) in &plan.missing_remote_history {
            println!("missing_remote_history_snapshot {snapshot} missing_objects {missing}");
        }
    }
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
        println!("removed_pack_metadata {}", removed.packs);
        println!(
            "removed_snapshot_payload_indexes {}",
            removed.snapshot_payload_indexes
        );
        println!(
            "removed_snapshot_payload_rows {}",
            removed.snapshot_payload_rows
        );
        println!("removed_large_pins {}", removed.large_pins);
        if args.remote_cleanup && read_config(paths)?.remote.is_some() {
            println!("remote_cleanup true");
            crate::sync_runtime::sync_cmd(
                paths,
                crate::cli::SyncArgs {
                    wait: false,
                    timeout_secs: 300,
                    command: None,
                },
            )?;
        } else {
            println!("remote_cleanup false");
        }
    }
    Ok(())
}

struct PrunePlan {
    keep: Vec<String>,
    delete: Vec<String>,
    missing_remote_history: Vec<(String, usize)>,
}

pub(crate) struct PrunedMetadata {
    pub(crate) blobs: usize,
    pub(crate) large_objects: usize,
    pub(crate) chunks: usize,
    pub(crate) packs: usize,
    pub(crate) snapshot_payload_indexes: usize,
    pub(crate) snapshot_payload_rows: usize,
    pub(crate) large_pins: usize,
}

fn build_prune_plan(paths: &Paths, conn: &Connection, args: &PruneArgs) -> Result<PrunePlan> {
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
    let mut plan: SnapshotPrunePlan = build_snapshot_prune_plan(
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
    let mut missing_remote_history = Vec::new();
    if args.drop_missing_remote_history {
        let missing =
            missing_remote_history_snapshots(paths, conn, &protected, current.as_deref())?;
        for (snapshot_id, missing_count) in missing {
            if plan.keep.binary_search(&snapshot_id).is_ok() {
                plan.keep.retain(|id| id != &snapshot_id);
            }
            if !plan.delete.contains(&snapshot_id) {
                plan.delete.push(snapshot_id.clone());
            }
            missing_remote_history.push((snapshot_id, missing_count));
        }
        plan.delete.sort();
        plan.delete.dedup();
        missing_remote_history.sort();
    }
    Ok(PrunePlan {
        keep: plan.keep,
        delete: plan.delete,
        missing_remote_history,
    })
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

fn missing_remote_history_snapshots(
    paths: &Paths,
    conn: &Connection,
    protected: &BTreeSet<String>,
    current: Option<&str>,
) -> Result<Vec<(String, usize)>> {
    let config = read_config(paths)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(Vec::new());
    };
    let remote = open_remote(remote_config)?;
    let remote_keys = remote_payload_key_index(&remote)?;
    let export = export_metadata(paths, conn, &config)?;
    let mut missing = Vec::new();
    for snapshot in &export.snapshots {
        if current == Some(snapshot.id.as_str()) || protected.contains(&snapshot.id) {
            continue;
        }
        let keys =
            crate::object_paths::local_object_keys_for_snapshot(paths, &export, &snapshot.id)?;
        let missing_count = keys
            .iter()
            .filter(|key| !remote_payload_index_contains(&remote_keys, key))
            .count();
        if missing_count > 0 {
            missing.push((snapshot.id.clone(), missing_count));
        }
    }
    Ok(missing)
}

pub(crate) fn prune_unreferenced_metadata(
    paths: &Paths,
    conn: &Connection,
) -> Result<PrunedMetadata> {
    let config = read_config(paths)?;
    let remote = config.remote.as_ref().map(open_remote).transpose()?;
    let mut live = LiveMetadataReferences::default();
    let mut incomplete_metadata_graph = false;
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
            match read_large_manifest_for_prune(paths, remote.as_ref(), &manifest_key) {
                Ok(large_manifest) => live.add_large_manifest(large_manifest),
                Err(err) => {
                    incomplete_metadata_graph = true;
                    eprintln!(
                        "warning: skipped large manifest during prune: {manifest_key}: {err:#}"
                    );
                }
            }
        }
        for root_tree in manifest.root_trees.values() {
            crate::snapshot_state::visit_tree_records(paths, root_tree, |record| {
                if let Some((oid, _)) = crate::majutsu_core::payload_blob_ref(&record.payload) {
                    live.blobs.insert(oid.to_string());
                } else if let Some((oid, manifest_key, _)) = payload_large_ref(&record.payload) {
                    live.large_objects.insert(oid.to_string());
                    match read_large_manifest_for_prune(paths, remote.as_ref(), manifest_key) {
                        Ok(large_manifest) => live.add_large_manifest(large_manifest),
                        Err(err) => {
                            incomplete_metadata_graph = true;
                            eprintln!(
                                "warning: skipped large manifest during prune: {manifest_key}: {err:#}"
                            );
                        }
                    }
                }
                Ok(())
            })?;
        }
    }
    let blobs = delete_rows_not_in(conn, "blobs", "oid", &live.blobs)?;
    let large_objects = delete_rows_not_in(conn, "large_objects", "oid", &live.large_objects)?;
    let large_pins = delete_rows_not_in(conn, "large_pins", "oid", &live.large_objects)?;
    let chunks = if let Some(live_chunks) =
        live_chunks_from_remaining_large_objects(paths, conn, remote.as_ref())?
    {
        delete_rows_not_in(conn, "chunks", "oid", &live_chunks)?
    } else if incomplete_metadata_graph {
        eprintln!(
            "warning: skipped destructive chunk metadata prune because the metadata graph is incomplete"
        );
        0
    } else {
        delete_rows_not_in(conn, "chunks", "oid", &live.chunks)?
    };
    let packs = delete_packs_without_blobs(conn)?;
    let (snapshot_payload_indexes, snapshot_payload_rows) =
        delete_snapshot_payload_indexes_without_snapshots(conn)?;
    Ok(PrunedMetadata {
        blobs,
        large_objects,
        chunks,
        packs,
        snapshot_payload_indexes,
        snapshot_payload_rows,
        large_pins,
    })
}

fn live_chunks_from_remaining_large_objects(
    paths: &Paths,
    conn: &Connection,
    remote: Option<&RemoteStore>,
) -> Result<Option<BTreeSet<String>>> {
    let mut live_chunks = BTreeSet::new();
    let mut stmt = conn.prepare("select manifest_key from large_objects order by oid")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let manifest_key = row?;
        match read_large_manifest_for_prune(paths, remote, &manifest_key) {
            Ok(large_manifest) => {
                for chunk in large_manifest.chunks {
                    live_chunks.insert(chunk.oid);
                }
            }
            Err(err) => {
                eprintln!(
                    "warning: skipped chunk metadata prune because remaining large manifest is unavailable: {manifest_key}: {err:#}"
                );
                return Ok(None);
            }
        }
    }
    Ok(Some(live_chunks))
}

fn read_large_manifest_for_prune(
    paths: &Paths,
    remote: Option<&RemoteStore>,
    manifest_key: &str,
) -> Result<LargeManifest> {
    if let Ok(bytes) = read_object(paths, manifest_key) {
        return Ok(serde_json::from_slice(&bytes)?);
    }
    let remote = remote
        .ok_or_else(|| anyhow::anyhow!("large manifest is not cached locally: {manifest_key}"))?;
    let remote_key = crate::majutsu_store::canonical_remote_alias(manifest_key)
        .unwrap_or_else(|| manifest_key.to_string());
    crate::decode_canonical_remote_export(paths, &remote.get(&remote_key)?)
}

fn delete_snapshot_payload_indexes_without_snapshots(conn: &Connection) -> Result<(usize, usize)> {
    let mut orphan_payload_stmt = conn.prepare(
        "select distinct snapshot_id from snapshot_payloads \
         where snapshot_id not in (select id from snapshots)",
    )?;
    let orphan_payload_rows = orphan_payload_stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut snapshot_ids = orphan_payload_rows.collect::<std::result::Result<Vec<_>, _>>()?;

    let mut stmt = conn.prepare(
        "select snapshot_id from snapshot_payload_index \
         where snapshot_id not in (select id from snapshots)",
    )?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    snapshot_ids.extend(rows.collect::<std::result::Result<Vec<_>, _>>()?);
    snapshot_ids.sort();
    snapshot_ids.dedup();
    let mut removed_rows = 0usize;
    for snapshot_id in &snapshot_ids {
        removed_rows += conn.execute(
            "delete from snapshot_payloads where snapshot_id=?1",
            params![snapshot_id],
        )?;
        conn.execute(
            "delete from snapshot_payload_index where snapshot_id=?1",
            params![snapshot_id],
        )?;
    }
    Ok((snapshot_ids.len(), removed_rows))
}

fn delete_packs_without_blobs(conn: &Connection) -> Result<usize> {
    let mut stmt = conn.prepare("select pack_id from packs")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut removed = 0usize;
    for row in rows {
        let pack_id = row?;
        let references: i64 = conn.query_row(
            "select count(*) from blobs where pack_id=?1",
            params![pack_id],
            |row| row.get(0),
        )?;
        if references == 0 {
            conn.execute("delete from packs where pack_id=?1", params![pack_id])?;
            removed += 1;
        }
    }
    Ok(removed)
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
