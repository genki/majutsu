use crate::majutsu_core::{LargeManifest, LiveMetadataReferences, payload_large_ref};
use crate::majutsu_pack::PackIndex;
use crate::majutsu_policy::{SnapshotPruneInput, SnapshotPrunePlan, build_snapshot_prune_plan};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, params};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::cache_runtime::{
    remote_payload_index_contains, remote_payload_key_index, remote_payload_key_index_for_host,
};
use crate::cli::{GcArgs, PruneArgs};
use crate::config::{Paths, maintenance_lock_path, read_config};
use crate::object_paths::{all_local_object_keys, local_object_keys_with_progress};
use crate::operation_log::record_op;
use crate::process_runtime::acquire_process_lock;
use crate::queue_runtime::{event_journal_records, upload_queue_items};
use crate::remote_store::RemoteStore;
use crate::root_state::roots;
use crate::snapshot_state::{current_snapshot, first_snapshots_for_roots};
use crate::util::{parse_db_time, path_to_slash, validate_local_object_key};
use crate::{ensure_ready, export_metadata, open_db, open_remote, read_object};

pub(crate) fn prune_cmd(paths: &Paths, args: PruneArgs) -> Result<()> {
    ensure_ready(paths)?;
    let _maintenance_lock = acquire_process_lock(&maintenance_lock_path(paths), "maintenance")?;
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
        // DB変更とoperation記録を同一transactionに閉じ、後段の失敗で部分削除を残さない。
        let transaction = conn.unchecked_transaction()?;
        let deleted_snapshots = plan.delete.len();
        let before = current_snapshot(&conn)?;
        for snapshot in &plan.delete {
            conn.execute("delete from snapshots where id=?1", params![snapshot])?;
        }
        let removed = prune_unreferenced_metadata(paths, &conn)?;
        transaction.commit()?;
        // operation logは外部ファイルにも追記するため、DB commit前に呼ぶと
        // commit失敗時にghost recordが残る。削除を確定した後に記録する。
        if let Err(err) = record_op(
            &conn,
            "prune",
            before.as_deref(),
            before.as_deref(),
            Some(&format!("deleted {} snapshots", plan.delete.len())),
        ) {
            eprintln!("prune operation logの記録に失敗しました: {err:#}");
        }
        println!("dry_run false");
        println!("deleted_snapshots {}", plan.delete.len());
        println!("removed_blob_metadata {}", removed.blobs);
        println!("removed_large_metadata {}", removed.large_objects);
        println!("removed_chunk_metadata {}", removed.chunks);
        println!("removed_pack_metadata {}", removed.packs);
        println!("rewritten_pack_indexes {}", removed.pack_indexes);
        println!(
            "removed_snapshot_payload_indexes {}",
            removed.snapshot_payload_indexes
        );
        println!(
            "removed_snapshot_payload_rows {}",
            removed.snapshot_payload_rows
        );
        println!("removed_large_pins {}", removed.large_pins);
        let remote_cleanup = args.remote_cleanup
            && read_config(paths)?
                .remote
                .as_ref()
                .is_some_and(|remote| !remote.url().is_ok_and(|url| url.starts_with("cluster://")));
        if remote_cleanup {
            println!("remote_cleanup true");
        } else {
            println!("remote_cleanup false");
        }
        // syncもmaintenance lockを取得するため、remote cleanupはDB変更と
        // lockの解放後に実行する。local GCも同じlockを取得するため、
        // connを保持したまま呼び出さない。
        drop(conn);
        drop(_maintenance_lock);
        if deleted_snapshots > 0 {
            gc_cmd(paths, GcArgs { dry_run: false })?;
        }
        if remote_cleanup {
            crate::sync_runtime::sync_cmd(
                paths,
                crate::cli::SyncArgs {
                    wait: false,
                    timeout_secs: 300,
                    changed_roots: Vec::new(),
                    retry_queued_only: false,
                    command: None,
                },
            )?;
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
    pub(crate) pack_indexes: usize,
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
    let protected = protected_ref_snapshots(paths, conn, &snapshots)?;
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
        let missing = missing_remote_history_snapshots(paths, conn, current.as_deref())?;
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
    paths: &Paths,
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
    let root_ids = roots(conn)?
        .into_iter()
        .map(|root| root.id)
        .collect::<BTreeSet<_>>();
    for snapshot in first_snapshots_for_roots(paths, conn, &root_ids)?.into_values() {
        if snapshot_ids.contains(&snapshot) {
            protected.insert(snapshot);
        }
    }
    Ok(protected)
}

fn missing_remote_history_snapshots(
    paths: &Paths,
    conn: &Connection,
    current: Option<&str>,
) -> Result<Vec<(String, usize)>> {
    let config = read_config(paths)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(Vec::new());
    };
    let remote = open_remote(remote_config)?;
    if remote.is_master_proxy() {
        bail!(
            "cluster remoteでは--drop-missing-remote-historyを使用できません。master-localとS3の一覧を完全に照合できないため、通常のpruneを使用してください"
        );
    }
    let remote_keys = if remote.is_s3_like() {
        remote_payload_key_index_for_host(
            &remote,
            &crate::majutsu_store::remote_host_label(&config.host.name),
        )?
    } else {
        remote_payload_key_index(&remote)?
    };
    let export = export_metadata(paths, conn, &config)?;
    let mut missing = Vec::new();
    for snapshot in &export.snapshots {
        if current == Some(snapshot.id.as_str()) {
            continue;
        }
        let keys = match crate::object_paths::local_object_keys_for_snapshot(
            paths,
            &export,
            &snapshot.id,
        ) {
            Ok(keys) => keys,
            Err(err) => {
                // 明示的な履歴破棄では、snapshot manifest自体がremoteから失われている
                // 場合に限って、ローカルの古い依存オブジェクト欠損を「履歴欠損」として
                // 扱う。manifestがremoteに残っているのにローカルだけ読めない場合は、
                // ストレージ障害を履歴欠損と誤認して削除しない。
                if !remote_payload_index_contains(&remote_keys, &snapshot.manifest_key) {
                    eprintln!(
                        "remote履歴manifestが見つからないためsnapshotを欠損扱いにします: {} ({err:#})",
                        snapshot.id
                    );
                    missing.push((snapshot.id.clone(), 1));
                    continue;
                }
                return Err(err).with_context(|| {
                    format!("snapshot {}のremote履歴を検証できません", snapshot.id)
                });
            }
        };
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
    let (live, _incomplete_metadata_graph) =
        if let Some(live) = indexed_live_metadata_references(conn)? {
            (live, false)
        } else {
            scan_live_metadata_references(paths, conn, remote.as_ref())?
        };
    let blobs = delete_rows_not_in(conn, "blobs", "oid", &live.blobs)?;
    let large_objects = delete_rows_not_in(conn, "large_objects", "oid", &live.large_objects)?;
    let large_pins = delete_rows_not_in(conn, "large_pins", "oid", &live.large_objects)?;
    let chunks = match live_chunks_from_remaining_large_objects(paths, conn, remote.as_ref())? {
        Some(live_chunks) => delete_rows_not_in(conn, "chunks", "oid", &live_chunks)?,
        None => {
            eprintln!(
                "warning: skipped destructive chunk metadata prune because the metadata graph is incomplete"
            );
            0
        }
    };
    let packs = delete_packs_without_blobs(conn)?;
    let pack_indexes = prune_stale_pack_indexes(paths, conn)?;
    let (snapshot_payload_indexes, snapshot_payload_rows) =
        delete_snapshot_payload_indexes_without_snapshots(conn)?;
    Ok(PrunedMetadata {
        blobs,
        large_objects,
        chunks,
        packs,
        pack_indexes,
        snapshot_payload_indexes,
        snapshot_payload_rows,
        large_pins,
    })
}

fn indexed_live_metadata_references(conn: &Connection) -> Result<Option<LiveMetadataReferences>> {
    let snapshot_count: i64 =
        conn.query_row("select count(*) from snapshots", [], |row| row.get(0))?;
    let indexed_count: i64 = conn.query_row(
        "select count(*) from snapshots
         inner join snapshot_payload_index on snapshot_payload_index.snapshot_id=snapshots.id",
        [],
        |row| row.get(0),
    )?;
    if indexed_count != snapshot_count {
        return Ok(None);
    }

    let payloads = {
        let mut stmt = conn.prepare(
            "select distinct snapshot_payloads.kind, snapshot_payloads.oid
             from snapshot_payloads
             inner join snapshots on snapshots.id=snapshot_payloads.snapshot_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut live = LiveMetadataReferences::default();
    for (kind, oid) in payloads {
        match kind.as_str() {
            "blob" => {
                live.blobs.insert(oid);
            }
            "large" => {
                live.large_objects.insert(oid);
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(live))
}

fn scan_live_metadata_references(
    paths: &Paths,
    conn: &Connection,
    remote: Option<&RemoteStore>,
) -> Result<(LiveMetadataReferences, bool)> {
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
            match read_large_manifest_for_prune(paths, remote, &manifest_key) {
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
                    match read_large_manifest_for_prune(paths, remote, manifest_key) {
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
    Ok((live, incomplete_metadata_graph))
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
    // cluster／S3のhost prefixと旧形式の差を共通の復号経路で吸収する。
    let variants = crate::remote_local_object_variants(paths, remote, manifest_key)?;
    for variant in variants {
        let decoded = crate::decode_object(paths, &variant.bytes)?;
        if let Ok(manifest) = serde_json::from_slice(&decoded) {
            return Ok(manifest);
        }
    }
    anyhow::bail!("pruneに必要なlarge manifestを取得できません: {manifest_key}")
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
    let pack_ids = {
        let mut stmt = conn.prepare("select pack_id from packs")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut removed = 0usize;
    for pack_id in pack_ids {
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

fn prune_stale_pack_indexes(paths: &Paths, conn: &Connection) -> Result<usize> {
    let mut stmt = conn
        .prepare("select pack_id, pack_key, index_key, object_count from packs order by pack_id")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, usize>(3)?,
        ))
    })?;
    let packs = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    let mut rewritten = 0usize;
    for (pack_id, pack_key, index_key, object_count) in packs {
        let mut blob_stmt =
            conn.prepare("select oid from blobs where pack_id=?1 order by pack_offset, oid")?;
        let rows = blob_stmt.query_map(params![&pack_id], |row| row.get::<_, String>(0))?;
        let live_oids = rows.collect::<std::result::Result<BTreeSet<_>, _>>()?;
        if live_oids.is_empty() {
            continue;
        }
        let mut index: PackIndex = serde_json::from_slice(
            &read_object(paths, &index_key)
                .with_context(|| format!("prune pack={pack_id} index={index_key}"))?,
        )
        .with_context(|| format!("prune pack indexの解析: {index_key}"))?;
        let before = index.entries.len();
        index.pack_id = pack_id.clone();
        index.pack_key = pack_key.clone();
        index.entries.retain(|entry| live_oids.contains(&entry.oid));
        if index.entries.len() == before && object_count == index.entries.len() {
            continue;
        }
        // 旧indexを上書きしない。DBがrollbackしても旧世代を引き続き復旧に使える。
        let bytes = serde_json::to_vec_pretty(&index)?;
        let digest = blake3::hash(&bytes).to_hex();
        let new_index_key = format!("objects/indexes/pack/{pack_id}-{digest}.json");
        let index_path = paths.home.join(&new_index_key);
        if let Some(parent) = index_path.parent() {
            fs::create_dir_all(parent)?;
        }
        crate::atomic_io::write_atomic(&index_path, &bytes)?;
        conn.execute(
            "update packs set object_count=?2, index_key=?3 where pack_id=?1",
            params![pack_id, index.entries.len(), new_index_key],
        )?;
        rewritten += 1;
    }
    Ok(rewritten)
}

fn delete_rows_not_in(
    conn: &Connection,
    table: &str,
    column: &str,
    live: &BTreeSet<String>,
) -> Result<usize> {
    let ids = {
        let mut stmt = conn.prepare(&format!("select {column} from {table}"))?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut removed = 0usize;
    for id in ids {
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

pub(crate) fn gc_cmd(paths: &Paths, args: GcArgs) -> Result<()> {
    ensure_ready(paths)?;
    let _maintenance_lock =
        acquire_process_lock(&paths.home.join("locks/maintenance.lock"), "maintenance")?;
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
    let queued = queued_local_object_keys(paths)?;
    let queued_count = queued.len();
    let mut referenced = referenced;
    referenced.extend(queued);
    eprintln!(
        "gc progress phase=local-object-scan referenced={} queued_protected={queued_count}",
        referenced.len()
    );
    let all_objects = all_local_object_keys(paths)?;
    let mut removed = 0usize;
    let mut unreferenced_objects = 0usize;
    let mut unreferenced_bytes = 0u64;
    for key in &all_objects {
        if !referenced.contains(key) {
            unreferenced_objects += 1;
            unreferenced_bytes = unreferenced_bytes.saturating_add(
                fs::metadata(paths.home.join(key))
                    .map(|metadata| metadata.len())
                    .unwrap_or(0),
            );
            if args.dry_run {
                continue;
            }
            fs::remove_file(paths.home.join(key))?;
            removed += 1;
            if removed.is_multiple_of(500) {
                eprintln!("gc progress phase=remove-unreferenced removed={removed}");
            }
        }
    }
    if args.dry_run {
        println!("dry_run true");
        println!("local_object_files {}", all_objects.len());
        println!("referenced_objects {}", referenced.len());
        println!("queued_protected_objects {queued_count}");
        println!("unreferenced_objects {unreferenced_objects}");
        println!("unreferenced_bytes {unreferenced_bytes}");
        println!("removed_unreferenced_objects 0");
        println!("compacted_snapshot_manifests 0");
        println!("compacted_snapshot_manifest_objects 0");
        return Ok(());
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

fn queued_local_object_keys(paths: &Paths) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for (_, item) in upload_queue_items(paths)? {
        let Some(source) = item.source.as_deref() else {
            continue;
        };
        let source = normalize_queue_source_path(paths, source);
        let Ok(relative) = source.strip_prefix(&paths.home) else {
            continue;
        };
        let key = path_to_slash(relative);
        if validate_local_object_key(&key).is_ok() {
            keys.insert(key);
        }
    }
    for event in event_journal_records(paths)? {
        for key in [
            event.durable_payload_key.as_deref(),
            event.durable_large_manifest_key.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if validate_local_object_key(key).is_ok() {
                keys.insert(key.to_string());
            }
        }
    }
    Ok(keys)
}

fn normalize_queue_source_path(paths: &Paths, source: &str) -> PathBuf {
    if source.starts_with('/') {
        Path::new("/").join(source.trim_start_matches('/'))
    } else {
        paths.home.join(source)
    }
}

fn compact_snapshot_manifest_metadata(paths: &Paths, conn: &Connection) -> Result<(usize, usize)> {
    let snapshots = snapshot_manifest_rows_for_compaction(conn)?;
    let mut compacted_metadata = 0usize;
    let mut compacted_objects = 0usize;
    for (id, manifest_key, manifest_json) in snapshots {
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

fn snapshot_manifest_rows_for_compaction(
    conn: &Connection,
) -> Result<Vec<(String, String, String)>> {
    let mut stmt =
        conn.prepare("select id, manifest_key, manifest_json from snapshots order by created_at")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
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

#[cfg(test)]
mod tests {
    use super::indexed_live_metadata_references;
    use rusqlite::Connection;

    fn payload_index_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "create table snapshots(id text primary key);
             create table snapshot_payload_index(snapshot_id text primary key, indexed_at text not null);
             create table snapshot_payloads(
                 snapshot_id text not null,
                 kind text not null,
                 oid text not null,
                 primary key(snapshot_id, kind, oid)
             );",
        )
        .unwrap();
        conn
    }

    #[test]
    fn complete_snapshot_payload_index_supplies_live_metadata() {
        let conn = payload_index_db();
        conn.execute_batch(
            "insert into snapshots values ('snap-a'), ('snap-b');
             insert into snapshot_payload_index values ('snap-a', 'now'), ('snap-b', 'now');
             insert into snapshot_payloads values
                 ('snap-a', 'blob', 'blob-a'),
                 ('snap-b', 'large', 'large-b');",
        )
        .unwrap();

        let live = indexed_live_metadata_references(&conn).unwrap().unwrap();
        assert_eq!(live.blobs.into_iter().collect::<Vec<_>>(), ["blob-a"]);
        assert_eq!(
            live.large_objects.into_iter().collect::<Vec<_>>(),
            ["large-b"]
        );
    }

    #[test]
    fn incomplete_snapshot_payload_index_falls_back_to_tree_scan() {
        let conn = payload_index_db();
        conn.execute_batch(
            "insert into snapshots values ('snap-a'), ('snap-b');
             insert into snapshot_payload_index values ('snap-a', 'now');
             insert into snapshot_payloads values ('snap-a', 'blob', 'blob-a');",
        )
        .unwrap();

        assert!(indexed_live_metadata_references(&conn).unwrap().is_none());
    }
}
