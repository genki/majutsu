use anyhow::{Result, anyhow, bail};
use majutsu_core::{RootSnapshot, SnapshotManifest, payload_blob_ref_mut, payload_large_ref_mut};
use rusqlite::params;
use std::collections::BTreeMap;

use crate::cli::KeyCommand;
use crate::config::{Paths, encryption_enabled, read_config};
use crate::object_paths::large_chunk_base_for_key;
use crate::operation_log::record_op;
use crate::snapshot_state::{current_snapshot, snapshot_manifest_from_parts};
use crate::util::blake3_hex;
use crate::{
    build_tree_manifest, create_layout, open_db, query_blobs, query_chunks, query_large_objects,
    random_key_hex, read_blob_payload, read_master_key, read_object, store_bytes, validate_key_hex,
    write_master_key,
};

pub(crate) fn key_cmd(paths: &Paths, command: KeyCommand) -> Result<()> {
    match command {
        KeyCommand::Export => {
            crate::ensure_ready(paths)?;
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
            crate::ensure_ready(paths)?;
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
        let manifest: majutsu_core::LargeManifest =
            serde_json::from_slice(&read_object(paths, &large.manifest_key)?)?;
        large_manifests.insert(large.oid.clone(), manifest);
    }
    let mut snapshots = Vec::new();
    let mut stmt =
        conn.prepare("select id, manifest_key, manifest_json from snapshots order by created_at")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (id, manifest_key, manifest_json) = row?;
        let manifest = snapshot_manifest_from_parts(paths, &id, &manifest_key, &manifest_json)?;
        snapshots.push((id, manifest));
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
        let manifest_key = crate::store_encoded_object_bytes(
            paths,
            &paths.objects,
            &manifest_oid,
            &crate::encode_compact_snapshot_manifest_for_local(paths, &manifest)?,
        )?;
        conn.execute(
            "update snapshots set manifest_key=?2, manifest_json=?3 where id=?1",
            params![snapshot_id, manifest_key, ""],
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
