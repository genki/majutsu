use crate::majutsu_core::{
    RootSnapshot, SnapshotManifest, payload_blob_ref_mut, payload_large_ref_mut,
};
use anyhow::{Result, anyhow, bail};
use rusqlite::params;
use std::collections::BTreeMap;

use crate::cli::KeyCommand;
use crate::config::{Paths, encryption_enabled, read_config};
use crate::object_paths::large_chunk_base_for_key;
use crate::operation_log::record_op;
use crate::snapshot_state::{current_snapshot, snapshot_manifest_from_parts};
use crate::util::blake3_hex;
use crate::{
    build_tree_manifest, create_layout, encode_compact_snapshot_manifest_for_local,
    encode_object_with_master_key_hex, key_rotation_pending_path, key_rotation_previous_path,
    open_db, prepare_tree_manifest_for_storage, query_blobs, query_chunks, query_large_objects,
    random_key_hex, read_blob_payload, read_master_key, read_object,
    store_encoded_object_bytes_with_key_hex, validate_key_hex, write_master_key,
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
    let mut conn = open_db(paths)?;
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
        let manifest: crate::majutsu_core::LargeManifest =
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
    drop(stmt);

    crate::majutsu_crypto::write_master_key(&key_rotation_previous_path(paths), &old_key)?;
    crate::majutsu_crypto::write_master_key(&key_rotation_pending_path(paths), &new_key)?;
    let mode = crate::config::encryption_mode(&config.security)?;
    let mut objects = 0usize;
    let mut blob_keys = BTreeMap::new();
    for blob in &blobs {
        let encoded =
            encode_object_with_master_key_hex(paths, &blob_payloads[&blob.oid], mode, &new_key)?;
        let key = store_encoded_object_bytes_with_key_hex(
            paths,
            &paths.objects,
            &blob.oid,
            &new_key,
            &encoded,
        )?;
        blob_keys.insert(blob.oid.clone(), key);
        objects += 1;
    }
    let mut chunk_keys = BTreeMap::new();
    for chunk in &chunks {
        let encoded =
            encode_object_with_master_key_hex(paths, &chunk_payloads[&chunk.oid], mode, &new_key)?;
        let key = store_encoded_object_bytes_with_key_hex(
            paths,
            &large_chunk_base_for_key(paths, &chunk.object_key),
            &chunk.oid,
            &new_key,
            &encoded,
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
        let encoded = encode_object_with_master_key_hex(paths, &bytes, mode, &new_key)?;
        let key = store_encoded_object_bytes_with_key_hex(
            paths,
            &paths.large_manifests,
            &manifest_oid,
            &new_key,
            &encoded,
        )?;
        large_manifest_keys.insert(large.oid.clone(), key);
        objects += 1;
    }

    let mut snapshots_rewritten = 0usize;
    let mut snapshot_updates = Vec::new();
    for (snapshot_id, mut manifest) in snapshots {
        rewrite_manifest_payload_keys(&mut manifest, &blob_keys, &large_manifest_keys)?;
        manifest.root_trees.clear();
        for (root_id, records) in &manifest.roots {
            let mut tree = build_tree_manifest(root_id, records.clone())?;
            let file_count = records.len();
            prepare_tree_manifest_for_storage(paths, &mut tree)?;
            let tree_json = serde_json::to_vec(&tree)?;
            let tree_oid = blake3_hex(&tree_json);
            let encoded = encode_object_with_master_key_hex(paths, &tree_json, mode, &new_key)?;
            let tree_key = store_encoded_object_bytes_with_key_hex(
                paths,
                &paths.trees,
                &tree_oid,
                &new_key,
                &encoded,
            )?;
            manifest.root_trees.insert(
                root_id.clone(),
                RootSnapshot {
                    tree_id: tree.tree_id,
                    tree_key,
                    file_count,
                },
            );
            objects += 1;
        }
        let manifest_json = serde_json::to_vec_pretty(&manifest)?;
        let manifest_oid = blake3_hex(&manifest_json);
        let manifest_key = store_encoded_object_bytes_with_key_hex(
            paths,
            &paths.objects,
            &manifest_oid,
            &new_key,
            &encode_compact_snapshot_manifest_with_key(paths, &manifest, mode, &new_key)?,
        )?;
        snapshot_updates.push((snapshot_id, manifest_key));
        snapshots_rewritten += 1;
        objects += 1;
    }
    let tx = conn.transaction()?;
    for blob in &blobs {
        let key = blob_keys
            .get(&blob.oid)
            .ok_or_else(|| anyhow!("missing rotated blob key {}", blob.oid))?;
        tx.execute(
            "update blobs set object_key=?2, pack_id=null, pack_offset=null, pack_len=null where oid=?1",
            params![blob.oid, key],
        )?;
    }
    tx.execute("delete from packs", [])?;
    for chunk in &chunks {
        let key = chunk_keys
            .get(&chunk.oid)
            .ok_or_else(|| anyhow!("missing rotated chunk key {}", chunk.oid))?;
        tx.execute(
            "update chunks set object_key=?2 where oid=?1",
            params![chunk.oid, key],
        )?;
    }
    for large in &large_objects {
        let key = large_manifest_keys
            .get(&large.oid)
            .ok_or_else(|| anyhow!("missing rotated large manifest key {}", large.oid))?;
        tx.execute(
            "update large_objects set manifest_key=?2 where oid=?1",
            params![large.oid, key],
        )?;
    }
    for (snapshot_id, manifest_key) in &snapshot_updates {
        tx.execute(
            "update snapshots set manifest_key=?2, manifest_json=?3 where id=?1",
            params![snapshot_id, manifest_key, ""],
        )?;
    }
    record_op(
        &tx,
        "key-rotation",
        current_snapshot(&tx)?.as_deref(),
        current_snapshot(&tx)?.as_deref(),
        Some(&format!("rewrote {objects} objects")),
    )?;
    tx.commit()?;
    write_master_key(paths, &new_key)?;
    let _ = std::fs::remove_file(key_rotation_pending_path(paths));
    let _ = std::fs::remove_file(key_rotation_previous_path(paths));
    Ok(KeyRotationResult {
        objects,
        snapshots: snapshots_rewritten,
        new_key,
    })
}

fn encode_compact_snapshot_manifest_with_key(
    paths: &Paths,
    manifest: &SnapshotManifest,
    mode: crate::majutsu_crypto::EncryptionMode,
    key_hex: &str,
) -> Result<Vec<u8>> {
    let current = encode_compact_snapshot_manifest_for_local(paths, manifest)?;
    let decoded = crate::decode_object(paths, &current)?;
    encode_object_with_master_key_hex(paths, &decoded, mode, key_hex)
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
