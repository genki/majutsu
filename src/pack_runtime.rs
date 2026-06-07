use anyhow::{Result, anyhow};
use chrono::Utc;
use majutsu_pack::{PackEntry, PackIndex, PackTier};
use majutsu_store::BlobExport;
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use crate::cli::PackArgs;
use crate::config::{PackConfig, Paths, read_config};
use crate::operation_log::record_op;
use crate::snapshot_state::current_snapshot;
use crate::util::new_id;
use crate::{
    encode_object, ensure_ready, open_db, query_blobs, query_packs, read_blob_payload, read_object,
};

pub(crate) fn pack_cmd(paths: &Paths, args: PackArgs) -> Result<()> {
    if args.compact {
        return pack_compact_cmd(paths);
    }
    pack_loose_blobs(paths)
}

fn pack_loose_blobs(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let blobs = query_blobs(&conn)?
        .into_iter()
        .filter(|blob| blob.pack_id.is_none())
        .collect::<Vec<_>>();
    if blobs.is_empty() {
        println!("packed 0 objects");
        return Ok(());
    }
    let packed = write_tiered_blob_packs(paths, &conn, &config.pack, &blobs, |blob| {
        read_object(paths, &blob.object_key)
    })?;
    record_op(
        &conn,
        "pack",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!("packed {} blobs", blobs.len())),
    )?;
    println!(
        "packed {} objects into {} pack(s)",
        blobs.len(),
        packed.len()
    );
    Ok(())
}

fn write_blob_packs<F>(
    paths: &Paths,
    conn: &Connection,
    blobs: &[BlobExport],
    tier: PackTier,
    target_size: u64,
    mut payload_for: F,
) -> Result<Vec<PackIndex>>
where
    F: FnMut(&BlobExport) -> Result<Vec<u8>>,
{
    let target_size = target_size.max(1);
    let mut indexes = Vec::new();
    let mut pack_id = new_id("pack");
    let prefixes = majutsu_pack::date_prefixes(tier, Utc::now());
    let mut pack_key = majutsu_pack::pack_key(&prefixes.pack_prefix, &pack_id);
    let mut index_key = majutsu_pack::index_key(&prefixes.index_prefix, &pack_id);
    let mut pack_bytes = Vec::new();
    let mut entries = Vec::new();
    let mut object_count = 0usize;
    for blob in blobs {
        let payload = payload_for(blob)?;
        let stored = encode_object(paths, &payload)?;
        let record_len = 8 + stored.len() as u64;
        if !entries.is_empty() && pack_bytes.len() as u64 + record_len > target_size {
            indexes.push(finish_pack(
                paths,
                conn,
                pack_id,
                pack_key,
                index_key,
                pack_bytes,
                entries,
                object_count,
            )?);
            pack_id = new_id("pack");
            pack_key = majutsu_pack::pack_key(&prefixes.pack_prefix, &pack_id);
            index_key = majutsu_pack::index_key(&prefixes.index_prefix, &pack_id);
            pack_bytes = Vec::new();
            entries = Vec::new();
            object_count = 0;
        }
        let offset = pack_bytes.len() as u64;
        pack_bytes.extend_from_slice(&(stored.len() as u64).to_le_bytes());
        pack_bytes.extend_from_slice(&stored);
        entries.push(PackEntry {
            oid: blob.oid.clone(),
            offset,
            len: 8 + stored.len() as u64,
        });
        object_count += 1;
    }
    if !entries.is_empty() {
        indexes.push(finish_pack(
            paths,
            conn,
            pack_id,
            pack_key,
            index_key,
            pack_bytes,
            entries,
            object_count,
        )?);
    }
    Ok(indexes)
}

fn write_tiered_blob_packs<F>(
    paths: &Paths,
    conn: &Connection,
    config: &PackConfig,
    blobs: &[BlobExport],
    mut payload_for: F,
) -> Result<Vec<PackIndex>>
where
    F: FnMut(&BlobExport) -> Result<Vec<u8>>,
{
    let (small_blobs, normal_blobs): (Vec<_>, Vec<_>) = blobs
        .iter()
        .cloned()
        .partition(|blob| majutsu_pack::tier_for_blob(blob.size) == PackTier::Small);
    let mut indexes = Vec::new();
    indexes.extend(write_blob_packs(
        paths,
        conn,
        &small_blobs,
        PackTier::Small,
        config.small_pack_target,
        |blob| payload_for(blob),
    )?);
    indexes.extend(write_blob_packs(
        paths,
        conn,
        &normal_blobs,
        PackTier::Normal,
        config.normal_pack_target,
        |blob| payload_for(blob),
    )?);
    Ok(indexes)
}

fn finish_pack(
    paths: &Paths,
    conn: &Connection,
    pack_id: String,
    pack_key: String,
    index_key: String,
    pack_bytes: Vec<u8>,
    entries: Vec<PackEntry>,
    object_count: usize,
) -> Result<PackIndex> {
    let pack_path = paths.home.join(&pack_key);
    if let Some(parent) = pack_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&pack_path, &pack_bytes)?;
    let index = PackIndex {
        version: 1,
        pack_id: pack_id.clone(),
        pack_key: pack_key.clone(),
        entries,
    };
    let index_path = paths.home.join(&index_key);
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&index_path, serde_json::to_vec_pretty(&index)?)?;
    conn.execute(
        "insert or replace into packs(pack_id, pack_key, index_key, object_count, size) values (?1, ?2, ?3, ?4, ?5)",
        params![pack_id, pack_key, index_key, object_count, pack_bytes.len() as u64],
    )?;
    for entry in &index.entries {
        conn.execute(
            "update blobs set pack_id=?2, pack_offset=?3, pack_len=?4 where oid=?1",
            params![entry.oid, index.pack_id, entry.offset, entry.len],
        )?;
    }
    Ok(index)
}

fn pack_compact_cmd(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let blobs = query_blobs(&conn)?;
    let packed = blobs.iter().filter(|blob| blob.pack_id.is_some()).count();
    if packed == 0 {
        println!("compacted 0 objects");
        return Ok(());
    }
    let old_pack_ids = query_packs(&conn)?
        .into_iter()
        .map(|pack| pack.pack_id)
        .collect::<BTreeSet<_>>();
    let mut payloads = BTreeMap::new();
    for blob in &blobs {
        payloads.insert(
            blob.oid.clone(),
            read_blob_payload(paths, &conn, &blob.oid, &blob.object_key)?,
        );
    }
    let indexes = write_tiered_blob_packs(paths, &conn, &config.pack, &blobs, |blob| {
        payloads
            .get(&blob.oid)
            .cloned()
            .ok_or_else(|| anyhow!("missing compact payload {}", blob.oid))
    })?;
    let new_pack_ids = indexes
        .iter()
        .map(|index| index.pack_id.clone())
        .collect::<BTreeSet<_>>();
    for old_pack_id in old_pack_ids.difference(&new_pack_ids) {
        conn.execute("delete from packs where pack_id=?1", params![old_pack_id])?;
    }
    record_op(
        &conn,
        "pack-compact",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!("compacted {} blobs", blobs.len())),
    )?;
    println!(
        "compacted {} objects into {} pack(s)",
        blobs.len(),
        indexes.len()
    );
    Ok(())
}
