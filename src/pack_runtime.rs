use anyhow::{Result, anyhow};
use chrono::Utc;
use majutsu_pack::{PackEntry, PackIndex, PackTier};
use majutsu_store::BlobExport;
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};

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
    let mut conn = open_db(paths)?;
    let blobs = query_blobs(&conn)?
        .into_iter()
        .filter(|blob| blob.pack_id.is_none())
        .collect::<Vec<_>>();
    if blobs.is_empty() {
        println!("packed 0 objects");
        return Ok(());
    }
    let packed = write_tiered_blob_packs(paths, &config.pack, &blobs, |blob| {
        read_object(paths, &blob.object_key)
    })?;
    persist_written_packs(&mut conn, &packed)?;
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
    blobs: &[BlobExport],
    tier: PackTier,
    target_size: u64,
    mut payload_for: F,
) -> Result<Vec<WrittenPack>>
where
    F: FnMut(&BlobExport) -> Result<Vec<u8>>,
{
    let target_size = target_size.max(1);
    let mut indexes = Vec::new();
    let prefixes = majutsu_pack::date_prefixes(tier, Utc::now());
    let mut pack = open_pack(paths, &prefixes.pack_prefix, &prefixes.index_prefix)?;
    for blob in blobs {
        let payload = payload_for(blob)?;
        let stored = encode_object(paths, &payload)?;
        let record_len = 8 + stored.len() as u64;
        if !pack.entries.is_empty() && pack.size + record_len > target_size {
            indexes.push(finish_pack(paths, pack)?);
            pack = open_pack(paths, &prefixes.pack_prefix, &prefixes.index_prefix)?;
        }
        let offset = pack.size;
        pack.writer
            .write_all(&(stored.len() as u64).to_le_bytes())?;
        pack.writer.write_all(&stored)?;
        pack.size += record_len;
        pack.entries.push(PackEntry {
            oid: blob.oid.clone(),
            offset,
            len: 8 + stored.len() as u64,
        });
    }
    if !pack.entries.is_empty() {
        indexes.push(finish_pack(paths, pack)?);
    }
    Ok(indexes)
}

fn write_tiered_blob_packs<F>(
    paths: &Paths,
    config: &PackConfig,
    blobs: &[BlobExport],
    mut payload_for: F,
) -> Result<Vec<WrittenPack>>
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
        &small_blobs,
        PackTier::Small,
        config.small_pack_target,
        |blob| payload_for(blob),
    )?);
    indexes.extend(write_blob_packs(
        paths,
        &normal_blobs,
        PackTier::Normal,
        config.normal_pack_target,
        |blob| payload_for(blob),
    )?);
    Ok(indexes)
}

struct WrittenPack {
    index: PackIndex,
    index_key: String,
    size: u64,
}

struct OpenPack {
    pack_id: String,
    pack_key: String,
    index_key: String,
    entries: Vec<PackEntry>,
    writer: BufWriter<File>,
    size: u64,
}

fn open_pack(paths: &Paths, pack_prefix: &str, index_prefix: &str) -> Result<OpenPack> {
    let pack_id = new_id("pack");
    let pack_key = majutsu_pack::pack_key(pack_prefix, &pack_id);
    let index_key = majutsu_pack::index_key(index_prefix, &pack_id);
    let pack_path = paths.home.join(&pack_key);
    if let Some(parent) = pack_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(&pack_path)?;
    advise_sequential(&file);
    Ok(OpenPack {
        pack_id,
        pack_key,
        index_key,
        entries: Vec::new(),
        writer: BufWriter::with_capacity(1024 * 1024, file),
        size: 0,
    })
}

fn finish_pack(paths: &Paths, mut pack: OpenPack) -> Result<WrittenPack> {
    pack.writer.flush()?;
    advise_dontneed(pack.writer.get_ref());
    let index = PackIndex {
        version: 1,
        pack_id: pack.pack_id.clone(),
        pack_key: pack.pack_key.clone(),
        entries: pack.entries,
    };
    let index_path = paths.home.join(&pack.index_key);
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&index_path, serde_json::to_vec_pretty(&index)?)?;
    Ok(WrittenPack {
        index,
        index_key: pack.index_key,
        size: pack.size,
    })
}

#[cfg(unix)]
fn advise_sequential(file: &File) {
    use std::os::fd::AsRawFd;
    let _ = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL) };
}

#[cfg(not(unix))]
fn advise_sequential(_file: &File) {}

#[cfg(unix)]
fn advise_dontneed(file: &File) {
    use std::os::fd::AsRawFd;
    let _ = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
}

#[cfg(not(unix))]
fn advise_dontneed(_file: &File) {}

fn persist_written_packs(conn: &mut Connection, packs: &[WrittenPack]) -> Result<()> {
    let tx = conn.transaction()?;
    for pack in packs {
        tx.execute(
            "insert or replace into packs(pack_id, pack_key, index_key, object_count, size) values (?1, ?2, ?3, ?4, ?5)",
            params![
                pack.index.pack_id.as_str(),
                pack.index.pack_key.as_str(),
                pack.index_key.as_str(),
                pack.index.entries.len(),
                pack.size,
            ],
        )?;
        for entry in &pack.index.entries {
            tx.execute(
                "update blobs set pack_id=?2, pack_offset=?3, pack_len=?4 where oid=?1",
                params![
                    entry.oid.as_str(),
                    pack.index.pack_id.as_str(),
                    entry.offset,
                    entry.len
                ],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn pack_compact_cmd(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let mut conn = open_db(paths)?;
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
    let indexes = write_tiered_blob_packs(paths, &config.pack, &blobs, |blob| {
        payloads
            .get(&blob.oid)
            .cloned()
            .ok_or_else(|| anyhow!("missing compact payload {}", blob.oid))
    })?;
    persist_written_packs(&mut conn, &indexes)?;
    let new_pack_ids = indexes
        .iter()
        .map(|pack| pack.index.pack_id.clone())
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
