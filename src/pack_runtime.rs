use crate::majutsu_pack::{PackEntry, PackExport, PackIndex, PackTier};
use crate::majutsu_store::BlobExport;
use anyhow::{Result, anyhow};
use chrono::Utc;
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};

use crate::cli::PackArgs;
use crate::config::{PackConfig, Paths, read_config};
use crate::majutsu_core::{FileRecord, payload_blob_ref};
use crate::operation_log::record_op;
use crate::snapshot_state::{current_snapshot, load_snapshot_by_id};
use crate::util::new_id;
use crate::{
    decode_object, encode_object, ensure_ready, open_db, pack_entry_payload, query_blobs,
    query_packs, read_blob_payload, read_object,
};

pub(crate) fn pack_cmd(paths: &Paths, args: PackArgs) -> Result<()> {
    if args.compact {
        return pack_compact_cmd(paths);
    }
    pack_loose_blobs(paths)
}

pub(crate) fn auto_compact_packs_if_needed(paths: &Paths, conn: &Connection) -> Result<bool> {
    if env::var("MAJUTSU_SYNC_AUTO_PACK_COMPACT").as_deref() == Ok("0") {
        return Ok(false);
    }
    let Some(stats) = pack_compaction_stats(paths, conn)? else {
        return Ok(false);
    };
    let reclaim_bytes = stats
        .all_pack_bytes
        .saturating_sub(stats.all_slice_bytes)
        .max(
            stats
                .current_pack_bytes
                .saturating_sub(stats.current_slice_bytes),
        );
    if reclaim_bytes < auto_compact_min_reclaim_bytes() {
        return Ok(false);
    }
    let all_utilization = utilization_percent(stats.all_slice_bytes, stats.all_pack_bytes);
    let current_utilization =
        utilization_percent(stats.current_slice_bytes, stats.current_pack_bytes);
    if all_utilization > auto_compact_max_utilization_percent()
        && current_utilization > auto_compact_max_utilization_percent()
    {
        return Ok(false);
    }
    eprintln!(
        "auto_pack_compact packs={} reclaim_bytes={} all_utilization={} current_utilization={}",
        stats.pack_count, reclaim_bytes, all_utilization, current_utilization
    );
    pack_compact_cmd(paths)?;
    Ok(true)
}

fn pack_loose_blobs(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let mut conn = open_db(paths)?;
    let loose_blobs = query_blobs(&conn)?
        .into_iter()
        .filter(|blob| blob.pack_id.is_none())
        .collect::<Vec<_>>();
    let (blobs, missing_blobs): (Vec<_>, Vec<_>) = loose_blobs
        .into_iter()
        .partition(|blob| paths.home.join(&blob.object_key).is_file());
    if !missing_blobs.is_empty() {
        let removed = remove_unreferenced_missing_loose_blobs(&mut conn, &missing_blobs)?;
        println!("skipped_missing_loose_blobs {}", missing_blobs.len());
        if removed > 0 {
            println!("removed_missing_unreferenced_blobs {removed}");
        }
    }
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

fn remove_unreferenced_missing_loose_blobs(
    conn: &mut Connection,
    missing_blobs: &[BlobExport],
) -> Result<usize> {
    let tx = conn.transaction()?;
    let mut removed = 0;
    for blob in missing_blobs {
        removed += tx.execute(
            "delete from blobs
             where oid=?1
               and pack_id is null
               and not exists (
                 select 1 from snapshot_payloads
                 where snapshot_payloads.kind='blob'
                   and snapshot_payloads.oid=blobs.oid
               )",
            params![blob.oid.as_str()],
        )?;
    }
    tx.commit()?;
    Ok(removed)
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
    let prefixes = crate::majutsu_pack::date_prefixes(tier, Utc::now());
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
        .partition(|blob| crate::majutsu_pack::tier_for_blob(blob.size) == PackTier::Small);
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
    let pack_key = crate::majutsu_pack::pack_key(pack_prefix, &pack_id);
    let index_key = crate::majutsu_pack::index_key(index_prefix, &pack_id);
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

#[cfg(any(target_os = "android", target_os = "linux"))]
fn advise_sequential(file: &File) {
    use std::os::fd::AsRawFd;
    let _ = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL) };
}

#[cfg(not(any(target_os = "android", target_os = "linux")))]
fn advise_sequential(_file: &File) {}

#[cfg(any(target_os = "android", target_os = "linux"))]
fn advise_dontneed(file: &File) {
    use std::os::fd::AsRawFd;
    let _ = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
}

#[cfg(not(any(target_os = "android", target_os = "linux")))]
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
    let packs = query_packs(&conn)?;
    let packs_by_id = packs
        .iter()
        .map(|pack| (pack.pack_id.clone(), pack.clone()))
        .collect::<BTreeMap<_, _>>();
    let old_pack_ids = packs
        .into_iter()
        .map(|pack| pack.pack_id)
        .collect::<BTreeSet<_>>();
    let current_blob_oids = current_snapshot_blob_oids(paths, &conn)?;
    let (mut current_blobs, mut history_blobs): (Vec<_>, Vec<_>) = blobs
        .into_iter()
        .partition(|blob| current_blob_oids.contains(&blob.oid));
    sort_blobs_for_compaction(&mut current_blobs);
    sort_blobs_for_compaction(&mut history_blobs);
    eprintln!(
        "compact reading {} blob(s), {} currently packed",
        current_blobs.len() + history_blobs.len(),
        packed
    );
    let mut reader = CompactPayloadReader::new(paths, &packs_by_id);
    let mut read_count = 0usize;
    let total_blobs = current_blobs.len() + history_blobs.len();
    let mut indexes = Vec::new();
    indexes.extend(write_tiered_blob_packs(
        paths,
        &config.pack,
        &current_blobs,
        |blob| {
            read_count += 1;
            if read_count == 1 || read_count.is_multiple_of(500) || read_count == total_blobs {
                eprintln!("compact read progress {}/{}", read_count, total_blobs);
            }
            reader.read_blob(paths, &conn, blob)
        },
    )?);
    indexes.extend(write_tiered_blob_packs(
        paths,
        &config.pack,
        &history_blobs,
        |blob| {
            read_count += 1;
            if read_count == 1 || read_count.is_multiple_of(500) || read_count == total_blobs {
                eprintln!("compact read progress {}/{}", read_count, total_blobs);
            }
            reader.read_blob(paths, &conn, blob)
        },
    )?);
    eprintln!(
        "compact current_blobs={} history_blobs={}",
        current_blobs.len(),
        history_blobs.len()
    );
    eprintln!("compact wrote {} pack(s)", indexes.len());
    let compacted_blob_count = total_blobs;
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
        Some(&format!("compacted {} blobs", compacted_blob_count)),
    )?;
    println!(
        "compacted {} objects into {} pack(s)",
        compacted_blob_count,
        indexes.len()
    );
    Ok(())
}

fn sort_blobs_for_compaction(blobs: &mut [BlobExport]) {
    blobs.sort_by(|left, right| {
        left.pack_id
            .cmp(&right.pack_id)
            .then_with(|| left.pack_offset.cmp(&right.pack_offset))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}

fn current_snapshot_blob_oids(paths: &Paths, conn: &Connection) -> Result<BTreeSet<String>> {
    let Some(current) = current_snapshot(conn)? else {
        return Ok(BTreeSet::new());
    };
    let manifest = load_snapshot_by_id(paths, conn, &current)?;
    let mut oids = BTreeSet::new();
    for root_tree in manifest.root_trees.values() {
        crate::snapshot_state::visit_tree_records(paths, root_tree, |record| {
            add_record_blob_oid(record, &mut oids);
            Ok(())
        })?;
    }
    for records in manifest.roots.values() {
        for record in records {
            add_record_blob_oid(record, &mut oids);
        }
    }
    Ok(oids)
}

fn add_record_blob_oid(record: &FileRecord, oids: &mut BTreeSet<String>) {
    if let Some((oid, _)) = payload_blob_ref(&record.payload) {
        oids.insert(oid.to_string());
    }
}

struct PackCompactionStats {
    pack_count: usize,
    all_pack_bytes: u64,
    all_slice_bytes: u64,
    current_pack_bytes: u64,
    current_slice_bytes: u64,
}

fn pack_compaction_stats(paths: &Paths, conn: &Connection) -> Result<Option<PackCompactionStats>> {
    let packs = query_packs(conn)?;
    if packs.len() < 2 {
        return Ok(None);
    }
    let blobs = query_blobs(conn)?;
    let current_oids = current_snapshot_blob_oids(paths, conn)?;
    let pack_size_by_id = packs
        .iter()
        .map(|pack| (pack.pack_id.as_str(), pack.size))
        .collect::<BTreeMap<_, _>>();
    let all_pack_bytes = packs
        .iter()
        .fold(0_u64, |sum, pack| sum.saturating_add(pack.size));
    let mut all_slice_bytes = 0_u64;
    let mut current_pack_ids = BTreeSet::new();
    let mut current_slice_bytes = 0_u64;
    for blob in blobs {
        let Some(pack_id) = blob.pack_id.as_deref() else {
            continue;
        };
        let Some(pack_len) = blob.pack_len else {
            continue;
        };
        all_slice_bytes = all_slice_bytes.saturating_add(pack_len);
        if current_oids.contains(&blob.oid) {
            current_pack_ids.insert(pack_id.to_string());
            current_slice_bytes = current_slice_bytes.saturating_add(pack_len);
        }
    }
    let current_pack_bytes = current_pack_ids
        .iter()
        .filter_map(|pack_id| pack_size_by_id.get(pack_id.as_str()))
        .fold(0_u64, |sum, size| sum.saturating_add(*size));
    Ok(Some(PackCompactionStats {
        pack_count: packs.len(),
        all_pack_bytes,
        all_slice_bytes,
        current_pack_bytes,
        current_slice_bytes,
    }))
}

fn utilization_percent(used: u64, total: u64) -> u64 {
    if total == 0 {
        return 100;
    }
    used.saturating_mul(100) / total
}

fn auto_compact_min_reclaim_bytes() -> u64 {
    env::var("MAJUTSU_SYNC_AUTO_PACK_COMPACT_MIN_RECLAIM_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(32 * 1024 * 1024)
}

fn auto_compact_max_utilization_percent() -> u64 {
    env::var("MAJUTSU_SYNC_AUTO_PACK_COMPACT_MAX_UTILIZATION_PERCENT")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(70)
}

struct CompactPayloadReader<'a> {
    paths: &'a Paths,
    packs_by_id: &'a BTreeMap<String, PackExport>,
    current_pack_id: Option<String>,
    current_pack_bytes: Vec<u8>,
}

impl<'a> CompactPayloadReader<'a> {
    fn new(paths: &'a Paths, packs_by_id: &'a BTreeMap<String, PackExport>) -> Self {
        Self {
            paths,
            packs_by_id,
            current_pack_id: None,
            current_pack_bytes: Vec::new(),
        }
    }

    fn read_blob(
        &mut self,
        paths: &Paths,
        conn: &Connection,
        blob: &BlobExport,
    ) -> Result<Vec<u8>> {
        let Some(pack_id) = blob.pack_id.as_deref() else {
            return read_object(paths, &blob.object_key);
        };
        let Some(pack) = self.packs_by_id.get(pack_id) else {
            return read_blob_payload(paths, conn, &blob.oid, &blob.object_key);
        };
        if !self.paths.home.join(&pack.pack_key).exists() {
            if self.paths.home.join(&blob.object_key).exists() {
                return read_object(paths, &blob.object_key);
            }
            return read_blob_payload(paths, conn, &blob.oid, &blob.object_key);
        }
        if self.current_pack_id.as_deref() != Some(pack_id) {
            self.current_pack_bytes = fs::read(self.paths.home.join(&pack.pack_key))?;
            self.current_pack_id = Some(pack_id.to_string());
        }
        let offset = blob
            .pack_offset
            .ok_or_else(|| anyhow!("missing pack offset for {}", blob.oid))?
            as usize;
        let len =
            blob.pack_len
                .ok_or_else(|| anyhow!("missing pack len for {}", blob.oid))? as usize;
        let slice = self
            .current_pack_bytes
            .get(offset..offset + len)
            .ok_or_else(|| anyhow!("pack entry out of range for {}", blob.oid))?;
        decode_object(paths, pack_entry_payload(&blob.oid, slice)?)
    }
}
