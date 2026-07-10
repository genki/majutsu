use crate::majutsu_core::{FileRecord, LargeManifest, Payload, SnapshotManifest};
use crate::majutsu_db::{EventJournalRecord, UploadQueueItem, expected_upload_queue_item_id};
use crate::majutsu_store::{
    canonical_remote_aliases, host_remote_key, is_content_addressed_remote_key,
};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration as StdDuration, Instant};
use walkdir::WalkDir;

use crate::atomic_io::write_atomic;
use crate::cli::EventCommand;
use crate::config::{Config, Paths, RootConfig};
use crate::object_paths::prefer_canonical_remote_only;
use crate::remote_store::RemoteStore;
use crate::snapshot_rules::{
    build_ignore, classify_large, effective_large_config, looks_binary, root_dir_allows_descend,
    root_record_is_managed,
};
use crate::snapshot_state::{current_snapshot, load_root_tree_entries, load_snapshot_by_id};
use crate::util::{blake3_hex, new_id, path_to_slash, stable_read, stable_read_in_root};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UploadQueueStats {
    pub(crate) total: usize,
    pub(crate) retrying: usize,
    pub(crate) delayed: usize,
    pub(crate) attempts: u64,
    pub(crate) max_attempts: u32,
    pub(crate) next_retry_after: Option<DateTime<Utc>>,
}

impl UploadQueueStats {
    pub(crate) fn has_backpressure(&self) -> bool {
        self.total > 0 && self.max_attempts > 0
    }
}

pub(crate) fn enqueue_inline_upload(paths: &Paths, key: &str, bytes: Vec<u8>) -> Result<()> {
    enqueue_inline_upload_with_overwrite(paths, key, bytes, false)
}

pub(crate) fn enqueue_inline_upload_overwrite(
    paths: &Paths,
    key: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    enqueue_inline_upload_with_overwrite(paths, key, bytes, true)
}

fn enqueue_inline_upload_with_overwrite(
    paths: &Paths,
    key: &str,
    bytes: Vec<u8>,
    overwrite: bool,
) -> Result<()> {
    let id = expected_upload_queue_item_id(key);
    let payload_path = upload_payload_path(paths, &id);
    if let Some(parent) = payload_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = payload_path.with_extension("tmp");
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, &payload_path)?;
    write_upload_item(
        paths,
        UploadQueueItem::file(
            id,
            key.to_string(),
            path_to_slash(&payload_path),
            Utc::now(),
        )
        .with_overwrite(overwrite),
    )
}

pub(crate) fn enqueue_file_upload(paths: &Paths, key: &str, source: &Path) -> Result<()> {
    enqueue_file_upload_with_overwrite(paths, key, source, false)
}

pub(crate) fn enqueue_file_upload_overwrite(paths: &Paths, key: &str, source: &Path) -> Result<()> {
    enqueue_file_upload_with_overwrite(paths, key, source, true)
}

fn enqueue_file_upload_with_overwrite(
    paths: &Paths,
    key: &str,
    source: &Path,
    overwrite: bool,
) -> Result<()> {
    remove_upload_payload(paths, &expected_upload_queue_item_id(key))?;
    write_upload_item(
        paths,
        UploadQueueItem::file(
            expected_upload_queue_item_id(key),
            key.to_string(),
            path_to_slash(source),
            Utc::now(),
        )
        .with_overwrite(overwrite),
    )
}

pub(crate) fn write_upload_item(paths: &Paths, item: UploadQueueItem) -> Result<()> {
    fs::create_dir_all(&paths.upload_queue)?;
    let path = paths.upload_queue.join(format!("{}.json", item.id));
    let item = if path.exists() {
        match fs::read(&path)
            .ok()
            .filter(|bytes| !bytes.is_empty())
            .and_then(|bytes| serde_json::from_slice::<UploadQueueItem>(&bytes).ok())
        {
            Some(existing) => item.preserve_retry_state(&existing),
            None => item,
        }
    } else {
        item
    };
    write_atomic(&path, &serde_json::to_vec_pretty(&item)?)?;
    Ok(())
}

pub(crate) fn upload_queue_contains_key(paths: &Paths, key: &str) -> bool {
    paths
        .upload_queue
        .join(format!("{}.json", expected_upload_queue_item_id(key)))
        .exists()
}

pub(crate) fn upload_queue_items(paths: &Paths) -> Result<Vec<(PathBuf, UploadQueueItem)>> {
    if !paths.upload_queue.exists() {
        return Ok(Vec::new());
    }
    crate::atomic_io::cleanup_stale_atomic_temps(
        &paths.upload_queue,
        StdDuration::from_secs(60 * 60),
    )?;
    let mut items = Vec::new();
    for entry in fs::read_dir(&paths.upload_queue)? {
        let entry = entry?;
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if !file_type.is_file() || entry.path().extension().and_then(OsStr::to_str) != Some("json")
        {
            continue;
        }
        let bytes = match fs::read(entry.path()) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if bytes.is_empty() {
            discard_upload_queue_file(paths, &entry.path());
            continue;
        }
        let item: UploadQueueItem = match serde_json::from_slice(&bytes) {
            Ok(item) => item,
            Err(_) => {
                discard_upload_queue_file(paths, &entry.path());
                continue;
            }
        };
        items.push((entry.path(), item));
    }
    items.sort_by(|a, b| a.1.key.cmp(&b.1.key));
    Ok(items)
}

fn discard_upload_queue_file(paths: &Paths, path: &Path) {
    if let Some(stem) = path.file_stem().and_then(OsStr::to_str) {
        let _ = remove_upload_payload(paths, stem);
    }
    let _ = fs::remove_file(path);
}

pub(crate) fn upload_queue_stats(paths: &Paths) -> Result<UploadQueueStats> {
    let items = upload_queue_items(paths)?;
    let mut stats = UploadQueueStats {
        total: items.len(),
        retrying: 0,
        delayed: 0,
        attempts: 0,
        max_attempts: 0,
        next_retry_after: None,
    };
    let now = Utc::now();
    for (_, item) in items {
        if item.attempts > 0 {
            stats.retrying += 1;
        }
        if let Some(retry_after) = item.retry_after
            && retry_after > now
        {
            stats.delayed += 1;
            stats.next_retry_after = stats
                .next_retry_after
                .map(|current| current.min(retry_after))
                .or(Some(retry_after));
        }
        stats.attempts += u64::from(item.attempts);
        stats.max_attempts = stats.max_attempts.max(item.attempts);
    }
    Ok(stats)
}

pub(crate) fn drain_upload_queue(
    paths: &Paths,
    remote: &RemoteStore,
    max_parallel_uploads: usize,
) -> Result<UploadDrainStats> {
    let mut stats = UploadDrainStats::default();
    let mut items = upload_queue_items(paths)?;
    items.sort_by(|a, b| {
        upload_publish_priority(&a.1.key)
            .cmp(&upload_publish_priority(&b.1.key))
            .then_with(|| a.1.key.cmp(&b.1.key))
    });
    let total = items.len();
    let progress_enabled = total > 0;
    let parallelism = upload_queue_parallelism(remote, max_parallel_uploads);
    let mut last_progress = Instant::now()
        .checked_sub(StdDuration::from_secs(10))
        .unwrap_or_else(Instant::now);
    for batch in items.chunks(parallelism) {
        if progress_enabled
            && (stats.uploaded == 0
                || stats.uploaded % 25 == 0
                || last_progress.elapsed() >= StdDuration::from_secs(5))
        {
            let key = batch
                .first()
                .map(|(_, item)| item.key.as_str())
                .unwrap_or("(none)");
            eprintln!(
                "sync upload progress {}/{} key={key}",
                stats.uploaded, total
            );
            last_progress = Instant::now();
        }

        let results = std::thread::scope(|scope| {
            let handles = batch
                .iter()
                .map(|(path, item)| {
                    scope.spawn(move || upload_queue_item(paths, remote, path, item.clone()))
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .map_err(|_| anyhow::anyhow!("upload queue worker panicked"))?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        for result in results {
            match result {
                UploadQueueItemResult::Uploaded { bytes } => {
                    stats.uploaded += 1;
                    stats.uploaded_bytes += bytes;
                }
                UploadQueueItemResult::Skipped => {
                    stats.skipped += 1;
                }
            }
        }
    }
    if progress_enabled {
        eprintln!("sync upload progress {}/{} done", stats.uploaded, total);
    }
    Ok(stats)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadDrainStats {
    pub(crate) uploaded: usize,
    pub(crate) uploaded_bytes: u64,
    pub(crate) skipped: usize,
}

enum UploadQueueItemResult {
    Uploaded { bytes: u64 },
    Skipped,
}

fn upload_queue_item(
    paths: &Paths,
    remote: &RemoteStore,
    path: &Path,
    item: UploadQueueItem,
) -> Result<UploadQueueItemResult> {
    if !item.overwrite && queued_upload_can_be_skipped(paths, remote, &item) {
        fs::remove_file(path)?;
        return Ok(UploadQueueItemResult::Skipped);
    }
    let upload_bytes = upload_item_payload_size(&item).unwrap_or(0);
    let upload_result = if let Some(bytes) = item.inline.clone() {
        if !item.overwrite
            && is_content_addressed_remote_key(&item.key)
            && remote.capabilities().conditional_put
        {
            remote.put_if_absent(&item.key, &bytes).map(|_| ())
        } else {
            remote.put(&item.key, &bytes)
        }
    } else if let Some(source) = &item.source {
        let source = Path::new(source);
        if !item.overwrite
            && is_content_addressed_remote_key(&item.key)
            && remote.capabilities().conditional_put
        {
            remote.put_file_if_absent(&item.key, source).map(|_| ())
        } else {
            remote.put_file(&item.key, source)
        }
    } else {
        bail!(
            "queued upload has neither inline payload nor source: {}",
            item.key
        );
    };
    match upload_result {
        Ok(()) => {
            remove_upload_payload(paths, &item.id)?;
            fs::remove_file(path)?;
            Ok(UploadQueueItemResult::Uploaded {
                bytes: upload_bytes,
            })
        }
        Err(err) => {
            let mut item = item;
            let next_attempt = item.attempts.saturating_add(1);
            item.record_attempt(Some(next_retry_after(Utc::now(), next_attempt)));
            write_atomic(path, &serde_json::to_vec_pretty(&item)?)?;
            Err(err).with_context(|| format!("upload failed for {}", item.key))
        }
    }
}

fn upload_item_payload_size(item: &UploadQueueItem) -> Result<u64> {
    if let Some(bytes) = &item.inline {
        return Ok(bytes.len() as u64);
    }
    if let Some(source) = &item.source {
        return Ok(fs::metadata(source)?.len());
    }
    Ok(0)
}

fn upload_queue_parallelism(remote: &RemoteStore, max_parallel_uploads: usize) -> usize {
    let configured = std::env::var("MAJUTSU_UPLOAD_QUEUE_PARALLELISM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| match remote {
            RemoteStore::S3(_) => max_parallel_uploads.max(32),
            RemoteStore::File(_) => max_parallel_uploads,
        });
    match remote {
        RemoteStore::S3(_) => configured.max(1),
        RemoteStore::File(_) => 1,
    }
}

fn upload_publish_priority(key: &str) -> u8 {
    if key.starts_with("refs/") || key.contains("/refs/") {
        return 3;
    }
    if key.ends_with("/current")
        || key.contains("/refs/current")
        || key.ends_with("/head.cbor.zst.enc")
    {
        return 3;
    }
    if key.starts_with("metadata/")
        || key.ends_with("/metadata/export.json")
        || key == "metadata/export.json"
    {
        return 2;
    }
    1
}

fn queued_upload_can_be_skipped(
    paths: &Paths,
    remote: &RemoteStore,
    item: &UploadQueueItem,
) -> bool {
    if !prefer_canonical_remote_only(&item.key) {
        return false;
    }
    let alias_exists = canonical_remote_aliases(&item.key)
        .into_iter()
        .any(|alias| remote.exists(&alias).unwrap_or(false));
    if alias_exists {
        let _ = remove_upload_payload(paths, &item.id);
    }
    alias_exists
}

fn upload_payload_dir(paths: &Paths) -> PathBuf {
    paths
        .upload_queue
        .parent()
        .unwrap_or(&paths.upload_queue)
        .join("upload-payloads")
}

fn upload_payload_path(paths: &Paths, id: &str) -> PathBuf {
    upload_payload_dir(paths).join(format!("{id}.bin"))
}

fn remove_upload_payload(paths: &Paths, id: &str) -> Result<()> {
    let payload_path = upload_payload_path(paths, id);
    match fs::remove_file(payload_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn next_retry_after(now: DateTime<Utc>, attempts: u32) -> DateTime<Utc> {
    now + Duration::seconds(retry_backoff_secs(attempts))
}

fn retry_backoff_secs(attempts: u32) -> i64 {
    let exponent = attempts.saturating_sub(1).min(6);
    (5 * 2_i64.pow(exponent)).min(300)
}

pub(crate) fn record_event(paths: &Paths, kind: &str, detail: &str) -> Result<()> {
    write_event_record(
        paths,
        EventJournalRecord::new(
            new_id("event"),
            kind.to_string(),
            Utc::now(),
            detail.to_string(),
        ),
    )
}

pub(crate) fn record_file_event(
    paths: &Paths,
    root_id: &str,
    path: &str,
    event_kind: &str,
    raw_backend: &str,
    detail: &str,
) -> Result<()> {
    write_event_record(
        paths,
        EventJournalRecord::new_file_event(
            new_id("event"),
            Utc::now(),
            detail.to_string(),
            root_id.to_string(),
            path.to_string(),
            event_kind.to_string(),
            raw_backend.to_string(),
        ),
    )
}

pub(crate) fn enqueue_live_diff_event_journals(
    paths: &Paths,
    current: Option<&SnapshotManifest>,
    roots: &[RootConfig],
) -> Result<usize> {
    let Some(current) = current else {
        return Ok(0);
    };
    let existing = event_journal_records(paths)?;
    let mut enqueued = 0usize;
    for root in roots
        .iter()
        .filter(|root| root.status == "active" && root.path.exists())
    {
        let snapshot_files = snapshot_root_file_map(paths, current, &root.id)?;
        let known_paths = journal_known_paths(paths, &root.id, &snapshot_files)?;
        let max_auto_add = max_auto_track_new_files();
        let unknown_add_count =
            count_unknown_journal_file_candidates(root, &known_paths, max_auto_add + 1)?;
        let config = crate::read_config(paths)?;
        let live_files = scan_live_root_for_journal(
            &config,
            root,
            &snapshot_files,
            &known_paths,
            unknown_add_count > max_auto_add,
        )?;
        let mut paths_all = snapshot_files.keys().cloned().collect::<Vec<_>>();
        paths_all.extend(
            live_files
                .keys()
                .filter(|key| !snapshot_files.contains_key(*key))
                .cloned(),
        );
        paths_all.sort();
        paths_all.dedup();
        for rel_path in paths_all {
            let snapshot_record = snapshot_files.get(&rel_path);
            let live_record = live_files.get(&rel_path);
            let Some(event_kind) = live_diff_event_kind(snapshot_record, live_record) else {
                continue;
            };
            if matches!(event_kind, "create" | "modify") && live_record.is_some() {
                crate::root_state::mark_path_journal_tracked(
                    &crate::open_db(paths)?,
                    &root.id,
                    &rel_path,
                )?;
            }
            if live_diff_event_already_covered(
                &existing,
                &root.id,
                &rel_path,
                current.timestamp,
                live_record,
            ) {
                continue;
            }
            write_event_record(
                paths,
                EventJournalRecord::new_file_event(
                    new_id("event"),
                    Utc::now(),
                    format!(
                        "live-diff root={} path={} kind={event_kind}",
                        root.id, rel_path
                    ),
                    root.id.clone(),
                    rel_path,
                    event_kind.to_string(),
                    "live-diff".into(),
                ),
            )?;
            enqueued += 1;
        }
    }
    Ok(enqueued)
}

fn write_event_record(paths: &Paths, event: EventJournalRecord) -> Result<()> {
    fs::create_dir_all(&paths.event_queue)?;
    let path = paths.event_queue.join(format!("{}.json", event.event_id));
    write_atomic(&path, &serde_json::to_vec_pretty(&event)?)?;
    Ok(())
}

pub(crate) fn import_remote_event_journals(
    paths: &Paths,
    remote: &RemoteStore,
    host_id: &str,
) -> Result<usize> {
    let prefix = remote_event_journal_prefix(host_id);
    let mut imported = 0usize;
    let mut keys = remote.list(&prefix)?;
    keys.sort();
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        let bytes = remote
            .get(&key)
            .with_context(|| format!("read remote durable journal {key}"))?;
        let mut record: EventJournalRecord = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse remote durable journal {key}"))?;
        record.remote_journal_key = Some(key);
        if record.remote_journal_synced_at.is_none() {
            record.remote_journal_synced_at = Some(Utc::now());
        }
        write_event_record(paths, record)?;
        imported += 1;
    }
    Ok(imported)
}

fn journal_known_paths(
    paths: &Paths,
    root_id: &str,
    snapshot_files: &BTreeMap<String, FileRecord>,
) -> Result<BTreeSet<String>> {
    let conn = crate::open_db(paths)?;
    let mut known = crate::root_state::tracked_paths_for_root(&conn, root_id)?;
    known.extend(snapshot_files.keys().cloned());
    Ok(known)
}

fn max_auto_track_new_files() -> usize {
    std::env::var("MAJUTSU_MAX_AUTO_TRACK_NEW_FILES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
}

fn count_unknown_journal_file_candidates(
    root: &RootConfig,
    known_paths: &BTreeSet<String>,
    stop_after: usize,
) -> Result<usize> {
    let ignore = build_ignore(root)?;
    let scan_base = journal_source_base(root);
    let mut count = 0usize;
    let walker = WalkDir::new(scan_base)
        .follow_links(root.follow_symlinks)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| {
            if entry.path() == scan_base {
                return true;
            }
            let Ok(rel) = entry.path().strip_prefix(scan_base) else {
                return true;
            };
            !entry.file_type().is_dir() || root_dir_allows_descend(root, &ignore, rel)
        });
    for entry in walker {
        let entry = entry?;
        if entry.path() == scan_base || entry.file_type().is_dir() {
            continue;
        }
        let rel = entry.path().strip_prefix(scan_base)?.to_path_buf();
        if !root_record_is_managed(root, &ignore, &rel, false) {
            continue;
        }
        let rel_s = path_to_slash(&rel);
        if !known_paths.contains(&rel_s) {
            count += 1;
            if count >= stop_after {
                break;
            }
        }
    }
    Ok(count)
}

fn journal_path_is_known(paths: &Paths, root_id: &str, rel_path: &str) -> Result<bool> {
    let conn = crate::open_db(paths)?;
    if crate::root_state::tracked_paths_for_root(&conn, root_id)?.contains(rel_path) {
        return Ok(true);
    }
    let Some(current) = current_snapshot(&conn)? else {
        return Ok(false);
    };
    let snapshot = load_snapshot_by_id(paths, &conn, &current)?;
    let files = snapshot_root_file_map(paths, &snapshot, root_id)?;
    Ok(files.contains_key(rel_path))
}

pub(crate) fn event_journal_records(paths: &Paths) -> Result<Vec<EventJournalRecord>> {
    if !paths.event_queue.exists() {
        return Ok(Vec::new());
    }
    crate::atomic_io::cleanup_stale_atomic_temps(
        &paths.event_queue,
        StdDuration::from_secs(60 * 60),
    )?;
    let mut records: Vec<EventJournalRecord> = Vec::new();
    for entry in fs::read_dir(&paths.event_queue)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(OsStr::to_str) == Some("json")
        {
            let bytes = match fs::read(entry.path()) {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            if bytes.is_empty() {
                let _ = fs::remove_file(entry.path());
                continue;
            }
            match serde_json::from_slice(&bytes) {
                Ok(record) => records.push(record),
                Err(_) => {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }
    records.sort_by_key(|a| a.observed_at);
    Ok(records)
}

fn snapshot_root_file_map(
    paths: &Paths,
    snapshot: &SnapshotManifest,
    root_id: &str,
) -> Result<BTreeMap<String, FileRecord>> {
    if let Some(records) = snapshot.roots.get(root_id) {
        return Ok(records
            .iter()
            .filter(|record| record.kind != "directory")
            .map(|record| (record.path.clone(), record.clone()))
            .collect());
    }
    let Some(root_tree) = snapshot.root_trees.get(root_id) else {
        return Ok(BTreeMap::new());
    };
    Ok(load_root_tree_entries(paths, root_tree)?
        .into_iter()
        .filter(|(_, record)| record.kind != "directory")
        .collect())
}

fn scan_live_root_for_journal(
    config: &Config,
    root: &RootConfig,
    snapshot_files: &BTreeMap<String, FileRecord>,
    known_paths: &BTreeSet<String>,
    unknown_batch_too_large: bool,
) -> Result<BTreeMap<String, FileRecord>> {
    let ignore = build_ignore(root)?;
    let mut records = BTreeMap::new();
    let scan_base = journal_source_base(root);
    let walker = WalkDir::new(scan_base)
        .follow_links(root.follow_symlinks)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| {
            if entry.path() == scan_base {
                return true;
            }
            let Ok(rel) = entry.path().strip_prefix(scan_base) else {
                return true;
            };
            !entry.file_type().is_dir() || root_dir_allows_descend(root, &ignore, rel)
        });
    for entry in walker {
        let entry = entry?;
        if entry.path() == scan_base {
            continue;
        }
        let rel = entry.path().strip_prefix(scan_base)?.to_path_buf();
        if !root_record_is_managed(root, &ignore, &rel, entry.file_type().is_dir()) {
            continue;
        }
        let rel_s = path_to_slash(&rel);
        let known_path = known_paths.contains(&rel_s);
        let link_meta = fs::symlink_metadata(entry.path())?;
        if entry.file_type().is_dir() {
            continue;
        }
        if !known_path && unknown_batch_too_large {
            continue;
        }
        let record = if link_meta.file_type().is_symlink() && !root.follow_symlinks {
            FileRecord {
                root_id: root.id.clone(),
                path: rel_s.clone(),
                kind: "symlink".into(),
                size: 0,
                mode: crate::fs_meta::file_mode(&link_meta),
                modified: crate::util::modified_secs(&link_meta),
                uid: crate::fs_meta::file_uid(&link_meta),
                gid: crate::fs_meta::file_gid(&link_meta),
                xattrs: BTreeMap::new(),
                payload: Payload::Symlink {
                    target: fs::read_link(entry.path())?.to_string_lossy().to_string(),
                },
            }
        } else if let Some(special_kind) = crate::fs_meta::special_file_kind(&link_meta) {
            FileRecord {
                root_id: root.id.clone(),
                path: rel_s.clone(),
                kind: "special".into(),
                size: 0,
                mode: crate::fs_meta::file_mode(&link_meta),
                modified: crate::util::modified_secs(&link_meta),
                uid: crate::fs_meta::file_uid(&link_meta),
                gid: crate::fs_meta::file_gid(&link_meta),
                xattrs: crate::fs_meta::read_xattrs(entry.path()),
                payload: Payload::Special { special_kind },
            }
        } else {
            let meta = if link_meta.file_type().is_symlink() {
                fs::metadata(entry.path())?
            } else {
                link_meta
            };
            if !meta.is_file() {
                continue;
            }
            let large_config = effective_large_config(config, root);
            let binary = looks_binary(entry.path()).unwrap_or(false);
            if !known_path
                && (classify_large(&large_config, &rel, meta.len(), binary)
                    || (large_config.enabled && meta.len() >= large_config.chunked_min_size))
            {
                continue;
            }
            let mode = crate::fs_meta::file_mode(&meta);
            let modified = crate::util::modified_secs(&meta);
            let uid = crate::fs_meta::file_uid(&meta);
            let gid = crate::fs_meta::file_gid(&meta);
            if let Some(snapshot_record) = snapshot_files.get(&rel_s)
                && snapshot_record.kind == "file"
                && snapshot_record.size == meta.len()
                && snapshot_record.mode == mode
                && snapshot_record.modified == modified
                && snapshot_record.uid == uid
                && snapshot_record.gid == gid
                && journal_payload_oid(&snapshot_record.payload).is_some()
            {
                let xattrs = crate::fs_meta::read_xattrs(entry.path());
                if snapshot_record.xattrs == xattrs {
                    records.insert(rel_s, snapshot_record.clone());
                    continue;
                }
            }
            let bytes = if root.follow_symlinks {
                stable_read(entry.path(), root.snapshot_mode.as_str())?
            } else {
                stable_read_in_root(scan_base, &rel, root.snapshot_mode.as_str())?
            };
            let oid = blake3_hex(&bytes);
            FileRecord {
                root_id: root.id.clone(),
                path: rel_s.clone(),
                kind: "file".into(),
                size: meta.len(),
                mode,
                modified,
                uid,
                gid,
                xattrs: crate::fs_meta::read_xattrs(entry.path()),
                payload: Payload::NormalBlob {
                    oid,
                    object_key: String::new(),
                },
            }
        };
        records.insert(rel_s, record);
    }
    Ok(records)
}

fn journal_source_base(root: &RootConfig) -> &Path {
    if root.snapshot_mode == "transactional"
        && let Some(source) = root.snapshot_source.as_deref()
        && source.exists()
    {
        return source;
    }
    &root.path
}

fn live_diff_event_kind(
    snapshot: Option<&FileRecord>,
    live: Option<&FileRecord>,
) -> Option<&'static str> {
    match (snapshot, live) {
        (None, Some(_)) => Some("create"),
        (Some(_), None) => Some("delete"),
        (Some(snapshot), Some(live)) if !journal_records_match(snapshot, live) => Some("modify"),
        _ => None,
    }
}

fn journal_records_match(a: &FileRecord, b: &FileRecord) -> bool {
    a.kind == b.kind
        && a.size == b.size
        && a.mode == b.mode
        && a.modified == b.modified
        && a.uid == b.uid
        && a.gid == b.gid
        && a.xattrs == b.xattrs
        && journal_payloads_match(&a.payload, &b.payload)
}

fn journal_payloads_match(a: &Payload, b: &Payload) -> bool {
    match (a, b) {
        (Payload::Directory, Payload::Directory) => true,
        (Payload::Symlink { target: a }, Payload::Symlink { target: b }) => a == b,
        (Payload::Special { special_kind: a }, Payload::Special { special_kind: b }) => a == b,
        _ => journal_payload_oid(a).is_some_and(|a| journal_payload_oid(b) == Some(a)),
    }
}

fn journal_payload_oid(payload: &Payload) -> Option<&str> {
    match payload {
        Payload::InlineSmall { oid, .. }
        | Payload::NormalBlob { oid, .. }
        | Payload::ChunkedBlob { oid, .. }
        | Payload::LargeObject { oid, .. }
        | Payload::Blob { oid, .. }
        | Payload::Large { oid, .. } => Some(oid),
        Payload::Directory | Payload::Symlink { .. } | Payload::Special { .. } => None,
    }
}

fn live_diff_event_already_covered(
    records: &[EventJournalRecord],
    root_id: &str,
    rel_path: &str,
    snapshot_timestamp: DateTime<Utc>,
    live_record: Option<&FileRecord>,
) -> bool {
    records
        .iter()
        .filter(|record| {
            record.root_id.as_deref() == Some(root_id)
                && record.path.as_deref() == Some(rel_path)
                && record.observed_at > snapshot_timestamp
        })
        .any(|record| {
            if record.remote_journal_synced_at.is_none() {
                return true;
            }
            match live_record {
                None => record.durable_tombstone == Some(true),
                Some(record_live) => durable_payload_matches(record, record_live),
            }
        })
}

fn durable_payload_matches(event: &EventJournalRecord, live: &FileRecord) -> bool {
    match &live.payload {
        Payload::Symlink { target } => {
            event.durable_entry_kind.as_deref() == Some("symlink")
                && event.durable_symlink_target.as_deref() == Some(target.as_str())
        }
        Payload::Special { special_kind } => {
            event.durable_entry_kind.as_deref() == Some("special")
                && event.durable_special_kind.as_deref() == Some(special_kind.as_str())
        }
        _ => {
            let Some(live_oid) = journal_payload_oid(&live.payload) else {
                return false;
            };
            event.durable_payload_oid.as_deref() == Some(live_oid)
        }
    }
}

pub(crate) fn enqueue_remote_event_journal_uploads(
    paths: &Paths,
    config: &Config,
    host_id: &str,
    roots: &[RootConfig],
    current: Option<&SnapshotManifest>,
) -> Result<usize> {
    let mut enqueued = 0usize;
    for mut record in event_journal_records(paths)?
        .into_iter()
        .filter(EventJournalRecord::is_remote_journal_pending)
    {
        if current.is_some_and(|snapshot| record.observed_at <= snapshot.timestamp) {
            continue;
        }
        let key = remote_event_journal_key(host_id, &record.event_id);
        record.remote_journal_key = Some(key.clone());
        enrich_remote_event_journal_payload(paths, config, host_id, roots, &mut record)?;
        let bytes = serde_json::to_vec_pretty(&record)?;
        enqueue_inline_upload_overwrite(paths, &key, bytes)?;
        write_event_record(paths, record)?;
        enqueued += 1;
    }
    Ok(enqueued)
}

fn enrich_remote_event_journal_payload(
    paths: &Paths,
    config: &Config,
    host_id: &str,
    roots: &[RootConfig],
    record: &mut EventJournalRecord,
) -> Result<()> {
    if record.durable_entry_kind.is_some()
        || record.durable_payload_key.is_some()
        || record.durable_tombstone == Some(true)
    {
        return Ok(());
    }
    let (Some(root_id), Some(rel_path)) = (record.root_id.as_deref(), record.path.as_deref())
    else {
        return Ok(());
    };
    let Some(root) = roots.iter().find(|root| root.id == root_id) else {
        return Ok(());
    };
    let source = journal_source_base(root).join(rel_path);
    let meta = match fs::symlink_metadata(&source) {
        Ok(meta) => meta,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            record.durable_tombstone = Some(true);
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    record.durable_mode = Some(crate::fs_meta::file_mode(&meta));
    record.durable_modified = crate::util::modified_secs(&meta);
    record.durable_uid = crate::fs_meta::file_uid(&meta);
    record.durable_gid = crate::fs_meta::file_gid(&meta);
    if meta.file_type().is_symlink() && !root.follow_symlinks {
        record.durable_entry_kind = Some("symlink".into());
        record.durable_symlink_target = Some(fs::read_link(&source)?.to_string_lossy().to_string());
        return Ok(());
    }
    if let Some(special_kind) = crate::fs_meta::special_file_kind(&meta) {
        record.durable_entry_kind = Some("special".into());
        record.durable_special_kind = Some(special_kind);
        return Ok(());
    }
    let meta = if meta.file_type().is_symlink() {
        fs::metadata(&source)?
    } else {
        meta
    };
    if !meta.is_file() {
        return Ok(());
    }
    let large_config = effective_large_config(config, root);
    let binary = looks_binary(&source).unwrap_or(false);
    if !journal_path_is_known(paths, root_id, rel_path)?
        && (classify_large(&large_config, Path::new(rel_path), meta.len(), binary)
            || (large_config.enabled && meta.len() >= large_config.chunked_min_size))
    {
        return Ok(());
    }
    record.durable_entry_kind = Some("file".into());
    record.durable_mode = Some(crate::fs_meta::file_mode(&meta));
    record.durable_modified = crate::util::modified_secs(&meta);
    record.durable_uid = crate::fs_meta::file_uid(&meta);
    record.durable_gid = crate::fs_meta::file_gid(&meta);
    if classify_large(&large_config, Path::new(rel_path), meta.len(), binary) {
        let (oid, manifest_key, chunk_count) = if root.follow_symlinks {
            crate::store_large_file(paths, &source, Path::new(rel_path), &large_config, binary)?
        } else {
            crate::store_large_file_in_root(
                paths,
                journal_source_base(root),
                Path::new(rel_path),
                &large_config,
                binary,
            )?
        };
        enqueue_large_manifest_uploads(paths, config, host_id, &manifest_key)?;
        record.durable_payload_oid = Some(oid);
        record.durable_payload_size = Some(meta.len());
        record.durable_large_manifest_key = Some(manifest_key);
        record.durable_large_chunk_count = Some(chunk_count);
        return Ok(());
    }
    if large_config.enabled && meta.len() >= large_config.chunked_min_size {
        let mut chunked_config = large_config.clone();
        chunked_config.chunk_size = large_config.chunked_chunk_size;
        let (oid, manifest_key, chunk_count) = if root.follow_symlinks {
            crate::store_large_file(paths, &source, Path::new(rel_path), &chunked_config, binary)?
        } else {
            crate::store_large_file_in_root(
                paths,
                journal_source_base(root),
                Path::new(rel_path),
                &chunked_config,
                binary,
            )?
        };
        enqueue_large_manifest_uploads(paths, config, host_id, &manifest_key)?;
        record.durable_payload_oid = Some(oid);
        record.durable_payload_size = Some(meta.len());
        record.durable_large_manifest_key = Some(manifest_key);
        record.durable_large_chunk_count = Some(chunk_count);
        return Ok(());
    }
    let bytes = if root.follow_symlinks {
        stable_read(&source, root.snapshot_mode.as_str())?
    } else {
        stable_read_in_root(
            journal_source_base(root),
            Path::new(rel_path),
            root.snapshot_mode.as_str(),
        )?
    };
    let oid = blake3_hex(&bytes);
    let object_key = crate::store_bytes(paths, &paths.objects, &oid, &bytes)?;
    let local_object = paths.home.join(&object_key);
    enqueue_file_upload(
        paths,
        &event_payload_remote_key(config, host_id, &object_key),
        &local_object,
    )?;
    record.durable_payload_oid = Some(oid);
    record.durable_payload_key = Some(object_key);
    record.durable_payload_size = Some(bytes.len() as u64);
    Ok(())
}

fn enqueue_large_manifest_uploads(
    paths: &Paths,
    config: &Config,
    host_id: &str,
    manifest_key: &str,
) -> Result<()> {
    let manifest_path = paths.home.join(manifest_key);
    enqueue_file_upload(
        paths,
        &event_payload_remote_key(config, host_id, manifest_key),
        &manifest_path,
    )?;
    let manifest: LargeManifest =
        serde_json::from_slice(&crate::read_object(paths, manifest_key)?)?;
    for chunk in manifest.chunks {
        let chunk_path = paths.home.join(&chunk.object_key);
        enqueue_file_upload(
            paths,
            &event_payload_remote_key(config, host_id, &chunk.object_key),
            &chunk_path,
        )?;
    }
    Ok(())
}

fn event_payload_remote_key(config: &Config, host_id: &str, key: &str) -> String {
    let is_s3_remote = config
        .remote
        .as_ref()
        .is_some_and(|remote| remote.url().is_ok_and(|url| url.starts_with("s3://")));
    if is_s3_remote {
        host_remote_key(host_id, key)
    } else {
        key.to_string()
    }
}

pub(crate) fn acknowledge_remote_event_journal_uploads(
    paths: &Paths,
    remote: &RemoteStore,
    host_id: &str,
) -> Result<usize> {
    let mut acknowledged = 0usize;
    for mut record in event_journal_records(paths)?
        .into_iter()
        .filter(EventJournalRecord::is_remote_journal_pending)
        .filter(|record| record.remote_journal_key.is_some())
    {
        let key = record
            .remote_journal_key
            .clone()
            .unwrap_or_else(|| remote_event_journal_key(host_id, &record.event_id));
        if remote.exists(&key)? {
            record.remote_journal_key = Some(key);
            record.remote_journal_synced_at = Some(Utc::now());
            write_event_record(paths, record)?;
            acknowledged += 1;
        }
    }
    Ok(acknowledged)
}

pub(crate) fn remote_event_journal_stats(paths: &Paths) -> Result<RemoteEventJournalStats> {
    let records = event_journal_records(paths)?;
    let mut stats = RemoteEventJournalStats::default();
    for record in records.iter().filter(|record| record.is_pending_trigger()) {
        stats.total += 1;
        if record.remote_journal_synced_at.is_some() {
            stats.durable += 1;
        } else {
            stats.pending += 1;
        }
    }
    Ok(stats)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RemoteEventJournalStats {
    pub(crate) total: usize,
    pub(crate) durable: usize,
    pub(crate) pending: usize,
}

fn remote_event_journal_key(host_id: &str, event_id: &str) -> String {
    format!("{host_id}/journal/{event_id}.json")
}

fn remote_event_journal_prefix(host_id: &str) -> String {
    format!("{host_id}/journal/")
}

pub(crate) fn has_pending_journal_events(paths: &Paths) -> Result<bool> {
    let records = event_journal_records(paths)?;
    Ok(crate::majutsu_db::has_pending_journal_events(&records))
}

pub(crate) fn compact_event_journal_force(paths: &Paths) -> Result<usize> {
    compact_event_journal_inner(paths, true, false).map(|result| result.removed)
}

pub(crate) fn event_cmd(paths: &Paths, command: EventCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    match command {
        EventCommand::Stat => {
            let stats = event_journal_stats(paths)?;
            print_event_journal_stats(&stats);
        }
        EventCommand::Compact { dry_run } => {
            let result = compact_event_journal_inner(paths, true, dry_run)?;
            println!("event_journal_records {}", result.total);
            println!("event_journal_pending {}", result.pending);
            println!("event_journal_removable {}", result.removable);
            println!("event_journal_removed {}", result.removed);
            println!("dry_run {}", dry_run);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub(crate) struct EventJournalStats {
    pub(crate) total: usize,
    pub(crate) pending: usize,
    pub(crate) processed: usize,
    pub(crate) removable: usize,
    pub(crate) oldest: Option<DateTime<Utc>>,
    pub(crate) newest: Option<DateTime<Utc>>,
    pub(crate) last_snapshot_finish: Option<DateTime<Utc>>,
    pub(crate) compact_before: Option<DateTime<Utc>>,
}

pub(crate) fn event_journal_stats(paths: &Paths) -> Result<EventJournalStats> {
    let records = event_journal_records(paths)?;
    let current = current_snapshot_for_event_compact(paths).ok().flatten();
    Ok(event_journal_stats_from_records(
        paths,
        &records,
        current.as_ref(),
    ))
}

fn print_event_journal_stats(stats: &EventJournalStats) {
    println!("event_journal_records {}", stats.total);
    println!("event_journal_processed {}", stats.processed);
    println!("event_journal_pending {}", stats.pending);
    println!("event_journal_removable {}", stats.removable);
    println!(
        "event_journal_oldest {}",
        stats
            .oldest
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "(none)".into())
    );
    println!(
        "event_journal_newest {}",
        stats
            .newest
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "(none)".into())
    );
    println!(
        "event_journal_last_snapshot_finish {}",
        stats
            .last_snapshot_finish
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "(none)".into())
    );
    println!(
        "event_journal_compact_before {}",
        stats
            .compact_before
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "(none)".into())
    );
}

fn event_journal_stats_from_records(
    paths: &Paths,
    records: &[EventJournalRecord],
    current: Option<&SnapshotManifest>,
) -> EventJournalStats {
    let snapshot_finishes = records
        .iter()
        .filter(|event| event.is_snapshot_finish())
        .map(|event| event.observed_at)
        .collect::<Vec<_>>();
    let last_snapshot_finish = snapshot_finishes.iter().copied().max();
    let pending = records
        .iter()
        .filter(|event| {
            event.is_pending_trigger()
                && last_snapshot_finish
                    .map(|finished_at| event.observed_at > finished_at)
                    .unwrap_or(true)
        })
        .count();
    let compact_before = previous_snapshot_finish(&snapshot_finishes);
    let removable = records
        .iter()
        .filter(|event| {
            event_removable_after_snapshot(
                paths,
                event,
                compact_before,
                last_snapshot_finish,
                current,
            )
        })
        .count();
    EventJournalStats {
        total: records.len(),
        pending,
        processed: records.len().saturating_sub(pending),
        removable,
        oldest: records.iter().map(|event| event.observed_at).min(),
        newest: records.iter().map(|event| event.observed_at).max(),
        last_snapshot_finish,
        compact_before,
    }
}

fn previous_snapshot_finish(finishes: &[DateTime<Utc>]) -> Option<DateTime<Utc>> {
    let mut finishes = finishes.to_vec();
    finishes.sort();
    finishes.dedup();
    finishes.iter().rev().nth(1).copied()
}

#[derive(Debug, Clone, Default)]
struct EventJournalCompactResult {
    total: usize,
    pending: usize,
    removable: usize,
    removed: usize,
}

fn compact_event_journal_inner(
    paths: &Paths,
    force: bool,
    dry_run: bool,
) -> Result<EventJournalCompactResult> {
    if !paths.event_queue.exists() {
        return Ok(EventJournalCompactResult::default());
    }
    let records = event_journal_records(paths)?;
    let current = current_snapshot_for_event_compact(paths)?;
    let stats = event_journal_stats_from_records(paths, &records, current.as_ref());
    let min_records = std::env::var("MAJUTSU_EVENT_COMPACT_MIN_RECORDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1024);
    if !force && records.len() <= min_records {
        return Ok(EventJournalCompactResult {
            total: stats.total,
            pending: stats.pending,
            removable: stats.removable,
            removed: 0,
        });
    }
    let Some(last_snapshot_finish) = stats.last_snapshot_finish else {
        return Ok(EventJournalCompactResult {
            total: stats.total,
            pending: stats.pending,
            removable: stats.removable,
            removed: 0,
        });
    };
    let mut removed = 0usize;
    for entry in fs::read_dir(&paths.event_queue)? {
        let entry = entry?;
        if !entry.file_type()?.is_file()
            || entry.path().extension().and_then(OsStr::to_str) != Some("json")
        {
            continue;
        }
        let bytes = match fs::read(entry.path()) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        let event: EventJournalRecord = serde_json::from_slice(&bytes)?;
        if event_removable_after_snapshot(
            paths,
            &event,
            stats.compact_before,
            Some(last_snapshot_finish),
            current.as_ref(),
        ) {
            if dry_run {
                removed += 1;
            } else {
                match fs::remove_file(entry.path()) {
                    Ok(()) => removed += 1,
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(err.into()),
                }
            }
        }
    }
    Ok(EventJournalCompactResult {
        total: stats.total,
        pending: stats.pending,
        removable: stats.removable,
        removed,
    })
}

fn current_snapshot_for_event_compact(paths: &Paths) -> Result<Option<SnapshotManifest>> {
    let conn = crate::open_db(paths)?;
    current_snapshot(&conn)?
        .as_deref()
        .map(|id| load_snapshot_by_id(paths, &conn, id))
        .transpose()
}

fn event_removable_after_snapshot(
    paths: &Paths,
    event: &EventJournalRecord,
    compact_before: Option<DateTime<Utc>>,
    last_snapshot_finish: Option<DateTime<Utc>>,
    current: Option<&SnapshotManifest>,
) -> bool {
    if event.is_snapshot_finish() || !event.is_pending_trigger() {
        return compact_before
            .map(|cutoff| event.observed_at < cutoff)
            .unwrap_or(false);
    }
    let Some(last_snapshot_finish) = last_snapshot_finish else {
        return false;
    };
    if event.observed_at >= last_snapshot_finish {
        return false;
    }
    if compact_event_journal_trust_snapshot_finish() {
        return true;
    }
    if event.remote_journal_synced_at.is_none() {
        return false;
    }
    let Some(current) = current else {
        return false;
    };
    durable_event_covered_by_snapshot(paths, event, current).unwrap_or(false)
}

fn compact_event_journal_trust_snapshot_finish() -> bool {
    std::env::var("MAJUTSU_EVENT_COMPACT_STRICT")
        .map(|value| matches!(value.as_str(), "0" | "false" | "FALSE" | "no" | "NO"))
        .unwrap_or(true)
}

fn durable_event_covered_by_snapshot(
    paths: &Paths,
    event: &EventJournalRecord,
    snapshot: &SnapshotManifest,
) -> Result<bool> {
    let (Some(root_id), Some(rel_path)) = (event.root_id.as_deref(), event.path.as_deref()) else {
        return Ok(event.observed_at <= snapshot.timestamp);
    };
    let snapshot_files = snapshot_root_file_map(paths, snapshot, root_id)?;
    let snapshot_record = snapshot_files.get(rel_path);
    if event.durable_tombstone == Some(true) {
        return Ok(snapshot_record.is_none());
    }
    let Some(record) = snapshot_record else {
        return Ok(false);
    };
    Ok(durable_event_matches_snapshot_record(event, record))
}

fn durable_event_matches_snapshot_record(event: &EventJournalRecord, record: &FileRecord) -> bool {
    match &record.payload {
        Payload::Symlink { target } => {
            event.durable_entry_kind.as_deref() == Some("symlink")
                && event.durable_symlink_target.as_deref() == Some(target.as_str())
        }
        Payload::Special { special_kind } => {
            event.durable_entry_kind.as_deref() == Some("special")
                && event.durable_special_kind.as_deref() == Some(special_kind.as_str())
        }
        _ => {
            let Some(record_oid) = journal_payload_oid(&record.payload) else {
                return false;
            };
            event.durable_payload_oid.as_deref() == Some(record_oid)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::upload_publish_priority;

    #[test]
    fn upload_publish_priority_keeps_current_refs_last() {
        assert!(
            upload_publish_priority("blobs/loose/aa/blob.enc")
                < upload_publish_priority("metadata/export.json")
        );
        assert!(
            upload_publish_priority("metadata/export.json")
                < upload_publish_priority("host-a/current")
        );
        assert!(
            upload_publish_priority("example/metadata/export.json")
                < upload_publish_priority("example/refs/current")
        );
        assert!(
            upload_publish_priority("example/metadata/export.json")
                < upload_publish_priority("example/head.cbor.zst.enc")
        );
    }
}
