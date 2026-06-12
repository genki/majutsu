use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use majutsu_db::{EventJournalRecord, UploadQueueItem, expected_upload_queue_item_id};
use majutsu_store::{canonical_remote_aliases, is_content_addressed_remote_key};
use std::ffi::OsStr;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration as StdDuration, Instant};

use crate::config::Paths;
use crate::object_paths::prefer_canonical_remote_only;
use crate::remote_store::RemoteStore;
use crate::util::{new_id, path_to_slash};

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
        let existing: UploadQueueItem = serde_json::from_slice(&fs::read(&path)?)?;
        item.preserve_retry_state(&existing)
    } else {
        item
    };
    fs::write(path, serde_json::to_vec_pretty(&item)?)?;
    Ok(())
}

pub(crate) fn upload_queue_items(paths: &Paths) -> Result<Vec<(PathBuf, UploadQueueItem)>> {
    if !paths.upload_queue.exists() {
        return Ok(Vec::new());
    }
    let mut items = Vec::new();
    for entry in fs::read_dir(&paths.upload_queue)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(OsStr::to_str) == Some("json")
        {
            let bytes = match fs::read(entry.path()) {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            let item: UploadQueueItem = serde_json::from_slice(&bytes)?;
            items.push((entry.path(), item));
        }
    }
    items.sort_by(|a, b| a.1.key.cmp(&b.1.key));
    Ok(items)
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
        if let Some(retry_after) = item.retry_after {
            if retry_after > now {
                stats.delayed += 1;
                stats.next_retry_after = stats
                    .next_retry_after
                    .map(|current| current.min(retry_after))
                    .or(Some(retry_after));
            }
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
    let progress_enabled = total >= 16;
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
            fs::write(path, serde_json::to_vec_pretty(&item)?)?;
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
    if key == "hosts/current" || key.starts_with("refs/") || key.contains("/refs/") {
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
        || key.starts_with("hosts/")
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
                < upload_publish_priority("hosts/current")
        );
        assert!(
            upload_publish_priority("hosts/example/metadata/export.json")
                < upload_publish_priority("hosts/example/refs/current")
        );
        assert!(
            upload_publish_priority("hosts/example/metadata/export.json")
                < upload_publish_priority("hosts/example/head.cbor.zst.enc")
        );
    }
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

fn write_event_record(paths: &Paths, event: EventJournalRecord) -> Result<()> {
    fs::create_dir_all(&paths.event_queue)?;
    let path = paths.event_queue.join(format!("{}.json", event.event_id));
    fs::write(path, serde_json::to_vec_pretty(&event)?)?;
    Ok(())
}

pub(crate) fn event_journal_records(paths: &Paths) -> Result<Vec<EventJournalRecord>> {
    if !paths.event_queue.exists() {
        return Ok(Vec::new());
    }
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
            records.push(serde_json::from_slice(&bytes)?);
        }
    }
    records.sort_by(|a, b| a.observed_at.cmp(&b.observed_at));
    Ok(records)
}

pub(crate) fn has_pending_journal_events(paths: &Paths) -> Result<bool> {
    let records = event_journal_records(paths)?;
    Ok(majutsu_db::has_pending_journal_events(&records))
}

pub(crate) fn compact_event_journal(paths: &Paths) -> Result<usize> {
    if !paths.event_queue.exists() {
        return Ok(0);
    }
    let records = event_journal_records(paths)?;
    let min_records = std::env::var("MAJUTSU_EVENT_COMPACT_MIN_RECORDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1024);
    if records.len() <= min_records {
        return Ok(0);
    }
    let Some(last_snapshot_finish) = records
        .iter()
        .filter(|event| event.is_snapshot_finish())
        .map(|event| event.observed_at)
        .max()
    else {
        return Ok(0);
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
        if event.observed_at < last_snapshot_finish {
            match fs::remove_file(entry.path()) {
                Ok(()) => removed += 1,
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
    }
    Ok(removed)
}
