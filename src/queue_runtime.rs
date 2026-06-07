use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use majutsu_db::{EventJournalRecord, UploadQueueItem, expected_upload_queue_item_id};
use majutsu_store::is_content_addressed_remote_key;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::Paths;
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
    write_upload_item(
        paths,
        UploadQueueItem::inline(
            expected_upload_queue_item_id(key),
            key.to_string(),
            bytes,
            Utc::now(),
        ),
    )
}

pub(crate) fn enqueue_file_upload(paths: &Paths, key: &str, source: &Path) -> Result<()> {
    write_upload_item(
        paths,
        UploadQueueItem::file(
            expected_upload_queue_item_id(key),
            key.to_string(),
            path_to_slash(source),
            Utc::now(),
        ),
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
            let item: UploadQueueItem = serde_json::from_slice(&fs::read(entry.path())?)?;
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

pub(crate) fn drain_upload_queue(paths: &Paths, remote: &RemoteStore) -> Result<usize> {
    let mut uploaded = 0usize;
    for (path, mut item) in upload_queue_items(paths)? {
        let bytes = if let Some(bytes) = item.inline.take() {
            bytes
        } else if let Some(source) = &item.source {
            fs::read(source).with_context(|| format!("read queued upload source {source}"))?
        } else {
            bail!(
                "queued upload has neither inline payload nor source: {}",
                item.key
            );
        };
        let upload_result = if is_content_addressed_remote_key(&item.key)
            && remote.capabilities().conditional_put
        {
            remote.put_if_absent(&item.key, &bytes).map(|_| ())
        } else {
            remote.put(&item.key, &bytes)
        };
        match upload_result {
            Ok(()) => {
                fs::remove_file(path)?;
                uploaded += 1;
            }
            Err(err) => {
                let next_attempt = item.attempts.saturating_add(1);
                item.record_attempt(Some(next_retry_after(Utc::now(), next_attempt)));
                fs::write(&path, serde_json::to_vec_pretty(&item)?)?;
                return Err(err).with_context(|| format!("upload failed for {}", item.key));
            }
        }
    }
    Ok(uploaded)
}

pub(crate) fn next_retry_after(now: DateTime<Utc>, attempts: u32) -> DateTime<Utc> {
    now + Duration::seconds(retry_backoff_secs(attempts))
}

fn retry_backoff_secs(attempts: u32) -> i64 {
    let exponent = attempts.saturating_sub(1).min(6);
    (5 * 2_i64.pow(exponent)).min(300)
}

pub(crate) fn record_event(paths: &Paths, kind: &str, detail: &str) -> Result<()> {
    fs::create_dir_all(&paths.event_queue)?;
    let event = EventJournalRecord::new(
        new_id("event"),
        kind.to_string(),
        Utc::now(),
        detail.to_string(),
    );
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
            records.push(serde_json::from_slice(&fs::read(entry.path())?)?);
        }
    }
    records.sort_by(|a, b| a.observed_at.cmp(&b.observed_at));
    Ok(records)
}

pub(crate) fn has_pending_journal_events(paths: &Paths) -> Result<bool> {
    let records = event_journal_records(paths)?;
    Ok(majutsu_db::has_pending_journal_events(&records))
}
