use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const ROOTS_TABLE: &str = "roots";
pub const SNAPSHOTS_TABLE: &str = "snapshots";
pub const OPERATIONS_TABLE: &str = "operations";
pub const BLOBS_TABLE: &str = "blobs";
pub const LARGE_OBJECTS_TABLE: &str = "large_objects";
pub const CHUNKS_TABLE: &str = "chunks";
pub const UPLOAD_QUEUE_TABLE: &str = "upload_queue";
pub const RESTORE_QUEUE_TABLE: &str = "restore_queue";
pub const REFS_TABLE: &str = "refs";
pub const PACKS_TABLE: &str = "packs";
pub const LARGE_PINS_TABLE: &str = "large_pins";
pub const REMOTE_REFS_TABLE: &str = "remote_refs";

pub const SCHEMA_SQL: &str = "
create table if not exists roots(id text primary key, data_json text not null);
create table if not exists snapshots(
  id text primary key,
  parent_id text,
  op_id text not null,
  created_at text not null,
  manifest_key text not null,
  manifest_json text not null
);
create table if not exists operations(
  id text primary key,
  parent_op text,
  kind text not null,
  actor text not null default 'local',
  status text not null default 'done',
  before_snapshot text,
  after_snapshot text,
  created_at text not null,
  message text,
  error text,
  remote_sync_state text
);
create table if not exists refs(name text primary key, value text not null);
create table if not exists blobs(oid text primary key, size integer not null, object_key text not null);
create table if not exists packs(pack_id text primary key, pack_key text not null, index_key text not null, object_count integer not null, size integer not null);
create table if not exists large_objects(oid text primary key, size integer not null, chunk_count integer not null, manifest_key text not null);
create table if not exists chunks(oid text primary key, size integer not null, object_key text not null);
create table if not exists large_pins(oid text primary key, pinned_at text not null, reason text);
create table if not exists remote_refs(
  remote text not null,
  name text not null,
  value text not null,
  observed_at text not null,
  primary key(remote, name)
);
";

pub const COMPAT_MIGRATIONS: &[&str] = &[
    "alter table blobs add column pack_id text",
    "alter table blobs add column pack_offset integer",
    "alter table blobs add column pack_len integer",
    "alter table operations add column parent_op text",
    "alter table operations add column actor text not null default 'local'",
    "alter table operations add column status text not null default 'done'",
    "alter table operations add column error text",
    "alter table operations add column remote_sync_state text",
];

pub fn schema_sql() -> &'static str {
    SCHEMA_SQL
}

pub fn compat_migrations() -> &'static [&'static str] {
    COMPAT_MIGRATIONS
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadQueueItem {
    pub id: String,
    pub key: String,
    pub source: Option<String>,
    pub inline: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadQueueItemIssue {
    IdDoesNotMatchKey { expected: String },
    InvalidKey { reason: RemoteObjectKeyIssue },
    BothSourceAndInline,
    MissingPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteObjectKeyIssue {
    NotRelativeSlashPath,
    UnsafeComponent,
}

impl UploadQueueItem {
    pub fn inline(id: String, key: String, bytes: Vec<u8>, created_at: DateTime<Utc>) -> Self {
        Self {
            id,
            key,
            source: None,
            inline: Some(bytes),
            created_at,
            attempts: 0,
            retry_after: None,
        }
    }

    pub fn file(id: String, key: String, source: String, created_at: DateTime<Utc>) -> Self {
        Self {
            id,
            key,
            source: Some(source),
            inline: None,
            created_at,
            attempts: 0,
            retry_after: None,
        }
    }

    pub fn preserve_retry_state(self, existing: &Self) -> Self {
        Self {
            attempts: existing.attempts,
            retry_after: existing.retry_after,
            created_at: existing.created_at,
            ..self
        }
    }

    pub fn record_attempt(&mut self, retry_after: Option<DateTime<Utc>>) {
        self.attempts += 1;
        self.retry_after = retry_after;
    }

    pub fn validation_issues(&self) -> Vec<UploadQueueItemIssue> {
        let mut issues = Vec::new();
        let expected_id = expected_upload_queue_item_id(&self.key);
        if self.id != expected_id {
            issues.push(UploadQueueItemIssue::IdDoesNotMatchKey {
                expected: expected_id,
            });
        }
        if let Some(reason) = remote_object_key_issue(&self.key) {
            issues.push(UploadQueueItemIssue::InvalidKey { reason });
        }
        match (&self.source, &self.inline) {
            (Some(_), Some(_)) => issues.push(UploadQueueItemIssue::BothSourceAndInline),
            (None, None) => issues.push(UploadQueueItemIssue::MissingPayload),
            _ => {}
        }
        issues
    }
}

pub fn expected_upload_queue_item_id(key: &str) -> String {
    format!("upload-{}", blake3::hash(key.as_bytes()).to_hex())
}

pub fn is_valid_remote_object_key(key: &str) -> bool {
    remote_object_key_issue(key).is_none()
}

pub fn remote_object_key_issue(key: &str) -> Option<RemoteObjectKeyIssue> {
    if key.is_empty() || key.starts_with('/') || key.ends_with('/') || key.contains('\\') {
        return Some(RemoteObjectKeyIssue::NotRelativeSlashPath);
    }
    if key
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Some(RemoteObjectKeyIssue::UnsafeComponent);
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventJournalRecord {
    pub event_id: String,
    pub kind: String,
    pub observed_at: DateTime<Utc>,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_backend: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventJournalRecordIssue {
    EmptyEventId,
    EmptyKind,
    EmptyDetail,
}

impl EventJournalRecord {
    pub fn new(event_id: String, kind: String, observed_at: DateTime<Utc>, detail: String) -> Self {
        Self {
            event_id,
            kind,
            observed_at,
            detail,
            root_id: None,
            path: None,
            event_kind: None,
            raw_backend: None,
        }
    }

    pub fn new_file_event(
        event_id: String,
        observed_at: DateTime<Utc>,
        detail: String,
        root_id: String,
        path: String,
        event_kind: String,
        raw_backend: String,
    ) -> Self {
        Self {
            event_id,
            kind: "fs-event".into(),
            observed_at,
            detail,
            root_id: Some(root_id),
            path: Some(path),
            event_kind: Some(event_kind),
            raw_backend: Some(raw_backend),
        }
    }

    pub fn is_snapshot_finish(&self) -> bool {
        self.kind == "snapshot-finish"
    }

    pub fn is_pending_trigger(&self) -> bool {
        matches!(self.kind.as_str(), "fs-event" | "periodic-rescan")
    }

    pub fn validation_issues(&self) -> Vec<EventJournalRecordIssue> {
        let mut issues = Vec::new();
        if self.event_id.trim().is_empty() {
            issues.push(EventJournalRecordIssue::EmptyEventId);
        }
        if self.kind.trim().is_empty() {
            issues.push(EventJournalRecordIssue::EmptyKind);
        }
        if self.detail.trim().is_empty() {
            issues.push(EventJournalRecordIssue::EmptyDetail);
        }
        issues
    }
}

pub fn has_pending_journal_events(records: &[EventJournalRecord]) -> bool {
    let last_snapshot_finish = records
        .iter()
        .filter(|event| event.is_snapshot_finish())
        .map(|event| event.observed_at)
        .max();
    records.iter().any(|event| {
        event.is_pending_trigger()
            && last_snapshot_finish
                .map(|finished_at| event.observed_at > finished_at)
                .unwrap_or(true)
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalRefIssue {
    Duplicate { name: String },
    MissingSnapshot { name: String, value: String },
    InvalidLastSynced { value: String, error: String },
    Unknown { name: String },
}

impl LocalRefIssue {
    pub fn message(&self) -> String {
        match self {
            Self::Duplicate { name } => format!("duplicate local ref {name}"),
            Self::MissingSnapshot { name, value } => {
                format!("local ref {name} points to missing snapshot {value}")
            }
            Self::InvalidLastSynced { value, error } => {
                format!("local ref last-synced has invalid timestamp {value}: {error}")
            }
            Self::Unknown { name } => format!("unknown local ref {name}"),
        }
    }
}

pub fn local_ref_issues<I>(refs: I, snapshot_ids: &BTreeSet<String>) -> Vec<LocalRefIssue>
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut issues = Vec::new();
    let mut seen = BTreeSet::new();
    for (name, value) in refs {
        if !seen.insert(name.clone()) {
            issues.push(LocalRefIssue::Duplicate { name: name.clone() });
        }
        match name.as_str() {
            "current" => {
                if !snapshot_ids.contains(&value) {
                    issues.push(LocalRefIssue::MissingSnapshot { name, value });
                }
            }
            "last-synced" => {
                if let Err(err) = DateTime::parse_from_rfc3339(&value) {
                    issues.push(LocalRefIssue::InvalidLastSynced {
                        value,
                        error: err.to_string(),
                    });
                }
            }
            "current-branch" => {
                if value.trim().is_empty() || value.contains('/') || value.contains('\\') {
                    issues.push(LocalRefIssue::Unknown { name });
                }
            }
            name if name.starts_with("branches/") => {
                if !snapshot_ids.contains(&value) {
                    issues.push(LocalRefIssue::MissingSnapshot {
                        name: name.to_string(),
                        value,
                    });
                }
            }
            _ => issues.push(LocalRefIssue::Unknown { name }),
        }
    }
    issues
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteRefIssue {
    EmptyRemote {
        name: String,
    },
    InvalidObservedAt {
        name: String,
        value: String,
        error: String,
    },
    UnsupportedName {
        name: String,
    },
    HostMismatch {
        host_id: String,
        config_host_id: String,
    },
    MissingSnapshot {
        name: String,
        value: String,
    },
    InvalidLastSynced {
        name: String,
        value: String,
        error: String,
    },
    UnsupportedRefName {
        name: String,
    },
}

impl RemoteRefIssue {
    pub fn message(&self) -> String {
        match self {
            Self::EmptyRemote { name } => {
                format!("remote ref has empty remote for {name}")
            }
            Self::InvalidObservedAt { name, value, error } => {
                format!("remote ref {name} has invalid observed_at {value}: {error}")
            }
            Self::UnsupportedName { name } => {
                format!("remote ref has unsupported name {name}")
            }
            Self::HostMismatch {
                host_id,
                config_host_id,
            } => {
                format!(
                    "remote ref host id {host_id} does not match config host id {config_host_id}"
                )
            }
            Self::MissingSnapshot { name, value } => {
                format!("remote ref {name} points to missing snapshot {value}")
            }
            Self::InvalidLastSynced { name, value, error } => {
                format!("remote ref {name} has invalid last-synced {value}: {error}")
            }
            Self::UnsupportedRefName { name } => {
                format!("remote ref has unsupported ref name {name}")
            }
        }
    }
}

pub fn remote_ref_issues<I>(
    refs: I,
    config_host_id: &str,
    snapshot_ids: &BTreeSet<String>,
) -> Vec<RemoteRefIssue>
where
    I: IntoIterator<Item = (String, String, String, String)>,
{
    let mut issues = Vec::new();
    for (remote, name, value, observed_at) in refs {
        if remote.trim().is_empty() {
            issues.push(RemoteRefIssue::EmptyRemote { name: name.clone() });
        }
        if let Err(err) = DateTime::parse_from_rfc3339(&observed_at) {
            issues.push(RemoteRefIssue::InvalidObservedAt {
                name: name.clone(),
                value: observed_at,
                error: err.to_string(),
            });
        }
        let Some((host_id, ref_name)) = parse_canonical_host_ref_name(&name) else {
            issues.push(RemoteRefIssue::UnsupportedName { name });
            continue;
        };
        if host_id != config_host_id {
            issues.push(RemoteRefIssue::HostMismatch {
                host_id: host_id.to_string(),
                config_host_id: config_host_id.to_string(),
            });
        }
        match ref_name {
            "current" => {
                if !snapshot_ids.contains(&value) {
                    issues.push(RemoteRefIssue::MissingSnapshot { name, value });
                }
            }
            "last-synced" => {
                if let Err(err) = DateTime::parse_from_rfc3339(&value) {
                    issues.push(RemoteRefIssue::InvalidLastSynced {
                        name,
                        value,
                        error: err.to_string(),
                    });
                }
            }
            _ => issues.push(RemoteRefIssue::UnsupportedRefName { name }),
        }
    }
    issues
}

pub fn parse_canonical_host_ref_name(name: &str) -> Option<(&str, &str)> {
    let rest = name.strip_prefix("hosts/")?;
    let (host_id, rest) = rest.split_once("/refs/")?;
    if host_id.is_empty() || rest.is_empty() || rest.contains('/') {
        return None;
    }
    Some((host_id, rest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn time(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    #[test]
    fn schema_defines_required_spec_tables() {
        for table in [
            ROOTS_TABLE,
            SNAPSHOTS_TABLE,
            OPERATIONS_TABLE,
            BLOBS_TABLE,
            PACKS_TABLE,
            LARGE_OBJECTS_TABLE,
            CHUNKS_TABLE,
            LARGE_PINS_TABLE,
            REFS_TABLE,
            REMOTE_REFS_TABLE,
        ] {
            assert!(
                SCHEMA_SQL.contains(&format!("create table if not exists {table}")),
                "schema should define {table}"
            );
        }
    }

    #[test]
    fn schema_preserves_operation_log_columns() {
        for column in [
            "parent_op",
            "kind text not null",
            "actor text not null default 'local'",
            "status text not null default 'done'",
            "before_snapshot",
            "after_snapshot",
            "created_at text not null",
        ] {
            assert!(
                SCHEMA_SQL.contains(column),
                "missing operation column {column}"
            );
        }
    }

    #[test]
    fn compat_migrations_cover_existing_legacy_columns() {
        assert!(COMPAT_MIGRATIONS.iter().any(|sql| sql.contains("pack_id")));
        assert!(
            COMPAT_MIGRATIONS
                .iter()
                .any(|sql| sql.contains("parent_op"))
        );
        assert!(COMPAT_MIGRATIONS.iter().any(|sql| sql.contains("actor")));
        assert!(COMPAT_MIGRATIONS.iter().any(|sql| sql.contains("status")));
    }

    #[test]
    fn upload_queue_item_preserves_retry_state_on_reenqueue() {
        let existing = UploadQueueItem::inline(
            "upload-a".into(),
            "objects/a".into(),
            b"old".to_vec(),
            time(10),
        );
        let mut existing = existing;
        existing.record_attempt(Some(time(30)));
        existing.record_attempt(Some(time(40)));

        let reenqueued = UploadQueueItem::file(
            "upload-a".into(),
            "objects/a".into(),
            "/tmp/a".into(),
            time(20),
        )
        .preserve_retry_state(&existing);

        assert_eq!(reenqueued.attempts, 2);
        assert_eq!(reenqueued.retry_after, Some(time(40)));
        assert_eq!(reenqueued.created_at, time(10));
        assert_eq!(reenqueued.source.as_deref(), Some("/tmp/a"));
        assert!(reenqueued.inline.is_none());
    }

    #[test]
    fn upload_queue_item_json_shape_is_stable() {
        let item = UploadQueueItem::inline(
            "upload-a".into(),
            "objects/a".into(),
            b"abc".to_vec(),
            time(10),
        );
        let json = serde_json::to_value(&item).unwrap();

        assert_eq!(json["id"], "upload-a");
        assert_eq!(json["key"], "objects/a");
        assert_eq!(json["source"], serde_json::Value::Null);
        assert_eq!(json["inline"], serde_json::json!([97, 98, 99]));
        assert_eq!(json["attempts"], 0);
        assert_eq!(json.get("retry_after"), None);
    }

    #[test]
    fn upload_queue_item_validation_reports_fsck_invariants() {
        let item = UploadQueueItem {
            id: "wrong".into(),
            key: "../bad".into(),
            source: Some("/tmp/a".into()),
            inline: Some(b"abc".to_vec()),
            created_at: time(10),
            attempts: 0,
            retry_after: None,
        };

        let issues = item.validation_issues();

        assert_eq!(
            issues,
            vec![
                UploadQueueItemIssue::IdDoesNotMatchKey {
                    expected: expected_upload_queue_item_id("../bad"),
                },
                UploadQueueItemIssue::InvalidKey {
                    reason: RemoteObjectKeyIssue::UnsafeComponent,
                },
                UploadQueueItemIssue::BothSourceAndInline,
            ]
        );

        let mut missing_payload = item;
        missing_payload.id = expected_upload_queue_item_id("objects/a");
        missing_payload.key = "objects/a".into();
        missing_payload.source = None;
        missing_payload.inline = None;
        assert_eq!(
            missing_payload.validation_issues(),
            vec![UploadQueueItemIssue::MissingPayload]
        );
    }

    #[test]
    fn upload_queue_item_validation_accepts_valid_file_and_inline_items() {
        let key = "objects/blobs/aa";
        let inline = UploadQueueItem::inline(
            expected_upload_queue_item_id(key),
            key.into(),
            b"abc".to_vec(),
            time(10),
        );
        let file = UploadQueueItem::file(
            expected_upload_queue_item_id(key),
            key.into(),
            "/tmp/a".into(),
            time(10),
        );

        assert!(inline.validation_issues().is_empty());
        assert!(file.validation_issues().is_empty());
    }

    #[test]
    fn remote_object_key_validation_rejects_unsafe_shapes() {
        for key in [
            "",
            "/objects/a",
            "objects/a/",
            "objects//a",
            "objects/../a",
            "a\\b",
        ] {
            assert!(!is_valid_remote_object_key(key), "{key} should be invalid");
        }
        assert!(is_valid_remote_object_key("objects/blobs/a"));
    }

    #[test]
    fn journal_pending_events_follow_latest_snapshot_finish() {
        assert!(has_pending_journal_events(&[EventJournalRecord::new(
            "event-1".into(),
            "fs-event".into(),
            time(10),
            "write a".into(),
        )]));

        assert!(!has_pending_journal_events(&[
            EventJournalRecord::new(
                "event-1".into(),
                "fs-event".into(),
                time(10),
                "write a".into(),
            ),
            EventJournalRecord::new(
                "event-2".into(),
                "snapshot-finish".into(),
                time(20),
                "snapshot complete".into(),
            ),
        ]));

        assert!(has_pending_journal_events(&[
            EventJournalRecord::new(
                "event-1".into(),
                "snapshot-finish".into(),
                time(20),
                "snapshot complete".into(),
            ),
            EventJournalRecord::new(
                "event-2".into(),
                "periodic-rescan".into(),
                time(30),
                "timer".into(),
            ),
        ]));

        assert!(!has_pending_journal_events(&[EventJournalRecord::new(
            "event-1".into(),
            "event-journal-replay".into(),
            time(30),
            "replay".into(),
        )]));
    }

    #[test]
    fn event_journal_record_validation_reports_empty_fields() {
        let event = EventJournalRecord::new(" ".into(), "".into(), time(10), "\t".into());

        assert_eq!(
            event.validation_issues(),
            vec![
                EventJournalRecordIssue::EmptyEventId,
                EventJournalRecordIssue::EmptyKind,
                EventJournalRecordIssue::EmptyDetail,
            ]
        );
    }

    #[test]
    fn event_journal_file_event_preserves_structured_watch_fields() {
        let event = EventJournalRecord::new_file_event(
            "event-1".into(),
            time(10),
            "modify /tmp/source/alpha.txt".into(),
            "docs".into(),
            "alpha.txt".into(),
            "modify".into(),
            "inotify".into(),
        );
        let json = serde_json::to_string(&event).unwrap();

        assert!(json.contains("\"kind\":\"fs-event\""));
        assert!(json.contains("\"root_id\":\"docs\""));
        assert!(json.contains("\"path\":\"alpha.txt\""));
        assert!(json.contains("\"event_kind\":\"modify\""));
        assert!(json.contains("\"raw_backend\":\"inotify\""));
        assert!(event.validation_issues().is_empty());
    }

    #[test]
    fn local_ref_validation_reports_unknown_and_broken_refs() {
        let issues = local_ref_issues(
            [
                ("current".to_string(), "missing-snap".to_string()),
                ("last-synced".to_string(), "not-time".to_string()),
                ("last-synced".to_string(), "also-not-time".to_string()),
                ("legacy".to_string(), "value".to_string()),
            ],
            &BTreeSet::from(["snap-1".to_string()]),
        );

        assert_eq!(
            issues,
            vec![
                LocalRefIssue::MissingSnapshot {
                    name: "current".into(),
                    value: "missing-snap".into(),
                },
                LocalRefIssue::InvalidLastSynced {
                    value: "not-time".into(),
                    error: "premature end of input".into(),
                },
                LocalRefIssue::Duplicate {
                    name: "last-synced".into(),
                },
                LocalRefIssue::InvalidLastSynced {
                    value: "also-not-time".into(),
                    error: "premature end of input".into(),
                },
                LocalRefIssue::Unknown {
                    name: "legacy".into(),
                },
            ]
        );
    }

    #[test]
    fn local_ref_issue_messages_are_stable() {
        assert_eq!(
            LocalRefIssue::Duplicate {
                name: "current".into()
            }
            .message(),
            "duplicate local ref current"
        );
        assert_eq!(
            LocalRefIssue::MissingSnapshot {
                name: "current".into(),
                value: "missing-snap".into(),
            }
            .message(),
            "local ref current points to missing snapshot missing-snap"
        );
        assert_eq!(
            LocalRefIssue::InvalidLastSynced {
                value: "bad-time".into(),
                error: "premature end of input".into(),
            }
            .message(),
            "local ref last-synced has invalid timestamp bad-time: premature end of input"
        );
        assert_eq!(
            LocalRefIssue::Unknown {
                name: "legacy".into()
            }
            .message(),
            "unknown local ref legacy"
        );
    }

    #[test]
    fn remote_ref_validation_reports_cache_invariants() {
        let issues = remote_ref_issues(
            [
                (
                    "".to_string(),
                    "hosts/other/refs/current".to_string(),
                    "missing-snap".to_string(),
                    "not-time".to_string(),
                ),
                (
                    "file://remote".to_string(),
                    "hosts/host-a/refs/last-synced".to_string(),
                    "bad-time".to_string(),
                    "2026-06-07T00:00:00Z".to_string(),
                ),
                (
                    "file://remote".to_string(),
                    "hosts/host-a/refs/legacy".to_string(),
                    "value".to_string(),
                    "2026-06-07T00:00:00Z".to_string(),
                ),
                (
                    "file://remote".to_string(),
                    "legacy/current".to_string(),
                    "snap-1".to_string(),
                    "2026-06-07T00:00:00Z".to_string(),
                ),
            ],
            "host-a",
            &BTreeSet::from(["snap-1".to_string()]),
        );

        assert_eq!(
            issues,
            vec![
                RemoteRefIssue::EmptyRemote {
                    name: "hosts/other/refs/current".into(),
                },
                RemoteRefIssue::InvalidObservedAt {
                    name: "hosts/other/refs/current".into(),
                    value: "not-time".into(),
                    error: "premature end of input".into(),
                },
                RemoteRefIssue::HostMismatch {
                    host_id: "other".into(),
                    config_host_id: "host-a".into(),
                },
                RemoteRefIssue::MissingSnapshot {
                    name: "hosts/other/refs/current".into(),
                    value: "missing-snap".into(),
                },
                RemoteRefIssue::InvalidLastSynced {
                    name: "hosts/host-a/refs/last-synced".into(),
                    value: "bad-time".into(),
                    error: "premature end of input".into(),
                },
                RemoteRefIssue::UnsupportedRefName {
                    name: "hosts/host-a/refs/legacy".into(),
                },
                RemoteRefIssue::UnsupportedName {
                    name: "legacy/current".into(),
                },
            ]
        );
    }

    #[test]
    fn remote_ref_issue_messages_are_stable() {
        assert_eq!(
            RemoteRefIssue::EmptyRemote {
                name: "hosts/host-a/refs/current".into(),
            }
            .message(),
            "remote ref has empty remote for hosts/host-a/refs/current"
        );
        assert_eq!(
            RemoteRefIssue::InvalidObservedAt {
                name: "hosts/host-a/refs/current".into(),
                value: "bad-time".into(),
                error: "premature end of input".into(),
            }
            .message(),
            "remote ref hosts/host-a/refs/current has invalid observed_at bad-time: premature end of input"
        );
        assert_eq!(
            RemoteRefIssue::UnsupportedName {
                name: "legacy/current".into(),
            }
            .message(),
            "remote ref has unsupported name legacy/current"
        );
        assert_eq!(
            RemoteRefIssue::HostMismatch {
                host_id: "other".into(),
                config_host_id: "host-a".into(),
            }
            .message(),
            "remote ref host id other does not match config host id host-a"
        );
        assert_eq!(
            RemoteRefIssue::MissingSnapshot {
                name: "hosts/host-a/refs/current".into(),
                value: "missing-snap".into(),
            }
            .message(),
            "remote ref hosts/host-a/refs/current points to missing snapshot missing-snap"
        );
        assert_eq!(
            RemoteRefIssue::InvalidLastSynced {
                name: "hosts/host-a/refs/last-synced".into(),
                value: "bad-time".into(),
                error: "premature end of input".into(),
            }
            .message(),
            "remote ref hosts/host-a/refs/last-synced has invalid last-synced bad-time: premature end of input"
        );
        assert_eq!(
            RemoteRefIssue::UnsupportedRefName {
                name: "hosts/host-a/refs/legacy".into(),
            }
            .message(),
            "remote ref has unsupported ref name hosts/host-a/refs/legacy"
        );
    }
}
