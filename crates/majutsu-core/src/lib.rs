use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type HostId = String;
pub type RootId = String;
pub type SnapshotId = String;
pub type OperationId = String;
pub type ObjectKey = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Host {
    pub id: HostId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Root {
    pub id: RootId,
    pub name: String,
    pub path: String,
    pub status: RootStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    Default,
    Strict,
    Transactional,
}

impl SnapshotMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "default" => Ok(Self::Default),
            "strict" => Ok(Self::Strict),
            "transactional" => Ok(Self::Transactional),
            _ => bail!("snapshot mode must be default, strict, or transactional"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Strict => "strict",
            Self::Transactional => "transactional",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RootStatus {
    #[serde(alias = "Active")]
    Active,
    #[serde(alias = "Paused")]
    Paused,
    #[serde(alias = "Missing")]
    Missing,
    #[serde(alias = "Unmounted")]
    Unmounted,
    #[serde(alias = "PermissionDenied")]
    PermissionDenied,
    #[serde(alias = "Deleted")]
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostSnapshot {
    #[serde(rename = "snapshot_id", alias = "id")]
    pub id: SnapshotId,
    pub parent: Option<SnapshotId>,
    #[serde(rename = "op_id", alias = "operation")]
    pub operation: OperationId,
    #[serde(default = "default_operation_timestamp")]
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub root_trees: BTreeMap<RootId, RootSnapshot>,
    #[serde(default)]
    pub roots: BTreeMap<RootId, RootSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootSnapshot {
    pub tree_id: String,
    pub tree_key: ObjectKey,
    pub file_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotExport {
    pub id: String,
    pub parent_id: Option<String>,
    pub op_id: String,
    pub created_at: String,
    pub manifest_key: String,
    pub manifest_json: String,
}

pub fn snapshot_export_matches(actual: &SnapshotExport, expected: &SnapshotExport) -> bool {
    actual == expected
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    #[serde(rename = "op_id", alias = "id")]
    pub id: OperationId,
    #[serde(rename = "parent_op", alias = "parent")]
    pub parent: Option<OperationId>,
    pub kind: OperationKind,
    #[serde(default = "default_operation_timestamp", alias = "created_at")]
    pub timestamp: DateTime<Utc>,
    #[serde(default = "default_operation_actor")]
    pub actor: String,
    #[serde(default)]
    pub status: OperationStatus,
    pub before_snapshot: Option<SnapshotId>,
    pub after_snapshot: Option<SnapshotId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_sync_state: Option<RemoteSyncState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum OperationStatus {
    #[serde(alias = "Done")]
    #[default]
    Done,
    #[serde(alias = "Running")]
    Running,
    #[serde(alias = "Failed")]
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RemoteSyncState {
    NotSynced,
    Queued,
    Synced,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationKind {
    Init,
    #[serde(alias = "InitialScan")]
    InitialScan,
    #[serde(alias = "FileEventsBatch")]
    FileEventsBatch,
    #[serde(alias = "ManualSnapshot")]
    ManualSnapshot,
    #[serde(alias = "ConfigChange")]
    ConfigChange,
    #[serde(alias = "RootAdded")]
    RootAdded,
    RootPaused,
    RootResumed,
    #[serde(rename = "root-mark-deleted")]
    RootMarkedDeleted,
    RootMissing,
    RootUnmounted,
    RootPermissionDenied,
    #[serde(alias = "RootRemoved")]
    RootRemoved,
    OpRestore,
    #[serde(alias = "Restore")]
    Restore,
    RestorePrepare,
    RestoreResume,
    Mount,
    MountFuse,
    Unmount,
    UnmountFuse,
    Hydrate,
    LargePin,
    LargeUnpin,
    #[serde(alias = "RemoteSync")]
    RemoteSync,
    Pack,
    PackCompact,
    #[serde(alias = "Prune")]
    Prune,
    Gc,
    #[serde(alias = "KeyRotation")]
    KeyRotation,
    #[serde(alias = "Fsck")]
    Fsck,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationLogEntry {
    pub id: String,
    pub parent_op: Option<String>,
    pub kind: String,
    pub actor: String,
    pub status: String,
    pub before_snapshot: Option<String>,
    pub after_snapshot: Option<String>,
    pub created_at: String,
    pub message: Option<String>,
}

pub fn encode_operation_log(operations: &[OperationLogEntry]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for operation in operations {
        bytes.extend(serde_cbor::to_vec(operation)?);
    }
    Ok(bytes)
}

pub fn decode_operation_log(bytes: &[u8]) -> Result<Vec<OperationLogEntry>> {
    let mut operations = Vec::new();
    let mut stream =
        serde_cbor::de::Deserializer::from_slice(bytes).into_iter::<OperationLogEntry>();
    while let Some(record) = stream.next() {
        let offset = stream.byte_offset();
        let operation = record.with_context(|| format!("decode CBOR stream at byte {offset}"))?;
        operations.push(operation);
    }
    Ok(operations)
}

pub fn operation_log_entry_matches(
    actual: &OperationLogEntry,
    expected: &OperationLogEntry,
) -> bool {
    actual == expected
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRecord {
    pub root_id: String,
    pub path: String,
    pub kind: String,
    pub size: u64,
    pub mode: u32,
    pub modified: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
    #[serde(default)]
    pub xattrs: BTreeMap<String, String>,
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Payload {
    Directory,
    InlineSmall {
        oid: String,
        object_key: String,
    },
    NormalBlob {
        oid: String,
        object_key: String,
    },
    ChunkedBlob {
        oid: String,
        manifest_key: String,
        chunk_count: usize,
    },
    LargeObject {
        oid: String,
        manifest_key: String,
        chunk_count: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        media_type: Option<String>,
        #[serde(default)]
        binary: bool,
        #[serde(default = "default_large_chunking")]
        chunking: String,
        #[serde(default = "default_large_pointer_compression")]
        compression: String,
        #[serde(default = "default_large_pointer_encryption")]
        encryption: String,
        #[serde(default = "default_large_storage_tier_hint")]
        storage_tier_hint: String,
        #[serde(default = "default_large_hydrate_policy")]
        hydrate_policy: String,
    },
    Blob {
        oid: String,
        object_key: String,
    },
    Large {
        oid: String,
        manifest_key: String,
        chunk_count: usize,
    },
    Symlink {
        target: String,
    },
    Special {
        special_kind: String,
    },
}

pub fn payload_blob_ref(payload: &Payload) -> Option<(&str, &str)> {
    match payload {
        Payload::InlineSmall { oid, object_key }
        | Payload::NormalBlob { oid, object_key }
        | Payload::Blob { oid, object_key } => Some((oid, object_key)),
        _ => None,
    }
}

pub fn payload_blob_ref_mut(payload: &mut Payload) -> Option<(&str, &mut String)> {
    match payload {
        Payload::InlineSmall { oid, object_key } => Some((oid.as_str(), object_key)),
        Payload::NormalBlob { oid, object_key } => Some((oid.as_str(), object_key)),
        Payload::Blob { oid, object_key } => Some((oid.as_str(), object_key)),
        _ => None,
    }
}

pub fn payload_large_ref(payload: &Payload) -> Option<(&str, &str, usize)> {
    match payload {
        Payload::ChunkedBlob {
            oid,
            manifest_key,
            chunk_count,
        }
        | Payload::LargeObject {
            oid,
            manifest_key,
            chunk_count,
            ..
        }
        | Payload::Large {
            oid,
            manifest_key,
            chunk_count,
        } => Some((oid, manifest_key, *chunk_count)),
        _ => None,
    }
}

pub fn payload_large_ref_mut(payload: &mut Payload) -> Option<(&str, &mut String)> {
    match payload {
        Payload::ChunkedBlob {
            oid, manifest_key, ..
        } => Some((oid.as_str(), manifest_key)),
        Payload::LargeObject {
            oid, manifest_key, ..
        } => Some((oid.as_str(), manifest_key)),
        Payload::Large {
            oid, manifest_key, ..
        } => Some((oid.as_str(), manifest_key)),
        _ => None,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub snapshot_id: String,
    pub parent: Option<String>,
    pub op_id: String,
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub root_trees: BTreeMap<String, RootSnapshot>,
    #[serde(default)]
    pub roots: BTreeMap<String, Vec<FileRecord>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TreeManifest {
    pub version: u32,
    pub tree_id: String,
    pub root_id: String,
    pub created_at: DateTime<Utc>,
    pub entries: BTreeMap<String, FileRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LargeManifest {
    pub version: u32,
    pub oid: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default)]
    pub binary: bool,
    #[serde(default = "default_large_chunking")]
    pub chunking: String,
    pub chunk_size: usize,
    pub chunks: Vec<LargeChunk>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LargeChunk {
    pub index: usize,
    pub offset: u64,
    pub len: u64,
    #[serde(default)]
    pub stored_len: Option<u64>,
    #[serde(default = "default_chunk_compression")]
    pub compression: String,
    pub oid: String,
    pub object_key: String,
}

fn default_large_chunking() -> String {
    "fixed".into()
}

fn default_large_pointer_compression() -> String {
    "per-chunk".into()
}

fn default_large_pointer_encryption() -> String {
    "none".into()
}

fn default_large_storage_tier_hint() -> String {
    "hot-manifest-cold-chunks".into()
}

fn default_large_hydrate_policy() -> String {
    "on-demand".into()
}

fn default_chunk_compression() -> String {
    "none".into()
}

fn default_operation_timestamp() -> DateTime<Utc> {
    DateTime::<Utc>::UNIX_EPOCH
}

fn default_operation_actor() -> String {
    "local".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_defaults_preserve_legacy_serialized_operations() {
        let json = r#"{
            "id": "op-1",
            "parent": null,
            "kind": "ManualSnapshot",
            "before_snapshot": null,
            "after_snapshot": "snap-1"
        }"#;

        let op: Operation = serde_json::from_str(json).unwrap();

        assert_eq!(op.actor, "local");
        assert_eq!(op.status, OperationStatus::Done);
        assert_eq!(op.timestamp, DateTime::<Utc>::UNIX_EPOCH);
        assert!(op.error.is_none());
        assert!(op.remote_sync_state.is_none());
    }

    #[test]
    fn root_status_model_accepts_current_cli_statuses() {
        let root: Root = serde_json::from_str(
            r#"{
                "id": "sample",
                "name": "Sample",
                "path": "/tmp/sample",
                "status": "permission-denied"
            }"#,
        )
        .unwrap();

        assert_eq!(root.status, RootStatus::PermissionDenied);
        assert_eq!(
            serde_json::to_value(RootStatus::PermissionDenied).unwrap(),
            "permission-denied"
        );
        assert_eq!(
            serde_json::from_value::<RootStatus>(serde_json::Value::String("Active".into()))
                .unwrap(),
            RootStatus::Active
        );
        for status in [
            "active",
            "paused",
            "missing",
            "unmounted",
            "permission-denied",
            "deleted",
        ] {
            serde_json::from_value::<RootStatus>(serde_json::Value::String(status.into()))
                .unwrap_or_else(|err| panic!("root status {status} should deserialize: {err}"));
        }
    }

    #[test]
    fn snapshot_mode_matches_spec_guarantee_levels() {
        assert_eq!(
            SnapshotMode::parse("default").unwrap(),
            SnapshotMode::Default
        );
        assert_eq!(SnapshotMode::parse("strict").unwrap(), SnapshotMode::Strict);
        assert_eq!(
            SnapshotMode::parse("transactional").unwrap(),
            SnapshotMode::Transactional
        );
        assert_eq!(SnapshotMode::Transactional.as_str(), "transactional");
        assert_eq!(
            SnapshotMode::parse("eventual").unwrap_err().to_string(),
            "snapshot mode must be default, strict, or transactional"
        );
    }

    #[test]
    fn host_snapshot_model_accepts_spec_and_legacy_field_names() {
        let spec_json = r#"{
            "snapshot_id": "snap-1",
            "parent": null,
            "op_id": "op-1",
            "timestamp": "2026-06-07T00:00:00Z",
            "root_trees": {
                "sample": {
                    "tree_id": "tree-1",
                    "tree_key": "objects/trees/aa/bb",
                    "file_count": 2
                }
            },
            "roots": {
                "sample": {
                    "tree_id": "tree-1",
                    "tree_key": "objects/trees/aa/bb",
                    "file_count": 2
                }
            }
        }"#;

        let snapshot: HostSnapshot = serde_json::from_str(spec_json).unwrap();

        assert_eq!(snapshot.id, "snap-1");
        assert_eq!(snapshot.operation, "op-1");
        assert!(snapshot.root_trees.contains_key("sample"));
        assert_eq!(
            serde_json::to_value(&snapshot).unwrap()["snapshot_id"],
            "snap-1"
        );
        assert_eq!(serde_json::to_value(&snapshot).unwrap()["op_id"], "op-1");

        let legacy_json = r#"{
            "id": "snap-legacy",
            "parent": null,
            "operation": "op-legacy",
            "roots": {}
        }"#;
        let legacy: HostSnapshot = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(legacy.id, "snap-legacy");
        assert_eq!(legacy.operation, "op-legacy");
        assert_eq!(legacy.timestamp, DateTime::<Utc>::UNIX_EPOCH);
    }

    #[test]
    fn snapshot_export_json_shape_is_stable() {
        let export = SnapshotExport {
            id: "snap-1".into(),
            parent_id: Some("snap-0".into()),
            op_id: "op-1".into(),
            created_at: "2026-06-07T00:00:00Z".into(),
            manifest_key: "objects/snapshots/snap-1.json".into(),
            manifest_json: "{}".into(),
        };

        let value = serde_json::to_value(&export).unwrap();

        assert_eq!(value["id"], "snap-1");
        assert_eq!(value["parent_id"], "snap-0");
        assert_eq!(value["op_id"], "op-1");
        assert_eq!(value["manifest_key"], "objects/snapshots/snap-1.json");
        assert_eq!(value["manifest_json"], "{}");
        assert!(snapshot_export_matches(&export, &export));
    }

    #[test]
    fn operation_model_accepts_current_cli_kinds_and_statuses() {
        let json = r#"{
            "id": "op-2",
            "parent": "op-1",
            "kind": "root-mark-deleted",
            "created_at": "2026-06-07T00:00:00Z",
            "actor": "user@host",
            "status": "failed",
            "before_snapshot": "snap-1",
            "after_snapshot": "snap-1",
            "message": "sample",
            "error": "snapshot failed",
            "remote_sync_state": "failed"
        }"#;

        let op: Operation = serde_json::from_str(json).unwrap();

        assert_eq!(op.id, "op-2");
        assert_eq!(op.parent.as_deref(), Some("op-1"));
        assert_eq!(op.kind, OperationKind::RootMarkedDeleted);
        assert_eq!(op.status, OperationStatus::Failed);
        assert_eq!(op.error.as_deref(), Some("snapshot failed"));
        assert_eq!(op.remote_sync_state, Some(RemoteSyncState::Failed));
        assert_eq!(serde_json::to_value(&op).unwrap()["op_id"], "op-2");
        assert_eq!(serde_json::to_value(&op).unwrap()["parent_op"], "op-1");
        assert!(
            serde_json::to_value(&op)
                .unwrap()
                .get("created_at")
                .is_none()
        );
        assert_eq!(
            serde_json::to_value(&op).unwrap()["remote_sync_state"],
            "failed"
        );
        assert_eq!(
            serde_json::to_value(OperationKind::RootMarkedDeleted).unwrap(),
            "root-mark-deleted"
        );
        assert_eq!(
            serde_json::to_value(OperationKind::RemoteSync).unwrap(),
            "remote-sync"
        );
        assert_eq!(
            serde_json::to_value(OperationKind::OpRestore).unwrap(),
            "op-restore"
        );
        assert_eq!(serde_json::to_value(OperationStatus::Done).unwrap(), "done");

        for kind in [
            "init",
            "root-added",
            "config-change",
            "root-removed",
            "root-paused",
            "root-resumed",
            "root-mark-deleted",
            "root-missing",
            "root-unmounted",
            "root-permission-denied",
            "manual-snapshot",
            "file-events-batch",
            "op-restore",
            "restore",
            "restore-prepare",
            "restore-resume",
            "mount",
            "mount-fuse",
            "unmount",
            "unmount-fuse",
            "hydrate",
            "large-pin",
            "large-unpin",
            "remote-sync",
            "key-rotation",
            "pack",
            "pack-compact",
            "prune",
            "gc",
            "fsck",
        ] {
            serde_json::from_value::<OperationKind>(serde_json::Value::String(kind.into()))
                .unwrap_or_else(|err| panic!("operation kind {kind} should deserialize: {err}"));
        }
    }

    #[test]
    fn operation_log_entries_round_trip_cbor_streams() {
        let operations = vec![
            OperationLogEntry {
                id: "op-1".into(),
                parent_op: None,
                kind: "init".into(),
                actor: "alice@host".into(),
                status: "done".into(),
                before_snapshot: None,
                after_snapshot: Some("snap-1".into()),
                created_at: "2026-06-07T00:00:00Z".into(),
                message: Some("initialized".into()),
            },
            OperationLogEntry {
                id: "op-2".into(),
                parent_op: Some("op-1".into()),
                kind: "manual-snapshot".into(),
                actor: "alice@host".into(),
                status: "failed".into(),
                before_snapshot: Some("snap-1".into()),
                after_snapshot: None,
                created_at: "2026-06-07T00:01:00Z".into(),
                message: Some("snapshot failed".into()),
            },
        ];

        let encoded = encode_operation_log(&operations).unwrap();
        let decoded = decode_operation_log(&encoded).unwrap();

        assert_eq!(decoded, operations);
        assert!(operation_log_entry_matches(&decoded[0], &operations[0]));
    }

    #[test]
    fn file_record_defaults_preserve_legacy_serialized_metadata() {
        let json = r#"{
            "root_id": "sample",
            "path": "alpha.txt",
            "kind": "file",
            "size": 5,
            "mode": 33188,
            "modified": null,
            "payload": {
                "type": "normal-blob",
                "oid": "oid-1",
                "object_key": "objects/blobs/oid-1"
            }
        }"#;

        let record: FileRecord = serde_json::from_str(json).unwrap();

        assert_eq!(record.uid, None);
        assert_eq!(record.gid, None);
        assert!(record.xattrs.is_empty());
    }
}
