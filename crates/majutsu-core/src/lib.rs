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
    pub id: SnapshotId,
    pub parent: Option<SnapshotId>,
    pub operation: OperationId,
    pub roots: BTreeMap<RootId, RootSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootSnapshot {
    pub tree_id: String,
    pub tree_key: ObjectKey,
    pub file_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub id: OperationId,
    pub parent: Option<OperationId>,
    pub kind: OperationKind,
    #[serde(default = "default_operation_timestamp")]
    pub timestamp: DateTime<Utc>,
    #[serde(default = "default_operation_actor")]
    pub actor: String,
    #[serde(default)]
    pub status: OperationStatus,
    pub before_snapshot: Option<SnapshotId>,
    pub after_snapshot: Option<SnapshotId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
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
    fn operation_model_accepts_current_cli_kinds_and_statuses() {
        let json = r#"{
            "id": "op-2",
            "parent": "op-1",
            "kind": "root-mark-deleted",
            "timestamp": "2026-06-07T00:00:00Z",
            "actor": "user@host",
            "status": "failed",
            "before_snapshot": "snap-1",
            "after_snapshot": "snap-1",
            "message": "sample"
        }"#;

        let op: Operation = serde_json::from_str(json).unwrap();

        assert_eq!(op.kind, OperationKind::RootMarkedDeleted);
        assert_eq!(op.status, OperationStatus::Failed);
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
