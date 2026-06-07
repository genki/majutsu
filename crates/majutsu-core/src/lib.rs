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
pub enum RootStatus {
    Active,
    Paused,
    Missing,
    Unmounted,
    PermissionDenied,
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
pub enum OperationStatus {
    #[default]
    Done,
    Running,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    InitialScan,
    FileEventsBatch,
    ManualSnapshot,
    ConfigChange,
    RootAdded,
    RootRemoved,
    Restore,
    RemoteSync,
    Prune,
    KeyRotation,
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
