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
    pub before_snapshot: Option<SnapshotId>,
    pub after_snapshot: Option<SnapshotId>,
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

fn default_chunk_compression() -> String {
    "none".into()
}
