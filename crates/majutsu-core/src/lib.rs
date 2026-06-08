use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryGraphIssue {
    SnapshotSelfParent {
        snapshot_id: String,
    },
    SnapshotMissingOperation {
        snapshot_id: String,
        operation_id: String,
    },
    OperationMissingParent {
        operation_id: String,
        parent_id: String,
    },
    OperationSelfParent {
        operation_id: String,
    },
}

pub fn history_graph_issues(
    snapshots: &[SnapshotExport],
    operations: &[OperationLogEntry],
) -> Vec<HistoryGraphIssue> {
    let operation_ids = operations
        .iter()
        .map(|operation| operation.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut issues = Vec::new();
    for snapshot in snapshots {
        if snapshot.parent_id.as_deref() == Some(snapshot.id.as_str()) {
            issues.push(HistoryGraphIssue::SnapshotSelfParent {
                snapshot_id: snapshot.id.clone(),
            });
        }
        if !operation_ids.contains(snapshot.op_id.as_str()) {
            issues.push(HistoryGraphIssue::SnapshotMissingOperation {
                snapshot_id: snapshot.id.clone(),
                operation_id: snapshot.op_id.clone(),
            });
        }
    }
    for operation in operations {
        if let Some(parent) = operation.parent_op.as_deref() {
            if !operation_ids.contains(parent) {
                issues.push(HistoryGraphIssue::OperationMissingParent {
                    operation_id: operation.id.clone(),
                    parent_id: parent.to_string(),
                });
            }
            if parent == operation.id {
                issues.push(HistoryGraphIssue::OperationSelfParent {
                    operation_id: operation.id.clone(),
                });
            }
        }
    }
    issues
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataReferenceIssue {
    DanglingBlob { oid: String },
    DanglingLargeObject { oid: String },
    DanglingChunk { oid: String },
}

pub fn metadata_reference_issues<B, L, C>(
    blob_oids: B,
    large_object_oids: L,
    chunk_oids: C,
    live_blobs: &BTreeSet<String>,
    live_large_objects: &BTreeSet<String>,
    live_chunks: &BTreeSet<String>,
) -> Vec<MetadataReferenceIssue>
where
    B: IntoIterator,
    B::Item: AsRef<str>,
    L: IntoIterator,
    L::Item: AsRef<str>,
    C: IntoIterator,
    C::Item: AsRef<str>,
{
    let mut issues = Vec::new();
    for oid in blob_oids {
        let oid = oid.as_ref();
        if !live_blobs.contains(oid) {
            issues.push(MetadataReferenceIssue::DanglingBlob {
                oid: oid.to_string(),
            });
        }
    }
    for oid in large_object_oids {
        let oid = oid.as_ref();
        if !live_large_objects.contains(oid) {
            issues.push(MetadataReferenceIssue::DanglingLargeObject {
                oid: oid.to_string(),
            });
        }
    }
    for oid in chunk_oids {
        let oid = oid.as_ref();
        if !live_chunks.contains(oid) {
            issues.push(MetadataReferenceIssue::DanglingChunk {
                oid: oid.to_string(),
            });
        }
    }
    issues
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveMetadataReferences {
    pub blobs: BTreeSet<String>,
    pub large_objects: BTreeSet<String>,
    pub chunks: BTreeSet<String>,
}

impl LiveMetadataReferences {
    pub fn add_snapshot_manifest(&mut self, manifest: &SnapshotManifest) -> Vec<String> {
        let mut large_manifest_keys = Vec::new();
        for records in manifest.roots.values() {
            for record in records {
                if let Some((oid, _)) = payload_blob_ref(&record.payload) {
                    self.blobs.insert(oid.to_string());
                } else if let Some((oid, manifest_key, _)) = payload_large_ref(&record.payload) {
                    self.large_objects.insert(oid.to_string());
                    large_manifest_keys.push(manifest_key.to_string());
                }
            }
        }
        large_manifest_keys
    }

    pub fn add_large_manifest(&mut self, manifest: LargeManifest) {
        for chunk in manifest.chunks {
            self.chunks.insert(chunk.oid);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostFileIssue {
    IdMismatch {
        host_id: String,
        config_host_id: String,
    },
    NameMismatch {
        host_name: String,
        config_host_name: String,
    },
}

pub fn host_file_issues(
    host_id: &str,
    host_name: &str,
    config_host_id: &str,
    config_host_name: &str,
) -> Vec<HostFileIssue> {
    let mut issues = Vec::new();
    if host_id != config_host_id {
        issues.push(HostFileIssue::IdMismatch {
            host_id: host_id.to_string(),
            config_host_id: config_host_id.to_string(),
        });
    }
    if host_name != config_host_name {
        issues.push(HostFileIssue::NameMismatch {
            host_name: host_name.to_string(),
            config_host_name: config_host_name.to_string(),
        });
    }
    issues
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigRootIssue {
    DuplicateRootId { id: String },
    DatabaseMissingConfig { id: String },
    ConfigMissingDatabase { id: String },
    ConfigMismatch { id: String },
}

pub fn config_root_consistency_issues<I, J, K, L>(
    all_config_root_ids: I,
    valid_config_root_ids: J,
    db_root_ids: K,
    mismatched_root_ids: L,
) -> Vec<ConfigRootIssue>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
    J: IntoIterator,
    J::Item: AsRef<str>,
    K: IntoIterator,
    K::Item: AsRef<str>,
    L: IntoIterator,
    L::Item: AsRef<str>,
{
    let mut issues = Vec::new();
    let mut seen_config_ids = BTreeSet::new();
    for id in all_config_root_ids {
        let id = id.as_ref();
        if !seen_config_ids.insert(id.to_string()) {
            issues.push(ConfigRootIssue::DuplicateRootId { id: id.to_string() });
        }
    }
    let valid_config_root_ids = valid_config_root_ids
        .into_iter()
        .map(|id| id.as_ref().to_string())
        .collect::<BTreeSet<_>>();
    let db_root_ids = db_root_ids
        .into_iter()
        .map(|id| id.as_ref().to_string())
        .collect::<BTreeSet<_>>();

    for id in &db_root_ids {
        if !valid_config_root_ids.contains(id) {
            issues.push(ConfigRootIssue::DatabaseMissingConfig { id: id.clone() });
        }
    }
    for id in &valid_config_root_ids {
        if !db_root_ids.contains(id) {
            issues.push(ConfigRootIssue::ConfigMissingDatabase { id: id.clone() });
        }
    }
    for id in mismatched_root_ids {
        let id = id.as_ref();
        if valid_config_root_ids.contains(id) && db_root_ids.contains(id) {
            issues.push(ConfigRootIssue::ConfigMismatch { id: id.to_string() });
        }
    }
    issues
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
    LifecycleApply,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_sync_state: Option<String>,
}

impl OperationLogEntry {
    pub fn validation_issues(&self) -> Vec<OperationLogEntryIssue> {
        let mut issues = Vec::new();
        if !self.id.starts_with("op-") {
            issues.push(OperationLogEntryIssue::InvalidId);
        }
        if !valid_operation_kind_label(&self.kind) {
            issues.push(OperationLogEntryIssue::InvalidKind(self.kind.clone()));
        }
        if !valid_operation_status_label(&self.status) {
            issues.push(OperationLogEntryIssue::InvalidStatus(self.status.clone()));
        }
        if let Some(remote_sync_state) = &self.remote_sync_state
            && !valid_remote_sync_state_label(remote_sync_state)
        {
            issues.push(OperationLogEntryIssue::InvalidRemoteSyncState(
                remote_sync_state.clone(),
            ));
        }
        if self.status == "failed" && self.error.as_deref().unwrap_or_default().trim().is_empty() {
            issues.push(OperationLogEntryIssue::FailedWithoutError);
        }
        if self.kind == "remote-sync" {
            match self.remote_sync_state.as_deref() {
                None => issues.push(OperationLogEntryIssue::RemoteSyncMissingState),
                Some("queued") if self.status == "running" => {}
                Some("synced") if self.status == "done" => {}
                Some("failed") if self.status == "failed" => {}
                Some(state) if valid_remote_sync_state_label(state) => {
                    issues.push(OperationLogEntryIssue::RemoteSyncStateMismatch {
                        status: self.status.clone(),
                        remote_sync_state: state.to_string(),
                    });
                }
                _ => {}
            }
        }
        if self.actor.trim().is_empty() {
            issues.push(OperationLogEntryIssue::EmptyActor);
        }
        if let Err(err) = DateTime::parse_from_rfc3339(&self.created_at) {
            issues.push(OperationLogEntryIssue::InvalidCreatedAt {
                value: self.created_at.clone(),
                error: err.to_string(),
            });
        }
        issues
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationLogEntryIssue {
    InvalidId,
    InvalidKind(String),
    InvalidStatus(String),
    InvalidRemoteSyncState(String),
    FailedWithoutError,
    RemoteSyncMissingState,
    RemoteSyncStateMismatch {
        status: String,
        remote_sync_state: String,
    },
    EmptyActor,
    InvalidCreatedAt {
        value: String,
        error: String,
    },
}

pub fn valid_operation_kind_label(kind: &str) -> bool {
    matches!(
        kind,
        "init"
            | "root-added"
            | "config-change"
            | "root-removed"
            | "root-paused"
            | "root-resumed"
            | "root-mark-deleted"
            | "root-missing"
            | "root-unmounted"
            | "root-permission-denied"
            | "initial-scan"
            | "manual-snapshot"
            | "file-events-batch"
            | "op-restore"
            | "restore"
            | "restore-prepare"
            | "restore-resume"
            | "mount"
            | "mount-fuse"
            | "unmount"
            | "unmount-fuse"
            | "hydrate"
            | "large-pin"
            | "large-unpin"
            | "remote-sync"
            | "key-rotation"
            | "pack"
            | "pack-compact"
            | "prune"
            | "gc"
            | "lifecycle-apply"
            | "fsck"
    )
}

pub fn valid_operation_status_label(status: &str) -> bool {
    matches!(status, "done" | "running" | "failed")
}

pub fn valid_remote_sync_state_label(state: &str) -> bool {
    matches!(state, "not-synced" | "queued" | "synced" | "failed")
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationLogComparisonIssue {
    CountMismatch { expected: usize, actual: usize },
    EntryMismatch { index: usize, id: String },
}

pub fn operation_log_comparison_issues(
    actual: &[OperationLogEntry],
    expected: &[OperationLogEntry],
) -> Vec<OperationLogComparisonIssue> {
    if actual.len() != expected.len() {
        return vec![OperationLogComparisonIssue::CountMismatch {
            expected: expected.len(),
            actual: actual.len(),
        }];
    }
    actual
        .iter()
        .zip(expected.iter())
        .enumerate()
        .filter(|(_, (actual, expected))| !operation_log_entry_matches(actual, expected))
        .map(
            |(index, (actual, _))| OperationLogComparisonIssue::EntryMismatch {
                index,
                id: actual.id.clone(),
            },
        )
        .collect()
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

pub fn snapshot_manifest_matches(actual: &SnapshotManifest, expected: &SnapshotManifest) -> bool {
    serde_json::to_value(actual).ok() == serde_json::to_value(expected).ok()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeManifestIssue {
    RootIdMismatch { expected: String, actual: String },
    TreeIdMismatch { expected: String, actual: String },
    FileCountMismatch { expected: usize, actual: usize },
}

pub fn tree_manifest_issues(
    actual: &TreeManifest,
    expected_root_id: &str,
    expected: &RootSnapshot,
) -> Vec<TreeManifestIssue> {
    let mut issues = Vec::new();
    if actual.root_id != expected_root_id {
        issues.push(TreeManifestIssue::RootIdMismatch {
            expected: expected_root_id.to_string(),
            actual: actual.root_id.clone(),
        });
    }
    if actual.tree_id != expected.tree_id {
        issues.push(TreeManifestIssue::TreeIdMismatch {
            expected: expected.tree_id.clone(),
            actual: actual.tree_id.clone(),
        });
    }
    if actual.entries.len() != expected.file_count {
        issues.push(TreeManifestIssue::FileCountMismatch {
            expected: expected.file_count,
            actual: actual.entries.len(),
        });
    }
    issues
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
    fn metadata_reference_validation_reports_dangling_metadata() {
        let issues = metadata_reference_issues(
            ["blob-live", "blob-dangling"],
            ["large-live", "large-dangling"],
            ["chunk-live", "chunk-dangling"],
            &BTreeSet::from(["blob-live".to_string()]),
            &BTreeSet::from(["large-live".to_string()]),
            &BTreeSet::from(["chunk-live".to_string()]),
        );

        assert_eq!(
            issues,
            vec![
                MetadataReferenceIssue::DanglingBlob {
                    oid: "blob-dangling".into(),
                },
                MetadataReferenceIssue::DanglingLargeObject {
                    oid: "large-dangling".into(),
                },
                MetadataReferenceIssue::DanglingChunk {
                    oid: "chunk-dangling".into(),
                },
            ]
        );
    }

    #[test]
    fn metadata_reference_validation_accepts_live_metadata() {
        assert!(
            metadata_reference_issues(
                ["blob-live"],
                ["large-live"],
                ["chunk-live"],
                &BTreeSet::from(["blob-live".to_string()]),
                &BTreeSet::from(["large-live".to_string()]),
                &BTreeSet::from(["chunk-live".to_string()]),
            )
            .is_empty()
        );
    }

    #[test]
    fn live_metadata_references_track_snapshot_and_large_manifest_objects() {
        let mut refs = LiveMetadataReferences::default();
        let manifest = SnapshotManifest {
            snapshot_id: "snap-1".into(),
            parent: None,
            op_id: "op-1".into(),
            timestamp: DateTime::<Utc>::UNIX_EPOCH,
            root_trees: BTreeMap::new(),
            roots: BTreeMap::from([(
                "root-a".into(),
                vec![
                    FileRecord {
                        root_id: "root-a".into(),
                        path: "small.txt".into(),
                        kind: "file".into(),
                        size: 5,
                        mode: 0,
                        modified: None,
                        uid: None,
                        gid: None,
                        xattrs: BTreeMap::new(),
                        payload: Payload::NormalBlob {
                            oid: "blob-1".into(),
                            object_key: "objects/blobs/blob-1".into(),
                        },
                    },
                    FileRecord {
                        root_id: "root-a".into(),
                        path: "large.bin".into(),
                        kind: "file".into(),
                        size: 10,
                        mode: 0,
                        modified: None,
                        uid: None,
                        gid: None,
                        xattrs: BTreeMap::new(),
                        payload: Payload::LargeObject {
                            oid: "large-1".into(),
                            manifest_key: "objects/large/large-1.json".into(),
                            chunk_count: 1,
                            media_type: None,
                            binary: true,
                            chunking: "fixed".into(),
                            compression: "none".into(),
                            encryption: "none".into(),
                            storage_tier_hint: "standard".into(),
                            hydrate_policy: "on-demand".into(),
                        },
                    },
                ],
            )]),
        };

        let manifest_keys = refs.add_snapshot_manifest(&manifest);
        refs.add_large_manifest(LargeManifest {
            version: 1,
            oid: "large-1".into(),
            size: 10,
            media_type: None,
            binary: true,
            chunking: "fixed".into(),
            chunk_size: 10,
            chunks: vec![LargeChunk {
                index: 0,
                offset: 0,
                len: 10,
                stored_len: None,
                compression: "none".into(),
                oid: "chunk-1".into(),
                object_key: "objects/large/chunks/chunk-1".into(),
            }],
        });

        assert_eq!(manifest_keys, vec!["objects/large/large-1.json"]);
        assert!(refs.blobs.contains("blob-1"));
        assert!(refs.large_objects.contains("large-1"));
        assert!(refs.chunks.contains("chunk-1"));
    }

    #[test]
    fn host_file_validation_reports_config_drift() {
        assert_eq!(
            host_file_issues("host-file", "old-name", "host-config", "new-name"),
            vec![
                HostFileIssue::IdMismatch {
                    host_id: "host-file".into(),
                    config_host_id: "host-config".into(),
                },
                HostFileIssue::NameMismatch {
                    host_name: "old-name".into(),
                    config_host_name: "new-name".into(),
                },
            ]
        );
        assert!(host_file_issues("host-a", "alice", "host-a", "alice").is_empty());
    }

    #[test]
    fn config_root_consistency_validation_reports_config_and_db_drift() {
        let issues = config_root_consistency_issues(
            ["alpha", "alpha", "config-only", "drift"],
            ["alpha", "config-only", "drift"],
            ["db-only", "drift"],
            ["drift", "not-common"],
        );

        assert_eq!(
            issues,
            vec![
                ConfigRootIssue::DuplicateRootId { id: "alpha".into() },
                ConfigRootIssue::DatabaseMissingConfig {
                    id: "db-only".into()
                },
                ConfigRootIssue::ConfigMissingDatabase { id: "alpha".into() },
                ConfigRootIssue::ConfigMissingDatabase {
                    id: "config-only".into(),
                },
                ConfigRootIssue::ConfigMismatch { id: "drift".into() },
            ]
        );
    }

    #[test]
    fn snapshot_manifest_comparison_uses_stable_json_shape() {
        let manifest = SnapshotManifest {
            snapshot_id: "snap-1".into(),
            parent: None,
            op_id: "op-1".into(),
            timestamp: DateTime::<Utc>::UNIX_EPOCH,
            root_trees: BTreeMap::new(),
            roots: BTreeMap::new(),
        };
        let mut changed = SnapshotManifest {
            snapshot_id: "snap-2".into(),
            parent: None,
            op_id: "op-1".into(),
            timestamp: DateTime::<Utc>::UNIX_EPOCH,
            root_trees: BTreeMap::new(),
            roots: BTreeMap::new(),
        };

        assert!(snapshot_manifest_matches(&manifest, &manifest));
        assert!(!snapshot_manifest_matches(&changed, &manifest));
        changed.snapshot_id = "snap-1".into();
        assert!(snapshot_manifest_matches(&changed, &manifest));
    }

    #[test]
    fn tree_manifest_validation_reports_snapshot_metadata_drift() {
        let tree = TreeManifest {
            version: 1,
            tree_id: "tree-actual".into(),
            root_id: "root-actual".into(),
            created_at: DateTime::<Utc>::UNIX_EPOCH,
            entries: BTreeMap::new(),
        };
        let expected = RootSnapshot {
            tree_id: "tree-expected".into(),
            tree_key: "objects/trees/tree-expected.json".into(),
            file_count: 1,
        };

        assert_eq!(
            tree_manifest_issues(&tree, "root-expected", &expected),
            vec![
                TreeManifestIssue::RootIdMismatch {
                    expected: "root-expected".into(),
                    actual: "root-actual".into(),
                },
                TreeManifestIssue::TreeIdMismatch {
                    expected: "tree-expected".into(),
                    actual: "tree-actual".into(),
                },
                TreeManifestIssue::FileCountMismatch {
                    expected: 1,
                    actual: 0,
                },
            ]
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
    fn history_graph_validation_reports_snapshot_and_operation_edges() {
        let snapshots = vec![
            SnapshotExport {
                id: "snap-1".into(),
                parent_id: Some("snap-1".into()),
                op_id: "op-missing".into(),
                created_at: "2026-06-07T00:00:00Z".into(),
                manifest_key: "objects/snapshots/snap-1.json".into(),
                manifest_json: "{}".into(),
            },
            SnapshotExport {
                id: "snap-2".into(),
                parent_id: Some("snap-1".into()),
                op_id: "op-1".into(),
                created_at: "2026-06-07T00:01:00Z".into(),
                manifest_key: "objects/snapshots/snap-2.json".into(),
                manifest_json: "{}".into(),
            },
        ];
        let operations = vec![
            OperationLogEntry {
                id: "op-1".into(),
                parent_op: Some("op-2".into()),
                kind: "init".into(),
                actor: "alice@host".into(),
                status: "done".into(),
                before_snapshot: None,
                after_snapshot: Some("snap-1".into()),
                created_at: "2026-06-07T00:00:00Z".into(),
                message: None,
                error: None,
                remote_sync_state: None,
            },
            OperationLogEntry {
                id: "op-self".into(),
                parent_op: Some("op-self".into()),
                kind: "manual-snapshot".into(),
                actor: "alice@host".into(),
                status: "done".into(),
                before_snapshot: Some("snap-1".into()),
                after_snapshot: Some("snap-2".into()),
                created_at: "2026-06-07T00:01:00Z".into(),
                message: None,
                error: None,
                remote_sync_state: None,
            },
        ];

        assert_eq!(
            history_graph_issues(&snapshots, &operations),
            vec![
                HistoryGraphIssue::SnapshotSelfParent {
                    snapshot_id: "snap-1".into(),
                },
                HistoryGraphIssue::SnapshotMissingOperation {
                    snapshot_id: "snap-1".into(),
                    operation_id: "op-missing".into(),
                },
                HistoryGraphIssue::OperationMissingParent {
                    operation_id: "op-1".into(),
                    parent_id: "op-2".into(),
                },
                HistoryGraphIssue::OperationSelfParent {
                    operation_id: "op-self".into(),
                },
            ]
        );
    }

    #[test]
    fn history_graph_validation_accepts_linked_history() {
        let snapshots = vec![SnapshotExport {
            id: "snap-1".into(),
            parent_id: None,
            op_id: "op-1".into(),
            created_at: "2026-06-07T00:00:00Z".into(),
            manifest_key: "objects/snapshots/snap-1.json".into(),
            manifest_json: "{}".into(),
        }];
        let operations = vec![OperationLogEntry {
            id: "op-1".into(),
            parent_op: None,
            kind: "init".into(),
            actor: "alice@host".into(),
            status: "done".into(),
            before_snapshot: None,
            after_snapshot: Some("snap-1".into()),
            created_at: "2026-06-07T00:00:00Z".into(),
            message: None,
            error: None,
            remote_sync_state: None,
        }];

        assert!(history_graph_issues(&snapshots, &operations).is_empty());
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
            "initial-scan",
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
            "lifecycle-apply",
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
                error: None,
                remote_sync_state: Some("synced".into()),
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
                error: Some("snapshot hook failed".into()),
                remote_sync_state: Some("failed".into()),
            },
        ];

        let encoded = encode_operation_log(&operations).unwrap();
        let decoded = decode_operation_log(&encoded).unwrap();

        assert_eq!(decoded, operations);
        assert!(operation_log_entry_matches(&decoded[0], &operations[0]));
    }

    #[test]
    fn operation_log_entry_validation_reports_fsck_issues() {
        let entry = OperationLogEntry {
            id: "bad-1".into(),
            parent_op: None,
            kind: "unknown".into(),
            actor: "  ".into(),
            status: "stuck".into(),
            before_snapshot: None,
            after_snapshot: None,
            created_at: "not-time".into(),
            message: None,
            error: None,
            remote_sync_state: Some("stale".into()),
        };

        let issues = entry.validation_issues();

        assert_eq!(issues.len(), 6);
        assert!(issues.contains(&OperationLogEntryIssue::InvalidId));
        assert!(issues.contains(&OperationLogEntryIssue::InvalidKind("unknown".into())));
        assert!(issues.contains(&OperationLogEntryIssue::InvalidStatus("stuck".into())));
        assert!(
            issues.contains(&OperationLogEntryIssue::InvalidRemoteSyncState(
                "stale".into()
            ))
        );
        assert!(issues.contains(&OperationLogEntryIssue::EmptyActor));
        assert!(issues.iter().any(|issue| matches!(
            issue,
            OperationLogEntryIssue::InvalidCreatedAt { value, .. } if value == "not-time"
        )));
    }

    #[test]
    fn operation_log_entry_validation_accepts_current_labels() {
        let entry = OperationLogEntry {
            id: "op-1".into(),
            parent_op: None,
            kind: "manual-snapshot".into(),
            actor: "alice@host".into(),
            status: "done".into(),
            before_snapshot: None,
            after_snapshot: Some("snap-1".into()),
            created_at: "2026-06-07T00:00:00Z".into(),
            message: None,
            error: None,
            remote_sync_state: Some("not-synced".into()),
        };

        assert!(entry.validation_issues().is_empty());
        assert!(valid_operation_kind_label("root-permission-denied"));
        assert!(valid_operation_status_label("failed"));
        assert!(valid_remote_sync_state_label("queued"));
        assert!(valid_remote_sync_state_label("synced"));
        assert!(!valid_remote_sync_state_label("stale"));
    }

    #[test]
    fn operation_log_entry_validation_reports_state_inconsistency() {
        let failed_without_error = OperationLogEntry {
            id: "op-1".into(),
            parent_op: None,
            kind: "manual-snapshot".into(),
            actor: "alice@host".into(),
            status: "failed".into(),
            before_snapshot: None,
            after_snapshot: None,
            created_at: "2026-06-07T00:00:00Z".into(),
            message: Some("snapshot failed".into()),
            error: None,
            remote_sync_state: None,
        };
        assert_eq!(
            failed_without_error.validation_issues(),
            vec![OperationLogEntryIssue::FailedWithoutError]
        );

        let remote_sync_without_state = OperationLogEntry {
            id: "op-2".into(),
            parent_op: Some("op-1".into()),
            kind: "remote-sync".into(),
            actor: "alice@host".into(),
            status: "done".into(),
            before_snapshot: None,
            after_snapshot: None,
            created_at: "2026-06-07T00:01:00Z".into(),
            message: None,
            error: None,
            remote_sync_state: None,
        };
        assert_eq!(
            remote_sync_without_state.validation_issues(),
            vec![OperationLogEntryIssue::RemoteSyncMissingState]
        );

        let remote_sync_mismatch = OperationLogEntry {
            id: "op-3".into(),
            parent_op: Some("op-2".into()),
            kind: "remote-sync".into(),
            actor: "alice@host".into(),
            status: "done".into(),
            before_snapshot: None,
            after_snapshot: None,
            created_at: "2026-06-07T00:02:00Z".into(),
            message: None,
            error: None,
            remote_sync_state: Some("queued".into()),
        };
        assert_eq!(
            remote_sync_mismatch.validation_issues(),
            vec![OperationLogEntryIssue::RemoteSyncStateMismatch {
                status: "done".into(),
                remote_sync_state: "queued".into(),
            }]
        );
    }

    #[test]
    fn operation_log_comparison_reports_count_and_entry_mismatches() {
        let expected = vec![OperationLogEntry {
            id: "op-1".into(),
            parent_op: None,
            kind: "init".into(),
            actor: "alice@host".into(),
            status: "done".into(),
            before_snapshot: None,
            after_snapshot: Some("snap-1".into()),
            created_at: "2026-06-07T00:00:00Z".into(),
            message: None,
            error: None,
            remote_sync_state: None,
        }];
        let mut actual = expected.clone();
        actual[0].status = "failed".into();

        assert_eq!(
            operation_log_comparison_issues(&actual, &expected),
            vec![OperationLogComparisonIssue::EntryMismatch {
                index: 0,
                id: "op-1".into(),
            }]
        );
        assert_eq!(
            operation_log_comparison_issues(&[], &expected),
            vec![OperationLogComparisonIssue::CountMismatch {
                expected: 1,
                actual: 0,
            }]
        );
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
