use std::collections::BTreeMap;

pub type HostId = String;
pub type RootId = String;
pub type SnapshotId = String;
pub type OperationId = String;
pub type ObjectKey = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Host {
    pub id: HostId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Root {
    pub id: RootId,
    pub name: String,
    pub path: String,
    pub status: RootStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootStatus {
    Active,
    Paused,
    Missing,
    Unmounted,
    PermissionDenied,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostSnapshot {
    pub id: SnapshotId,
    pub parent: Option<SnapshotId>,
    pub operation: OperationId,
    pub roots: BTreeMap<RootId, RootSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootSnapshot {
    pub tree_id: String,
    pub tree_key: ObjectKey,
    pub file_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    pub id: OperationId,
    pub parent: Option<OperationId>,
    pub kind: OperationKind,
    pub before_snapshot: Option<SnapshotId>,
    pub after_snapshot: Option<SnapshotId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
