use majutsu_core::{ObjectKey, RootId, SnapshotId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorePlanSummary {
    pub snapshot: SnapshotId,
    pub root: Option<RootId>,
    pub restore_files: usize,
    pub modify_files: usize,
    pub keep_files: usize,
    pub delete_files: usize,
    pub required_objects: Vec<ObjectKey>,
    pub missing_objects: Vec<ObjectKey>,
    pub archived_objects: Vec<ObjectKey>,
}
