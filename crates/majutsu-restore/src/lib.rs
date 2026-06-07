use chrono::{DateTime, Utc};
use majutsu_core::{ObjectKey, RootId, SnapshotId};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreQueueItem {
    pub id: String,
    pub snapshot_id: SnapshotId,
    pub root: Option<RootId>,
    pub path: Option<String>,
    pub target: String,
    pub required_objects: Vec<ObjectKey>,
    pub archived_objects: Vec<ObjectKey>,
    #[serde(default)]
    pub missing_objects: Vec<ObjectKey>,
    #[serde(default)]
    pub archive_requested_objects: Vec<ObjectKey>,
    #[serde(default)]
    pub force: bool,
    #[serde(default = "default_check_conflicts")]
    pub check_conflicts: bool,
    pub created_at: DateTime<Utc>,
    pub status: String,
}

impl RestoreQueueItem {
    pub fn is_resumable(&self) -> bool {
        matches!(
            self.status.as_str(),
            "prepared" | "ready" | "archive-requested"
        )
    }

    pub fn mark_archive_requested(&mut self, requested: Vec<ObjectKey>) {
        if !requested.is_empty() {
            self.status = "archive-requested".into();
            self.archive_requested_objects = requested;
        }
    }

    pub fn mark_ready_if_archives_hydrated(&mut self) {
        self.archive_requested_objects
            .retain(|key| self.archived_objects.contains(key));
        if self.archived_objects.is_empty() {
            self.status = "ready".into();
        }
    }

    pub fn mark_done(&mut self) {
        self.status = "done".into();
    }
}

fn default_check_conflicts() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestorePathState {
    Missing,
    Matches,
    Differs,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreChangeStats {
    pub restore_files: usize,
    pub modify_files: usize,
    pub keep_files: usize,
    pub delete_files: usize,
}

pub fn count_restore_changes<'a, T, E, F>(
    files: &'a [T],
    delete_count: usize,
    mut classify: F,
) -> Result<RestoreChangeStats, E>
where
    F: FnMut(&'a T) -> Result<RestorePathState, E>,
{
    let mut stats = RestoreChangeStats {
        delete_files: delete_count,
        ..RestoreChangeStats::default()
    };
    for file in files {
        match classify(file)? {
            RestorePathState::Missing => stats.restore_files += 1,
            RestorePathState::Matches => stats.keep_files += 1,
            RestorePathState::Differs => stats.modify_files += 1,
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::{RestorePathState, RestoreQueueItem, count_restore_changes};
    use chrono::{DateTime, Utc};

    #[test]
    fn counts_restore_change_categories() {
        let states = [
            RestorePathState::Missing,
            RestorePathState::Matches,
            RestorePathState::Differs,
            RestorePathState::Differs,
        ];
        let stats = count_restore_changes(&states, 3, |state| Ok::<_, ()>(*state)).unwrap();

        assert_eq!(stats.restore_files, 1);
        assert_eq!(stats.keep_files, 1);
        assert_eq!(stats.modify_files, 2);
        assert_eq!(stats.delete_files, 3);
    }

    #[test]
    fn restore_queue_item_defaults_preserve_legacy_jobs() {
        let json = r#"{
            "id": "restore-1",
            "snapshot_id": "snap-1",
            "root": null,
            "path": null,
            "target": "original-roots",
            "required_objects": ["objects/blobs/aa"],
            "archived_objects": [],
            "created_at": "2026-06-07T00:00:00Z",
            "status": "prepared"
        }"#;

        let job: RestoreQueueItem = serde_json::from_str(json).unwrap();

        assert!(job.missing_objects.is_empty());
        assert!(job.archive_requested_objects.is_empty());
        assert!(!job.force);
        assert!(job.check_conflicts);
        assert!(job.is_resumable());
    }

    #[test]
    fn restore_queue_item_tracks_archive_state_transitions() {
        let mut job = RestoreQueueItem {
            id: "restore-1".into(),
            snapshot_id: "snap-1".into(),
            root: Some("sample".into()),
            path: Some("subtree".into()),
            target: "/tmp/restore".into(),
            required_objects: vec!["objects/blobs/aa".into()],
            archived_objects: vec!["objects/blobs/aa".into()],
            missing_objects: Vec::new(),
            archive_requested_objects: Vec::new(),
            force: false,
            check_conflicts: true,
            created_at: DateTime::<Utc>::UNIX_EPOCH,
            status: "prepared".into(),
        };

        job.mark_archive_requested(vec!["objects/blobs/aa".into()]);
        assert_eq!(job.status, "archive-requested");
        assert_eq!(job.archive_requested_objects, vec!["objects/blobs/aa"]);
        assert!(job.is_resumable());

        job.archived_objects.clear();
        job.mark_ready_if_archives_hydrated();
        assert_eq!(job.status, "ready");
        assert!(job.archive_requested_objects.is_empty());

        job.mark_done();
        assert_eq!(job.status, "done");
        assert!(!job.is_resumable());
    }
}
