#[cfg(not(feature = "standalone-crate"))]
use crate::majutsu_core;
use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone, Utc};
#[cfg(feature = "standalone-crate")]
#[allow(clippy::single_component_path_imports)]
use majutsu_core;
use majutsu_core::{ObjectKey, RootId, SnapshotId};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreObjectAvailability {
    pub required_objects: Vec<ObjectKey>,
    pub archived_objects: Vec<ObjectKey>,
    pub missing_objects: Vec<ObjectKey>,
}

pub fn classify_restore_object_availability<F, G, E>(
    required_objects: Vec<ObjectKey>,
    mut local_exists: F,
    mut remote_available: G,
) -> Result<RestoreObjectAvailability, E>
where
    F: FnMut(&ObjectKey) -> Result<bool, E>,
    G: FnMut(&ObjectKey) -> Result<bool, E>,
{
    let mut archived_objects = Vec::new();
    let mut missing_objects = Vec::new();
    for key in &required_objects {
        if local_exists(key)? {
            continue;
        }
        if remote_available(key)? {
            archived_objects.push(key.clone());
        } else {
            missing_objects.push(key.clone());
        }
    }
    Ok(RestoreObjectAvailability {
        required_objects,
        archived_objects,
        missing_objects,
    })
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
    pub fn has_valid_status(&self) -> bool {
        matches!(
            self.status.as_str(),
            "prepared" | "ready" | "archive-requested" | "done"
        )
    }

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

    pub fn has_duplicate_required_objects(&self) -> bool {
        let unique = self
            .required_objects
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        unique.len() != self.required_objects.len()
    }

    pub fn pending_objects_outside_required(&self) -> Vec<&ObjectKey> {
        let required = self
            .required_objects
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        self.archived_objects
            .iter()
            .chain(self.missing_objects.iter())
            .chain(self.archive_requested_objects.iter())
            .filter(|key| !required.contains(key))
            .collect()
    }

    pub fn done_with_pending_objects(&self) -> bool {
        self.status == "done"
            && (!self.archived_objects.is_empty() || !self.missing_objects.is_empty())
    }

    pub fn non_resumable_message(&self) -> Option<String> {
        if self.is_resumable() {
            None
        } else {
            Some(format!(
                "restore job {} is not resumable: status {}",
                self.id, self.status
            ))
        }
    }

    pub fn blocking_resume_message(&self) -> Option<String> {
        if let Some(message) = self.missing_objects_message() {
            Some(message)
        } else if !self.archived_objects.is_empty() {
            Some(format!(
                "restore job {} still has archived objects pending: {}",
                self.id,
                self.archived_objects.len()
            ))
        } else {
            None
        }
    }

    pub fn missing_objects_message(&self) -> Option<String> {
        if self.missing_objects.is_empty() {
            None
        } else {
            Some(format!(
                "restore job {} has missing objects: {}",
                self.id,
                self.missing_objects.len()
            ))
        }
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

pub fn validate_relative_filter_path(path: &Path, label: &str) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        bail!("{label} must be a relative path inside the selected root");
    }
    let mut has_component = false;
    for component in path.components() {
        match component {
            Component::Normal(_) => has_component = true,
            _ => bail!("{label} must not contain '.', '..', prefixes, or root separators"),
        }
    }
    if !has_component {
        bail!("{label} must not be empty");
    }
    Ok(())
}

pub fn parse_restore_time(input: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S") {
        return local_datetime(dt, input).map(|dt| dt.with_timezone(&Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow!("invalid date: {input}"))?;
        return local_datetime(dt, input).map(|dt| dt.with_timezone(&Utc));
    }
    if input == "now" {
        return Ok(now);
    }
    if let Some(dt) = parse_relative_ago(input, now)? {
        return Ok(dt);
    }
    bail!(
        "time must be RFC3339, YYYY-MM-DD HH:MM:SS, YYYY-MM-DD, relative ago, or now, got: {input}"
    );
}

fn local_datetime(naive: NaiveDateTime, input: &str) -> Result<DateTime<Local>> {
    match Local.from_local_datetime(&naive) {
        chrono::LocalResult::Single(value) => Ok(value),
        chrono::LocalResult::Ambiguous(earliest, _) => Ok(earliest),
        chrono::LocalResult::None => bail!("invalid local time: {input}"),
    }
}

pub fn parse_restore_time_rfc3339(input: &str, now: DateTime<Utc>) -> Result<String> {
    Ok(parse_restore_time(input, now)?.to_rfc3339())
}

pub fn parse_db_time(input: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(input)?.with_timezone(&Utc))
}

pub fn parse_relative_ago(input: &str, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
    let normalized = input.trim().to_ascii_lowercase();
    let Some(value) = normalized.strip_suffix(" ago") else {
        return Ok(None);
    };
    let compact = value.trim();
    if let Ok(dt) = parse_duration_ago(compact, now) {
        return Ok(Some(dt));
    }
    let parts = compact.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 {
        return Ok(None);
    }
    let number: i64 = parts[0].parse()?;
    let seconds = match parts[1] {
        "second" | "seconds" | "sec" | "secs" => number,
        "minute" | "minutes" | "min" | "mins" => number * 60,
        "hour" | "hours" => number * 60 * 60,
        "day" | "days" => number * 24 * 60 * 60,
        _ => return Ok(None),
    };
    Ok(Some(now - chrono::Duration::seconds(seconds)))
}

pub fn parse_duration_ago(input: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let (number, unit) = input.split_at(input.len().saturating_sub(1));
    let value: i64 = number.parse()?;
    let seconds = match unit {
        "d" => value * 24 * 60 * 60,
        "h" => value * 60 * 60,
        "m" => value * 60,
        "s" => value,
        _ => bail!("duration must use s, m, h, or d suffix: {input}"),
    };
    Ok(now - chrono::Duration::seconds(seconds))
}

#[cfg(test)]
mod tests {
    use super::{
        RestorePathState, RestoreQueueItem, classify_restore_object_availability,
        count_restore_changes, parse_db_time, parse_duration_ago, parse_relative_ago,
        parse_restore_time, parse_restore_time_rfc3339, validate_relative_filter_path,
    };
    use chrono::{DateTime, Local, TimeZone, Utc};
    use std::path::Path;

    fn time(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

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
    fn classifies_restore_object_availability_without_losing_required_order() {
        let required = vec![
            "objects/a".to_string(),
            "objects/b".to_string(),
            "objects/c".to_string(),
        ];
        let availability = classify_restore_object_availability(
            required.clone(),
            |key| -> anyhow::Result<bool> { Ok(key == "objects/a") },
            |key| -> anyhow::Result<bool> { Ok(key == "objects/b") },
        )
        .unwrap();

        assert_eq!(availability.required_objects, required);
        assert_eq!(availability.archived_objects, vec!["objects/b"]);
        assert_eq!(availability.missing_objects, vec!["objects/c"]);
    }

    #[test]
    fn classify_restore_object_availability_propagates_lookup_errors() {
        let err = classify_restore_object_availability(
            vec!["objects/a".to_string()],
            |_| -> anyhow::Result<bool> { Ok(false) },
            |_| -> anyhow::Result<bool> { anyhow::bail!("remote unavailable") },
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("remote unavailable"));
    }

    #[test]
    fn relative_filter_path_rejects_unsafe_paths() {
        validate_relative_filter_path(Path::new("subtree/file.txt"), "restore --path").unwrap();
        validate_relative_filter_path(Path::new("subtree"), "restore --path").unwrap();

        assert_eq!(
            validate_relative_filter_path(Path::new(""), "restore --path")
                .unwrap_err()
                .to_string(),
            "restore --path must be a relative path inside the selected root"
        );
        assert_eq!(
            validate_relative_filter_path(Path::new("/tmp/out"), "restore --path")
                .unwrap_err()
                .to_string(),
            "restore --path must be a relative path inside the selected root"
        );
        assert_eq!(
            validate_relative_filter_path(Path::new("../out"), "restore --path")
                .unwrap_err()
                .to_string(),
            "restore --path must not contain '.', '..', prefixes, or root separators"
        );
        assert_eq!(
            validate_relative_filter_path(Path::new("./out"), "restore --path")
                .unwrap_err()
                .to_string(),
            "restore --path must not contain '.', '..', prefixes, or root separators"
        );
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
        assert_eq!(
            job.non_resumable_message().as_deref(),
            Some("restore job restore-1 is not resumable: status done")
        );
    }

    #[test]
    fn restore_queue_item_validates_fsck_invariants() {
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

        assert!(job.has_valid_status());
        assert!(!job.has_duplicate_required_objects());
        assert!(job.pending_objects_outside_required().is_empty());
        assert!(!job.done_with_pending_objects());

        job.status = "unknown".into();
        assert!(!job.has_valid_status());

        job.status = "done".into();
        assert!(job.done_with_pending_objects());

        job.required_objects.push("objects/blobs/aa".into());
        assert!(job.has_duplicate_required_objects());

        job.archive_requested_objects = vec!["objects/blobs/bb".into()];
        let outside = job
            .pending_objects_outside_required()
            .into_iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(outside, vec!["objects/blobs/bb"]);
    }

    #[test]
    fn restore_queue_item_reports_resume_blockers_in_priority_order() {
        let mut job = RestoreQueueItem {
            id: "restore-1".into(),
            snapshot_id: "snap-1".into(),
            root: Some("sample".into()),
            path: None,
            target: "original-roots".into(),
            required_objects: vec!["objects/blobs/aa".into(), "objects/blobs/bb".into()],
            archived_objects: vec!["objects/blobs/aa".into()],
            missing_objects: vec!["objects/blobs/bb".into()],
            archive_requested_objects: vec!["objects/blobs/aa".into()],
            force: false,
            check_conflicts: true,
            created_at: DateTime::<Utc>::UNIX_EPOCH,
            status: "archive-requested".into(),
        };

        assert_eq!(
            job.blocking_resume_message().as_deref(),
            Some("restore job restore-1 has missing objects: 1")
        );
        job.missing_objects.clear();
        assert_eq!(
            job.blocking_resume_message().as_deref(),
            Some("restore job restore-1 still has archived objects pending: 1")
        );
        job.archived_objects.clear();
        assert!(job.blocking_resume_message().is_none());
    }

    #[test]
    fn restore_queue_item_done_allows_cleared_archives() {
        let job = RestoreQueueItem {
            id: "restore-1".into(),
            snapshot_id: "snap-1".into(),
            root: Some("sample".into()),
            path: None,
            target: "original-roots".into(),
            required_objects: vec!["objects/blobs/aa".into()],
            archived_objects: Vec::new(),
            missing_objects: Vec::new(),
            archive_requested_objects: vec!["objects/blobs/aa".into()],
            force: false,
            check_conflicts: true,
            created_at: DateTime::<Utc>::UNIX_EPOCH,
            status: "done".into(),
        };

        assert!(!job.done_with_pending_objects());
        assert!(job.pending_objects_outside_required().is_empty());
    }

    #[test]
    fn restore_time_accepts_spec_datetime_formats() {
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 12, 0, 0).unwrap();
        let local_datetime = |year, month, day, hour, min, sec| {
            Local
                .with_ymd_and_hms(year, month, day, hour, min, sec)
                .earliest()
                .unwrap()
                .with_timezone(&Utc)
        };

        assert_eq!(
            parse_restore_time("2026-06-06T14:05:00+09:00", now).unwrap(),
            Utc.with_ymd_and_hms(2026, 6, 6, 5, 5, 0).unwrap()
        );
        assert_eq!(
            parse_restore_time("2026-06-06 14:05:00", now).unwrap(),
            local_datetime(2026, 6, 6, 14, 5, 0)
        );
        assert_eq!(
            parse_restore_time("2026-06-06", now).unwrap(),
            local_datetime(2026, 6, 6, 0, 0, 0)
        );
        assert_eq!(parse_restore_time("now", now).unwrap(), now);
        assert_eq!(
            parse_restore_time_rfc3339("2026-06-06", now).unwrap(),
            local_datetime(2026, 6, 6, 0, 0, 0).to_rfc3339()
        );
    }

    #[test]
    fn relative_ago_forms_are_deterministic() {
        let now = time(1_000_000);

        assert_eq!(parse_duration_ago("10m", now).unwrap(), time(999_400));
        assert_eq!(parse_duration_ago("2h", now).unwrap(), time(992_800));
        assert_eq!(
            parse_relative_ago("10 minutes ago", now).unwrap(),
            Some(time(999_400))
        );
        assert_eq!(
            parse_relative_ago("1 day ago", now).unwrap(),
            Some(time(913_600))
        );
        assert_eq!(parse_relative_ago("yesterday", now).unwrap(), None);
    }

    #[test]
    fn db_time_accepts_rfc3339() {
        assert_eq!(
            parse_db_time("2026-06-06T14:05:00+09:00").unwrap(),
            Utc.with_ymd_and_hms(2026, 6, 6, 5, 5, 0).unwrap()
        );
    }
}
