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
    use super::{RestorePathState, count_restore_changes};

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
}
