#[cfg(not(feature = "standalone-crate"))]
use crate::majutsu_core;
use chrono::{DateTime, Datelike, Utc};
#[cfg(feature = "standalone-crate")]
use majutsu_core;
use majutsu_core::ObjectKey;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const SMALL_BLOB_MAX_SIZE: u64 = 128 * 1024;
pub const DEFAULT_SMALL_PACK_TARGET: u64 = 64 * 1024 * 1024;
pub const DEFAULT_NORMAL_PACK_TARGET: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackEntry {
    pub oid: String,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackIndex {
    pub version: u32,
    pub pack_id: String,
    pub pack_key: ObjectKey,
    pub entries: Vec<PackEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackExport {
    pub pack_id: String,
    pub pack_key: String,
    pub index_key: String,
    pub object_count: usize,
    pub size: u64,
}

impl PackExport {
    pub fn new(
        pack_id: String,
        pack_key: String,
        index_key: String,
        object_count: usize,
        size: u64,
    ) -> Self {
        Self {
            pack_id,
            pack_key,
            index_key,
            object_count,
            size,
        }
    }

    pub fn matches_index(&self, index: &PackIndex) -> bool {
        self.pack_id == index.pack_id
            && self.pack_key == index.pack_key
            && self.object_count == index.entries.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedBlobMetadata {
    pub oid: String,
    pub pack_offset: Option<u64>,
    pub pack_len: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackIndexIssue {
    PackMetadataMismatch,
    EntryWithoutBlobMetadata { oid: String },
    EntryOffsetMismatch { oid: String },
    MissingBlobEntry { oid: String },
}

pub fn pack_index_issues(
    pack: &PackExport,
    index: &PackIndex,
    expected_blobs: &[PackedBlobMetadata],
) -> Vec<PackIndexIssue> {
    let mut issues = Vec::new();
    if !pack.matches_index(index) || index.entries.len() != expected_blobs.len() {
        issues.push(PackIndexIssue::PackMetadataMismatch);
        return issues;
    }

    let mut seen = BTreeSet::new();
    for entry in &index.entries {
        let Some(blob) = expected_blobs.iter().find(|blob| blob.oid == entry.oid) else {
            issues.push(PackIndexIssue::EntryWithoutBlobMetadata {
                oid: entry.oid.clone(),
            });
            continue;
        };
        seen.insert(entry.oid.as_str());
        if blob.pack_offset != Some(entry.offset) || blob.pack_len != Some(entry.len) {
            issues.push(PackIndexIssue::EntryOffsetMismatch {
                oid: entry.oid.clone(),
            });
        }
    }
    for blob in expected_blobs {
        if !seen.contains(blob.oid.as_str()) {
            issues.push(PackIndexIssue::MissingBlobEntry {
                oid: blob.oid.clone(),
            });
        }
    }
    issues
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackObjectIssue {
    SizeMismatch,
    MissingBlobOffset { oid: String },
    MissingBlobLength { oid: String },
    BlobRangeOutOfBounds { oid: String },
}

pub fn pack_object_issues(
    pack: &PackExport,
    actual_size: u64,
    expected_blobs: &[PackedBlobMetadata],
) -> Vec<PackObjectIssue> {
    let mut issues = Vec::new();
    if actual_size != pack.size {
        issues.push(PackObjectIssue::SizeMismatch);
    }
    for blob in expected_blobs {
        let Some(offset) = blob.pack_offset else {
            issues.push(PackObjectIssue::MissingBlobOffset {
                oid: blob.oid.clone(),
            });
            continue;
        };
        let Some(len) = blob.pack_len else {
            issues.push(PackObjectIssue::MissingBlobLength {
                oid: blob.oid.clone(),
            });
            continue;
        };
        if offset.checked_add(len).is_none_or(|end| end > pack.size)
            || offset.checked_add(len).is_none_or(|end| end > actual_size)
        {
            issues.push(PackObjectIssue::BlobRangeOutOfBounds {
                oid: blob.oid.clone(),
            });
        }
    }
    issues
}

pub fn missing_pack_metadata_ids<'a>(
    blob_pack_ids: impl IntoIterator<Item = &'a str>,
    metadata_pack_ids: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let metadata_pack_ids = metadata_pack_ids.into_iter().collect::<BTreeSet<_>>();
    let mut missing = Vec::new();
    for pack_id in blob_pack_ids {
        if !metadata_pack_ids.contains(pack_id) && !missing.contains(&pack_id.to_string()) {
            missing.push(pack_id.to_string());
        }
    }
    missing
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackDatePrefixes {
    pub pack_prefix: String,
    pub index_prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackTier {
    Small,
    Normal,
}

impl PackTier {
    pub fn as_str(self) -> &'static str {
        match self {
            PackTier::Small => "small",
            PackTier::Normal => "normal",
        }
    }
}

pub fn tier_for_blob(size: u64) -> PackTier {
    if size <= SMALL_BLOB_MAX_SIZE {
        PackTier::Small
    } else {
        PackTier::Normal
    }
}

pub fn date_prefixes(tier: PackTier, now: DateTime<Utc>) -> PackDatePrefixes {
    PackDatePrefixes {
        pack_prefix: format!(
            "objects/packs/{}/{:04}/{:02}/{:02}",
            tier.as_str(),
            now.year(),
            now.month(),
            now.day()
        ),
        index_prefix: format!("objects/indexes/pack/{:04}/{:02}", now.year(), now.month()),
    }
}

pub fn pack_key(prefix: &str, pack_id: &str) -> String {
    format!("{prefix}/{pack_id}.mpack")
}

pub fn index_key(prefix: &str, pack_id: &str) -> String {
    format!("{prefix}/{pack_id}.json")
}

pub fn default_small_pack_target() -> u64 {
    DEFAULT_SMALL_PACK_TARGET
}

pub fn default_normal_pack_target() -> u64 {
    DEFAULT_NORMAL_PACK_TARGET
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn classifies_small_and_normal_blob_tiers() {
        assert_eq!(tier_for_blob(0), PackTier::Small);
        assert_eq!(tier_for_blob(SMALL_BLOB_MAX_SIZE), PackTier::Small);
        assert_eq!(tier_for_blob(SMALL_BLOB_MAX_SIZE + 1), PackTier::Normal);
    }

    #[test]
    fn date_prefixes_match_pack_layout() {
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 12, 0, 0).unwrap();
        let prefixes = date_prefixes(PackTier::Normal, now);

        assert_eq!(prefixes.pack_prefix, "objects/packs/normal/2026/06/07");
        assert_eq!(prefixes.index_prefix, "objects/indexes/pack/2026/06");
        assert_eq!(
            pack_key(&prefixes.pack_prefix, "pack-1"),
            "objects/packs/normal/2026/06/07/pack-1.mpack"
        );
        assert_eq!(
            index_key(&prefixes.index_prefix, "pack-1"),
            "objects/indexes/pack/2026/06/pack-1.json"
        );
    }

    #[test]
    fn default_pack_targets_match_spec_config_defaults() {
        assert_eq!(default_small_pack_target(), 64 * 1024 * 1024);
        assert_eq!(default_normal_pack_target(), 256 * 1024 * 1024);
        assert!(default_small_pack_target() < default_normal_pack_target());
    }

    #[test]
    fn pack_export_json_shape_is_stable() {
        let export = PackExport::new(
            "pack-1".into(),
            "objects/packs/normal/2026/06/07/pack-1.mpack".into(),
            "objects/indexes/pack/2026/06/pack-1.json".into(),
            2,
            128,
        );

        let value = serde_json::to_value(export).unwrap();

        assert_eq!(value["pack_id"], "pack-1");
        assert_eq!(
            value["pack_key"],
            "objects/packs/normal/2026/06/07/pack-1.mpack"
        );
        assert_eq!(
            value["index_key"],
            "objects/indexes/pack/2026/06/pack-1.json"
        );
        assert_eq!(value["object_count"], 2);
        assert_eq!(value["size"], 128);
    }

    #[test]
    fn pack_export_matches_pack_index_metadata() {
        let index = PackIndex {
            version: 1,
            pack_id: "pack-1".into(),
            pack_key: "objects/packs/normal/pack-1.mpack".into(),
            entries: vec![
                PackEntry {
                    oid: "a".into(),
                    offset: 0,
                    len: 4,
                },
                PackEntry {
                    oid: "b".into(),
                    offset: 4,
                    len: 4,
                },
            ],
        };
        let export = PackExport::new(
            "pack-1".into(),
            "objects/packs/normal/pack-1.mpack".into(),
            "objects/indexes/pack/pack-1.json".into(),
            2,
            8,
        );

        assert!(export.matches_index(&index));
        assert!(
            !PackExport {
                object_count: 1,
                ..export
            }
            .matches_index(&index)
        );
    }

    #[test]
    fn pack_index_validation_reports_blob_metadata_mismatches() {
        let pack = PackExport::new(
            "pack-1".into(),
            "objects/packs/normal/pack-1.mpack".into(),
            "objects/indexes/pack/pack-1.json".into(),
            3,
            16,
        );
        let index = PackIndex {
            version: 1,
            pack_id: "pack-1".into(),
            pack_key: "objects/packs/normal/pack-1.mpack".into(),
            entries: vec![
                PackEntry {
                    oid: "a".into(),
                    offset: 0,
                    len: 4,
                },
                PackEntry {
                    oid: "b".into(),
                    offset: 4,
                    len: 5,
                },
                PackEntry {
                    oid: "extra".into(),
                    offset: 9,
                    len: 1,
                },
            ],
        };
        let blobs = vec![
            PackedBlobMetadata {
                oid: "a".into(),
                pack_offset: Some(0),
                pack_len: Some(4),
            },
            PackedBlobMetadata {
                oid: "b".into(),
                pack_offset: Some(4),
                pack_len: Some(4),
            },
            PackedBlobMetadata {
                oid: "missing".into(),
                pack_offset: Some(8),
                pack_len: Some(4),
            },
        ];

        assert_eq!(
            pack_index_issues(&pack, &index, &blobs),
            vec![
                PackIndexIssue::EntryOffsetMismatch { oid: "b".into() },
                PackIndexIssue::EntryWithoutBlobMetadata {
                    oid: "extra".into(),
                },
                PackIndexIssue::MissingBlobEntry {
                    oid: "missing".into(),
                },
            ]
        );
    }

    #[test]
    fn pack_index_validation_reports_pack_metadata_mismatch_first() {
        let pack = PackExport::new(
            "pack-1".into(),
            "objects/packs/pack-1.mpack".into(),
            "idx".into(),
            1,
            8,
        );
        let index = PackIndex {
            version: 1,
            pack_id: "other".into(),
            pack_key: "objects/packs/pack-1.mpack".into(),
            entries: vec![],
        };

        assert_eq!(
            pack_index_issues(&pack, &index, &[]),
            vec![PackIndexIssue::PackMetadataMismatch]
        );
    }

    #[test]
    fn pack_object_validation_reports_size_and_range_issues() {
        let pack = PackExport::new(
            "pack-1".into(),
            "objects/packs/pack-1.mpack".into(),
            "idx".into(),
            3,
            8,
        );
        let blobs = vec![
            PackedBlobMetadata {
                oid: "missing-offset".into(),
                pack_offset: None,
                pack_len: Some(1),
            },
            PackedBlobMetadata {
                oid: "missing-len".into(),
                pack_offset: Some(0),
                pack_len: None,
            },
            PackedBlobMetadata {
                oid: "out-of-bounds".into(),
                pack_offset: Some(7),
                pack_len: Some(2),
            },
        ];

        assert_eq!(
            pack_object_issues(&pack, 7, &blobs),
            vec![
                PackObjectIssue::SizeMismatch,
                PackObjectIssue::MissingBlobOffset {
                    oid: "missing-offset".into(),
                },
                PackObjectIssue::MissingBlobLength {
                    oid: "missing-len".into(),
                },
                PackObjectIssue::BlobRangeOutOfBounds {
                    oid: "out-of-bounds".into(),
                },
            ]
        );
    }

    #[test]
    fn missing_pack_metadata_ids_are_reported_once() {
        assert_eq!(
            missing_pack_metadata_ids(["pack-a", "pack-a", "pack-b"], ["pack-b"]),
            vec!["pack-a".to_string()]
        );
    }
}
