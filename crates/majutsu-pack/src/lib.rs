use chrono::{DateTime, Datelike, Utc};
use majutsu_core::ObjectKey;
use serde::{Deserialize, Serialize};

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
}
