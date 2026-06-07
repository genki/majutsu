use chrono::{DateTime, Datelike, Utc};
use majutsu_core::ObjectKey;
use serde::{Deserialize, Serialize};

pub const SMALL_BLOB_MAX_SIZE: u64 = 128 * 1024;

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
