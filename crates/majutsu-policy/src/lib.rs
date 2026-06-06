#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringRule {
    pub name: String,
    pub prefix: String,
    pub after_days: Option<u32>,
    pub storage: StorageTier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageTier {
    Standard,
    Infrequent,
    Archive,
    DeepArchive,
}

pub fn is_hot_metadata_prefix(prefix: &str) -> bool {
    prefix.starts_with("hosts/")
        || prefix.starts_with("metadata/")
        || prefix.starts_with("trees/")
        || prefix.starts_with("large/manifests/")
        || prefix.starts_with("indexes/")
}
