use std::ops::Range;

#[cfg(not(feature = "standalone-crate"))]
use crate::majutsu_core;
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
#[cfg(feature = "standalone-crate")]
#[allow(clippy::single_component_path_imports)]
use majutsu_core;
use majutsu_core::ObjectKey;
use serde::{Deserialize, Serialize};

pub trait ObjectStore {
    type Error;

    fn put(&self, key: &ObjectKey, body: &[u8]) -> Result<(), Self::Error>;
    fn get(&self, key: &ObjectKey) -> Result<Vec<u8>, Self::Error>;
    fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Vec<u8>, Self::Error>;
    fn exists(&self, key: &ObjectKey) -> Result<bool, Self::Error>;
    fn delete(&self, key: &ObjectKey) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteCapabilities {
    pub lifecycle_rules: bool,
    pub object_tags: bool,
    pub storage_class_on_put: bool,
    pub restore_archived_object: bool,
    pub multipart_upload: bool,
    pub range_get: bool,
    pub conditional_put: bool,
}

impl RemoteCapabilities {
    pub fn file() -> Self {
        Self {
            lifecycle_rules: false,
            object_tags: false,
            storage_class_on_put: false,
            restore_archived_object: true,
            multipart_upload: false,
            range_get: true,
            conditional_put: true,
        }
    }

    pub fn s3(signature_v2: bool, multipart_enabled: bool) -> Self {
        Self {
            lifecycle_rules: true,
            object_tags: !signature_v2,
            storage_class_on_put: !signature_v2,
            restore_archived_object: true,
            multipart_upload: multipart_enabled && !signature_v2,
            range_get: true,
            conditional_put: !signature_v2,
        }
    }
}

pub const REMOTE_HOST_INDEX_KEY: &str = "hosts/index.json";
pub const LEGACY_METADATA_EXPORT_KEY: &str = "metadata/export.json";
pub const REMOTE_CHUNK_INDEX_SHARD_KEY: &str = "indexes/chunk-index/shard-0000.cbor.zst.enc";
pub const DEFAULT_CHUNK_INDEX_SHARD: &str = "shard-0000";

pub fn remote_gc_mark_key(host_id: &str) -> String {
    format!("gc/marks/{host_id}.json")
}

pub fn remote_gc_tombstone_prefix(host_id: &str) -> String {
    format!("gc/tombstones/{host_id}/")
}

pub fn remote_gc_tombstone_key(host_id: &str, tombstone_id: &str) -> String {
    format!("{}{tombstone_id}.json", remote_gc_tombstone_prefix(host_id))
}

pub fn host_metadata_key(host_id: &str) -> String {
    format!("hosts/{host_id}/metadata/export.json")
}

pub fn host_legacy_current_key(host_id: &str) -> String {
    format!("hosts/{host_id}/current")
}

pub fn host_ref_key(host_id: &str, name: &str) -> String {
    format!("hosts/{host_id}/refs/{name}")
}

pub fn host_current_ref_key(host_id: &str) -> String {
    host_ref_key(host_id, "current")
}

pub fn host_last_synced_ref_key(host_id: &str) -> String {
    host_ref_key(host_id, "last-synced")
}

pub fn host_root_ack_ref_key(host_id: &str, root_id: &str) -> String {
    format!("hosts/{host_id}/roots/{root_id}/ack")
}

pub fn host_root_ack_ref_prefix(host_id: &str) -> String {
    format!("hosts/{host_id}/roots/")
}

pub fn host_snapshot_key(host_id: &str, snapshot_id: &str) -> String {
    format!("hosts/{host_id}/snapshots/{snapshot_id}.json")
}

pub fn host_snapshots_prefix(host_id: &str) -> String {
    format!("hosts/{host_id}/snapshots/")
}

pub fn host_snapshot_canonical_key(host_id: &str, snapshot_id: &str) -> String {
    format!("hosts/{host_id}/snapshots/{snapshot_id}.cbor.zst.enc")
}

pub fn host_operation_key(host_id: &str, op_id: &str) -> String {
    format!("hosts/{host_id}/ops/{op_id}.json")
}

pub fn host_ops_prefix(host_id: &str) -> String {
    format!("hosts/{host_id}/ops/")
}

pub fn host_operation_canonical_key(host_id: &str, op_id: &str) -> String {
    format!("hosts/{host_id}/ops/{op_id}.cbor.zst.enc")
}

pub fn host_oplog_key(host_id: &str) -> String {
    format!("hosts/{host_id}/ops/local-oplog.cborl")
}

pub fn host_oplog_canonical_key(host_id: &str) -> String {
    format!("hosts/{host_id}/ops/local-oplog.cborl.zst.enc")
}

pub fn archive_restore_status(key: &str, status: u16) -> Result<bool> {
    match status {
        200 | 202 | 204 | 409 => Ok(true),
        404 => Ok(false),
        _ => bail!("archive restore request failed for {key}: HTTP {status}"),
    }
}

pub fn s3_archive_restore_request_xml(days: u32, tier: &str) -> Result<String> {
    if days == 0 {
        bail!("archive restore days must be greater than zero");
    }
    let tier = tier.trim();
    if tier.is_empty() {
        bail!("archive restore tier must not be empty");
    }
    Ok(format!(
        "<RestoreRequest><Days>{days}</Days><GlacierJobParameters><Tier>{}</Tier></GlacierJobParameters></RestoreRequest>",
        xml_escape(tier)
    ))
}

fn xml_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub fn canonical_remote_alias(key: &str) -> Option<String> {
    if let Some(rest) = key.strip_prefix("objects/trees/") {
        Some(format!("trees/{rest}.cbor.zst.enc"))
    } else if let Some(rest) = key.strip_prefix("objects/blobs/") {
        Some(format!("blobs/loose/{rest}.blob.enc"))
    } else if let Some(rest) = key.strip_prefix("objects/packs/small/") {
        Some(format!("packs/small/{rest}"))
    } else if let Some(rest) = key.strip_prefix("objects/packs/normal/") {
        Some(format!("packs/normal/{rest}"))
    } else if let Some(rest) = key.strip_prefix("objects/indexes/pack/") {
        let rest = rest.strip_suffix(".json").unwrap_or(rest);
        Some(format!("indexes/pack-index/{rest}.cbor.zst.enc"))
    } else if let Some(rest) = key.strip_prefix("objects/large/manifests/") {
        Some(format!("large/manifests/{rest}.cbor.zst.enc"))
    } else if let Some(rest) = key.strip_prefix("objects/large/chunks/fixed/") {
        Some(format!("large/chunks/fixed-8m/{rest}.chunk.enc"))
    } else {
        key.strip_prefix("objects/large/chunks/fastcdc/")
            .map(|rest| format!("large/chunks/fastcdc/{rest}.chunk.enc"))
    }
}

pub fn canonical_remote_aliases(key: &str) -> Vec<String> {
    let Some(alias) = canonical_remote_alias(key) else {
        return Vec::new();
    };
    if alias == key {
        Vec::new()
    } else {
        vec![alias]
    }
}

pub fn is_content_addressed_remote_key(key: &str) -> bool {
    key.starts_with("objects/")
        || key.starts_with("trees/")
        || key.starts_with("blobs/loose/")
        || key.starts_with("packs/")
        || key.starts_with("indexes/pack-index/")
        || key.starts_with("large/")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteObjectAvailabilityIssue {
    MissingObjectOrAlias,
    MissingCanonicalAlias,
}

pub fn remote_object_availability_issues(
    legacy_exists: bool,
    aliases: &[String],
    alias_exists: bool,
) -> Vec<RemoteObjectAvailabilityIssue> {
    let mut issues = Vec::new();
    if !legacy_exists && !alias_exists {
        issues.push(RemoteObjectAvailabilityIssue::MissingObjectOrAlias);
    }
    if legacy_exists && !aliases.is_empty() && !alias_exists {
        issues.push(RemoteObjectAvailabilityIssue::MissingCanonicalAlias);
    }
    issues
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobExport {
    pub oid: String,
    pub size: u64,
    pub object_key: String,
    #[serde(default)]
    pub pack_id: Option<String>,
    #[serde(default)]
    pub pack_offset: Option<u64>,
    #[serde(default)]
    pub pack_len: Option<u64>,
}

impl BlobExport {
    pub fn new(oid: String, size: u64, object_key: String) -> Self {
        Self {
            oid,
            size,
            object_key,
            pack_id: None,
            pack_offset: None,
            pack_len: None,
        }
    }

    pub fn with_pack_location(mut self, pack_id: String, offset: u64, len: u64) -> Self {
        self.pack_id = Some(pack_id);
        self.pack_offset = Some(offset);
        self.pack_len = Some(len);
        self
    }

    pub fn is_packed(&self) -> bool {
        self.pack_id.is_some()
    }

    pub fn has_complete_pack_location(&self) -> bool {
        self.pack_id.is_some() && self.pack_offset.is_some() && self.pack_len.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteChunkIndexShard {
    pub version: u32,
    pub shard: String,
    pub updated_at: DateTime<Utc>,
    pub chunks: Vec<RemoteChunkIndexEntry>,
}

impl RemoteChunkIndexShard {
    pub fn new(updated_at: DateTime<Utc>, chunks: Vec<RemoteChunkIndexEntry>) -> Self {
        Self {
            version: 1,
            shard: DEFAULT_CHUNK_INDEX_SHARD.into(),
            updated_at,
            chunks,
        }
    }

    pub fn has_duplicate_oids(&self) -> bool {
        let unique = self
            .chunks
            .iter()
            .map(|entry| &entry.oid)
            .collect::<std::collections::BTreeSet<_>>();
        unique.len() != self.chunks.len()
    }

    pub fn validation_issues(
        &self,
        expected: &[RemoteChunkIndexEntry],
    ) -> Vec<RemoteChunkIndexIssue> {
        let mut issues = Vec::new();
        if self.version != 1
            || self.shard != DEFAULT_CHUNK_INDEX_SHARD
            || self.chunks.len() != expected.len()
        {
            issues.push(RemoteChunkIndexIssue::ShardMetadataMismatch);
            return issues;
        }
        if self.has_duplicate_oids() {
            issues.push(RemoteChunkIndexIssue::DuplicateShardOids);
        }
        let expected = expected
            .iter()
            .map(|entry| (entry.oid.as_str(), entry))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut seen = std::collections::BTreeSet::new();
        for entry in &self.chunks {
            let Some(expected_entry) = expected.get(entry.oid.as_str()) else {
                issues.push(RemoteChunkIndexIssue::UnexpectedEntry(entry.oid.clone()));
                continue;
            };
            if !seen.insert(entry.oid.as_str()) {
                issues.push(RemoteChunkIndexIssue::DuplicateEntry(entry.oid.clone()));
            }
            if entry != *expected_entry {
                issues.push(RemoteChunkIndexIssue::EntryMismatch(entry.oid.clone()));
            }
        }
        for oid in expected.keys() {
            if !seen.contains(oid) {
                issues.push(RemoteChunkIndexIssue::MissingEntry((*oid).to_string()));
            }
        }
        issues
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteChunkIndexEntry {
    pub oid: String,
    pub size: u64,
    pub object_key: String,
    pub canonical_key: String,
}

impl RemoteChunkIndexEntry {
    pub fn new(oid: String, size: u64, object_key: String, canonical_key: Option<String>) -> Self {
        let canonical_key = canonical_key.unwrap_or_else(|| object_key.clone());
        Self {
            oid,
            size,
            object_key,
            canonical_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteChunkIndexIssue {
    ShardMetadataMismatch,
    DuplicateShardOids,
    UnexpectedEntry(String),
    DuplicateEntry(String),
    EntryMismatch(String),
    MissingEntry(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteHostSummary {
    pub id: String,
    pub name: String,
    pub last_synced_at: DateTime<Utc>,
    pub current_snapshot: Option<String>,
    pub metadata_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteHostIndex {
    pub version: u32,
    pub updated_at: DateTime<Utc>,
    pub hosts: Vec<RemoteHostSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteHostIndexIssue {
    DuplicateHostId(String),
    DuplicateMetadataKey(String),
}

impl RemoteHostIndex {
    pub fn empty(updated_at: DateTime<Utc>) -> Self {
        Self {
            version: 1,
            updated_at,
            hosts: Vec::new(),
        }
    }

    pub fn sort_hosts(&mut self) {
        self.hosts.sort_by(|a, b| a.id.cmp(&b.id));
    }

    pub fn upsert_host(&mut self, summary: RemoteHostSummary, updated_at: DateTime<Utc>) {
        self.hosts.retain(|host| host.id != summary.id);
        self.hosts.push(summary);
        self.sort_hosts();
        self.updated_at = updated_at;
    }

    pub fn duplicate_issues(&self) -> Vec<RemoteHostIndexIssue> {
        let mut issues = Vec::new();
        let mut seen_host_ids = std::collections::BTreeSet::new();
        let mut seen_metadata_keys = std::collections::BTreeSet::new();
        for host in &self.hosts {
            if !seen_host_ids.insert(host.id.clone()) {
                issues.push(RemoteHostIndexIssue::DuplicateHostId(host.id.clone()));
            }
            if !seen_metadata_keys.insert(host.metadata_key.clone()) {
                issues.push(RemoteHostIndexIssue::DuplicateMetadataKey(
                    host.metadata_key.clone(),
                ));
            }
        }
        issues
    }
}

pub fn select_remote_host(
    hosts: Vec<RemoteHostSummary>,
    selector: &str,
) -> Result<RemoteHostSummary> {
    let mut by_id = hosts
        .iter()
        .filter(|host| host.id == selector)
        .cloned()
        .collect::<Vec<_>>();
    match by_id.len() {
        0 => {}
        1 => return Ok(by_id.remove(0)),
        _ => bail!("remote host id is duplicated in hosts/index.json: {selector}"),
    }
    let mut by_name = hosts
        .into_iter()
        .filter(|host| host.name == selector)
        .collect::<Vec<_>>();
    match by_name.len() {
        0 => bail!("remote host not found: {selector}"),
        1 => Ok(by_name.remove(0)),
        _ => bail!("remote host name is ambiguous: {selector}; use the host id"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteGcMark {
    pub version: u32,
    pub host_id: String,
    pub marked_at: DateTime<Utc>,
    pub current_snapshot: Option<String>,
    pub object_keys: Vec<String>,
}

impl RemoteGcMark {
    pub fn new(
        host_id: String,
        marked_at: DateTime<Utc>,
        current_snapshot: Option<String>,
        mut object_keys: Vec<String>,
    ) -> Self {
        object_keys.sort();
        object_keys.dedup();
        Self {
            version: 1,
            host_id,
            marked_at,
            current_snapshot,
            object_keys,
        }
    }

    pub fn has_duplicate_object_keys(&self) -> bool {
        let unique = self
            .object_keys
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        unique.len() != self.object_keys.len()
    }

    pub fn validation_issues(
        &self,
        host_id: &str,
        current_snapshot: Option<&String>,
        expected_object_keys: &std::collections::BTreeSet<String>,
    ) -> Vec<RemoteGcMarkIssue> {
        let mut issues = Vec::new();
        if self.version != 1 {
            issues.push(RemoteGcMarkIssue::UnsupportedVersion);
        }
        if self.host_id != host_id {
            issues.push(RemoteGcMarkIssue::HostMismatch(self.host_id.clone()));
        }
        if self.current_snapshot.as_ref() != current_snapshot {
            issues.push(RemoteGcMarkIssue::CurrentSnapshotMismatch);
        }
        if self.has_duplicate_object_keys() {
            issues.push(RemoteGcMarkIssue::DuplicateObjectKeys);
        }
        let actual = self
            .object_keys
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        for key in expected_object_keys {
            if !actual.contains(key) {
                issues.push(RemoteGcMarkIssue::MissingLiveObject(key.clone()));
            }
        }
        for key in actual {
            if !expected_object_keys.contains(key) {
                issues.push(RemoteGcMarkIssue::UnexpectedObjectKey(key.clone()));
            }
        }
        issues
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteGcMarkIssue {
    UnsupportedVersion,
    HostMismatch(String),
    CurrentSnapshotMismatch,
    DuplicateObjectKeys,
    MissingLiveObject(String),
    UnexpectedObjectKey(String),
}

impl RemoteGcMarkIssue {
    pub fn message(&self, mark_key: &str, expected_host_id: &str) -> String {
        match self {
            Self::UnsupportedVersion => {
                format!("unsupported remote gc mark version {mark_key}")
            }
            Self::HostMismatch(actual) => {
                format!("remote gc mark host id {actual} does not match {expected_host_id}")
            }
            Self::CurrentSnapshotMismatch => {
                format!("remote gc mark current snapshot does not match metadata {mark_key}")
            }
            Self::DuplicateObjectKeys => {
                format!("remote gc mark contains duplicate object keys {mark_key}")
            }
            Self::MissingLiveObject(key) => {
                format!("remote gc mark is missing live object {mark_key} {key}")
            }
            Self::UnexpectedObjectKey(key) => {
                format!("remote gc mark contains unexpected object {mark_key} {key}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteGcTombstone {
    pub version: u32,
    pub host_id: String,
    pub deleted_at: DateTime<Utc>,
    pub key: String,
}

impl RemoteGcTombstone {
    pub fn new(host_id: String, deleted_at: DateTime<Utc>, key: String) -> Self {
        Self {
            version: 1,
            host_id,
            deleted_at,
            key,
        }
    }

    pub fn has_valid_deleted_key(&self) -> bool {
        !self.key.is_empty() && !self.key.starts_with('/') && !self.key.contains("..")
    }

    pub fn validation_issues(&self, host_id: &str) -> Vec<RemoteGcTombstoneIssue> {
        let mut issues = Vec::new();
        if self.version != 1 {
            issues.push(RemoteGcTombstoneIssue::UnsupportedVersion);
        }
        if self.host_id != host_id {
            issues.push(RemoteGcTombstoneIssue::HostMismatch(self.host_id.clone()));
        }
        if !self.has_valid_deleted_key() {
            issues.push(RemoteGcTombstoneIssue::InvalidDeletedKey);
        }
        issues
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteGcTombstoneIssue {
    UnsupportedVersion,
    HostMismatch(String),
    InvalidDeletedKey,
}

impl RemoteGcTombstoneIssue {
    pub fn message(&self, tombstone_key: &str, expected_host_id: &str) -> String {
        match self {
            Self::UnsupportedVersion => {
                format!("unsupported remote gc tombstone version {tombstone_key}")
            }
            Self::HostMismatch(actual) => {
                format!("remote gc tombstone host id {actual} does not match {expected_host_id}")
            }
            Self::InvalidDeletedKey => {
                format!("remote gc tombstone has invalid deleted key {tombstone_key}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_remote_supports_local_recovery_capabilities() {
        let capabilities = RemoteCapabilities::file();

        assert!(!capabilities.lifecycle_rules);
        assert!(!capabilities.object_tags);
        assert!(!capabilities.storage_class_on_put);
        assert!(capabilities.restore_archived_object);
        assert!(!capabilities.multipart_upload);
        assert!(capabilities.range_get);
        assert!(capabilities.conditional_put);
    }

    #[test]
    fn canonical_remote_aliases_cover_local_object_layouts() {
        assert_eq!(
            canonical_remote_alias("objects/trees/tree-1").as_deref(),
            Some("trees/tree-1.cbor.zst.enc")
        );
        assert_eq!(
            canonical_remote_alias("objects/blobs/blob-1").as_deref(),
            Some("blobs/loose/blob-1.blob.enc")
        );
        assert_eq!(
            canonical_remote_alias("objects/packs/small/pack-1.mpack").as_deref(),
            Some("packs/small/pack-1.mpack")
        );
        assert_eq!(
            canonical_remote_alias("objects/indexes/pack/2026/06/pack-1.json").as_deref(),
            Some("indexes/pack-index/2026/06/pack-1.cbor.zst.enc")
        );
        assert_eq!(
            canonical_remote_alias("objects/large/manifests/large-1").as_deref(),
            Some("large/manifests/large-1.cbor.zst.enc")
        );
        assert_eq!(
            canonical_remote_alias("objects/large/chunks/fixed/chunk-1").as_deref(),
            Some("large/chunks/fixed-8m/chunk-1.chunk.enc")
        );
        assert_eq!(
            canonical_remote_alias("objects/large/chunks/fastcdc/chunk-1").as_deref(),
            Some("large/chunks/fastcdc/chunk-1.chunk.enc")
        );
        assert!(canonical_remote_alias("logs/local").is_none());
        assert!(is_content_addressed_remote_key(
            "large/chunks/fixed-8m/chunk-1.chunk.enc"
        ));
        assert!(is_content_addressed_remote_key(
            "indexes/pack-index/2026/06/pack-1.cbor.zst.enc"
        ));
        assert!(!is_content_addressed_remote_key(
            "indexes/chunk-index/shard-0000.cbor.zst.enc"
        ));
        assert!(!is_content_addressed_remote_key("queue/uploads/item.json"));
    }

    #[test]
    fn host_scoped_remote_keys_are_stable() {
        assert_eq!(
            host_metadata_key("host-a"),
            "hosts/host-a/metadata/export.json"
        );
        assert_eq!(host_legacy_current_key("host-a"), "hosts/host-a/current");
        assert_eq!(host_current_ref_key("host-a"), "hosts/host-a/refs/current");
        assert_eq!(
            host_last_synced_ref_key("host-a"),
            "hosts/host-a/refs/last-synced"
        );
        assert_eq!(
            host_snapshot_key("host-a", "snap-1"),
            "hosts/host-a/snapshots/snap-1.json"
        );
        assert_eq!(host_snapshots_prefix("host-a"), "hosts/host-a/snapshots/");
        assert_eq!(
            host_snapshot_canonical_key("host-a", "snap-1"),
            "hosts/host-a/snapshots/snap-1.cbor.zst.enc"
        );
        assert_eq!(
            host_operation_key("host-a", "op-1"),
            "hosts/host-a/ops/op-1.json"
        );
        assert_eq!(host_ops_prefix("host-a"), "hosts/host-a/ops/");
        assert_eq!(
            host_operation_canonical_key("host-a", "op-1"),
            "hosts/host-a/ops/op-1.cbor.zst.enc"
        );
        assert_eq!(
            host_oplog_key("host-a"),
            "hosts/host-a/ops/local-oplog.cborl"
        );
        assert_eq!(
            host_oplog_canonical_key("host-a"),
            "hosts/host-a/ops/local-oplog.cborl.zst.enc"
        );
    }

    #[test]
    fn remote_object_availability_reports_missing_alias_states() {
        assert_eq!(
            remote_object_availability_issues(false, &["alias".into()], false),
            vec![RemoteObjectAvailabilityIssue::MissingObjectOrAlias]
        );
        assert_eq!(
            remote_object_availability_issues(true, &["alias".into()], false),
            vec![RemoteObjectAvailabilityIssue::MissingCanonicalAlias]
        );
        assert!(remote_object_availability_issues(false, &["alias".into()], true).is_empty());
        assert!(remote_object_availability_issues(true, &[], false).is_empty());
    }

    #[test]
    fn s3_v4_supports_policy_tags_multipart_and_conditional_put() {
        let capabilities = RemoteCapabilities::s3(false, true);

        assert!(capabilities.lifecycle_rules);
        assert!(capabilities.object_tags);
        assert!(capabilities.storage_class_on_put);
        assert!(capabilities.restore_archived_object);
        assert!(capabilities.multipart_upload);
        assert!(capabilities.range_get);
        assert!(capabilities.conditional_put);
    }

    #[test]
    fn s3_v2_disables_unsigned_capabilities() {
        let capabilities = RemoteCapabilities::s3(true, true);

        assert!(capabilities.lifecycle_rules);
        assert!(!capabilities.object_tags);
        assert!(!capabilities.storage_class_on_put);
        assert!(capabilities.restore_archived_object);
        assert!(!capabilities.multipart_upload);
        assert!(capabilities.range_get);
        assert!(!capabilities.conditional_put);
    }

    #[test]
    fn s3_multipart_follows_large_object_policy() {
        assert!(!RemoteCapabilities::s3(false, false).multipart_upload);
    }

    #[test]
    fn archive_restore_status_maps_s3_responses() {
        for status in [200, 202, 204, 409] {
            assert!(archive_restore_status("objects/large/chunk", status).unwrap());
        }
        assert!(!archive_restore_status("objects/large/chunk", 404).unwrap());
        let err = archive_restore_status("objects/large/chunk", 500)
            .unwrap_err()
            .to_string();
        assert!(err.contains("archive restore request failed for objects/large/chunk: HTTP 500"));
    }

    #[test]
    fn s3_archive_restore_request_xml_validates_and_escapes_inputs() {
        assert_eq!(
            s3_archive_restore_request_xml(7, "Standard").unwrap(),
            "<RestoreRequest><Days>7</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>"
        );
        assert_eq!(
            s3_archive_restore_request_xml(3, " Bulk & <slow> ").unwrap(),
            "<RestoreRequest><Days>3</Days><GlacierJobParameters><Tier>Bulk &amp; &lt;slow&gt;</Tier></GlacierJobParameters></RestoreRequest>"
        );
        assert!(
            s3_archive_restore_request_xml(0, "Standard")
                .unwrap_err()
                .to_string()
                .contains("archive restore days must be greater than zero")
        );
        assert!(
            s3_archive_restore_request_xml(1, " ")
                .unwrap_err()
                .to_string()
                .contains("archive restore tier must not be empty")
        );
    }

    fn host(id: &str, name: &str, metadata_key: &str) -> RemoteHostSummary {
        RemoteHostSummary {
            id: id.into(),
            name: name.into(),
            last_synced_at: DateTime::<Utc>::UNIX_EPOCH,
            current_snapshot: Some(format!("snap-{id}")),
            metadata_key: metadata_key.into(),
        }
    }

    #[test]
    fn remote_host_index_upserts_and_sorts_hosts() {
        let mut index = RemoteHostIndex::empty(DateTime::<Utc>::UNIX_EPOCH);

        index.upsert_host(
            host("b", "Beta", "hosts/b/metadata/export.json"),
            Utc::now(),
        );
        index.upsert_host(
            host("a", "Alpha", "hosts/a/metadata/export.json"),
            Utc::now(),
        );
        index.upsert_host(
            host("b", "Beta 2", "hosts/b/metadata/export.json"),
            Utc::now(),
        );

        assert_eq!(index.hosts.len(), 2);
        assert_eq!(index.hosts[0].id, "a");
        assert_eq!(index.hosts[1].name, "Beta 2");
    }

    #[test]
    fn remote_host_index_detects_duplicate_ids_and_metadata_keys() {
        let mut index = RemoteHostIndex::empty(DateTime::<Utc>::UNIX_EPOCH);
        index.hosts = vec![
            host("a", "Alpha", "hosts/a/metadata/export.json"),
            host("a", "Alpha copy", "hosts/a-copy/metadata/export.json"),
            host("b", "Beta", "hosts/a/metadata/export.json"),
        ];

        assert_eq!(
            index.duplicate_issues(),
            vec![
                RemoteHostIndexIssue::DuplicateHostId("a".into()),
                RemoteHostIndexIssue::DuplicateMetadataKey("hosts/a/metadata/export.json".into()),
            ]
        );
    }

    #[test]
    fn remote_host_selection_prefers_unique_id_then_unique_name() {
        let hosts = vec![
            host("host-a", "shared", "hosts/a/metadata/export.json"),
            host("host-b", "shared", "hosts/b/metadata/export.json"),
            host("host-c", "single", "hosts/c/metadata/export.json"),
        ];

        assert_eq!(
            select_remote_host(hosts.clone(), "host-b")
                .unwrap()
                .metadata_key,
            "hosts/b/metadata/export.json"
        );
        assert_eq!(
            select_remote_host(hosts.clone(), "single").unwrap().id,
            "host-c"
        );
        assert!(select_remote_host(hosts, "shared").is_err());
    }

    #[test]
    fn remote_host_index_json_shape_is_stable() {
        let index = RemoteHostIndex {
            version: 1,
            updated_at: DateTime::<Utc>::UNIX_EPOCH,
            hosts: vec![host("host-a", "Alpha", "hosts/a/metadata/export.json")],
        };

        let value = serde_json::to_value(index).unwrap();

        assert_eq!(value["version"], 1);
        assert_eq!(value["hosts"][0]["id"], "host-a");
        assert_eq!(
            value["hosts"][0]["metadata_key"],
            "hosts/a/metadata/export.json"
        );
    }

    #[test]
    fn remote_gc_mark_keys_are_sorted_and_deduplicated() {
        let mark = RemoteGcMark::new(
            "host-a".into(),
            DateTime::<Utc>::UNIX_EPOCH,
            Some("snap-1".into()),
            vec!["objects/b".into(), "objects/a".into(), "objects/a".into()],
        );

        assert_eq!(remote_gc_mark_key("host-a"), "gc/marks/host-a.json");
        assert_eq!(mark.version, 1);
        assert_eq!(mark.object_keys, vec!["objects/a", "objects/b"]);
        assert!(!mark.has_duplicate_object_keys());
    }

    #[test]
    fn remote_gc_mark_detects_wire_duplicates() {
        let mark = RemoteGcMark {
            version: 1,
            host_id: "host-a".into(),
            marked_at: DateTime::<Utc>::UNIX_EPOCH,
            current_snapshot: None,
            object_keys: vec!["objects/a".into(), "objects/a".into()],
        };

        assert!(mark.has_duplicate_object_keys());
    }

    #[test]
    fn remote_gc_mark_validation_reports_metadata_issues() {
        let mark = RemoteGcMark {
            version: 2,
            host_id: "wrong-host".into(),
            marked_at: DateTime::<Utc>::UNIX_EPOCH,
            current_snapshot: Some("old-snap".into()),
            object_keys: vec![
                "objects/a".into(),
                "objects/a".into(),
                "objects/extra".into(),
            ],
        };
        let expected = ["objects/a".to_string(), "objects/b".to_string()]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            mark.validation_issues("host-a", Some(&"snap-1".to_string()), &expected),
            vec![
                RemoteGcMarkIssue::UnsupportedVersion,
                RemoteGcMarkIssue::HostMismatch("wrong-host".into()),
                RemoteGcMarkIssue::CurrentSnapshotMismatch,
                RemoteGcMarkIssue::DuplicateObjectKeys,
                RemoteGcMarkIssue::MissingLiveObject("objects/b".into()),
                RemoteGcMarkIssue::UnexpectedObjectKey("objects/extra".into()),
            ]
        );
    }

    #[test]
    fn remote_gc_mark_issue_messages_are_stable() {
        assert_eq!(
            RemoteGcMarkIssue::UnsupportedVersion.message("gc/marks/host-a.json", "host-a"),
            "unsupported remote gc mark version gc/marks/host-a.json"
        );
        assert_eq!(
            RemoteGcMarkIssue::HostMismatch("wrong-host".into())
                .message("gc/marks/host-a.json", "host-a"),
            "remote gc mark host id wrong-host does not match host-a"
        );
        assert_eq!(
            RemoteGcMarkIssue::CurrentSnapshotMismatch.message("gc/marks/host-a.json", "host-a"),
            "remote gc mark current snapshot does not match metadata gc/marks/host-a.json"
        );
        assert_eq!(
            RemoteGcMarkIssue::DuplicateObjectKeys.message("gc/marks/host-a.json", "host-a"),
            "remote gc mark contains duplicate object keys gc/marks/host-a.json"
        );
        assert_eq!(
            RemoteGcMarkIssue::MissingLiveObject("objects/a".into())
                .message("gc/marks/host-a.json", "host-a"),
            "remote gc mark is missing live object gc/marks/host-a.json objects/a"
        );
        assert_eq!(
            RemoteGcMarkIssue::UnexpectedObjectKey("objects/extra".into())
                .message("gc/marks/host-a.json", "host-a"),
            "remote gc mark contains unexpected object gc/marks/host-a.json objects/extra"
        );
    }

    #[test]
    fn remote_gc_mark_validation_accepts_matching_mark() {
        let expected = ["objects/a".to_string(), "objects/b".to_string()]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let mark = RemoteGcMark::new(
            "host-a".into(),
            DateTime::<Utc>::UNIX_EPOCH,
            Some("snap-1".into()),
            expected.iter().cloned().collect(),
        );

        assert!(
            mark.validation_issues("host-a", Some(&"snap-1".to_string()), &expected)
                .is_empty()
        );
    }

    #[test]
    fn remote_gc_tombstone_keys_and_deleted_key_validation_are_stable() {
        let tombstone = RemoteGcTombstone::new(
            "host-a".into(),
            DateTime::<Utc>::UNIX_EPOCH,
            "hosts/host-a/ops/op-1.json".into(),
        );

        assert_eq!(
            remote_gc_tombstone_prefix("host-a"),
            "gc/tombstones/host-a/"
        );
        assert_eq!(
            remote_gc_tombstone_key("host-a", "tombstone-1"),
            "gc/tombstones/host-a/tombstone-1.json"
        );
        assert_eq!(tombstone.version, 1);
        assert!(tombstone.has_valid_deleted_key());
        assert!(
            !RemoteGcTombstone::new(
                "host-a".into(),
                DateTime::<Utc>::UNIX_EPOCH,
                "../bad".into()
            )
            .has_valid_deleted_key()
        );
    }

    #[test]
    fn remote_gc_tombstone_validation_reports_metadata_issues() {
        let tombstone = RemoteGcTombstone {
            version: 2,
            host_id: "wrong-host".into(),
            deleted_at: DateTime::<Utc>::UNIX_EPOCH,
            key: "../bad".into(),
        };

        assert_eq!(
            tombstone.validation_issues("host-a"),
            vec![
                RemoteGcTombstoneIssue::UnsupportedVersion,
                RemoteGcTombstoneIssue::HostMismatch("wrong-host".into()),
                RemoteGcTombstoneIssue::InvalidDeletedKey,
            ]
        );
    }

    #[test]
    fn remote_gc_tombstone_issue_messages_are_stable() {
        assert_eq!(
            RemoteGcTombstoneIssue::UnsupportedVersion
                .message("gc/tombstones/host-a/tombstone-1.json", "host-a"),
            "unsupported remote gc tombstone version gc/tombstones/host-a/tombstone-1.json"
        );
        assert_eq!(
            RemoteGcTombstoneIssue::HostMismatch("wrong-host".into())
                .message("gc/tombstones/host-a/tombstone-1.json", "host-a"),
            "remote gc tombstone host id wrong-host does not match host-a"
        );
        assert_eq!(
            RemoteGcTombstoneIssue::InvalidDeletedKey
                .message("gc/tombstones/host-a/tombstone-1.json", "host-a"),
            "remote gc tombstone has invalid deleted key gc/tombstones/host-a/tombstone-1.json"
        );
    }

    #[test]
    fn remote_chunk_index_shard_shape_and_defaults_are_stable() {
        let shard = RemoteChunkIndexShard::new(
            DateTime::<Utc>::UNIX_EPOCH,
            vec![RemoteChunkIndexEntry::new(
                "oid-1".into(),
                42,
                "objects/chunks/oid-1".into(),
                Some("objects/by-hash/chunks/oid-1.cbor.zst.enc".into()),
            )],
        );

        assert_eq!(
            REMOTE_CHUNK_INDEX_SHARD_KEY,
            "indexes/chunk-index/shard-0000.cbor.zst.enc"
        );
        assert_eq!(shard.version, 1);
        assert_eq!(shard.shard, DEFAULT_CHUNK_INDEX_SHARD);
        assert_eq!(
            shard.chunks[0].canonical_key,
            "objects/by-hash/chunks/oid-1.cbor.zst.enc"
        );
        assert!(!shard.has_duplicate_oids());
    }

    #[test]
    fn remote_chunk_index_entry_defaults_canonical_key_to_object_key() {
        let entry =
            RemoteChunkIndexEntry::new("oid-1".into(), 42, "objects/chunks/oid-1".into(), None);

        assert_eq!(entry.canonical_key, "objects/chunks/oid-1");
    }

    #[test]
    fn remote_chunk_index_shard_detects_duplicate_oids() {
        let shard = RemoteChunkIndexShard::new(
            DateTime::<Utc>::UNIX_EPOCH,
            vec![
                RemoteChunkIndexEntry::new("oid-1".into(), 1, "a".into(), None),
                RemoteChunkIndexEntry::new("oid-1".into(), 1, "b".into(), None),
            ],
        );

        assert!(shard.has_duplicate_oids());
    }

    #[test]
    fn remote_chunk_index_validation_accepts_matching_entries() {
        let expected = vec![RemoteChunkIndexEntry::new(
            "oid-1".into(),
            42,
            "objects/chunks/oid-1".into(),
            Some("objects/by-hash/chunks/oid-1".into()),
        )];
        let shard = RemoteChunkIndexShard::new(DateTime::<Utc>::UNIX_EPOCH, expected.clone());

        assert!(shard.validation_issues(&expected).is_empty());
    }

    #[test]
    fn remote_chunk_index_validation_reports_shape_mismatch() {
        let expected = vec![RemoteChunkIndexEntry::new(
            "oid-1".into(),
            42,
            "objects/chunks/oid-1".into(),
            None,
        )];
        let mut shard = RemoteChunkIndexShard::new(DateTime::<Utc>::UNIX_EPOCH, expected.clone());
        shard.shard = "other".into();

        assert_eq!(
            shard.validation_issues(&expected),
            vec![RemoteChunkIndexIssue::ShardMetadataMismatch]
        );
    }

    #[test]
    fn remote_chunk_index_validation_reports_entry_issues() {
        let expected = vec![
            RemoteChunkIndexEntry::new("oid-1".into(), 42, "objects/chunks/oid-1".into(), None),
            RemoteChunkIndexEntry::new("oid-2".into(), 43, "objects/chunks/oid-2".into(), None),
            RemoteChunkIndexEntry::new("oid-3".into(), 44, "objects/chunks/oid-3".into(), None),
        ];
        let shard = RemoteChunkIndexShard::new(
            DateTime::<Utc>::UNIX_EPOCH,
            vec![
                RemoteChunkIndexEntry::new("oid-1".into(), 99, "objects/chunks/oid-1".into(), None),
                RemoteChunkIndexEntry::new("oid-2".into(), 43, "objects/chunks/oid-2".into(), None),
                RemoteChunkIndexEntry::new("oid-2".into(), 43, "objects/chunks/oid-2".into(), None),
            ],
        );

        assert_eq!(
            shard.validation_issues(&expected),
            vec![
                RemoteChunkIndexIssue::DuplicateShardOids,
                RemoteChunkIndexIssue::EntryMismatch("oid-1".into()),
                RemoteChunkIndexIssue::DuplicateEntry("oid-2".into()),
                RemoteChunkIndexIssue::MissingEntry("oid-3".into()),
            ]
        );
    }

    #[test]
    fn blob_export_json_shape_and_pack_location_are_stable() {
        let loose = BlobExport::new("blob-1".into(), 42, "objects/blobs/blob-1".into());
        let packed = loose.clone().with_pack_location("pack-1".into(), 128, 42);

        let loose_value = serde_json::to_value(&loose).unwrap();
        let packed_value = serde_json::to_value(&packed).unwrap();

        assert_eq!(loose_value["oid"], "blob-1");
        assert_eq!(loose_value["pack_id"], serde_json::Value::Null);
        assert!(!loose.is_packed());
        assert!(!loose.has_complete_pack_location());
        assert!(packed.is_packed());
        assert!(packed.has_complete_pack_location());
        assert_eq!(packed_value["pack_id"], "pack-1");
        assert_eq!(packed_value["pack_offset"], 128);
        assert_eq!(packed_value["pack_len"], 42);
    }
}
