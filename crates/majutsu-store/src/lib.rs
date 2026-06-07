use std::ops::Range;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
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
}
