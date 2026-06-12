use anyhow::Result;
use majutsu_core::{TreeManifest, payload_large_ref};
use std::path::PathBuf;
use walkdir::WalkDir;

use crate::config::{MetadataExport, Paths};
use crate::util::path_to_slash;
use majutsu_store::{canonical_remote_alias, canonical_remote_aliases};
use std::fs;

pub(crate) fn local_object_keys(paths: &Paths, export: &MetadataExport) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for snapshot in &export.snapshots {
        keys.push(snapshot.manifest_key.clone());
        if let Ok(manifest) = snapshot_manifest_for_object_keys(paths, snapshot) {
            for root_tree in manifest.root_trees.values() {
                keys.push(root_tree.tree_key.clone());
                if let Ok(tree) = tree_manifest_for_object_keys(paths, &root_tree.tree_key) {
                    for record in tree.entries.values() {
                        if let Some((_, manifest_key, _)) = payload_large_ref(&record.payload) {
                            keys.push(manifest_key.to_string());
                        }
                    }
                }
            }
        }
    }
    for blob in &export.blobs {
        if blob.pack_id.is_none() {
            keys.push(blob.object_key.clone());
        }
    }
    for pack in &export.packs {
        keys.push(pack.pack_key.clone());
        keys.push(pack.index_key.clone());
    }
    for large in &export.large_objects {
        keys.push(large.manifest_key.clone());
    }
    for chunk in &export.chunks {
        keys.push(chunk.object_key.clone());
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn snapshot_manifest_for_object_keys(
    paths: &Paths,
    snapshot: &majutsu_core::SnapshotExport,
) -> Result<majutsu_core::SnapshotManifest> {
    if !snapshot.manifest_json.trim().is_empty() {
        return Ok(serde_json::from_str(&snapshot.manifest_json)?);
    }
    let bytes = fs::read(paths.home.join(&snapshot.manifest_key))?;
    Ok(serde_json::from_slice(&crate::decode_object(
        paths, &bytes,
    )?)?)
}

fn tree_manifest_for_object_keys(paths: &Paths, tree_key: &str) -> Result<TreeManifest> {
    let bytes = fs::read(paths.home.join(tree_key))?;
    Ok(serde_json::from_slice(&crate::decode_object(
        paths, &bytes,
    )?)?)
}

pub(crate) fn prefer_canonical_remote_only(key: &str) -> bool {
    key.starts_with("objects/large/chunks/fixed/")
        || key.starts_with("objects/large/chunks/fastcdc/")
}

pub(crate) fn prefer_s3_canonical_remote_only(key: &str) -> bool {
    prefer_canonical_remote_only(key)
        || key.starts_with("objects/trees/")
        || key.starts_with("objects/blobs/")
        || key.starts_with("objects/packs/")
        || key.starts_with("objects/indexes/pack/")
        || key.starts_with("objects/large/manifests/")
}

#[cfg(test)]
pub(crate) fn remote_live_object_keys(export: &MetadataExport) -> Vec<String> {
    remote_live_object_keys_with_canonical_policy(export, prefer_canonical_remote_only)
}

#[cfg(test)]
pub(crate) fn s3_remote_live_object_keys(export: &MetadataExport) -> Vec<String> {
    remote_live_object_keys_with_canonical_policy(export, prefer_s3_canonical_remote_only)
}

pub(crate) fn remote_live_object_keys_for_local(
    paths: &Paths,
    export: &MetadataExport,
) -> Result<Vec<String>> {
    remote_live_object_keys_from_local_keys(
        local_object_keys(paths, export)?,
        prefer_canonical_remote_only,
    )
}

pub(crate) fn s3_remote_live_object_keys_for_local(
    paths: &Paths,
    export: &MetadataExport,
) -> Result<Vec<String>> {
    remote_live_object_keys_from_local_keys(
        local_object_keys(paths, export)?,
        prefer_s3_canonical_remote_only,
    )
}

#[cfg(test)]
fn remote_live_object_keys_with_canonical_policy(
    export: &MetadataExport,
    canonical_only: fn(&str) -> bool,
) -> Vec<String> {
    let local_keys = local_object_keys_from_metadata(export);
    remote_live_object_keys_from_local_keys(local_keys, canonical_only)
        .expect("metadata-only remote live object key calculation cannot fail")
}

fn remote_live_object_keys_from_local_keys(
    local_keys: Vec<String>,
    canonical_only: fn(&str) -> bool,
) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for key in &local_keys {
        if !canonical_only(key) {
            keys.push(key.clone());
        }
        keys.extend(canonical_remote_aliases(key));
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

#[cfg(test)]
pub(crate) fn local_object_keys_from_metadata(export: &MetadataExport) -> Vec<String> {
    let mut keys = Vec::new();
    for snapshot in &export.snapshots {
        keys.push(snapshot.manifest_key.clone());
        if let Ok(manifest) =
            serde_json::from_str::<majutsu_core::SnapshotManifest>(&snapshot.manifest_json)
        {
            for root_tree in manifest.root_trees.values() {
                keys.push(root_tree.tree_key.clone());
            }
        }
    }
    for blob in &export.blobs {
        if blob.pack_id.is_none() {
            keys.push(blob.object_key.clone());
        }
    }
    for pack in &export.packs {
        keys.push(pack.pack_key.clone());
        keys.push(pack.index_key.clone());
    }
    for large in &export.large_objects {
        keys.push(large.manifest_key.clone());
    }
    for chunk in &export.chunks {
        keys.push(chunk.object_key.clone());
    }
    keys.sort();
    keys.dedup();
    keys
}

pub(crate) fn canonical_alias_for_legacy_key(key: &str) -> Option<String> {
    canonical_remote_alias(key).filter(|alias| alias != key)
}

pub(crate) fn large_chunk_base(paths: &Paths, chunking: &str) -> PathBuf {
    match chunking {
        "fastcdc" => paths.home.join("objects/large/chunks/fastcdc"),
        _ => paths.large_chunks.clone(),
    }
}

pub(crate) fn large_chunk_base_for_key(paths: &Paths, key: &str) -> PathBuf {
    if key.starts_with("objects/large/chunks/fastcdc/") {
        large_chunk_base(paths, "fastcdc")
    } else {
        large_chunk_base(paths, "fixed")
    }
}

pub(crate) fn all_local_object_keys(paths: &Paths) -> Result<Vec<String>> {
    let root = paths.home.join("objects");
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut keys = Vec::new();
    for entry in WalkDir::new(&root).sort_by_file_name() {
        let entry = entry?;
        if entry.file_type().is_file() {
            keys.push(path_to_slash(entry.path().strip_prefix(&paths.home)?));
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        Config, HostConfig, LargeCompressionConfig, LargeConfig, MetadataExport, PackConfig,
        RestoreConfig, SecurityConfig, TieringConfig, WatchConfig, default_chunk_size,
        default_large_binary_min_size, default_large_chunked_chunk_size,
        default_large_chunked_min_size, default_large_chunking, default_large_max_parallel_uploads,
        default_large_min_size,
    };
    use chrono::Utc;
    use majutsu_store::BlobExport;
    use std::collections::BTreeMap;

    fn empty_export() -> MetadataExport {
        MetadataExport {
            version: 1,
            exported_at: Utc::now(),
            config: Config {
                host: HostConfig {
                    id: "host".into(),
                    name: "host".into(),
                },
                remote: None,
                roots: Vec::new(),
                large: LargeConfig {
                    enabled: true,
                    min_size: default_large_min_size(),
                    binary_min_size: default_large_binary_min_size(),
                    chunked_min_size: default_large_chunked_min_size(),
                    chunked_chunk_size: default_large_chunked_chunk_size(),
                    default_chunking: default_large_chunking(),
                    chunk_size: default_chunk_size(),
                    max_parallel_uploads: default_large_max_parallel_uploads(),
                    multipart: true,
                    always: Vec::new(),
                    never: Vec::new(),
                    compression: LargeCompressionConfig::default(),
                },
                pack: PackConfig::default(),
                watch: WatchConfig::default(),
                security: SecurityConfig::default(),
                tiering: TieringConfig::default(),
                restore: RestoreConfig::default(),
            },
            roots: Vec::new(),
            snapshots: Vec::new(),
            operations: Vec::new(),
            refs: BTreeMap::new(),
            blobs: Vec::new(),
            large_objects: Vec::new(),
            chunks: Vec::new(),
            packs: Vec::new(),
            large_pins: Vec::new(),
        }
    }

    #[test]
    fn s3_live_keys_omit_legacy_blob_when_canonical_alias_exists() {
        let mut export = empty_export();
        export.blobs.push(BlobExport::new(
            "abcdef".into(),
            3,
            "objects/blobs/ab/cdef".into(),
        ));

        let legacy = remote_live_object_keys(&export);
        assert!(legacy.contains(&"objects/blobs/ab/cdef".to_string()));
        assert!(legacy.contains(&"blobs/loose/ab/cdef.blob.enc".to_string()));

        let s3 = s3_remote_live_object_keys(&export);
        assert!(!s3.contains(&"objects/blobs/ab/cdef".to_string()));
        assert!(s3.contains(&"blobs/loose/ab/cdef.blob.enc".to_string()));
    }
}
