use crate::majutsu_core::{
    FileRecord, LargeManifest, TreeManifest, TreeNodeManifest, payload_blob_ref, payload_large_ref,
};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use walkdir::WalkDir;

use crate::config::{MetadataExport, Paths};
use crate::majutsu_store::{canonical_remote_alias, canonical_remote_aliases};
use crate::util::path_to_slash;
use std::fs;

pub(crate) fn local_object_keys(paths: &Paths, export: &MetadataExport) -> Result<Vec<String>> {
    local_object_keys_inner(paths, export, None, None)
}

pub(crate) fn local_object_keys_with_progress(
    paths: &Paths,
    export: &MetadataExport,
    phase: &str,
) -> Result<Vec<String>> {
    local_object_keys_inner(paths, export, Some(phase), None)
}

pub(crate) fn local_object_keys_for_snapshot(
    paths: &Paths,
    export: &MetadataExport,
    snapshot_id: &str,
) -> Result<Vec<String>> {
    local_object_keys_inner(paths, export, None, Some(snapshot_id))
}

fn local_object_keys_inner(
    paths: &Paths,
    export: &MetadataExport,
    progress_phase: Option<&str>,
    only_snapshot_id: Option<&str>,
) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut referenced_blob_oids = BTreeSet::new();
    let snapshots = export
        .snapshots
        .iter()
        .filter(|snapshot| only_snapshot_id.map(|id| snapshot.id == id).unwrap_or(true))
        .collect::<Vec<_>>();
    let snapshot_total = snapshots.len();
    for (index, snapshot) in snapshots.iter().enumerate() {
        keys.push(snapshot.manifest_key.clone());
        if let Ok(manifest) = snapshot_manifest_for_object_keys(paths, snapshot) {
            for root_tree in manifest.root_trees.values() {
                keys.push(root_tree.tree_key.clone());
                if let Ok(tree) = tree_manifest_for_object_keys(paths, &root_tree.tree_key) {
                    if let Some(root_node) = &tree.root_node {
                        keys.push(root_node.node_key.clone());
                        push_child_tree_node_keys(paths, &mut keys, &root_node.node_key)?;
                    }
                    for node in tree.subtree_nodes.values() {
                        keys.push(node.node_key.clone());
                        push_child_tree_node_keys(paths, &mut keys, &node.node_key)?;
                    }
                    for record in tree_entries_for_object_keys(paths, &tree)?.values() {
                        if let Some((oid, _object_key)) = payload_blob_ref(&record.payload) {
                            referenced_blob_oids.insert(oid.to_string());
                        }
                        if let Some((_, manifest_key, _)) = payload_large_ref(&record.payload) {
                            keys.push(manifest_key.to_string());
                            if let Ok(large) = large_manifest_for_object_keys(paths, manifest_key) {
                                for chunk in large.chunks {
                                    keys.push(chunk.object_key);
                                }
                            }
                        }
                    }
                }
            }
        }
        let checked = index + 1;
        if let Some(phase) = progress_phase
            && (checked == snapshot_total || checked.is_multiple_of(100))
        {
            eprintln!(
                "gc progress phase={phase} checked={checked}/{snapshot_total} keys={}",
                keys.len()
            );
        }
    }
    for blob in export
        .blobs
        .iter()
        .filter(|blob| referenced_blob_oids.contains(blob.oid.as_str()))
    {
        if blob.pack_id.is_none() {
            keys.push(blob.object_key.clone());
        }
    }
    let live_pack_ids = export
        .blobs
        .iter()
        .filter(|blob| referenced_blob_oids.contains(blob.oid.as_str()))
        .filter_map(|blob| blob.pack_id.as_ref())
        .collect::<BTreeSet<_>>();
    for pack in export
        .packs
        .iter()
        .filter(|pack| live_pack_ids.contains(&pack.pack_id))
    {
        keys.push(pack.pack_key.clone());
        keys.push(pack.index_key.clone());
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn snapshot_manifest_for_object_keys(
    paths: &Paths,
    snapshot: &crate::majutsu_core::SnapshotExport,
) -> Result<crate::majutsu_core::SnapshotManifest> {
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

fn large_manifest_for_object_keys(paths: &Paths, manifest_key: &str) -> Result<LargeManifest> {
    let bytes = fs::read(paths.home.join(manifest_key))?;
    Ok(serde_json::from_slice(&crate::decode_object(
        paths, &bytes,
    )?)?)
}

fn tree_entries_for_object_keys(
    paths: &Paths,
    tree: &TreeManifest,
) -> Result<BTreeMap<String, FileRecord>> {
    if !tree.entries.is_empty() || tree.root_node.is_none() {
        return Ok(tree.entries.clone());
    }
    let root_node = tree.root_node.as_ref().expect("checked above");
    let bytes = fs::read(paths.home.join(&root_node.node_key))?;
    let node: TreeNodeManifest = serde_json::from_slice(&crate::decode_object(paths, &bytes)?)?;
    tree_entries_from_node_for_object_keys(paths, node)
}

fn tree_entries_from_node_for_object_keys(
    paths: &Paths,
    node: TreeNodeManifest,
) -> Result<BTreeMap<String, FileRecord>> {
    let mut entries = node.entries;
    for child in node.child_nodes.values() {
        let bytes = fs::read(paths.home.join(&child.node_key))?;
        let child_node: TreeNodeManifest =
            serde_json::from_slice(&crate::decode_object(paths, &bytes)?)?;
        entries.extend(tree_entries_from_node_for_object_keys(paths, child_node)?);
    }
    Ok(entries)
}

fn push_child_tree_node_keys(paths: &Paths, keys: &mut Vec<String>, node_key: &str) -> Result<()> {
    let bytes = fs::read(paths.home.join(node_key))?;
    let node: TreeNodeManifest = serde_json::from_slice(&crate::decode_object(paths, &bytes)?)?;
    for child in node.child_nodes.values() {
        keys.push(child.node_key.clone());
        push_child_tree_node_keys(paths, keys, &child.node_key)?;
    }
    Ok(())
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
            serde_json::from_str::<crate::majutsu_core::SnapshotManifest>(&snapshot.manifest_json)
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
    let live_pack_ids = export
        .blobs
        .iter()
        .filter_map(|blob| blob.pack_id.as_ref())
        .collect::<std::collections::BTreeSet<_>>();
    for pack in export
        .packs
        .iter()
        .filter(|pack| live_pack_ids.contains(&pack.pack_id))
    {
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
    use crate::majutsu_store::BlobExport;
    use chrono::Utc;
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
