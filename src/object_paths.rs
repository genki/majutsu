use anyhow::Result;
use majutsu_core::SnapshotManifest;
use std::path::PathBuf;
use walkdir::WalkDir;

use crate::config::{MetadataExport, Paths};
use crate::util::path_to_slash;
use majutsu_store::canonical_remote_aliases;

pub(crate) fn local_object_keys(export: &MetadataExport) -> Vec<String> {
    let mut keys = Vec::new();
    for snapshot in &export.snapshots {
        keys.push(snapshot.manifest_key.clone());
        if let Ok(manifest) = serde_json::from_str::<SnapshotManifest>(&snapshot.manifest_json) {
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

pub(crate) fn prefer_canonical_remote_only(key: &str) -> bool {
    key.starts_with("objects/large/chunks/fixed/")
        || key.starts_with("objects/large/chunks/fastcdc/")
}

pub(crate) fn remote_live_object_keys(export: &MetadataExport) -> Vec<String> {
    let local_keys = local_object_keys(export);
    let mut keys = Vec::new();
    for key in &local_keys {
        if !prefer_canonical_remote_only(key) {
            keys.push(key.clone());
        }
        keys.extend(canonical_remote_aliases(key));
    }
    keys.sort();
    keys.dedup();
    keys
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
