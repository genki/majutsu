use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use crate::atomic_io::write_atomic;
use crate::config::{Config, MetadataExport, Paths};
use crate::majutsu_core::{
    FileRecord, LargeManifest, Payload, SnapshotExport, SnapshotManifest, TreeManifest,
    TreeNodeManifest, payload_blob_ref,
};
use crate::util::{REMOTE_METADATA_DECODE_LIMIT, zstd_decode_all_limited};

const ROOT_SIZE_SUMMARY_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RootSizeSummary {
    pub(crate) version: u32,
    pub(crate) host_id: String,
    pub(crate) snapshot_id: String,
    pub(crate) generated_at: String,
    pub(crate) roots: Vec<RootSizeSummaryRow>,
    pub(crate) totals: RootSizeSummaryTotals,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RootSizeSummaryRow {
    pub(crate) root: String,
    pub(crate) files: usize,
    pub(crate) dirs: usize,
    pub(crate) client_bytes: u64,
    pub(crate) used_bytes: u64,
    pub(crate) backend_bytes: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) backend_objects: usize,
    pub(crate) missing_objects: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RootSizeSummaryTotals {
    #[serde(default)]
    pub(crate) billed_bytes: u64,
    #[serde(default)]
    pub(crate) billed_objects: usize,
    #[serde(default)]
    pub(crate) row_used_bytes: u64,
    #[serde(default)]
    pub(crate) unique_used_bytes: u64,
    pub(crate) current_backend_bytes: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) objects: usize,
    pub(crate) backend_prefix_bytes: u64,
    pub(crate) backend_prefix_objects: usize,
    #[serde(default)]
    pub(crate) backend_prefix_exact: bool,
    #[serde(default)]
    pub(crate) backend_prefix_scope: String,
}

#[derive(Default)]
struct BuilderStat {
    files: usize,
    dirs: usize,
    client_bytes: u64,
    payload_keys: BTreeSet<String>,
    metadata_keys: BTreeSet<String>,
    packed_payload_keys: BTreeSet<String>,
    packed_slice_bytes: u64,
}

#[derive(Clone)]
struct PackedBlobSizeRef {
    pack_key: String,
    index_key: String,
    pack_len: u64,
}

pub(crate) fn root_size_summary_key(host_id: &str) -> String {
    format!("hosts/{host_id}/root-size-summary.cbor.zst.enc")
}

pub(crate) fn build_root_size_summary(
    paths: &Paths,
    config: &Config,
    export: &MetadataExport,
) -> Result<Option<RootSizeSummary>> {
    let Some(current) = export.refs.get("current") else {
        return Ok(None);
    };
    let Some(snapshot) = export
        .snapshots
        .iter()
        .find(|snapshot| snapshot.id == *current)
    else {
        return Ok(None);
    };
    let manifest = read_snapshot_manifest(paths, snapshot)?;
    let packed_blobs = packed_blob_size_refs(paths)?;
    let mut stats = BTreeMap::<String, BuilderStat>::new();

    for (root_id, root_snapshot) in &manifest.root_trees {
        let tree: TreeManifest = read_local_metadata_object(paths, &root_snapshot.tree_key)
            .with_context(|| format!("read root tree {}", root_snapshot.tree_key))?;
        let stat = stats.entry(root_id.clone()).or_default();
        stat.metadata_keys.insert(root_snapshot.tree_key.clone());
        if let Some(root_node) = &tree.root_node {
            stat.metadata_keys.insert(root_node.node_key.clone());
        }
        for node in tree.subtree_nodes.values() {
            stat.metadata_keys.insert(node.node_key.clone());
        }
        let entries = local_tree_entries(paths, tree)?;
        for record in entries.values() {
            match record.kind.as_str() {
                "directory" => stat.dirs += 1,
                _ => {
                    stat.files += 1;
                    stat.client_bytes = stat.client_bytes.saturating_add(record.size);
                }
            }
            add_payload_keys(paths, &packed_blobs, &record.payload, stat)?;
        }
    }

    for (root_id, records) in &manifest.roots {
        let stat = stats.entry(root_id.clone()).or_default();
        for record in records {
            match record.kind.as_str() {
                "directory" => stat.dirs += 1,
                _ => {
                    stat.files += 1;
                    stat.client_bytes = stat.client_bytes.saturating_add(record.size);
                }
            }
            add_payload_keys(paths, &packed_blobs, &record.payload, stat)?;
        }
    }

    let mut rows = Vec::new();
    let mut unique_keys = BTreeSet::new();
    let mut unique_payload_keys = BTreeSet::new();
    let mut unique_metadata_keys = BTreeSet::new();
    for (root, stat) in stats {
        let payload_bytes = sum_local_sizes(paths, &stat.payload_keys);
        let metadata_bytes = sum_local_sizes(paths, &stat.metadata_keys);
        let packed_payload_bytes = sum_local_sizes(paths, &stat.packed_payload_keys);
        let all_keys = stat
            .payload_keys
            .union(&stat.metadata_keys)
            .cloned()
            .collect::<BTreeSet<_>>();
        unique_keys.extend(all_keys.iter().cloned());
        unique_payload_keys.extend(stat.payload_keys.iter().cloned());
        unique_metadata_keys.extend(stat.metadata_keys.iter().cloned());
        let backend_bytes = sum_local_sizes(paths, &all_keys);
        let used_bytes = backend_bytes
            .saturating_sub(packed_payload_bytes)
            .saturating_add(stat.packed_slice_bytes);
        rows.push(RootSizeSummaryRow {
            root,
            files: stat.files,
            dirs: stat.dirs,
            client_bytes: stat.client_bytes,
            used_bytes,
            backend_bytes,
            payload_bytes,
            metadata_bytes,
            backend_objects: all_keys.len(),
            missing_objects: missing_local_objects(paths, &all_keys),
        });
    }
    rows.sort_by(|left, right| left.root.cmp(&right.root));

    let current_backend_bytes = sum_local_sizes(paths, &unique_keys);
    let payload_bytes = sum_local_sizes(paths, &unique_payload_keys);
    let metadata_bytes = sum_local_sizes(paths, &unique_metadata_keys);
    let row_used_bytes = rows.iter().map(|row| row.used_bytes).sum();
    let totals = RootSizeSummaryTotals {
        billed_bytes: 0,
        billed_objects: 0,
        row_used_bytes,
        unique_used_bytes: row_used_bytes,
        current_backend_bytes,
        payload_bytes,
        metadata_bytes,
        objects: unique_keys.len(),
        backend_prefix_bytes: 0,
        backend_prefix_objects: 0,
        backend_prefix_exact: false,
        backend_prefix_scope: "not-scanned".into(),
    };

    Ok(Some(RootSizeSummary {
        version: ROOT_SIZE_SUMMARY_VERSION,
        host_id: config.host.id.clone(),
        snapshot_id: current.clone(),
        generated_at: snapshot.created_at.clone(),
        roots: rows,
        totals,
    }))
}

pub(crate) fn encode_root_size_summary(
    paths: &Paths,
    summary: &RootSizeSummary,
) -> Result<Vec<u8>> {
    let cbor = serde_cbor::to_vec(summary)?;
    let compressed = zstd::stream::encode_all(cbor.as_slice(), 3)?;
    crate::encode_object(paths, &compressed)
}

pub(crate) fn write_cached_root_size_summary(
    paths: &Paths,
    summary: &RootSizeSummary,
) -> Result<()> {
    let path = cached_root_size_summary_path(paths, &summary.host_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    write_atomic(&path, &serde_json::to_vec(summary)?)?;
    Ok(())
}

fn cached_root_size_summary_path(paths: &Paths, host_id: &str) -> std::path::PathBuf {
    let safe_host_id = host_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    paths
        .home
        .join("cache")
        .join(format!("root-size-summary-{safe_host_id}.json"))
}

fn read_snapshot_manifest(paths: &Paths, snapshot: &SnapshotExport) -> Result<SnapshotManifest> {
    if !snapshot.manifest_json.trim().is_empty() {
        return Ok(serde_json::from_str(&snapshot.manifest_json)?);
    }
    read_local_metadata_object(paths, &snapshot.manifest_key)
        .with_context(|| format!("read snapshot manifest {}", snapshot.manifest_key))
}

fn read_local_metadata_object<T: for<'de> serde::Deserialize<'de>>(
    paths: &Paths,
    key: &str,
) -> Result<T> {
    let bytes = fs::read(paths.home.join(key))
        .with_context(|| format!("metadata object is not cached locally: {key}"))?;
    let decoded = crate::decode_object(paths, &bytes)?;
    if let Ok(value) = serde_json::from_slice(&decoded) {
        return Ok(value);
    }
    if let Ok(decompressed) =
        zstd_decode_all_limited(decoded.as_slice(), REMOTE_METADATA_DECODE_LIMIT, key)
        && let Ok(value) = serde_cbor::from_slice(&decompressed)
    {
        return Ok(value);
    }
    Err(anyhow!("unsupported metadata object encoding: {key}"))
}

fn local_tree_entries(paths: &Paths, tree: TreeManifest) -> Result<BTreeMap<String, FileRecord>> {
    if !tree.entries.is_empty() || tree.root_node.is_none() {
        return Ok(tree.entries);
    }
    let root_node = tree.root_node.expect("checked above");
    let node: TreeNodeManifest = read_local_metadata_object(paths, &root_node.node_key)
        .with_context(|| format!("read root tree node {}", root_node.node_key))?;
    local_tree_entries_from_node(paths, node)
}

fn local_tree_entries_from_node(
    paths: &Paths,
    node: TreeNodeManifest,
) -> Result<BTreeMap<String, FileRecord>> {
    let mut entries = node.entries;
    for child in node.child_nodes.values() {
        let child_node: TreeNodeManifest = read_local_metadata_object(paths, &child.node_key)
            .with_context(|| format!("read child tree node {}", child.node_key))?;
        entries.extend(local_tree_entries_from_node(paths, child_node)?);
    }
    Ok(entries)
}

fn add_payload_keys(
    paths: &Paths,
    packed_blobs: &BTreeMap<String, PackedBlobSizeRef>,
    payload: &Payload,
    stat: &mut BuilderStat,
) -> Result<()> {
    if let Some((oid, object_key)) = payload_blob_ref(payload) {
        if let Some(packed) = packed_blobs.get(oid) {
            stat.packed_payload_keys.insert(packed.pack_key.clone());
            stat.payload_keys.insert(packed.pack_key.clone());
            stat.metadata_keys.insert(packed.index_key.clone());
            stat.packed_slice_bytes = stat.packed_slice_bytes.saturating_add(packed.pack_len);
        } else {
            stat.payload_keys.insert(object_key.to_string());
        }
        return Ok(());
    }
    if let Some((manifest_key, chunk_count)) = payload_large_manifest(payload) {
        stat.metadata_keys.insert(manifest_key.to_string());
        let manifest: LargeManifest = read_local_metadata_object(paths, manifest_key)
            .with_context(|| format!("read large manifest {manifest_key}"))?;
        if manifest.chunks.len() != chunk_count {
            bail!(
                "large manifest chunk count mismatch for {manifest_key}: payload={chunk_count} manifest={}",
                manifest.chunks.len()
            );
        }
        for chunk in manifest.chunks {
            stat.payload_keys.insert(chunk.object_key);
        }
    }
    Ok(())
}

fn payload_large_manifest(payload: &Payload) -> Option<(&str, usize)> {
    match payload {
        Payload::ChunkedBlob {
            manifest_key,
            chunk_count,
            ..
        }
        | Payload::LargeObject {
            manifest_key,
            chunk_count,
            ..
        }
        | Payload::Large {
            manifest_key,
            chunk_count,
            ..
        } => Some((manifest_key, *chunk_count)),
        _ => None,
    }
}

fn packed_blob_size_refs(paths: &Paths) -> Result<BTreeMap<String, PackedBlobSizeRef>> {
    let conn = crate::open_db(paths)?;
    let mut stmt = conn.prepare(
        "select b.oid, p.pack_key, p.index_key, coalesce(b.pack_len, 0) \
         from blobs b join packs p on b.pack_id=p.pack_id \
         where b.pack_id is not null",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            PackedBlobSizeRef {
                pack_key: row.get(1)?,
                index_key: row.get(2)?,
                pack_len: row.get(3)?,
            },
        ))
    })?;
    let mut packed = BTreeMap::new();
    for row in rows {
        let (oid, reference) = row?;
        packed.insert(oid, reference);
    }
    Ok(packed)
}

fn sum_local_sizes(paths: &Paths, keys: &BTreeSet<String>) -> u64 {
    keys.iter()
        .filter_map(|key| fs::metadata(paths.home.join(key)).ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn missing_local_objects(paths: &Paths, keys: &BTreeSet<String>) -> usize {
    keys.iter()
        .filter(|key| fs::metadata(paths.home.join(key)).is_err())
        .count()
}
