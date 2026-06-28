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
use crate::majutsu_store::{canonical_remote_alias, host_remote_key, remote_host_label};
use crate::remote_store::{RemoteObjectStat, RemoteStore};
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
    packed_payload_oids: BTreeSet<String>,
    packed_slice_bytes: u64,
}

#[derive(Clone)]
struct PackedBlobSizeRef {
    pack_key: String,
    index_key: String,
    pack_len: u64,
}

struct SummaryRemoteSizes {
    sizes: BTreeMap<String, u64>,
    is_s3_remote: bool,
    host_prefix: String,
}

impl SummaryRemoteSizes {
    fn new(config: &Config, remote: &RemoteStore, objects: &[RemoteObjectStat]) -> Self {
        Self {
            sizes: objects
                .iter()
                .map(|object| (object.key.clone(), object.size))
                .collect(),
            is_s3_remote: matches!(remote, RemoteStore::S3(_)),
            host_prefix: remote_host_label(&config.host.name),
        }
    }
}

struct SummaryResolvedKeys {
    found: BTreeSet<String>,
    bytes: u64,
    missing: usize,
}

pub(crate) fn root_size_summary_key(host_id: &str) -> String {
    format!("{host_id}/root-size-summary.cbor.zst.enc")
}

pub(crate) fn build_root_size_summary_with_remote_objects(
    paths: &Paths,
    config: &Config,
    export: &MetadataExport,
    remote: &RemoteStore,
    remote_objects: &[RemoteObjectStat],
) -> Result<Option<RootSizeSummary>> {
    let remote_sizes = SummaryRemoteSizes::new(config, remote, remote_objects);
    build_root_size_summary_inner(paths, config, export, Some(&remote_sizes))
}

fn build_root_size_summary_inner(
    paths: &Paths,
    config: &Config,
    export: &MetadataExport,
    remote_sizes: Option<&SummaryRemoteSizes>,
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
    let known_object_sizes = known_payload_object_sizes(paths)?;
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

    if should_scan_legacy_roots(&manifest) {
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
    }

    let mut rows = Vec::new();
    let mut unique_keys = BTreeSet::new();
    let mut unique_payload_keys = BTreeSet::new();
    let mut unique_metadata_keys = BTreeSet::new();
    let mut unique_packed_payload_keys = BTreeSet::new();
    let mut unique_packed_payload_oids = BTreeSet::new();
    let mut unique_packed_slice_bytes = 0u64;
    for (root, stat) in stats {
        let payload_keys =
            resolve_summary_keys(paths, &known_object_sizes, &stat.payload_keys, remote_sizes);
        let metadata_keys = resolve_summary_keys(
            paths,
            &known_object_sizes,
            &stat.metadata_keys,
            remote_sizes,
        );
        let packed_payload_keys = resolve_summary_keys(
            paths,
            &known_object_sizes,
            &stat.packed_payload_keys,
            remote_sizes,
        );
        let all_keys = payload_keys
            .found
            .union(&metadata_keys.found)
            .cloned()
            .collect::<BTreeSet<_>>();
        unique_keys.extend(all_keys.iter().cloned());
        unique_payload_keys.extend(payload_keys.found.iter().cloned());
        unique_metadata_keys.extend(metadata_keys.found.iter().cloned());
        unique_packed_payload_keys.extend(packed_payload_keys.found.iter().cloned());
        for oid in &stat.packed_payload_oids {
            if unique_packed_payload_oids.insert(oid.clone())
                && let Some(packed) = packed_blobs.get(oid)
            {
                unique_packed_slice_bytes =
                    unique_packed_slice_bytes.saturating_add(packed.pack_len);
            }
        }
        let payload_bytes = payload_keys.bytes;
        let metadata_bytes = metadata_keys.bytes;
        let packed_payload_bytes = packed_payload_keys.bytes;
        let backend_bytes = sum_summary_keys(paths, &known_object_sizes, &all_keys, remote_sizes);
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
            missing_objects: payload_keys.missing + metadata_keys.missing,
        });
    }
    rows.sort_by(|left, right| left.root.cmp(&right.root));

    let current_backend_bytes =
        sum_summary_keys(paths, &known_object_sizes, &unique_keys, remote_sizes);
    let payload_bytes = sum_summary_keys(
        paths,
        &known_object_sizes,
        &unique_payload_keys,
        remote_sizes,
    );
    let metadata_bytes = sum_summary_keys(
        paths,
        &known_object_sizes,
        &unique_metadata_keys,
        remote_sizes,
    );
    let row_used_bytes = rows.iter().map(|row| row.used_bytes).sum();
    let packed_payload_bytes = sum_summary_keys(
        paths,
        &known_object_sizes,
        &unique_packed_payload_keys,
        remote_sizes,
    );
    let unique_used_bytes = current_backend_bytes
        .saturating_sub(packed_payload_bytes)
        .saturating_add(unique_packed_slice_bytes);
    let totals = RootSizeSummaryTotals {
        billed_bytes: 0,
        billed_objects: 0,
        row_used_bytes,
        unique_used_bytes,
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

pub(crate) fn decode_root_size_summary(paths: &Paths, bytes: &[u8]) -> Result<RootSizeSummary> {
    let decoded = crate::decode_object(paths, bytes)?;
    let cbor = zstd_decode_all_limited(
        decoded.as_slice(),
        REMOTE_METADATA_DECODE_LIMIT,
        "root size summary",
    )
    .context("decode root size summary zstd")?;
    serde_cbor::from_slice(&cbor).context("parse root size summary cbor")
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

pub(crate) fn read_cached_root_size_summary(
    paths: &Paths,
    host_id: &str,
) -> Result<Option<RootSizeSummary>> {
    let path = cached_root_size_summary_path(paths, host_id);
    if !path.exists() {
        return Ok(None);
    }
    let summary =
        serde_json::from_slice(&fs::read(path)?).context("parse cached root size summary")?;
    Ok(Some(summary))
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
            stat.packed_payload_oids.insert(oid.to_string());
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

fn should_scan_legacy_roots(manifest: &SnapshotManifest) -> bool {
    manifest.root_trees.is_empty()
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

fn known_payload_object_sizes(paths: &Paths) -> Result<BTreeMap<String, u64>> {
    let conn = crate::open_db(paths)?;
    let mut sizes = BTreeMap::new();
    let mut pack_stmt = conn.prepare("select pack_key, size from packs")?;
    let pack_rows = pack_stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
    })?;
    for row in pack_rows {
        let (key, size) = row?;
        sizes.insert(key, size);
    }
    Ok(sizes)
}

fn resolve_summary_keys(
    paths: &Paths,
    known_sizes: &BTreeMap<String, u64>,
    keys: &BTreeSet<String>,
    remote_sizes: Option<&SummaryRemoteSizes>,
) -> SummaryResolvedKeys {
    let mut found = BTreeSet::new();
    let mut bytes = 0u64;
    let mut missing = 0usize;
    for key in keys {
        if let Some(remote_sizes) = remote_sizes {
            let Some(remote_key) = summary_remote_key_candidates(key, remote_sizes)
                .into_iter()
                .find(|candidate| remote_sizes.sizes.contains_key(candidate))
            else {
                missing += 1;
                continue;
            };
            bytes = bytes.saturating_add(remote_sizes.sizes.get(&remote_key).copied().unwrap_or(0));
            found.insert(remote_key);
            continue;
        }
        if local_or_known_size(paths, known_sizes, key).is_some() {
            found.insert(key.clone());
        } else {
            missing += 1;
        }
    }
    if remote_sizes.is_none() {
        bytes = sum_summary_keys(paths, known_sizes, &found, None);
    }
    SummaryResolvedKeys {
        found,
        bytes,
        missing,
    }
}

fn sum_summary_keys(
    paths: &Paths,
    known_sizes: &BTreeMap<String, u64>,
    keys: &BTreeSet<String>,
    remote_sizes: Option<&SummaryRemoteSizes>,
) -> u64 {
    if let Some(remote_sizes) = remote_sizes {
        return keys
            .iter()
            .filter_map(|key| remote_sizes.sizes.get(key).copied())
            .sum();
    }
    keys.iter()
        .filter_map(|key| local_or_known_size(paths, known_sizes, key))
        .sum()
}

fn local_or_known_size(
    paths: &Paths,
    known_sizes: &BTreeMap<String, u64>,
    key: &str,
) -> Option<u64> {
    if let Some(size) = known_sizes.get(key).copied() {
        return Some(size);
    }
    fs::metadata(paths.home.join(key))
        .ok()
        .map(|metadata| metadata.len())
}

fn summary_remote_key_candidates(key: &str, remote_sizes: &SummaryRemoteSizes) -> Vec<String> {
    let alias = canonical_remote_alias(key).filter(|alias| alias != key);
    match (remote_sizes.is_s3_remote, alias) {
        (true, Some(alias)) => vec![
            host_remote_key(&remote_sizes.host_prefix, &alias),
            host_remote_key(&remote_sizes.host_prefix, key),
        ],
        (true, None) => vec![host_remote_key(&remote_sizes.host_prefix, key)],
        (false, Some(alias)) => vec![key.to_string(), alias],
        (false, None) => vec![key.to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::resolve_paths;
    use crate::majutsu_core::RootSnapshot;
    use chrono::Utc;

    #[test]
    fn summary_sizes_prefer_remote_canonical_size_over_logical_chunk_size() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = resolve_paths(Some(tmp.path().join("state"))).unwrap();
        let key = "objects/large/chunks/fixed/chunk-1".to_string();
        let mut keys = BTreeSet::new();
        keys.insert(key.clone());
        let known_sizes = BTreeMap::from([(key, 1024_u64 * 1024)]);
        let remote_sizes = SummaryRemoteSizes {
            sizes: BTreeMap::from([(
                "vagrant/large/chunks/fixed-8m/chunk-1.chunk.enc".to_string(),
                123_u64,
            )]),
            is_s3_remote: true,
            host_prefix: "vagrant".to_string(),
        };

        let resolved = resolve_summary_keys(&paths, &known_sizes, &keys, Some(&remote_sizes));

        assert_eq!(resolved.bytes, 123);
        assert_eq!(resolved.missing, 0);
        assert!(
            resolved
                .found
                .contains("vagrant/large/chunks/fixed-8m/chunk-1.chunk.enc")
        );
    }

    #[test]
    fn legacy_roots_are_scanned_only_when_tree_manifest_is_absent() {
        let mut manifest = SnapshotManifest {
            snapshot_id: "snap-test".into(),
            parent: None,
            op_id: "op-test".into(),
            timestamp: Utc::now(),
            roots: BTreeMap::new(),
            root_trees: BTreeMap::new(),
        };
        assert!(should_scan_legacy_roots(&manifest));

        manifest.root_trees.insert(
            "sample".into(),
            RootSnapshot {
                tree_id: "tree-test".into(),
                tree_key: "objects/trees/tree-test.json".into(),
                file_count: 1,
            },
        );
        assert!(!should_scan_legacy_roots(&manifest));
    }
}
