use crate::majutsu_store::{canonical_remote_alias, host_remote_key, remote_host_label};
use anyhow::{Context, Result, anyhow};
use std::collections::BTreeSet;
use std::fs;
use std::thread;

use crate::cli::{CacheCommand, CachePruneArgs};
use crate::config::{MetadataExport, Paths, read_config};
use crate::operation_log::record_op;
use crate::remote_store::{RemoteStore, open_remote};
use crate::snapshot_state::current_snapshot;
use crate::{ensure_ready, export_metadata, open_db, remote_object_available_for_paths};

#[derive(Debug, Default)]
struct CachePrunePlan {
    candidates: Vec<CachePruneCandidate>,
    remote_missing: usize,
    local_missing: usize,
}

#[derive(Debug)]
struct CachePruneCandidate {
    key: String,
    bytes: u64,
    metadata: bool,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct PayloadCachePruneStats {
    pub(crate) candidates: usize,
    pub(crate) candidate_bytes: u64,
    pub(crate) payload_candidates: usize,
    pub(crate) payload_candidate_bytes: u64,
    pub(crate) remote_missing: usize,
    pub(crate) local_missing: usize,
    pub(crate) removed: usize,
    pub(crate) removed_bytes: u64,
    pub(crate) payload_removed: usize,
    pub(crate) payload_removed_bytes: u64,
    pub(crate) metadata_candidates: usize,
    pub(crate) metadata_candidate_bytes: u64,
    pub(crate) metadata_removed: usize,
    pub(crate) metadata_removed_bytes: u64,
}

impl CachePrunePlan {
    fn candidate_count(&self) -> usize {
        self.candidates.len()
    }

    fn candidate_bytes(&self) -> u64 {
        self.candidates
            .iter()
            .map(|candidate| candidate.bytes)
            .sum()
    }

    fn stats(&self) -> PayloadCachePruneStats {
        PayloadCachePruneStats {
            candidates: self.candidate_count(),
            candidate_bytes: self.candidate_bytes(),
            payload_candidates: self
                .candidates
                .iter()
                .filter(|candidate| !candidate.metadata)
                .count(),
            payload_candidate_bytes: self
                .candidates
                .iter()
                .filter(|candidate| !candidate.metadata)
                .map(|candidate| candidate.bytes)
                .sum(),
            metadata_candidates: self
                .candidates
                .iter()
                .filter(|candidate| candidate.metadata)
                .count(),
            metadata_candidate_bytes: self
                .candidates
                .iter()
                .filter(|candidate| candidate.metadata)
                .map(|candidate| candidate.bytes)
                .sum(),
            remote_missing: self.remote_missing,
            local_missing: self.local_missing,
            removed: 0,
            removed_bytes: 0,
            payload_removed: 0,
            payload_removed_bytes: 0,
            metadata_removed: 0,
            metadata_removed_bytes: 0,
        }
    }
}

pub(crate) fn cache_cmd(paths: &Paths, command: CacheCommand) -> Result<()> {
    ensure_ready(paths)?;
    match command {
        CacheCommand::Stat(args) => cache_prune(paths, args, true),
        CacheCommand::Prune(args) => cache_prune(paths, args, false),
    }
}

fn cache_prune(paths: &Paths, args: CachePruneArgs, stat_only: bool) -> Result<()> {
    let config = read_config(paths)?;
    let remote_config = config
        .remote
        .as_ref()
        .ok_or_else(|| anyhow!("remote is not configured; synced cache cannot be proven safe"))?;
    let remote = open_remote(remote_config)?;
    let conn = open_db(paths)?;
    let export = export_metadata(paths, &conn, &config)?;
    let plan = build_cache_prune_plan(paths, &remote, &export, args.metadata)?;
    let stats = plan.stats();
    let dry_run = stat_only || args.dry_run;
    println!("cache_candidates {}", stats.candidates);
    println!("cache_bytes {}", stats.candidate_bytes);
    println!("payload_cache_candidates {}", stats.payload_candidates);
    println!("payload_cache_bytes {}", stats.payload_candidate_bytes);
    println!("payload_cache_synced_prunable {}", stats.payload_candidates);
    println!(
        "payload_cache_synced_prunable_bytes {}",
        stats.payload_candidate_bytes
    );
    println!("payload_cache_remote_missing {}", stats.remote_missing);
    println!("payload_cache_unsynced_required {}", stats.remote_missing);
    println!("payload_cache_local_missing {}", stats.local_missing);
    println!("metadata_cache_candidates {}", stats.metadata_candidates);
    println!("metadata_cache_bytes {}", stats.metadata_candidate_bytes);
    println!("dry_run {}", dry_run);
    if dry_run {
        return Ok(());
    }
    let removed_stats = remove_payload_cache_candidates(paths, &plan)?;
    let current = current_snapshot(&conn)?;
    record_op(
        &conn,
        "cache-prune",
        current.as_deref(),
        current.as_deref(),
        Some(&format!(
            "removed {} synced payload cache objects ({} bytes)",
            removed_stats.removed, removed_stats.removed_bytes
        )),
    )?;
    println!("removed_cache_objects {}", removed_stats.removed);
    println!("removed_cache_bytes {}", removed_stats.removed_bytes);
    println!(
        "removed_payload_cache_objects {}",
        removed_stats.payload_removed
    );
    println!(
        "removed_payload_cache_bytes {}",
        removed_stats.payload_removed_bytes
    );
    println!(
        "removed_metadata_cache_objects {}",
        removed_stats.metadata_removed
    );
    println!(
        "removed_metadata_cache_bytes {}",
        removed_stats.metadata_removed_bytes
    );
    Ok(())
}

pub(crate) fn prune_synced_payload_cache(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<PayloadCachePruneStats> {
    let plan = build_cache_prune_plan(paths, remote, export, false)?;
    remove_payload_cache_candidates(paths, &plan)
}

pub(crate) fn prune_synced_metadata_cache(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<PayloadCachePruneStats> {
    let mut local_missing = 0usize;
    let mut local_candidates = Vec::new();
    for key in metadata_cache_keys(paths, export)? {
        let path = paths.home.join(&key);
        let Ok(metadata) = path.metadata() else {
            local_missing += 1;
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        local_candidates.push(CachePruneCandidate {
            key,
            bytes: metadata.len(),
            metadata: true,
        });
    }
    let config = read_config(paths)?;
    let mut plan = if local_candidates.is_empty() {
        CachePrunePlan {
            local_missing,
            ..CachePrunePlan::default()
        }
    } else if local_candidates.len() <= 64 {
        filter_remote_available_by_head(paths, remote, local_candidates)?
    } else {
        let remote_keys =
            remote_payload_key_index_for_host(remote, &remote_host_label(&config.host.name))?;
        filter_remote_available_from_index(&remote_keys, local_candidates)
    };
    plan.local_missing = local_missing;
    plan.candidates.sort_by(|a, b| a.key.cmp(&b.key));
    remove_payload_cache_candidates(paths, &plan)
}

fn remove_payload_cache_candidates(
    paths: &Paths,
    plan: &CachePrunePlan,
) -> Result<PayloadCachePruneStats> {
    let mut stats = plan.stats();
    for candidate in &plan.candidates {
        let path = paths.home.join(&candidate.key);
        match fs::remove_file(&path) {
            Ok(()) => {
                stats.removed += 1;
                stats.removed_bytes += candidate.bytes;
                if candidate.metadata {
                    stats.metadata_removed += 1;
                    stats.metadata_removed_bytes += candidate.bytes;
                } else {
                    stats.payload_removed += 1;
                    stats.payload_removed_bytes += candidate.bytes;
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err).with_context(|| format!("remove {}", candidate.key)),
        }
    }
    Ok(stats)
}

fn build_cache_prune_plan(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
    include_metadata: bool,
) -> Result<CachePrunePlan> {
    let mut local_missing = 0usize;
    let mut local_candidates = Vec::new();
    for key in payload_cache_keys(export) {
        let path = paths.home.join(&key);
        let Ok(metadata) = path.metadata() else {
            local_missing += 1;
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        local_candidates.push(CachePruneCandidate {
            key,
            bytes: metadata.len(),
            metadata: false,
        });
    }
    if include_metadata {
        for key in metadata_cache_keys(paths, export)? {
            let path = paths.home.join(&key);
            let Ok(metadata) = path.metadata() else {
                local_missing += 1;
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            local_candidates.push(CachePruneCandidate {
                key,
                bytes: metadata.len(),
                metadata: true,
            });
        }
    }
    if local_candidates.is_empty() {
        return Ok(CachePrunePlan {
            local_missing,
            ..CachePrunePlan::default()
        });
    }
    let config = read_config(paths)?;
    let mut plan = if local_candidates.len() <= 64 {
        filter_remote_available_by_head(paths, remote, local_candidates)?
    } else {
        let remote_keys =
            remote_payload_key_index_for_host(remote, &remote_host_label(&config.host.name))?;
        filter_remote_available_from_index(&remote_keys, local_candidates)
    };
    plan.local_missing = local_missing;
    plan.candidates.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(plan)
}

fn filter_remote_available_by_head(
    paths: &Paths,
    remote: &RemoteStore,
    candidates: Vec<CachePruneCandidate>,
) -> Result<CachePrunePlan> {
    let mut plan = CachePrunePlan::default();
    for batch in candidates.chunks(cache_remote_parallelism()) {
        let availability = thread::scope(|scope| {
            let handles = batch
                .iter()
                .map(|candidate| {
                    scope.spawn(move || {
                        remote_object_available_for_paths(paths, remote, &candidate.key)
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .map_err(|_| anyhow!("cache remote probe worker panicked"))?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        for (candidate, available) in batch.iter().zip(availability) {
            if available {
                plan.candidates.push(CachePruneCandidate {
                    key: candidate.key.clone(),
                    bytes: candidate.bytes,
                    metadata: candidate.metadata,
                });
            } else {
                plan.remote_missing += 1;
            }
        }
    }
    Ok(plan)
}

fn cache_remote_parallelism() -> usize {
    std::env::var("MAJUTSU_CACHE_REMOTE_PARALLELISM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16)
        .min(64)
}

fn filter_remote_available_from_index(
    remote_keys: &BTreeSet<String>,
    candidates: Vec<CachePruneCandidate>,
) -> CachePrunePlan {
    let mut plan = CachePrunePlan::default();
    for candidate in candidates {
        if remote_keys.contains(&candidate.key)
            || canonical_remote_alias(&candidate.key)
                .as_ref()
                .is_some_and(|alias| remote_keys.contains(alias))
        {
            plan.candidates.push(candidate);
        } else {
            plan.remote_missing += 1;
        }
    }
    plan
}

pub(crate) fn remote_payload_key_index(remote: &RemoteStore) -> Result<BTreeSet<String>> {
    remote_payload_key_index_with_host(remote, None)
}

pub(crate) fn remote_payload_key_index_for_host(
    remote: &RemoteStore,
    host_id: &str,
) -> Result<BTreeSet<String>> {
    remote_payload_key_index_with_host(remote, Some(host_id))
}

fn remote_payload_key_index_with_host(
    remote: &RemoteStore,
    host_id: Option<&str>,
) -> Result<BTreeSet<String>> {
    let prefixes = [
        "objects/blobs/",
        "blobs/loose/",
        "objects/packs/",
        "packs/",
        "objects/indexes/pack/",
        "indexes/pack-index/",
        "objects/large/chunks/",
        "large/chunks/",
        "objects/large/manifests/",
        "large/manifests/",
        "objects/trees/",
        "trees/",
    ];
    let listed = thread::scope(|scope| {
        let handles = prefixes
            .iter()
            .map(|prefix| {
                scope.spawn(move || {
                    let remote_prefix = host_id
                        .map(|host_id| host_remote_key(host_id, prefix))
                        .unwrap_or_else(|| prefix.to_string());
                    remote.list(&remote_prefix)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow!("remote payload index worker panicked"))?
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let host_prefix = host_id.map(|host_id| format!("{host_id}/"));
    let mut keys = BTreeSet::new();
    for listed_keys in listed {
        for key in listed_keys {
            if let Some(host_prefix) = host_prefix.as_deref() {
                let Some(stripped) = key.strip_prefix(host_prefix) else {
                    continue;
                };
                keys.insert(stripped.to_string());
            } else {
                keys.insert(key);
            }
        }
    }
    Ok(keys)
}

pub(crate) fn remote_payload_index_contains(remote_keys: &BTreeSet<String>, key: &str) -> bool {
    remote_keys.contains(key)
        || canonical_remote_alias(key)
            .as_ref()
            .is_some_and(|alias| remote_keys.contains(alias))
}

fn payload_cache_keys(export: &MetadataExport) -> Vec<String> {
    let mut keys = Vec::new();
    keys.extend(
        export
            .blobs
            .iter()
            .filter(|blob| blob.pack_id.is_none())
            .map(|blob| blob.object_key.clone()),
    );
    keys.extend(export.packs.iter().map(|pack| pack.pack_key.clone()));
    keys.extend(export.chunks.iter().map(|chunk| chunk.object_key.clone()));
    keys.sort();
    keys.dedup();
    keys
}

fn metadata_cache_keys(paths: &Paths, export: &MetadataExport) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for snapshot in &export.snapshots {
        let manifest = if snapshot.manifest_json.trim().is_empty() {
            let path = paths.home.join(&snapshot.manifest_key);
            if !path.exists() {
                continue;
            }
            let bytes = fs::read(path)?;
            serde_json::from_slice::<crate::majutsu_core::SnapshotManifest>(&crate::decode_object(
                paths, &bytes,
            )?)?
        } else {
            serde_json::from_str::<crate::majutsu_core::SnapshotManifest>(&snapshot.manifest_json)?
        };
        {
            keys.extend(
                manifest
                    .root_trees
                    .values()
                    .map(|root_tree| root_tree.tree_key.clone()),
            );
        }
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}
