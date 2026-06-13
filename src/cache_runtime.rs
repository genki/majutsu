use anyhow::{Context, Result, anyhow};
use majutsu_store::canonical_remote_alias;
use std::collections::BTreeSet;
use std::fs;

use crate::cli::{CacheCommand, CachePruneArgs};
use crate::config::{MetadataExport, Paths, read_config};
use crate::operation_log::record_op;
use crate::remote_store::{RemoteStore, open_remote};
use crate::snapshot_state::current_snapshot;
use crate::{ensure_ready, export_metadata, open_db, remote_object_available};

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
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct PayloadCachePruneStats {
    pub(crate) candidates: usize,
    pub(crate) candidate_bytes: u64,
    pub(crate) remote_missing: usize,
    pub(crate) local_missing: usize,
    pub(crate) removed: usize,
    pub(crate) removed_bytes: u64,
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
            remote_missing: self.remote_missing,
            local_missing: self.local_missing,
            removed: 0,
            removed_bytes: 0,
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
    let plan = build_cache_prune_plan(paths, &remote, &export)?;
    let stats = plan.stats();
    let dry_run = stat_only || args.dry_run;
    println!("payload_cache_candidates {}", stats.candidates);
    println!("payload_cache_bytes {}", stats.candidate_bytes);
    println!("payload_cache_synced_prunable {}", stats.candidates);
    println!(
        "payload_cache_synced_prunable_bytes {}",
        stats.candidate_bytes
    );
    println!("payload_cache_remote_missing {}", stats.remote_missing);
    println!("payload_cache_unsynced_required {}", stats.remote_missing);
    println!("payload_cache_local_missing {}", stats.local_missing);
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
    println!("removed_payload_cache_objects {}", removed_stats.removed);
    println!(
        "removed_payload_cache_bytes {}",
        removed_stats.removed_bytes
    );
    Ok(())
}

pub(crate) fn prune_synced_payload_cache(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<PayloadCachePruneStats> {
    let plan = build_cache_prune_plan(paths, remote, export)?;
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
        });
    }
    if local_candidates.is_empty() {
        return Ok(CachePrunePlan {
            local_missing,
            ..CachePrunePlan::default()
        });
    }
    let mut plan = if local_candidates.len() <= 64 {
        filter_remote_available_by_head(remote, local_candidates)?
    } else {
        let remote_keys = remote_payload_key_index(remote)?;
        filter_remote_available_from_index(&remote_keys, local_candidates)
    };
    plan.local_missing = local_missing;
    plan.candidates.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(plan)
}

fn filter_remote_available_by_head(
    remote: &RemoteStore,
    candidates: Vec<CachePruneCandidate>,
) -> Result<CachePrunePlan> {
    let mut plan = CachePrunePlan::default();
    for candidate in candidates {
        if remote_object_available(remote, &candidate.key)? {
            plan.candidates.push(candidate);
        } else {
            plan.remote_missing += 1;
        }
    }
    Ok(plan)
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
    let mut keys = BTreeSet::new();
    for prefix in [
        "objects/blobs/",
        "blobs/loose/",
        "objects/packs/",
        "packs/",
        "objects/large/chunks/",
        "large/chunks/",
    ] {
        keys.extend(remote.list(prefix)?);
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

pub(crate) fn payload_cache_key_set(export: &MetadataExport) -> std::collections::BTreeSet<String> {
    payload_cache_keys(export).into_iter().collect()
}
