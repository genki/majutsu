use crate::majutsu_store::{
    LEGACY_METADATA_EXPORT_KEY, REMOTE_HOST_INDEX_KEY, RemoteGcMark as GcMarkExport,
    RemoteGcTombstone as GcTombstoneExport, RemoteHostIndex, RemoteHostSummary,
    host_current_ref_key, host_last_synced_ref_key, host_metadata_key, remote_gc_mark_key,
    remote_gc_tombstone_prefix, select_remote_host,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;

use crate::cli::RemoteCommand;
use crate::config::{MetadataExport, Paths, read_config};
use crate::fsck_runtime::{RemoteFsckOptions, remote_fsck_with_options};
use crate::majutsu_core::SnapshotManifest;
use crate::majutsu_store::canonical_remote_alias;
use crate::object_paths::local_object_keys;
use crate::operation_log::record_op;
use crate::remote_store::{RemoteStore, open_remote_with_upload_policy};
use crate::snapshot_state::current_snapshot;
use crate::util::{blake3_hex, parse_db_time};
use crate::{
    decode_object, ensure_ready, export_metadata, open_db, remote_object_available, remote_ref,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[derive(Debug, serde::Deserialize)]
struct RemoteHeadExport {
    version: u32,
    host_id: String,
    host_name: String,
    current_snapshot: Option<String>,
    last_synced: Option<String>,
    #[serde(default)]
    root_acks: BTreeMap<String, RemoteRootAck>,
    metadata_key: String,
}

#[derive(Debug, serde::Deserialize)]
struct RemoteRootAck {
    snapshot_id: String,
    tree_id: String,
    tree_key: String,
    file_count: usize,
    synced_at: Option<String>,
}

fn remote_head_key(host_id: &str) -> String {
    format!("hosts/{host_id}/head.cbor.zst.enc")
}

fn read_remote_head(
    paths: &Paths,
    remote: &RemoteStore,
    host_id: &str,
) -> Result<Option<RemoteHeadExport>> {
    let Some(bytes) = remote.get_optional(&remote_head_key(host_id))? else {
        return Ok(None);
    };
    let decoded = decode_object(paths, &bytes)?;
    let decompressed = zstd::stream::decode_all(decoded.as_slice())?;
    Ok(Some(serde_cbor::from_slice(&decompressed)?))
}

pub(crate) fn remote_cmd(paths: &Paths, command: RemoteCommand) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let remote = open_remote_with_upload_policy(
        config
            .remote
            .as_ref()
            .ok_or_else(|| anyhow!("remote is not configured; run `mj init --remote ...`"))?,
        config.large.multipart,
        config.large.max_parallel_uploads,
    )?;
    match command {
        RemoteCommand::Check => {
            let keys = remote.list("")?;
            println!("remote {}", remote.describe());
            println!("objects {}", keys.len());
            let metadata_key = if remote.exists(REMOTE_HOST_INDEX_KEY)? {
                REMOTE_HOST_INDEX_KEY
            } else if remote.exists(LEGACY_METADATA_EXPORT_KEY)? {
                LEGACY_METADATA_EXPORT_KEY
            } else {
                bail!(
                    "remote metadata is missing: metadata/export.json and hosts/index.json not found"
                );
            };
            if remote.exists(metadata_key)? {
                println!("metadata ok");
                println!("metadata_key {metadata_key}");
                let first = remote.get_range(metadata_key, 0, 1)?;
                println!("range_get {}", first.len());
            }
        }
        RemoteCommand::Fsck {
            objects,
            parallelism,
            sample,
            timeout_secs,
            deep,
            payload_only,
        } => {
            if deep {
                remote_fsck_with_options(
                    paths,
                    &remote,
                    RemoteFsckOptions {
                        metadata: !payload_only,
                        payload: true,
                        payload_sample: sample,
                        timeout: timeout_secs.map(Duration::from_secs),
                    },
                )?;
            } else if objects {
                remote_fsck_objects(
                    paths,
                    &remote,
                    RemoteObjectScanOptions {
                        parallelism,
                        sample,
                        timeout: timeout_secs.map(Duration::from_secs),
                    },
                )?;
            } else {
                remote_fsck_quick(paths, &remote)?;
            }
            let conn = open_db(paths)?;
            let current = current_snapshot(&conn)?;
            record_op(
                &conn,
                "fsck",
                current.as_deref(),
                current.as_deref(),
                Some(if deep {
                    "checked remote state deeply"
                } else if objects {
                    "checked remote object existence"
                } else {
                    "checked remote metadata health"
                }),
            )?;
        }
        RemoteCommand::Repair {
            dry_run,
            parallelism,
            sample,
            timeout_secs,
        } => {
            remote_repair(
                paths,
                &remote,
                RemoteObjectScanOptions {
                    parallelism,
                    sample,
                    timeout: timeout_secs.map(Duration::from_secs),
                },
                dry_run,
            )?;
            let conn = open_db(paths)?;
            let current = current_snapshot(&conn)?;
            record_op(
                &conn,
                "remote-repair",
                current.as_deref(),
                current.as_deref(),
                Some(if dry_run {
                    "planned remote object repair"
                } else {
                    "re-uploaded missing remote objects"
                }),
            )?;
        }
        RemoteCommand::Capabilities => {
            let capabilities = remote.capabilities();
            println!("remote {}", remote.describe());
            println!("lifecycle_rules {}", capabilities.lifecycle_rules);
            println!("object_tags {}", capabilities.object_tags);
            println!("storage_class_on_put {}", capabilities.storage_class_on_put);
            println!(
                "restore_archived_object {}",
                capabilities.restore_archived_object
            );
            println!("multipart_upload {}", capabilities.multipart_upload);
            println!("range_get {}", capabilities.range_get);
            println!("conditional_put {}", capabilities.conditional_put);
        }
        RemoteCommand::Hosts => {
            let index = remote_host_index_with_legacy(&remote)?;
            println!("remote {}", remote.describe());
            println!("hosts {}", index.hosts.len());
            for host in index.hosts {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    host.id,
                    host.name,
                    host.last_synced_at.to_rfc3339(),
                    host.current_snapshot.unwrap_or_else(|| "(none)".into()),
                    host.metadata_key
                );
            }
        }
        RemoteCommand::Host {
            id,
            snapshots,
            operations,
        } => {
            let index = remote_host_index_with_legacy(&remote)?;
            let host = select_remote_host(index.hosts, &id)?;
            let metadata_bytes = remote.get(&host.metadata_key)?;
            let export: MetadataExport = serde_json::from_slice(&decode_remote_metadata_bytes(
                &host.metadata_key,
                &metadata_bytes,
            )?)?;
            println!("id {}", host.id);
            println!("name {}", host.name);
            println!("last_synced_at {}", host.last_synced_at.to_rfc3339());
            println!(
                "current_snapshot {}",
                host.current_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!("metadata_key {}", host.metadata_key);
            println!("roots {}", export.roots.len());
            println!("snapshots {}", export.snapshots.len());
            println!("operations {}", export.operations.len());
            if snapshots {
                print_remote_host_snapshots(&export);
            }
            if operations {
                print_remote_host_operations(&export);
            }
        }
    }
    Ok(())
}

fn remote_fsck_quick(paths: &Paths, remote: &RemoteStore) -> Result<()> {
    let start = std::time::Instant::now();
    let mut missing = 0usize;
    let mut checked_metadata = 0usize;
    let index = remote_host_index_with_legacy(remote)?;
    if index.hosts.is_empty() {
        bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found");
    }
    for issue in index.duplicate_issues() {
        missing += 1;
        eprintln!("remote host index issue: {issue:?}");
    }
    for host in &index.hosts {
        checked_metadata += 1;
        let expected_metadata_key = host_metadata_key(&host.id);
        let expected_compressed_metadata_key = compressed_metadata_key(&expected_metadata_key);
        if host.metadata_key != expected_metadata_key
            && host.metadata_key != expected_compressed_metadata_key
            && host.metadata_key != LEGACY_METADATA_EXPORT_KEY
        {
            missing += 1;
            eprintln!(
                "host index metadata_key {} does not match canonical key {}",
                host.metadata_key, expected_metadata_key
            );
        }
        let Some(bytes) = remote.get_optional(&host.metadata_key)? else {
            missing += 1;
            eprintln!("missing host metadata {} {}", host.id, host.metadata_key);
            continue;
        };
        let metadata_bytes = match decode_remote_metadata_bytes(&host.metadata_key, &bytes) {
            Ok(bytes) => bytes,
            Err(err) => {
                missing += 1;
                eprintln!("invalid host metadata {}: {err}", host.metadata_key);
                continue;
            }
        };
        let export: MetadataExport = match serde_json::from_slice(&metadata_bytes) {
            Ok(export) => export,
            Err(err) => {
                missing += 1;
                eprintln!("invalid host metadata {}: {err}", host.metadata_key);
                continue;
            }
        };
        validate_quick_host_metadata(paths, remote, host, &export, &mut missing)?;
    }
    if missing > 0 {
        bail!("remote fsck quick found {missing} issue(s)");
    }
    println!("remote fsck quick ok");
    println!("mode metadata");
    println!("checked_metadata {checked_metadata}");
    println!("elapsed_secs {}", start.elapsed().as_secs());
    println!("hint use `mj remote fsck --objects` to verify every referenced object");
    println!("hint use `mj remote fsck --deep` for payload decode/hash verification");
    Ok(())
}

fn compressed_metadata_key(key: &str) -> String {
    format!("{key}.zst")
}

fn decode_remote_metadata_bytes(key: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    if key.ends_with(".zst") {
        return zstd::stream::decode_all(bytes)
            .with_context(|| format!("decode compressed metadata {key}"));
    }
    Ok(bytes.to_vec())
}

fn validate_quick_host_metadata(
    paths: &Paths,
    remote: &RemoteStore,
    host: &RemoteHostSummary,
    export: &MetadataExport,
    missing: &mut usize,
) -> Result<()> {
    if export.config.host.id != host.id {
        *missing += 1;
        eprintln!(
            "host index id {} does not match metadata host id {}",
            host.id, export.config.host.id
        );
    }
    if export.config.host.name != host.name {
        *missing += 1;
        eprintln!(
            "host index name {} does not match metadata host name {}",
            host.name, export.config.host.name
        );
    }
    let current = export.refs.get("current");
    if host.current_snapshot.as_ref() != current {
        *missing += 1;
        eprintln!(
            "host index current snapshot does not match metadata for {}",
            host.id
        );
    }
    let head = read_remote_head(paths, remote, &host.id)?;
    if let Some(head) = head.as_ref() {
        if head.version != 1 {
            *missing += 1;
            eprintln!(
                "unsupported remote head version {}",
                remote_head_key(&host.id)
            );
        }
        if head.host_id != host.id {
            *missing += 1;
            eprintln!(
                "remote head host id {} does not match {}",
                head.host_id, host.id
            );
        }
        if head.host_name != export.config.host.name {
            *missing += 1;
            eprintln!(
                "remote head host name does not match metadata for {}",
                host.id
            );
        }
        if head.metadata_key != host.metadata_key {
            *missing += 1;
            eprintln!(
                "remote head metadata key does not match host index for {}",
                host.id
            );
        }
        if head.current_snapshot.as_ref() != current {
            *missing += 1;
            eprintln!(
                "remote head current snapshot does not match metadata for {}",
                host.id
            );
        }
        validate_remote_head_root_acks(paths, remote, export, head, missing)?;
    }
    if let Some(current) = current {
        let key = host_current_ref_key(&host.id);
        match remote_ref(remote, &key)? {
            Some(value) if value == *current => {}
            Some(value) => {
                if !matches!(remote, RemoteStore::S3(_)) || head.is_none() {
                    *missing += 1;
                    eprintln!("{key} points to {value}, expected {current}");
                }
            }
            None => {
                if !matches!(remote, RemoteStore::S3(_)) || head.is_none() {
                    *missing += 1;
                    eprintln!("missing remote ref {key}");
                }
            }
        }
    }
    if let Some(last_synced) = export.refs.get("last-synced") {
        match parse_db_time(last_synced) {
            Ok(value) if host.last_synced_at == value => {}
            Ok(value) => {
                *missing += 1;
                eprintln!(
                    "host index last_synced_at {} does not match metadata last-synced {} for {}",
                    host.last_synced_at.to_rfc3339(),
                    value.to_rfc3339(),
                    host.id
                );
            }
            Err(err) => {
                *missing += 1;
                eprintln!("invalid metadata last-synced for {}: {err}", host.id);
            }
        }
        let key = host_last_synced_ref_key(&host.id);
        if let Some(head) = head.as_ref()
            && head.last_synced.as_ref() != Some(last_synced)
        {
            *missing += 1;
            eprintln!(
                "remote head last-synced does not match metadata for {}",
                host.id
            );
        }
        match remote_ref(remote, &key)? {
            Some(value) if value == *last_synced => {}
            Some(value) => {
                if !matches!(remote, RemoteStore::S3(_)) || head.is_none() {
                    *missing += 1;
                    eprintln!("{key} points to {value}, expected {last_synced}");
                }
            }
            None => {
                if !matches!(remote, RemoteStore::S3(_)) || head.is_none() {
                    *missing += 1;
                    eprintln!("missing remote ref {key}");
                }
            }
        }
    }
    validate_quick_gc_records(remote, &host.id, current, head.as_ref(), missing)
}

fn validate_remote_head_root_acks(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
    head: &RemoteHeadExport,
    missing: &mut usize,
) -> Result<()> {
    let Some(current) = export.refs.get("current") else {
        if !head.root_acks.is_empty() {
            *missing += 1;
            eprintln!("remote head has root_acks without current snapshot");
        }
        return Ok(());
    };
    let manifest = match remote_current_snapshot_manifest(paths, remote, export, current) {
        Ok(manifest) => manifest,
        Err(err) => {
            *missing += 1;
            eprintln!("remote head root_acks cannot load current snapshot {current}: {err:#}");
            return Ok(());
        }
    };
    for root_id in manifest.root_trees.keys() {
        if !head.root_acks.contains_key(root_id) {
            *missing += 1;
            eprintln!("remote head root_acks missing root {root_id}");
        }
    }
    for (root_id, ack) in &head.root_acks {
        let Some(root_tree) = manifest.root_trees.get(root_id) else {
            *missing += 1;
            eprintln!("remote head root_acks contains unexpected root {root_id}");
            continue;
        };
        if ack.snapshot_id != *current {
            *missing += 1;
            eprintln!(
                "remote head root_acks {root_id} snapshot {} does not match current {current}",
                ack.snapshot_id
            );
        }
        if ack.tree_id != root_tree.tree_id {
            *missing += 1;
            eprintln!("remote head root_acks {root_id} tree_id does not match current snapshot");
        }
        if ack.tree_key != root_tree.tree_key {
            *missing += 1;
            eprintln!("remote head root_acks {root_id} tree_key does not match current snapshot");
        }
        if ack.file_count != root_tree.file_count {
            *missing += 1;
            eprintln!("remote head root_acks {root_id} file_count does not match current snapshot");
        }
        if let Some(synced_at) = ack.synced_at.as_deref()
            && let Err(err) = parse_db_time(synced_at)
        {
            *missing += 1;
            eprintln!("remote head root_acks {root_id} synced_at is invalid: {err}");
        }
        if ack.synced_at != head.last_synced {
            *missing += 1;
            eprintln!("remote head root_acks {root_id} synced_at does not match head last-synced");
        }
    }
    Ok(())
}

fn remote_current_snapshot_manifest(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
    current: &str,
) -> Result<SnapshotManifest> {
    let snapshot = export
        .snapshots
        .iter()
        .find(|snapshot| snapshot.id == current)
        .ok_or_else(|| anyhow!("metadata is missing current snapshot export {current}"))?;
    if !snapshot.manifest_json.trim().is_empty() {
        return serde_json::from_str(&snapshot.manifest_json)
            .with_context(|| format!("parse current snapshot manifest {current}"));
    }
    let bytes = crate::download_local_object_from_remote(paths, remote, &snapshot.manifest_key)
        .with_context(|| {
            format!(
                "download current snapshot manifest {}",
                snapshot.manifest_key
            )
        })?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse current snapshot manifest {}", snapshot.manifest_key))
}

fn validate_quick_gc_records(
    remote: &RemoteStore,
    host_id: &str,
    current: Option<&String>,
    head: Option<&RemoteHeadExport>,
    missing: &mut usize,
) -> Result<()> {
    let compact_head_authoritative = matches!(remote, RemoteStore::S3(_)) && head.is_some();
    let mark_key = remote_gc_mark_key(host_id);
    let Some(bytes) = remote.get_optional(&mark_key)? else {
        if !compact_head_authoritative {
            *missing += 1;
            eprintln!("missing remote gc mark {mark_key}");
        }
        return Ok(());
    };
    let mark: GcMarkExport = match serde_json::from_slice(&bytes) {
        Ok(mark) => mark,
        Err(err) => {
            *missing += 1;
            eprintln!("invalid remote gc mark {mark_key}: {err}");
            return Ok(());
        }
    };
    if mark.version != 1 {
        *missing += 1;
        eprintln!("unsupported remote gc mark version {mark_key}");
    }
    if mark.host_id != host_id {
        *missing += 1;
        eprintln!(
            "remote gc mark host id {} does not match {}",
            mark.host_id, host_id
        );
    }
    if mark.current_snapshot.as_ref() != current && !compact_head_authoritative {
        *missing += 1;
        eprintln!("remote gc mark current snapshot does not match metadata {mark_key}");
    }
    if mark.has_duplicate_object_keys() {
        *missing += 1;
        eprintln!("remote gc mark contains duplicate object keys {mark_key}");
    }
    for key in remote.list(&remote_gc_tombstone_prefix(host_id))? {
        if !key.ends_with(".json") {
            continue;
        }
        let tombstone: GcTombstoneExport = match serde_json::from_slice(&remote.get(&key)?) {
            Ok(tombstone) => tombstone,
            Err(err) => {
                *missing += 1;
                eprintln!("invalid remote gc tombstone {key}: {err}");
                continue;
            }
        };
        for issue in tombstone.validation_issues(host_id) {
            *missing += 1;
            eprintln!("remote gc tombstone issue {key}: {issue:?}");
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RemoteObjectScanOptions {
    parallelism: usize,
    sample: Option<usize>,
    timeout: Option<Duration>,
}

struct RemoteObjectScan {
    checked: usize,
    total: usize,
    missing: Vec<String>,
    timed_out: bool,
    elapsed: Duration,
    resumed: bool,
    session_path: Option<PathBuf>,
    session_fingerprint: Option<String>,
}

fn remote_fsck_objects(
    paths: &Paths,
    remote: &RemoteStore,
    options: RemoteObjectScanOptions,
) -> Result<()> {
    let keys = referenced_local_object_keys(paths)?;
    let scan = scan_remote_object_availability(remote, keys, options)?;
    for key in &scan.missing {
        eprintln!("missing remote object {key}");
    }
    if scan.timed_out {
        bail!(
            "remote fsck objects timed out after checking {}/{} object(s); missing={}",
            scan.checked,
            scan.total,
            scan.missing.len()
        );
    }
    if !scan.missing.is_empty() {
        bail!(
            "remote fsck objects found {} missing object(s)",
            scan.missing.len()
        );
    }
    println!("remote fsck objects ok");
    println!("mode objects");
    println!("checked_objects {}", scan.checked);
    println!("total_objects {}", scan.total);
    println!("missing_objects 0");
    println!("elapsed_secs {}", scan.elapsed.as_secs());
    println!("hint use `mj remote fsck` for quick metadata health verification");
    println!("hint use `mj remote fsck --deep` for payload decode/hash verification");
    Ok(())
}

fn referenced_local_object_keys(paths: &Paths) -> Result<Vec<String>> {
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let export = export_metadata(paths, &conn, &config)?;
    local_object_keys(paths, &export)
}

fn scan_remote_object_availability(
    remote: &RemoteStore,
    mut keys: Vec<String>,
    options: RemoteObjectScanOptions,
) -> Result<RemoteObjectScan> {
    if !remote.exists(REMOTE_HOST_INDEX_KEY)? && !remote.exists(LEGACY_METADATA_EXPORT_KEY)? {
        bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found");
    }
    if let Some(sample) = options.sample {
        keys.truncate(sample);
    }
    let total = keys.len();
    let start = Instant::now();
    let deadline = options.timeout.map(|timeout| start + timeout);
    let parallelism = options.parallelism.clamp(1, 128).min(total.max(1));
    let (tx, rx) = mpsc::channel();
    std::thread::scope(|scope| {
        for worker in 0..parallelism {
            let tx = tx.clone();
            let keys = &keys;
            scope.spawn(move || {
                let mut checked = 0usize;
                let mut missing = Vec::new();
                let mut timed_out = false;
                for (idx, key) in keys.iter().enumerate().skip(worker).step_by(parallelism) {
                    if let Some(deadline) = deadline
                        && Instant::now() >= deadline
                    {
                        timed_out = true;
                        break;
                    }
                    match remote_object_available(remote, key) {
                        Ok(true) => {}
                        Ok(false) => missing.push(key.clone()),
                        Err(err) => {
                            let _ = tx.send(Err(err.context(format!("check remote object {key}"))));
                            return;
                        }
                    }
                    checked += 1;
                    if checked.is_multiple_of(500) {
                        let _ = tx.send(Ok(RemoteObjectWorkerResult::Progress {
                            checked,
                            worker,
                            last_index: idx + 1,
                        }));
                    }
                }
                let _ = tx.send(Ok(RemoteObjectWorkerResult::Done {
                    checked,
                    missing,
                    timed_out,
                }));
            });
        }
        drop(tx);
        let mut checked = 0usize;
        let mut missing = Vec::new();
        let mut timed_out = false;
        for result in rx {
            match result? {
                RemoteObjectWorkerResult::Progress {
                    checked: worker_checked,
                    worker,
                    last_index,
                } => eprintln!(
                    "remote fsck objects progress worker={} worker_checked={} last_index={} elapsed_secs={}",
                    worker,
                    worker_checked,
                    last_index,
                    start.elapsed().as_secs()
                ),
                RemoteObjectWorkerResult::Done {
                    checked: worker_checked,
                    missing: worker_missing,
                    timed_out: worker_timed_out,
                } => {
                    checked += worker_checked;
                    missing.extend(worker_missing);
                    timed_out |= worker_timed_out;
                }
            }
        }
        missing.sort();
        Ok(RemoteObjectScan {
            checked,
            total,
            missing,
            timed_out,
            elapsed: start.elapsed(),
            resumed: false,
            session_path: None,
            session_fingerprint: None,
        })
    })
}

enum RemoteObjectWorkerResult {
    Progress {
        checked: usize,
        worker: usize,
        last_index: usize,
    },
    Done {
        checked: usize,
        missing: Vec<String>,
        timed_out: bool,
    },
}

fn remote_repair(
    paths: &Paths,
    remote: &RemoteStore,
    options: RemoteObjectScanOptions,
    dry_run: bool,
) -> Result<()> {
    let mut keys = referenced_local_object_keys(paths)?;
    if let Some(sample) = options.sample {
        keys.truncate(sample);
    }
    let scan = scan_remote_object_availability_for_repair(paths, remote, keys, options)?;
    let mut repaired = 0usize;
    let mut missing_local = 0usize;
    let mut still_missing = BTreeSet::new();
    for key in &scan.missing {
        let Some((source, remote_key)) = repair_source_and_remote_key(paths, remote, key) else {
            missing_local += 1;
            still_missing.insert(key.clone());
            eprintln!("cannot repair {key}: local object is missing");
            continue;
        };
        if dry_run {
            println!("repair_candidate {key} -> {remote_key}");
            still_missing.insert(key.clone());
            continue;
        }
        if remote.put_file_if_absent(&remote_key, &source)? {
            repaired += 1;
            println!("repaired {key} -> {remote_key}");
        } else {
            println!("already_present {key} -> {remote_key}");
        }
    }
    if scan.timed_out {
        eprintln!(
            "remote repair scan timed out after checking {}/{} object(s)",
            scan.checked, scan.total
        );
    }
    if scan.resumed {
        println!("resumed true");
    }
    if let Some(session_path) = &scan.session_path {
        println!("session {}", session_path.display());
        if scan.timed_out || missing_local > 0 || (dry_run && !scan.missing.is_empty()) {
            save_repair_session(
                session_path,
                &RepairSession {
                    version: 1,
                    key_fingerprint: scan
                        .session_fingerprint
                        .clone()
                        .unwrap_or_else(|| remote.describe()),
                    next_index: scan.checked,
                    missing: still_missing.into_iter().collect(),
                    total: scan.total,
                    updated_at: Utc::now().to_rfc3339(),
                },
            )?;
        } else {
            let _ = fs::remove_file(session_path);
        }
    }
    println!("remote repair complete");
    println!("checked_objects {}", scan.checked);
    println!("total_objects {}", scan.total);
    println!("missing_objects {}", scan.missing.len());
    println!("repaired_objects {repaired}");
    println!("missing_local_objects {missing_local}");
    println!("dry_run {dry_run}");
    if missing_local > 0 {
        bail!(
            "remote repair could not repair {missing_local} object(s) because local copies are missing"
        );
    }
    Ok(())
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct RepairSession {
    version: u32,
    key_fingerprint: String,
    next_index: usize,
    missing: Vec<String>,
    total: usize,
    updated_at: String,
}

fn scan_remote_object_availability_for_repair(
    paths: &Paths,
    remote: &RemoteStore,
    keys: Vec<String>,
    options: RemoteObjectScanOptions,
) -> Result<RemoteObjectScan> {
    if !remote.exists(REMOTE_HOST_INDEX_KEY)? && !remote.exists(LEGACY_METADATA_EXPORT_KEY)? {
        bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found");
    }
    let total = keys.len();
    let start = Instant::now();
    let deadline = options.timeout.map(|timeout| start + timeout);
    let parallelism = options.parallelism.clamp(1, 128).min(total.max(1));
    let session_path = repair_session_path(paths);
    let fingerprint = repair_key_fingerprint(&keys, remote, options);
    let mut session = load_repair_session(&session_path)?
        .filter(|session| session.version == 1 && session.key_fingerprint == fingerprint)
        .unwrap_or_else(|| RepairSession {
            version: 1,
            key_fingerprint: fingerprint.clone(),
            next_index: 0,
            missing: Vec::new(),
            total,
            updated_at: Utc::now().to_rfc3339(),
        });
    let resumed = session.next_index > 0 || !session.missing.is_empty();
    session.total = total;
    session.next_index = session.next_index.min(total);
    let mut missing = session.missing.iter().cloned().collect::<BTreeSet<_>>();
    let mut cursor = session.next_index;
    let mut checked_this_run = 0usize;
    let mut timed_out = false;

    while cursor < total {
        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            timed_out = true;
            break;
        }
        let end = cursor.saturating_add(parallelism).min(total);
        let batch = &keys[cursor..end];
        let (tx, rx) = mpsc::channel();
        std::thread::scope(|scope| {
            for key in batch {
                let tx = tx.clone();
                scope.spawn(move || {
                    let result = remote_object_available(remote, key)
                        .with_context(|| format!("check remote object {key}"));
                    let _ = tx.send((key.clone(), result));
                });
            }
        });
        drop(tx);
        for (key, available) in rx {
            if !available? {
                missing.insert(key);
            }
        }
        cursor = end;
        checked_this_run += batch.len();
        if checked_this_run.is_multiple_of(512) || cursor == total {
            session.next_index = cursor;
            session.missing = missing.iter().cloned().collect();
            session.updated_at = Utc::now().to_rfc3339();
            save_repair_session(&session_path, &session)?;
        }
    }

    session.next_index = cursor;
    session.missing = missing.iter().cloned().collect();
    session.updated_at = Utc::now().to_rfc3339();
    save_repair_session(&session_path, &session)?;

    Ok(RemoteObjectScan {
        checked: cursor,
        total,
        missing: missing.into_iter().collect(),
        timed_out,
        elapsed: start.elapsed(),
        resumed,
        session_path: Some(session_path),
        session_fingerprint: Some(fingerprint),
    })
}

fn repair_session_path(paths: &Paths) -> PathBuf {
    paths.home.join("cache/remote-repair-session.json")
}

fn load_repair_session(path: &PathBuf) -> Result<Option<RepairSession>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn save_repair_session(path: &PathBuf, session: &RepairSession) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(session)?;
    fs::write(path, bytes)?;
    Ok(())
}

fn repair_key_fingerprint(
    keys: &[String],
    remote: &RemoteStore,
    options: RemoteObjectScanOptions,
) -> String {
    let mut input = String::new();
    input.push_str(&remote.describe());
    input.push('\n');
    input.push_str(&format!("sample={:?}\n", options.sample));
    for key in keys {
        input.push_str(key);
        input.push('\n');
    }
    blake3_hex(input.as_bytes())
}

fn repair_source_and_remote_key(
    paths: &Paths,
    remote: &RemoteStore,
    key: &str,
) -> Option<(PathBuf, String)> {
    let source = paths.home.join(key);
    if !source.exists() {
        return None;
    }
    let remote_key = if matches!(remote, RemoteStore::S3(_)) {
        canonical_remote_alias(key).unwrap_or_else(|| key.to_string())
    } else {
        key.to_string()
    };
    Some((source, remote_key))
}

pub(crate) fn read_remote_host_index(remote: &RemoteStore) -> Result<RemoteHostIndex> {
    if let Some(bytes) = remote.get_optional(REMOTE_HOST_INDEX_KEY)? {
        let mut index: RemoteHostIndex = serde_json::from_slice(&bytes)?;
        index.sort_hosts();
        return Ok(index);
    }
    Ok(RemoteHostIndex::empty(Utc::now()))
}

pub(crate) fn remote_host_index_with_legacy(remote: &RemoteStore) -> Result<RemoteHostIndex> {
    let mut index = read_remote_host_index(remote)?;
    if index.hosts.is_empty()
        && let Some(bytes) = remote.get_optional(LEGACY_METADATA_EXPORT_KEY)?
    {
        let export: MetadataExport = serde_json::from_slice(&bytes)?;
        index.hosts.push(RemoteHostSummary {
            id: export.config.host.id.clone(),
            name: export.config.host.name.clone(),
            last_synced_at: export.exported_at,
            current_snapshot: export.refs.get("current").cloned(),
            metadata_key: LEGACY_METADATA_EXPORT_KEY.into(),
        });
    }
    Ok(index)
}

fn print_remote_host_snapshots(export: &MetadataExport) {
    println!("snapshot_id\tcreated_at\tparent\top_id");
    for snapshot in &export.snapshots {
        println!(
            "{}\t{}\t{}\t{}",
            snapshot.id,
            snapshot.created_at,
            snapshot.parent_id.as_deref().unwrap_or("-"),
            snapshot.op_id
        );
    }
}

fn print_remote_host_operations(export: &MetadataExport) {
    println!("op_id\tcreated_at\tkind\tstatus\tbefore\tafter\tmessage");
    for operation in &export.operations {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            operation.id,
            operation.created_at,
            operation.kind,
            operation.status,
            operation.before_snapshot.as_deref().unwrap_or("-"),
            operation.after_snapshot.as_deref().unwrap_or("-"),
            operation.message.as_deref().unwrap_or("")
        );
    }
}
