use crate::majutsu_store::{
    RemoteGcMark as GcMarkExport, RemoteGcTombstone as GcTombstoneExport, RemoteHostIndex,
    RemoteHostSummary, host_current_ref_key, host_last_synced_ref_key, host_metadata_key,
    host_remote_key, remote_gc_mark_key, remote_gc_tombstone_prefix, remote_host_label,
    select_remote_host,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;

use crate::cli::RemoteCommand;
use crate::config::{MetadataExport, Paths, read_config};
use crate::fsck_runtime::{RemoteFsckOptions, remote_fsck_with_options};
use crate::majutsu_core::{
    LargeManifest, SnapshotExport, SnapshotManifest, TreeManifest, TreeNodeManifest,
    payload_blob_ref, payload_large_ref,
};
use crate::majutsu_store::canonical_remote_alias;
use crate::object_paths::{
    canonical_alias_for_legacy_key, local_object_keys, local_object_keys_for_snapshot,
};
use crate::operation_log::record_op;
use crate::remote_store::{RemoteStore, open_remote_with_upload_policy, remote_config_diagnostics};
use crate::snapshot_state::current_snapshot;
use crate::util::{
    REMOTE_HEAD_DECODE_LIMIT, REMOTE_METADATA_DECODE_LIMIT, blake3_hex, parse_db_time,
    zstd_decode_all_limited,
};
use crate::{
    decode_object, ensure_ready, export_metadata, open_db, read_object,
    remote_object_available_for_paths, remote_ref,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{Duration, Instant};

const DEFAULT_REMOTE_FSCK_QUICK_TIMEOUT_SECS: u64 = 60;

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
    format!("{host_id}/head.cbor.zst.enc")
}

fn read_remote_head(
    paths: &Paths,
    remote: &RemoteStore,
    host_prefix: &str,
) -> Result<Option<RemoteHeadExport>> {
    let Some(bytes) = remote.get_optional(&remote_head_key(host_prefix))? else {
        return Ok(None);
    };
    let decoded = decode_object(paths, &bytes)?;
    let decompressed =
        zstd_decode_all_limited(decoded.as_slice(), REMOTE_HEAD_DECODE_LIMIT, "remote head")?;
    Ok(Some(serde_cbor::from_slice(&decompressed)?))
}

pub(crate) fn remote_cmd(paths: &Paths, command: RemoteCommand) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let remote_config = config
        .remote
        .as_ref()
        .ok_or_else(|| anyhow!("remote is not configured; run `mj init --remote ...`"))?;
    if matches!(command, RemoteCommand::Check) {
        for (name, value) in remote_config_diagnostics(remote_config)? {
            println!("{name} {value}");
        }
    }
    let remote = open_remote_with_upload_policy(
        remote_config,
        config.large.multipart,
        config.large.max_parallel_uploads,
    )?;
    match command {
        RemoteCommand::Init { force } => {
            let existing_objects = remote.list("")?;
            if !force && !existing_objects.is_empty() {
                bail!(
                    "remote root is not empty; rerun with --force only if this path is dedicated to majutsu"
                );
            }
            println!("remote {}", remote.describe());
            println!("initialized remote root");
            println!("hosts 0");
        }
        RemoteCommand::Check => {
            println!("remote {}", remote.describe());
            println!("objects not-scanned");
            println!("objects_exact false");
            let index = read_remote_host_index(&remote)?;
            println!("hosts {}", index.hosts.len());
            let Some(host) = index
                .hosts
                .iter()
                .find(|host| host.id == config.host.id)
                .or_else(|| index.hosts.first())
            else {
                bail!(
                    "remote metadata is missing: <host-prefix>/metadata/export.json.zst not found"
                );
            };
            println!("metadata ok");
            println!("host_id {}", host.id);
            println!("host_name {}", host.name);
            println!("metadata_key {}", host.metadata_key);
            let first = remote.get_range(&host.metadata_key, 0, 1)?;
            println!("range_get {}", first.len());
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
                remote_fsck_quick(
                    paths,
                    &remote,
                    timeout_secs.map(Duration::from_secs).unwrap_or_else(|| {
                        Duration::from_secs(DEFAULT_REMOTE_FSCK_QUICK_TIMEOUT_SECS)
                    }),
                )?;
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
            canonical_aliases_only,
            history,
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
                canonical_aliases_only,
                !history,
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
        RemoteCommand::Explain { key } => {
            remote_explain_object(paths, &key)?;
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
            let index = read_remote_host_index(&remote)?;
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
        RemoteCommand::MigrateLegacy { host, dry_run } => {
            migrate_legacy_remote(paths, &remote, host.as_deref(), dry_run)?;
        }
        RemoteCommand::Host {
            id,
            snapshots,
            operations,
        } => {
            let index = read_remote_host_index(&remote)?;
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

pub(crate) fn repair_missing_referenced_objects(
    paths: &Paths,
    remote: &RemoteStore,
) -> Result<RepairSummary> {
    remote_repair_summary(
        paths,
        remote,
        RemoteObjectScanOptions {
            parallelism: 16,
            sample: None,
            timeout: Some(Duration::from_secs(120)),
        },
        false,
        false,
        true,
    )
}

fn remote_fsck_quick(paths: &Paths, remote: &RemoteStore, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    let mut missing = 0usize;
    let mut checked_metadata = 0usize;
    let mut context = QuickFsckContext {
        start,
        timeout,
        checked_metadata,
    };
    eprintln!(
        "remote fsck progress phase=start timeout_secs={}",
        timeout.as_secs()
    );
    context.ensure("host-index")?;
    eprintln!("remote fsck progress phase=host-index");
    let index = read_remote_host_index(remote)?;
    if index.hosts.is_empty() {
        bail!("remote metadata is missing: <host-prefix>/metadata/export.json.zst not found");
    }
    eprintln!(
        "remote fsck progress phase=host-index-done hosts={} elapsed_secs={}",
        index.hosts.len(),
        start.elapsed().as_secs()
    );
    for issue in index.duplicate_issues() {
        missing += 1;
        eprintln!("remote host metadata issue: {issue:?}");
    }
    for (host_index, host) in index.hosts.iter().enumerate() {
        context.checked_metadata = checked_metadata;
        context.ensure("host")?;
        eprintln!(
            "remote fsck progress phase=host index={}/{} host={}",
            host_index + 1,
            index.hosts.len(),
            host.name
        );
        let host_prefix = remote_host_prefix(host);
        let expected_metadata_key = host_metadata_key(&host_prefix);
        let expected_compressed_metadata_key = compressed_metadata_key(&expected_metadata_key);
        if host.metadata_key != expected_metadata_key
            && host.metadata_key != expected_compressed_metadata_key
        {
            missing += 1;
            eprintln!(
                "remote host metadata_key {} does not match canonical key {}",
                host.metadata_key, expected_metadata_key
            );
        }
        let Some(bytes) = remote.get_optional(&host.metadata_key)? else {
            missing += 1;
            eprintln!("missing host metadata {} {}", host.id, host.metadata_key);
            checked_metadata += 1;
            continue;
        };
        let metadata_bytes = match decode_remote_metadata_bytes(&host.metadata_key, &bytes) {
            Ok(bytes) => bytes,
            Err(err) => {
                missing += 1;
                eprintln!("invalid host metadata {}: {err}", host.metadata_key);
                checked_metadata += 1;
                continue;
            }
        };
        let export: MetadataExport = match serde_json::from_slice(&metadata_bytes) {
            Ok(export) => export,
            Err(err) => {
                missing += 1;
                eprintln!("invalid host metadata {}: {err}", host.metadata_key);
                checked_metadata += 1;
                continue;
            }
        };
        validate_quick_host_metadata(paths, remote, host, &export, context, &mut missing)?;
        checked_metadata += 1;
        eprintln!(
            "remote fsck progress phase=host-done index={}/{} host={} elapsed_secs={}",
            host_index + 1,
            index.hosts.len(),
            host.name,
            start.elapsed().as_secs()
        );
    }
    context.checked_metadata = checked_metadata;
    context.ensure("done")?;
    if missing > 0 {
        bail!("remote fsck quick found {missing} issue(s)");
    }
    eprintln!(
        "remote fsck progress phase=done hosts={} elapsed_secs={}",
        checked_metadata,
        start.elapsed().as_secs()
    );
    println!("remote fsck quick ok");
    println!("mode metadata");
    println!("checked_metadata {checked_metadata}");
    println!("elapsed_secs {}", start.elapsed().as_secs());
    println!("hint use `mj remote fsck --objects` to verify every referenced object");
    println!("hint use `mj remote fsck --deep` for payload decode/hash verification");
    Ok(())
}

#[derive(Clone, Copy)]
struct QuickFsckContext {
    start: Instant,
    timeout: Duration,
    checked_metadata: usize,
}

impl QuickFsckContext {
    fn ensure(self, phase: &str) -> Result<()> {
        if self.start.elapsed() < self.timeout {
            return Ok(());
        }
        bail!(
            "remote fsck quick timed out after {} second(s) phase={} checked_metadata={}",
            self.timeout.as_secs(),
            phase,
            self.checked_metadata
        )
    }
}

fn compressed_metadata_key(key: &str) -> String {
    format!("{key}.zst")
}

fn decode_remote_metadata_bytes(key: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    if key.ends_with(".zst") {
        return zstd_decode_all_limited(
            bytes,
            REMOTE_METADATA_DECODE_LIMIT,
            &format!("compressed metadata {key}"),
        );
    }
    Ok(bytes.to_vec())
}

fn validate_quick_host_metadata(
    paths: &Paths,
    remote: &RemoteStore,
    host: &RemoteHostSummary,
    export: &MetadataExport,
    context: QuickFsckContext,
    missing: &mut usize,
) -> Result<()> {
    context.ensure("host-metadata")?;
    if export.config.host.id != host.id {
        *missing += 1;
        eprintln!(
            "remote host id {} does not match metadata host id {}",
            host.id, export.config.host.id
        );
    }
    if export.config.host.name != host.name {
        *missing += 1;
        eprintln!(
            "remote host name {} does not match metadata host name {}",
            host.name, export.config.host.name
        );
    }
    let host_prefix = remote_host_prefix(host);
    let expected_host_prefix = remote_host_label(&export.config.host.name);
    if host_prefix != expected_host_prefix {
        *missing += 1;
        eprintln!(
            "remote host prefix {host_prefix} does not match metadata host name prefix {expected_host_prefix}"
        );
    }
    let head = read_remote_head(paths, remote, &host_prefix)?;
    context.ensure("head")?;
    let compact_head_authoritative = matches!(remote, RemoteStore::S3(_)) && head.is_some();
    let current = export.refs.get("current");
    if host.current_snapshot.as_ref() != current && !compact_head_authoritative {
        *missing += 1;
        eprintln!(
            "remote host current snapshot does not match metadata for {}",
            host.id
        );
    }
    if let Some(head) = head.as_ref() {
        if head.version != 1 {
            *missing += 1;
            eprintln!(
                "unsupported remote head version {}",
                remote_head_key(&host_prefix)
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
                "remote head metadata key does not match host metadata for {}",
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
        let key = host_current_ref_key(&host_prefix);
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
            Ok(value) if host.last_synced_at == value || compact_head_authoritative => {}
            Ok(value) => {
                *missing += 1;
                eprintln!(
                    "remote host last_synced_at {} does not match metadata last-synced {} for {}",
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
        let key = host_last_synced_ref_key(&host_prefix);
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
    validate_quick_gc_records(
        remote,
        &host_prefix,
        &host.id,
        current,
        head.as_ref(),
        context,
        missing,
    )
}

fn remote_host_prefix(host: &RemoteHostSummary) -> String {
    host.metadata_key
        .split_once('/')
        .map(|(prefix, _)| prefix.to_string())
        .unwrap_or_else(|| host.name.clone())
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
    expected_mark_host_id: &str,
    current: Option<&String>,
    head: Option<&RemoteHeadExport>,
    context: QuickFsckContext,
    missing: &mut usize,
) -> Result<()> {
    let compact_head_authoritative = matches!(remote, RemoteStore::S3(_)) && head.is_some();
    context.ensure("gc-mark")?;
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
    if mark.host_id != expected_mark_host_id {
        *missing += 1;
        eprintln!(
            "remote gc mark host id {} does not match {}",
            mark.host_id, expected_mark_host_id
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
    if compact_head_authoritative {
        eprintln!(
            "remote fsck progress phase=gc host={} tombstones=skipped_compact_head",
            host_id
        );
        return Ok(());
    }
    context.ensure("gc-tombstones")?;
    for key in remote.list(&remote_gc_tombstone_prefix(host_id))? {
        context.ensure("gc-tombstones")?;
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
    let keys = referenced_local_object_keys(paths, false)?;
    let scan = scan_remote_object_availability(paths, remote, keys, options)?;
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

fn referenced_local_object_keys(paths: &Paths, current_only: bool) -> Result<Vec<String>> {
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let export = export_metadata(paths, &conn, &config)?;
    if current_only && let Some(current) = export.refs.get("current") {
        return local_object_keys_for_snapshot(paths, &export, current);
    }
    local_object_keys(paths, &export)
}

fn scan_remote_object_availability(
    paths: &Paths,
    remote: &RemoteStore,
    mut keys: Vec<String>,
    options: RemoteObjectScanOptions,
) -> Result<RemoteObjectScan> {
    if read_remote_host_index(remote)?.hosts.is_empty() {
        bail!("remote metadata is missing: <host-prefix>/metadata/export.json.zst not found");
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
                    match remote_object_available_for_paths(paths, remote, key) {
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
    canonical_aliases_only: bool,
    current_only: bool,
) -> Result<()> {
    let summary = if canonical_aliases_only {
        remote_repair_canonical_aliases(paths, remote, dry_run, true)?
    } else {
        remote_repair_summary(paths, remote, options, dry_run, true, current_only)?
    };
    if summary.missing_local > 0 {
        bail!(
            "remote repair could not repair {} object(s) because local copies are missing",
            summary.missing_local
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RepairSummary {
    pub(crate) checked: usize,
    pub(crate) total: usize,
    pub(crate) missing: usize,
    pub(crate) repaired: usize,
    pub(crate) missing_local: usize,
}

fn remote_repair_summary(
    paths: &Paths,
    remote: &RemoteStore,
    options: RemoteObjectScanOptions,
    dry_run: bool,
    verbose: bool,
    current_only: bool,
) -> Result<RepairSummary> {
    let mut keys = referenced_local_object_keys(paths, current_only)?;
    if let Some(sample) = options.sample {
        keys.truncate(sample);
    }
    let scan = scan_remote_object_availability_for_repair(paths, remote, keys, options)?;
    let mut repaired = 0usize;
    let mut missing_local = 0usize;
    let mut still_missing = BTreeSet::new();
    let config = read_config(paths)?;
    let host_label = remote_host_label(&config.host.name);
    for key in &scan.missing {
        let Some((source, remote_key)) =
            repair_source_and_remote_key(paths, remote, &host_label, key)
        else {
            missing_local += 1;
            still_missing.insert(key.clone());
            eprintln!("cannot repair {key}: local object is missing");
            continue;
        };
        if dry_run {
            if verbose {
                println!("repair_candidate {key} -> {remote_key}");
            }
            still_missing.insert(key.clone());
            continue;
        }
        if remote.put_file_if_absent(&remote_key, &source)? {
            repaired += 1;
            if verbose {
                println!("repaired {key} -> {remote_key}");
            }
        } else if verbose {
            println!("already_present {key} -> {remote_key}");
        }
    }
    if verbose && scan.timed_out {
        eprintln!(
            "remote repair scan timed out after checking {}/{} object(s)",
            scan.checked, scan.total
        );
    }
    if verbose && scan.resumed {
        println!("resumed true");
    }
    if let Some(session_path) = &scan.session_path {
        if verbose {
            println!("session {}", session_path.display());
        }
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
    if verbose {
        println!("remote repair complete");
        println!(
            "object_scope {}",
            if current_only { "current" } else { "history" }
        );
        println!("checked_objects {}", scan.checked);
        println!("total_objects {}", scan.total);
        println!("missing_objects {}", scan.missing.len());
        println!("repaired_objects {repaired}");
        println!("missing_local_objects {missing_local}");
        println!("dry_run {dry_run}");
    }
    Ok(RepairSummary {
        checked: scan.checked,
        total: scan.total,
        missing: scan.missing.len(),
        repaired,
        missing_local,
    })
}

fn remote_repair_canonical_aliases(
    paths: &Paths,
    remote: &RemoteStore,
    dry_run: bool,
    verbose: bool,
) -> Result<RepairSummary> {
    if read_remote_host_index(remote)?.hosts.is_empty() {
        bail!("remote metadata is missing: <host-prefix>/metadata/export.json.zst not found");
    }

    let mut skipped_local_missing = 0usize;
    let alias_candidates = referenced_local_object_keys(paths, false)?
        .into_iter()
        .filter_map(|key| canonical_alias_for_legacy_key(&key).map(|alias| (key, alias)))
        .filter(|(key, _)| {
            let exists = paths.home.join(key).exists();
            if !exists {
                skipped_local_missing += 1;
            }
            exists
        })
        .collect::<Vec<_>>();
    let config = read_config(paths)?;
    let host_label = remote_host_label(&config.host.name);
    let remote_keys =
        list_remote_alias_candidate_keys(remote, &host_label, &alias_candidates, verbose)
            .context("list remote keys for canonical alias repair")?;
    let mut checked = 0usize;
    let mut missing = 0usize;
    let mut repaired = 0usize;
    let missing_local = 0usize;

    for (key, alias) in alias_candidates {
        checked += 1;
        let remote_alias = if matches!(remote, RemoteStore::S3(_)) {
            host_remote_key(&host_label, &alias)
        } else {
            alias.clone()
        };
        if remote_keys.contains(&remote_alias) {
            continue;
        }
        missing += 1;
        let source = paths.home.join(&key);
        if dry_run {
            if verbose {
                println!("repair_candidate {key} -> {remote_alias}");
            }
            continue;
        }
        if remote.put_file_if_absent(&remote_alias, &source)? {
            repaired += 1;
            if verbose {
                println!("repaired {key} -> {remote_alias}");
            }
        } else if verbose {
            println!("already_present {key} -> {remote_alias}");
        }
    }

    if verbose {
        println!("remote repair complete");
        println!("mode canonical_aliases");
        println!("checked_objects {checked}");
        println!("total_objects {checked}");
        println!("missing_objects {missing}");
        println!("repaired_objects {repaired}");
        println!("missing_local_objects {missing_local}");
        println!("skipped_local_missing_objects {skipped_local_missing}");
        println!("dry_run {dry_run}");
    }

    Ok(RepairSummary {
        checked,
        total: checked,
        missing,
        repaired,
        missing_local,
    })
}

fn list_remote_alias_candidate_keys(
    remote: &RemoteStore,
    host_label: &str,
    alias_candidates: &[(String, String)],
    verbose: bool,
) -> Result<BTreeSet<String>> {
    let prefixes = alias_candidates
        .iter()
        .filter_map(|(_, alias)| canonical_alias_listing_prefix(alias))
        .collect::<BTreeSet<_>>();
    let mut keys = BTreeSet::new();
    if verbose {
        eprintln!(
            "canonical alias repair: listing {} remote prefix(es) for {} alias candidate(s)",
            prefixes.len(),
            alias_candidates.len()
        );
    }
    for prefix in prefixes {
        let started = Instant::now();
        let list_prefix = if matches!(remote, RemoteStore::S3(_)) {
            host_remote_key(host_label, &prefix)
        } else {
            prefix.clone()
        };
        let listed = remote
            .list(&list_prefix)
            .with_context(|| format!("list remote prefix {list_prefix}"))?;
        let listed_len = listed.len();
        keys.extend(listed);
        if verbose {
            eprintln!(
                "canonical alias repair: listed prefix {prefix} objects={} elapsed_secs={}",
                listed_len,
                started.elapsed().as_secs()
            );
        }
    }
    Ok(keys)
}

fn canonical_alias_listing_prefix(alias: &str) -> Option<String> {
    for prefix in [
        "trees/",
        "blobs/loose/",
        "packs/small/",
        "packs/normal/",
        "indexes/pack-index/",
        "large/manifests/",
        "large/chunks/fixed-8m/",
        "large/chunks/fastcdc/",
    ] {
        if alias.starts_with(prefix) {
            return Some(prefix.to_string());
        }
    }
    None
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
    if read_remote_host_index(remote)?.hosts.is_empty() {
        bail!("remote metadata is missing: <host-prefix>/metadata/export.json.zst not found");
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
                    let result = crate::remote_object_available_for_paths(paths, remote, key)
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
    host_label: &str,
    key: &str,
) -> Option<(PathBuf, String)> {
    let source = paths.home.join(key);
    if !source.exists() {
        return None;
    }
    let remote_key = repair_remote_key(matches!(remote, RemoteStore::S3(_)), host_label, key);
    Some((source, remote_key))
}

fn repair_remote_key(s3_remote: bool, host_label: &str, key: &str) -> String {
    if s3_remote {
        host_remote_key(
            host_label,
            &canonical_remote_alias(key).unwrap_or_else(|| key.to_string()),
        )
    } else {
        key.to_string()
    }
}

fn remote_explain_object(paths: &Paths, key: &str) -> Result<()> {
    let conn = open_db(paths)?;
    let config = read_config(paths)?;
    let export = export_metadata(paths, &conn, &config)?;
    let mut hits = Vec::<String>::new();
    let local_path = paths.home.join(key);
    println!("object {key}");
    println!("local_present {}", local_path.exists());

    let mut skipped = 0usize;
    for snapshot in &export.snapshots {
        if snapshot.manifest_key == key {
            hits.push(format!("snapshot {} manifest", snapshot.id));
        }
        match snapshot_manifest_for_explain(paths, snapshot) {
            Ok(manifest) => explain_snapshot_manifest(paths, &manifest, key, &mut hits),
            Err(err) => {
                skipped += 1;
                eprintln!("warning: skipped snapshot {}: {err:#}", snapshot.id);
            }
        }
    }
    for large in &export.large_objects {
        if large.manifest_key == key {
            hits.push(format!(
                "large_object oid={} manifest {}",
                large.oid, large.manifest_key
            ));
        }
        if let Ok(manifest) = read_large_manifest_for_explain(paths, &large.manifest_key) {
            for chunk in manifest.chunks {
                if chunk.object_key == key {
                    hits.push(format!(
                        "large_object oid={} chunk offset={} size={}",
                        large.oid, chunk.offset, chunk.len
                    ));
                }
            }
        }
    }
    for blob in &export.blobs {
        if blob.object_key == key {
            hits.push(format!(
                "blob oid={} size={} pack={}",
                blob.oid,
                blob.size,
                blob.pack_id.as_deref().unwrap_or("(none)")
            ));
        }
    }
    for pack in &export.packs {
        if pack.pack_key == key {
            hits.push(format!("pack {} object", pack.pack_id));
        }
        if pack.index_key == key {
            hits.push(format!("pack {} index", pack.pack_id));
        }
    }

    println!("references {}", hits.len());
    println!("skipped_snapshots {skipped}");
    for hit in hits {
        println!("ref {hit}");
    }
    Ok(())
}

fn snapshot_manifest_for_explain(
    paths: &Paths,
    snapshot: &SnapshotExport,
) -> Result<SnapshotManifest> {
    if !snapshot.manifest_json.trim().is_empty() {
        return serde_json::from_str(&snapshot.manifest_json)
            .with_context(|| format!("parse snapshot manifest {}", snapshot.id));
    }
    let bytes = read_object(paths, &snapshot.manifest_key)
        .with_context(|| format!("read snapshot manifest {}", snapshot.manifest_key))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse snapshot manifest {}", snapshot.manifest_key))
}

fn explain_snapshot_manifest(
    paths: &Paths,
    manifest: &SnapshotManifest,
    key: &str,
    hits: &mut Vec<String>,
) {
    for (root_id, root_tree) in &manifest.root_trees {
        if root_tree.tree_key == key {
            hits.push(format!(
                "snapshot {} root {root_id} root_tree tree_id={} files={}",
                manifest.snapshot_id, root_tree.tree_id, root_tree.file_count
            ));
        }
        if let Ok(tree) = read_tree_manifest_for_explain(paths, &root_tree.tree_key) {
            explain_tree_manifest(paths, &manifest.snapshot_id, root_id, &tree, key, hits);
        }
    }
}

fn explain_tree_manifest(
    paths: &Paths,
    snapshot_id: &str,
    root_id: &str,
    tree: &TreeManifest,
    key: &str,
    hits: &mut Vec<String>,
) {
    if tree
        .root_node
        .as_ref()
        .is_some_and(|node| node.node_key == key)
    {
        hits.push(format!(
            "snapshot {snapshot_id} root {root_id} tree {} root_node",
            tree.tree_id
        ));
    }
    for (name, node) in &tree.subtree_nodes {
        if node.node_key == key {
            hits.push(format!(
                "snapshot {snapshot_id} root {root_id} tree {} subtree {name}",
                tree.tree_id
            ));
        }
    }
    for record in tree.entries.values() {
        explain_file_record(snapshot_id, root_id, &record.path, record, key, hits);
    }
    if let Some(root_node) = &tree.root_node {
        explain_tree_node(
            paths,
            snapshot_id,
            root_id,
            "",
            &root_node.node_key,
            key,
            hits,
        );
    }
    for (name, node) in &tree.subtree_nodes {
        explain_tree_node(paths, snapshot_id, root_id, name, &node.node_key, key, hits);
    }
}

fn explain_tree_node(
    paths: &Paths,
    snapshot_id: &str,
    root_id: &str,
    prefix: &str,
    node_key: &str,
    key: &str,
    hits: &mut Vec<String>,
) {
    let Ok(node) = read_tree_node_for_explain(paths, node_key) else {
        return;
    };
    for (name, child) in &node.child_nodes {
        let child_path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };
        if child.node_key == key {
            hits.push(format!(
                "snapshot {snapshot_id} root {root_id} node {node_key} child {child_path}"
            ));
        }
        explain_tree_node(
            paths,
            snapshot_id,
            root_id,
            &child_path,
            &child.node_key,
            key,
            hits,
        );
    }
    for record in node.entries.values() {
        explain_file_record(snapshot_id, root_id, &record.path, record, key, hits);
    }
}

fn explain_file_record(
    snapshot_id: &str,
    root_id: &str,
    path: &str,
    record: &crate::majutsu_core::FileRecord,
    key: &str,
    hits: &mut Vec<String>,
) {
    if let Some((oid, object_key)) = payload_blob_ref(&record.payload)
        && object_key == key
    {
        hits.push(format!(
            "snapshot {snapshot_id} root {root_id} file {path} blob {oid}"
        ));
    }
    if let Some((oid, manifest_key, _)) = payload_large_ref(&record.payload)
        && manifest_key == key
    {
        hits.push(format!(
            "snapshot {snapshot_id} root {root_id} file {path} large_manifest {oid}"
        ));
    }
}

fn read_tree_manifest_for_explain(paths: &Paths, key: &str) -> Result<TreeManifest> {
    let bytes = read_object(paths, key).with_context(|| format!("read tree manifest {key}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse tree manifest {key}"))
}

fn read_tree_node_for_explain(paths: &Paths, key: &str) -> Result<TreeNodeManifest> {
    let bytes = read_object(paths, key).with_context(|| format!("read tree node {key}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse tree node {key}"))
}

fn read_large_manifest_for_explain(paths: &Paths, key: &str) -> Result<LargeManifest> {
    let bytes = read_object(paths, key).with_context(|| format!("read large manifest {key}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse large manifest {key}"))
}

pub(crate) fn read_remote_host_index(remote: &RemoteStore) -> Result<RemoteHostIndex> {
    let mut index = RemoteHostIndex::empty(Utc::now());
    let host_prefixes = remote.list_common_prefixes("")?;
    for host_prefix in host_prefixes {
        let host_id = host_prefix.trim_end_matches('/');
        if host_id.is_empty() {
            continue;
        }
        let compressed_key = format!("{host_id}/metadata/export.json.zst");
        let plain_key = format!("{host_id}/metadata/export.json");
        let (metadata_key, bytes) = match remote.get_optional(&compressed_key)? {
            Some(bytes) => (compressed_key, bytes),
            None => match remote.get_optional(&plain_key)? {
                Some(bytes) => (plain_key, bytes),
                None => continue,
            },
        };
        if metadata_key.is_empty() {
            continue;
        }
        let metadata_bytes = decode_remote_metadata_bytes(&metadata_key, &bytes)?;
        let export: MetadataExport = serde_json::from_slice(&metadata_bytes)
            .with_context(|| format!("parse remote host metadata {metadata_key}"))?;
        let last_synced_at = export
            .refs
            .get("last-synced")
            .and_then(|value| parse_db_time(value).ok())
            .unwrap_or(export.exported_at);
        index.hosts.push(RemoteHostSummary {
            id: export.config.host.id.clone(),
            name: export.config.host.name.clone(),
            last_synced_at,
            current_snapshot: export.refs.get("current").cloned(),
            metadata_key,
        });
    }
    index.sort_hosts();
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

#[derive(Debug)]
struct LegacyRemoteHost {
    id: String,
    name: String,
    metadata_key: String,
    source_prefix: Option<String>,
    export: MetadataExport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LegacyMigrationItem {
    source: String,
    logical_key: Option<String>,
}

fn migrate_legacy_remote(
    paths: &Paths,
    remote: &RemoteStore,
    selector: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    let candidates = discover_legacy_remote_hosts(remote)?;
    let host = select_legacy_remote_host(candidates, selector)?;
    let target_prefix = remote_host_label(&host.name);
    let metadata_json = crate::sync_runtime::metadata_export_json_for_remote(remote, &host.export)?;
    let metadata_destination =
        legacy_metadata_destination(matches!(remote, RemoteStore::S3(_)), &target_prefix);
    let metadata_bytes = if matches!(remote, RemoteStore::S3(_)) {
        zstd::stream::encode_all(metadata_json.as_slice(), 3)?
    } else {
        metadata_json
    };

    let mut items = BTreeMap::<String, LegacyMigrationItem>::new();
    add_legacy_migration_item(
        &mut items,
        host.metadata_key.clone(),
        metadata_destination.clone(),
        None,
    )?;

    // 旧 host prefix にある timeline/ref/config は、同じ相対パスで新 prefix へ移す。
    if let Some(source_prefix) = &host.source_prefix {
        for source in remote.list(source_prefix)? {
            if source == host.metadata_key {
                continue;
            }
            let Some(relative) = source.strip_prefix(source_prefix) else {
                continue;
            };
            if relative.is_empty()
                || relative == "metadata/export.json"
                || relative == "metadata/export.json.zst"
            {
                continue;
            }
            let destination = host_remote_key(&target_prefix, relative);
            add_legacy_migration_item(&mut items, source, destination, None)?;
        }
    }

    let content_keys = legacy_remote_content_keys(paths, remote, &host)?;
    for key in content_keys {
        let (source, destination_relative) = resolve_legacy_content_source(remote, &host, &key)?;
        let destination = if matches!(remote, RemoteStore::S3(_)) {
            host_remote_key(&target_prefix, &destination_relative)
        } else {
            destination_relative
        };
        add_legacy_migration_item(&mut items, source, destination, Some(key))?;
    }

    let snapshot_manifest_keys = host
        .export
        .snapshots
        .iter()
        .map(|snapshot| snapshot.manifest_key.as_str())
        .collect::<BTreeSet<_>>();
    println!("remote {}", remote.describe());
    println!("legacy_host_id {}", host.id);
    println!("legacy_host_name {}", host.name);
    println!("legacy_metadata {}", host.metadata_key);
    println!("target_prefix {target_prefix}");
    println!("migration_items {}", items.len());
    println!("dry_run {dry_run}");

    let mut copied = 0usize;
    let mut existing = 0usize;
    for (destination, item) in items {
        let source = item.source;
        let mut bytes = if source == host.metadata_key {
            metadata_bytes.clone()
        } else {
            remote
                .get_optional(&source)?
                .ok_or_else(|| anyhow!("legacy migration source is missing: {source}"))?
        };
        if matches!(remote, RemoteStore::S3(_))
            && let Some(logical_key) = item.logical_key.as_deref()
            && (snapshot_manifest_keys.contains(logical_key)
                || legacy_structured_alias(logical_key))
        {
            bytes = encode_legacy_canonical_content(
                paths,
                logical_key,
                &source,
                &bytes,
                snapshot_manifest_keys.contains(logical_key),
            )?;
        }
        if dry_run {
            println!("migration_item {source} -> {destination}");
            continue;
        }
        if destination == source {
            existing += 1;
            continue;
        }
        if remote.exists(&destination)? {
            let current = remote.get(&destination)?;
            if current != bytes {
                bail!(
                    "legacy migration destination differs from source: {destination}; remove it or use a separate remote prefix"
                );
            }
            existing += 1;
            continue;
        }
        remote.put(&destination, &bytes)?;
        copied += 1;
        println!("migrated {source} -> {destination}");
    }
    if !dry_run {
        println!("copied {copied}");
        println!("already_present {existing}");
        println!(
            "next: run mj remote check, then remove the old {} prefix and old global objects after verification",
            host.source_prefix.as_deref().unwrap_or("metadata")
        );
    }
    Ok(())
}

fn discover_legacy_remote_hosts(remote: &RemoteStore) -> Result<Vec<LegacyRemoteHost>> {
    let mut hosts = Vec::new();
    for key in remote.list("hosts/")? {
        if !key.ends_with("/metadata/export.json") && !key.ends_with("/metadata/export.json.zst") {
            continue;
        }
        let bytes = remote
            .get(&key)
            .with_context(|| format!("read legacy host metadata {key}"))?;
        let decoded = decode_remote_metadata_bytes(&key, &bytes)?;
        let export: MetadataExport = serde_json::from_slice(&decoded)
            .with_context(|| format!("parse legacy host metadata {key}"))?;
        let prefix = key
            .strip_suffix("metadata/export.json.zst")
            .or_else(|| key.strip_suffix("metadata/export.json"))
            .map(str::to_string);
        hosts.push(LegacyRemoteHost {
            id: export.config.host.id.clone(),
            name: export.config.host.name.clone(),
            metadata_key: key,
            source_prefix: prefix,
            export,
        });
    }
    for key in ["metadata/export.json.zst", "metadata/export.json"] {
        let Some(bytes) = remote.get_optional(key)? else {
            continue;
        };
        let decoded = decode_remote_metadata_bytes(key, &bytes)?;
        let export: MetadataExport = serde_json::from_slice(&decoded)
            .with_context(|| format!("parse legacy metadata {key}"))?;
        hosts.push(LegacyRemoteHost {
            id: export.config.host.id.clone(),
            name: export.config.host.name.clone(),
            metadata_key: key.to_string(),
            source_prefix: None,
            export,
        });
        break;
    }
    hosts.sort_by(|a, b| {
        a.id.cmp(&b.id)
            .then_with(|| a.metadata_key.cmp(&b.metadata_key))
    });
    hosts.dedup_by(|a, b| a.metadata_key == b.metadata_key);
    Ok(hosts)
}

fn legacy_metadata_destination(s3: bool, host_prefix: &str) -> String {
    let key = host_metadata_key(host_prefix);
    if s3 { format!("{key}.zst") } else { key }
}

fn select_legacy_remote_host(
    hosts: Vec<LegacyRemoteHost>,
    selector: Option<&str>,
) -> Result<LegacyRemoteHost> {
    if hosts.is_empty() {
        bail!("legacy remote metadata was not found under metadata/ or hosts/");
    }
    if let Some(selector) = selector {
        let matches = hosts
            .into_iter()
            .filter(|host| host.id == selector || host.name == selector)
            .collect::<Vec<_>>();
        return match matches.len() {
            1 => Ok(matches.into_iter().next().expect("one matching host")),
            0 => bail!("legacy remote host not found: {selector}"),
            _ => bail!("legacy remote host selector is ambiguous: {selector}"),
        };
    }
    match hosts.len() {
        1 => Ok(hosts.into_iter().next().expect("one legacy host")),
        _ => bail!("legacy remote contains multiple hosts; rerun with --host <id-or-name>"),
    }
}

fn add_legacy_migration_item(
    items: &mut BTreeMap<String, LegacyMigrationItem>,
    source: String,
    destination: String,
    logical_key: Option<String>,
) -> Result<()> {
    let item = LegacyMigrationItem {
        source: source.clone(),
        logical_key,
    };
    if let Some(previous) = items.insert(destination.clone(), item.clone())
        && (previous.source != source || previous.logical_key != item.logical_key)
    {
        bail!(
            "legacy migration has conflicting sources for destination {destination}: {} and {source}",
            previous.source
        );
    }
    Ok(())
}

fn legacy_remote_content_keys(
    paths: &Paths,
    remote: &RemoteStore,
    host: &LegacyRemoteHost,
) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    let packed_oids = host
        .export
        .blobs
        .iter()
        .filter(|blob| blob.pack_id.is_some())
        .map(|blob| blob.oid.as_str())
        .collect::<BTreeSet<_>>();
    for blob in &host.export.blobs {
        if blob.pack_id.is_none() {
            keys.insert(blob.object_key.clone());
        }
    }
    for pack in &host.export.packs {
        keys.insert(pack.pack_key.clone());
        keys.insert(pack.index_key.clone());
    }
    for large in &host.export.large_objects {
        keys.insert(large.manifest_key.clone());
    }
    for chunk in &host.export.chunks {
        keys.insert(chunk.object_key.clone());
    }
    for snapshot in &host.export.snapshots {
        keys.insert(snapshot.manifest_key.clone());
        let manifest = if snapshot.manifest_json.trim().is_empty() {
            let (source, bytes) = read_legacy_object(remote, host, &snapshot.manifest_key)?;
            serde_json::from_slice::<SnapshotManifest>(&decode_legacy_object_bytes(
                paths,
                &snapshot.manifest_key,
                &source,
                &bytes,
            )?)?
        } else {
            serde_json::from_str::<SnapshotManifest>(&snapshot.manifest_json)?
        };
        let mut visited = BTreeSet::new();
        for root_tree in manifest.root_trees.values() {
            collect_legacy_tree_keys(
                paths,
                remote,
                host,
                &root_tree.tree_key,
                &packed_oids,
                &mut keys,
                &mut visited,
            )?;
        }
        for records in manifest.roots.values() {
            for record in records {
                collect_legacy_payload_key(
                    paths,
                    remote,
                    host,
                    &record.payload,
                    &packed_oids,
                    &mut keys,
                    &mut visited,
                )?;
            }
        }
    }
    Ok(keys)
}

fn collect_legacy_tree_keys(
    paths: &Paths,
    remote: &RemoteStore,
    host: &LegacyRemoteHost,
    key: &str,
    packed_oids: &BTreeSet<&str>,
    keys: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> Result<()> {
    if !visited.insert(key.to_string()) {
        return Ok(());
    }
    keys.insert(key.to_string());
    let (source, bytes) = read_legacy_object(remote, host, key)?;
    let decoded = decode_legacy_object_bytes(paths, key, &source, &bytes)?;
    let tree: TreeManifest = serde_json::from_slice(&decoded)
        .with_context(|| format!("parse legacy tree manifest {key}"))?;
    for record in tree.entries.values() {
        collect_legacy_payload_key(
            paths,
            remote,
            host,
            &record.payload,
            packed_oids,
            keys,
            visited,
        )?;
    }
    if let Some(root) = &tree.root_node {
        collect_legacy_tree_node_keys(
            paths,
            remote,
            host,
            &root.node_key,
            packed_oids,
            keys,
            visited,
        )?;
    }
    for node in tree.subtree_nodes.values() {
        collect_legacy_tree_node_keys(
            paths,
            remote,
            host,
            &node.node_key,
            packed_oids,
            keys,
            visited,
        )?;
    }
    Ok(())
}

fn collect_legacy_tree_node_keys(
    paths: &Paths,
    remote: &RemoteStore,
    host: &LegacyRemoteHost,
    key: &str,
    packed_oids: &BTreeSet<&str>,
    keys: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> Result<()> {
    if !visited.insert(key.to_string()) {
        return Ok(());
    }
    keys.insert(key.to_string());
    let (source, bytes) = read_legacy_object(remote, host, key)?;
    let decoded = decode_legacy_object_bytes(paths, key, &source, &bytes)?;
    let node: TreeNodeManifest = serde_json::from_slice(&decoded)
        .with_context(|| format!("parse legacy tree node {key}"))?;
    for record in node.entries.values() {
        collect_legacy_payload_key(
            paths,
            remote,
            host,
            &record.payload,
            packed_oids,
            keys,
            visited,
        )?;
    }
    for child in node.child_nodes.values() {
        collect_legacy_tree_node_keys(
            paths,
            remote,
            host,
            &child.node_key,
            packed_oids,
            keys,
            visited,
        )?;
    }
    Ok(())
}

fn collect_legacy_payload_key(
    paths: &Paths,
    remote: &RemoteStore,
    host: &LegacyRemoteHost,
    payload: &crate::majutsu_core::Payload,
    packed_oids: &BTreeSet<&str>,
    keys: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> Result<()> {
    if let Some((oid, key)) = payload_blob_ref(payload)
        && !packed_oids.contains(oid)
    {
        keys.insert(key.to_string());
    }
    if let Some((_, key, _)) = payload_large_ref(payload) {
        keys.insert(key.to_string());
        if visited.insert(key.to_string()) {
            let (source, bytes) = read_legacy_object(remote, host, key)?;
            let decoded = decode_legacy_object_bytes(paths, key, &source, &bytes)?;
            let manifest: LargeManifest = serde_json::from_slice(&decoded)
                .with_context(|| format!("parse legacy large manifest {key}"))?;
            for chunk in manifest.chunks {
                keys.insert(chunk.object_key);
            }
        }
    }
    Ok(())
}

fn read_legacy_object(
    remote: &RemoteStore,
    host: &LegacyRemoteHost,
    key: &str,
) -> Result<(String, Vec<u8>)> {
    let alias = canonical_remote_alias(key);
    let mut candidates = Vec::new();
    if let Some(prefix) = &host.source_prefix
        && !key.starts_with(prefix)
    {
        candidates.push(format!("{prefix}{key}"));
    }
    candidates.push(key.to_string());
    if let Some(alias) = alias {
        if let Some(prefix) = &host.source_prefix {
            candidates.push(format!("{prefix}{alias}"));
        }
        candidates.push(alias);
    }
    candidates.dedup();
    for candidate in candidates {
        if let Some(bytes) = remote.get_optional(&candidate)? {
            return Ok((candidate, bytes));
        }
    }
    bail!("legacy remote object is missing: {key}")
}

fn decode_legacy_object_bytes(
    paths: &Paths,
    key: &str,
    source: &str,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    let canonical_source = canonical_remote_alias(key)
        .is_some_and(|alias| source == alias || source.ends_with(&format!("/{alias}")));
    if canonical_source {
        let local = crate::canonical_remote_object_to_local_bytes(paths, key, bytes)?;
        return crate::decode_object(paths, &local);
    }
    crate::decode_object(paths, bytes)
}

fn resolve_legacy_content_source(
    remote: &RemoteStore,
    host: &LegacyRemoteHost,
    key: &str,
) -> Result<(String, String)> {
    let source = read_legacy_object(remote, host, key)?.0;
    let relative = if let Some(prefix) = &host.source_prefix {
        source
            .strip_prefix(prefix)
            .unwrap_or(source.as_str())
            .to_string()
    } else {
        source.clone()
    };
    let destination = if matches!(remote, RemoteStore::S3(_)) {
        canonical_remote_alias(key).unwrap_or_else(|| key.to_string())
    } else if relative == key || canonical_remote_alias(key).as_deref() == Some(relative.as_str()) {
        relative
    } else {
        key.to_string()
    };
    Ok((source, destination))
}

fn encode_legacy_canonical_content(
    paths: &Paths,
    key: &str,
    source: &str,
    bytes: &[u8],
    snapshot_manifest: bool,
) -> Result<Vec<u8>> {
    let alias = canonical_remote_alias(key)
        .ok_or_else(|| anyhow!("legacy content key has no canonical alias: {key}"))?;
    if source == alias || source.ends_with(&format!("/{alias}")) {
        return Ok(bytes.to_vec());
    }
    let decoded = crate::decode_object(paths, bytes)?;
    if snapshot_manifest {
        let manifest: SnapshotManifest = serde_json::from_slice(&decoded)
            .with_context(|| format!("decode legacy snapshot manifest {key}"))?;
        return crate::encode_compact_snapshot_manifest_for_remote(paths, &manifest);
    }
    if key.starts_with("objects/trees/nodes/") {
        let manifest: TreeNodeManifest = serde_json::from_slice(&decoded)
            .with_context(|| format!("decode legacy tree node {key}"))?;
        return crate::sync_runtime::encode_canonical_remote_export(paths, &manifest);
    }
    if key.starts_with("objects/trees/") {
        let manifest: TreeManifest = serde_json::from_slice(&decoded)
            .with_context(|| format!("decode legacy tree manifest {key}"))?;
        return crate::sync_runtime::encode_canonical_remote_export(paths, &manifest);
    }
    if key.starts_with("objects/indexes/pack/") {
        let index: crate::majutsu_pack::PackIndex = serde_json::from_slice(&decoded)
            .with_context(|| format!("decode legacy pack index {key}"))?;
        return crate::sync_runtime::encode_canonical_remote_export(paths, &index);
    }
    if key.starts_with("objects/large/manifests/") {
        let manifest: LargeManifest = serde_json::from_slice(&decoded)
            .with_context(|| format!("decode legacy large manifest {key}"))?;
        return crate::sync_runtime::encode_canonical_remote_export(paths, &manifest);
    }
    crate::encode_object(paths, &decoded)
}

fn legacy_structured_alias(key: &str) -> bool {
    key.starts_with("objects/blobs/")
        || key.starts_with("objects/trees/")
        || key.starts_with("objects/indexes/pack/")
        || key.starts_with("objects/large/manifests/")
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_remote_key_uses_host_label_for_s3() {
        let key = "objects/blobs/aa/bb";
        let remote_key = repair_remote_key(true, "mba22", key);
        assert_eq!(remote_key, "mba22/blobs/loose/aa/bb.blob.enc");
    }

    #[test]
    fn repair_remote_key_keeps_file_remote_local_key() {
        let key = "objects/blobs/aa/bb";
        let remote_key = repair_remote_key(false, "mba22", key);
        assert_eq!(remote_key, key);
    }

    #[test]
    fn legacy_metadata_destination_uses_s3_compressed_suffix() {
        let file_key = legacy_metadata_destination(false, "vagrant");
        let s3_key = legacy_metadata_destination(true, "vagrant");
        assert_eq!(file_key, "vagrant/metadata/export.json");
        assert_eq!(s3_key, "vagrant/metadata/export.json.zst");
    }
}
