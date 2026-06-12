use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use majutsu_store::{
    LEGACY_METADATA_EXPORT_KEY, REMOTE_HOST_INDEX_KEY, RemoteGcMark as GcMarkExport,
    RemoteGcTombstone as GcTombstoneExport, RemoteHostIndex, RemoteHostSummary,
    host_current_ref_key, host_last_synced_ref_key, host_metadata_key, remote_gc_mark_key,
    remote_gc_tombstone_prefix, select_remote_host,
};

use crate::cli::RemoteCommand;
use crate::config::{MetadataExport, Paths, read_config};
use crate::fsck_runtime::remote_fsck;
use crate::object_paths::local_object_keys;
use crate::operation_log::record_op;
use crate::remote_store::{RemoteStore, open_remote_with_upload_policy};
use crate::snapshot_state::current_snapshot;
use crate::util::parse_db_time;
use crate::{ensure_ready, export_metadata, open_db, remote_object_available, remote_ref};

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
        RemoteCommand::Fsck { objects, deep } => {
            if deep {
                remote_fsck(paths, &remote)?;
            } else if objects {
                remote_fsck_objects(paths, &remote)?;
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

fn remote_fsck_quick(_paths: &Paths, remote: &RemoteStore) -> Result<()> {
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
        validate_quick_host_metadata(remote, host, &export, &mut missing)?;
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
    if let Some(current) = current {
        let key = host_current_ref_key(&host.id);
        match remote_ref(remote, &key)? {
            Some(value) if value == *current => {}
            Some(value) => {
                *missing += 1;
                eprintln!("{key} points to {value}, expected {current}");
            }
            None => {
                *missing += 1;
                eprintln!("missing remote ref {key}");
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
        match remote_ref(remote, &key)? {
            Some(value) if value == *last_synced => {}
            Some(value) => {
                *missing += 1;
                eprintln!("{key} points to {value}, expected {last_synced}");
            }
            None => {
                *missing += 1;
                eprintln!("missing remote ref {key}");
            }
        }
    }
    validate_quick_gc_records(remote, &host.id, current, missing)
}

fn validate_quick_gc_records(
    remote: &RemoteStore,
    host_id: &str,
    current: Option<&String>,
    missing: &mut usize,
) -> Result<()> {
    let mark_key = remote_gc_mark_key(host_id);
    let Some(bytes) = remote.get_optional(&mark_key)? else {
        *missing += 1;
        eprintln!("missing remote gc mark {mark_key}");
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
    if mark.current_snapshot.as_ref() != current {
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

fn remote_fsck_objects(paths: &Paths, remote: &RemoteStore) -> Result<()> {
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let export = export_metadata(paths, &conn, &config)?;
    if !remote.exists(REMOTE_HOST_INDEX_KEY)? && !remote.exists(LEGACY_METADATA_EXPORT_KEY)? {
        bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found");
    }
    let keys = local_object_keys(paths, &export)?;
    let mut missing = 0usize;
    let start = std::time::Instant::now();
    for (idx, key) in keys.iter().enumerate() {
        if !remote_object_available(remote, key)? {
            missing += 1;
            eprintln!("missing remote object {key}");
        }
        if (idx + 1) % 500 == 0 {
            eprintln!(
                "remote fsck quick progress checked_objects={} elapsed_secs={}",
                idx + 1,
                start.elapsed().as_secs()
            );
        }
    }
    if missing > 0 {
        bail!("remote fsck objects found {missing} missing object(s)");
    }
    println!("remote fsck objects ok");
    println!("mode objects");
    println!("checked_objects {}", keys.len());
    println!("elapsed_secs {}", start.elapsed().as_secs());
    println!("hint use `mj remote fsck` for quick metadata health verification");
    println!("hint use `mj remote fsck --deep` for payload decode/hash verification");
    Ok(())
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
