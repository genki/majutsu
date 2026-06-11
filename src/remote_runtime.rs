use anyhow::{Result, anyhow, bail};
use chrono::Utc;
use majutsu_store::{
    LEGACY_METADATA_EXPORT_KEY, REMOTE_HOST_INDEX_KEY, RemoteHostIndex, RemoteHostSummary,
    select_remote_host,
};

use crate::cli::RemoteCommand;
use crate::config::{MetadataExport, Paths, read_config};
use crate::fsck_runtime::remote_fsck;
use crate::object_paths::local_object_keys;
use crate::operation_log::record_op;
use crate::remote_store::{RemoteStore, open_remote_with_upload_policy};
use crate::snapshot_state::current_snapshot;
use crate::{ensure_ready, export_metadata, open_db, remote_object_available};

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
        RemoteCommand::Fsck { deep } => {
            if deep {
                remote_fsck(paths, &remote)?;
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
                } else {
                    "checked remote object existence"
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
            let export: MetadataExport = serde_json::from_slice(&remote.get(&host.metadata_key)?)?;
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
        bail!("remote fsck quick found {missing} missing object(s)");
    }
    println!("remote fsck quick ok");
    println!("checked_objects {}", keys.len());
    println!("elapsed_secs {}", start.elapsed().as_secs());
    println!("hint use `mj remote fsck --deep` for payload decode/hash verification");
    Ok(())
}

pub(crate) fn read_remote_host_index(remote: &RemoteStore) -> Result<RemoteHostIndex> {
    if remote.exists(REMOTE_HOST_INDEX_KEY)? {
        let mut index: RemoteHostIndex =
            serde_json::from_slice(&remote.get(REMOTE_HOST_INDEX_KEY)?)?;
        index.sort_hosts();
        return Ok(index);
    }
    Ok(RemoteHostIndex::empty(Utc::now()))
}

pub(crate) fn remote_host_index_with_legacy(remote: &RemoteStore) -> Result<RemoteHostIndex> {
    let mut index = read_remote_host_index(remote)?;
    if index.hosts.is_empty() && remote.exists(LEGACY_METADATA_EXPORT_KEY)? {
        let export: MetadataExport =
            serde_json::from_slice(&remote.get(LEGACY_METADATA_EXPORT_KEY)?)?;
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
