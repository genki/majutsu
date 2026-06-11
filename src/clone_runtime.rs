use anyhow::{Context, Result, bail};
use majutsu_store::{LEGACY_METADATA_EXPORT_KEY, RemoteHostSummary, select_remote_host};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::cli::CloneArgs;
use crate::config::{
    MetadataExport, Paths, RemoteConfig, resolve_paths, validate_config, write_config,
};
use crate::db_refs::persist_export_remote_refs;
use crate::object_paths::local_object_keys;
use crate::remote_runtime::{read_remote_host_index, remote_host_index_with_legacy};
use crate::remote_store::{RemoteStore, open_remote};

pub(crate) fn clone_cmd(paths: &Paths, args: CloneArgs) -> Result<()> {
    if paths.home.exists() && paths.home.read_dir()?.next().is_some() {
        bail!("target majutsu home is not empty: {}", paths.home.display());
    }
    let remote_config = RemoteConfig::from_url(args.remote);
    let remote = open_remote(&remote_config)?;
    let metadata = clone_metadata_selection(&remote, args.host.as_deref())?;
    let export_bytes = remote.get(&metadata.key)?;
    let mut export: MetadataExport = serde_json::from_slice(&export_bytes)?;
    export.config.remote = Some(remote_config);
    let compact_snapshot_metadata = export
        .snapshots
        .iter()
        .any(|snapshot| snapshot.manifest_json.trim().is_empty());
    validate_config(&export.config)?;
    crate::validate_clone_host_summary(&metadata.host, metadata.host_index, &export)?;
    if !compact_snapshot_metadata {
        crate::validate_clone_metadata(&export)?;
    }
    crate::validate_clone_remote_refs(&remote, metadata.host.as_ref(), &export)?;
    crate::validate_clone_remote_lifecycle_artifacts(&remote)?;
    if !compact_snapshot_metadata {
        ensure_clone_objects_available(&remote, &export)?;
    }
    let staging_home = clone_staging_home(&paths.home);
    let staging_paths = resolve_paths(Some(staging_home.clone()))?;
    let clone_result = (|| -> Result<()> {
        crate::create_layout(&staging_paths)?;
        write_config(&staging_paths, &export.config)?;
        fs::write(
            &staging_paths.host,
            toml::to_string_pretty(&export.config.host)?,
        )?;
        if remote.exists("keys/recipients.toml")? {
            fs::write(
                staging_paths.home.join("keys/recipients.toml"),
                remote.get("keys/recipients.toml")?,
            )?;
        }
        if export.config.security.encryption != "none" {
            let key = env::var("MAJUTSU_MASTER_KEY")
                .context("encrypted clone requires MAJUTSU_MASTER_KEY=<64-hex-key>")?;
            crate::write_master_key(&staging_paths, &key)?;
        }
        if compact_snapshot_metadata {
            hydrate_compact_snapshot_manifests(&staging_paths, &remote, &mut export)?;
            crate::validate_clone_metadata(&export)?;
            ensure_clone_objects_available(&remote, &export)?;
        }
        crate::validate_clone_remote_gc_mark(&remote, metadata.host.as_ref(), &export)?;
        crate::validate_clone_remote_oplog(
            &staging_paths,
            &remote,
            metadata.host.as_ref(),
            &export.operations,
        )?;
        crate::validate_clone_remote_timeline_exports(
            &staging_paths,
            &remote,
            metadata.host.as_ref(),
            &export.snapshots,
            &export.operations,
        )?;
        crate::validate_clone_remote_snapshot_objects(&staging_paths, &remote, &export)?;
        crate::validate_clone_remote_blob_objects(&staging_paths, &remote, &export)?;
        crate::validate_clone_remote_chunk_index(&staging_paths, &remote, &export)?;
        crate::validate_clone_remote_pack_objects(&staging_paths, &remote, &export)?;
        crate::validate_clone_remote_large_objects(&staging_paths, &remote, &export)?;
        for key in local_object_keys(&export) {
            let dest = staging_paths.home.join(&key);
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(
                dest,
                crate::download_local_object_from_remote(&staging_paths, &remote, &key)?,
            )?;
        }
        let conn = crate::open_db(&staging_paths)?;
        crate::import_metadata(&conn, &export)?;
        persist_export_remote_refs(
            &conn,
            &remote.describe(),
            &export.config.host.id,
            &export.refs,
        )?;
        Ok(())
    })();
    if let Err(err) = clone_result {
        let _ = fs::remove_dir_all(&staging_home);
        return Err(err);
    }
    if paths.home.exists() {
        fs::remove_dir(&paths.home)
            .with_context(|| format!("remove empty clone target {}", paths.home.display()))?;
    }
    fs::rename(&staging_home, &paths.home).with_context(|| {
        format!(
            "move clone staging {} to {}",
            staging_home.display(),
            paths.home.display()
        )
    })?;
    println!("cloned {} into {}", remote.describe(), paths.home.display());
    println!("host {} {}", export.config.host.name, export.config.host.id);
    Ok(())
}

struct CloneMetadataSelection {
    key: String,
    host: Option<RemoteHostSummary>,
    host_index: bool,
}

fn clone_metadata_selection(
    remote: &RemoteStore,
    host: Option<&str>,
) -> Result<CloneMetadataSelection> {
    if let Some(host_id) = host {
        let index = read_remote_host_index(remote)?;
        if !index.hosts.is_empty() {
            let host = select_remote_host(index.hosts, host_id)?;
            return Ok(CloneMetadataSelection {
                key: host.metadata_key.clone(),
                host: Some(host),
                host_index: true,
            });
        }
        let index = remote_host_index_with_legacy(remote)?;
        let host = select_remote_host(index.hosts, host_id)?;
        return Ok(CloneMetadataSelection {
            key: host.metadata_key.clone(),
            host: Some(host),
            host_index: false,
        });
    }
    let index = read_remote_host_index(remote)?;
    match index.hosts.as_slice() {
        [host] => Ok(CloneMetadataSelection {
            key: host.metadata_key.clone(),
            host: Some(host.clone()),
            host_index: true,
        }),
        [] if remote.exists(LEGACY_METADATA_EXPORT_KEY)? => Ok(CloneMetadataSelection {
            key: LEGACY_METADATA_EXPORT_KEY.into(),
            host: None,
            host_index: false,
        }),
        [] => {
            bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found")
        }
        _ => bail!("remote contains multiple hosts; rerun clone with --host"),
    }
}

fn clone_staging_home(home: &Path) -> PathBuf {
    let parent = home.parent().unwrap_or_else(|| Path::new("."));
    let name = home
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "majutsu".into());
    parent.join(format!(".{name}.clone-{}", Uuid::new_v4()))
}

fn hydrate_compact_snapshot_manifests(
    paths: &Paths,
    remote: &RemoteStore,
    export: &mut MetadataExport,
) -> Result<()> {
    for snapshot in &mut export.snapshots {
        if !snapshot.manifest_json.trim().is_empty() {
            continue;
        }
        let dest = paths.home.join(&snapshot.manifest_key);
        if !dest.exists() {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(
                &dest,
                crate::download_local_object_from_remote(paths, remote, &snapshot.manifest_key)?,
            )?;
        }
        let bytes = crate::read_object(paths, &snapshot.manifest_key)
            .with_context(|| format!("decode snapshot manifest {}", snapshot.manifest_key))?;
        let manifest: majutsu_core::SnapshotManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse snapshot manifest {}", snapshot.manifest_key))?;
        snapshot.manifest_json = serde_json::to_string(&manifest)?;
    }
    Ok(())
}

fn ensure_clone_objects_available(remote: &RemoteStore, export: &MetadataExport) -> Result<()> {
    let mut missing = Vec::new();
    for key in local_object_keys(export) {
        if !crate::remote_object_available(remote, &key)? {
            missing.push(key);
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let sample = missing
        .iter()
        .take(5)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if missing.len() > 5 {
        format!(", ... {} more", missing.len() - 5)
    } else {
        String::new()
    };
    bail!(
        "remote is missing {} object(s) required for clone: {sample}{suffix}",
        missing.len()
    )
}
