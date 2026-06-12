use anyhow::{Context, Result, bail};
use majutsu_store::{
    LEGACY_METADATA_EXPORT_KEY, RemoteHostIndex, RemoteHostSummary, select_remote_host,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Instant;
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
    let trace = CloneTrace::new();
    if paths.home.exists() && paths.home.read_dir()?.next().is_some() {
        bail!("target majutsu home is not empty: {}", paths.home.display());
    }
    let remote_config = RemoteConfig::from_url(args.remote);
    let remote = open_remote(&remote_config)?;
    trace.mark("open remote");
    let loaded = clone_loaded_metadata(&remote, args.host.as_deref())?;
    trace.mark("select metadata");
    let mut export = match loaded.export {
        Some(export) => {
            trace.mark("download metadata");
            trace.mark("parse metadata");
            export
        }
        None => {
            let export_bytes = clone_metadata_bytes(&remote, &loaded.selection.key)?;
            trace.mark("download metadata");
            let export = serde_json::from_slice(&export_bytes)?;
            trace.mark("parse metadata");
            export
        }
    };
    export.config.remote = Some(remote_config);
    let compact_snapshot_metadata = export.snapshots.iter().any(snapshot_metadata_is_compact);
    let staging_home = clone_staging_home(&paths.home);
    let staging_paths = resolve_paths(Some(staging_home.clone()))?;
    validate_config(&export.config)?;
    crate::validate_clone_host_summary(
        &loaded.selection.host,
        loaded.selection.host_index,
        &export,
    )?;
    crate::validate_clone_large_pin_metadata(&export)?;
    if !compact_snapshot_metadata {
        crate::validate_clone_metadata(&export)?;
    }
    crate::validate_clone_remote_refs(&remote, loaded.selection.host.as_ref(), &export)?;
    trace.mark("validate bootstrap metadata");
    let clone_result = (|| -> Result<()> {
        crate::create_layout(&staging_paths)?;
        write_config(&staging_paths, &export.config)?;
        fs::write(
            &staging_paths.host,
            toml::to_string_pretty(&export.config.host)?,
        )?;
        if let Some(recipients) = remote.get_optional("keys/recipients.toml")? {
            fs::write(staging_paths.home.join("keys/recipients.toml"), recipients)?;
        }
        if export.config.security.encryption != "none" {
            let key = env::var("MAJUTSU_MASTER_KEY")
                .context("encrypted clone requires MAJUTSU_MASTER_KEY=<64-hex-key>")?;
            crate::write_master_key(&staging_paths, &key)?;
        }
        if compact_snapshot_metadata {
            download_compact_snapshot_manifests(&staging_paths, &remote, &export)?;
        }
        trace.mark("write bootstrap files");
        if !matches!(remote, RemoteStore::S3(_)) {
            crate::validate_clone_remote_lifecycle_artifacts(&remote)?;
            crate::validate_clone_remote_gc_mark(
                &staging_paths,
                &remote,
                loaded.selection.host.as_ref(),
                &export,
            )?;
            crate::validate_clone_remote_oplog(
                &staging_paths,
                &remote,
                loaded.selection.host.as_ref(),
                &export.operations,
            )?;
            crate::validate_clone_remote_timeline_exports(
                &staging_paths,
                &remote,
                loaded.selection.host.as_ref(),
                &export.snapshots,
                &export.operations,
            )?;
            crate::validate_clone_remote_snapshot_objects(&staging_paths, &remote, &export)?;
            crate::validate_clone_remote_blob_objects(&staging_paths, &remote, &export)?;
            crate::validate_clone_remote_chunk_index(&staging_paths, &remote, &export)?;
            crate::validate_clone_remote_pack_objects(&staging_paths, &remote, &export)?;
            crate::validate_clone_remote_large_objects(&staging_paths, &remote, &export)?;
        }
        trace.mark("validate remote objects");
        if clone_should_materialize_objects(&remote) {
            materialize_clone_objects(&staging_paths, &remote, &export)?;
        }
        trace.mark("materialize objects");
        let mut conn = crate::open_db(&staging_paths)?;
        crate::import_metadata(&mut conn, &export)?;
        persist_export_remote_refs(
            &conn,
            &remote.describe(),
            &export.config.host.id,
            &export.refs,
        )?;
        trace.mark("import metadata");
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
    trace.mark("finish");
    Ok(())
}

struct CloneTrace {
    enabled: bool,
    start: Instant,
}

impl CloneTrace {
    fn new() -> Self {
        Self {
            enabled: env::var("MAJUTSU_TRACE_CLONE")
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
                .unwrap_or(false),
            start: Instant::now(),
        }
    }

    fn mark(&self, label: &str) {
        if self.enabled {
            eprintln!(
                "clone_trace elapsed_ms={} stage={label}",
                self.start.elapsed().as_millis()
            );
        }
    }
}

fn snapshot_metadata_is_compact(snapshot: &majutsu_core::SnapshotExport) -> bool {
    if snapshot.manifest_json.trim().is_empty() {
        return true;
    }
    serde_json::from_str::<majutsu_core::SnapshotManifest>(&snapshot.manifest_json)
        .map(|manifest| manifest.roots.is_empty() && !manifest.root_trees.is_empty())
        .unwrap_or(false)
}

fn clone_metadata_bytes(remote: &RemoteStore, key: &str) -> Result<Vec<u8>> {
    if matches!(remote, RemoteStore::S3(_)) {
        if key.ends_with(".zst") {
            let bytes = remote
                .get(key)
                .with_context(|| format!("read compressed metadata {key}"))?;
            return zstd::stream::decode_all(bytes.as_slice())
                .with_context(|| format!("decode compressed metadata {key}"));
        }
        let compressed_key = compressed_metadata_key(key);
        if let Some(bytes) = remote
            .get_optional(&compressed_key)
            .with_context(|| format!("read compressed metadata {compressed_key}"))?
        {
            return zstd::stream::decode_all(bytes.as_slice())
                .with_context(|| format!("decode compressed metadata {compressed_key}"));
        }
    }
    remote
        .get(key)
        .with_context(|| format!("read metadata {key}"))
}

fn compressed_metadata_key(key: &str) -> String {
    format!("{key}.zst")
}

struct CloneLoadedMetadata {
    selection: CloneMetadataSelection,
    export: Option<MetadataExport>,
}

struct CloneMetadataSelection {
    key: String,
    host: Option<RemoteHostSummary>,
    host_index: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct CloneBootstrapExport {
    version: u32,
    host_index: RemoteHostIndex,
    #[serde(default)]
    metadata: Option<MetadataExport>,
}

const CLONE_BOOTSTRAP_KEY: &str = "metadata/bootstrap.json.zst";
const CLONE_BOOTSTRAP_VERSION: u32 = 2;

fn clone_loaded_metadata(remote: &RemoteStore, host: Option<&str>) -> Result<CloneLoadedMetadata> {
    if let Some(loaded) = clone_loaded_metadata_from_bootstrap(remote, host)? {
        return Ok(loaded);
    }
    Ok(CloneLoadedMetadata {
        selection: clone_metadata_selection(remote, host)?,
        export: None,
    })
}

fn clone_loaded_metadata_from_bootstrap(
    remote: &RemoteStore,
    host: Option<&str>,
) -> Result<Option<CloneLoadedMetadata>> {
    if !matches!(remote, RemoteStore::S3(_)) {
        return Ok(None);
    }
    let Some(bytes) = remote
        .get_optional(CLONE_BOOTSTRAP_KEY)
        .with_context(|| format!("read clone bootstrap {CLONE_BOOTSTRAP_KEY}"))?
    else {
        return Ok(None);
    };
    let decoded = zstd::stream::decode_all(bytes.as_slice())
        .with_context(|| format!("decode clone bootstrap {CLONE_BOOTSTRAP_KEY}"))?;
    let bootstrap: CloneBootstrapExport = serde_json::from_slice(&decoded)
        .with_context(|| format!("parse clone bootstrap {CLONE_BOOTSTRAP_KEY}"))?;
    if bootstrap.version != 1 && bootstrap.version != CLONE_BOOTSTRAP_VERSION {
        return Ok(None);
    }
    let mut index = bootstrap.host_index;
    index.sort_hosts();
    let selected = match host {
        Some(host_id) => select_remote_host(index.hosts, host_id).ok(),
        None => match index.hosts.as_slice() {
            [host] => Some(host.clone()),
            _ => None,
        },
    };
    let Some(host) = selected else {
        return Ok(None);
    };
    if let Some(metadata) = bootstrap.metadata {
        if host.id != metadata.config.host.id {
            return Ok(None);
        }
        return Ok(Some(CloneLoadedMetadata {
            selection: CloneMetadataSelection {
                key: host.metadata_key.clone(),
                host: Some(host),
                host_index: true,
            },
            export: Some(metadata),
        }));
    }
    Ok(Some(CloneLoadedMetadata {
        selection: CloneMetadataSelection {
            key: host.metadata_key.clone(),
            host: Some(host),
            host_index: true,
        },
        export: None,
    }))
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

fn download_compact_snapshot_manifests(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<()> {
    let keys = export
        .snapshots
        .iter()
        .filter(|snapshot| snapshot.manifest_json.trim().is_empty())
        .map(|snapshot| snapshot.manifest_key.clone())
        .collect::<Vec<_>>();
    materialize_keys(paths, remote, keys)
}

fn materialize_clone_objects(
    paths: &Paths,
    remote: &RemoteStore,
    export: &MetadataExport,
) -> Result<()> {
    materialize_keys(paths, remote, local_object_keys(paths, export)?)
}

fn materialize_keys(paths: &Paths, remote: &RemoteStore, keys: Vec<String>) -> Result<()> {
    if matches!(remote, RemoteStore::S3(_)) {
        return materialize_keys_parallel(paths, remote, keys);
    }
    for key in keys {
        materialize_one_key(paths, remote, &key)?;
    }
    Ok(())
}

fn materialize_one_key(paths: &Paths, remote: &RemoteStore, key: &str) -> Result<()> {
    let dest = paths.home.join(key);
    if dest.exists() {
        return Ok(());
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = crate::download_local_object_from_remote(paths, remote, key)
        .with_context(|| format!("download clone object {key}"))?;
    fs::write(&dest, bytes).with_context(|| format!("write clone object {key}"))?;
    Ok(())
}

fn materialize_keys_parallel(paths: &Paths, remote: &RemoteStore, keys: Vec<String>) -> Result<()> {
    let keys = keys
        .into_iter()
        .filter(|key| !paths.home.join(key).exists())
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Ok(());
    }
    let workers = clone_parallel_downloads().min(keys.len());
    let paths = Arc::new(paths.clone());
    let remote = Arc::new(remote.clone());
    let keys = Arc::new(Mutex::new(keys.into_iter()));
    let (err_tx, err_rx) = mpsc::channel::<anyhow::Error>();
    let mut handles = Vec::new();
    for _ in 0..workers {
        let paths = Arc::clone(&paths);
        let remote = Arc::clone(&remote);
        let keys = Arc::clone(&keys);
        let err_tx = err_tx.clone();
        handles.push(thread::spawn(move || {
            loop {
                let key = {
                    let mut keys = match keys.lock() {
                        Ok(keys) => keys,
                        Err(err) => {
                            let _ =
                                err_tx.send(anyhow::anyhow!("clone work queue poisoned: {err}"));
                            return;
                        }
                    };
                    keys.next()
                };
                let Some(key) = key else {
                    return;
                };
                if let Err(err) = materialize_one_key(&paths, &remote, &key) {
                    let _ = err_tx.send(err);
                    return;
                }
            }
        }));
    }
    drop(err_tx);
    let mut first_error = err_rx.into_iter().next();
    for handle in handles {
        if let Err(err) = handle.join() {
            first_error
                .get_or_insert_with(|| anyhow::anyhow!("clone download worker panicked: {err:?}"));
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(())
}

fn clone_parallel_downloads() -> usize {
    env::var("MAJUTSU_CLONE_PARALLEL_DOWNLOADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(32)
}

fn clone_should_materialize_objects(remote: &RemoteStore) -> bool {
    if !matches!(remote, RemoteStore::S3(_)) {
        return true;
    }
    env::var("MAJUTSU_CLONE_FULL_MATERIALIZE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}
