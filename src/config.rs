use crate::majutsu_cli::{parse_byte_size, parse_duration_millis};
use crate::majutsu_core::{OperationLogEntry as OperationExport, SnapshotExport, SnapshotMode};
use crate::majutsu_crypto::EncryptionMode;
use crate::majutsu_large::{ChunkExport, LargeObjectExport, LargePinExport};
use crate::majutsu_pack::PackExport;
use crate::majutsu_store::BlobExport;
use crate::majutsu_watch::WatchMode;
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;

use crate::watch_runtime::{default_watch_backend, normalize_watch_backend};

#[derive(Debug, Clone)]
pub(crate) struct Paths {
    pub(crate) home: PathBuf,
    pub(crate) db: PathBuf,
    pub(crate) config: PathBuf,
    pub(crate) host: PathBuf,
    pub(crate) objects: PathBuf,
    pub(crate) trees: PathBuf,
    pub(crate) large_chunks: PathBuf,
    pub(crate) large_manifests: PathBuf,
    pub(crate) packs: PathBuf,
    pub(crate) pack_indexes: PathBuf,
    pub(crate) logs: PathBuf,
    pub(crate) runtime: PathBuf,
    pub(crate) daemon_pid: PathBuf,
    pub(crate) daemon_lock: PathBuf,
    pub(crate) snapshot_lock: PathBuf,
    pub(crate) sync_lock: PathBuf,
    pub(crate) upload_queue: PathBuf,
    pub(crate) event_queue: PathBuf,
    pub(crate) master_key: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Config {
    pub(crate) host: HostConfig,
    pub(crate) remote: Option<RemoteConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) roots: Vec<ConfigRoot>,
    pub(crate) large: LargeConfig,
    #[serde(default)]
    pub(crate) pack: PackConfig,
    #[serde(default)]
    pub(crate) watch: WatchConfig,
    #[serde(default)]
    pub(crate) security: SecurityConfig,
    #[serde(default)]
    pub(crate) tiering: TieringConfig,
    #[serde(default)]
    pub(crate) restore: RestoreConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HostConfig {
    pub(crate) id: String,
    pub(crate) name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct RemoteConfig {
    #[serde(default)]
    pub(crate) url: Option<String>,
    #[serde(default, rename = "type")]
    pub(crate) remote_type: Option<String>,
    #[serde(default)]
    pub(crate) path: Option<PathBuf>,
    #[serde(default)]
    pub(crate) bucket: Option<String>,
    #[serde(default)]
    pub(crate) prefix: Option<String>,
    #[serde(default)]
    pub(crate) endpoint: Option<String>,
    #[serde(default)]
    pub(crate) region: Option<String>,
    #[serde(default)]
    pub(crate) signature_version: Option<String>,
}

impl RemoteConfig {
    pub(crate) fn from_url(url: String) -> Self {
        Self {
            url: Some(url),
            remote_type: None,
            path: None,
            bucket: None,
            prefix: None,
            endpoint: None,
            region: None,
            signature_version: None,
        }
    }

    pub(crate) fn url(&self) -> Result<String> {
        if let Some(url) = &self.url {
            return Ok(url.clone());
        }
        match self.remote_type.as_deref() {
            Some("file") => {
                let path = self
                    .path
                    .as_ref()
                    .ok_or_else(|| anyhow!("file remote requires [remote].path"))?;
                Ok(format!("file://{}", path.display()))
            }
            Some("s3") | None if self.bucket.is_some() => {
                let bucket = self
                    .bucket
                    .as_ref()
                    .ok_or_else(|| anyhow!("s3 remote requires [remote].bucket"))?;
                let prefix = self.prefix.as_deref().unwrap_or_default().trim_matches('/');
                if prefix.is_empty() {
                    Ok(format!("s3://{bucket}"))
                } else {
                    Ok(format!("s3://{bucket}/{prefix}"))
                }
            }
            Some(other) => bail!("unsupported remote type: {other}"),
            None => bail!("remote requires url, or type plus path/bucket"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct SecurityConfig {
    #[serde(default)]
    pub(crate) encryption: String,
    #[serde(default = "default_security_key_id")]
    pub(crate) key_id: String,
    #[serde(default = "default_security_hash")]
    pub(crate) hash: String,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            encryption: "none".into(),
            key_id: default_security_key_id(),
            hash: default_security_hash(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub(crate) struct RestoreConfig {
    #[serde(default)]
    pub(crate) archive: RestoreArchiveConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct RestoreArchiveConfig {
    #[serde(default = "default_restore_archive_days")]
    pub(crate) days: u32,
    #[serde(default = "default_restore_archive_tier")]
    pub(crate) tier: String,
}

impl Default for RestoreArchiveConfig {
    fn default() -> Self {
        Self {
            days: default_restore_archive_days(),
            tier: default_restore_archive_tier(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TieringConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_tiering_rules")]
    pub(crate) rules: Vec<TieringRule>,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rules: default_tiering_rules(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TieringRule {
    pub(crate) name: String,
    pub(crate) prefix: String,
    #[serde(default)]
    pub(crate) after: Option<String>,
    #[serde(default, alias = "transition_to", alias = "keep")]
    pub(crate) storage: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct OperationChangeExport {
    pub(crate) op_id: String,
    pub(crate) root_id: String,
    pub(crate) path: String,
    pub(crate) status: String,
}

pub(crate) const METADATA_EXPORT_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MetadataExport {
    pub(crate) version: u32,
    pub(crate) exported_at: DateTime<Utc>,
    pub(crate) config: Config,
    pub(crate) roots: Vec<RootConfig>,
    pub(crate) snapshots: Vec<SnapshotExport>,
    pub(crate) operations: Vec<OperationExport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) operation_changes: Vec<OperationChangeExport>,
    pub(crate) refs: BTreeMap<String, String>,
    pub(crate) blobs: Vec<BlobExport>,
    pub(crate) large_objects: Vec<LargeObjectExport>,
    pub(crate) chunks: Vec<ChunkExport>,
    pub(crate) packs: Vec<PackExport>,
    #[serde(default)]
    pub(crate) large_pins: Vec<LargePinExport>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LazyMountEntry {
    pub(crate) version: u32,
    pub(crate) snapshot_id: String,
    pub(crate) root_id: String,
    pub(crate) path: String,
    pub(crate) size: u64,
    pub(crate) manifest_key: String,
    pub(crate) chunk_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MountViewMetadata {
    pub(crate) version: u32,
    pub(crate) snapshot_id: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) hydrate_large: bool,
    pub(crate) files: usize,
    pub(crate) lazy_large_files: usize,
    pub(crate) hydrated_large_files: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct LargeConfig {
    pub(crate) enabled: bool,
    #[serde(
        default = "default_large_min_size",
        deserialize_with = "deserialize_u64_bytes"
    )]
    pub(crate) min_size: u64,
    #[serde(
        default = "default_large_binary_min_size",
        deserialize_with = "deserialize_u64_bytes"
    )]
    pub(crate) binary_min_size: u64,
    #[serde(
        default = "default_large_chunked_min_size",
        deserialize_with = "deserialize_u64_bytes"
    )]
    pub(crate) chunked_min_size: u64,
    #[serde(
        default = "default_large_chunked_chunk_size",
        deserialize_with = "deserialize_usize_bytes"
    )]
    pub(crate) chunked_chunk_size: usize,
    #[serde(default = "default_large_chunking")]
    pub(crate) default_chunking: String,
    #[serde(
        default = "default_chunk_size",
        alias = "target_chunk_size",
        deserialize_with = "deserialize_usize_bytes"
    )]
    pub(crate) chunk_size: usize,
    #[serde(default = "default_large_max_parallel_uploads")]
    pub(crate) max_parallel_uploads: usize,
    #[serde(default = "default_true")]
    pub(crate) multipart: bool,
    pub(crate) always: Vec<String>,
    pub(crate) never: Vec<String>,
    #[serde(default)]
    pub(crate) compression: LargeCompressionConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct PackConfig {
    #[serde(
        default = "default_small_pack_target",
        deserialize_with = "deserialize_u64_bytes"
    )]
    pub(crate) small_pack_target: u64,
    #[serde(
        default = "default_normal_pack_target",
        deserialize_with = "deserialize_u64_bytes"
    )]
    pub(crate) normal_pack_target: u64,
}

impl Default for PackConfig {
    fn default() -> Self {
        Self {
            small_pack_target: default_small_pack_target(),
            normal_pack_target: default_normal_pack_target(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct WatchConfig {
    #[serde(default = "default_watch_backend")]
    pub(crate) backend: String,
    #[serde(default = "default_watch_mode")]
    pub(crate) mode: String,
    #[serde(
        default = "default_watch_debounce_ms",
        deserialize_with = "deserialize_millis"
    )]
    pub(crate) debounce: u64,
    #[serde(
        default = "default_watch_settle_ms",
        deserialize_with = "deserialize_millis"
    )]
    pub(crate) settle: u64,
    #[serde(
        default = "default_watch_buffer_max_ms",
        deserialize_with = "deserialize_millis"
    )]
    pub(crate) buffer_max: u64,
    #[serde(default = "default_watch_buffer_max_events")]
    pub(crate) buffer_max_events: usize,
    #[serde(
        default = "default_watch_periodic_rescan_secs",
        deserialize_with = "deserialize_seconds"
    )]
    pub(crate) periodic_rescan: u64,
    #[serde(
        default = "default_watch_interval_secs",
        deserialize_with = "deserialize_seconds"
    )]
    pub(crate) interval: u64,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            backend: default_watch_backend(),
            mode: default_watch_mode(),
            debounce: default_watch_debounce_ms(),
            settle: default_watch_settle_ms(),
            buffer_max: default_watch_buffer_max_ms(),
            buffer_max_events: default_watch_buffer_max_events(),
            periodic_rescan: default_watch_periodic_rescan_secs(),
            interval: default_watch_interval_secs(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ConfigRoot {
    pub(crate) id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    pub(crate) path: PathBuf,
    #[serde(default = "default_include")]
    pub(crate) include: Vec<String>,
    #[serde(default)]
    pub(crate) exclude: Vec<String>,
    #[serde(default)]
    pub(crate) explicit_track: Vec<String>,
    #[serde(default)]
    pub(crate) explicit_untrack: Vec<String>,
    #[serde(default)]
    pub(crate) follow_symlinks: bool,
    #[serde(default)]
    pub(crate) require_mount: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) degraded: Option<RootDegraded>,
    #[serde(default = "default_snapshot_mode")]
    pub(crate) snapshot_mode: String,
    #[serde(default)]
    pub(crate) pre_snapshot: Option<String>,
    #[serde(default)]
    pub(crate) post_snapshot: Option<String>,
    #[serde(default)]
    pub(crate) snapshot_source: Option<PathBuf>,
    #[serde(default)]
    pub(crate) application_plugin: Option<String>,
    #[serde(default)]
    pub(crate) large: Option<RootLargeConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) volatile: Option<RootVolatileConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct LargeCompressionConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_large_compression_algorithm")]
    pub(crate) algorithm: String,
    #[serde(default = "default_large_compression_level")]
    pub(crate) level: i32,
    #[serde(
        default = "default_large_compression_sample_bytes",
        deserialize_with = "deserialize_usize_bytes"
    )]
    pub(crate) sample_bytes: usize,
    #[serde(default = "default_large_compression_min_gain_ratio")]
    pub(crate) min_gain_ratio: f64,
    #[serde(default = "default_large_compression_skip_extensions")]
    pub(crate) skip_extensions: Vec<String>,
}

impl Default for LargeCompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: default_large_compression_algorithm(),
            level: default_large_compression_level(),
            sample_bytes: default_large_compression_sample_bytes(),
            min_gain_ratio: default_large_compression_min_gain_ratio(),
            skip_extensions: default_large_compression_skip_extensions(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct RootConfig {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) path: PathBuf,
    #[serde(default = "default_include")]
    pub(crate) include: Vec<String>,
    #[serde(default)]
    pub(crate) exclude: Vec<String>,
    #[serde(default)]
    pub(crate) explicit_track: Vec<String>,
    #[serde(default)]
    pub(crate) explicit_untrack: Vec<String>,
    pub(crate) follow_symlinks: bool,
    #[serde(default)]
    pub(crate) require_mount: bool,
    #[serde(default = "default_root_status")]
    pub(crate) status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) degraded: Option<RootDegraded>,
    #[serde(default = "default_snapshot_mode")]
    pub(crate) snapshot_mode: String,
    #[serde(default)]
    pub(crate) pre_snapshot: Option<String>,
    #[serde(default)]
    pub(crate) post_snapshot: Option<String>,
    #[serde(default)]
    pub(crate) snapshot_source: Option<PathBuf>,
    #[serde(default)]
    pub(crate) application_plugin: Option<String>,
    #[serde(default)]
    pub(crate) large: Option<RootLargeConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) volatile: Option<RootVolatileConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct RootDegraded {
    pub(crate) kind: String,
    pub(crate) at: DateTime<Utc>,
    pub(crate) message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct RootLargeConfig {
    #[serde(default, deserialize_with = "deserialize_option_u64_bytes")]
    pub(crate) min_size: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_option_u64_bytes")]
    pub(crate) binary_min_size: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_option_u64_bytes")]
    pub(crate) chunked_min_size: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_option_usize_bytes")]
    pub(crate) chunked_chunk_size: Option<usize>,
    #[serde(default)]
    pub(crate) default_chunking: Option<String>,
    #[serde(
        default,
        alias = "target_chunk_size",
        deserialize_with = "deserialize_option_usize_bytes"
    )]
    pub(crate) chunk_size: Option<usize>,
    #[serde(default)]
    pub(crate) always: Vec<String>,
    #[serde(default)]
    pub(crate) never: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct RootVolatileConfig {
    #[serde(default)]
    pub(crate) patterns: Vec<String>,
    #[serde(default = "default_volatile_mode")]
    pub(crate) mode: String,
}

pub(crate) fn resolve_paths(home_arg: Option<PathBuf>) -> Result<Paths> {
    resolve_paths_with_scope(home_arg, false)
}

pub(crate) fn resolve_paths_with_scope(home_arg: Option<PathBuf>, system: bool) -> Result<Paths> {
    #[cfg(windows)]
    if system && home_arg.is_none() {
        return resolve_paths(Some(crate::platform_runtime::configured_system_state_home()));
    }

    let home = if let Some(home) = home_arg {
        home
    } else if let Ok(home) = env::var("MAJUTSU_HOME") {
        PathBuf::from(home)
    } else if system {
        configured_state_home_from(crate::platform_runtime::system_config_path(), None)?
            .unwrap_or_else(crate::platform_runtime::system_state_home)
    } else if let Some(home) = configured_state_home()? {
        home
    } else {
        let user_home = env::var("HOME").context("HOME is not set")?;
        PathBuf::from(user_home).join(".majutsu")
    };
    Ok(Paths {
        db: home.join("db/majutsu.sqlite"),
        config: home.join("config.toml"),
        host: home.join("host.toml"),
        objects: home.join("objects/blobs"),
        trees: home.join("objects/trees"),
        large_chunks: home.join("objects/large/chunks/fixed"),
        large_manifests: home.join("objects/large/manifests"),
        packs: home.join("objects/packs/normal"),
        pack_indexes: home.join("objects/indexes/pack"),
        logs: home.join("logs"),
        runtime: home.join("runtime"),
        daemon_pid: home.join("runtime/daemon.pid"),
        daemon_lock: home.join("locks/daemon.lock"),
        snapshot_lock: home.join("locks/snapshot.lock"),
        sync_lock: home.join("locks/sync.lock"),
        upload_queue: home.join("queue/uploads"),
        event_queue: home.join("queue/events"),
        master_key: home.join("keys/master.key"),
        home,
    })
}

fn configured_state_home() -> Result<Option<PathBuf>> {
    let user_home = env::var("HOME").ok().map(PathBuf::from);
    let config_home = env::var("XDG_CONFIG_HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| user_home.as_ref().map(|home| home.join(".config")));
    let Some(config_home) = config_home else {
        return Ok(None);
    };
    let path = config_home.join("majutsu/config.toml");
    if !path.exists() {
        return Ok(None);
    }
    configured_state_home_from(path, user_home)
}

fn configured_state_home_from(
    path: PathBuf,
    user_home: Option<PathBuf>,
) -> Result<Option<PathBuf>> {
    if !path.exists() {
        return Ok(None);
    }
    let value: toml::Value = toml::from_str(&fs::read_to_string(path)?)?;
    let Some(home) = value
        .get("state")
        .and_then(|state| state.get("home"))
        .and_then(|home| home.as_str())
    else {
        return Ok(None);
    };
    if let Some(rest) = home.strip_prefix("~/")
        && let Some(user_home) = user_home
    {
        return Ok(Some(user_home.join(rest)));
    }
    Ok(Some(PathBuf::from(home)))
}

pub(crate) fn policy_config(tiering: &TieringConfig) -> crate::majutsu_policy::PolicyConfig {
    crate::majutsu_policy::PolicyConfig {
        enabled: tiering.enabled,
        rules: tiering
            .rules
            .iter()
            .map(|rule| crate::majutsu_policy::PolicyRule {
                name: rule.name.clone(),
                prefix: rule.prefix.clone(),
                after: rule.after.clone(),
                storage: rule.storage.clone(),
            })
            .collect(),
    }
}

pub(crate) fn read_config(paths: &Paths) -> Result<Config> {
    let config: Config = toml::from_str(&fs::read_to_string(&paths.config)?)?;
    validate_config(&config)?;
    Ok(config)
}

pub(crate) fn validate_config(config: &Config) -> Result<()> {
    normalize_watch_backend(&config.watch.backend)?;
    validate_watch_mode(&config.watch.mode)?;
    validate_large_config(&config.large)?;
    validate_pack_config(&config.pack)?;
    validate_security_config(&config.security)?;
    validate_restore_archive_config(&config.restore.archive)?;
    validate_tiering_config(&config.tiering)?;
    Ok(())
}

pub(crate) fn write_config(paths: &Paths, config: &Config) -> Result<()> {
    crate::atomic_io::write_atomic(&paths.config, toml::to_string_pretty(config)?.as_bytes())?;
    Ok(())
}

pub(crate) fn validate_snapshot_mode(mode: &str) -> Result<()> {
    SnapshotMode::parse(mode).map(|_| ())
}

pub(crate) fn validate_watch_mode(mode: &str) -> Result<()> {
    WatchMode::normalize(mode)
        .map(|_| ())
        .map_err(anyhow::Error::msg)
}

fn validate_security_config(security: &SecurityConfig) -> Result<()> {
    encryption_enabled(security)?;
    crate::majutsu_crypto::validate_security_hash(&security.hash)
}

fn validate_large_config(large: &LargeConfig) -> Result<()> {
    validate_large_chunking(&large.default_chunking)?;
    if large.chunk_size == 0 {
        bail!("large chunk_size must be greater than zero");
    }
    if large.chunked_min_size == 0 {
        bail!("large chunked_min_size must be greater than zero");
    }
    if large.chunked_chunk_size == 0 {
        bail!("large chunked_chunk_size must be greater than zero");
    }
    if large.max_parallel_uploads == 0 {
        bail!("large max_parallel_uploads must be greater than zero");
    }
    Ok(())
}

fn validate_pack_config(pack: &PackConfig) -> Result<()> {
    if pack.small_pack_target == 0 {
        bail!("pack small_pack_target must be greater than zero");
    }
    if pack.normal_pack_target == 0 {
        bail!("pack normal_pack_target must be greater than zero");
    }
    Ok(())
}

fn validate_tiering_config(tiering: &TieringConfig) -> Result<()> {
    crate::majutsu_policy::s3_lifecycle_policy(&policy_config(tiering)).map(|_| ())
}

pub(crate) fn encryption_enabled(security: &SecurityConfig) -> Result<bool> {
    crate::majutsu_crypto::encryption_enabled(&security.encryption)
}

pub(crate) fn encryption_mode(security: &SecurityConfig) -> Result<EncryptionMode> {
    EncryptionMode::parse(&security.encryption)
}

pub(crate) fn validate_large_chunking(chunking: &str) -> Result<()> {
    crate::majutsu_large::validate_chunking(chunking)
}

pub(crate) fn validate_restore_archive_config(config: &RestoreArchiveConfig) -> Result<()> {
    if config.days == 0 {
        bail!("restore archive days must be greater than zero");
    }
    if config.tier.trim().is_empty() {
        bail!("restore archive tier must not be empty");
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ByteSizeValue {
    Integer(u64),
    String(String),
}

fn deserialize_u64_bytes<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = ByteSizeValue::deserialize(deserializer)?;
    byte_size_value(value).map_err(serde::de::Error::custom)
}

fn deserialize_usize_bytes<'de, D>(deserializer: D) -> std::result::Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let value = deserialize_u64_bytes(deserializer)?;
    usize::try_from(value).map_err(serde::de::Error::custom)
}

fn deserialize_option_u64_bytes<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(value) = Option::<ByteSizeValue>::deserialize(deserializer)? else {
        return Ok(None);
    };
    byte_size_value(value)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

fn deserialize_option_usize_bytes<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(value) = deserialize_option_u64_bytes(deserializer)? else {
        return Ok(None);
    };
    usize::try_from(value)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

fn byte_size_value(value: ByteSizeValue) -> Result<u64> {
    match value {
        ByteSizeValue::Integer(value) => Ok(value),
        ByteSizeValue::String(value) => parse_byte_size(&value),
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DurationValue {
    Integer(u64),
    String(String),
}

fn deserialize_millis<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = DurationValue::deserialize(deserializer)?;
    duration_value_millis(value).map_err(serde::de::Error::custom)
}

fn deserialize_seconds<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = DurationValue::deserialize(deserializer)?;
    duration_value_seconds(value).map_err(serde::de::Error::custom)
}

fn duration_value_millis(value: DurationValue) -> Result<u64> {
    match value {
        DurationValue::Integer(value) => Ok(value),
        DurationValue::String(value) => parse_duration_millis(&value),
    }
}

fn duration_value_seconds(value: DurationValue) -> Result<u64> {
    match value {
        DurationValue::Integer(value) => Ok(value),
        DurationValue::String(value) => Ok(parse_duration_millis(&value)?.div_ceil(1000)),
    }
}

pub(crate) fn default_include() -> Vec<String> {
    vec!["**".into()]
}

pub(crate) fn default_root_status() -> String {
    "active".into()
}

pub(crate) fn default_snapshot_mode() -> String {
    "default".into()
}

pub(crate) fn default_volatile_mode() -> String {
    "checkpoint".into()
}

pub(crate) fn default_large_chunking() -> String {
    crate::majutsu_large::default_chunking().into()
}

fn default_true() -> bool {
    true
}

pub(crate) fn default_large_min_size() -> u64 {
    crate::majutsu_large::default_large_min_size()
}

pub(crate) fn default_large_max_parallel_uploads() -> usize {
    crate::majutsu_large::default_max_parallel_uploads()
}

pub(crate) fn default_large_binary_min_size() -> u64 {
    crate::majutsu_large::default_large_binary_min_size()
}

pub(crate) fn default_large_chunked_min_size() -> u64 {
    crate::majutsu_large::default_chunked_min_size()
}

pub(crate) fn default_large_chunked_chunk_size() -> usize {
    crate::majutsu_large::default_chunked_chunk_size()
}

pub(crate) fn default_chunk_size() -> usize {
    crate::majutsu_large::default_chunk_size()
}

fn default_large_compression_algorithm() -> String {
    crate::majutsu_large::default_compression_algorithm().into()
}

fn default_large_compression_level() -> i32 {
    crate::majutsu_large::default_compression_level()
}

fn default_large_compression_sample_bytes() -> usize {
    crate::majutsu_large::default_compression_sample_bytes()
}

fn default_large_compression_min_gain_ratio() -> f64 {
    crate::majutsu_large::default_compression_min_gain_ratio()
}

fn default_large_compression_skip_extensions() -> Vec<String> {
    crate::majutsu_large::default_compression_skip_extensions()
}

fn default_small_pack_target() -> u64 {
    crate::majutsu_pack::default_small_pack_target()
}

fn default_normal_pack_target() -> u64 {
    crate::majutsu_pack::default_normal_pack_target()
}

fn default_watch_mode() -> String {
    crate::majutsu_watch::default_mode().into()
}

fn default_watch_debounce_ms() -> u64 {
    crate::majutsu_watch::default_debounce().as_millis() as u64
}

fn default_watch_settle_ms() -> u64 {
    crate::majutsu_watch::default_settle().as_millis() as u64
}

fn default_watch_buffer_max_ms() -> u64 {
    crate::majutsu_watch::default_buffer_max().as_millis() as u64
}

fn default_watch_buffer_max_events() -> usize {
    crate::majutsu_watch::default_buffer_max_events()
}

fn default_watch_periodic_rescan_secs() -> u64 {
    crate::majutsu_watch::default_periodic_rescan().as_secs()
}

fn default_watch_interval_secs() -> u64 {
    crate::majutsu_watch::default_poll_interval().as_secs()
}

pub(crate) fn default_security_key_id() -> String {
    crate::majutsu_crypto::default_security_key_id().into()
}

pub(crate) fn default_security_hash() -> String {
    crate::majutsu_crypto::default_security_hash().into()
}

fn default_restore_archive_days() -> u32 {
    7
}

fn default_restore_archive_tier() -> String {
    "Standard".into()
}

fn default_tiering_rules() -> Vec<TieringRule> {
    crate::majutsu_policy::default_tiering_rules()
        .into_iter()
        .map(|rule| TieringRule {
            name: rule.name,
            prefix: rule.prefix,
            after: rule.after,
            storage: rule.storage,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_duration_strings_round_subsecond_up_without_disabling() {
        assert_eq!(
            duration_value_seconds(DurationValue::String("500ms".into())).unwrap(),
            1
        );
        assert_eq!(
            duration_value_seconds(DurationValue::String("1500ms".into())).unwrap(),
            2
        );
        assert_eq!(
            duration_value_seconds(DurationValue::String("0s".into())).unwrap(),
            0
        );
    }
}
