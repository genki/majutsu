use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use clap::{Args, Parser, Subcommand};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use hmac::{Hmac, Mac};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use libc::{EIO, EISDIR, ENOENT, EROFS};
use majutsu_core::{
    FileRecord, LargeChunk, LargeManifest, Payload, RootSnapshot, SnapshotManifest, TreeManifest,
    payload_blob_ref, payload_blob_ref_mut, payload_large_ref, payload_large_ref_mut,
};
use majutsu_crypto::EncryptionMode;
use majutsu_pack::{PackEntry, PackIndex, PackTier};
use majutsu_restore::{RestoreChangeStats, RestorePathState, count_restore_changes};
use notify::{Config as NotifyConfig, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, DATE, ETAG, HOST, RANGE};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Deserializer, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;
use uuid::Uuid;
use walkdir::WalkDir;

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

const DEFAULT_LARGE_MIN_SIZE: u64 = 64 * 1024 * 1024;
const DEFAULT_LARGE_BINARY_MIN_SIZE: u64 = 16 * 1024 * 1024;
const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const MIN_MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
const DEFAULT_MULTIPART_THRESHOLD: usize = 64 * 1024 * 1024;
const CHUNK_INDEX_SHARD_KEY: &str = "indexes/chunk-index/shard-0000.cbor.zst.enc";

#[derive(Parser)]
#[command(
    name = "mj",
    version,
    about = "Host-level multi-root snapshot history agent"
)]
struct Cli {
    #[arg(long, global = true)]
    home: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init(InitArgs),
    Root {
        #[command(subcommand)]
        command: RootCommand,
    },
    Snapshot(SnapshotArgs),
    Status,
    Log(LogArgs),
    Op {
        #[command(subcommand)]
        command: OpCommand,
    },
    Diff(DiffArgs),
    Restore(RestoreTopArgs),
    Mount(MountArgs),
    Unmount(UnmountArgs),
    Hydrate(HydrateArgs),
    Large {
        #[command(subcommand)]
        command: LargeCommand,
    },
    Sync {
        #[command(subcommand)]
        command: Option<SyncCommand>,
    },
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
    Lifecycle {
        #[command(subcommand)]
        command: LifecycleCommand,
    },
    Clone(CloneArgs),
    Watch(WatchArgs),
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
    Pack(PackArgs),
    Prune(PruneArgs),
    Gc,
    Fsck,
}

#[derive(Args)]
struct InitArgs {
    #[arg(long)]
    remote: Option<String>,
    #[arg(long)]
    host_name: Option<String>,
    #[arg(long, default_value_t = false)]
    encrypt: bool,
}

#[derive(Subcommand)]
enum RootCommand {
    Add(RootAddArgs),
    Set(RootSetArgs),
    List,
    Remove { id: String },
    Pause { id: String },
    Resume { id: String },
    MarkDeleted { id: String },
}

#[derive(Args)]
struct RootAddArgs {
    id: String,
    path: PathBuf,
    #[arg(long)]
    name: Option<String>,
    #[arg(long = "exclude")]
    exclude: Vec<String>,
    #[arg(long = "include")]
    include: Vec<String>,
    #[arg(long, default_value_t = false)]
    follow_symlinks: bool,
    #[arg(long, default_value_t = false)]
    require_mount: bool,
    #[arg(long, default_value = "default")]
    snapshot_mode: String,
    #[arg(long)]
    pre_snapshot: Option<String>,
    #[arg(long)]
    post_snapshot: Option<String>,
    #[arg(long)]
    snapshot_source: Option<PathBuf>,
    #[arg(long)]
    application_plugin: Option<String>,
    #[arg(long)]
    large_min_size: Option<u64>,
    #[arg(long)]
    large_binary_min_size: Option<u64>,
    #[arg(long)]
    large_chunk_size: Option<usize>,
    #[arg(long)]
    large_chunking: Option<String>,
    #[arg(long = "large-always")]
    large_always: Vec<String>,
    #[arg(long = "large-never")]
    large_never: Vec<String>,
}

#[derive(Args)]
struct RootSetArgs {
    id: String,
    #[arg(long)]
    path: Option<PathBuf>,
    #[arg(long)]
    name: Option<String>,
    #[arg(long = "include")]
    include: Vec<String>,
    #[arg(long, default_value_t = false)]
    clear_include: bool,
    #[arg(long = "exclude")]
    exclude: Vec<String>,
    #[arg(long, default_value_t = false)]
    clear_exclude: bool,
    #[arg(long, default_value_t = false)]
    follow_symlinks: bool,
    #[arg(long, default_value_t = false)]
    no_follow_symlinks: bool,
    #[arg(long, default_value_t = false)]
    require_mount: bool,
    #[arg(long, default_value_t = false)]
    no_require_mount: bool,
    #[arg(long)]
    snapshot_mode: Option<String>,
    #[arg(long)]
    pre_snapshot: Option<String>,
    #[arg(long, default_value_t = false)]
    clear_pre_snapshot: bool,
    #[arg(long)]
    post_snapshot: Option<String>,
    #[arg(long, default_value_t = false)]
    clear_post_snapshot: bool,
    #[arg(long)]
    snapshot_source: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    clear_snapshot_source: bool,
    #[arg(long)]
    application_plugin: Option<String>,
    #[arg(long, default_value_t = false)]
    clear_application_plugin: bool,
    #[arg(long)]
    large_min_size: Option<u64>,
    #[arg(long)]
    large_binary_min_size: Option<u64>,
    #[arg(long)]
    large_chunk_size: Option<usize>,
    #[arg(long)]
    large_chunking: Option<String>,
    #[arg(long = "large-always")]
    large_always: Vec<String>,
    #[arg(long = "large-never")]
    large_never: Vec<String>,
    #[arg(long, default_value_t = false)]
    clear_large_policy: bool,
    #[arg(long, default_value_t = false)]
    clear_large_always: bool,
    #[arg(long, default_value_t = false)]
    clear_large_never: bool,
}

#[derive(Args)]
struct SnapshotArgs {
    #[arg(long)]
    message: Option<String>,
}

#[derive(Args)]
struct LogArgs {
    #[arg(long, default_value_t = 20)]
    limit: usize,
    #[arg(long)]
    root: Option<String>,
}

#[derive(Args)]
struct DiffArgs {
    from: Option<String>,
    to: Option<String>,
    #[arg(long)]
    at: Option<String>,
    #[arg(long)]
    root: Option<String>,
}

#[derive(Subcommand)]
enum RestoreCommand {
    Plan(RestoreArgs),
    Apply(RestoreArgs),
    Prepare(RestoreArgs),
    Resume { job_id: String },
}

#[derive(Args)]
struct RestoreTopArgs {
    #[command(subcommand)]
    command: Option<RestoreCommand>,
    #[command(flatten)]
    args: RestoreArgs,
}

#[derive(Args, Clone)]
struct RestoreArgs {
    #[arg(long)]
    snapshot: Option<String>,
    #[arg(long)]
    at: Option<String>,
    #[arg(long)]
    root: Option<String>,
    #[arg(long)]
    path: Option<PathBuf>,
    #[arg(long)]
    to: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    force: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    check_conflicts: bool,
}

#[derive(Args, Clone)]
struct MountArgs {
    #[arg(long)]
    snapshot: Option<String>,
    #[arg(long)]
    at: Option<String>,
    #[arg(long)]
    root: Option<String>,
    #[arg(long)]
    path: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    hydrate_large: bool,
    #[arg(long, default_value = "materialized")]
    backend: String,
    mountpoint: PathBuf,
}

#[derive(Args)]
struct UnmountArgs {
    mountpoint: PathBuf,
}

#[derive(Args)]
struct HydrateArgs {
    view: PathBuf,
    #[arg(long)]
    root: Option<String>,
    #[arg(long)]
    path: Option<PathBuf>,
}

#[derive(Subcommand)]
enum LargeCommand {
    List,
    Stat,
    Verify,
    Pin(LargePinArgs),
    Unpin(LargeUnpinArgs),
}

#[derive(Subcommand)]
enum SyncCommand {
    Status,
}

#[derive(Args)]
struct LargePinArgs {
    #[arg(long)]
    root: Option<String>,
    #[arg(long)]
    since: Option<String>,
}

#[derive(Args)]
struct LargeUnpinArgs {
    #[arg(long)]
    older_than: Option<String>,
}

#[derive(Subcommand)]
enum RemoteCommand {
    Check,
    Fsck,
    Capabilities,
    Hosts,
    Host { id: String },
}

#[derive(Subcommand)]
enum OpCommand {
    Log(LogArgs),
    Show { op_id: String },
    Restore { op_id: String },
}

#[derive(Subcommand)]
enum LifecycleCommand {
    Policy {
        #[arg(long, default_value = "gcs")]
        provider: String,
    },
}

#[derive(Args)]
struct CloneArgs {
    #[arg(long)]
    remote: String,
    #[arg(long)]
    host: Option<String>,
}

#[derive(Args)]
struct WatchArgs {
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    foreground: bool,
    #[arg(long)]
    interval_secs: Option<u64>,
    #[arg(long)]
    debounce_ms: Option<u64>,
    #[arg(long)]
    settle_ms: Option<u64>,
    #[arg(long)]
    periodic_rescan_secs: Option<u64>,
    #[arg(long)]
    backend: Option<String>,
    #[arg(long, default_value_t = false)]
    once: bool,
}

#[derive(Clone)]
struct ResolvedWatchArgs {
    foreground: bool,
    mode: String,
    interval_secs: u64,
    debounce_ms: u64,
    settle_ms: u64,
    periodic_rescan_secs: u64,
    backend: String,
    once: bool,
}

#[derive(Subcommand)]
enum DaemonCommand {
    Start {
        #[arg(long)]
        interval_secs: Option<u64>,
        #[arg(long)]
        settle_ms: Option<u64>,
        #[arg(long)]
        periodic_rescan_secs: Option<u64>,
    },
    Stop,
    Status,
}

#[derive(Subcommand)]
enum KeyCommand {
    Export,
    Import {
        hex: String,
    },
    Rotate {
        #[arg(long)]
        new_key: Option<String>,
    },
}

#[derive(Args)]
struct PruneArgs {
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    dry_run: bool,
    #[arg(long, default_value_t = 90)]
    keep_daily: u32,
    #[arg(long, default_value_t = 36)]
    keep_monthly: u32,
}

#[derive(Args)]
struct PackArgs {
    #[arg(long, default_value_t = false)]
    compact: bool,
}

#[derive(Debug)]
struct Paths {
    home: PathBuf,
    db: PathBuf,
    config: PathBuf,
    host: PathBuf,
    objects: PathBuf,
    trees: PathBuf,
    large_chunks: PathBuf,
    large_manifests: PathBuf,
    packs: PathBuf,
    pack_indexes: PathBuf,
    logs: PathBuf,
    runtime: PathBuf,
    daemon_pid: PathBuf,
    daemon_lock: PathBuf,
    snapshot_lock: PathBuf,
    upload_queue: PathBuf,
    event_queue: PathBuf,
    master_key: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    host: HostConfig,
    remote: Option<RemoteConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    roots: Vec<ConfigRoot>,
    large: LargeConfig,
    #[serde(default)]
    pack: PackConfig,
    #[serde(default)]
    watch: WatchConfig,
    #[serde(default)]
    security: SecurityConfig,
    #[serde(default)]
    tiering: TieringConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct HostConfig {
    id: String,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RemoteConfig {
    #[serde(default)]
    url: Option<String>,
    #[serde(default, rename = "type")]
    remote_type: Option<String>,
    #[serde(default)]
    path: Option<PathBuf>,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    signature_version: Option<String>,
}

impl RemoteConfig {
    fn from_url(url: String) -> Self {
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

    fn url(&self) -> Result<String> {
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
struct SecurityConfig {
    #[serde(default)]
    encryption: String,
    #[serde(default = "default_security_key_id")]
    key_id: String,
    #[serde(default = "default_security_hash")]
    hash: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TieringConfig {
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_tiering_rules")]
    rules: Vec<TieringRule>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TieringRule {
    name: String,
    prefix: String,
    #[serde(default)]
    after: Option<String>,
    #[serde(default, alias = "transition_to", alias = "keep")]
    storage: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MetadataExport {
    version: u32,
    exported_at: DateTime<Utc>,
    config: Config,
    roots: Vec<RootConfig>,
    snapshots: Vec<SnapshotExport>,
    operations: Vec<OperationExport>,
    refs: BTreeMap<String, String>,
    blobs: Vec<BlobExport>,
    large_objects: Vec<LargeObjectExport>,
    chunks: Vec<ChunkExport>,
    packs: Vec<PackExport>,
    #[serde(default)]
    large_pins: Vec<LargePinExport>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PackExport {
    pack_id: String,
    pack_key: String,
    index_key: String,
    object_count: usize,
    size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct UploadQueueItem {
    id: String,
    key: String,
    source: Option<String>,
    inline: Option<Vec<u8>>,
    created_at: DateTime<Utc>,
    attempts: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct EventJournalRecord {
    event_id: String,
    kind: String,
    observed_at: DateTime<Utc>,
    detail: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RestoreQueueItem {
    id: String,
    snapshot_id: String,
    root: Option<String>,
    path: Option<String>,
    target: String,
    required_objects: Vec<String>,
    archived_objects: Vec<String>,
    #[serde(default)]
    missing_objects: Vec<String>,
    #[serde(default)]
    archive_requested_objects: Vec<String>,
    #[serde(default)]
    force: bool,
    #[serde(default = "default_true")]
    check_conflicts: bool,
    created_at: DateTime<Utc>,
    status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct LazyMountEntry {
    version: u32,
    snapshot_id: String,
    root_id: String,
    path: String,
    size: u64,
    manifest_key: String,
    chunk_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct MountViewMetadata {
    version: u32,
    snapshot_id: String,
    created_at: DateTime<Utc>,
    hydrate_large: bool,
    files: usize,
    lazy_large_files: usize,
    hydrated_large_files: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RemoteHostSummary {
    id: String,
    name: String,
    last_synced_at: DateTime<Utc>,
    current_snapshot: Option<String>,
    metadata_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoteHostIndex {
    version: u32,
    updated_at: DateTime<Utc>,
    hosts: Vec<RemoteHostSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoteCapabilities {
    lifecycle_rules: bool,
    object_tags: bool,
    storage_class_on_put: bool,
    restore_archived_object: bool,
    multipart_upload: bool,
    range_get: bool,
    conditional_put: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotExport {
    id: String,
    parent_id: Option<String>,
    op_id: String,
    created_at: String,
    manifest_key: String,
    manifest_json: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct OperationExport {
    id: String,
    parent_op: Option<String>,
    kind: String,
    actor: String,
    status: String,
    before_snapshot: Option<String>,
    after_snapshot: Option<String>,
    created_at: String,
    message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct BlobExport {
    oid: String,
    size: u64,
    object_key: String,
    #[serde(default)]
    pack_id: Option<String>,
    #[serde(default)]
    pack_offset: Option<u64>,
    #[serde(default)]
    pack_len: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LargeObjectExport {
    oid: String,
    size: u64,
    chunk_count: usize,
    manifest_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkExport {
    oid: String,
    size: u64,
    object_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkIndexShard {
    version: u32,
    shard: String,
    updated_at: DateTime<Utc>,
    chunks: Vec<ChunkIndexEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkIndexEntry {
    oid: String,
    size: u64,
    object_key: String,
    canonical_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GcMarkExport {
    version: u32,
    host_id: String,
    marked_at: DateTime<Utc>,
    current_snapshot: Option<String>,
    object_keys: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GcTombstoneExport {
    version: u32,
    host_id: String,
    deleted_at: DateTime<Utc>,
    key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct LargePinExport {
    oid: String,
    pinned_at: String,
    reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LargeConfig {
    enabled: bool,
    #[serde(
        default = "default_large_min_size",
        deserialize_with = "deserialize_u64_bytes"
    )]
    min_size: u64,
    #[serde(
        default = "default_large_binary_min_size",
        deserialize_with = "deserialize_u64_bytes"
    )]
    binary_min_size: u64,
    #[serde(default = "default_large_chunking")]
    default_chunking: String,
    #[serde(
        default = "default_chunk_size",
        alias = "target_chunk_size",
        deserialize_with = "deserialize_usize_bytes"
    )]
    chunk_size: usize,
    #[serde(default = "default_large_max_parallel_uploads")]
    max_parallel_uploads: usize,
    #[serde(default = "default_true")]
    multipart: bool,
    always: Vec<String>,
    never: Vec<String>,
    #[serde(default)]
    compression: LargeCompressionConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PackConfig {
    #[serde(
        default = "default_small_pack_target",
        deserialize_with = "deserialize_u64_bytes"
    )]
    small_pack_target: u64,
    #[serde(
        default = "default_normal_pack_target",
        deserialize_with = "deserialize_u64_bytes"
    )]
    normal_pack_target: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WatchConfig {
    #[serde(default = "default_watch_backend")]
    backend: String,
    #[serde(default = "default_watch_mode")]
    mode: String,
    #[serde(
        default = "default_watch_debounce_ms",
        deserialize_with = "deserialize_millis"
    )]
    debounce: u64,
    #[serde(
        default = "default_watch_settle_ms",
        deserialize_with = "deserialize_millis"
    )]
    settle: u64,
    #[serde(
        default = "default_watch_periodic_rescan_secs",
        deserialize_with = "deserialize_seconds"
    )]
    periodic_rescan: u64,
    #[serde(
        default = "default_watch_interval_secs",
        deserialize_with = "deserialize_seconds"
    )]
    interval: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ConfigRoot {
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    path: PathBuf,
    #[serde(default = "default_include")]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    follow_symlinks: bool,
    #[serde(default)]
    require_mount: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(default = "default_snapshot_mode")]
    snapshot_mode: String,
    #[serde(default)]
    pre_snapshot: Option<String>,
    #[serde(default)]
    post_snapshot: Option<String>,
    #[serde(default)]
    snapshot_source: Option<PathBuf>,
    #[serde(default)]
    application_plugin: Option<String>,
    #[serde(default)]
    large: Option<RootLargeConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LargeCompressionConfig {
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_large_compression_algorithm")]
    algorithm: String,
    #[serde(default = "default_large_compression_level")]
    level: i32,
    #[serde(
        default = "default_large_compression_sample_bytes",
        deserialize_with = "deserialize_usize_bytes"
    )]
    sample_bytes: usize,
    #[serde(default = "default_large_compression_min_gain_ratio")]
    min_gain_ratio: f64,
    #[serde(default = "default_large_compression_skip_extensions")]
    skip_extensions: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RootConfig {
    id: String,
    name: String,
    path: PathBuf,
    #[serde(default = "default_include")]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    follow_symlinks: bool,
    #[serde(default)]
    require_mount: bool,
    #[serde(default = "default_root_status")]
    status: String,
    #[serde(default = "default_snapshot_mode")]
    snapshot_mode: String,
    #[serde(default)]
    pre_snapshot: Option<String>,
    #[serde(default)]
    post_snapshot: Option<String>,
    #[serde(default)]
    snapshot_source: Option<PathBuf>,
    #[serde(default)]
    application_plugin: Option<String>,
    #[serde(default)]
    large: Option<RootLargeConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RootLargeConfig {
    #[serde(default, deserialize_with = "deserialize_option_u64_bytes")]
    min_size: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_option_u64_bytes")]
    binary_min_size: Option<u64>,
    #[serde(default)]
    default_chunking: Option<String>,
    #[serde(
        default,
        alias = "target_chunk_size",
        deserialize_with = "deserialize_option_usize_bytes"
    )]
    chunk_size: Option<usize>,
    #[serde(default)]
    always: Vec<String>,
    #[serde(default)]
    never: Vec<String>,
}

#[cfg(unix)]
fn file_mode(meta: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode()
}

#[cfg(not(unix))]
fn file_mode(_: &fs::Metadata) -> u32 {
    0
}

#[cfg(unix)]
fn special_file_kind(meta: &fs::Metadata) -> Option<String> {
    use std::os::unix::fs::FileTypeExt;
    let file_type = meta.file_type();
    if file_type.is_fifo() {
        Some("fifo".into())
    } else if file_type.is_socket() {
        Some("socket".into())
    } else if file_type.is_block_device() {
        Some("block-device".into())
    } else if file_type.is_char_device() {
        Some("char-device".into())
    } else {
        None
    }
}

#[cfg(not(unix))]
fn special_file_kind(_: &fs::Metadata) -> Option<String> {
    None
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths = resolve_paths(cli.home)?;
    match cli.command {
        Command::Init(args) => init(&paths, args),
        Command::Root { command } => root_cmd(&paths, command),
        Command::Snapshot(args) => snapshot(&paths, args),
        Command::Status => status(&paths),
        Command::Log(args) => log_ops(&paths, args),
        Command::Op { command } => op_cmd(&paths, command),
        Command::Diff(args) => diff_cmd(&paths, args),
        Command::Restore(args) => restore_cmd(&paths, args),
        Command::Mount(args) => mount_cmd(&paths, args),
        Command::Unmount(args) => unmount_cmd(&paths, args),
        Command::Hydrate(args) => hydrate_cmd(&paths, args),
        Command::Large { command } => large_cmd(&paths, command),
        Command::Sync { command } => sync_cmd(&paths, command),
        Command::Remote { command } => remote_cmd(&paths, command),
        Command::Lifecycle { command } => lifecycle_cmd(&paths, command),
        Command::Clone(args) => clone_cmd(&paths, args),
        Command::Watch(args) => watch_cmd(&paths, args),
        Command::Daemon { command } => daemon_cmd(&paths, command),
        Command::Key { command } => key_cmd(&paths, command),
        Command::Pack(args) => pack_cmd(&paths, args),
        Command::Prune(args) => prune_cmd(&paths, args),
        Command::Gc => gc_cmd(&paths),
        Command::Fsck => fsck(&paths),
    }
}

fn resolve_paths(home_arg: Option<PathBuf>) -> Result<Paths> {
    let home = if let Some(home) = home_arg {
        home
    } else if let Ok(home) = env::var("MAJUTSU_HOME") {
        PathBuf::from(home)
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
    let value: toml::Value = toml::from_str(&fs::read_to_string(path)?)?;
    let Some(home) = value
        .get("state")
        .and_then(|state| state.get("home"))
        .and_then(|home| home.as_str())
    else {
        return Ok(None);
    };
    if let Some(rest) = home.strip_prefix("~/") {
        if let Some(user_home) = user_home {
            return Ok(Some(user_home.join(rest)));
        }
    }
    Ok(Some(PathBuf::from(home)))
}

fn init(paths: &Paths, args: InitArgs) -> Result<()> {
    create_layout(paths)?;
    let host_name = args
        .host_name
        .or_else(|| hostname_from_env().ok())
        .unwrap_or_else(|| "unknown-host".to_string());
    let config = if paths.config.exists() {
        read_config(paths)?
    } else {
        Config {
            host: HostConfig {
                id: Uuid::new_v4().to_string(),
                name: host_name,
            },
            remote: args.remote.map(RemoteConfig::from_url),
            roots: Vec::new(),
            large: LargeConfig {
                enabled: true,
                min_size: DEFAULT_LARGE_MIN_SIZE,
                binary_min_size: DEFAULT_LARGE_BINARY_MIN_SIZE,
                default_chunking: "fixed".into(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                max_parallel_uploads: default_large_max_parallel_uploads(),
                multipart: true,
                always: vec![
                    "*.mp4".into(),
                    "*.mov".into(),
                    "*.mkv".into(),
                    "*.zip".into(),
                    "*.tar".into(),
                    "*.tar.zst".into(),
                    "*.parquet".into(),
                    "*.sqlite".into(),
                    "*.db".into(),
                    "*.vmdk".into(),
                    "*.qcow2".into(),
                    "*.iso".into(),
                    "*.psd".into(),
                    "*.blend".into(),
                ],
                never: vec![
                    "*.rs".into(),
                    "*.toml".into(),
                    "*.yaml".into(),
                    "*.json".into(),
                    "*.md".into(),
                ],
                compression: LargeCompressionConfig::default(),
            },
            pack: PackConfig::default(),
            watch: WatchConfig::default(),
            security: SecurityConfig {
                encryption: if args.encrypt {
                    "age".into()
                } else {
                    "none".into()
                },
                key_id: default_security_key_id(),
                hash: default_security_hash(),
            },
            tiering: TieringConfig::default(),
        }
    };
    write_config(paths, &config)?;
    fs::write(&paths.host, toml::to_string_pretty(&config.host)?)?;
    if encryption_enabled(&config.security)? && !paths.master_key.exists() {
        write_master_key(paths, &random_key_hex()?)?;
    }
    if config.security.encryption == "age" {
        majutsu_crypto::ensure_age_keyring(&recipients_path(paths))?;
    }
    let conn = open_db(paths)?;
    migrate(&conn)?;
    record_op(&conn, "init", None, None, Some("initialized majutsu home"))?;
    println!("initialized {}", paths.home.display());
    println!("host {} {}", config.host.name, config.host.id);
    Ok(())
}

fn root_cmd(paths: &Paths, command: RootCommand) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    match command {
        RootCommand::Add(args) => {
            let path = absolutize(&args.path)?;
            if !path.exists() {
                bail!("root path does not exist: {}", path.display());
            }
            if root_by_id_optional(&conn, &args.id)?.is_some() {
                bail!(
                    "root already exists: {}; use `mj root set` to change it",
                    args.id
                );
            }
            validate_snapshot_mode(&args.snapshot_mode)?;
            if let Some(chunking) = &args.large_chunking {
                validate_large_chunking(chunking)?;
            }
            let snapshot_source = args
                .snapshot_source
                .as_deref()
                .map(absolutize)
                .transpose()?;
            if snapshot_source.is_some() && args.snapshot_mode != "transactional" {
                bail!("--snapshot-source requires --snapshot-mode transactional");
            }
            let large = root_large_override(&args);
            let root = RootConfig {
                name: args.name.unwrap_or_else(|| args.id.clone()),
                id: args.id,
                path,
                include: if args.include.is_empty() {
                    default_include()
                } else {
                    args.include
                },
                exclude: args.exclude,
                follow_symlinks: args.follow_symlinks,
                require_mount: args.require_mount,
                status: "active".into(),
                snapshot_mode: args.snapshot_mode,
                pre_snapshot: args.pre_snapshot,
                post_snapshot: args.post_snapshot,
                snapshot_source,
                application_plugin: args.application_plugin,
                large,
            };
            conn.execute(
                "insert into roots(id, data_json) values (?1, ?2)",
                params![root.id, serde_json::to_string(&root)?],
            )?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-added", None, None, Some(&root.id))?;
            println!("added root {} -> {}", root.id, root.path.display());
        }
        RootCommand::Set(args) => {
            let mut root = root_by_id(&conn, &args.id)?;
            if let Some(path) = &args.path {
                let path = absolutize(path)?;
                if !path.exists() {
                    bail!("root path does not exist: {}", path.display());
                }
                root.path = path;
            }
            if let Some(name) = &args.name {
                root.name = name.clone();
            }
            if args.clear_include {
                root.include = default_include();
            }
            if !args.include.is_empty() {
                root.include = args.include.clone();
            }
            if args.clear_exclude {
                root.exclude.clear();
            }
            root.exclude.extend(args.exclude.clone());
            if args.follow_symlinks && args.no_follow_symlinks {
                bail!("use either --follow-symlinks or --no-follow-symlinks, not both");
            }
            if args.follow_symlinks {
                root.follow_symlinks = true;
            }
            if args.no_follow_symlinks {
                root.follow_symlinks = false;
            }
            if args.require_mount && args.no_require_mount {
                bail!("use either --require-mount or --no-require-mount, not both");
            }
            if args.require_mount {
                root.require_mount = true;
            }
            if args.no_require_mount {
                root.require_mount = false;
            }
            if let Some(mode) = &args.snapshot_mode {
                validate_snapshot_mode(&mode)?;
                root.snapshot_mode = mode.clone();
            }
            if args.clear_pre_snapshot {
                root.pre_snapshot = None;
            }
            if let Some(pre_snapshot) = &args.pre_snapshot {
                root.pre_snapshot = Some(pre_snapshot.clone());
            }
            if args.clear_post_snapshot {
                root.post_snapshot = None;
            }
            if let Some(post_snapshot) = &args.post_snapshot {
                root.post_snapshot = Some(post_snapshot.clone());
            }
            if args.clear_snapshot_source {
                root.snapshot_source = None;
            }
            if let Some(snapshot_source) = &args.snapshot_source {
                root.snapshot_source = Some(absolutize(snapshot_source)?);
            }
            if args.clear_application_plugin {
                root.application_plugin = None;
            }
            if let Some(application_plugin) = &args.application_plugin {
                root.application_plugin = Some(application_plugin.clone());
            }
            if root.snapshot_source.is_some() && root.snapshot_mode != "transactional" {
                bail!("--snapshot-source requires snapshot_mode transactional");
            }
            apply_root_large_set(&mut root, &args)?;
            save_root(&conn, &root)?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "config-change", None, None, Some(&root.id))?;
            println!("updated root {} -> {}", root.id, root.path.display());
        }
        RootCommand::List => {
            for root in roots(&conn)? {
                println!(
                    "{}\t{}\t{}\t{}",
                    root.id,
                    root.status,
                    root.name,
                    root.path.display()
                );
            }
        }
        RootCommand::Remove { id } => {
            let _ = root_by_id(&conn, &id)?;
            conn.execute("delete from roots where id=?1", params![id])?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-removed", None, None, Some(&id))?;
            println!("removed root {id}");
        }
        RootCommand::Pause { id } => {
            update_root_status(&conn, &id, "paused")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-paused", None, None, Some(&id))?;
            println!("paused root {id}");
        }
        RootCommand::Resume { id } => {
            update_root_status(&conn, &id, "active")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-resumed", None, None, Some(&id))?;
            println!("resumed root {id}");
        }
        RootCommand::MarkDeleted { id } => {
            update_root_status(&conn, &id, "deleted")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(&conn, "root-mark-deleted", None, None, Some(&id))?;
            println!("marked root {id} deleted");
        }
    }
    Ok(())
}

fn snapshot(paths: &Paths, args: SnapshotArgs) -> Result<()> {
    ensure_ready(paths)?;
    let _lock = acquire_process_lock(&paths.snapshot_lock, "snapshot")?;
    record_event(
        paths,
        "snapshot-start",
        args.message.as_deref().unwrap_or("manual"),
    )?;
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let parent = current_snapshot(&conn)?;
    let parent_manifest = parent
        .as_deref()
        .map(|id| load_snapshot_by_id(&conn, id))
        .transpose()?;
    let op_id = new_id("op");
    let snapshot_id = new_id("snap");
    let mut by_root = BTreeMap::new();
    let mut root_trees = BTreeMap::new();
    let mut total_files = 0usize;
    let mut large_files = 0usize;
    for root in roots(&conn)? {
        if root.status != "active" {
            eprintln!("root {}, skipped: status={}", root.id, root.status);
            record_event(
                paths,
                "root-skipped",
                &format!("{} status={}", root.id, root.status),
            )?;
            if root.status != "deleted" {
                carry_forward_root_snapshot(
                    parent_manifest.as_ref(),
                    &root.id,
                    &mut root_trees,
                    &mut by_root,
                );
            }
            continue;
        }
        if !root.path.exists() {
            update_root_status(&conn, &root.id, "missing")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(
                &conn,
                "root-missing",
                parent.as_deref(),
                parent.as_deref(),
                Some(&root.id),
            )?;
            eprintln!("root missing, skipped: {} {}", root.id, root.path.display());
            record_event(
                paths,
                "root-missing",
                &format!("{} {}", root.id, root.path.display()),
            )?;
            carry_forward_root_snapshot(
                parent_manifest.as_ref(),
                &root.id,
                &mut root_trees,
                &mut by_root,
            );
            continue;
        }
        if root.require_mount && !is_mount_point(&root.path) {
            update_root_status(&conn, &root.id, "unmounted")?;
            sync_roots_to_config(paths, &conn)?;
            record_op(
                &conn,
                "root-unmounted",
                parent.as_deref(),
                parent.as_deref(),
                Some(&root.id),
            )?;
            eprintln!(
                "root unmounted, skipped: {} {}",
                root.id,
                root.path.display()
            );
            record_event(
                paths,
                "root-unmounted",
                &format!("{} {}", root.id, root.path.display()),
            )?;
            carry_forward_root_snapshot(
                parent_manifest.as_ref(),
                &root.id,
                &mut root_trees,
                &mut by_root,
            );
            continue;
        }
        if let Err(err) = run_pre_snapshot_hook(paths, &root) {
            record_snapshot_failure(
                &conn,
                &op_id,
                snapshot_operation_kind(args.message.as_deref()),
                parent.as_deref(),
                &root.id,
                &err,
            )?;
            return Err(err);
        }
        let scan_root_config = snapshot_scan_root(paths, &root)?;
        let records_result = scan_root(paths, &config, &scan_root_config);
        let post_result = run_post_snapshot_hook(paths, &root);
        let records = match records_result {
            Ok(records) => records,
            Err(err) if is_permission_denied_error(&err) => {
                update_root_status(&conn, &root.id, "permission-denied")?;
                sync_roots_to_config(paths, &conn)?;
                record_op(
                    &conn,
                    "root-permission-denied",
                    parent.as_deref(),
                    parent.as_deref(),
                    Some(&root.id),
                )?;
                eprintln!(
                    "root permission-denied, skipped: {} {}",
                    root.id,
                    root.path.display()
                );
                record_event(
                    paths,
                    "root-permission-denied",
                    &format!("{} {}", root.id, root.path.display()),
                )?;
                carry_forward_root_snapshot(
                    parent_manifest.as_ref(),
                    &root.id,
                    &mut root_trees,
                    &mut by_root,
                );
                continue;
            }
            Err(err) => {
                record_snapshot_failure(
                    &conn,
                    &op_id,
                    snapshot_operation_kind(args.message.as_deref()),
                    parent.as_deref(),
                    &root.id,
                    &err,
                )?;
                return Err(err);
            }
        };
        if let Err(err) = post_result {
            record_snapshot_failure(
                &conn,
                &op_id,
                snapshot_operation_kind(args.message.as_deref()),
                parent.as_deref(),
                &root.id,
                &err,
            )?;
            return Err(err);
        }
        large_files += records
            .iter()
            .filter(|r| payload_large_ref(&r.payload).is_some())
            .count();
        total_files += records
            .iter()
            .filter(|r| !matches!(r.payload, Payload::Directory))
            .count();
        let tree = build_tree_manifest(&root.id, records)?;
        let root_snapshot = if let Some(previous) = parent_manifest
            .as_ref()
            .and_then(|parent| parent.root_trees.get(&root.id))
            .filter(|previous| previous.tree_id == tree.tree_id)
        {
            previous.clone()
        } else {
            let tree_json = serde_json::to_vec_pretty(&tree)?;
            let tree_oid = blake3_hex(&tree_json);
            let tree_key = store_bytes(paths, &paths.trees, &tree_oid, &tree_json)?;
            RootSnapshot {
                tree_id: tree.tree_id.clone(),
                tree_key,
                file_count: tree.entries.len(),
            }
        };
        root_trees.insert(root.id.clone(), root_snapshot);
        by_root.insert(root.id, tree.entries.into_values().collect());
    }
    let manifest = SnapshotManifest {
        snapshot_id: snapshot_id.clone(),
        parent: parent.clone(),
        op_id: op_id.clone(),
        timestamp: Utc::now(),
        root_trees,
        roots: by_root,
    };
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let manifest_oid = blake3_hex(&manifest_json);
    let manifest_key = store_bytes(paths, &paths.objects, &manifest_oid, &manifest_json)?;
    conn.execute(
        "insert into snapshots(id, parent_id, op_id, created_at, manifest_key, manifest_json)
         values (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            snapshot_id,
            parent,
            op_id,
            manifest.timestamp.to_rfc3339(),
            manifest_key,
            String::from_utf8(manifest_json)?
        ],
    )?;
    conn.execute(
        "insert into refs(name, value) values ('current', ?1)
         on conflict(name) do update set value=excluded.value",
        params![manifest.snapshot_id],
    )?;
    record_op_with_id(
        &conn,
        &op_id,
        snapshot_operation_kind(args.message.as_deref()),
        manifest.parent.as_deref(),
        Some(&manifest.snapshot_id),
        args.message.as_deref(),
    )?;
    println!("snapshot {}", manifest.snapshot_id);
    println!("files {total_files}, large {large_files}");
    record_event(paths, "snapshot-finish", &manifest.snapshot_id)?;
    Ok(())
}

fn snapshot_operation_kind(message: Option<&str>) -> &'static str {
    if message
        .map(|message| message.starts_with("watch "))
        .unwrap_or(false)
    {
        "file-events-batch"
    } else {
        "manual-snapshot"
    }
}

fn record_snapshot_failure(
    conn: &Connection,
    op_id: &str,
    kind: &str,
    parent: Option<&str>,
    root_id: &str,
    err: &anyhow::Error,
) -> Result<()> {
    record_op_with_id_and_status(
        conn,
        op_id,
        kind,
        parent,
        parent,
        "failed",
        Some(&format!("snapshot failed for root {root_id}: {err:#}")),
    )
}

fn status(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let roots = roots(&conn)?;
    let current = current_snapshot(&conn)?;
    println!("home {}", paths.home.display());
    println!("roots {}", roots.len());
    for root in roots {
        let state = if root.status == "active" && !root.path.exists() {
            "missing"
        } else {
            root.status.as_str()
        };
        println!("  {}\t{}\t{}", root.id, state, root.path.display());
    }
    println!("current {}", current.unwrap_or_else(|| "(none)".into()));
    Ok(())
}

fn log_ops(paths: &Paths, args: LogArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    print_op_log(&conn, &args)
}

fn print_op_log(conn: &Connection, args: &LogArgs) -> Result<()> {
    let mut stmt = conn.prepare(
        "select id, kind, before_snapshot, after_snapshot, created_at, message
         from operations order by rowid desc",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;
    let mut printed = 0usize;
    for row in rows {
        let (id, kind, before, after, created, message) = row?;
        if let Some(root) = &args.root {
            let matches_root = message.as_deref() == Some(root)
                || before
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(&conn, snapshot, root).ok())
                    .unwrap_or(false)
                || after
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(&conn, snapshot, root).ok())
                    .unwrap_or(false);
            if !matches_root {
                continue;
            }
        }
        if printed >= args.limit {
            break;
        }
        println!(
            "{id}\t{created}\t{kind}\t{} -> {}\t{}",
            before.unwrap_or_default(),
            after.unwrap_or_default(),
            message.unwrap_or_default()
        );
        printed += 1;
    }
    Ok(())
}

fn op_cmd(paths: &Paths, command: OpCommand) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    match command {
        OpCommand::Log(args) => print_op_log(&conn, &args),
        OpCommand::Show { op_id } => {
            let op = query_operation(&conn, &op_id)?;
            println!("id {}", op.id);
            println!("parent {}", op.parent_op.unwrap_or_else(|| "(none)".into()));
            println!("kind {}", op.kind);
            println!("actor {}", op.actor);
            println!("status {}", op.status);
            println!(
                "before {}",
                op.before_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!(
                "after {}",
                op.after_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!("created_at {}", op.created_at);
            println!("message {}", op.message.unwrap_or_default());
            Ok(())
        }
        OpCommand::Restore { op_id } => {
            let op = query_operation(&conn, &op_id)?;
            let before = current_snapshot(&conn)?;
            let snapshot = op
                .after_snapshot
                .or(op.before_snapshot)
                .ok_or_else(|| anyhow!("operation has no snapshot to restore: {op_id}"))?;
            conn.execute(
                "insert into refs(name, value) values ('current', ?1)
                 on conflict(name) do update set value=excluded.value",
                params![snapshot],
            )?;
            record_op(
                &conn,
                "op-restore",
                before.as_deref(),
                Some(&snapshot),
                Some(&op_id),
            )?;
            println!("current {}", snapshot);
            Ok(())
        }
    }
}

fn lifecycle_cmd(paths: &Paths, command: LifecycleCommand) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    match command {
        LifecycleCommand::Policy { provider } => match provider.as_str() {
            "gcs" => {
                let policy = majutsu_policy::gcs_lifecycle_policy(&policy_config(&config.tiering))?;
                println!("{}", serde_json::to_string_pretty(&policy)?);
            }
            "s3" | "aws" => {
                let policy = majutsu_policy::s3_lifecycle_policy(&policy_config(&config.tiering))?;
                println!("{}", serde_json::to_string_pretty(&policy)?);
            }
            other => bail!("unsupported lifecycle provider: {other}"),
        },
    }
    Ok(())
}

fn policy_config(tiering: &TieringConfig) -> majutsu_policy::PolicyConfig {
    majutsu_policy::PolicyConfig {
        enabled: tiering.enabled,
        rules: tiering
            .rules
            .iter()
            .map(|rule| majutsu_policy::PolicyRule {
                name: rule.name.clone(),
                prefix: rule.prefix.clone(),
                after: rule.after.clone(),
                storage: rule.storage.clone(),
            })
            .collect(),
    }
}

fn diff_cmd(paths: &Paths, args: DiffArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    if args.at.is_some() && args.from.is_some() {
        bail!("use either a positional from snapshot or --at, not both");
    }
    let to_id = args
        .to
        .clone()
        .or_else(|| current_snapshot(&conn).ok().flatten())
        .ok_or_else(|| anyhow!("no target snapshot"))?;
    let to = load_snapshot_by_id(&conn, &to_id)?;
    let from_id = if let Some(at) = &args.at {
        Some(snapshot_id_at(&conn, at)?)
    } else {
        args.from.or_else(|| to.parent.clone())
    };
    let from = if let Some(from_id) = from_id {
        Some(load_snapshot_by_id(&conn, &from_id)?)
    } else {
        None
    };
    let from_files = from
        .as_ref()
        .map(snapshot_file_map)
        .transpose()?
        .unwrap_or_default();
    let to_files = snapshot_file_map(&to)?;
    let mut paths_all = from_files.keys().cloned().collect::<Vec<_>>();
    paths_all.extend(
        to_files
            .keys()
            .filter(|key| !from_files.contains_key(*key))
            .cloned(),
    );
    paths_all.sort();
    for key in paths_all {
        if let Some(root) = &args.root {
            if !key.starts_with(&format!("{root}/")) {
                continue;
            }
        }
        match (from_files.get(&key), to_files.get(&key)) {
            (None, Some(_)) => println!("A\t{key}"),
            (Some(_), None) => println!("D\t{key}"),
            (Some(a), Some(b)) if serde_json::to_value(a)? != serde_json::to_value(b)? => {
                println!("M\t{key}");
            }
            _ => {}
        }
    }
    Ok(())
}

fn restore_cmd(paths: &Paths, top_args: RestoreTopArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let command = top_args
        .command
        .unwrap_or_else(|| RestoreCommand::Apply(top_args.args));
    match command {
        RestoreCommand::Plan(args) => {
            let plan = build_restore_plan(paths, &conn, &args)?;
            print_restore_plan(paths, &conn, &plan)?;
            if args.check_conflicts {
                let conflicts = restore_conflicts(paths, &conn, &plan)?;
                print_restore_conflicts(&conflicts);
            }
        }
        RestoreCommand::Apply(args) => {
            let plan = build_restore_plan(paths, &conn, &args)?;
            apply_restore_plan(paths, &plan, args.force, args.check_conflicts)?;
            let after = plan.snapshot.snapshot_id.as_str();
            record_op(
                &conn,
                "restore",
                None,
                Some(after),
                Some(&format!("to {}", restore_target_label(&plan))),
            )?;
            print_restore_plan(paths, &conn, &plan)?;
            println!("restored to {}", restore_target_label(&plan));
        }
        RestoreCommand::Prepare(args) => {
            let plan = build_restore_plan(paths, &conn, &args)?;
            let mut job = build_restore_job(paths, &plan, &args)?;
            request_archive_restore_for_job(paths, &mut job)?;
            write_restore_job(paths, &job)?;
            record_op(
                &conn,
                "restore-prepare",
                None,
                Some(&plan.snapshot.snapshot_id),
                Some(&job.id),
            )?;
            println!("restore_job {}", job.id);
            println!("snapshot {}", job.snapshot_id);
            println!("required_objects {}", job.required_objects.len());
            println!("archived_objects {}", job.archived_objects.len());
            println!("missing_objects {}", job.missing_objects.len());
            println!(
                "archive_requested_objects {}",
                job.archive_requested_objects.len()
            );
        }
        RestoreCommand::Resume { job_id } => {
            let mut job = read_restore_job(paths, &job_id)?;
            ensure_restore_job_resumable(&job)?;
            if !job.missing_objects.is_empty() {
                bail!(
                    "restore job {} has missing objects: {}",
                    job.id,
                    job.missing_objects.len()
                );
            }
            hydrate_restore_job_objects(paths, &mut job)?;
            if !job.archived_objects.is_empty() {
                bail!(
                    "restore job {} still has archived objects pending: {}",
                    job.id,
                    job.archived_objects.len()
                );
            }
            let args = RestoreArgs {
                snapshot: Some(job.snapshot_id.clone()),
                at: None,
                root: job.root.clone(),
                path: job.path.as_ref().map(PathBuf::from),
                to: if job.target == "original-roots" {
                    None
                } else {
                    Some(PathBuf::from(&job.target))
                },
                force: job.force,
                check_conflicts: job.check_conflicts,
            };
            let plan = build_restore_plan(paths, &conn, &args)?;
            apply_restore_plan(paths, &plan, job.force, job.check_conflicts)?;
            mark_restore_job_done(paths, &job.id)?;
            record_op(
                &conn,
                "restore-resume",
                None,
                Some(&plan.snapshot.snapshot_id),
                Some(&job.id),
            )?;
            println!("resumed {}", job.id);
            println!("restored to {}", restore_target_label(&plan));
        }
    }
    Ok(())
}

fn mount_cmd(paths: &Paths, args: MountArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let restore_args = RestoreArgs {
        snapshot: args.snapshot.clone(),
        at: args.at.clone(),
        root: args.root.clone(),
        path: args.path.clone(),
        to: Some(args.mountpoint.clone()),
        force: true,
        check_conflicts: false,
    };
    let plan = build_restore_plan(paths, &conn, &restore_args)?;
    if args.backend == "fuse" {
        return mount_fuse_cmd(paths, &conn, &plan);
    }
    if args.backend != "materialized" {
        bail!("mount backend must be materialized or fuse");
    }
    let mountpoint = plan
        .to
        .as_ref()
        .ok_or_else(|| anyhow!("mount requires a target directory"))?;
    prepare_mountpoint(mountpoint)?;
    let lazy_root = mountpoint.join(".majutsu-lazy");
    let mut lazy_files = 0usize;
    let mut hydrated_large = 0usize;
    let mut directory_metadata = Vec::new();
    for record in &plan.files {
        let dest = mountpoint.join(&record.root_id).join(&record.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        match &record.payload {
            Payload::Directory => {
                prepare_directory_restore_destination(&dest, false)?;
                fs::create_dir_all(&dest)?;
                directory_metadata.push((dest, record));
            }
            Payload::Special { special_kind } => {
                restore_special_file(&dest, record, special_kind, true)?;
            }
            Payload::Symlink { target } => {
                #[cfg(unix)]
                std::os::unix::fs::symlink(target, &dest)?;
                #[cfg(not(unix))]
                fs::write(&dest, target)?;
            }
            payload => {
                if let Some((oid, object_key)) = payload_blob_ref(payload) {
                    write_atomic(&dest, &read_blob_payload(paths, &conn, oid, object_key)?)?;
                    apply_file_metadata(&dest, record)?;
                } else if let Some((_, manifest_key, chunk_count)) = payload_large_ref(payload) {
                    if args.hydrate_large {
                        let manifest: LargeManifest =
                            serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                        write_large_chunks_atomic(paths, &dest, &manifest)?;
                        apply_file_metadata(&dest, record)?;
                        hydrated_large += 1;
                    } else {
                        let file = File::create(&dest)?;
                        file.set_len(record.size)?;
                        apply_file_metadata(&dest, record)?;
                        let sidecar = lazy_root
                            .join(&record.root_id)
                            .join(format!("{}.json", record.path));
                        if let Some(parent) = sidecar.parent() {
                            fs::create_dir_all(parent)?;
                        }
                        let entry = LazyMountEntry {
                            version: 1,
                            snapshot_id: plan.snapshot.snapshot_id.clone(),
                            root_id: record.root_id.clone(),
                            path: record.path.clone(),
                            size: record.size,
                            manifest_key: manifest_key.to_string(),
                            chunk_count,
                        };
                        fs::write(sidecar, serde_json::to_vec_pretty(&entry)?)?;
                        lazy_files += 1;
                    }
                }
            }
        }
    }
    for (dest, record) in directory_metadata {
        apply_file_metadata(&dest, record)?;
    }
    record_op(
        &conn,
        "mount",
        None,
        Some(&plan.snapshot.snapshot_id),
        Some(&format!("at {}", mountpoint.display())),
    )?;
    let mount_metadata = MountViewMetadata {
        version: 1,
        snapshot_id: plan.snapshot.snapshot_id.clone(),
        created_at: Utc::now(),
        hydrate_large: args.hydrate_large,
        files: plan.files.len(),
        lazy_large_files: lazy_files,
        hydrated_large_files: hydrated_large,
    };
    fs::write(
        mountpoint.join(".majutsu-mount.json"),
        serde_json::to_vec_pretty(&mount_metadata)?,
    )?;
    println!("mounted snapshot {}", plan.snapshot.snapshot_id);
    println!("target {}", mountpoint.display());
    println!("files {}", plan.files.len());
    println!("lazy_large_files {lazy_files}");
    println!("hydrated_large_files {hydrated_large}");
    Ok(())
}

fn unmount_cmd(paths: &Paths, args: UnmountArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let marker = args.mountpoint.join(".majutsu-mount.json");
    if !marker.exists() && is_mountpoint(&args.mountpoint)? {
        unmount_fuse(&args.mountpoint)?;
        record_op(
            &conn,
            "unmount-fuse",
            None,
            None,
            Some(&format!("from {}", args.mountpoint.display())),
        )?;
        println!("unmounted {}", args.mountpoint.display());
        return Ok(());
    }
    if !marker.exists() {
        bail!(
            "{} is not a majutsu mount view; missing .majutsu-mount.json",
            args.mountpoint.display()
        );
    }
    let metadata: MountViewMetadata = serde_json::from_slice(&fs::read(&marker)?)
        .with_context(|| format!("read mount metadata {}", marker.display()))?;
    fs::remove_dir_all(&args.mountpoint)
        .with_context(|| format!("remove mount view {}", args.mountpoint.display()))?;
    record_op(
        &conn,
        "unmount",
        Some(&metadata.snapshot_id),
        None,
        Some(&format!("from {}", args.mountpoint.display())),
    )?;
    println!("unmounted {}", args.mountpoint.display());
    println!("snapshot {}", metadata.snapshot_id);
    Ok(())
}

fn prepare_mountpoint(mountpoint: &Path) -> Result<()> {
    if !mountpoint.exists() {
        fs::create_dir_all(mountpoint)?;
        return Ok(());
    }
    let meta = fs::symlink_metadata(mountpoint)?;
    if !meta.file_type().is_dir() {
        bail!("mountpoint is not a directory: {}", mountpoint.display());
    }
    if fs::read_dir(mountpoint)?.next().is_some() {
        bail!("mountpoint is not empty: {}", mountpoint.display());
    }
    Ok(())
}

const FUSE_TTL: Duration = Duration::from_secs(1);

#[derive(Clone)]
enum FuseNodeKind {
    Directory { children: BTreeMap<OsString, u64> },
    File { record: FileRecord },
    Symlink { target: String },
}

#[derive(Clone)]
struct FuseNode {
    parent: u64,
    attr: FileAttr,
    kind: FuseNodeKind,
}

struct MajutsuFuseFs {
    paths: Paths,
    nodes: BTreeMap<u64, FuseNode>,
}

impl MajutsuFuseFs {
    fn from_plan(paths: &Paths, plan: &RestorePlan) -> Result<Self> {
        let mut fs = Self {
            paths: Paths {
                home: paths.home.clone(),
                db: paths.db.clone(),
                config: paths.config.clone(),
                host: paths.host.clone(),
                objects: paths.objects.clone(),
                trees: paths.trees.clone(),
                large_chunks: paths.large_chunks.clone(),
                large_manifests: paths.large_manifests.clone(),
                packs: paths.packs.clone(),
                pack_indexes: paths.pack_indexes.clone(),
                logs: paths.logs.clone(),
                runtime: paths.runtime.clone(),
                daemon_pid: paths.daemon_pid.clone(),
                daemon_lock: paths.daemon_lock.clone(),
                snapshot_lock: paths.snapshot_lock.clone(),
                upload_queue: paths.upload_queue.clone(),
                event_queue: paths.event_queue.clone(),
                master_key: paths.master_key.clone(),
            },
            nodes: BTreeMap::new(),
        };
        fs.nodes.insert(
            1,
            FuseNode {
                parent: 1,
                attr: fuse_attr(1, FileType::Directory, 0, 0o755, None),
                kind: FuseNodeKind::Directory {
                    children: BTreeMap::new(),
                },
            },
        );
        for record in &plan.files {
            let parent = fs.ensure_dir_path(1, Path::new(&record.root_id))?;
            let rel = Path::new(&record.path);
            let file_parent = if let Some(parent_path) = rel.parent() {
                if parent_path.as_os_str().is_empty() {
                    parent
                } else {
                    fs.ensure_dir_path(parent, parent_path)?
                }
            } else {
                parent
            };
            let name = rel
                .file_name()
                .ok_or_else(|| anyhow!("invalid snapshot path: {}", record.path))?
                .to_os_string();
            let ino = fs.next_ino();
            let kind = match &record.payload {
                Payload::Directory => FuseNodeKind::Directory {
                    children: BTreeMap::new(),
                },
                Payload::Symlink { target } => FuseNodeKind::Symlink {
                    target: target.clone(),
                },
                Payload::Special { .. } => FuseNodeKind::File {
                    record: record.clone(),
                },
                _ => FuseNodeKind::File {
                    record: record.clone(),
                },
            };
            let file_type = fuse_record_file_type(record, &kind);
            fs.nodes.insert(
                ino,
                FuseNode {
                    parent: file_parent,
                    attr: fuse_attr(
                        ino,
                        file_type,
                        record.size,
                        fuse_file_perm(record.mode, file_type),
                        record.modified,
                    ),
                    kind,
                },
            );
            fs.add_child(file_parent, name, ino)?;
        }
        Ok(fs)
    }

    fn next_ino(&self) -> u64 {
        self.nodes.keys().next_back().copied().unwrap_or(1) + 1
    }

    fn ensure_dir_path(&mut self, start: u64, path: &Path) -> Result<u64> {
        let mut current = start;
        for component in path.components() {
            let name = component.as_os_str().to_os_string();
            if name.is_empty() {
                continue;
            }
            let existing = self.nodes.get(&current).and_then(|node| match &node.kind {
                FuseNodeKind::Directory { children } => children.get(&name).copied(),
                _ => None,
            });
            if let Some(ino) = existing {
                current = ino;
                continue;
            }
            let ino = self.next_ino();
            self.nodes.insert(
                ino,
                FuseNode {
                    parent: current,
                    attr: fuse_attr(ino, FileType::Directory, 0, 0o755, None),
                    kind: FuseNodeKind::Directory {
                        children: BTreeMap::new(),
                    },
                },
            );
            self.add_child(current, name, ino)?;
            current = ino;
        }
        Ok(current)
    }

    fn add_child(&mut self, parent: u64, name: OsString, ino: u64) -> Result<()> {
        let node = self
            .nodes
            .get_mut(&parent)
            .ok_or_else(|| anyhow!("missing parent inode {parent}"))?;
        if let FuseNodeKind::Directory { children } = &mut node.kind {
            children.insert(name, ino);
            Ok(())
        } else {
            bail!("parent inode {parent} is not a directory")
        }
    }

    fn read_file(&self, record: &FileRecord, offset: i64, size: u32) -> Result<Vec<u8>> {
        if offset < 0 {
            return Ok(Vec::new());
        }
        let start = offset as u64;
        if start >= record.size {
            return Ok(Vec::new());
        }
        let end = (start + size as u64).min(record.size);
        if let Some((_, object_key)) = payload_blob_ref(&record.payload) {
            let data = read_object(&self.paths, object_key)?;
            Ok(data[start as usize..end as usize].to_vec())
        } else if let Some((_, manifest_key, _)) = payload_large_ref(&record.payload) {
            let manifest: LargeManifest =
                serde_json::from_slice(&read_object(&self.paths, manifest_key)?)?;
            let mut out = Vec::with_capacity((end - start) as usize);
            for chunk in manifest.chunks {
                let chunk_start = chunk.offset;
                let chunk_end = chunk.offset + chunk.len;
                if chunk_end <= start || chunk_start >= end {
                    continue;
                }
                let data = read_large_chunk(&self.paths, &chunk)?;
                let slice_start = start.saturating_sub(chunk_start) as usize;
                let slice_end = (end.min(chunk_end) - chunk_start) as usize;
                out.extend_from_slice(&data[slice_start..slice_end]);
            }
            Ok(out)
        } else {
            Ok(Vec::new())
        }
    }
}

impl Filesystem for MajutsuFuseFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(parent_node) = self.nodes.get(&parent) else {
            reply.error(ENOENT);
            return;
        };
        if let FuseNodeKind::Directory { children } = &parent_node.kind {
            if let Some(ino) = children.get(name).and_then(|ino| self.nodes.get(ino)) {
                reply.entry(&FUSE_TTL, &ino.attr, 0);
                return;
            }
        }
        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if let Some(node) = self.nodes.get(&ino) {
            reply.attr(&FUSE_TTL, &node.attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            reply.error(EROFS);
            return;
        }
        match self.nodes.get(&ino).map(|node| &node.kind) {
            Some(FuseNodeKind::File { .. }) => reply.opened(0, 0),
            Some(FuseNodeKind::Directory { .. }) => reply.error(EISDIR),
            _ => reply.error(ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.nodes.get(&ino).map(|node| &node.kind) {
            Some(FuseNodeKind::File { record }) => match self.read_file(record, offset, size) {
                Ok(data) => reply.data(&data),
                Err(_) => reply.error(EIO),
            },
            Some(FuseNodeKind::Directory { .. }) => reply.error(EISDIR),
            _ => reply.error(ENOENT),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.nodes.get(&ino).map(|node| &node.kind) {
            Some(FuseNodeKind::Symlink { target }) => reply.data(target.as_bytes()),
            _ => reply.error(ENOENT),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let Some(node) = self.nodes.get(&ino) else {
            reply.error(ENOENT);
            return;
        };
        let FuseNodeKind::Directory { children } = &node.kind else {
            reply.error(ENOENT);
            return;
        };
        let mut entries = Vec::with_capacity(children.len() + 2);
        entries.push((ino, FileType::Directory, OsString::from(".")));
        entries.push((node.parent, FileType::Directory, OsString::from("..")));
        for (name, child_ino) in children {
            if let Some(child) = self.nodes.get(child_ino) {
                entries.push((*child_ino, child.attr.kind, name.clone()));
            }
        }
        for (i, (entry_ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry_ino, (i + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        reply.error(EROFS);
    }
}

fn mount_fuse_cmd(paths: &Paths, conn: &Connection, plan: &RestorePlan) -> Result<()> {
    let mountpoint = plan
        .to
        .as_ref()
        .ok_or_else(|| anyhow!("fuse mount requires a target directory"))?;
    prepare_mountpoint(mountpoint)?;
    let fs = MajutsuFuseFs::from_plan(paths, plan)?;
    record_op(
        conn,
        "mount-fuse",
        None,
        Some(&plan.snapshot.snapshot_id),
        Some(&format!("at {}", mountpoint.display())),
    )?;
    println!("mounted snapshot {}", plan.snapshot.snapshot_id);
    println!("target {}", mountpoint.display());
    println!("backend fuse");
    println!("files {}", plan.files.len());
    fuser::mount2(
        fs,
        mountpoint,
        &[
            MountOption::RO,
            MountOption::FSName("majutsu".into()),
            MountOption::Subtype("majutsu".into()),
            MountOption::DefaultPermissions,
        ],
    )
    .with_context(|| format!("mount fuse view {}", mountpoint.display()))?;
    Ok(())
}

fn fuse_attr(ino: u64, kind: FileType, size: u64, perm: u16, modified: Option<i64>) -> FileAttr {
    let time = modified
        .and_then(|seconds| u64::try_from(seconds).ok())
        .map(|seconds| UNIX_EPOCH + Duration::from_secs(seconds))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: time,
        mtime: time,
        ctime: time,
        crtime: time,
        kind,
        perm,
        nlink: if kind == FileType::Directory { 2 } else { 1 },
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

fn fuse_file_perm(mode: u32, kind: FileType) -> u16 {
    if kind == FileType::Symlink {
        return 0o777;
    }
    let perm = (mode & 0o777) as u16;
    if perm == 0 { 0o644 } else { perm }
}

fn fuse_record_file_type(record: &FileRecord, kind: &FuseNodeKind) -> FileType {
    match &record.payload {
        Payload::Special { special_kind } => match special_kind.as_str() {
            "fifo" => FileType::NamedPipe,
            "socket" => FileType::Socket,
            "block-device" => FileType::BlockDevice,
            "char-device" => FileType::CharDevice,
            _ => FileType::RegularFile,
        },
        _ => match kind {
            FuseNodeKind::Directory { .. } => FileType::Directory,
            FuseNodeKind::Symlink { .. } => FileType::Symlink,
            FuseNodeKind::File { .. } => FileType::RegularFile,
        },
    }
}

fn is_mountpoint(path: &Path) -> Result<bool> {
    let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let needle = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    Ok(mounts
        .lines()
        .any(|line| line.split_whitespace().nth(4).map(Path::new) == Some(needle.as_path())))
}

fn unmount_fuse(path: &Path) -> Result<()> {
    let status = ProcessCommand::new("fusermount3")
        .arg("-u")
        .arg(path)
        .status()
        .or_else(|_| {
            ProcessCommand::new("fusermount")
                .arg("-u")
                .arg(path)
                .status()
        })?;
    if !status.success() {
        bail!("failed to unmount {}", path.display());
    }
    Ok(())
}

fn hydrate_cmd(paths: &Paths, args: HydrateArgs) -> Result<()> {
    ensure_ready(paths)?;
    if let Some(path) = &args.path {
        validate_relative_filter_path(path, "hydrate --path")?;
    }
    let conn = open_db(paths)?;
    let lazy_root = args.view.join(".majutsu-lazy");
    if !lazy_root.exists() {
        bail!("lazy metadata not found: {}", lazy_root.display());
    }
    let requested_path = args.path.as_ref().map(|path| path_to_slash(path));
    let mut sidecars = Vec::new();
    for entry in WalkDir::new(&lazy_root).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() || entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let lazy: LazyMountEntry = serde_json::from_slice(&fs::read(entry.path())?)
            .with_context(|| format!("read lazy metadata {}", entry.path().display()))?;
        if args
            .root
            .as_deref()
            .is_some_and(|root| root != lazy.root_id)
        {
            continue;
        }
        if requested_path
            .as_deref()
            .is_some_and(|path| path != lazy.path)
        {
            continue;
        }
        sidecars.push((entry.path().to_path_buf(), lazy));
    }
    if sidecars.is_empty() {
        bail!("no lazy large files matched");
    }
    let mut hydrated = 0usize;
    for (sidecar, lazy) in sidecars {
        let manifest: LargeManifest =
            serde_json::from_slice(&read_object(paths, &lazy.manifest_key)?)
                .with_context(|| format!("read large manifest {}", lazy.manifest_key))?;
        let dest = args.view.join(&lazy.root_id).join(&lazy.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        write_large_chunks_atomic(paths, &dest, &manifest)?;
        fs::remove_file(sidecar)?;
        hydrated += 1;
    }
    record_op(
        &conn,
        "hydrate",
        None,
        None,
        Some(&format!("view {}", args.view.display())),
    )?;
    println!("hydrated_large_files {hydrated}");
    Ok(())
}

fn large_cmd(paths: &Paths, command: LargeCommand) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    match command {
        LargeCommand::List => {
            let mut stmt = conn.prepare(
                "select l.oid, l.size, l.chunk_count, l.manifest_key, p.oid is not null
                 from large_objects l left join large_pins p on p.oid = l.oid
                 order by l.rowid desc",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, usize>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            })?;
            for row in rows {
                let (oid, size, chunks, key, pinned) = row?;
                let pin = if pinned { "pinned" } else { "unpinned" };
                println!("{oid}\t{size}\t{chunks}\t{pin}\t{key}");
            }
        }
        LargeCommand::Stat => {
            let count: i64 =
                conn.query_row("select count(*) from large_objects", [], |r| r.get(0))?;
            let bytes: Option<u64> =
                conn.query_row("select sum(size) from large_objects", [], |r| r.get(0))?;
            let chunks: i64 = conn.query_row("select count(*) from chunks", [], |r| r.get(0))?;
            let pins: i64 = conn.query_row("select count(*) from large_pins", [], |r| r.get(0))?;
            println!("large_objects {count}");
            println!("logical_bytes {}", bytes.unwrap_or(0));
            println!("chunks {chunks}");
            println!("pinned {pins}");
        }
        LargeCommand::Verify => fsck(paths)?,
        LargeCommand::Pin(args) => {
            let snapshot =
                current_snapshot(&conn)?.ok_or_else(|| anyhow!("no current snapshot"))?;
            let manifests = large_pin_snapshots(&conn, args.since.as_deref(), &snapshot)?;
            let mut pinned = 0usize;
            let mut seen = BTreeSet::new();
            for manifest in manifests {
                for (root_id, records) in manifest.roots {
                    if args.root.as_deref().is_some_and(|filter| filter != root_id) {
                        continue;
                    }
                    for record in records {
                        if let Some((oid, _, _)) = payload_large_ref(&record.payload) {
                            let oid = oid.to_string();
                            if seen.insert(oid.clone()) {
                                conn.execute(
                                    "insert or replace into large_pins(oid, pinned_at, reason) values (?1, ?2, ?3)",
                                    params![
                                        oid,
                                        Utc::now().to_rfc3339(),
                                        args.since
                                            .as_ref()
                                            .map(|since| format!("pin since {since}"))
                                    ],
                                )?;
                                pinned += 1;
                            }
                        }
                    }
                }
            }
            record_op(
                &conn,
                "large-pin",
                Some(&snapshot),
                Some(&snapshot),
                Some(&format!("pinned {pinned} large objects")),
            )?;
            println!("pinned {pinned}");
        }
        LargeCommand::Unpin(args) => {
            let removed = if let Some(older_than) = args.older_than {
                let cutoff = parse_duration_ago(&older_than)?;
                conn.execute(
                    "delete from large_pins where pinned_at <= ?1",
                    params![cutoff.to_rfc3339()],
                )?
            } else {
                conn.execute("delete from large_pins", [])?
            };
            record_op(
                &conn,
                "large-unpin",
                current_snapshot(&conn)?.as_deref(),
                current_snapshot(&conn)?.as_deref(),
                Some(&format!("unpinned {removed} large objects")),
            )?;
            println!("unpinned {removed}");
        }
    }
    Ok(())
}

fn large_pin_snapshots(
    conn: &Connection,
    since: Option<&str>,
    current_snapshot_id: &str,
) -> Result<Vec<SnapshotManifest>> {
    let Some(since) = since else {
        return Ok(vec![load_snapshot_by_id(conn, current_snapshot_id)?]);
    };
    let cutoff = parse_pin_since(since)?;
    let mut stmt =
        conn.prepare("select manifest_json, created_at from snapshots order by created_at")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut manifests = Vec::new();
    for row in rows {
        let (json, created_at) = row?;
        if parse_db_time(&created_at)? >= cutoff {
            manifests.push(serde_json::from_str(&json)?);
        }
    }
    Ok(manifests)
}

fn parse_pin_since(input: &str) -> Result<DateTime<Utc>> {
    parse_duration_ago(input).or_else(|_| {
        let parsed = parse_time(input)?;
        parse_db_time(&parsed)
    })
}

fn sync_cmd(paths: &Paths, command: Option<SyncCommand>) -> Result<()> {
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
    let conn = open_db(paths)?;
    if let Some(SyncCommand::Status) = command {
        return sync_status(paths, &conn, &remote);
    }
    let current = current_snapshot(&conn)?;
    let previous_last_synced = ref_value(&conn, "last-synced")?;
    let synced_at = Utc::now().to_rfc3339();
    set_ref_value(&conn, "last-synced", &synced_at)?;
    let sync_op = record_op(
        &conn,
        "remote-sync",
        current.as_deref(),
        current.as_deref(),
        Some("pushed metadata and objects"),
    )?;
    let result = enqueue_and_drain_sync(paths, &conn, &config, &remote);
    if result.is_err() {
        restore_ref_value(&conn, "last-synced", previous_last_synced.as_deref())?;
        delete_operation(&conn, &sync_op)?;
    }
    result
}

fn enqueue_and_drain_sync(
    paths: &Paths,
    conn: &Connection,
    config: &Config,
    remote: &RemoteStore,
) -> Result<()> {
    let export = export_metadata(conn, config)?;
    enqueue_inline_upload(
        paths,
        "metadata/export.json",
        serde_json::to_vec_pretty(&export)?,
    )?;
    let host_metadata_key = format!("hosts/{}/metadata/export.json", config.host.id);
    enqueue_inline_upload(
        paths,
        &host_metadata_key,
        serde_json::to_vec_pretty(&export)?,
    )?;
    for snapshot in &export.snapshots {
        enqueue_inline_upload(
            paths,
            &host_snapshot_key(&config.host.id, &snapshot.id),
            serde_json::to_vec_pretty(snapshot)?,
        )?;
        enqueue_inline_upload(
            paths,
            &host_snapshot_canonical_key(&config.host.id, &snapshot.id),
            encode_canonical_remote_export(paths, snapshot)?,
        )?;
    }
    for operation in &export.operations {
        enqueue_inline_upload(
            paths,
            &host_operation_key(&config.host.id, &operation.id),
            serde_json::to_vec_pretty(operation)?,
        )?;
        enqueue_inline_upload(
            paths,
            &host_operation_canonical_key(&config.host.id, &operation.id),
            encode_canonical_remote_export(paths, operation)?,
        )?;
    }
    enqueue_inline_upload(
        paths,
        "config.toml",
        toml::to_string_pretty(&config)?.into_bytes(),
    )?;
    enqueue_inline_upload(
        paths,
        "host.toml",
        toml::to_string_pretty(&config.host)?.into_bytes(),
    )?;
    if let Some(current) = export.refs.get("current") {
        enqueue_inline_upload(paths, "hosts/current", current.as_bytes().to_vec())?;
        enqueue_inline_upload(
            paths,
            &format!("hosts/{}/current", config.host.id),
            current.as_bytes().to_vec(),
        )?;
        enqueue_inline_upload(
            paths,
            &format!("hosts/{}/refs/current", config.host.id),
            current.as_bytes().to_vec(),
        )?;
    }
    if let Some(last_synced) = export.refs.get("last-synced") {
        enqueue_inline_upload(
            paths,
            &format!("hosts/{}/refs/last-synced", config.host.id),
            last_synced.as_bytes().to_vec(),
        )?;
    }
    let host_index = update_remote_host_index(&remote, &config, &export, &host_metadata_key)?;
    enqueue_inline_upload(
        paths,
        "hosts/index.json",
        serde_json::to_vec_pretty(&host_index)?,
    )?;
    enqueue_inline_upload(
        paths,
        &format!("gc/marks/{}.json", config.host.id),
        serde_json::to_vec_pretty(&build_gc_mark_export(&config, &export))?,
    )?;
    let recipients = paths.home.join("keys/recipients.toml");
    if recipients.exists() {
        enqueue_file_upload(paths, "keys/recipients.toml", &recipients)?;
    }
    enqueue_inline_upload(
        paths,
        CHUNK_INDEX_SHARD_KEY,
        encode_canonical_remote_export(paths, &build_chunk_index_shard(&export))?,
    )?;

    for key in local_object_keys(&export) {
        let local = paths.home.join(&key);
        if local.exists() {
            enqueue_file_upload(paths, &key, &local)?;
            for alias in canonical_remote_aliases(&key) {
                if canonical_alias_uses_structured_encoding(&key) {
                    enqueue_inline_upload(
                        paths,
                        &alias,
                        encode_canonical_local_object(paths, &key)?,
                    )?;
                } else {
                    enqueue_file_upload(paths, &alias, &local)?;
                }
            }
        }
    }
    let uploaded = drain_upload_queue(paths, &remote)?;
    let pruned_remote_exports = prune_remote_host_exports(&remote, &config.host.id, &export)?;
    println!("synced {} objects to {}", uploaded, remote.describe());
    println!("pruned_remote_exports {}", pruned_remote_exports);
    Ok(())
}

fn ref_value(conn: &Connection, name: &str) -> Result<Option<String>> {
    conn.query_row(
        "select value from refs where name=?1",
        params![name],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn set_ref_value(conn: &Connection, name: &str, value: &str) -> Result<()> {
    conn.execute(
        "insert into refs(name, value) values (?1, ?2)
         on conflict(name) do update set value=excluded.value",
        params![name, value],
    )?;
    Ok(())
}

fn restore_ref_value(conn: &Connection, name: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        set_ref_value(conn, name, value)
    } else {
        conn.execute("delete from refs where name=?1", params![name])?;
        Ok(())
    }
}

fn delete_operation(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("delete from operations where id=?1", params![id])?;
    rewrite_local_oplog(conn)?;
    Ok(())
}

fn sync_status(paths: &Paths, conn: &Connection, remote: &RemoteStore) -> Result<()> {
    let local_current = current_snapshot(conn)?;
    let config = read_config(paths)?;
    let canonical_current = format!("hosts/{}/refs/current", config.host.id);
    let canonical_last_synced = format!("hosts/{}/refs/last-synced", config.host.id);
    let mut remote_current = remote_ref(remote, &canonical_current)?;
    if remote_current.is_none() {
        remote_current = remote_ref(remote, "hosts/current")?;
    }
    let remote_last_synced = remote_ref(remote, &canonical_last_synced)?;
    let export = export_metadata(conn, &read_config(paths)?)?;
    let local_keys = local_object_keys(&export);
    let mut missing_remote = 0usize;
    for key in &local_keys {
        if !remote_object_available(remote, key)? {
            missing_remote += 1;
        }
    }
    println!("remote {}", remote.describe());
    println!(
        "local_current {}",
        local_current.unwrap_or_else(|| "(none)".into())
    );
    println!(
        "remote_current {}",
        remote_current.unwrap_or_else(|| "(none)".into())
    );
    println!(
        "remote_last_synced {}",
        remote_last_synced.unwrap_or_else(|| "(none)".into())
    );
    println!("local_objects {}", local_keys.len());
    println!("missing_remote_objects {}", missing_remote);
    println!("queued_uploads {}", upload_queue_items(paths)?.len());
    Ok(())
}

fn remote_ref(remote: &RemoteStore, key: &str) -> Result<Option<String>> {
    if remote.exists(key)? {
        return Ok(Some(
            String::from_utf8(remote.get(key)?)?.trim().to_string(),
        ));
    }
    Ok(None)
}

fn host_snapshot_key(host_id: &str, snapshot_id: &str) -> String {
    format!("hosts/{host_id}/snapshots/{snapshot_id}.json")
}

fn host_snapshot_canonical_key(host_id: &str, snapshot_id: &str) -> String {
    format!("hosts/{host_id}/snapshots/{snapshot_id}.cbor.zst.enc")
}

fn host_operation_key(host_id: &str, op_id: &str) -> String {
    format!("hosts/{host_id}/ops/{op_id}.json")
}

fn host_operation_canonical_key(host_id: &str, op_id: &str) -> String {
    format!("hosts/{host_id}/ops/{op_id}.cbor.zst.enc")
}

fn encode_canonical_remote_export<T: Serialize>(paths: &Paths, value: &T) -> Result<Vec<u8>> {
    let cbor = serde_cbor::to_vec(value)?;
    let compressed = zstd::stream::encode_all(cbor.as_slice(), 3)?;
    encode_object(paths, &compressed)
}

fn decode_canonical_remote_export<T: for<'de> Deserialize<'de>>(
    paths: &Paths,
    bytes: &[u8],
) -> Result<T> {
    let compressed = decode_object(paths, bytes)?;
    let cbor = zstd::stream::decode_all(compressed.as_slice())?;
    Ok(serde_cbor::from_slice(&cbor)?)
}

fn build_chunk_index_shard(export: &MetadataExport) -> ChunkIndexShard {
    let chunks = export
        .chunks
        .iter()
        .map(|chunk| ChunkIndexEntry {
            oid: chunk.oid.clone(),
            size: chunk.size,
            object_key: chunk.object_key.clone(),
            canonical_key: canonical_remote_alias(&chunk.object_key)
                .unwrap_or_else(|| chunk.object_key.clone()),
        })
        .collect();
    ChunkIndexShard {
        version: 1,
        shard: "shard-0000".into(),
        updated_at: Utc::now(),
        chunks,
    }
}

fn build_gc_mark_export(config: &Config, export: &MetadataExport) -> GcMarkExport {
    let mut object_keys = local_object_keys(export);
    for key in object_keys.clone() {
        object_keys.extend(canonical_remote_aliases(&key));
    }
    object_keys.sort();
    object_keys.dedup();
    GcMarkExport {
        version: 1,
        host_id: config.host.id.clone(),
        marked_at: Utc::now(),
        current_snapshot: export.refs.get("current").cloned(),
        object_keys,
    }
}

fn encode_canonical_local_object(paths: &Paths, key: &str) -> Result<Vec<u8>> {
    let bytes = read_object(paths, key)?;
    if key.starts_with("objects/trees/") {
        let manifest: TreeManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode tree manifest {key}"))?;
        encode_canonical_remote_export(paths, &manifest)
    } else if key.starts_with("objects/indexes/pack/") {
        let index: PackIndex =
            serde_json::from_slice(&bytes).with_context(|| format!("decode pack index {key}"))?;
        encode_canonical_remote_export(paths, &index)
    } else if key.starts_with("objects/large/manifests/") {
        let manifest: LargeManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode large manifest {key}"))?;
        encode_canonical_remote_export(paths, &manifest)
    } else {
        encode_object(paths, &bytes)
    }
}

fn canonical_alias_uses_structured_encoding(key: &str) -> bool {
    key.starts_with("objects/trees/")
        || key.starts_with("objects/indexes/pack/")
        || key.starts_with("objects/large/manifests/")
}

fn update_remote_host_index(
    remote: &RemoteStore,
    config: &Config,
    export: &MetadataExport,
    metadata_key: &str,
) -> Result<RemoteHostIndex> {
    let mut index = read_remote_host_index(remote)?;
    let last_synced_at = export
        .refs
        .get("last-synced")
        .map(|value| parse_db_time(value))
        .transpose()?
        .unwrap_or(export.exported_at);
    let summary = RemoteHostSummary {
        id: config.host.id.clone(),
        name: config.host.name.clone(),
        last_synced_at,
        current_snapshot: export.refs.get("current").cloned(),
        metadata_key: metadata_key.to_string(),
    };
    index.hosts.retain(|host| host.id != summary.id);
    index.hosts.push(summary);
    index.hosts.sort_by(|a, b| a.id.cmp(&b.id));
    index.updated_at = Utc::now();
    Ok(index)
}

fn prune_remote_host_exports(
    remote: &RemoteStore,
    host_id: &str,
    export: &MetadataExport,
) -> Result<usize> {
    let live_snapshots = export
        .snapshots
        .iter()
        .flat_map(|snapshot| {
            [
                host_snapshot_key(host_id, &snapshot.id),
                host_snapshot_canonical_key(host_id, &snapshot.id),
            ]
        })
        .collect::<BTreeSet<_>>();
    let live_ops = export
        .operations
        .iter()
        .flat_map(|operation| {
            [
                host_operation_key(host_id, &operation.id),
                host_operation_canonical_key(host_id, &operation.id),
            ]
        })
        .collect::<BTreeSet<_>>();
    let mut removed = 0usize;
    for key in remote.list(&format!("hosts/{host_id}/snapshots/"))? {
        if (key.ends_with(".json") || key.ends_with(".cbor.zst.enc"))
            && !live_snapshots.contains(&key)
        {
            write_remote_gc_tombstone(remote, host_id, &key)?;
            remote.delete(&key)?;
            removed += 1;
        }
    }
    for key in remote.list(&format!("hosts/{host_id}/ops/"))? {
        if (key.ends_with(".json") || key.ends_with(".cbor.zst.enc")) && !live_ops.contains(&key) {
            write_remote_gc_tombstone(remote, host_id, &key)?;
            remote.delete(&key)?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn write_remote_gc_tombstone(remote: &RemoteStore, host_id: &str, key: &str) -> Result<()> {
    let tombstone = GcTombstoneExport {
        version: 1,
        host_id: host_id.to_string(),
        deleted_at: Utc::now(),
        key: key.to_string(),
    };
    remote.put(
        &format!("gc/tombstones/{host_id}/{}.json", new_id("tombstone")),
        &serde_json::to_vec_pretty(&tombstone)?,
    )
}

fn read_remote_host_index(remote: &RemoteStore) -> Result<RemoteHostIndex> {
    if remote.exists("hosts/index.json")? {
        let mut index: RemoteHostIndex = serde_json::from_slice(&remote.get("hosts/index.json")?)?;
        index.hosts.sort_by(|a, b| a.id.cmp(&b.id));
        return Ok(index);
    }
    Ok(RemoteHostIndex {
        version: 1,
        updated_at: Utc::now(),
        hosts: Vec::new(),
    })
}

fn remote_host_index_with_legacy(remote: &RemoteStore) -> Result<RemoteHostIndex> {
    let mut index = read_remote_host_index(remote)?;
    if index.hosts.is_empty() && remote.exists("metadata/export.json")? {
        let export: MetadataExport = serde_json::from_slice(&remote.get("metadata/export.json")?)?;
        index.hosts.push(RemoteHostSummary {
            id: export.config.host.id.clone(),
            name: export.config.host.name.clone(),
            last_synced_at: export.exported_at,
            current_snapshot: export.refs.get("current").cloned(),
            metadata_key: "metadata/export.json".into(),
        });
    }
    Ok(index)
}

fn enqueue_inline_upload(paths: &Paths, key: &str, bytes: Vec<u8>) -> Result<()> {
    write_upload_item(
        paths,
        UploadQueueItem {
            id: format!("upload-{}", blake3_hex(key.as_bytes())),
            key: key.to_string(),
            source: None,
            inline: Some(bytes),
            created_at: Utc::now(),
            attempts: 0,
        },
    )
}

fn enqueue_file_upload(paths: &Paths, key: &str, source: &Path) -> Result<()> {
    write_upload_item(
        paths,
        UploadQueueItem {
            id: format!("upload-{}", blake3_hex(key.as_bytes())),
            key: key.to_string(),
            source: Some(path_to_slash(source)),
            inline: None,
            created_at: Utc::now(),
            attempts: 0,
        },
    )
}

fn write_upload_item(paths: &Paths, item: UploadQueueItem) -> Result<()> {
    fs::create_dir_all(&paths.upload_queue)?;
    let path = paths.upload_queue.join(format!("{}.json", item.id));
    let item = if path.exists() {
        let existing: UploadQueueItem = serde_json::from_slice(&fs::read(&path)?)?;
        UploadQueueItem {
            attempts: existing.attempts,
            created_at: existing.created_at,
            ..item
        }
    } else {
        item
    };
    fs::write(path, serde_json::to_vec_pretty(&item)?)?;
    Ok(())
}

fn upload_queue_items(paths: &Paths) -> Result<Vec<(PathBuf, UploadQueueItem)>> {
    if !paths.upload_queue.exists() {
        return Ok(Vec::new());
    }
    let mut items = Vec::new();
    for entry in fs::read_dir(&paths.upload_queue)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(OsStr::to_str) == Some("json")
        {
            let item: UploadQueueItem = serde_json::from_slice(&fs::read(entry.path())?)?;
            items.push((entry.path(), item));
        }
    }
    items.sort_by(|a, b| a.1.key.cmp(&b.1.key));
    Ok(items)
}

fn drain_upload_queue(paths: &Paths, remote: &RemoteStore) -> Result<usize> {
    let mut uploaded = 0usize;
    for (path, mut item) in upload_queue_items(paths)? {
        let bytes = if let Some(bytes) = item.inline.take() {
            bytes
        } else if let Some(source) = &item.source {
            fs::read(source).with_context(|| format!("read queued upload source {source}"))?
        } else {
            bail!(
                "queued upload has neither inline payload nor source: {}",
                item.key
            );
        };
        let upload_result = if is_content_addressed_remote_key(&item.key)
            && remote.capabilities().conditional_put
        {
            remote.put_if_absent(&item.key, &bytes).map(|_| ())
        } else {
            remote.put(&item.key, &bytes)
        };
        match upload_result {
            Ok(()) => {
                fs::remove_file(path)?;
                uploaded += 1;
            }
            Err(err) => {
                item.attempts += 1;
                fs::write(&path, serde_json::to_vec_pretty(&item)?)?;
                return Err(err).with_context(|| format!("upload failed for {}", item.key));
            }
        }
    }
    Ok(uploaded)
}

fn record_event(paths: &Paths, kind: &str, detail: &str) -> Result<()> {
    fs::create_dir_all(&paths.event_queue)?;
    let event = EventJournalRecord {
        event_id: new_id("event"),
        kind: kind.to_string(),
        observed_at: Utc::now(),
        detail: detail.to_string(),
    };
    let path = paths.event_queue.join(format!("{}.json", event.event_id));
    fs::write(path, serde_json::to_vec_pretty(&event)?)?;
    Ok(())
}

fn event_journal_records(paths: &Paths) -> Result<Vec<EventJournalRecord>> {
    if !paths.event_queue.exists() {
        return Ok(Vec::new());
    }
    let mut records: Vec<EventJournalRecord> = Vec::new();
    for entry in fs::read_dir(&paths.event_queue)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(OsStr::to_str) == Some("json")
        {
            records.push(serde_json::from_slice(&fs::read(entry.path())?)?);
        }
    }
    records.sort_by(|a, b| a.observed_at.cmp(&b.observed_at));
    Ok(records)
}

fn has_pending_journal_events(paths: &Paths) -> Result<bool> {
    let records = event_journal_records(paths)?;
    let last_snapshot_finish = records
        .iter()
        .filter(|event| event.kind == "snapshot-finish")
        .map(|event| event.observed_at)
        .max();
    Ok(records.iter().any(|event| {
        matches!(event.kind.as_str(), "fs-event" | "periodic-rescan")
            && last_snapshot_finish
                .map(|finished_at| event.observed_at > finished_at)
                .unwrap_or(true)
    }))
}

fn replay_pending_journal_events(paths: &Paths) -> Result<bool> {
    if !has_pending_journal_events(paths)? {
        return Ok(false);
    }
    record_event(
        paths,
        "event-journal-replay",
        "pending filesystem events after last snapshot-finish",
    )?;
    snapshot(
        paths,
        SnapshotArgs {
            message: Some("watch journal replay snapshot".into()),
        },
    )?;
    Ok(true)
}

fn remote_cmd(paths: &Paths, command: RemoteCommand) -> Result<()> {
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
            let metadata_key = if remote.exists("metadata/export.json")? {
                "metadata/export.json"
            } else if remote.exists("hosts/index.json")? {
                "hosts/index.json"
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
        RemoteCommand::Fsck => {
            remote_fsck(&remote)?;
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
        RemoteCommand::Host { id } => {
            let index = remote_host_index_with_legacy(&remote)?;
            let host = index
                .hosts
                .into_iter()
                .find(|host| host.id == id || host.name == id)
                .ok_or_else(|| anyhow!("remote host not found: {id}"))?;
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
        }
    }
    Ok(())
}

fn clone_cmd(paths: &Paths, args: CloneArgs) -> Result<()> {
    if paths.home.exists() && paths.home.read_dir()?.next().is_some() {
        bail!("target majutsu home is not empty: {}", paths.home.display());
    }
    create_layout(paths)?;
    let remote_config = RemoteConfig::from_url(args.remote);
    let remote = open_remote(&remote_config)?;
    let metadata_key = clone_metadata_key(&remote, args.host.as_deref())?;
    let export_bytes = remote.get(&metadata_key)?;
    let mut export: MetadataExport = serde_json::from_slice(&export_bytes)?;
    export.config.remote = Some(remote_config);
    write_config(paths, &export.config)?;
    fs::write(&paths.host, toml::to_string_pretty(&export.config.host)?)?;
    if remote.exists("keys/recipients.toml")? {
        fs::write(
            paths.home.join("keys/recipients.toml"),
            remote.get("keys/recipients.toml")?,
        )?;
    }
    if export.config.security.encryption != "none" {
        if let Ok(key) = env::var("MAJUTSU_MASTER_KEY") {
            write_master_key(paths, &key)?;
        }
    }
    for key in local_object_keys(&export) {
        let dest = paths.home.join(&key);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            dest,
            download_local_object_from_remote(paths, &remote, &key)?,
        )?;
    }
    let conn = open_db(paths)?;
    import_metadata(&conn, &export)?;
    println!("cloned {} into {}", remote.describe(), paths.home.display());
    println!("host {} {}", export.config.host.name, export.config.host.id);
    Ok(())
}

fn clone_metadata_key(remote: &RemoteStore, host: Option<&str>) -> Result<String> {
    if let Some(host_id) = host {
        let index = remote_host_index_with_legacy(remote)?;
        return index
            .hosts
            .into_iter()
            .find(|host| host.id == host_id || host.name == host_id)
            .map(|host| host.metadata_key)
            .ok_or_else(|| anyhow!("remote host not found: {host_id}"));
    }
    let index = remote_host_index_with_legacy(remote)?;
    match index.hosts.as_slice() {
        [host] => Ok(host.metadata_key.clone()),
        [] if remote.exists("metadata/export.json")? => Ok("metadata/export.json".into()),
        [] => {
            bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found")
        }
        _ => bail!("remote contains multiple hosts; rerun clone with --host"),
    }
}

fn download_local_object_from_remote(
    paths: &Paths,
    remote: &RemoteStore,
    key: &str,
) -> Result<Vec<u8>> {
    if remote.exists(key)? {
        return remote.get(key).with_context(|| format!("download {key}"));
    }
    let Some(alias) = canonical_remote_alias(key) else {
        return remote.get(key).with_context(|| format!("download {key}"));
    };
    let bytes = remote
        .get(&alias)
        .with_context(|| format!("download {key} via canonical alias {alias}"))?;
    canonical_remote_object_to_local_bytes(paths, key, &bytes)
}

fn canonical_remote_object_to_local_bytes(
    paths: &Paths,
    key: &str,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    if key.starts_with("objects/trees/") {
        let manifest: TreeManifest = decode_canonical_remote_export(paths, bytes)?;
        return Ok(encode_object(
            paths,
            &serde_json::to_vec_pretty(&manifest)?,
        )?);
    }
    if key.starts_with("objects/indexes/pack/") {
        let index: PackIndex = decode_canonical_remote_export(paths, bytes)?;
        return Ok(serde_json::to_vec_pretty(&index)?);
    }
    if key.starts_with("objects/large/manifests/") {
        let manifest: LargeManifest = decode_canonical_remote_export(paths, bytes)?;
        return Ok(encode_object(
            paths,
            &serde_json::to_vec_pretty(&manifest)?,
        )?);
    }
    Ok(bytes.to_vec())
}

fn remote_object_available(remote: &RemoteStore, key: &str) -> Result<bool> {
    if remote.exists(key)? {
        return Ok(true);
    }
    let Some(alias) = canonical_remote_alias(key) else {
        return Ok(false);
    };
    remote.exists(&alias)
}

fn remote_available_key(remote: &RemoteStore, key: &str) -> Result<String> {
    if remote.exists(key)? {
        return Ok(key.to_string());
    }
    if let Some(alias) = canonical_remote_alias(key) {
        if remote.exists(&alias)? {
            return Ok(alias);
        }
    }
    Ok(key.to_string())
}

fn watch_cmd(paths: &Paths, args: WatchArgs) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let args = resolve_watch_args(args, &config.watch);
    let backend = normalize_watch_backend(&args.backend)?;
    if !args.foreground {
        let pid = start_watch_daemon(
            paths,
            backend,
            args.interval_secs,
            args.debounce_ms,
            args.settle_ms,
            args.periodic_rescan_secs,
        )?;
        println!("started daemon pid {pid}");
        return Ok(());
    }
    let _lock = acquire_process_lock(&paths.daemon_lock, "daemon")?;
    start_daemon_ipc(paths)?;
    match backend {
        "notify" => watch_notify(paths, args, "notify"),
        "inotify" => watch_notify(paths, args, "inotify"),
        "poll" => watch_poll(paths, &args),
        other => bail!("unsupported watch backend: {other}"),
    }
}

fn normalize_watch_backend(backend: &str) -> Result<&'static str> {
    match backend {
        "notify" | "native" => Ok("notify"),
        "poll" => Ok("poll"),
        "inotify" => {
            if cfg!(target_os = "linux") {
                Ok("inotify")
            } else {
                bail!("inotify backend is only available on Linux")
            }
        }
        other => bail!("unsupported watch backend: {other}"),
    }
}

fn resolve_watch_args(args: WatchArgs, config: &WatchConfig) -> ResolvedWatchArgs {
    ResolvedWatchArgs {
        foreground: args.foreground,
        mode: config.mode.clone(),
        interval_secs: args.interval_secs.unwrap_or(config.interval),
        debounce_ms: args.debounce_ms.unwrap_or(config.debounce),
        settle_ms: args.settle_ms.unwrap_or(config.settle),
        periodic_rescan_secs: args.periodic_rescan_secs.unwrap_or(config.periodic_rescan),
        backend: args.backend.unwrap_or_else(|| config.backend.clone()),
        once: args.once,
    }
}

fn default_daemon_backend() -> &'static str {
    if cfg!(target_os = "linux") {
        "inotify"
    } else {
        "notify"
    }
}

fn default_watch_backend() -> String {
    default_daemon_backend().into()
}

fn watch_poll(paths: &Paths, args: &ResolvedWatchArgs) -> Result<()> {
    record_event(
        paths,
        "watch-start",
        &format!(
            "backend=poll mode={} interval_secs={}",
            args.mode, args.interval_secs
        ),
    )?;
    loop {
        snapshot(
            paths,
            SnapshotArgs {
                message: Some("watch snapshot".into()),
            },
        )?;
        if args.once {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(args.interval_secs.max(1)));
    }
    record_event(paths, "watch-stop", "foreground watch stopped")?;
    Ok(())
}

fn watch_notify(paths: &Paths, args: ResolvedWatchArgs, backend_label: &str) -> Result<()> {
    let conn = open_db(paths)?;
    let active_roots = roots(&conn)?
        .into_iter()
        .filter(|root| root.status == "active" && root.path.exists())
        .collect::<Vec<_>>();
    if active_roots.is_empty() {
        bail!("no active roots are available to watch");
    }
    record_event(
        paths,
        "watch-start",
        &format!(
            "backend={} mode={} debounce_ms={} settle_ms={} periodic_rescan_secs={}",
            backend_label, args.mode, args.debounce_ms, args.settle_ms, args.periodic_rescan_secs
        ),
    )?;
    let (tx, rx) = mpsc::channel();
    let mut watcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        NotifyConfig::default(),
    )?;
    for root in &active_roots {
        watcher.watch(&root.path, RecursiveMode::Recursive)?;
        record_event(
            paths,
            "watch-root",
            &format!("{} {}", root.id, root.path.display()),
        )?;
    }
    if replay_pending_journal_events(paths)? && args.once {
        record_event(
            paths,
            "watch-stop",
            &format!("foreground {backend_label} watch stopped after journal replay"),
        )?;
        return Ok(());
    }
    loop {
        let event = match recv_watch_event(&rx, args.periodic_rescan_secs)? {
            Some(event) => event,
            None => {
                record_event(
                    paths,
                    "periodic-rescan",
                    &format!("interval_secs={}", args.periodic_rescan_secs),
                )?;
                snapshot(
                    paths,
                    SnapshotArgs {
                        message: Some("watch periodic rescan".into()),
                    },
                )?;
                if args.once {
                    break;
                }
                continue;
            }
        };
        let detail = format_notify_event(&event);
        record_event(paths, "fs-event", &detail)?;
        if args.mode == "strict" {
            snapshot(
                paths,
                SnapshotArgs {
                    message: Some("watch strict event snapshot".into()),
                },
            )?;
            if args.once {
                break;
            }
            continue;
        }
        let debounce = std::time::Duration::from_millis(args.debounce_ms.max(1));
        let settle = std::time::Duration::from_millis(args.settle_ms);
        drain_watch_debounce(paths, &rx, debounce)?;
        if !settle.is_zero() {
            record_event(
                paths,
                "watch-settle",
                &format!("settle_ms={}", args.settle_ms),
            )?;
            loop {
                match rx.recv_timeout(settle) {
                    Ok(Ok(next)) => {
                        record_event(paths, "fs-event", &format_notify_event(&next))?;
                        drain_watch_debounce(paths, &rx, debounce)?;
                        record_event(
                            paths,
                            "watch-settle",
                            &format!("settle_ms={}", args.settle_ms),
                        )?;
                        continue;
                    }
                    Ok(Err(err)) => return Err(err.into()),
                    Err(mpsc::RecvTimeoutError::Timeout) => break,
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        bail!("watch channel disconnected")
                    }
                }
            }
        }
        snapshot(
            paths,
            SnapshotArgs {
                message: Some("watch event snapshot".into()),
            },
        )?;
        if args.once {
            break;
        }
    }
    record_event(
        paths,
        "watch-stop",
        &format!("foreground {backend_label} watch stopped"),
    )?;
    Ok(())
}

fn drain_watch_debounce(
    paths: &Paths,
    rx: &mpsc::Receiver<notify::Result<notify::Event>>,
    debounce: Duration,
) -> Result<()> {
    loop {
        match rx.recv_timeout(debounce) {
            Ok(Ok(next)) => {
                record_event(paths, "fs-event", &format_notify_event(&next))?;
            }
            Ok(Err(err)) => return Err(err.into()),
            Err(mpsc::RecvTimeoutError::Timeout) => return Ok(()),
            Err(mpsc::RecvTimeoutError::Disconnected) => bail!("watch channel disconnected"),
        }
    }
}

fn recv_watch_event(
    rx: &mpsc::Receiver<notify::Result<notify::Event>>,
    periodic_rescan_secs: u64,
) -> Result<Option<notify::Event>> {
    if periodic_rescan_secs == 0 {
        return rx.recv()?.map(Some).map_err(Into::into);
    }
    match rx.recv_timeout(Duration::from_secs(periodic_rescan_secs)) {
        Ok(event) => event.map(Some).map_err(Into::into),
        Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
        Err(mpsc::RecvTimeoutError::Disconnected) => bail!("watch channel disconnected"),
    }
}

fn start_watch_daemon(
    paths: &Paths,
    backend: &str,
    interval_secs: u64,
    debounce_ms: u64,
    settle_ms: u64,
    periodic_rescan_secs: u64,
) -> Result<u32> {
    if let Some(pid) = read_pid(&paths.daemon_pid)? {
        if pid_alive(pid) {
            bail!("daemon already running with pid {pid}");
        }
    }
    fs::create_dir_all(&paths.runtime)?;
    fs::create_dir_all(&paths.logs)?;
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(paths.logs.join("majutsu.log"))?;
    let child = ProcessCommand::new(env::current_exe()?)
        .arg("--home")
        .arg(&paths.home)
        .arg("watch")
        .arg("--foreground")
        .arg("true")
        .arg("--backend")
        .arg(backend)
        .arg("--interval-secs")
        .arg(interval_secs.to_string())
        .arg("--debounce-ms")
        .arg(debounce_ms.to_string())
        .arg("--settle-ms")
        .arg(settle_ms.to_string())
        .arg("--periodic-rescan-secs")
        .arg(periodic_rescan_secs.to_string())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()?;
    let pid = child.id();
    fs::write(&paths.daemon_pid, pid.to_string())?;
    Ok(pid)
}

fn daemon_cmd(paths: &Paths, command: DaemonCommand) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    match command {
        DaemonCommand::Start {
            interval_secs,
            settle_ms,
            periodic_rescan_secs,
        } => {
            let backend = normalize_watch_backend(&config.watch.backend)?;
            let pid = start_watch_daemon(
                paths,
                backend,
                interval_secs.unwrap_or(config.watch.interval),
                config.watch.debounce,
                settle_ms.unwrap_or(config.watch.settle),
                periodic_rescan_secs.unwrap_or(config.watch.periodic_rescan),
            )?;
            println!("started daemon pid {pid}");
        }
        DaemonCommand::Stop => {
            let pid =
                read_pid(&paths.daemon_pid)?.ok_or_else(|| anyhow!("daemon pid file not found"))?;
            if pid_alive(pid) {
                let status = ProcessCommand::new("kill").arg(pid.to_string()).status()?;
                if !status.success() {
                    bail!("failed to stop daemon pid {pid}");
                }
            }
            let _ = fs::remove_file(&paths.daemon_pid);
            let _ = fs::remove_file(paths.runtime.join("daemon.sock"));
            println!("stopped daemon pid {pid}");
        }
        DaemonCommand::Status => {
            if let Some(pid) = read_pid(&paths.daemon_pid)? {
                if pid_alive(pid) {
                    if let Ok(reply) = daemon_ipc_request(paths, "status") {
                        println!("{reply}");
                    } else {
                        println!("running pid {pid}");
                        println!("ipc unavailable");
                    }
                } else {
                    println!("stale pid {pid}");
                }
            } else {
                println!("stopped");
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn start_daemon_ipc(paths: &Paths) -> Result<()> {
    fs::create_dir_all(&paths.runtime)?;
    let sock = paths.runtime.join("daemon.sock");
    let _ = fs::remove_file(&sock);
    let listener = UnixListener::bind(&sock)?;
    let home = paths.home.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    let _ = handle_daemon_ipc(&home, &mut stream);
                }
                Err(_) => break,
            }
        }
    });
    Ok(())
}

#[cfg(not(unix))]
fn start_daemon_ipc(_: &Paths) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn handle_daemon_ipc(home: &Path, stream: &mut UnixStream) -> Result<()> {
    let mut command = String::new();
    stream.read_to_string(&mut command)?;
    let paths = resolve_paths(Some(home.to_path_buf()))?;
    match command.trim() {
        "status" => {
            let conn = open_db(&paths)?;
            let roots = roots(&conn)?.len();
            let current = current_snapshot(&conn)?.unwrap_or_else(|| "(none)".into());
            let pid = std::process::id();
            writeln!(stream, "running pid {pid}")?;
            writeln!(stream, "ipc ok")?;
            writeln!(stream, "roots {roots}")?;
            writeln!(stream, "current {current}")?;
        }
        other => {
            writeln!(stream, "error unknown command {other}")?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn daemon_ipc_request(paths: &Paths, command: &str) -> Result<String> {
    let mut stream = UnixStream::connect(paths.runtime.join("daemon.sock"))?;
    stream.write_all(command.as_bytes())?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut reply = String::new();
    stream.read_to_string(&mut reply)?;
    Ok(reply.trim_end().to_string())
}

#[cfg(not(unix))]
fn daemon_ipc_request(_: &Paths, _: &str) -> Result<String> {
    bail!("daemon IPC is not supported on this platform")
}

fn key_cmd(paths: &Paths, command: KeyCommand) -> Result<()> {
    match command {
        KeyCommand::Export => {
            ensure_ready(paths)?;
            let key = read_master_key(paths)?;
            println!("{key}");
        }
        KeyCommand::Import { hex } => {
            create_layout(paths)?;
            validate_key_hex(&hex)?;
            write_master_key(paths, &hex)?;
            println!("imported master key into {}", paths.master_key.display());
        }
        KeyCommand::Rotate { new_key } => {
            ensure_ready(paths)?;
            let rotated = rotate_master_key(paths, new_key)?;
            println!("rotated master key");
            println!("objects_rewritten {}", rotated.objects);
            println!("snapshots_rewritten {}", rotated.snapshots);
            println!("new_key {}", rotated.new_key);
        }
    }
    Ok(())
}

struct KeyRotationResult {
    objects: usize,
    snapshots: usize,
    new_key: String,
}

fn rotate_master_key(paths: &Paths, new_key: Option<String>) -> Result<KeyRotationResult> {
    let config = read_config(paths)?;
    if !encryption_enabled(&config.security)? {
        bail!("key rotation requires encrypted state");
    }
    let conn = open_db(paths)?;
    if !query_packs(&conn)?.is_empty() {
        bail!("key rotation with pack files is not supported yet; run before packing");
    }
    let old_key = read_master_key(paths)?;
    let new_key = new_key.unwrap_or(random_key_hex()?);
    validate_key_hex(&new_key)?;
    if old_key.trim() == new_key.trim() {
        bail!("new key must differ from current key");
    }

    let blobs = query_blobs(&conn)?;
    let chunks = query_chunks(&conn)?;
    let large_objects = query_large_objects(&conn)?;
    let mut blob_payloads = BTreeMap::new();
    for blob in &blobs {
        blob_payloads.insert(blob.oid.clone(), read_object(paths, &blob.object_key)?);
    }
    let mut chunk_payloads = BTreeMap::new();
    for chunk in &chunks {
        chunk_payloads.insert(chunk.oid.clone(), read_object(paths, &chunk.object_key)?);
    }
    let mut large_manifests = BTreeMap::new();
    for large in &large_objects {
        let manifest: LargeManifest =
            serde_json::from_slice(&read_object(paths, &large.manifest_key)?)?;
        large_manifests.insert(large.oid.clone(), manifest);
    }
    let mut snapshots = Vec::new();
    let mut stmt = conn.prepare("select id, manifest_json from snapshots order by created_at")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (id, json) = row?;
        snapshots.push((id, serde_json::from_str::<SnapshotManifest>(&json)?));
    }

    write_master_key(paths, &new_key)?;
    let mut objects = 0usize;
    let mut blob_keys = BTreeMap::new();
    for blob in &blobs {
        let key = store_bytes(paths, &paths.objects, &blob.oid, &blob_payloads[&blob.oid])?;
        conn.execute(
            "update blobs set object_key=?2 where oid=?1",
            params![blob.oid, key],
        )?;
        blob_keys.insert(blob.oid.clone(), key);
        objects += 1;
    }
    let mut chunk_keys = BTreeMap::new();
    for chunk in &chunks {
        let key = store_bytes(
            paths,
            &large_chunk_base_for_key(paths, &chunk.object_key),
            &chunk.oid,
            &chunk_payloads[&chunk.oid],
        )?;
        conn.execute(
            "update chunks set object_key=?2 where oid=?1",
            params![chunk.oid, key],
        )?;
        chunk_keys.insert(chunk.oid.clone(), key);
        objects += 1;
    }
    let mut large_manifest_keys = BTreeMap::new();
    for large in &large_objects {
        let mut manifest = large_manifests
            .remove(&large.oid)
            .ok_or_else(|| anyhow!("missing loaded large manifest {}", large.oid))?;
        for chunk in &mut manifest.chunks {
            chunk.object_key = chunk_keys
                .get(&chunk.oid)
                .ok_or_else(|| anyhow!("missing rotated chunk key {}", chunk.oid))?
                .clone();
        }
        let bytes = serde_json::to_vec_pretty(&manifest)?;
        let manifest_oid = blake3_hex(&bytes);
        let key = store_bytes(paths, &paths.large_manifests, &manifest_oid, &bytes)?;
        conn.execute(
            "update large_objects set manifest_key=?2 where oid=?1",
            params![large.oid, key],
        )?;
        large_manifest_keys.insert(large.oid.clone(), key);
        objects += 1;
    }

    let mut snapshots_rewritten = 0usize;
    for (snapshot_id, mut manifest) in snapshots {
        rewrite_manifest_payload_keys(&mut manifest, &blob_keys, &large_manifest_keys)?;
        manifest.root_trees.clear();
        for (root_id, records) in &manifest.roots {
            let tree = build_tree_manifest(root_id, records.clone())?;
            let tree_json = serde_json::to_vec_pretty(&tree)?;
            let tree_oid = blake3_hex(&tree_json);
            let tree_key = store_bytes(paths, &paths.trees, &tree_oid, &tree_json)?;
            manifest.root_trees.insert(
                root_id.clone(),
                RootSnapshot {
                    tree_id: tree.tree_id,
                    tree_key,
                    file_count: tree.entries.len(),
                },
            );
            objects += 1;
        }
        let manifest_json = serde_json::to_vec_pretty(&manifest)?;
        let manifest_oid = blake3_hex(&manifest_json);
        let manifest_key = store_bytes(paths, &paths.objects, &manifest_oid, &manifest_json)?;
        conn.execute(
            "update snapshots set manifest_key=?2, manifest_json=?3 where id=?1",
            params![snapshot_id, manifest_key, String::from_utf8(manifest_json)?],
        )?;
        snapshots_rewritten += 1;
        objects += 1;
    }
    record_op(
        &conn,
        "key-rotation",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!("rewrote {objects} objects")),
    )?;
    Ok(KeyRotationResult {
        objects,
        snapshots: snapshots_rewritten,
        new_key,
    })
}

fn rewrite_manifest_payload_keys(
    manifest: &mut SnapshotManifest,
    blob_keys: &BTreeMap<String, String>,
    large_manifest_keys: &BTreeMap<String, String>,
) -> Result<()> {
    for records in manifest.roots.values_mut() {
        for record in records {
            if let Some((oid, object_key)) = payload_blob_ref_mut(&mut record.payload) {
                *object_key = blob_keys
                    .get(oid)
                    .ok_or_else(|| anyhow!("missing rotated blob key {oid}"))?
                    .clone();
            } else if let Some((oid, manifest_key)) = payload_large_ref_mut(&mut record.payload) {
                *manifest_key = large_manifest_keys
                    .get(oid)
                    .ok_or_else(|| anyhow!("missing rotated large manifest key {oid}"))?
                    .clone();
            }
        }
    }
    Ok(())
}

fn pack_cmd(paths: &Paths, args: PackArgs) -> Result<()> {
    if args.compact {
        return pack_compact_cmd(paths);
    }
    pack_loose_blobs(paths)
}

fn pack_loose_blobs(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let blobs = query_blobs(&conn)?
        .into_iter()
        .filter(|blob| blob.pack_id.is_none())
        .collect::<Vec<_>>();
    if blobs.is_empty() {
        println!("packed 0 objects");
        return Ok(());
    }
    let packed = write_tiered_blob_packs(paths, &conn, &config.pack, &blobs, |blob| {
        read_object(paths, &blob.object_key)
    })?;
    record_op(
        &conn,
        "pack",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!("packed {} blobs", blobs.len())),
    )?;
    println!(
        "packed {} objects into {} pack(s)",
        blobs.len(),
        packed.len()
    );
    Ok(())
}

fn write_blob_packs<F>(
    paths: &Paths,
    conn: &Connection,
    blobs: &[BlobExport],
    tier: PackTier,
    target_size: u64,
    mut payload_for: F,
) -> Result<Vec<PackIndex>>
where
    F: FnMut(&BlobExport) -> Result<Vec<u8>>,
{
    let target_size = target_size.max(1);
    let mut indexes = Vec::new();
    let mut pack_id = new_id("pack");
    let prefixes = majutsu_pack::date_prefixes(tier, Utc::now());
    let mut pack_key = majutsu_pack::pack_key(&prefixes.pack_prefix, &pack_id);
    let mut index_key = majutsu_pack::index_key(&prefixes.index_prefix, &pack_id);
    let mut pack_bytes = Vec::new();
    let mut entries = Vec::new();
    let mut object_count = 0usize;
    for blob in blobs {
        let payload = payload_for(blob)?;
        let stored = encode_object(paths, &payload)?;
        let record_len = 8 + stored.len() as u64;
        if !entries.is_empty() && pack_bytes.len() as u64 + record_len > target_size {
            indexes.push(finish_pack(
                paths,
                conn,
                pack_id,
                pack_key,
                index_key,
                pack_bytes,
                entries,
                object_count,
            )?);
            pack_id = new_id("pack");
            pack_key = majutsu_pack::pack_key(&prefixes.pack_prefix, &pack_id);
            index_key = majutsu_pack::index_key(&prefixes.index_prefix, &pack_id);
            pack_bytes = Vec::new();
            entries = Vec::new();
            object_count = 0;
        }
        let offset = pack_bytes.len() as u64;
        pack_bytes.extend_from_slice(&(stored.len() as u64).to_le_bytes());
        pack_bytes.extend_from_slice(&stored);
        entries.push(PackEntry {
            oid: blob.oid.clone(),
            offset,
            len: 8 + stored.len() as u64,
        });
        object_count += 1;
    }
    if !entries.is_empty() {
        indexes.push(finish_pack(
            paths,
            conn,
            pack_id,
            pack_key,
            index_key,
            pack_bytes,
            entries,
            object_count,
        )?);
    }
    Ok(indexes)
}

fn write_tiered_blob_packs<F>(
    paths: &Paths,
    conn: &Connection,
    config: &PackConfig,
    blobs: &[BlobExport],
    mut payload_for: F,
) -> Result<Vec<PackIndex>>
where
    F: FnMut(&BlobExport) -> Result<Vec<u8>>,
{
    let (small_blobs, normal_blobs): (Vec<_>, Vec<_>) = blobs
        .iter()
        .cloned()
        .partition(|blob| majutsu_pack::tier_for_blob(blob.size) == PackTier::Small);
    let mut indexes = Vec::new();
    indexes.extend(write_blob_packs(
        paths,
        conn,
        &small_blobs,
        PackTier::Small,
        config.small_pack_target,
        |blob| payload_for(blob),
    )?);
    indexes.extend(write_blob_packs(
        paths,
        conn,
        &normal_blobs,
        PackTier::Normal,
        config.normal_pack_target,
        |blob| payload_for(blob),
    )?);
    Ok(indexes)
}

fn finish_pack(
    paths: &Paths,
    conn: &Connection,
    pack_id: String,
    pack_key: String,
    index_key: String,
    pack_bytes: Vec<u8>,
    entries: Vec<PackEntry>,
    object_count: usize,
) -> Result<PackIndex> {
    let pack_path = paths.home.join(&pack_key);
    if let Some(parent) = pack_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&pack_path, &pack_bytes)?;
    let index = PackIndex {
        version: 1,
        pack_id: pack_id.clone(),
        pack_key: pack_key.clone(),
        entries,
    };
    let index_path = paths.home.join(&index_key);
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&index_path, serde_json::to_vec_pretty(&index)?)?;
    conn.execute(
        "insert or replace into packs(pack_id, pack_key, index_key, object_count, size) values (?1, ?2, ?3, ?4, ?5)",
        params![pack_id, pack_key, index_key, object_count, pack_bytes.len() as u64],
    )?;
    for entry in &index.entries {
        conn.execute(
            "update blobs set pack_id=?2, pack_offset=?3, pack_len=?4 where oid=?1",
            params![entry.oid, index.pack_id, entry.offset, entry.len],
        )?;
    }
    Ok(index)
}

fn pack_compact_cmd(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let blobs = query_blobs(&conn)?;
    let packed = blobs.iter().filter(|blob| blob.pack_id.is_some()).count();
    if packed == 0 {
        println!("compacted 0 objects");
        return Ok(());
    }
    let old_pack_ids = query_packs(&conn)?
        .into_iter()
        .map(|pack| pack.pack_id)
        .collect::<BTreeSet<_>>();
    let mut payloads = BTreeMap::new();
    for blob in &blobs {
        payloads.insert(
            blob.oid.clone(),
            read_blob_payload(paths, &conn, &blob.oid, &blob.object_key)?,
        );
    }
    let indexes = write_tiered_blob_packs(paths, &conn, &config.pack, &blobs, |blob| {
        payloads
            .get(&blob.oid)
            .cloned()
            .ok_or_else(|| anyhow!("missing compact payload {}", blob.oid))
    })?;
    let new_pack_ids = indexes
        .iter()
        .map(|index| index.pack_id.clone())
        .collect::<BTreeSet<_>>();
    for old_pack_id in old_pack_ids.difference(&new_pack_ids) {
        conn.execute("delete from packs where pack_id=?1", params![old_pack_id])?;
    }
    record_op(
        &conn,
        "pack-compact",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!("compacted {} blobs", blobs.len())),
    )?;
    println!(
        "compacted {} objects into {} pack(s)",
        blobs.len(),
        indexes.len()
    );
    Ok(())
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

fn prune_cmd(paths: &Paths, args: PruneArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let plan = build_prune_plan(&conn, &args)?;
    let total = plan.keep.len() + plan.delete.len();
    println!("snapshots {total}");
    println!("keep_daily {}", args.keep_daily);
    println!("keep_monthly {}", args.keep_monthly);
    println!("keep_snapshots {}", plan.keep.len());
    println!("candidate_snapshots {}", plan.delete.len());
    if args.dry_run {
        println!("dry_run true");
    } else {
        let before = current_snapshot(&conn)?;
        for snapshot in &plan.delete {
            conn.execute("delete from snapshots where id=?1", params![snapshot])?;
        }
        let removed = prune_unreferenced_metadata(paths, &conn)?;
        record_op(
            &conn,
            "prune",
            before.as_deref(),
            before.as_deref(),
            Some(&format!("deleted {} snapshots", plan.delete.len())),
        )?;
        println!("dry_run false");
        println!("deleted_snapshots {}", plan.delete.len());
        println!("removed_blob_metadata {}", removed.blobs);
        println!("removed_large_metadata {}", removed.large_objects);
        println!("removed_chunk_metadata {}", removed.chunks);
    }
    Ok(())
}

struct PrunePlan {
    keep: Vec<String>,
    delete: Vec<String>,
}

struct SnapshotPruneRow {
    id: String,
    created_at: DateTime<Utc>,
}

struct PrunedMetadata {
    blobs: usize,
    large_objects: usize,
    chunks: usize,
}

fn build_prune_plan(conn: &Connection, args: &PruneArgs) -> Result<PrunePlan> {
    let current = current_snapshot(conn)?;
    let mut stmt = conn.prepare("select id, created_at from snapshots order by created_at desc")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let snapshots = rows
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .map(|(id, created)| {
            Ok(SnapshotPruneRow {
                id,
                created_at: parse_db_time(&created)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut keep = std::collections::BTreeSet::new();
    if let Some(current) = current {
        keep.insert(current);
    }
    let mut daily = std::collections::BTreeSet::new();
    let mut monthly = std::collections::BTreeSet::new();
    for snapshot in &snapshots {
        let day = snapshot.created_at.format("%Y-%m-%d").to_string();
        if daily.len() < args.keep_daily as usize && daily.insert(day) {
            keep.insert(snapshot.id.clone());
        }
        let month = snapshot.created_at.format("%Y-%m").to_string();
        if monthly.len() < args.keep_monthly as usize && monthly.insert(month) {
            keep.insert(snapshot.id.clone());
        }
    }
    let mut keep = keep.into_iter().collect::<Vec<_>>();
    keep.sort();
    let delete = snapshots
        .into_iter()
        .map(|snapshot| snapshot.id)
        .filter(|id| !keep.binary_search(id).is_ok())
        .collect::<Vec<_>>();
    Ok(PrunePlan { keep, delete })
}

fn prune_unreferenced_metadata(paths: &Paths, conn: &Connection) -> Result<PrunedMetadata> {
    let mut live_blobs = std::collections::BTreeSet::new();
    let mut live_large = std::collections::BTreeSet::new();
    let mut live_chunks = std::collections::BTreeSet::new();
    let mut stmt = conn.prepare("select manifest_json from snapshots")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let manifest: SnapshotManifest = serde_json::from_str(&row?)?;
        for records in manifest.roots.values() {
            for record in records {
                if let Some((oid, _)) = payload_blob_ref(&record.payload) {
                    live_blobs.insert(oid.to_string());
                } else if let Some((oid, manifest_key, _)) = payload_large_ref(&record.payload) {
                    live_large.insert(oid.to_string());
                    let large_manifest: LargeManifest =
                        serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                    for chunk in large_manifest.chunks {
                        live_chunks.insert(chunk.oid);
                    }
                }
            }
        }
    }
    let blobs = delete_rows_not_in(conn, "blobs", "oid", &live_blobs)?;
    let large_objects = delete_rows_not_in(conn, "large_objects", "oid", &live_large)?;
    let chunks = delete_rows_not_in(conn, "chunks", "oid", &live_chunks)?;
    Ok(PrunedMetadata {
        blobs,
        large_objects,
        chunks,
    })
}

fn delete_rows_not_in(
    conn: &Connection,
    table: &str,
    column: &str,
    live: &std::collections::BTreeSet<String>,
) -> Result<usize> {
    let mut stmt = conn.prepare(&format!("select {column} from {table}"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut removed = 0usize;
    for row in rows {
        let id = row?;
        if !live.contains(&id) {
            conn.execute(
                &format!("delete from {table} where {column}=?1"),
                params![id],
            )?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn gc_cmd(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let config = read_config(paths)?;
    let export = export_metadata(&conn, &config)?;
    let referenced = local_object_keys(&export)
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut removed = 0usize;
    for key in all_local_object_keys(paths)? {
        if !referenced.contains(&key) {
            fs::remove_file(paths.home.join(&key))?;
            removed += 1;
        }
    }
    record_op(
        &conn,
        "gc",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!("removed {removed} unreferenced objects")),
    )?;
    println!("removed_unreferenced_objects {removed}");
    Ok(())
}

fn fsck(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let mut missing = 0usize;
    let config = read_config(paths)?;
    let export = export_metadata(&conn, &config)?;
    for key in local_object_keys(&export) {
        let full = paths.home.join(&key);
        if !full.exists() {
            missing += 1;
            eprintln!("missing object {key}");
        } else if let Err(err) = read_object(paths, &key) {
            missing += 1;
            eprintln!("unreadable object {key}: {err}");
        }
    }
    let mut stmt = conn.prepare("select oid, object_key, pack_id from blobs")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    for row in rows {
        let (oid, key, pack_id) = row?;
        if pack_id.is_some() {
            if let Err(err) = read_blob_payload(paths, &conn, &oid, &key) {
                missing += 1;
                eprintln!("unreadable packed blob {oid}: {err}");
            }
        } else if !paths.home.join(&key).exists() {
            missing += 1;
            eprintln!("missing blob {oid} {key}");
        } else if let Err(err) = read_object(paths, &key) {
            missing += 1;
            eprintln!("unreadable blob {oid} {key}: {err}");
        }
    }
    let mut stmt = conn.prepare("select oid, object_key from chunks")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (oid, key) = row?;
        if !paths.home.join(&key).exists() {
            missing += 1;
            eprintln!("missing chunk {oid} {key}");
        } else if let Err(err) = read_object(paths, &key) {
            missing += 1;
            eprintln!("unreadable chunk {oid} {key}: {err}");
        }
    }
    let mut stmt = conn.prepare("select oid, manifest_key from large_objects")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (oid, manifest_key) = row?;
        match read_object(paths, &manifest_key)
            .and_then(|bytes| serde_json::from_slice::<LargeManifest>(&bytes).map_err(Into::into))
        {
            Ok(manifest) => {
                for chunk in &manifest.chunks {
                    match read_large_chunk(paths, chunk) {
                        Ok(bytes) if blake3_hex(&bytes) == chunk.oid => {}
                        Ok(_) => {
                            missing += 1;
                            eprintln!("large chunk hash mismatch {} {}", oid, chunk.object_key);
                        }
                        Err(err) => {
                            missing += 1;
                            eprintln!("unreadable large chunk {} {}: {err}", oid, chunk.object_key);
                        }
                    }
                }
            }
            Err(err) => {
                missing += 1;
                eprintln!("unreadable large manifest {oid} {manifest_key}: {err}");
            }
        }
    }
    if missing > 0 {
        bail!("fsck found {missing} missing objects");
    }
    println!("fsck ok");
    Ok(())
}

fn remote_fsck(remote: &RemoteStore) -> Result<()> {
    let mut missing = 0usize;
    let mut verified_hosts = 0usize;
    let has_legacy_export = remote.exists("metadata/export.json")?;
    let has_host_index = remote.exists("hosts/index.json")?;

    if has_legacy_export {
        let export = remote_fsck_export(remote, "metadata/export.json", None, &mut missing)?;
        if let Some(current) = export.refs.get("current") {
            let legacy_current = remote_ref(remote, "hosts/current")?;
            if legacy_current.as_deref() != Some(current.as_str()) {
                missing += 1;
                eprintln!("legacy hosts/current does not match metadata current ref");
            }
        }
    }

    if has_host_index {
        let index = read_remote_host_index(remote)?;
        for host in &index.hosts {
            verified_hosts += 1;
            if !remote.exists(&host.metadata_key)? {
                missing += 1;
                eprintln!("missing host metadata {} {}", host.id, host.metadata_key);
                continue;
            }
            let export =
                remote_fsck_export(remote, &host.metadata_key, Some(&host.id), &mut missing)?;
            if export.config.host.id != host.id {
                missing += 1;
                eprintln!(
                    "host index id {} does not match metadata host id {}",
                    host.id, export.config.host.id
                );
            }
            if export.config.host.name != host.name {
                missing += 1;
                eprintln!(
                    "host index name {} does not match metadata host name {}",
                    host.name, export.config.host.name
                );
            }
            let current = export.refs.get("current");
            if host.current_snapshot.as_ref() != current {
                missing += 1;
                eprintln!(
                    "host index current snapshot does not match metadata for {}",
                    host.id
                );
            }
            let current_ref_key = format!("hosts/{}/refs/current", host.id);
            if let Some(current) = current {
                match remote_ref(remote, &current_ref_key)? {
                    Some(remote_current) if remote_current == *current => {}
                    Some(remote_current) => {
                        missing += 1;
                        eprintln!(
                            "{current_ref_key} points to {remote_current}, expected {current}"
                        );
                    }
                    None => {
                        missing += 1;
                        eprintln!("missing remote ref {current_ref_key}");
                    }
                }
                let legacy_current_key = format!("hosts/{}/current", host.id);
                if let Some(legacy_current) = remote_ref(remote, &legacy_current_key)? {
                    if legacy_current != *current {
                        missing += 1;
                        eprintln!(
                            "{legacy_current_key} points to {legacy_current}, expected {current}"
                        );
                    }
                }
            }
            if let Some(last_synced) = export.refs.get("last-synced") {
                match parse_db_time(last_synced) {
                    Ok(metadata_last_synced) if host.last_synced_at == metadata_last_synced => {}
                    Ok(metadata_last_synced) => {
                        missing += 1;
                        eprintln!(
                            "host index last_synced_at {} does not match metadata last-synced {} for {}",
                            host.last_synced_at.to_rfc3339(),
                            metadata_last_synced.to_rfc3339(),
                            host.id
                        );
                    }
                    Err(err) => {
                        missing += 1;
                        eprintln!("invalid metadata last-synced for {}: {err}", host.id);
                    }
                }
                let last_synced_ref_key = format!("hosts/{}/refs/last-synced", host.id);
                match remote_ref(remote, &last_synced_ref_key)? {
                    Some(remote_last_synced) if remote_last_synced == *last_synced => {}
                    Some(remote_last_synced) => {
                        missing += 1;
                        eprintln!(
                            "{last_synced_ref_key} points to {remote_last_synced}, expected {last_synced}"
                        );
                    }
                    None => {
                        missing += 1;
                        eprintln!("missing remote ref {last_synced_ref_key}");
                    }
                }
            }
            for snapshot in &export.snapshots {
                let key = host_snapshot_canonical_key(&host.id, &snapshot.id);
                if !remote.exists(&key)? {
                    missing += 1;
                    eprintln!("missing canonical host snapshot export {key}");
                }
            }
            for operation in &export.operations {
                let key = host_operation_canonical_key(&host.id, &operation.id);
                if !remote.exists(&key)? {
                    missing += 1;
                    eprintln!("missing canonical host operation export {key}");
                }
            }
        }
    }

    if !has_legacy_export && !has_host_index {
        bail!("remote metadata is missing: metadata/export.json and hosts/index.json not found");
    }
    if has_host_index && verified_hosts == 0 {
        missing += 1;
        eprintln!("hosts/index.json contains no hosts");
    }
    if missing > 0 {
        bail!("remote fsck found {missing} issue(s)");
    }
    println!("remote fsck ok");
    println!("hosts {}", verified_hosts);
    if has_legacy_export {
        println!("legacy_metadata ok");
    }
    Ok(())
}

fn remote_fsck_export(
    remote: &RemoteStore,
    metadata_key: &str,
    host_id: Option<&str>,
    missing: &mut usize,
) -> Result<MetadataExport> {
    let export: MetadataExport = serde_json::from_slice(&remote.get(metadata_key)?)
        .with_context(|| format!("parse remote metadata {metadata_key}"))?;
    if !export.chunks.is_empty() && !remote.exists(CHUNK_INDEX_SHARD_KEY)? {
        *missing += 1;
        eprintln!("missing remote chunk index shard {CHUNK_INDEX_SHARD_KEY}");
    }
    for key in local_object_keys(&export) {
        let legacy_exists = remote.exists(&key)?;
        let aliases = canonical_remote_aliases(&key);
        let alias_exists = aliases
            .iter()
            .map(|alias| remote.exists(alias))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .any(|exists| exists);
        if !legacy_exists && !alias_exists {
            *missing += 1;
            eprintln!("missing remote object {key} or canonical alias");
        }
        if legacy_exists && !aliases.is_empty() && !alias_exists {
            *missing += 1;
            eprintln!("missing canonical remote object alias for {key}");
        }
    }
    if let Some(current) = export.refs.get("current") {
        let found = export
            .snapshots
            .iter()
            .any(|snapshot| &snapshot.id == current);
        if !found {
            *missing += 1;
            eprintln!("remote current ref points to missing snapshot {current}");
        }
    }
    if let Some(host_id) = host_id {
        for snapshot in &export.snapshots {
            let key = host_snapshot_key(host_id, &snapshot.id);
            if !remote.exists(&key)? {
                continue;
            }
            let remote_snapshot: SnapshotExport = serde_json::from_slice(&remote.get(&key)?)
                .with_context(|| format!("parse remote snapshot export {key}"))?;
            if remote_snapshot.id != snapshot.id
                || remote_snapshot.manifest_key != snapshot.manifest_key
                || remote_snapshot.op_id != snapshot.op_id
            {
                *missing += 1;
                eprintln!("host snapshot export does not match metadata {key}");
            }
        }
        for operation in &export.operations {
            let key = host_operation_key(host_id, &operation.id);
            if !remote.exists(&key)? {
                continue;
            }
            let remote_operation: OperationExport = serde_json::from_slice(&remote.get(&key)?)
                .with_context(|| format!("parse remote operation export {key}"))?;
            if remote_operation.id != operation.id
                || remote_operation.kind != operation.kind
                || remote_operation.after_snapshot != operation.after_snapshot
            {
                *missing += 1;
                eprintln!("host operation export does not match metadata {key}");
            }
        }
    }
    Ok(export)
}

#[derive(Debug)]
struct RestorePlan {
    snapshot: SnapshotManifest,
    to: Option<PathBuf>,
    root_paths: BTreeMap<String, PathBuf>,
    files: Vec<FileRecord>,
    deletes: Vec<RestoreDelete>,
}

#[derive(Debug)]
struct RestoreDelete {
    root_id: String,
    path: String,
}

struct RestoreObjectStats {
    required_objects: usize,
    required_chunks: usize,
    local_objects: usize,
    remote_objects: usize,
    archive_or_missing_objects: usize,
}

fn build_restore_plan(
    _paths: &Paths,
    conn: &Connection,
    args: &RestoreArgs,
) -> Result<RestorePlan> {
    if let Some(path) = &args.path {
        validate_relative_filter_path(path, "restore --path")?;
    }
    let snapshot = load_snapshot(conn, args)?;
    let root_paths = roots(conn)?
        .into_iter()
        .map(|root| (root.id, root.path))
        .collect::<BTreeMap<_, _>>();
    let mut files = Vec::new();
    let mut plan_roots = Vec::new();
    for (root_id, records) in &snapshot.roots {
        if let Some(filter_root) = &args.root {
            if filter_root != root_id {
                continue;
            }
        }
        plan_roots.push(root_id.clone());
        for record in records {
            if let Some(path_filter) = &args.path {
                if !Path::new(&record.path).starts_with(path_filter) {
                    continue;
                }
            }
            files.push(FileRecord {
                root_id: record.root_id.clone(),
                path: record.path.clone(),
                kind: record.kind.clone(),
                size: record.size,
                mode: record.mode,
                modified: record.modified,
                xattrs: record.xattrs.clone(),
                payload: match &record.payload {
                    Payload::Directory => Payload::Directory,
                    Payload::InlineSmall { oid, object_key } => Payload::InlineSmall {
                        oid: oid.clone(),
                        object_key: object_key.clone(),
                    },
                    Payload::NormalBlob { oid, object_key } => Payload::NormalBlob {
                        oid: oid.clone(),
                        object_key: object_key.clone(),
                    },
                    Payload::ChunkedBlob {
                        oid,
                        manifest_key,
                        chunk_count,
                    } => Payload::ChunkedBlob {
                        oid: oid.clone(),
                        manifest_key: manifest_key.clone(),
                        chunk_count: *chunk_count,
                    },
                    Payload::LargeObject {
                        oid,
                        manifest_key,
                        chunk_count,
                        media_type,
                        binary,
                        chunking,
                        compression,
                        encryption,
                        storage_tier_hint,
                        hydrate_policy,
                    } => Payload::LargeObject {
                        oid: oid.clone(),
                        manifest_key: manifest_key.clone(),
                        chunk_count: *chunk_count,
                        media_type: media_type.clone(),
                        binary: *binary,
                        chunking: chunking.clone(),
                        compression: compression.clone(),
                        encryption: encryption.clone(),
                        storage_tier_hint: storage_tier_hint.clone(),
                        hydrate_policy: hydrate_policy.clone(),
                    },
                    Payload::Blob { oid, object_key } => Payload::Blob {
                        oid: oid.clone(),
                        object_key: object_key.clone(),
                    },
                    Payload::Large {
                        oid,
                        manifest_key,
                        chunk_count,
                    } => Payload::Large {
                        oid: oid.clone(),
                        manifest_key: manifest_key.clone(),
                        chunk_count: *chunk_count,
                    },
                    Payload::Symlink { target } => Payload::Symlink {
                        target: target.clone(),
                    },
                    Payload::Special { special_kind } => Payload::Special {
                        special_kind: special_kind.clone(),
                    },
                },
            });
        }
    }
    let deletes = build_restore_deletes(args, &root_paths, &plan_roots, &files)?;
    Ok(RestorePlan {
        snapshot,
        to: args.to.clone(),
        root_paths,
        files,
        deletes,
    })
}

fn restore_destination(plan: &RestorePlan, record: &FileRecord) -> Result<PathBuf> {
    if let Some(to) = &plan.to {
        return Ok(to.join(&record.root_id).join(&record.path));
    }
    let root = plan.root_paths.get(&record.root_id).ok_or_else(|| {
        anyhow!(
            "snapshot root is not configured locally: {}",
            record.root_id
        )
    })?;
    Ok(root.join(&record.path))
}

fn build_restore_deletes(
    args: &RestoreArgs,
    root_paths: &BTreeMap<String, PathBuf>,
    root_ids: &[String],
    files: &[FileRecord],
) -> Result<Vec<RestoreDelete>> {
    let mut snapshot_paths: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for record in files {
        snapshot_paths
            .entry(record.root_id.clone())
            .or_default()
            .insert(record.path.clone());
    }
    let mut deletes = Vec::new();
    for root_id in root_ids {
        if let Some(filter_root) = &args.root {
            if filter_root != root_id {
                continue;
            }
        }
        let base = restore_root_base(args, root_paths, root_id)?;
        let scan_base = args
            .path
            .as_ref()
            .map(|path| base.join(path))
            .unwrap_or_else(|| base.clone());
        if !scan_base.try_exists()? {
            continue;
        }
        for entry in WalkDir::new(&scan_base).follow_links(false) {
            let entry = entry?;
            if entry.file_type().is_dir() {
                continue;
            }
            let rel = entry.path().strip_prefix(&base)?.to_path_buf();
            let rel_s = path_to_slash(&rel);
            if !snapshot_paths
                .get(root_id)
                .map(|paths| paths.contains(&rel_s))
                .unwrap_or(false)
            {
                deletes.push(RestoreDelete {
                    root_id: root_id.clone(),
                    path: rel_s,
                });
            }
        }
    }
    deletes.sort_by(|a, b| {
        a.root_id
            .cmp(&b.root_id)
            .then_with(|| b.path.len().cmp(&a.path.len()))
            .then_with(|| a.path.cmp(&b.path))
    });
    Ok(deletes)
}

fn restore_root_base(
    args: &RestoreArgs,
    root_paths: &BTreeMap<String, PathBuf>,
    root_id: &str,
) -> Result<PathBuf> {
    if let Some(to) = &args.to {
        return Ok(to.join(root_id));
    }
    root_paths
        .get(root_id)
        .cloned()
        .ok_or_else(|| anyhow!("snapshot root is not configured locally: {root_id}"))
}

fn restore_delete_destination(plan: &RestorePlan, delete: &RestoreDelete) -> Result<PathBuf> {
    if let Some(to) = &plan.to {
        return Ok(to.join(&delete.root_id).join(&delete.path));
    }
    let root = plan.root_paths.get(&delete.root_id).ok_or_else(|| {
        anyhow!(
            "snapshot root is not configured locally: {}",
            delete.root_id
        )
    })?;
    Ok(root.join(&delete.path))
}

fn restore_target_label(plan: &RestorePlan) -> String {
    plan.to
        .as_ref()
        .map(|to| to.display().to_string())
        .unwrap_or_else(|| "original-roots".into())
}

fn validate_relative_filter_path(path: &Path, label: &str) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        bail!("{label} must be a relative path inside the selected root");
    }
    let mut has_component = false;
    for component in path.components() {
        match component {
            std::path::Component::Normal(_) => has_component = true,
            _ => bail!("{label} must not contain '.', '..', prefixes, or root separators"),
        }
    }
    if !has_component {
        bail!("{label} must not be empty");
    }
    Ok(())
}

fn print_restore_plan(paths: &Paths, conn: &Connection, plan: &RestorePlan) -> Result<()> {
    let large = plan
        .files
        .iter()
        .filter(|r| payload_large_ref(&r.payload).is_some())
        .count();
    let bytes: u64 = plan.files.iter().map(|r| r.size).sum();
    let changes = restore_change_stats(paths, conn, plan)?;
    println!("snapshot {}", plan.snapshot.snapshot_id);
    if let Some(to) = &plan.to {
        println!("target {}", to.display());
    } else {
        println!("target original-roots");
    }
    println!(
        "restore {} files, {} bytes, {} large files",
        plan.files.len(),
        bytes,
        large
    );
    println!("delete {} files", plan.deletes.len());
    println!("restore_files {}", changes.restore_files);
    println!("modify_files {}", changes.modify_files);
    println!("keep_files {}", changes.keep_files);
    println!("delete_files {}", changes.delete_files);
    let stats = restore_object_stats(paths, conn, plan)?;
    println!("large_files {large}");
    println!("required_objects {}", stats.required_objects);
    println!("required_chunks {}", stats.required_chunks);
    println!("local_objects {}", stats.local_objects);
    println!("remote_objects {}", stats.remote_objects);
    println!(
        "archive_or_missing_objects {}",
        stats.archive_or_missing_objects
    );
    Ok(())
}

fn restore_change_stats(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<RestoreChangeStats> {
    count_restore_changes(&plan.files, plan.deletes.len(), |record| {
        let dest = restore_destination(plan, record)?;
        if !dest.try_exists()? {
            Ok(RestorePathState::Missing)
        } else if restore_record_matches_path(paths, conn, record, &dest).unwrap_or(false) {
            Ok(RestorePathState::Matches)
        } else {
            Ok(RestorePathState::Differs)
        }
    })
}

fn restore_object_stats(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<RestoreObjectStats> {
    let required_objects = required_object_keys_for_plan(paths, conn, plan)?;
    let required_chunks = required_chunk_count_for_plan(paths, plan)?;
    let local_objects = required_objects
        .iter()
        .filter(|key| paths.home.join(key).exists())
        .count();
    let remote = read_config(paths)
        .ok()
        .and_then(|config| config.remote.and_then(|remote| open_remote(&remote).ok()));
    let mut remote_objects = 0usize;
    if let Some(remote) = remote.as_ref() {
        for key in &required_objects {
            if remote_object_available(remote, key)? {
                remote_objects += 1;
            }
        }
    }
    let archive_or_missing_objects = required_objects.len().saturating_sub(
        required_objects
            .iter()
            .filter(|key| {
                paths.home.join(key).exists()
                    || remote
                        .as_ref()
                        .and_then(|remote| remote_object_available(remote, key).ok())
                        .unwrap_or(false)
            })
            .count(),
    );
    Ok(RestoreObjectStats {
        required_objects: required_objects.len(),
        required_chunks,
        local_objects,
        remote_objects,
        archive_or_missing_objects,
    })
}

fn required_chunk_count_for_plan(paths: &Paths, plan: &RestorePlan) -> Result<usize> {
    let mut chunks = 0usize;
    for record in &plan.files {
        if let Some((_, manifest_key, _)) = payload_large_ref(&record.payload) {
            let manifest: LargeManifest =
                serde_json::from_slice(&read_object(paths, manifest_key)?)?;
            chunks += manifest.chunks.len();
        }
    }
    Ok(chunks)
}

fn build_restore_job(
    paths: &Paths,
    plan: &RestorePlan,
    args: &RestoreArgs,
) -> Result<RestoreQueueItem> {
    let conn = open_db(paths)?;
    let required_objects = required_object_keys_for_plan(paths, &conn, plan)?;
    let remote = read_config(paths)
        .ok()
        .and_then(|config| config.remote.and_then(|remote| open_remote(&remote).ok()));
    let mut archived_objects = Vec::new();
    let mut missing_objects = Vec::new();
    for key in &required_objects {
        if paths.home.join(key).exists() {
            continue;
        }
        if remote
            .as_ref()
            .and_then(|remote| remote_object_available(remote, key).ok())
            .unwrap_or(false)
        {
            archived_objects.push(key.clone());
        } else {
            missing_objects.push(key.clone());
        }
    }
    Ok(RestoreQueueItem {
        id: new_id("restore"),
        snapshot_id: plan.snapshot.snapshot_id.clone(),
        root: args.root.clone(),
        path: args.path.as_ref().map(|path| path_to_slash(path)),
        target: args
            .to
            .as_ref()
            .map(|to| to.display().to_string())
            .unwrap_or_else(|| "original-roots".into()),
        required_objects,
        archived_objects,
        missing_objects,
        archive_requested_objects: Vec::new(),
        force: args.force,
        check_conflicts: args.check_conflicts,
        created_at: Utc::now(),
        status: "prepared".into(),
    })
}

fn request_archive_restore_for_job(paths: &Paths, job: &mut RestoreQueueItem) -> Result<()> {
    if job.archived_objects.is_empty() {
        return Ok(());
    }
    let config = read_config(paths)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(());
    };
    let remote = open_remote(remote_config)?;
    let mut requested = Vec::new();
    for key in &job.archived_objects {
        let restore_key = remote_available_key(&remote, key)?;
        if remote.restore_archive(&restore_key, 7, "Standard")? {
            requested.push(key.clone());
        }
    }
    if !requested.is_empty() {
        job.status = "archive-requested".into();
        job.archive_requested_objects = requested;
    }
    Ok(())
}

fn hydrate_restore_job_objects(paths: &Paths, job: &mut RestoreQueueItem) -> Result<()> {
    if job.archived_objects.is_empty() {
        return Ok(());
    }
    let config = read_config(paths)?;
    let Some(remote_config) = config.remote.as_ref() else {
        return Ok(());
    };
    let remote = open_remote(remote_config)?;
    let mut still_pending = Vec::new();
    let mut hydrated = Vec::new();
    for key in &job.archived_objects {
        let dest = paths.home.join(key);
        if dest.exists() {
            hydrated.push(key.clone());
            continue;
        }
        if !remote_object_available(&remote, key)? {
            still_pending.push(key.clone());
            continue;
        }
        match download_local_object_from_remote(paths, &remote, key) {
            Ok(bytes) => {
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&dest, bytes).with_context(|| format!("hydrate restore object {key}"))?;
                hydrated.push(key.clone());
            }
            Err(_) => still_pending.push(key.clone()),
        }
    }
    job.archived_objects = still_pending;
    job.archive_requested_objects
        .retain(|key| job.archived_objects.contains(key));
    if job.archived_objects.is_empty() {
        job.status = "ready".into();
    }
    write_restore_job(paths, job)?;
    if !hydrated.is_empty() {
        record_event(
            paths,
            "restore-hydrate",
            &format!("{} hydrated_objects={}", job.id, hydrated.len()),
        )?;
    }
    Ok(())
}

fn required_object_keys_for_plan(
    paths: &Paths,
    conn: &Connection,
    plan: &RestorePlan,
) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for record in &plan.files {
        if let Some((oid, object_key)) = payload_blob_ref(&record.payload) {
            let blob = query_blobs(conn)?
                .into_iter()
                .find(|blob| blob.oid == oid)
                .ok_or_else(|| anyhow!("missing blob metadata for {oid}"))?;
            if let Some(pack_id) = blob.pack_id {
                let pack: PackExport = conn.query_row(
                    "select pack_id, pack_key, index_key, object_count, size from packs where pack_id=?1",
                    params![pack_id],
                    |row| {
                        Ok(PackExport {
                            pack_id: row.get(0)?,
                            pack_key: row.get(1)?,
                            index_key: row.get(2)?,
                            object_count: row.get(3)?,
                            size: row.get(4)?,
                        })
                    },
                )?;
                keys.push(pack.pack_key);
                keys.push(pack.index_key);
            } else {
                keys.push(object_key.to_string());
            }
        } else if let Some((_, manifest_key, _)) = payload_large_ref(&record.payload) {
            keys.push(manifest_key.to_string());
            let manifest: LargeManifest =
                serde_json::from_slice(&read_object(paths, manifest_key)?)?;
            for chunk in manifest.chunks {
                keys.push(chunk.object_key);
            }
        }
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn write_restore_job(paths: &Paths, job: &RestoreQueueItem) -> Result<()> {
    let dir = paths.home.join("queue/restores");
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join(format!("{}.json", job.id)),
        serde_json::to_vec_pretty(job)?,
    )?;
    Ok(())
}

fn read_restore_job(paths: &Paths, job_id: &str) -> Result<RestoreQueueItem> {
    let path = paths
        .home
        .join("queue/restores")
        .join(format!("{job_id}.json"));
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn ensure_restore_job_resumable(job: &RestoreQueueItem) -> Result<()> {
    match job.status.as_str() {
        "prepared" | "ready" | "archive-requested" => Ok(()),
        other => bail!("restore job {} is not resumable: status {other}", job.id),
    }
}

fn mark_restore_job_done(paths: &Paths, job_id: &str) -> Result<()> {
    let mut job = read_restore_job(paths, job_id)?;
    job.status = "done".into();
    write_restore_job(paths, &job)
}

fn apply_restore_plan(
    paths: &Paths,
    plan: &RestorePlan,
    force: bool,
    check_conflicts: bool,
) -> Result<()> {
    let conn = open_db(paths)?;
    if check_conflicts && !force {
        let conflicts = restore_conflicts(paths, &conn, plan)?;
        if !conflicts.is_empty() {
            print_restore_conflicts(&conflicts);
            bail!("restore has conflicts; rerun with --force to overwrite");
        }
        if !plan.deletes.is_empty() {
            print_restore_deletes(plan);
            bail!("restore would delete extra files; rerun with --force to delete them");
        }
    }
    for delete in &plan.deletes {
        let dest = restore_delete_destination(plan, delete)?;
        if fs::symlink_metadata(&dest).is_ok() {
            fs::remove_file(&dest)?;
            remove_empty_restore_parents(plan, delete, &dest)?;
        }
    }
    let mut directory_metadata = Vec::new();
    for record in &plan.files {
        let dest = restore_destination(plan, record)?;
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        match &record.payload {
            Payload::Directory => {
                prepare_directory_restore_destination(&dest, force)?;
                fs::create_dir_all(&dest)?;
                directory_metadata.push((dest, record));
            }
            Payload::Special { special_kind } => {
                restore_special_file(&dest, record, special_kind, force)?;
            }
            Payload::Symlink { target } => {
                restore_symlink(&dest, target, force)?;
            }
            payload => {
                if let Some((oid, object_key)) = payload_blob_ref(payload) {
                    prepare_file_restore_destination(&dest, force)?;
                    write_atomic(&dest, &read_blob_payload(paths, &conn, oid, object_key)?)?;
                    apply_file_metadata(&dest, record)?;
                } else if let Some((_, manifest_key, _)) = payload_large_ref(payload) {
                    prepare_file_restore_destination(&dest, force)?;
                    let manifest: LargeManifest =
                        serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                    write_large_chunks_atomic(paths, &dest, &manifest)?;
                    apply_file_metadata(&dest, record)?;
                }
            }
        }
    }
    for (dest, record) in directory_metadata {
        apply_file_metadata(&dest, record)?;
    }
    Ok(())
}

fn remove_empty_restore_parents(
    plan: &RestorePlan,
    delete: &RestoreDelete,
    path: &Path,
) -> Result<()> {
    let Some(mut current) = path.parent().map(Path::to_path_buf) else {
        return Ok(());
    };
    let stop = if let Some(to) = &plan.to {
        to.join(&delete.root_id)
    } else {
        plan.root_paths
            .get(&delete.root_id)
            .cloned()
            .unwrap_or_else(|| PathBuf::from("/"))
    };
    while current.starts_with(&stop) && current != stop {
        if fs::remove_dir(&current).is_err() {
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }
    Ok(())
}

fn restore_conflicts(paths: &Paths, conn: &Connection, plan: &RestorePlan) -> Result<Vec<String>> {
    let mut conflicts = Vec::new();
    for record in &plan.files {
        let dest = restore_destination(plan, record)?;
        if !dest.try_exists()? {
            continue;
        }
        if !restore_record_matches_path(paths, conn, record, &dest)? {
            conflicts.push(format!("{}\t{}", record.root_id, record.path));
        }
    }
    Ok(conflicts)
}

fn restore_record_matches_path(
    paths: &Paths,
    conn: &Connection,
    record: &FileRecord,
    dest: &Path,
) -> Result<bool> {
    let meta = fs::symlink_metadata(dest)?;
    match &record.payload {
        Payload::Directory => Ok(meta.file_type().is_dir()),
        Payload::Special { special_kind } => restore_special_matches(&meta, special_kind),
        Payload::Symlink { target } => {
            #[cfg(unix)]
            {
                if !meta.file_type().is_symlink() {
                    return Ok(false);
                }
                Ok(fs::read_link(dest)?.as_os_str() == OsStr::new(target))
            }
            #[cfg(not(unix))]
            {
                if !meta.file_type().is_file() {
                    return Ok(false);
                }
                Ok(fs::read_to_string(dest)? == *target)
            }
        }
        payload => {
            if let Some((oid, object_key)) = payload_blob_ref(payload) {
                if !meta.file_type().is_file() {
                    return Ok(false);
                }
                Ok(fs::read(dest)? == read_blob_payload(paths, conn, oid, object_key)?)
            } else if let Some((_, manifest_key, _)) = payload_large_ref(payload) {
                if !meta.file_type().is_file() || meta.len() != record.size {
                    return Ok(false);
                }
                let manifest: LargeManifest =
                    serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                let mut current = File::open(dest)?;
                for chunk in manifest.chunks {
                    let expected = read_large_chunk(paths, &chunk)?;
                    let mut actual = vec![0u8; expected.len()];
                    current.read_exact(&mut actual)?;
                    if actual != expected {
                        return Ok(false);
                    }
                }
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

fn print_restore_conflicts(conflicts: &[String]) {
    println!("conflicts {}", conflicts.len());
    for conflict in conflicts.iter().take(20) {
        println!("conflict\t{conflict}");
    }
    if conflicts.len() > 20 {
        println!("conflict\t... {} more", conflicts.len() - 20);
    }
}

fn print_restore_deletes(plan: &RestorePlan) {
    println!("deletes {}", plan.deletes.len());
    for delete in plan.deletes.iter().take(20) {
        println!("delete\t{}\t{}", delete.root_id, delete.path);
    }
    if plan.deletes.len() > 20 {
        println!("delete\t... {} more", plan.deletes.len() - 20);
    }
}

fn restore_symlink(dest: &Path, target: &str, force: bool) -> Result<()> {
    if let Ok(meta) = fs::symlink_metadata(dest) {
        if !force {
            bail!("symlink restore target exists: {}", dest.display());
        }
        if meta.file_type().is_dir() {
            bail!("symlink restore target is a directory: {}", dest.display());
        }
        fs::remove_file(dest)?;
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, dest)?;
    #[cfg(not(unix))]
    fs::write(dest, target)?;
    Ok(())
}

fn prepare_file_restore_destination(dest: &Path, force: bool) -> Result<()> {
    if fs::symlink_metadata(dest)
        .map(|meta| meta.file_type().is_dir())
        .unwrap_or(false)
    {
        if !force {
            bail!("restore target is a directory: {}", dest.display());
        }
        fs::remove_dir(dest)
            .with_context(|| format!("remove empty restore target directory {}", dest.display()))?;
    }
    Ok(())
}

fn prepare_directory_restore_destination(dest: &Path, force: bool) -> Result<()> {
    let Ok(meta) = fs::symlink_metadata(dest) else {
        return Ok(());
    };
    if meta.file_type().is_dir() {
        return Ok(());
    }
    if !force {
        bail!("directory restore target exists: {}", dest.display());
    }
    fs::remove_file(dest)?;
    Ok(())
}

fn apply_file_metadata(dest: &Path, record: &FileRecord) -> Result<()> {
    apply_xattrs(dest, &record.xattrs)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if record.mode != 0 {
            fs::set_permissions(dest, fs::Permissions::from_mode(record.mode & 0o7777))?;
        }
    }
    if let Some(seconds) = record.modified {
        set_path_mtime(dest, seconds)?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_path_mtime(path: &Path, seconds: i64) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let raw_path = CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("invalid mtime path {}", path.display()))?;
    let times = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
        libc::timespec {
            tv_sec: seconds as libc::time_t,
            tv_nsec: 0,
        },
    ];
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, raw_path.as_ptr(), times.as_ptr(), 0) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("set mtime {}", path.display()));
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_path_mtime(path: &Path, seconds: i64) -> Result<()> {
    filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(seconds, 0))?;
    Ok(())
}

#[cfg(unix)]
fn restore_special_file(
    dest: &Path,
    record: &FileRecord,
    special_kind: &str,
    force: bool,
) -> Result<()> {
    if special_kind != "fifo" {
        bail!(
            "restore of special file kind {special_kind} is not supported: {}",
            dest.display()
        );
    }
    if let Ok(meta) = fs::symlink_metadata(dest) {
        if restore_special_matches(&meta, special_kind)? {
            apply_file_metadata(dest, record)?;
            return Ok(());
        }
        if force {
            fs::remove_file(dest)?;
        } else {
            bail!("special file restore target exists: {}", dest.display());
        }
    }
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let raw_path = CString::new(dest.as_os_str().as_bytes())
        .with_context(|| format!("invalid fifo path {}", dest.display()))?;
    let mode = if record.mode == 0 {
        0o666
    } else {
        record.mode & 0o7777
    };
    let rc = unsafe { libc::mkfifo(raw_path.as_ptr(), mode as libc::mode_t) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("create fifo {}", dest.display()));
    }
    apply_file_metadata(dest, record)
}

#[cfg(not(unix))]
fn restore_special_file(
    dest: &Path,
    _record: &FileRecord,
    special_kind: &str,
    _force: bool,
) -> Result<()> {
    bail!(
        "restore of special file kind {special_kind} is not supported on this platform: {}",
        dest.display()
    )
}

fn restore_special_matches(meta: &fs::Metadata, special_kind: &str) -> Result<bool> {
    Ok(special_file_kind(meta).as_deref() == Some(special_kind))
}

fn read_xattrs(path: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(names) = xattr::list(path) else {
        return out;
    };
    for name in names {
        let name_s = name.to_string_lossy().to_string();
        let Ok(Some(value)) = xattr::get(path, &name) else {
            continue;
        };
        out.insert(
            name_s,
            base64::engine::general_purpose::STANDARD.encode(value),
        );
    }
    out
}

fn apply_xattrs(path: &Path, xattrs: &BTreeMap<String, String>) -> Result<()> {
    for (name, encoded) in xattrs {
        let value = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .with_context(|| format!("decode xattr {name}"))?;
        if xattr::set(path, name, &value).is_err() {
            continue;
        }
    }
    Ok(())
}

fn scan_root(paths: &Paths, config: &Config, root: &RootConfig) -> Result<Vec<FileRecord>> {
    let ignore = build_ignore(root)?;
    let mut records = Vec::new();
    let walker = WalkDir::new(&root.path)
        .follow_links(root.follow_symlinks)
        .sort_by_file_name();
    for entry in walker {
        let entry = entry?;
        if entry.path() == root.path {
            continue;
        }
        let rel = entry.path().strip_prefix(&root.path)?.to_path_buf();
        if !is_included(&root.include, &rel) {
            continue;
        }
        if is_ignored(&ignore, &rel, entry.file_type().is_dir()) {
            if entry.file_type().is_dir() {
                continue;
            }
            continue;
        }
        let rel_s = path_to_slash(&rel);
        if entry.file_type().is_dir() {
            let meta = fs::symlink_metadata(entry.path())?;
            records.push(FileRecord {
                root_id: root.id.clone(),
                path: rel_s,
                kind: "directory".into(),
                size: 0,
                mode: file_mode(&meta),
                modified: modified_secs(&meta),
                xattrs: read_xattrs(entry.path()),
                payload: Payload::Directory,
            });
            continue;
        }
        let link_meta = fs::symlink_metadata(entry.path())?;
        if link_meta.file_type().is_symlink() && !root.follow_symlinks {
            let target = fs::read_link(entry.path())?.to_string_lossy().to_string();
            records.push(FileRecord {
                root_id: root.id.clone(),
                path: rel_s,
                kind: "symlink".into(),
                size: 0,
                mode: file_mode(&link_meta),
                modified: modified_secs(&link_meta),
                xattrs: BTreeMap::new(),
                payload: Payload::Symlink { target },
            });
            continue;
        }
        if let Some(special_kind) = special_file_kind(&link_meta) {
            records.push(FileRecord {
                root_id: root.id.clone(),
                path: rel_s,
                kind: "special".into(),
                size: 0,
                mode: file_mode(&link_meta),
                modified: modified_secs(&link_meta),
                xattrs: read_xattrs(entry.path()),
                payload: Payload::Special { special_kind },
            });
            continue;
        }
        let meta = if link_meta.file_type().is_symlink() {
            fs::metadata(entry.path())?
        } else {
            link_meta
        };
        if !meta.is_file() {
            continue;
        }
        let large_config = effective_large_config(config, root);
        let binary = looks_binary(entry.path()).unwrap_or(false);
        let large = classify_large(&large_config, &rel, meta.len(), binary);
        let payload = if large {
            let (oid, manifest_key, chunk_count) =
                store_large_file(paths, entry.path(), &rel, &large_config, binary)?;
            Payload::LargeObject {
                oid,
                manifest_key,
                chunk_count,
                media_type: media_type_for_path(&rel),
                binary,
                chunking: large_config.default_chunking.clone(),
                compression: large_pointer_compression(&large_config),
                encryption: config.security.encryption.clone(),
                storage_tier_hint: "hot-manifest-cold-chunks".into(),
                hydrate_policy: "on-demand".into(),
            }
        } else {
            let bytes = stable_read(entry.path(), root.snapshot_mode.as_str())?;
            let oid = blake3_hex(&bytes);
            let object_key = store_bytes(paths, &paths.objects, &oid, &bytes)?;
            let conn = open_db(paths)?;
            conn.execute(
                "insert or ignore into blobs(oid, size, object_key) values (?1, ?2, ?3)",
                params![oid, bytes.len() as u64, object_key],
            )?;
            if bytes.len() as u64 <= majutsu_pack::SMALL_BLOB_MAX_SIZE {
                Payload::InlineSmall { oid, object_key }
            } else {
                Payload::NormalBlob { oid, object_key }
            }
        };
        records.push(FileRecord {
            root_id: root.id.clone(),
            path: rel_s,
            kind: "file".into(),
            size: meta.len(),
            mode: file_mode(&meta),
            modified: modified_secs(&meta),
            xattrs: read_xattrs(entry.path()),
            payload,
        });
    }
    Ok(records)
}

fn is_permission_denied_error(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
        {
            return true;
        }
        if cause
            .downcast_ref::<walkdir::Error>()
            .and_then(|walkdir| walkdir.io_error())
            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
        {
            return true;
        }
    }
    false
}

fn build_tree_manifest(root_id: &str, records: Vec<FileRecord>) -> Result<TreeManifest> {
    let mut entries = BTreeMap::new();
    for record in records {
        entries.insert(record.path.clone(), record);
    }
    let identity = serde_json::to_vec(&entries)?;
    Ok(TreeManifest {
        version: 1,
        tree_id: format!("tree-{}", blake3_hex(&identity)),
        root_id: root_id.to_string(),
        created_at: Utc::now(),
        entries,
    })
}

fn store_large_file(
    paths: &Paths,
    path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    if config.default_chunking == "fixed" {
        return store_large_file_fixed_streaming(paths, path, rel, config, binary);
    }
    store_large_file_buffered(paths, path, rel, config, binary)
}

fn store_large_file_buffered(
    paths: &Paths,
    path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    let bytes = stable_read(path, "strict")?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes);
    let mut chunks = Vec::new();
    let ranges =
        majutsu_large::chunk_ranges_for_bytes(&config.default_chunking, config.chunk_size, &bytes);
    for (index, (start, end)) in ranges.into_iter().enumerate() {
        let chunk = &bytes[start..end];
        let chunk_oid = blake3_hex(chunk);
        let stored = compress_large_chunk(config, rel, chunk)?;
        let object_key = store_bytes(
            paths,
            &large_chunk_base(paths, &config.default_chunking),
            &chunk_oid,
            &stored.bytes,
        )?;
        chunks.push(LargeChunk {
            index,
            offset: start as u64,
            len: chunk.len() as u64,
            stored_len: Some(stored.bytes.len() as u64),
            compression: stored.compression,
            oid: chunk_oid.clone(),
            object_key: object_key.clone(),
        });
        let conn = open_db(paths)?;
        conn.execute(
            "insert or ignore into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk_oid, chunk.len() as u64, object_key],
        )?;
    }
    let oid = hasher.finalize().to_hex().to_string();
    let manifest = LargeManifest {
        version: 1,
        oid: oid.clone(),
        size: bytes.len() as u64,
        media_type: media_type_for_path(rel),
        binary,
        chunking: config.default_chunking.clone(),
        chunk_size: config.chunk_size,
        chunks,
    };
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let manifest_oid = blake3_hex(&manifest_json);
    let manifest_key = store_bytes(paths, &paths.large_manifests, &manifest_oid, &manifest_json)?;
    let conn = open_db(paths)?;
    conn.execute(
        "insert or ignore into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
        params![oid, bytes.len() as u64, manifest.chunks.len(), manifest_key],
    )?;
    Ok((oid, manifest_key, manifest.chunks.len()))
}

fn store_large_file_fixed_streaming(
    paths: &Paths,
    path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    let attempts = 8;
    let mut last_error = None;
    for _ in 0..attempts {
        match store_large_file_fixed_streaming_once(paths, path, rel, config, binary) {
            Ok(result) => return Ok(result),
            Err(err) if is_file_changed_error(&err) => {
                last_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("file changed while reading: {}", path.display())))
}

fn store_large_file_fixed_streaming_once(
    paths: &Paths,
    path: &Path,
    rel: &Path,
    config: &LargeConfig,
    binary: bool,
) -> Result<(String, String, usize)> {
    let before = fs::metadata(path)?;
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut chunks = Vec::new();
    let mut buffer = vec![0u8; config.chunk_size.max(1)];
    let mut offset = 0u64;
    let mut index = 0usize;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        let chunk = &buffer[..n];
        hasher.update(chunk);
        let chunk_oid = blake3_hex(chunk);
        let stored = compress_large_chunk(config, rel, chunk)?;
        let object_key = store_bytes(
            paths,
            &large_chunk_base(paths, &config.default_chunking),
            &chunk_oid,
            &stored.bytes,
        )?;
        chunks.push(LargeChunk {
            index,
            offset,
            len: n as u64,
            stored_len: Some(stored.bytes.len() as u64),
            compression: stored.compression,
            oid: chunk_oid,
            object_key,
        });
        offset += n as u64;
        index += 1;
    }
    let after = fs::metadata(path)?;
    if !stable_metadata_matches(&before, &after) {
        bail!("file changed while reading: {}", path.display());
    }
    let oid = hasher.finalize().to_hex().to_string();
    let manifest = LargeManifest {
        version: 1,
        oid: oid.clone(),
        size: offset,
        media_type: media_type_for_path(rel),
        binary,
        chunking: config.default_chunking.clone(),
        chunk_size: config.chunk_size,
        chunks,
    };
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let manifest_oid = blake3_hex(&manifest_json);
    let manifest_key = store_bytes(paths, &paths.large_manifests, &manifest_oid, &manifest_json)?;
    let conn = open_db(paths)?;
    for chunk in &manifest.chunks {
        conn.execute(
            "insert or ignore into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk.oid, chunk.len, chunk.object_key],
        )?;
    }
    conn.execute(
        "insert or ignore into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
        params![oid, manifest.size, manifest.chunks.len(), manifest_key],
    )?;
    Ok((oid, manifest_key, manifest.chunks.len()))
}

fn compress_large_chunk(
    config: &LargeConfig,
    rel: &Path,
    bytes: &[u8],
) -> Result<majutsu_large::StoredLargeChunk> {
    let name = rel.file_name().and_then(OsStr::to_str).unwrap_or_default();
    Ok(majutsu_large::compress_chunk_if_useful(
        bytes,
        config.compression.enabled,
        &config.compression.algorithm,
        config.compression.level,
        config.compression.sample_bytes,
        config.compression.min_gain_ratio,
        &config.compression.skip_extensions,
        name,
    )?)
}

fn read_large_chunk(paths: &Paths, chunk: &LargeChunk) -> Result<Vec<u8>> {
    let bytes = read_object(paths, &chunk.object_key)?;
    match chunk.compression.as_str() {
        "none" => Ok(bytes),
        "zstd" => Ok(zstd::stream::decode_all(bytes.as_slice())?),
        other => bail!("unsupported large chunk compression: {other}"),
    }
}

fn create_layout(paths: &Paths) -> Result<()> {
    fs::create_dir_all(paths.db.parent().unwrap())?;
    fs::create_dir_all(&paths.objects)?;
    fs::create_dir_all(&paths.trees)?;
    fs::create_dir_all(&paths.large_chunks)?;
    fs::create_dir_all(paths.home.join("objects/large/chunks/fastcdc"))?;
    fs::create_dir_all(&paths.large_manifests)?;
    fs::create_dir_all(&paths.packs)?;
    fs::create_dir_all(paths.home.join("objects/packs/small"))?;
    fs::create_dir_all(&paths.pack_indexes)?;
    fs::create_dir_all(&paths.logs)?;
    for dir in [
        "ops",
        "queue/events",
        "queue/uploads",
        "queue/restores",
        "cache",
        "cache/blobs",
        "cache/large",
        "cache/packs",
        "cache/indexes",
        "keys",
        "locks",
        "runtime",
    ] {
        fs::create_dir_all(paths.home.join(dir))?;
    }
    let recipients = paths.home.join("keys/recipients.toml");
    if !recipients.exists() {
        fs::write(recipients, "recipients = []\n")?;
    }
    Ok(())
}

fn ensure_ready(paths: &Paths) -> Result<()> {
    if !paths.config.exists() || !paths.db.exists() {
        bail!("majutsu home is not initialized: run `mj init`");
    }
    Ok(())
}

fn open_db(paths: &Paths) -> Result<Connection> {
    if let Some(parent) = paths.db.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(&paths.db)?;
    migrate(&conn)?;
    if paths.config.exists() {
        let config = read_config(paths)?;
        sync_config_roots(paths, &conn, &config)?;
    }
    Ok(conn)
}

fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        create table if not exists roots(id text primary key, data_json text not null);
        create table if not exists snapshots(
          id text primary key,
          parent_id text,
          op_id text not null,
          created_at text not null,
          manifest_key text not null,
          manifest_json text not null
        );
        create table if not exists operations(
          id text primary key,
          parent_op text,
          kind text not null,
          actor text not null default 'local',
          status text not null default 'done',
          before_snapshot text,
          after_snapshot text,
          created_at text not null,
          message text
        );
        create table if not exists refs(name text primary key, value text not null);
        create table if not exists blobs(oid text primary key, size integer not null, object_key text not null);
        create table if not exists packs(pack_id text primary key, pack_key text not null, index_key text not null, object_count integer not null, size integer not null);
        create table if not exists large_objects(oid text primary key, size integer not null, chunk_count integer not null, manifest_key text not null);
        create table if not exists chunks(oid text primary key, size integer not null, object_key text not null);
        create table if not exists large_pins(oid text primary key, pinned_at text not null, reason text);
        ",
    )?;
    let _ = conn.execute("alter table blobs add column pack_id text", []);
    let _ = conn.execute("alter table blobs add column pack_offset integer", []);
    let _ = conn.execute("alter table blobs add column pack_len integer", []);
    let _ = conn.execute("alter table operations add column parent_op text", []);
    let _ = conn.execute(
        "alter table operations add column actor text not null default 'local'",
        [],
    );
    let _ = conn.execute(
        "alter table operations add column status text not null default 'done'",
        [],
    );
    Ok(())
}

fn export_metadata(conn: &Connection, config: &Config) -> Result<MetadataExport> {
    let roots = roots(conn)?;
    let config_roots = roots.iter().map(ConfigRoot::from).collect();
    let mut snapshots = Vec::new();
    let mut stmt = conn.prepare(
        "select id, parent_id, op_id, created_at, manifest_key, manifest_json from snapshots order by created_at",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(SnapshotExport {
            id: row.get(0)?,
            parent_id: row.get(1)?,
            op_id: row.get(2)?,
            created_at: row.get(3)?,
            manifest_key: row.get(4)?,
            manifest_json: row.get(5)?,
        })
    })?;
    for row in rows {
        snapshots.push(row?);
    }

    let operations = query_operations(conn)?;

    let mut refs = BTreeMap::new();
    let mut stmt = conn.prepare("select name, value from refs order by name")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (name, value) = row?;
        refs.insert(name, value);
    }

    Ok(MetadataExport {
        version: 1,
        exported_at: Utc::now(),
        config: Config {
            host: HostConfig {
                id: config.host.id.clone(),
                name: config.host.name.clone(),
            },
            remote: config.remote.clone(),
            roots: config_roots,
            large: LargeConfig {
                enabled: config.large.enabled,
                min_size: config.large.min_size,
                binary_min_size: config.large.binary_min_size,
                default_chunking: config.large.default_chunking.clone(),
                chunk_size: config.large.chunk_size,
                max_parallel_uploads: config.large.max_parallel_uploads,
                multipart: config.large.multipart,
                always: config.large.always.clone(),
                never: config.large.never.clone(),
                compression: config.large.compression.clone(),
            },
            pack: config.pack.clone(),
            watch: config.watch.clone(),
            security: config.security.clone(),
            tiering: config.tiering.clone(),
        },
        roots,
        snapshots,
        operations,
        refs,
        blobs: query_blobs(conn)?,
        large_objects: query_large_objects(conn)?,
        chunks: query_chunks(conn)?,
        packs: query_packs(conn)?,
        large_pins: query_large_pins(conn)?,
    })
}

fn import_metadata(conn: &Connection, export: &MetadataExport) -> Result<()> {
    for root in &export.roots {
        conn.execute(
            "insert or replace into roots(id, data_json) values (?1, ?2)",
            params![root.id, serde_json::to_string(root)?],
        )?;
    }
    for snapshot in &export.snapshots {
        conn.execute(
            "insert or replace into snapshots(id, parent_id, op_id, created_at, manifest_key, manifest_json)
             values (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                snapshot.id,
                snapshot.parent_id,
                snapshot.op_id,
                snapshot.created_at,
                snapshot.manifest_key,
                snapshot.manifest_json
            ],
        )?;
    }
    for op in &export.operations {
        conn.execute(
            "insert or replace into operations(id, parent_op, kind, actor, status, before_snapshot, after_snapshot, created_at, message)
             values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                op.id,
                op.parent_op,
                op.kind,
                op.actor,
                op.status,
                op.before_snapshot,
                op.after_snapshot,
                op.created_at,
                op.message
            ],
        )?;
    }
    for (name, value) in &export.refs {
        conn.execute(
            "insert or replace into refs(name, value) values (?1, ?2)",
            params![name, value],
        )?;
    }
    for blob in &export.blobs {
        conn.execute(
            "insert or replace into blobs(oid, size, object_key, pack_id, pack_offset, pack_len) values (?1, ?2, ?3, ?4, ?5, ?6)",
            params![blob.oid, blob.size, blob.object_key, blob.pack_id, blob.pack_offset, blob.pack_len],
        )?;
    }
    for pack in &export.packs {
        conn.execute(
            "insert or replace into packs(pack_id, pack_key, index_key, object_count, size) values (?1, ?2, ?3, ?4, ?5)",
            params![pack.pack_id, pack.pack_key, pack.index_key, pack.object_count, pack.size],
        )?;
    }
    for large in &export.large_objects {
        conn.execute(
            "insert or replace into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
            params![large.oid, large.size, large.chunk_count, large.manifest_key],
        )?;
    }
    for chunk in &export.chunks {
        conn.execute(
            "insert or replace into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk.oid, chunk.size, chunk.object_key],
        )?;
    }
    for pin in &export.large_pins {
        conn.execute(
            "insert or replace into large_pins(oid, pinned_at, reason) values (?1, ?2, ?3)",
            params![pin.oid, pin.pinned_at, pin.reason],
        )?;
    }
    Ok(())
}

fn query_blobs(conn: &Connection) -> Result<Vec<BlobExport>> {
    let mut stmt = conn.prepare(
        "select oid, size, object_key, pack_id, pack_offset, pack_len from blobs order by oid",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(BlobExport {
            oid: row.get(0)?,
            size: row.get(1)?,
            object_key: row.get(2)?,
            pack_id: row.get(3)?,
            pack_offset: row.get(4)?,
            pack_len: row.get(5)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_packs(conn: &Connection) -> Result<Vec<PackExport>> {
    let mut stmt = conn.prepare(
        "select pack_id, pack_key, index_key, object_count, size from packs order by pack_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(PackExport {
            pack_id: row.get(0)?,
            pack_key: row.get(1)?,
            index_key: row.get(2)?,
            object_count: row.get(3)?,
            size: row.get(4)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_large_objects(conn: &Connection) -> Result<Vec<LargeObjectExport>> {
    let mut stmt = conn
        .prepare("select oid, size, chunk_count, manifest_key from large_objects order by oid")?;
    let rows = stmt.query_map([], |row| {
        Ok(LargeObjectExport {
            oid: row.get(0)?,
            size: row.get(1)?,
            chunk_count: row.get(2)?,
            manifest_key: row.get(3)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_chunks(conn: &Connection) -> Result<Vec<ChunkExport>> {
    let mut stmt = conn.prepare("select oid, size, object_key from chunks order by oid")?;
    let rows = stmt.query_map([], |row| {
        Ok(ChunkExport {
            oid: row.get(0)?,
            size: row.get(1)?,
            object_key: row.get(2)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_large_pins(conn: &Connection) -> Result<Vec<LargePinExport>> {
    let mut stmt = conn.prepare("select oid, pinned_at, reason from large_pins order by oid")?;
    let rows = stmt.query_map([], |row| {
        Ok(LargePinExport {
            oid: row.get(0)?,
            pinned_at: row.get(1)?,
            reason: row.get(2)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn local_object_keys(export: &MetadataExport) -> Vec<String> {
    let mut keys = Vec::new();
    for snapshot in &export.snapshots {
        keys.push(snapshot.manifest_key.clone());
        if let Ok(manifest) = serde_json::from_str::<SnapshotManifest>(&snapshot.manifest_json) {
            for root_tree in manifest.root_trees.values() {
                keys.push(root_tree.tree_key.clone());
            }
        }
    }
    for blob in &export.blobs {
        if blob.pack_id.is_none() {
            keys.push(blob.object_key.clone());
        }
    }
    for pack in &export.packs {
        keys.push(pack.pack_key.clone());
        keys.push(pack.index_key.clone());
    }
    for large in &export.large_objects {
        keys.push(large.manifest_key.clone());
    }
    for chunk in &export.chunks {
        keys.push(chunk.object_key.clone());
    }
    keys.sort();
    keys.dedup();
    keys
}

fn canonical_remote_aliases(key: &str) -> Vec<String> {
    let Some(alias) = canonical_remote_alias(key) else {
        return Vec::new();
    };
    if alias == key {
        Vec::new()
    } else {
        vec![alias]
    }
}

fn large_chunk_base(paths: &Paths, chunking: &str) -> PathBuf {
    match chunking {
        "fastcdc" => paths.home.join("objects/large/chunks/fastcdc"),
        _ => paths.large_chunks.clone(),
    }
}

fn large_chunk_base_for_key(paths: &Paths, key: &str) -> PathBuf {
    if key.starts_with("objects/large/chunks/fastcdc/") {
        large_chunk_base(paths, "fastcdc")
    } else {
        large_chunk_base(paths, "fixed")
    }
}

fn canonical_remote_alias(key: &str) -> Option<String> {
    if let Some(rest) = key.strip_prefix("objects/trees/") {
        Some(format!("trees/{rest}.cbor.zst.enc"))
    } else if let Some(rest) = key.strip_prefix("objects/blobs/") {
        Some(format!("blobs/loose/{rest}.blob.enc"))
    } else if let Some(rest) = key.strip_prefix("objects/packs/small/") {
        Some(format!("packs/small/{rest}"))
    } else if let Some(rest) = key.strip_prefix("objects/packs/normal/") {
        Some(format!("packs/normal/{rest}"))
    } else if let Some(rest) = key.strip_prefix("objects/indexes/pack/") {
        let rest = rest.strip_suffix(".json").unwrap_or(rest);
        Some(format!("indexes/pack-index/{rest}.cbor.zst.enc"))
    } else if let Some(rest) = key.strip_prefix("objects/large/manifests/") {
        Some(format!("large/manifests/{rest}.cbor.zst.enc"))
    } else if let Some(rest) = key.strip_prefix("objects/large/chunks/fixed/") {
        Some(format!("large/chunks/fixed-8m/{rest}.chunk.enc"))
    } else {
        key.strip_prefix("objects/large/chunks/fastcdc/")
            .map(|rest| format!("large/chunks/fastcdc/{rest}.chunk.enc"))
    }
}

fn is_content_addressed_remote_key(key: &str) -> bool {
    key.starts_with("objects/")
        || key.starts_with("trees/")
        || key.starts_with("blobs/loose/")
        || key.starts_with("packs/")
        || key.starts_with("indexes/")
        || key.starts_with("large/")
}

fn all_local_object_keys(paths: &Paths) -> Result<Vec<String>> {
    let root = paths.home.join("objects");
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut keys = Vec::new();
    for entry in WalkDir::new(&root).sort_by_file_name() {
        let entry = entry?;
        if entry.file_type().is_file() {
            keys.push(path_to_slash(entry.path().strip_prefix(&paths.home)?));
        }
    }
    Ok(keys)
}

fn roots(conn: &Connection) -> Result<Vec<RootConfig>> {
    let mut stmt = conn.prepare("select data_json from roots order by id")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(serde_json::from_str(&row?)?);
    }
    Ok(out)
}

impl From<&RootConfig> for ConfigRoot {
    fn from(root: &RootConfig) -> Self {
        Self {
            id: root.id.clone(),
            name: Some(root.name.clone()),
            path: root.path.clone(),
            include: root.include.clone(),
            exclude: root.exclude.clone(),
            follow_symlinks: root.follow_symlinks,
            require_mount: root.require_mount,
            status: Some(root.status.clone()),
            snapshot_mode: root.snapshot_mode.clone(),
            pre_snapshot: root.pre_snapshot.clone(),
            post_snapshot: root.post_snapshot.clone(),
            snapshot_source: root.snapshot_source.clone(),
            application_plugin: root.application_plugin.clone(),
            large: root.large.clone(),
        }
    }
}

fn sync_config_roots(paths: &Paths, conn: &Connection, config: &Config) -> Result<()> {
    if config.roots.is_empty() {
        return Ok(());
    }
    for config_root in &config.roots {
        let existing = root_by_id_optional(conn, &config_root.id)?;
        let root = config_root.to_root_config(paths, existing.as_ref())?;
        conn.execute(
            "insert into roots(id, data_json) values (?1, ?2)
             on conflict(id) do update set data_json=excluded.data_json",
            params![root.id, serde_json::to_string(&root)?],
        )?;
    }
    Ok(())
}

fn sync_roots_to_config(paths: &Paths, conn: &Connection) -> Result<()> {
    let mut config = read_config(paths)?;
    config.roots = roots(conn)?.iter().map(ConfigRoot::from).collect();
    write_config(paths, &config)
}

impl ConfigRoot {
    fn to_root_config(&self, paths: &Paths, existing: Option<&RootConfig>) -> Result<RootConfig> {
        validate_snapshot_mode(&self.snapshot_mode)?;
        if let Some(large) = &self.large {
            if let Some(chunking) = &large.default_chunking {
                validate_large_chunking(chunking)?;
            }
        }
        let snapshot_source = self
            .snapshot_source
            .as_ref()
            .map(|path| config_relative_path(paths, path))
            .transpose()?;
        if snapshot_source.is_some() && self.snapshot_mode != "transactional" {
            bail!(
                "root {} snapshot_source requires snapshot_mode transactional",
                self.id
            );
        }
        Ok(RootConfig {
            id: self.id.clone(),
            name: self.name.clone().unwrap_or_else(|| self.id.clone()),
            path: config_relative_path(paths, &self.path)?,
            include: self.include.clone(),
            exclude: self.exclude.clone(),
            follow_symlinks: self.follow_symlinks,
            require_mount: self.require_mount,
            status: self
                .status
                .clone()
                .or_else(|| existing.map(|root| root.status.clone()))
                .unwrap_or_else(default_root_status),
            snapshot_mode: self.snapshot_mode.clone(),
            pre_snapshot: self.pre_snapshot.clone(),
            post_snapshot: self.post_snapshot.clone(),
            snapshot_source,
            application_plugin: self.application_plugin.clone(),
            large: self.large.clone(),
        })
    }
}

fn config_relative_path(paths: &Paths, path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let base = paths.config.parent().unwrap_or(&paths.home);
    Ok(base.join(path))
}

fn root_by_id_optional(conn: &Connection, id: &str) -> Result<Option<RootConfig>> {
    let data: Option<String> = conn
        .query_row(
            "select data_json from roots where id=?1",
            params![id],
            |row| row.get(0),
        )
        .optional()?;
    data.map(|data| serde_json::from_str(&data).map_err(Into::into))
        .transpose()
}

fn root_by_id(conn: &Connection, id: &str) -> Result<RootConfig> {
    root_by_id_optional(conn, id)?.ok_or_else(|| anyhow!("unknown root: {id}"))
}

fn save_root(conn: &Connection, root: &RootConfig) -> Result<()> {
    conn.execute(
        "update roots set data_json=?2 where id=?1",
        params![root.id, serde_json::to_string(root)?],
    )?;
    Ok(())
}

fn update_root_status(conn: &Connection, id: &str, status: &str) -> Result<()> {
    let mut root = root_by_id(conn, id)?;
    root.status = status.to_string();
    save_root(conn, &root)
}

fn current_snapshot(conn: &Connection) -> Result<Option<String>> {
    conn.query_row("select value from refs where name='current'", [], |row| {
        row.get(0)
    })
    .optional()
    .map_err(Into::into)
}

fn load_snapshot(conn: &Connection, args: &RestoreArgs) -> Result<SnapshotManifest> {
    let id = if let Some(id) = &args.snapshot {
        id.clone()
    } else if let Some(at) = &args.at {
        snapshot_id_at(conn, at)?
    } else {
        current_snapshot(conn)?.ok_or_else(|| anyhow!("no current snapshot"))?
    };
    let json: String = conn.query_row(
        "select manifest_json from snapshots where id=?1",
        params![id],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

fn snapshot_id_at(conn: &Connection, at: &str) -> Result<String> {
    conn.query_row(
        "select id from snapshots where created_at <= ?1 order by created_at desc limit 1",
        params![parse_time(at)?],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| anyhow!("no snapshot at or before {at}"))
}

fn load_snapshot_by_id(conn: &Connection, id: &str) -> Result<SnapshotManifest> {
    let json: String = conn.query_row(
        "select manifest_json from snapshots where id=?1",
        params![id],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

fn snapshot_contains_root(conn: &Connection, snapshot_id: &str, root: &str) -> Result<bool> {
    Ok(load_snapshot_by_id(conn, snapshot_id)?
        .roots
        .contains_key(root))
}

fn snapshot_file_map(snapshot: &SnapshotManifest) -> Result<BTreeMap<String, &FileRecord>> {
    let mut out = BTreeMap::new();
    for (root_id, records) in &snapshot.roots {
        for record in records {
            out.insert(format!("{}/{}", root_id, record.path), record);
        }
    }
    Ok(out)
}

fn carry_forward_root_snapshot(
    parent: Option<&SnapshotManifest>,
    root_id: &str,
    root_trees: &mut BTreeMap<String, RootSnapshot>,
    by_root: &mut BTreeMap<String, Vec<FileRecord>>,
) {
    let Some(parent) = parent else {
        return;
    };
    if let Some(root_tree) = parent.root_trees.get(root_id) {
        root_trees.insert(root_id.to_string(), root_tree.clone());
    }
    if let Some(records) = parent.roots.get(root_id) {
        by_root.insert(root_id.to_string(), records.clone());
    }
}

fn record_op(
    conn: &Connection,
    kind: &str,
    before: Option<&str>,
    after: Option<&str>,
    message: Option<&str>,
) -> Result<String> {
    let id = new_id("op");
    record_op_with_id(conn, &id, kind, before, after, message)?;
    Ok(id)
}

fn record_op_with_id(
    conn: &Connection,
    id: &str,
    kind: &str,
    before: Option<&str>,
    after: Option<&str>,
    message: Option<&str>,
) -> Result<()> {
    record_op_with_id_and_status(conn, id, kind, before, after, "done", message)
}

fn record_op_with_id_and_status(
    conn: &Connection,
    id: &str,
    kind: &str,
    before: Option<&str>,
    after: Option<&str>,
    status: &str,
    message: Option<&str>,
) -> Result<()> {
    let created_at = Utc::now().to_rfc3339();
    let parent_op = current_operation(conn)?;
    let actor = operation_actor();
    conn.execute(
        "insert into operations(id, parent_op, kind, actor, status, before_snapshot, after_snapshot, created_at, message)
         values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            id, parent_op, kind, actor, status, before, after, created_at, message
        ],
    )?;
    append_local_oplog(
        conn,
        &OperationExport {
            id: id.to_string(),
            parent_op,
            kind: kind.to_string(),
            actor,
            status: status.to_string(),
            before_snapshot: before.map(str::to_string),
            after_snapshot: after.map(str::to_string),
            created_at,
            message: message.map(str::to_string),
        },
    )?;
    Ok(())
}

fn append_local_oplog(conn: &Connection, op: &OperationExport) -> Result<()> {
    let Some(path) = local_oplog_path(conn)? else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    let bytes = serde_cbor::to_vec(op)?;
    file.write_all(&bytes)?;
    Ok(())
}

fn rewrite_local_oplog(conn: &Connection) -> Result<()> {
    let Some(path) = local_oplog_path(conn)? else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let operations = query_operations(conn)?;
    let tmp = path.with_extension("cborl.tmp");
    let result = (|| -> Result<()> {
        let mut file = File::create(&tmp)?;
        for op in &operations {
            file.write_all(&serde_cbor::to_vec(op)?)?;
        }
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, &path)?;
        fsync_parent_dir(&path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn local_oplog_path(conn: &Connection) -> Result<Option<PathBuf>> {
    let db_path = conn
        .query_row(
            "select file from pragma_database_list where name='main'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(db_path) = db_path.filter(|path| !path.is_empty()) else {
        return Ok(None);
    };
    let db_path = PathBuf::from(db_path);
    let Some(home) = db_path
        .parent()
        .and_then(|parent| parent.parent())
        .map(Path::to_path_buf)
    else {
        return Ok(None);
    };
    Ok(Some(home.join("ops/local-oplog.cborl")))
}

fn current_operation(conn: &Connection) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "select id from operations order by rowid desc limit 1",
            [],
            |row| row.get(0),
        )
        .optional()?)
}

fn operation_actor() -> String {
    let user = env::var("USER")
        .or_else(|_| env::var("LOGNAME"))
        .unwrap_or_else(|_| "local".into());
    let host = env::var("HOSTNAME").unwrap_or_else(|_| "host".into());
    format!("{user}@{host}")
}

fn query_operation(conn: &Connection, op_id: &str) -> Result<OperationExport> {
    conn.query_row(
        "select id, parent_op, kind, actor, status, before_snapshot, after_snapshot, created_at, message from operations where id=?1",
        params![op_id],
        |row| {
            Ok(OperationExport {
                id: row.get(0)?,
                parent_op: row.get(1)?,
                kind: row.get(2)?,
                actor: row.get(3)?,
                status: row.get(4)?,
                before_snapshot: row.get(5)?,
                after_snapshot: row.get(6)?,
                created_at: row.get(7)?,
                message: row.get(8)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| anyhow!("unknown operation: {op_id}"))
}

fn query_operations(conn: &Connection) -> Result<Vec<OperationExport>> {
    let mut stmt = conn.prepare(
        "select id, parent_op, kind, actor, status, before_snapshot, after_snapshot, created_at, message from operations order by created_at",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(OperationExport {
            id: row.get(0)?,
            parent_op: row.get(1)?,
            kind: row.get(2)?,
            actor: row.get(3)?,
            status: row.get(4)?,
            before_snapshot: row.get(5)?,
            after_snapshot: row.get(6)?,
            created_at: row.get(7)?,
            message: row.get(8)?,
        })
    })?;
    let mut operations = Vec::new();
    for row in rows {
        operations.push(row?);
    }
    Ok(operations)
}

fn read_config(paths: &Paths) -> Result<Config> {
    let config: Config = toml::from_str(&fs::read_to_string(&paths.config)?)?;
    normalize_watch_backend(&config.watch.backend)?;
    validate_watch_mode(&config.watch.mode)?;
    validate_security_config(&config.security)?;
    Ok(config)
}

fn write_config(paths: &Paths, config: &Config) -> Result<()> {
    fs::write(&paths.config, toml::to_string_pretty(config)?)?;
    Ok(())
}

fn build_ignore(root: &RootConfig) -> Result<Gitignore> {
    let mut builder = GitignoreBuilder::new(&root.path);
    for pattern in &root.exclude {
        builder.add_line(None, pattern)?;
    }
    Ok(builder.build()?)
}

fn validate_snapshot_mode(mode: &str) -> Result<()> {
    match mode {
        "default" | "strict" | "transactional" => Ok(()),
        _ => bail!("snapshot mode must be default, strict, or transactional"),
    }
}

fn validate_watch_mode(mode: &str) -> Result<()> {
    match mode {
        "default" | "strict" | "transactional" => Ok(()),
        _ => bail!("watch mode must be default, strict, or transactional"),
    }
}

fn validate_security_config(security: &SecurityConfig) -> Result<()> {
    encryption_enabled(security)?;
    match security.hash.as_str() {
        "blake3-keyed" | "blake3" | "sha256" => Ok(()),
        _ => bail!("security hash must be blake3-keyed, blake3, or sha256"),
    }
}

fn encryption_enabled(security: &SecurityConfig) -> Result<bool> {
    match security.encryption.as_str() {
        "" | "none" => Ok(false),
        "age" | "chacha20poly1305" => Ok(true),
        _ => bail!("security encryption must be none, age, or chacha20poly1305"),
    }
}

fn encryption_mode(security: &SecurityConfig) -> Result<EncryptionMode> {
    match security.encryption.as_str() {
        "" | "none" => Ok(EncryptionMode::None),
        "age" => Ok(EncryptionMode::Age),
        "chacha20poly1305" => Ok(EncryptionMode::ChaCha20Poly1305),
        _ => bail!("security encryption must be none, age, or chacha20poly1305"),
    }
}

fn validate_large_chunking(chunking: &str) -> Result<()> {
    match chunking {
        "fixed" | "fastcdc" => Ok(()),
        _ => bail!("large chunking must be fixed or fastcdc"),
    }
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

fn parse_byte_size(input: &str) -> Result<u64> {
    let normalized = input.trim().replace('_', "");
    if normalized.is_empty() {
        bail!("size must not be empty");
    }
    if let Ok(value) = normalized.parse::<u64>() {
        return Ok(value);
    }
    let split_at = normalized
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .ok_or_else(|| anyhow!("size is missing a unit: {input}"))?;
    let number = normalized[..split_at].trim();
    let unit = normalized[split_at..]
        .trim()
        .replace(' ', "")
        .to_ascii_lowercase();
    let value: f64 = number
        .parse()
        .with_context(|| format!("invalid size number: {input}"))?;
    if !value.is_finite() || value < 0.0 {
        bail!("invalid size number: {input}");
    }
    let multiplier = match unit.as_str() {
        "b" | "byte" | "bytes" => 1.0,
        "k" | "kb" => 1_000.0,
        "m" | "mb" => 1_000_000.0,
        "g" | "gb" => 1_000_000_000.0,
        "t" | "tb" => 1_000_000_000_000.0,
        "kib" => 1024.0,
        "mib" => 1024.0 * 1024.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => bail!("unsupported size unit in {input}"),
    };
    let bytes = value * multiplier;
    if bytes > u64::MAX as f64 {
        bail!("size is too large: {input}");
    }
    Ok(bytes.round() as u64)
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
        DurationValue::String(value) => Ok(parse_duration_millis(&value)? / 1000),
    }
}

fn parse_duration_millis(input: &str) -> Result<u64> {
    let normalized = input.trim().replace('_', "");
    if normalized.is_empty() {
        bail!("duration must not be empty");
    }
    if let Ok(value) = normalized.parse::<u64>() {
        return Ok(value);
    }
    let split_at = normalized
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .ok_or_else(|| anyhow!("duration is missing a unit: {input}"))?;
    let number = normalized[..split_at].trim();
    let unit = normalized[split_at..]
        .trim()
        .replace(' ', "")
        .to_ascii_lowercase();
    let value: f64 = number
        .parse()
        .with_context(|| format!("invalid duration number: {input}"))?;
    if !value.is_finite() || value < 0.0 {
        bail!("invalid duration number: {input}");
    }
    let multiplier = match unit.as_str() {
        "ms" | "millisecond" | "milliseconds" => 1.0,
        "s" | "sec" | "secs" | "second" | "seconds" => 1000.0,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000.0,
        "d" | "day" | "days" => 86_400_000.0,
        _ => bail!("unsupported duration unit in {input}"),
    };
    let millis = value * multiplier;
    if millis > u64::MAX as f64 {
        bail!("duration is too large: {input}");
    }
    Ok(millis.round() as u64)
}

fn run_pre_snapshot_hook(paths: &Paths, root: &RootConfig) -> Result<()> {
    if root.snapshot_mode == "transactional" {
        run_application_plugin(paths, root, "pre")?;
        if let Some(command) = &root.pre_snapshot {
            record_event(paths, "pre-snapshot", &format!("{} {}", root.id, command))?;
            run_hook(command, &root.path)?;
        }
    }
    Ok(())
}

fn run_post_snapshot_hook(paths: &Paths, root: &RootConfig) -> Result<()> {
    if root.snapshot_mode == "transactional" {
        if let Some(command) = &root.post_snapshot {
            record_event(paths, "post-snapshot", &format!("{} {}", root.id, command))?;
            run_hook(command, &root.path)?;
        }
        run_application_plugin(paths, root, "post")?;
    }
    Ok(())
}

fn run_application_plugin(paths: &Paths, root: &RootConfig, phase: &str) -> Result<()> {
    let Some(command) = &root.application_plugin else {
        return Ok(());
    };
    record_event(
        paths,
        &format!("application-plugin-{phase}"),
        &format!("{} {}", root.id, command),
    )?;
    let mut process = ProcessCommand::new("sh");
    process
        .arg("-c")
        .arg(command)
        .current_dir(&root.path)
        .env("MAJUTSU_HOME", &paths.home)
        .env("MAJUTSU_PLUGIN_PHASE", phase)
        .env("MAJUTSU_ROOT_ID", &root.id)
        .env("MAJUTSU_ROOT_NAME", &root.name)
        .env("MAJUTSU_ROOT_PATH", &root.path);
    if let Some(source) = &root.snapshot_source {
        process.env("MAJUTSU_SNAPSHOT_SOURCE", source);
    }
    let status = process.status()?;
    if !status.success() {
        bail!("application plugin failed during {phase}: {command}");
    }
    Ok(())
}

fn snapshot_scan_root(paths: &Paths, root: &RootConfig) -> Result<RootConfig> {
    let Some(source) = &root.snapshot_source else {
        return Ok(root.clone());
    };
    if root.snapshot_mode != "transactional" {
        bail!(
            "snapshot source requires transactional snapshot mode for root {}",
            root.id
        );
    }
    if !source.exists() {
        bail!(
            "snapshot source does not exist for root {}: {}",
            root.id,
            source.display()
        );
    }
    if !source.is_dir() {
        bail!(
            "snapshot source is not a directory for root {}: {}",
            root.id,
            source.display()
        );
    }
    record_event(
        paths,
        "snapshot-source",
        &format!("{} {}", root.id, source.display()),
    )?;
    let mut scan_root = root.clone();
    scan_root.path = source.clone();
    Ok(scan_root)
}

fn run_hook(command: &str, cwd: &Path) -> Result<()> {
    let status = ProcessCommand::new("sh")
        .arg("-c")
        .arg(command)
        .current_dir(cwd)
        .status()?;
    if !status.success() {
        bail!("snapshot hook failed: {command}");
    }
    Ok(())
}

fn is_included(patterns: &[String], rel: &Path) -> bool {
    if patterns.is_empty() {
        return true;
    }
    let rel = path_to_slash(rel);
    patterns
        .iter()
        .any(|pattern| path_pattern_match(pattern, &rel))
}

fn is_ignored(ignore: &Gitignore, rel: &Path, is_dir: bool) -> bool {
    ignore.matched_path_or_any_parents(rel, is_dir).is_ignore()
}

fn effective_large_config(config: &Config, root: &RootConfig) -> LargeConfig {
    let mut large = LargeConfig {
        enabled: config.large.enabled,
        min_size: config.large.min_size,
        binary_min_size: config.large.binary_min_size,
        default_chunking: config.large.default_chunking.clone(),
        chunk_size: config.large.chunk_size,
        max_parallel_uploads: config.large.max_parallel_uploads,
        multipart: config.large.multipart,
        always: config.large.always.clone(),
        never: config.large.never.clone(),
        compression: config.large.compression.clone(),
    };
    if let Some(root_large) = &root.large {
        if let Some(min_size) = root_large.min_size {
            large.min_size = min_size;
        }
        if let Some(binary_min_size) = root_large.binary_min_size {
            large.binary_min_size = binary_min_size;
        }
        if let Some(default_chunking) = &root_large.default_chunking {
            large.default_chunking = default_chunking.clone();
        }
        if let Some(chunk_size) = root_large.chunk_size {
            large.chunk_size = chunk_size;
        }
        if !root_large.always.is_empty() {
            large.always = root_large.always.clone();
        }
        if !root_large.never.is_empty() {
            large.never = root_large.never.clone();
        }
    }
    large
}

fn classify_large(config: &LargeConfig, rel: &Path, size: u64, binary: bool) -> bool {
    if !config.enabled {
        return false;
    }
    let name = rel.file_name().and_then(OsStr::to_str).unwrap_or_default();
    if config.never.iter().any(|p| glob_match(p, name)) {
        return false;
    }
    if config.always.iter().any(|p| glob_match(p, name)) {
        return true;
    }
    size >= config.min_size || (binary && size >= config.binary_min_size)
}

fn glob_match(pattern: &str, name: &str) -> bool {
    if let Some(ext) = pattern.strip_prefix("*.") {
        return name
            .rsplit_once('.')
            .map(|(_, e)| e.eq_ignore_ascii_case(ext))
            .unwrap_or(false);
    }
    pattern == name
}

fn path_pattern_match(pattern: &str, rel: &str) -> bool {
    if pattern == "**" || pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix("/**") {
        return rel == prefix || rel.starts_with(&format!("{prefix}/"));
    }
    if let Some(ext) = pattern.strip_prefix("*.") {
        return rel
            .rsplit_once('.')
            .map(|(_, e)| e.eq_ignore_ascii_case(ext))
            .unwrap_or(false);
    }
    if let Some(suffix) = pattern.strip_prefix("**/") {
        if let Some(middle) = suffix.strip_suffix("/**") {
            return rel == middle
                || rel.starts_with(&format!("{middle}/"))
                || rel.contains(&format!("/{middle}/"));
        }
        return rel == suffix || rel.ends_with(&format!("/{suffix}"));
    }
    rel == pattern || rel.starts_with(&format!("{pattern}/"))
}

fn default_include() -> Vec<String> {
    vec!["**".into()]
}

fn default_root_status() -> String {
    "active".into()
}

fn default_snapshot_mode() -> String {
    "default".into()
}

fn default_large_chunking() -> String {
    "fixed".into()
}

fn default_true() -> bool {
    true
}

fn default_large_min_size() -> u64 {
    DEFAULT_LARGE_MIN_SIZE
}

fn default_large_max_parallel_uploads() -> usize {
    8
}

fn default_large_binary_min_size() -> u64 {
    DEFAULT_LARGE_BINARY_MIN_SIZE
}

fn default_chunk_size() -> usize {
    DEFAULT_CHUNK_SIZE
}

fn default_large_compression_algorithm() -> String {
    "zstd".into()
}

fn default_large_compression_level() -> i32 {
    3
}

fn default_large_compression_sample_bytes() -> usize {
    1024 * 1024
}

fn default_large_compression_min_gain_ratio() -> f64 {
    0.05
}

fn default_large_compression_skip_extensions() -> Vec<String> {
    vec![
        "*.jpg".into(),
        "*.jpeg".into(),
        "*.png".into(),
        "*.heic".into(),
        "*.mp4".into(),
        "*.mov".into(),
        "*.zip".into(),
        "*.gz".into(),
        "*.zst".into(),
        "*.xz".into(),
        "*.parquet".into(),
    ]
}

fn default_small_pack_target() -> u64 {
    64 * 1024 * 1024
}

fn default_normal_pack_target() -> u64 {
    256 * 1024 * 1024
}

impl Default for PackConfig {
    fn default() -> Self {
        Self {
            small_pack_target: default_small_pack_target(),
            normal_pack_target: default_normal_pack_target(),
        }
    }
}

fn default_watch_mode() -> String {
    "default".into()
}

fn default_watch_debounce_ms() -> u64 {
    1500
}

fn default_watch_settle_ms() -> u64 {
    500
}

fn default_watch_periodic_rescan_secs() -> u64 {
    3600
}

fn default_watch_interval_secs() -> u64 {
    60
}

fn default_security_key_id() -> String {
    "default".into()
}

fn default_security_hash() -> String {
    "blake3-keyed".into()
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            backend: default_watch_backend(),
            mode: default_watch_mode(),
            debounce: default_watch_debounce_ms(),
            settle: default_watch_settle_ms(),
            periodic_rescan: default_watch_periodic_rescan_secs(),
            interval: default_watch_interval_secs(),
        }
    }
}

fn default_tiering_rules() -> Vec<TieringRule> {
    vec![
        TieringRule {
            name: "keep-host-metadata-hot".into(),
            prefix: "hosts/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        TieringRule {
            name: "keep-bootstrap-metadata-hot".into(),
            prefix: "metadata/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        TieringRule {
            name: "keep-trees-hot".into(),
            prefix: "trees/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        TieringRule {
            name: "keep-large-manifests-hot".into(),
            prefix: "large/manifests/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        TieringRule {
            name: "keep-indexes-hot".into(),
            prefix: "indexes/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        TieringRule {
            name: "packs-to-ia".into(),
            prefix: "packs/normal/".into(),
            after: Some("30d".into()),
            storage: Some("infrequent".into()),
        },
        TieringRule {
            name: "fixed-large-chunks-to-archive".into(),
            prefix: "large/chunks/fixed-8m/".into(),
            after: Some("180d".into()),
            storage: Some("archive".into()),
        },
        TieringRule {
            name: "fastcdc-large-chunks-to-archive".into(),
            prefix: "large/chunks/fastcdc/".into(),
            after: Some("180d".into()),
            storage: Some("archive".into()),
        },
    ]
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rules: default_tiering_rules(),
        }
    }
}

fn root_large_override(args: &RootAddArgs) -> Option<RootLargeConfig> {
    if args.large_min_size.is_none()
        && args.large_binary_min_size.is_none()
        && args.large_chunk_size.is_none()
        && args.large_chunking.is_none()
        && args.large_always.is_empty()
        && args.large_never.is_empty()
    {
        return None;
    }
    Some(RootLargeConfig {
        min_size: args.large_min_size,
        binary_min_size: args.large_binary_min_size,
        default_chunking: args.large_chunking.clone(),
        chunk_size: args.large_chunk_size,
        always: args.large_always.clone(),
        never: args.large_never.clone(),
    })
}

fn apply_root_large_set(root: &mut RootConfig, args: &RootSetArgs) -> Result<()> {
    if let Some(chunking) = &args.large_chunking {
        validate_large_chunking(chunking)?;
    }
    if args.clear_large_policy {
        root.large = None;
    }
    let wants_large = args.large_min_size.is_some()
        || args.large_binary_min_size.is_some()
        || args.large_chunk_size.is_some()
        || args.large_chunking.is_some()
        || !args.large_always.is_empty()
        || !args.large_never.is_empty()
        || args.clear_large_always
        || args.clear_large_never;
    if !wants_large {
        return Ok(());
    }
    let large = root.large.get_or_insert_with(|| RootLargeConfig {
        min_size: None,
        binary_min_size: None,
        default_chunking: None,
        chunk_size: None,
        always: Vec::new(),
        never: Vec::new(),
    });
    if let Some(min_size) = args.large_min_size {
        large.min_size = Some(min_size);
    }
    if let Some(binary_min_size) = args.large_binary_min_size {
        large.binary_min_size = Some(binary_min_size);
    }
    if let Some(chunk_size) = args.large_chunk_size {
        large.chunk_size = Some(chunk_size);
    }
    if let Some(chunking) = &args.large_chunking {
        large.default_chunking = Some(chunking.clone());
    }
    if args.clear_large_always {
        large.always.clear();
    }
    large.always.extend(args.large_always.clone());
    if args.clear_large_never {
        large.never.clear();
    }
    large.never.extend(args.large_never.clone());
    Ok(())
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

fn read_pid(path: &Path) -> Result<Option<u32>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    Ok(Some(text.trim().parse()?))
}

struct ProcessLock {
    path: PathBuf,
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_process_lock(path: &Path, name: &str) -> Result<ProcessLock> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let pid = std::process::id();
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => {
            writeln!(file, "{pid}")?;
            Ok(ProcessLock {
                path: path.to_path_buf(),
            })
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = fs::read_to_string(path).unwrap_or_default();
            if let Ok(existing_pid) = existing.trim().parse::<u32>() {
                if pid_alive(existing_pid) {
                    bail!("{name} already running with pid {existing_pid}");
                }
            }
            fs::remove_file(path)?;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            writeln!(file, "{pid}")?;
            Ok(ProcessLock {
                path: path.to_path_buf(),
            })
        }
        Err(err) => Err(err).with_context(|| format!("acquire {name} lock")),
    }
}

fn pid_alive(pid: u32) -> bool {
    ProcessCommand::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn format_notify_event(event: &notify::Event) -> String {
    let kind = match &event.kind {
        EventKind::Create(_) => "create",
        EventKind::Modify(_) => "modify",
        EventKind::Remove(_) => "remove",
        EventKind::Access(_) => "access",
        EventKind::Other => "other",
        _ => "unknown",
    };
    let paths = event
        .paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("{kind} {paths}")
}

fn looks_binary(path: &Path) -> Result<bool> {
    let mut f = File::open(path)?;
    let mut buf = [0u8; 8192];
    let n = f.read(&mut buf)?;
    Ok(buf[..n].contains(&0))
}

fn media_type_for_path(path: &Path) -> Option<String> {
    let name = path
        .file_name()
        .and_then(OsStr::to_str)?
        .to_ascii_lowercase();
    let media_type = if name.ends_with(".tar.zst") {
        "application/zstd"
    } else {
        match path
            .extension()
            .and_then(OsStr::to_str)
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref()
        {
            Some("blend") => "application/x-blender",
            Some("db") | Some("sqlite") => "application/vnd.sqlite3",
            Some("gz") => "application/gzip",
            Some("heic") => "image/heic",
            Some("iso") => "application/x-iso9660-image",
            Some("jpeg") | Some("jpg") => "image/jpeg",
            Some("json") => "application/json",
            Some("log") | Some("txt") => "text/plain",
            Some("md") => "text/markdown",
            Some("mkv") => "video/x-matroska",
            Some("mov") => "video/quicktime",
            Some("mp4") => "video/mp4",
            Some("parquet") => "application/vnd.apache.parquet",
            Some("png") => "image/png",
            Some("psd") => "image/vnd.adobe.photoshop",
            Some("qcow2") => "application/x-qcow2",
            Some("tar") => "application/x-tar",
            Some("toml") => "application/toml",
            Some("vmdk") => "application/x-vmdk",
            Some("yaml") | Some("yml") => "application/yaml",
            Some("zip") => "application/zip",
            Some("zst") => "application/zstd",
            _ => return None,
        }
    };
    Some(media_type.to_string())
}

fn large_pointer_compression(config: &LargeConfig) -> String {
    if config.compression.enabled {
        format!("per-chunk:{}", config.compression.algorithm)
    } else {
        "none".into()
    }
}

fn stable_read(path: &Path, mode: &str) -> Result<Vec<u8>> {
    let attempts = if mode == "strict" { 8 } else { 3 };
    let mut last_error = None;
    for attempt in 0..attempts {
        let before = fs::metadata(path)?;
        let bytes = fs::read(path)?;
        let after = fs::metadata(path)?;
        if stable_metadata_matches(&before, &after) {
            return Ok(bytes);
        }
        last_error = Some(anyhow!("file changed while reading: {}", path.display()));
        std::thread::sleep(std::time::Duration::from_millis(25 * (attempt + 1) as u64));
    }
    Err(last_error.unwrap_or_else(|| anyhow!("file did not become stable: {}", path.display())))
}

fn is_file_changed_error(err: &anyhow::Error) -> bool {
    err.to_string().starts_with("file changed while reading:")
}

fn stable_metadata_matches(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return false;
    }
    stable_file_id(before) == stable_file_id(after)
}

#[cfg(unix)]
fn stable_file_id(meta: &fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(meta.ino())
}

#[cfg(not(unix))]
fn stable_file_id(_: &fs::Metadata) -> Option<u64> {
    None
}

fn store_bytes(paths: &Paths, base: &Path, oid: &str, bytes: &[u8]) -> Result<String> {
    let storage_id = object_storage_id(paths, oid)?;
    let (a, b) = storage_id.split_at(2);
    let dir = base.join(a);
    fs::create_dir_all(&dir)?;
    let path = dir.join(b);
    if !path.exists() {
        let tmp = path.with_extension("tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(&encode_object(paths, bytes)?)?;
        f.sync_all()?;
        fs::rename(tmp, &path)?;
    }
    let rel = path.strip_prefix(&paths.home).unwrap_or(&path);
    Ok(path_to_slash(rel))
}

fn object_storage_id(paths: &Paths, oid: &str) -> Result<String> {
    if !object_keys_are_hmac(paths)? {
        return Ok(oid.to_string());
    }
    let key_hex = read_master_key(paths)?;
    let key_bytes = hex::decode(key_hex.trim())?;
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&key_bytes)?;
    mac.update(b"majutsu-object-key-v1\0");
    mac.update(oid.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn object_keys_are_hmac(paths: &Paths) -> Result<bool> {
    if !paths.config.exists() {
        return Ok(false);
    }
    encryption_enabled(&read_config(paths)?.security)
}

fn write_atomic(dest: &Path, bytes: &[u8]) -> Result<()> {
    write_atomic_with(dest, |file| {
        file.write_all(bytes)?;
        Ok(())
    })
}

fn write_large_chunks_atomic(paths: &Paths, dest: &Path, manifest: &LargeManifest) -> Result<()> {
    write_atomic_with(dest, |file| {
        for chunk in &manifest.chunks {
            file.write_all(&read_large_chunk(paths, chunk)?)?;
        }
        Ok(())
    })
}

fn write_atomic_with<F>(dest: &Path, write_contents: F) -> Result<()>
where
    F: FnOnce(&mut File) -> Result<()>,
{
    if fs::symlink_metadata(dest)
        .map(|meta| meta.file_type().is_dir())
        .unwrap_or(false)
    {
        bail!("restore target is a directory: {}", dest.display());
    }
    let (tmp, mut file) = create_atomic_temp(dest)?;
    let result = (|| -> Result<()> {
        write_contents(&mut file)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, dest)?;
        fsync_parent_dir(dest)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn create_atomic_temp(dest: &Path) -> Result<(PathBuf, File)> {
    let parent = dest.parent().unwrap_or_else(|| Path::new("."));
    for _ in 0..16 {
        let tmp = atomic_temp_path(dest);
        let file = match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err).with_context(|| format!("create {}", tmp.display())),
        };
        return Ok((tmp, file));
    }
    bail!(
        "failed to allocate temporary restore file in {}",
        parent.display()
    )
}

fn atomic_temp_path(dest: &Path) -> PathBuf {
    let parent = dest.parent().unwrap_or_else(|| Path::new("."));
    let file_name = dest
        .file_name()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("restore"));
    let mut tmp_name = OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(".mjtmp-");
    tmp_name.push(Uuid::new_v4().to_string());
    parent.join(tmp_name)
}

fn fsync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    let dir = File::open(parent)?;
    dir.sync_all()?;
    Ok(())
}

fn encode_object(paths: &Paths, bytes: &[u8]) -> Result<Vec<u8>> {
    let config = if paths.config.exists() {
        Some(read_config(paths)?)
    } else {
        None
    };
    if config
        .as_ref()
        .map(|config| encryption_enabled(&config.security))
        .transpose()?
        .unwrap_or(false)
    {
        let mode = config
            .as_ref()
            .map(|config| encryption_mode(&config.security))
            .transpose()?
            .unwrap_or(EncryptionMode::None);
        majutsu_crypto::encode_object(bytes, mode, &paths.master_key, &recipients_path(paths))
    } else {
        Ok(bytes.to_vec())
    }
}

fn read_object(paths: &Paths, key: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(paths.home.join(key))?;
    decode_object(paths, &bytes)
}

fn read_blob_payload(
    paths: &Paths,
    conn: &Connection,
    oid: &str,
    fallback_key: &str,
) -> Result<Vec<u8>> {
    let blob = query_blobs(conn)?
        .into_iter()
        .find(|blob| blob.oid == oid)
        .ok_or_else(|| anyhow!("missing blob metadata for {oid}"))?;
    if let Some(pack_id) = blob.pack_id {
        let pack: PackExport = conn.query_row(
            "select pack_id, pack_key, index_key, object_count, size from packs where pack_id=?1",
            params![pack_id],
            |row| {
                Ok(PackExport {
                    pack_id: row.get(0)?,
                    pack_key: row.get(1)?,
                    index_key: row.get(2)?,
                    object_count: row.get(3)?,
                    size: row.get(4)?,
                })
            },
        )?;
        let offset = blob
            .pack_offset
            .ok_or_else(|| anyhow!("missing pack offset for {oid}"))? as usize;
        let len = blob
            .pack_len
            .ok_or_else(|| anyhow!("missing pack len for {oid}"))? as usize;
        let bytes = fs::read(paths.home.join(pack.pack_key))?;
        let slice = bytes
            .get(offset..offset + len)
            .ok_or_else(|| anyhow!("pack entry out of range for {oid}"))?;
        if slice.len() < 8 {
            bail!("pack entry too short for {oid}");
        }
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&slice[..8]);
        let stored_len = u64::from_le_bytes(len_bytes) as usize;
        if stored_len != slice.len() - 8 {
            bail!("pack entry length mismatch for {oid}");
        }
        decode_object(paths, &slice[8..])
    } else {
        read_object(paths, fallback_key)
    }
}

fn decode_object(paths: &Paths, bytes: &[u8]) -> Result<Vec<u8>> {
    majutsu_crypto::decode_object(bytes, &paths.master_key, &recipients_path(paths))
}

fn recipients_path(paths: &Paths) -> PathBuf {
    paths.home.join("keys/recipients.toml")
}

fn random_key_hex() -> Result<String> {
    majutsu_crypto::random_key_hex()
}

fn validate_key_hex(hex_key: &str) -> Result<()> {
    majutsu_crypto::validate_key_hex(hex_key)
}

fn read_master_key(paths: &Paths) -> Result<String> {
    majutsu_crypto::read_master_key(&paths.master_key)
}

fn write_master_key(paths: &Paths, hex_key: &str) -> Result<()> {
    majutsu_crypto::write_master_key(&paths.master_key, hex_key)
}

fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4())
}

fn hostname_from_env() -> Result<String> {
    if let Ok(hostname) = env::var("HOSTNAME") {
        if !hostname.is_empty() {
            return Ok(hostname);
        }
    }
    Ok(fs::read_to_string("/etc/hostname")?.trim().to_string())
}

fn absolutize(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn is_mount_point(path: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        let Ok(target) = fs::canonicalize(path) else {
            return false;
        };
        let Ok(mountinfo) = fs::read_to_string("/proc/self/mountinfo") else {
            return false;
        };
        for line in mountinfo.lines() {
            let Some(before_sep) = line.split(" - ").next() else {
                continue;
            };
            let mut fields = before_sep.split_whitespace();
            let mount_point = fields.nth(4);
            if let Some(mount_point) = mount_point {
                if PathBuf::from(unescape_mountinfo_path(mount_point)) == target {
                    return true;
                }
            }
        }
        false
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        false
    }
}

fn unescape_mountinfo_path(input: &str) -> String {
    let mut out = String::new();
    let bytes = input.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\\'
            && i + 3 < bytes.len()
            && bytes[i + 1].is_ascii_digit()
            && bytes[i + 2].is_ascii_digit()
            && bytes[i + 3].is_ascii_digit()
        {
            if let Ok(value) = u8::from_str_radix(&input[i + 1..i + 4], 8) {
                out.push(value as char);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn path_to_slash(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn modified_secs(meta: &fs::Metadata) -> Option<i64> {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
}

fn parse_time(input: &str) -> Result<String> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Ok(dt.with_timezone(&Utc).to_rfc3339());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.and_utc().to_rfc3339());
    }
    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        return Ok(date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow!("invalid date: {input}"))?
            .and_utc()
            .to_rfc3339());
    }
    if input == "now" {
        return Ok(Utc::now().to_rfc3339());
    }
    if let Some(dt) = parse_relative_ago(input)? {
        return Ok(dt.to_rfc3339());
    }
    bail!(
        "time must be RFC3339, YYYY-MM-DD HH:MM:SS, YYYY-MM-DD, relative ago, or now, got: {input}"
    );
}

fn parse_relative_ago(input: &str) -> Result<Option<DateTime<Utc>>> {
    let normalized = input.trim().to_ascii_lowercase();
    let Some(value) = normalized.strip_suffix(" ago") else {
        return Ok(None);
    };
    let compact = value.trim();
    if let Ok(dt) = parse_duration_ago(compact) {
        return Ok(Some(dt));
    }
    let parts = compact.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 {
        return Ok(None);
    }
    let number: i64 = parts[0].parse()?;
    let seconds = match parts[1] {
        "second" | "seconds" | "sec" | "secs" => number,
        "minute" | "minutes" | "min" | "mins" => number * 60,
        "hour" | "hours" => number * 60 * 60,
        "day" | "days" => number * 24 * 60 * 60,
        _ => return Ok(None),
    };
    Ok(Some(Utc::now() - chrono::Duration::seconds(seconds)))
}

fn parse_db_time(input: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(input)?.with_timezone(&Utc))
}

fn parse_duration_ago(input: &str) -> Result<DateTime<Utc>> {
    let (number, unit) = input.split_at(input.len().saturating_sub(1));
    let value: i64 = number.parse()?;
    let seconds = match unit {
        "d" => value * 24 * 60 * 60,
        "h" => value * 60 * 60,
        "m" => value * 60,
        "s" => value,
        _ => bail!("duration must use s, m, h, or d suffix: {input}"),
    };
    Ok(Utc::now() - chrono::Duration::seconds(seconds))
}

enum RemoteStore {
    File(FileRemote),
    S3(S3Remote),
}

struct FileRemote {
    root: PathBuf,
}

struct S3Remote {
    bucket: String,
    prefix: String,
    endpoint: String,
    region: String,
    signature_version: String,
    access_key: String,
    secret_key: String,
    storage_class: Option<String>,
    object_tags: Vec<(String, String)>,
    multipart_enabled: bool,
    max_parallel_uploads: usize,
    client: Client,
}

fn open_remote(config: &RemoteConfig) -> Result<RemoteStore> {
    open_remote_with_upload_policy(config, true, default_large_max_parallel_uploads())
}

fn open_remote_with_upload_policy(
    config: &RemoteConfig,
    multipart_enabled: bool,
    max_parallel_uploads: usize,
) -> Result<RemoteStore> {
    let remote_url = config.url()?;
    if let Some(path) = remote_url.strip_prefix("file://") {
        return Ok(RemoteStore::File(FileRemote {
            root: PathBuf::from(path),
        }));
    }
    if remote_url.starts_with("s3://") {
        let url = Url::parse(&remote_url)?;
        let bucket = url
            .host_str()
            .ok_or_else(|| anyhow!("s3 remote is missing bucket: {remote_url}"))?
            .to_string();
        let prefix = url.path().trim_matches('/').to_string();
        return Ok(RemoteStore::S3(S3Remote {
            bucket,
            prefix,
            endpoint: config
                .endpoint
                .clone()
                .or_else(|| env::var("AWS_ENDPOINT_URL").ok())
                .unwrap_or_else(|| "https://storage.googleapis.com".into()),
            region: config
                .region
                .clone()
                .or_else(|| env::var("AWS_DEFAULT_REGION").ok())
                .unwrap_or_else(|| "us-east-1".into()),
            signature_version: config
                .signature_version
                .clone()
                .or_else(|| env::var("AWS_SIGNATURE_VERSION").ok())
                .unwrap_or_else(|| "s3v4".into()),
            access_key: env::var("AWS_ACCESS_KEY_ID")
                .context("AWS_ACCESS_KEY_ID is required for s3 remote")?,
            secret_key: env::var("AWS_SECRET_ACCESS_KEY")
                .context("AWS_SECRET_ACCESS_KEY is required for s3 remote")?,
            storage_class: optional_env("MAJUTSU_S3_STORAGE_CLASS")?,
            object_tags: parse_s3_object_tags_env()?,
            multipart_enabled,
            max_parallel_uploads: max_parallel_uploads.max(1),
            client: Client::new(),
        }));
    }
    bail!("unsupported remote URL: {remote_url}");
}

impl RemoteStore {
    fn describe(&self) -> String {
        match self {
            RemoteStore::File(remote) => format!("file://{}", remote.root.display()),
            RemoteStore::S3(remote) => {
                let prefix = if remote.prefix.is_empty() {
                    String::new()
                } else {
                    format!("/{}", remote.prefix)
                };
                format!("s3://{}{}", remote.bucket, prefix)
            }
        }
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        match self {
            RemoteStore::File(remote) => {
                let path = remote.root.join(key);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, bytes)?;
                Ok(())
            }
            RemoteStore::S3(remote) => remote.put(key, bytes),
        }
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        match self {
            RemoteStore::File(remote) => {
                let path = remote.root.join(key);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                match fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                {
                    Ok(mut file) => {
                        file.write_all(bytes)?;
                        file.sync_all()?;
                        Ok(true)
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
                    Err(err) => Err(err.into()),
                }
            }
            RemoteStore::S3(remote) => remote.put_if_absent(key, bytes),
        }
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        match self {
            RemoteStore::File(remote) => Ok(fs::read(remote.root.join(key))?),
            RemoteStore::S3(remote) => remote.get(key),
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        match self {
            RemoteStore::File(remote) => {
                let path = remote.root.join(key);
                if path.exists() {
                    fs::remove_file(path)?;
                }
                Ok(())
            }
            RemoteStore::S3(remote) => remote.delete(key),
        }
    }

    fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        match self {
            RemoteStore::File(remote) => {
                let mut file = File::open(remote.root.join(key))?;
                file.seek(SeekFrom::Start(start))?;
                let mut limited = Vec::with_capacity(len as usize);
                let mut take = file.take(len);
                take.read_to_end(&mut limited)?;
                Ok(limited)
            }
            RemoteStore::S3(remote) => remote.get_range(key, start, len),
        }
    }

    fn exists(&self, key: &str) -> Result<bool> {
        match self {
            RemoteStore::File(remote) => Ok(remote.root.join(key).exists()),
            RemoteStore::S3(remote) => remote.exists(key),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        match self {
            RemoteStore::File(remote) => list_file_remote(&remote.root, prefix),
            RemoteStore::S3(remote) => remote.list(prefix),
        }
    }

    fn restore_archive(&self, key: &str, days: u32, tier: &str) -> Result<bool> {
        match self {
            RemoteStore::File(_) => Ok(true),
            RemoteStore::S3(remote) => remote.restore_archive(key, days, tier),
        }
    }

    fn capabilities(&self) -> RemoteCapabilities {
        match self {
            RemoteStore::File(_) => RemoteCapabilities {
                lifecycle_rules: false,
                object_tags: false,
                storage_class_on_put: false,
                restore_archived_object: true,
                multipart_upload: false,
                range_get: true,
                conditional_put: true,
            },
            RemoteStore::S3(remote) => RemoteCapabilities {
                lifecycle_rules: true,
                object_tags: !remote.uses_sigv2(),
                storage_class_on_put: !remote.uses_sigv2(),
                restore_archived_object: true,
                multipart_upload: remote.multipart_enabled && !remote.uses_sigv2(),
                range_get: true,
                conditional_put: !remote.uses_sigv2(),
            },
        }
    }
}

impl S3Remote {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.put_object(key, bytes, false).map(|_| ())
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        if self.uses_sigv2() {
            bail!("conditional put requires S3 Signature V4");
        }
        self.put_object(key, bytes, true)
    }

    fn put_object(&self, key: &str, bytes: &[u8], if_absent: bool) -> Result<bool> {
        if self.should_use_multipart(bytes.len()) {
            if if_absent && self.exists(key)? {
                return Ok(false);
            }
            self.put_multipart(key, bytes)?;
            return Ok(true);
        }
        let remote_key = self.remote_key(key);
        let url = self.object_url(&remote_key);
        let response = if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}", self.bucket, remote_key);
            let auth = self.auth_v2("PUT", "", "application/octet-stream", &date, &path)?;
            self.client
                .put(url)
                .header(DATE, date)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(AUTHORIZATION, auth)
                .body(bytes.to_vec())
                .send()?
        } else {
            let payload_hash = sha256_hex(bytes);
            let mut extra_headers = self.put_object_headers(key)?;
            if if_absent {
                extra_headers.push(("if-none-match".to_string(), "*".to_string()));
            }
            let auth = self.auth_v4("PUT", &remote_key, "", &payload_hash, &extra_headers)?;
            let mut request = self
                .client
                .put(url)
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .header(CONTENT_TYPE, "application/octet-stream");
            for (name, value) in extra_headers {
                request = request.header(name.as_str(), value.as_str());
            }
            request.body(bytes.to_vec()).send()?
        };
        if if_absent && matches!(response.status().as_u16(), 409 | 412) {
            return Ok(false);
        }
        if !response.status().is_success() {
            bail!("s3 put failed for {key}: HTTP {}", response.status());
        }
        Ok(true)
    }

    fn put_multipart(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let remote_key = self.remote_key(key);
        let upload_id = self.initiate_multipart(&remote_key)?;
        let result = (|| {
            let mut parts = self.upload_multipart_parts(&remote_key, &upload_id, bytes)?;
            parts.sort_by_key(|part| part.part_number);
            self.complete_multipart(&remote_key, &upload_id, &parts)
        })();
        if result.is_err() {
            let _ = self.abort_multipart(&remote_key, &upload_id);
        }
        result.with_context(|| format!("multipart upload failed for {key}"))
    }

    fn upload_multipart_parts(
        &self,
        remote_key: &str,
        upload_id: &str,
        bytes: &[u8],
    ) -> Result<Vec<CompletedPart>> {
        let chunks = bytes
            .chunks(MIN_MULTIPART_PART_SIZE)
            .enumerate()
            .map(|(idx, chunk)| (idx + 1, chunk))
            .collect::<Vec<_>>();
        let mut parts = Vec::with_capacity(chunks.len());
        let parallelism = self.max_parallel_uploads.max(1);
        for batch in chunks.chunks(parallelism) {
            let batch_parts = std::thread::scope(|scope| {
                let handles = batch
                    .iter()
                    .map(|(part_number, chunk)| {
                        scope.spawn(move || {
                            let etag =
                                self.upload_part(remote_key, upload_id, *part_number, chunk)?;
                            Ok(CompletedPart {
                                part_number: *part_number,
                                etag,
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .map_err(|_| anyhow!("multipart upload worker panicked"))?
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            parts.extend(batch_parts);
        }
        Ok(parts)
    }

    fn initiate_multipart(&self, remote_key: &str) -> Result<String> {
        let query = "uploads=".to_string();
        let payload_hash = sha256_hex(b"");
        let extra_headers = self.put_object_headers(remote_key)?;
        let auth = self.auth_v4("POST", remote_key, &query, &payload_hash, &extra_headers)?;
        let mut request = self
            .client
            .post(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization);
        for (name, value) in extra_headers {
            request = request.header(name.as_str(), value.as_str());
        }
        let response = request.body(Vec::new()).send()?;
        if !response.status().is_success() {
            bail!(
                "s3 initiate multipart failed: HTTP {} {}",
                response.status(),
                response.text().unwrap_or_default()
            );
        }
        parse_xml_text(&response.text()?, "UploadId")?
            .ok_or_else(|| anyhow!("missing multipart UploadId"))
    }

    fn upload_part(
        &self,
        remote_key: &str,
        upload_id: &str,
        part_number: usize,
        bytes: &[u8],
    ) -> Result<String> {
        let query = canonical_query(&[
            ("partNumber", part_number.to_string()),
            ("uploadId", upload_id.to_string()),
        ]);
        let payload_hash = sha256_hex(bytes);
        let auth = self.auth_v4("PUT", remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .put(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .body(bytes.to_vec())
            .send()?;
        if !response.status().is_success() {
            bail!(
                "s3 upload part {part_number} failed: HTTP {} {}",
                response.status(),
                response.text().unwrap_or_default()
            );
        }
        response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string())
            .ok_or_else(|| anyhow!("s3 upload part {part_number} response had no ETag"))
    }

    fn complete_multipart(
        &self,
        remote_key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<()> {
        let query = canonical_query(&[("uploadId", upload_id.to_string())]);
        let mut body = String::from("<CompleteMultipartUpload>");
        for part in parts {
            body.push_str("<Part>");
            body.push_str(&format!("<PartNumber>{}</PartNumber>", part.part_number));
            body.push_str("<ETag>");
            body.push_str(&xml_escape(&part.etag));
            body.push_str("</ETag>");
            body.push_str("</Part>");
        }
        body.push_str("</CompleteMultipartUpload>");
        let payload_hash = sha256_hex(body.as_bytes());
        let auth = self.auth_v4("POST", remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .post(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .send()?;
        if !response.status().is_success() {
            bail!(
                "s3 complete multipart failed: HTTP {} {}",
                response.status(),
                response.text().unwrap_or_default()
            );
        }
        Ok(())
    }

    fn abort_multipart(&self, remote_key: &str, upload_id: &str) -> Result<()> {
        let query = canonical_query(&[("uploadId", upload_id.to_string())]);
        let payload_hash = sha256_hex(b"");
        let auth = self.auth_v4("DELETE", remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .delete(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .body(Vec::new())
            .send()?;
        if response.status().is_success() {
            Ok(())
        } else {
            bail!("s3 abort multipart failed: HTTP {}", response.status())
        }
    }

    fn restore_archive(&self, key: &str, days: u32, tier: &str) -> Result<bool> {
        let remote_key = self.remote_key(key);
        let query = "restore=".to_string();
        let body = format!(
            "<RestoreRequest><Days>{days}</Days><GlacierJobParameters><Tier>{}</Tier></GlacierJobParameters></RestoreRequest>",
            xml_escape(tier)
        );
        if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}?restore", self.bucket, remote_key);
            let auth = self.auth_v2("POST", "", "application/xml", &date, &path)?;
            let response = self
                .client
                .post(self.object_url_query(&remote_key, &query))
                .header(DATE, date)
                .header(CONTENT_TYPE, "application/xml")
                .header(AUTHORIZATION, auth)
                .body(body)
                .send()?;
            return archive_restore_status(key, response.status().as_u16());
        }
        let payload_hash = sha256_hex(body.as_bytes());
        let auth = self.auth_v4("POST", &remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .post(self.object_url_query(&remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .send()?;
        archive_restore_status(key, response.status().as_u16())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.get_with_range(key, None)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let remote_key = self.remote_key(key);
        let response = if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}", self.bucket, remote_key);
            let auth = self.auth_v2("DELETE", "", "", &date, &path)?;
            self.client
                .delete(self.object_url(&remote_key))
                .header(DATE, date)
                .header(AUTHORIZATION, auth)
                .send()?
        } else {
            let payload_hash = sha256_hex(b"");
            let auth = self.auth_v4("DELETE", &remote_key, "", &payload_hash, &[])?;
            self.client
                .delete(self.object_url(&remote_key))
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .send()?
        };
        if response.status().is_success() || response.status().as_u16() == 404 {
            Ok(())
        } else {
            bail!("s3 delete failed for {key}: HTTP {}", response.status())
        }
    }

    fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        let end = start
            .checked_add(len)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| anyhow!("invalid range {start}+{len}"))?;
        self.get_with_range(key, Some(format!("bytes={start}-{end}")))
    }

    fn get_with_range(&self, key: &str, range: Option<String>) -> Result<Vec<u8>> {
        let remote_key = self.remote_key(key);
        let response = if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}", self.bucket, remote_key);
            let auth = self.auth_v2("GET", "", "", &date, &path)?;
            let mut request = self
                .client
                .get(self.object_url(&remote_key))
                .header(DATE, date)
                .header(AUTHORIZATION, auth);
            if let Some(range) = &range {
                request = request.header(RANGE, range);
            }
            request.send()?
        } else {
            let payload_hash = sha256_hex(b"");
            let mut extra = Vec::new();
            if let Some(range) = &range {
                extra.push(("range".to_string(), range.clone()));
            }
            let auth = self.auth_v4("GET", &remote_key, "", &payload_hash, &extra)?;
            let mut request = self
                .client
                .get(self.object_url(&remote_key))
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization);
            if let Some(range) = &range {
                request = request.header(RANGE, range);
            }
            request.send()?
        };
        if !response.status().is_success() {
            bail!("s3 get failed for {key}: HTTP {}", response.status());
        }
        Ok(response.bytes()?.to_vec())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let remote_key = self.remote_key(key);
        let response = if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}", self.bucket, remote_key);
            let auth = self.auth_v2("HEAD", "", "", &date, &path)?;
            self.client
                .head(self.object_url(&remote_key))
                .header(DATE, date)
                .header(AUTHORIZATION, auth)
                .send()?
        } else {
            let payload_hash = sha256_hex(b"");
            let auth = self.auth_v4("HEAD", &remote_key, "", &payload_hash, &[])?;
            self.client
                .head(self.object_url(&remote_key))
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .send()?
        };
        if response.status().is_success() {
            Ok(true)
        } else if response.status().as_u16() == 404 {
            Ok(false)
        } else {
            bail!("s3 head failed for {key}: HTTP {}", response.status());
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let remote_prefix = self.remote_key(prefix);
        let query = format!("prefix={}", uri_encode(&remote_prefix, true));
        let url = format!(
            "{}/{}/?{}",
            self.endpoint.trim_end_matches('/'),
            self.bucket,
            query
        );
        let response = if self.uses_sigv2() {
            let date = http_date();
            let resource = format!("/{}/", self.bucket);
            let auth = self.auth_v2("GET", "", "", &date, &resource)?;
            self.client
                .get(url)
                .header(DATE, date)
                .header(AUTHORIZATION, auth)
                .send()?
        } else {
            let payload_hash = sha256_hex(b"");
            let auth = self.auth_v4("GET", "", &query, &payload_hash, &[])?;
            self.client
                .get(url)
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .send()?
        };
        if !response.status().is_success() {
            bail!("s3 list failed: HTTP {}", response.status());
        }
        let xml = response.text()?;
        let mut reader = Reader::from_str(&xml);
        reader.config_mut().trim_text(true);
        let mut in_key = false;
        let mut keys = Vec::new();
        loop {
            match reader.read_event() {
                Ok(Event::Start(e)) if e.name().as_ref() == b"Key" => in_key = true,
                Ok(Event::End(e)) if e.name().as_ref() == b"Key" => in_key = false,
                Ok(Event::Text(e)) if in_key => {
                    let key = e.unescape()?.into_owned();
                    if let Some(local) = self.local_key(&key) {
                        keys.push(local);
                    }
                }
                Ok(Event::Eof) => break,
                Err(err) => return Err(err.into()),
                _ => {}
            }
        }
        Ok(keys)
    }

    fn auth_v2(
        &self,
        method: &str,
        md5: &str,
        content_type: &str,
        date: &str,
        resource: &str,
    ) -> Result<String> {
        let canonical = format!("{method}\n{md5}\n{content_type}\n{date}\n{resource}");
        let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(self.secret_key.as_bytes())?;
        mac.update(canonical.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        Ok(format!("AWS {}:{}", self.access_key, signature))
    }

    fn auth_v4(
        &self,
        method: &str,
        remote_key: &str,
        canonical_query: &str,
        payload_hash: &str,
        extra_headers: &[(String, String)],
    ) -> Result<SigV4Auth> {
        let now = Utc::now();
        let datestamp = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let canonical_uri = if remote_key.is_empty() {
            format!("/{}/", self.bucket)
        } else {
            format!("/{}/{}", self.bucket, uri_encode(remote_key, false))
        };
        let mut headers = vec![
            ("host".to_string(), self.host_header()?),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        headers.extend(extra_headers.iter().cloned());
        headers.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_headers = headers
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let signed_headers = headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{}/{}/s3/aws4_request", datestamp, self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let signing_key = self.sigv4_signing_key(&datestamp)?;
        let signature = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes())?;
        Ok(SigV4Auth {
            amz_date,
            authorization: format!(
                "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
                self.access_key, scope, signed_headers, signature
            ),
        })
    }

    fn sigv4_signing_key(&self, datestamp: &str) -> Result<Vec<u8>> {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.secret_key).as_bytes(),
            datestamp.as_bytes(),
        )?;
        let k_region = hmac_sha256(&k_date, self.region.as_bytes())?;
        let k_service = hmac_sha256(&k_region, b"s3")?;
        hmac_sha256(&k_service, b"aws4_request")
    }

    fn put_object_headers(&self, key: &str) -> Result<Vec<(String, String)>> {
        let mut headers = Vec::new();
        if let Some(storage_class) = &self.storage_class {
            headers.push(("x-amz-storage-class".to_string(), storage_class.clone()));
        }
        if !self.object_tags.is_empty() {
            let mut tags = vec![(
                "majutsu-class".to_string(),
                s3_object_class(key).to_string(),
            )];
            tags.extend(self.object_tags.iter().cloned());
            headers.push(("x-amz-tagging".to_string(), encode_s3_object_tags(&tags)?));
        }
        Ok(headers)
    }

    fn uses_sigv2(&self) -> bool {
        self.signature_version.contains('2')
    }

    fn host_header(&self) -> Result<String> {
        let url = Url::parse(&self.endpoint)?;
        Ok(url
            .host_str()
            .ok_or_else(|| anyhow!("endpoint has no host: {}", self.endpoint))?
            .to_string())
    }

    fn object_url(&self, remote_key: &str) -> String {
        format!(
            "{}/{}/{}",
            self.endpoint.trim_end_matches('/'),
            self.bucket,
            remote_key
        )
    }

    fn object_url_query(&self, remote_key: &str, query: &str) -> String {
        format!("{}?{}", self.object_url(remote_key), query)
    }

    fn multipart_threshold(&self) -> usize {
        env::var("MAJUTSU_S3_MULTIPART_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MULTIPART_THRESHOLD)
            .max(MIN_MULTIPART_PART_SIZE)
    }

    fn should_use_multipart(&self, len: usize) -> bool {
        self.multipart_enabled && !self.uses_sigv2() && len >= self.multipart_threshold()
    }

    fn remote_key(&self, key: &str) -> String {
        let clean = key.trim_start_matches('/');
        if self.prefix.is_empty() {
            clean.to_string()
        } else if clean.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix.trim_matches('/'), clean)
        }
    }

    fn local_key(&self, remote_key: &str) -> Option<String> {
        if self.prefix.is_empty() {
            Some(remote_key.to_string())
        } else {
            remote_key
                .strip_prefix(&format!("{}/", self.prefix.trim_matches('/')))
                .map(|s| s.to_string())
        }
    }
}

struct SigV4Auth {
    amz_date: String,
    authorization: String,
}

struct CompletedPart {
    part_number: usize,
    etag: String,
}

fn list_file_remote(root: &Path, prefix: &str) -> Result<Vec<String>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut keys = Vec::new();
    for entry in WalkDir::new(root).sort_by_file_name() {
        let entry = entry?;
        if entry.file_type().is_file() {
            let rel = path_to_slash(entry.path().strip_prefix(root)?);
            if rel.starts_with(prefix) {
                keys.push(rel);
            }
        }
    }
    Ok(keys)
}

fn http_date() -> String {
    Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn hmac_sha256(key: &[u8], bytes: &[u8]) -> Result<Vec<u8>> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)?;
    mac.update(bytes);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_sha256_hex(key: &[u8], bytes: &[u8]) -> Result<String> {
    Ok(hex::encode(hmac_sha256(key, bytes)?))
}

fn canonical_query(params: &[(&str, String)]) -> String {
    let mut pairs = params
        .iter()
        .map(|(key, value)| (uri_encode(key, true), uri_encode(value, true)))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => {
            let value = value.trim().to_string();
            if value.is_empty() {
                Ok(None)
            } else if value.contains('\n') || value.contains('\r') {
                bail!("{name} must not contain newlines")
            } else {
                Ok(Some(value))
            }
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn parse_s3_object_tags_env() -> Result<Vec<(String, String)>> {
    let Some(value) = optional_env("MAJUTSU_S3_OBJECT_TAGS")? else {
        return Ok(Vec::new());
    };
    parse_s3_object_tags(&value)
}

fn parse_s3_object_tags(input: &str) -> Result<Vec<(String, String)>> {
    input
        .split('&')
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| anyhow!("S3 object tag must be key=value: {part}"))?;
            let key = key.trim();
            let value = value.trim();
            validate_s3_tag_part("S3 object tag key", key)?;
            validate_s3_tag_part("S3 object tag value", value)?;
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

fn validate_s3_tag_part(label: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{label} must not be empty");
    }
    if value.contains('\n') || value.contains('\r') {
        bail!("{label} must not contain newlines");
    }
    Ok(())
}

fn encode_s3_object_tags(tags: &[(String, String)]) -> Result<String> {
    tags.iter()
        .map(|(key, value)| {
            validate_s3_tag_part("S3 object tag key", key)?;
            validate_s3_tag_part("S3 object tag value", value)?;
            Ok(format!(
                "{}={}",
                uri_encode(key, true),
                uri_encode(value, true)
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("&"))
}

fn s3_object_class(key: &str) -> &'static str {
    let key = key.trim_start_matches('/');
    if key.starts_with("hosts/")
        || key.starts_with("metadata/")
        || key.ends_with("/metadata/export.json")
    {
        "metadata"
    } else if key.starts_with("refs/") || key.contains("/refs/") || key.ends_with("current") {
        "ref"
    } else if key.starts_with("objects/trees/") || key.starts_with("trees/") {
        "tree"
    } else if key.starts_with("objects/packs/") || key.starts_with("packs/") {
        "pack"
    } else if key.starts_with("objects/large/") || key.starts_with("large/") {
        "large"
    } else if key.starts_with("objects/indexes/") || key.starts_with("indexes/") {
        "index"
    } else if key.starts_with("objects/blobs/") || key.starts_with("blobs/") {
        "blob"
    } else {
        "object"
    }
}

fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::new();
    for byte in input.as_bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (*byte == b'/' && !encode_slash);
        if keep {
            out.push(*byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn parse_xml_text(xml: &str, tag: &str) -> Result<Option<String>> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut in_tag = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) if e.name().as_ref() == tag.as_bytes() => in_tag = true,
            Ok(Event::End(e)) if e.name().as_ref() == tag.as_bytes() => in_tag = false,
            Ok(Event::Text(e)) if in_tag => return Ok(Some(e.unescape()?.into_owned())),
            Ok(Event::Eof) => return Ok(None),
            Err(err) => return Err(err.into()),
            _ => {}
        }
    }
}

fn xml_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn archive_restore_status(key: &str, status: u16) -> Result<bool> {
    match status {
        200 | 202 | 204 | 409 => Ok(true),
        404 => Ok(false),
        _ => bail!("archive restore request failed for {key}: HTTP {status}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_s3_remote() -> S3Remote {
        S3Remote {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            endpoint: "https://storage.googleapis.com".to_string(),
            region: "auto".to_string(),
            signature_version: "s3v4".to_string(),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            storage_class: Some("STANDARD_IA".to_string()),
            object_tags: vec![("purpose".to_string(), "backup data".to_string())],
            multipart_enabled: true,
            max_parallel_uploads: 8,
            client: Client::new(),
        }
    }

    #[test]
    fn s3_put_headers_include_storage_class_and_encoded_tags() {
        let remote = test_s3_remote();
        let headers = remote
            .put_object_headers("objects/large/chunks/fixed/chunk-1")
            .unwrap();
        assert!(headers.contains(&("x-amz-storage-class".to_string(), "STANDARD_IA".to_string())));
        assert!(headers.contains(&(
            "x-amz-tagging".to_string(),
            "majutsu-class=large&purpose=backup%20data".to_string()
        )));
    }

    #[test]
    fn s3_capabilities_honor_multipart_policy() {
        let mut remote = test_s3_remote();
        remote.multipart_enabled = false;
        let store = RemoteStore::S3(remote);
        assert!(!store.capabilities().multipart_upload);
    }

    #[test]
    fn s3_multipart_threshold_requires_enabled_policy() {
        let mut remote = test_s3_remote();
        remote.multipart_enabled = false;
        assert!(!remote.should_use_multipart(DEFAULT_MULTIPART_THRESHOLD));
        remote.multipart_enabled = true;
        assert!(remote.should_use_multipart(DEFAULT_MULTIPART_THRESHOLD));
    }

    #[test]
    fn s3_sigv4_signs_put_attribute_headers() {
        let remote = test_s3_remote();
        let headers = remote
            .put_object_headers("objects/packs/normal/pack-1")
            .unwrap();
        let auth = remote
            .auth_v4(
                "PUT",
                "prefix/objects/packs/normal/pack-1",
                "",
                "hash",
                &headers,
            )
            .unwrap();
        assert!(auth.authorization.contains(
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-storage-class;x-amz-tagging"
        ));
    }

    #[test]
    fn s3_sigv4_signs_conditional_put_header() {
        let remote = test_s3_remote();
        let mut headers = remote
            .put_object_headers("objects/blobs/loose/blob-1")
            .unwrap();
        headers.push(("if-none-match".to_string(), "*".to_string()));
        let auth = remote
            .auth_v4(
                "PUT",
                "prefix/objects/blobs/loose/blob-1",
                "",
                "hash",
                &headers,
            )
            .unwrap();
        assert!(auth.authorization.contains("SignedHeaders=host;if-none-match;x-amz-content-sha256;x-amz-date;x-amz-storage-class;x-amz-tagging"));
    }

    #[test]
    fn file_remote_put_if_absent_does_not_overwrite_existing_object() {
        let tmp = tempfile::tempdir().unwrap();
        let remote = RemoteStore::File(FileRemote {
            root: tmp.path().to_path_buf(),
        });

        assert!(remote.put_if_absent("objects/test", b"first").unwrap());
        assert!(!remote.put_if_absent("objects/test", b"second").unwrap());
        assert_eq!(remote.get("objects/test").unwrap(), b"first");
    }

    #[cfg(unix)]
    #[test]
    fn stable_metadata_detects_same_size_file_replacement() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("data.txt");
        let replacement = tmp.path().join("replacement.txt");
        fs::write(&path, b"same-size-a").unwrap();
        fs::write(&replacement, b"same-size-b").unwrap();
        let before = fs::metadata(&path).unwrap();
        fs::rename(&replacement, &path).unwrap();
        filetime::set_file_mtime(
            &path,
            filetime::FileTime::from_system_time(before.modified().unwrap()),
        )
        .unwrap();
        let after = fs::metadata(&path).unwrap();

        assert_eq!(before.len(), after.len());
        assert_eq!(before.modified().unwrap(), after.modified().unwrap());
        assert!(!stable_metadata_matches(&before, &after));
    }
}
