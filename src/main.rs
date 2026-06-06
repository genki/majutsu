use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use hmac::{Hmac, Mac};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use notify::{Config as NotifyConfig, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use quick_xml::Reader;
use quick_xml::events::Event;
use rand::RngCore;
use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, DATE, ETAG, HOST, RANGE};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::mpsc;
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
    Restore {
        #[command(subcommand)]
        command: RestoreCommand,
    },
    Mount(MountArgs),
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
    #[arg(long, default_value = "default")]
    snapshot_mode: String,
    #[arg(long)]
    pre_snapshot: Option<String>,
    #[arg(long)]
    post_snapshot: Option<String>,
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
    root: Option<String>,
}

#[derive(Subcommand)]
enum RestoreCommand {
    Plan(RestoreArgs),
    Apply(RestoreArgs),
    Prepare(RestoreArgs),
    Resume { job_id: String },
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
    to: PathBuf,
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
    mountpoint: PathBuf,
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
}

#[derive(Args)]
struct WatchArgs {
    #[arg(long, default_value_t = true)]
    foreground: bool,
    #[arg(long, default_value_t = 60)]
    interval_secs: u64,
    #[arg(long, default_value_t = 1500)]
    debounce_ms: u64,
    #[arg(long, default_value = "notify")]
    backend: String,
    #[arg(long, default_value_t = false)]
    once: bool,
}

#[derive(Subcommand)]
enum DaemonCommand {
    Start {
        #[arg(long, default_value_t = 60)]
        interval_secs: u64,
    },
    Stop,
    Status,
}

#[derive(Subcommand)]
enum KeyCommand {
    Export,
    Import { hex: String },
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
    upload_queue: PathBuf,
    event_queue: PathBuf,
    master_key: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    host: HostConfig,
    remote: Option<RemoteConfig>,
    large: LargeConfig,
    #[serde(default)]
    security: SecurityConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct HostConfig {
    id: String,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoteConfig {
    url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SecurityConfig {
    #[serde(default)]
    encryption: String,
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
struct PackIndex {
    version: u32,
    pack_id: String,
    pack_key: String,
    entries: Vec<PackEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PackEntry {
    oid: String,
    offset: u64,
    len: u64,
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
    archive_requested_objects: Vec<String>,
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
    kind: String,
    before_snapshot: Option<String>,
    after_snapshot: Option<String>,
    created_at: String,
    message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
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
struct LargePinExport {
    oid: String,
    pinned_at: String,
    reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LargeConfig {
    enabled: bool,
    min_size: u64,
    binary_min_size: u64,
    #[serde(default = "default_large_chunking")]
    default_chunking: String,
    chunk_size: usize,
    always: Vec<String>,
    never: Vec<String>,
    #[serde(default)]
    compression: LargeCompressionConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LargeCompressionConfig {
    enabled: bool,
    algorithm: String,
    level: i32,
    min_gain_ratio: f64,
    skip_extensions: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RootConfig {
    id: String,
    name: String,
    path: PathBuf,
    #[serde(default = "default_include")]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    follow_symlinks: bool,
    #[serde(default = "default_root_status")]
    status: String,
    #[serde(default = "default_snapshot_mode")]
    snapshot_mode: String,
    #[serde(default)]
    pre_snapshot: Option<String>,
    #[serde(default)]
    post_snapshot: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileRecord {
    root_id: String,
    path: String,
    kind: String,
    size: u64,
    mode: u32,
    modified: Option<i64>,
    payload: Payload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum Payload {
    Blob {
        oid: String,
        object_key: String,
    },
    Large {
        oid: String,
        manifest_key: String,
        chunk_count: usize,
    },
    Symlink {
        target: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotManifest {
    snapshot_id: String,
    parent: Option<String>,
    op_id: String,
    timestamp: DateTime<Utc>,
    #[serde(default)]
    root_trees: BTreeMap<String, RootSnapshot>,
    #[serde(default)]
    roots: BTreeMap<String, Vec<FileRecord>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RootSnapshot {
    tree_id: String,
    tree_key: String,
    file_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct TreeManifest {
    version: u32,
    tree_id: String,
    root_id: String,
    created_at: DateTime<Utc>,
    entries: BTreeMap<String, FileRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LargeManifest {
    version: u32,
    oid: String,
    size: u64,
    #[serde(default = "default_large_chunking")]
    chunking: String,
    chunk_size: usize,
    chunks: Vec<LargeChunk>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LargeChunk {
    index: usize,
    offset: u64,
    len: u64,
    #[serde(default)]
    stored_len: Option<u64>,
    #[serde(default = "default_chunk_compression")]
    compression: String,
    oid: String,
    object_key: String,
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
        Command::Restore { command } => restore_cmd(&paths, command),
        Command::Mount(args) => mount_cmd(&paths, args),
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
            remote: args.remote.map(|url| RemoteConfig { url }),
            large: LargeConfig {
                enabled: true,
                min_size: DEFAULT_LARGE_MIN_SIZE,
                binary_min_size: DEFAULT_LARGE_BINARY_MIN_SIZE,
                default_chunking: "fixed".into(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                always: vec![
                    "*.mp4".into(),
                    "*.mov".into(),
                    "*.mkv".into(),
                    "*.zip".into(),
                    "*.tar".into(),
                    "*.tar.zst".into(),
                    "*.sqlite".into(),
                    "*.db".into(),
                    "*.iso".into(),
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
            security: SecurityConfig {
                encryption: if args.encrypt {
                    "chacha20poly1305".into()
                } else {
                    "none".into()
                },
            },
        }
    };
    write_config(paths, &config)?;
    fs::write(&paths.host, toml::to_string_pretty(&config.host)?)?;
    if config.security.encryption != "none" && !paths.master_key.exists() {
        write_master_key(paths, &random_key_hex()?)?;
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
            validate_snapshot_mode(&args.snapshot_mode)?;
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
                status: "active".into(),
                snapshot_mode: args.snapshot_mode,
                pre_snapshot: args.pre_snapshot,
                post_snapshot: args.post_snapshot,
            };
            conn.execute(
                "insert into roots(id, data_json) values (?1, ?2)
                 on conflict(id) do update set data_json=excluded.data_json",
                params![root.id, serde_json::to_string(&root)?],
            )?;
            record_op(&conn, "root-added", None, None, Some(&root.id))?;
            println!("added root {} -> {}", root.id, root.path.display());
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
            conn.execute("delete from roots where id=?1", params![id])?;
            record_op(&conn, "root-removed", None, None, Some(&id))?;
            println!("removed root {id}");
        }
        RootCommand::Pause { id } => {
            update_root_status(&conn, &id, "paused")?;
            record_op(&conn, "root-paused", None, None, Some(&id))?;
            println!("paused root {id}");
        }
        RootCommand::Resume { id } => {
            update_root_status(&conn, &id, "active")?;
            record_op(&conn, "root-resumed", None, None, Some(&id))?;
            println!("resumed root {id}");
        }
        RootCommand::MarkDeleted { id } => {
            update_root_status(&conn, &id, "deleted")?;
            record_op(&conn, "root-mark-deleted", None, None, Some(&id))?;
            println!("marked root {id} deleted");
        }
    }
    Ok(())
}

fn snapshot(paths: &Paths, args: SnapshotArgs) -> Result<()> {
    ensure_ready(paths)?;
    record_event(
        paths,
        "snapshot-start",
        args.message.as_deref().unwrap_or("manual"),
    )?;
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let parent = current_snapshot(&conn)?;
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
            continue;
        }
        if !root.path.exists() {
            update_root_status(&conn, &root.id, "missing")?;
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
            continue;
        }
        run_pre_snapshot_hook(paths, &root)?;
        let records_result = scan_root(paths, &config, &root);
        let post_result = run_post_snapshot_hook(paths, &root);
        let records = records_result?;
        post_result?;
        large_files += records
            .iter()
            .filter(|r| matches!(r.payload, Payload::Large { .. }))
            .count();
        total_files += records.len();
        let tree = build_tree_manifest(&root.id, records)?;
        let tree_json = serde_json::to_vec_pretty(&tree)?;
        let tree_oid = blake3_hex(&tree_json);
        let tree_key = store_bytes(paths, &paths.trees, &tree_oid, &tree_json)?;
        root_trees.insert(
            root.id.clone(),
            RootSnapshot {
                tree_id: tree.tree_id,
                tree_key,
                file_count: tree.entries.len(),
            },
        );
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
        "manual-snapshot",
        manifest.parent.as_deref(),
        Some(&manifest.snapshot_id),
        args.message.as_deref(),
    )?;
    println!("snapshot {}", manifest.snapshot_id);
    println!("files {total_files}, large {large_files}");
    record_event(paths, "snapshot-finish", &manifest.snapshot_id)?;
    Ok(())
}

fn status(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let roots = roots(&conn)?;
    let current = current_snapshot(&conn)?;
    println!("home {}", paths.home.display());
    println!("roots {}", roots.len());
    for root in roots {
        let state = if root.path.exists() {
            "active"
        } else {
            "missing"
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
         from operations order by rowid desc limit ?1",
    )?;
    let rows = stmt.query_map(params![args.limit as i64], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;
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
        println!(
            "{id}\t{created}\t{kind}\t{} -> {}\t{}",
            before.unwrap_or_default(),
            after.unwrap_or_default(),
            message.unwrap_or_default()
        );
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
            println!("kind {}", op.kind);
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
    match command {
        LifecycleCommand::Policy { provider } => match provider.as_str() {
            "gcs" => {
                let policy = serde_json::json!({
                    "rule": [
                        {
                            "action": { "type": "SetStorageClass", "storageClass": "NEARLINE" },
                            "condition": {
                                "age": 30,
                                "matchesPrefix": ["objects/packs/normal/"]
                            }
                        },
                        {
                            "action": { "type": "SetStorageClass", "storageClass": "ARCHIVE" },
                            "condition": {
                                "age": 180,
                                "matchesPrefix": ["objects/large/chunks/fixed/"]
                            }
                        }
                    ]
                });
                println!("{}", serde_json::to_string_pretty(&policy)?);
            }
            "s3" | "aws" => {
                let policy = serde_json::json!({
                    "Rules": [
                        {
                            "ID": "majutsu-packs-to-ia",
                            "Status": "Enabled",
                            "Filter": { "Prefix": "objects/packs/normal/" },
                            "Transitions": [
                                { "Days": 30, "StorageClass": "STANDARD_IA" }
                            ]
                        },
                        {
                            "ID": "majutsu-large-to-archive",
                            "Status": "Enabled",
                            "Filter": { "Prefix": "objects/large/chunks/fixed/" },
                            "Transitions": [
                                { "Days": 180, "StorageClass": "DEEP_ARCHIVE" }
                            ]
                        }
                    ]
                });
                println!("{}", serde_json::to_string_pretty(&policy)?);
            }
            other => bail!("unsupported lifecycle provider: {other}"),
        },
    }
    Ok(())
}

fn diff_cmd(paths: &Paths, args: DiffArgs) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let to_id = args
        .to
        .or_else(|| current_snapshot(&conn).ok().flatten())
        .ok_or_else(|| anyhow!("no target snapshot"))?;
    let to = load_snapshot_by_id(&conn, &to_id)?;
    let from_id = args.from.or_else(|| to.parent.clone());
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

fn restore_cmd(paths: &Paths, command: RestoreCommand) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    match command {
        RestoreCommand::Plan(args) => {
            let plan = build_restore_plan(paths, &conn, &args)?;
            print_restore_plan(&plan);
        }
        RestoreCommand::Apply(args) => {
            let plan = build_restore_plan(paths, &conn, &args)?;
            apply_restore_plan(paths, &plan)?;
            let after = plan.snapshot.snapshot_id.as_str();
            record_op(
                &conn,
                "restore",
                None,
                Some(after),
                Some(&format!("to {}", plan.to.display())),
            )?;
            print_restore_plan(&plan);
            println!("restored to {}", plan.to.display());
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
            println!(
                "archive_requested_objects {}",
                job.archive_requested_objects.len()
            );
        }
        RestoreCommand::Resume { job_id } => {
            let job = read_restore_job(paths, &job_id)?;
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
                to: PathBuf::from(&job.target),
            };
            let plan = build_restore_plan(paths, &conn, &args)?;
            apply_restore_plan(paths, &plan)?;
            mark_restore_job_done(paths, &job.id)?;
            record_op(
                &conn,
                "restore-resume",
                None,
                Some(&plan.snapshot.snapshot_id),
                Some(&job.id),
            )?;
            println!("resumed {}", job.id);
            println!("restored to {}", plan.to.display());
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
        to: args.mountpoint.clone(),
    };
    let plan = build_restore_plan(paths, &conn, &restore_args)?;
    fs::create_dir_all(&plan.to)?;
    let lazy_root = plan.to.join(".majutsu-lazy");
    let mut lazy_files = 0usize;
    let mut hydrated_large = 0usize;
    for record in &plan.files {
        let dest = plan.to.join(&record.root_id).join(&record.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        match &record.payload {
            Payload::Blob { oid, object_key } => {
                write_atomic(&dest, &read_blob_payload(paths, &conn, oid, object_key)?)?;
            }
            Payload::Large {
                manifest_key,
                chunk_count,
                ..
            } if args.hydrate_large => {
                let manifest: LargeManifest =
                    serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                let tmp = dest.with_extension("mjtmp");
                let mut out = File::create(&tmp)?;
                for chunk in manifest.chunks {
                    out.write_all(&read_large_chunk(paths, &chunk)?)?;
                }
                out.sync_all()?;
                fs::rename(tmp, dest)?;
                hydrated_large += 1;
                let _ = chunk_count;
            }
            Payload::Large {
                manifest_key,
                chunk_count,
                ..
            } => {
                let file = File::create(&dest)?;
                file.set_len(record.size)?;
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
                    manifest_key: manifest_key.clone(),
                    chunk_count: *chunk_count,
                };
                fs::write(sidecar, serde_json::to_vec_pretty(&entry)?)?;
                lazy_files += 1;
            }
            Payload::Symlink { target } => {
                #[cfg(unix)]
                std::os::unix::fs::symlink(target, &dest)?;
                #[cfg(not(unix))]
                fs::write(&dest, target)?;
            }
        }
    }
    record_op(
        &conn,
        "mount",
        None,
        Some(&plan.snapshot.snapshot_id),
        Some(&format!("at {}", plan.to.display())),
    )?;
    println!("mounted snapshot {}", plan.snapshot.snapshot_id);
    println!("target {}", plan.to.display());
    println!("files {}", plan.files.len());
    println!("lazy_large_files {lazy_files}");
    println!("hydrated_large_files {hydrated_large}");
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
            let manifest = load_snapshot_by_id(&conn, &snapshot)?;
            let mut pinned = 0usize;
            for (root_id, records) in manifest.roots {
                if args.root.as_deref().is_some_and(|filter| filter != root_id) {
                    continue;
                }
                for record in records {
                    if let Payload::Large { oid, .. } = record.payload {
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

fn sync_cmd(paths: &Paths, command: Option<SyncCommand>) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let remote = open_remote(
        config
            .remote
            .as_ref()
            .ok_or_else(|| anyhow!("remote is not configured; run `mj init --remote ...`"))?,
    )?;
    let conn = open_db(paths)?;
    if let Some(SyncCommand::Status) = command {
        return sync_status(paths, &conn, &remote);
    }
    let current = current_snapshot(&conn)?;
    record_op(
        &conn,
        "remote-sync",
        current.as_deref(),
        current.as_deref(),
        Some("pushed metadata and objects"),
    )?;
    let export = export_metadata(&conn, &config)?;
    let export_json = serde_json::to_vec_pretty(&export)?;
    enqueue_inline_upload(paths, "metadata/export.json", export_json)?;
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
    }

    for key in local_object_keys(&export) {
        let local = paths.home.join(&key);
        if local.exists() {
            enqueue_file_upload(paths, &key, &local)?;
        }
    }
    let uploaded = drain_upload_queue(paths, &remote)?;
    println!("synced {} objects to {}", uploaded, remote.describe());
    Ok(())
}

fn sync_status(paths: &Paths, conn: &Connection, remote: &RemoteStore) -> Result<()> {
    let local_current = current_snapshot(conn)?;
    let remote_current = if remote.exists("hosts/current")? {
        Some(
            String::from_utf8(remote.get("hosts/current")?)?
                .trim()
                .to_string(),
        )
    } else {
        None
    };
    let export = export_metadata(conn, &read_config(paths)?)?;
    let local_keys = local_object_keys(&export);
    let mut missing_remote = 0usize;
    for key in &local_keys {
        if !remote.exists(key)? {
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
    println!("local_objects {}", local_keys.len());
    println!("missing_remote_objects {}", missing_remote);
    println!("queued_uploads {}", upload_queue_items(paths)?.len());
    Ok(())
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
        match remote.put(&item.key, &bytes) {
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

fn remote_cmd(paths: &Paths, command: RemoteCommand) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let remote = open_remote(
        config
            .remote
            .as_ref()
            .ok_or_else(|| anyhow!("remote is not configured; run `mj init --remote ...`"))?,
    )?;
    match command {
        RemoteCommand::Check => {
            let keys = remote.list("")?;
            println!("remote {}", remote.describe());
            println!("objects {}", keys.len());
            if remote.exists("metadata/export.json")? {
                println!("metadata ok");
                let first = remote.get_range("metadata/export.json", 0, 1)?;
                println!("range_get {}", first.len());
            } else {
                bail!("metadata/export.json is missing on remote");
            }
        }
        RemoteCommand::Fsck => {
            remote_fsck(&remote)?;
        }
    }
    Ok(())
}

fn clone_cmd(paths: &Paths, args: CloneArgs) -> Result<()> {
    if paths.home.exists() && paths.home.read_dir()?.next().is_some() {
        bail!("target majutsu home is not empty: {}", paths.home.display());
    }
    create_layout(paths)?;
    let remote_config = RemoteConfig { url: args.remote };
    let remote = open_remote(&remote_config)?;
    let export_bytes = remote.get("metadata/export.json")?;
    let mut export: MetadataExport = serde_json::from_slice(&export_bytes)?;
    export.config.remote = Some(remote_config);
    write_config(paths, &export.config)?;
    fs::write(&paths.host, toml::to_string_pretty(&export.config.host)?)?;
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
            remote
                .get(&key)
                .with_context(|| format!("download {key}"))?,
        )?;
    }
    let conn = open_db(paths)?;
    import_metadata(&conn, &export)?;
    println!("cloned {} into {}", remote.describe(), paths.home.display());
    println!("host {} {}", export.config.host.name, export.config.host.id);
    Ok(())
}

fn watch_cmd(paths: &Paths, args: WatchArgs) -> Result<()> {
    ensure_ready(paths)?;
    if !args.foreground {
        bail!("daemonized watch is not implemented yet; use --foreground");
    }
    start_daemon_ipc(paths)?;
    match args.backend.as_str() {
        "notify" => watch_notify(paths, args),
        "poll" => watch_poll(paths, args),
        other => bail!("unsupported watch backend: {other}"),
    }
}

fn watch_poll(paths: &Paths, args: WatchArgs) -> Result<()> {
    record_event(
        paths,
        "watch-start",
        &format!("backend=poll interval_secs={}", args.interval_secs),
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

fn watch_notify(paths: &Paths, args: WatchArgs) -> Result<()> {
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
        &format!("backend=notify debounce_ms={}", args.debounce_ms),
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
    loop {
        let event = rx.recv()??;
        let detail = format_notify_event(&event);
        record_event(paths, "fs-event", &detail)?;
        let debounce = std::time::Duration::from_millis(args.debounce_ms.max(1));
        loop {
            match rx.recv_timeout(debounce) {
                Ok(Ok(next)) => {
                    record_event(paths, "fs-event", &format_notify_event(&next))?;
                    continue;
                }
                Ok(Err(err)) => return Err(err.into()),
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => bail!("watch channel disconnected"),
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
    record_event(paths, "watch-stop", "foreground notify watch stopped")?;
    Ok(())
}

fn daemon_cmd(paths: &Paths, command: DaemonCommand) -> Result<()> {
    ensure_ready(paths)?;
    match command {
        DaemonCommand::Start { interval_secs } => {
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
                .arg("--backend")
                .arg("notify")
                .arg("--interval-secs")
                .arg(interval_secs.to_string())
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log))
                .spawn()?;
            fs::write(&paths.daemon_pid, child.id().to_string())?;
            println!("started daemon pid {}", child.id());
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
    let conn = open_db(paths)?;
    let blobs = query_blobs(&conn)?
        .into_iter()
        .filter(|blob| blob.pack_id.is_none())
        .collect::<Vec<_>>();
    if blobs.is_empty() {
        println!("packed 0 objects");
        return Ok(());
    }
    let pack_id = new_id("pack");
    let pack_key = format!("objects/packs/normal/{}.mpack", pack_id);
    let index_key = format!("objects/indexes/pack/{}.json", pack_id);
    let mut pack_bytes = Vec::new();
    let mut entries = Vec::new();
    for blob in &blobs {
        let payload = read_object(paths, &blob.object_key)?;
        let stored = encode_object(paths, &payload)?;
        let offset = pack_bytes.len() as u64;
        pack_bytes.extend_from_slice(&(stored.len() as u64).to_le_bytes());
        pack_bytes.extend_from_slice(&stored);
        entries.push(PackEntry {
            oid: blob.oid.clone(),
            offset,
            len: 8 + stored.len() as u64,
        });
    }
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
        params![pack_id, pack_key, index_key, blobs.len(), pack_bytes.len() as u64],
    )?;
    for entry in &index.entries {
        conn.execute(
            "update blobs set pack_id=?2, pack_offset=?3, pack_len=?4 where oid=?1",
            params![entry.oid, index.pack_id, entry.offset, entry.len],
        )?;
    }
    record_op(
        &conn,
        "pack",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!("packed {} blobs", blobs.len())),
    )?;
    println!("packed {} objects into {}", blobs.len(), index.pack_key);
    Ok(())
}

fn pack_compact_cmd(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let blobs = query_blobs(&conn)?;
    let packed = blobs.iter().filter(|blob| blob.pack_id.is_some()).count();
    if packed == 0 {
        println!("compacted 0 objects");
        return Ok(());
    }
    let pack_id = new_id("pack");
    let pack_key = format!("objects/packs/normal/{}.mpack", pack_id);
    let index_key = format!("objects/indexes/pack/{}.json", pack_id);
    let mut pack_bytes = Vec::new();
    let mut entries = Vec::new();
    for blob in &blobs {
        let payload = read_blob_payload(paths, &conn, &blob.oid, &blob.object_key)?;
        let stored = encode_object(paths, &payload)?;
        let offset = pack_bytes.len() as u64;
        pack_bytes.extend_from_slice(&(stored.len() as u64).to_le_bytes());
        pack_bytes.extend_from_slice(&stored);
        entries.push(PackEntry {
            oid: blob.oid.clone(),
            offset,
            len: 8 + stored.len() as u64,
        });
    }
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
    conn.execute("delete from packs", [])?;
    conn.execute(
        "insert into packs(pack_id, pack_key, index_key, object_count, size) values (?1, ?2, ?3, ?4, ?5)",
        params![pack_id, pack_key, index_key, blobs.len(), pack_bytes.len() as u64],
    )?;
    for entry in &index.entries {
        conn.execute(
            "update blobs set pack_id=?2, pack_offset=?3, pack_len=?4 where oid=?1",
            params![entry.oid, index.pack_id, entry.offset, entry.len],
        )?;
    }
    record_op(
        &conn,
        "pack-compact",
        current_snapshot(&conn)?.as_deref(),
        current_snapshot(&conn)?.as_deref(),
        Some(&format!("compacted {} blobs", blobs.len())),
    )?;
    println!("compacted {} objects into {}", blobs.len(), index.pack_key);
    Ok(())
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            encryption: "none".into(),
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
                match &record.payload {
                    Payload::Blob { oid, .. } => {
                        live_blobs.insert(oid.clone());
                    }
                    Payload::Large {
                        oid, manifest_key, ..
                    } => {
                        live_large.insert(oid.clone());
                        let large_manifest: LargeManifest =
                            serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                        for chunk in large_manifest.chunks {
                            live_chunks.insert(chunk.oid);
                        }
                    }
                    Payload::Symlink { .. } => {}
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
    if !remote.exists("metadata/export.json")? {
        bail!("metadata/export.json is missing on remote");
    }
    let export: MetadataExport = serde_json::from_slice(&remote.get("metadata/export.json")?)?;
    let mut missing = 0usize;
    for key in local_object_keys(&export) {
        if !remote.exists(&key)? {
            missing += 1;
            eprintln!("missing remote object {key}");
        }
    }
    if let Some(current) = export.refs.get("current") {
        let found = export
            .snapshots
            .iter()
            .any(|snapshot| &snapshot.id == current);
        if !found {
            missing += 1;
            eprintln!("remote current ref points to missing snapshot {current}");
        }
    }
    if missing > 0 {
        bail!("remote fsck found {missing} issue(s)");
    }
    println!("remote fsck ok");
    println!("snapshots {}", export.snapshots.len());
    println!("objects {}", local_object_keys(&export).len());
    Ok(())
}

#[derive(Debug)]
struct RestorePlan {
    snapshot: SnapshotManifest,
    to: PathBuf,
    files: Vec<FileRecord>,
}

fn build_restore_plan(
    _paths: &Paths,
    conn: &Connection,
    args: &RestoreArgs,
) -> Result<RestorePlan> {
    let snapshot = load_snapshot(conn, args)?;
    let mut files = Vec::new();
    for (root_id, records) in &snapshot.roots {
        if let Some(filter_root) = &args.root {
            if filter_root != root_id {
                continue;
            }
        }
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
                payload: match &record.payload {
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
                },
            });
        }
    }
    Ok(RestorePlan {
        snapshot,
        to: args.to.clone(),
        files,
    })
}

fn print_restore_plan(plan: &RestorePlan) {
    let large = plan
        .files
        .iter()
        .filter(|r| matches!(r.payload, Payload::Large { .. }))
        .count();
    let bytes: u64 = plan.files.iter().map(|r| r.size).sum();
    println!("snapshot {}", plan.snapshot.snapshot_id);
    println!("target {}", plan.to.display());
    println!(
        "restore {} files, {} bytes, {} large files",
        plan.files.len(),
        bytes,
        large
    );
}

fn build_restore_job(
    paths: &Paths,
    plan: &RestorePlan,
    args: &RestoreArgs,
) -> Result<RestoreQueueItem> {
    let conn = open_db(paths)?;
    let required_objects = required_object_keys_for_plan(paths, &conn, plan)?;
    let archived_objects = required_objects
        .iter()
        .filter(|key| !paths.home.join(key).exists())
        .cloned()
        .collect::<Vec<_>>();
    Ok(RestoreQueueItem {
        id: new_id("restore"),
        snapshot_id: plan.snapshot.snapshot_id.clone(),
        root: args.root.clone(),
        path: args.path.as_ref().map(|path| path_to_slash(path)),
        target: args.to.display().to_string(),
        required_objects,
        archived_objects,
        archive_requested_objects: Vec::new(),
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
        if remote.restore_archive(key, 7, "Standard")? {
            requested.push(key.clone());
        }
    }
    if !requested.is_empty() {
        job.status = "archive-requested".into();
        job.archive_requested_objects = requested;
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
        match &record.payload {
            Payload::Blob { oid, object_key } => {
                let blob = query_blobs(conn)?
                    .into_iter()
                    .find(|blob| blob.oid == *oid)
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
                    keys.push(object_key.clone());
                }
            }
            Payload::Large { manifest_key, .. } => {
                keys.push(manifest_key.clone());
                let manifest: LargeManifest =
                    serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                for chunk in manifest.chunks {
                    keys.push(chunk.object_key);
                }
            }
            Payload::Symlink { .. } => {}
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

fn mark_restore_job_done(paths: &Paths, job_id: &str) -> Result<()> {
    let mut job = read_restore_job(paths, job_id)?;
    job.status = "done".into();
    write_restore_job(paths, &job)
}

fn apply_restore_plan(paths: &Paths, plan: &RestorePlan) -> Result<()> {
    let conn = open_db(paths)?;
    for record in &plan.files {
        let dest = plan.to.join(&record.root_id).join(&record.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        match &record.payload {
            Payload::Blob { oid, object_key } => {
                write_atomic(&dest, &read_blob_payload(paths, &conn, oid, object_key)?)?;
            }
            Payload::Large { manifest_key, .. } => {
                let manifest: LargeManifest =
                    serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                let tmp = dest.with_extension("mjtmp");
                let mut out = File::create(&tmp)?;
                for chunk in manifest.chunks {
                    out.write_all(&read_large_chunk(paths, &chunk)?)?;
                }
                out.sync_all()?;
                fs::rename(tmp, dest)?;
            }
            Payload::Symlink { target } => {
                #[cfg(unix)]
                std::os::unix::fs::symlink(target, &dest)?;
                #[cfg(not(unix))]
                fs::write(&dest, target)?;
            }
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
            continue;
        }
        let meta = fs::symlink_metadata(entry.path())?;
        if meta.file_type().is_symlink() {
            let target = fs::read_link(entry.path())?.to_string_lossy().to_string();
            records.push(FileRecord {
                root_id: root.id.clone(),
                path: rel_s,
                kind: "symlink".into(),
                size: 0,
                mode: file_mode(&meta),
                modified: modified_secs(&meta),
                payload: Payload::Symlink { target },
            });
            continue;
        }
        if !meta.is_file() {
            continue;
        }
        let binary = looks_binary(entry.path()).unwrap_or(false);
        let large = classify_large(config, &rel, meta.len(), binary);
        let payload = if large {
            let (oid, manifest_key, chunk_count) =
                store_large_file(paths, entry.path(), &rel, &config.large)?;
            Payload::Large {
                oid,
                manifest_key,
                chunk_count,
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
            Payload::Blob { oid, object_key }
        };
        records.push(FileRecord {
            root_id: root.id.clone(),
            path: rel_s,
            kind: "file".into(),
            size: meta.len(),
            mode: file_mode(&meta),
            modified: modified_secs(&meta),
            payload,
        });
    }
    Ok(records)
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
) -> Result<(String, String, usize)> {
    let bytes = stable_read(path, "strict")?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes);
    let mut chunks = Vec::new();
    let ranges = large_chunk_ranges_for_bytes(config, &bytes);
    for (index, (start, end)) in ranges.into_iter().enumerate() {
        let chunk = &bytes[start..end];
        let chunk_oid = blake3_hex(chunk);
        let stored = compress_large_chunk(config, rel, chunk)?;
        let object_key = store_bytes(paths, &paths.large_chunks, &chunk_oid, &stored.bytes)?;
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

fn large_chunk_ranges_for_bytes(config: &LargeConfig, bytes: &[u8]) -> Vec<(usize, usize)> {
    if config.default_chunking == "fastcdc" {
        content_defined_ranges(bytes, config.chunk_size)
    } else {
        fixed_ranges(bytes.len(), config.chunk_size)
    }
}

fn fixed_ranges(len: usize, chunk_size: usize) -> Vec<(usize, usize)> {
    let chunk_size = chunk_size.max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    while start < len {
        let end = (start + chunk_size).min(len);
        ranges.push((start, end));
        start = end;
    }
    ranges
}

fn content_defined_ranges(bytes: &[u8], target: usize) -> Vec<(usize, usize)> {
    let len = bytes.len();
    let target = target.max(1024);
    let min = (target / 4).max(1024).min(target);
    let max = (target * 4).max(min + 1);
    let mask = target.next_power_of_two().saturating_sub(1).max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    let mut rolling = 0u64;
    while start < len {
        let hard_end = (start + max).min(len);
        let mut end = hard_end;
        let mut i = start;
        while i < hard_end {
            rolling = rolling
                .rotate_left(1)
                .wrapping_add(bytes[i] as u64)
                .wrapping_mul(0x9E37_79B1_85EB_CA87);
            let current_len = i + 1 - start;
            if current_len >= min && ((rolling as usize) & mask) == 0 {
                end = i + 1;
                break;
            }
            i += 1;
        }
        ranges.push((start, end));
        start = end;
    }
    ranges
}

struct StoredLargeChunk {
    bytes: Vec<u8>,
    compression: String,
}

fn compress_large_chunk(
    config: &LargeConfig,
    rel: &Path,
    bytes: &[u8],
) -> Result<StoredLargeChunk> {
    if !should_compress_large(config, rel) {
        return Ok(StoredLargeChunk {
            bytes: bytes.to_vec(),
            compression: "none".into(),
        });
    }
    let compressed = zstd::stream::encode_all(bytes, config.compression.level)?;
    let gain = 1.0 - (compressed.len() as f64 / bytes.len().max(1) as f64);
    if gain >= config.compression.min_gain_ratio {
        Ok(StoredLargeChunk {
            bytes: compressed,
            compression: "zstd".into(),
        })
    } else {
        Ok(StoredLargeChunk {
            bytes: bytes.to_vec(),
            compression: "none".into(),
        })
    }
}

fn should_compress_large(config: &LargeConfig, rel: &Path) -> bool {
    if !config.compression.enabled || config.compression.algorithm != "zstd" {
        return false;
    }
    let name = rel.file_name().and_then(OsStr::to_str).unwrap_or_default();
    !config
        .compression
        .skip_extensions
        .iter()
        .any(|pattern| glob_match(pattern, name))
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
    fs::create_dir_all(&paths.large_manifests)?;
    fs::create_dir_all(&paths.packs)?;
    fs::create_dir_all(&paths.pack_indexes)?;
    fs::create_dir_all(&paths.logs)?;
    for dir in [
        "ops",
        "queue/events",
        "queue/uploads",
        "queue/restores",
        "cache",
        "keys",
        "locks",
        "runtime",
    ] {
        fs::create_dir_all(paths.home.join(dir))?;
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
          kind text not null,
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
    Ok(())
}

fn export_metadata(conn: &Connection, config: &Config) -> Result<MetadataExport> {
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

    let mut operations = Vec::new();
    let mut stmt = conn.prepare(
        "select id, kind, before_snapshot, after_snapshot, created_at, message from operations order by created_at",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(OperationExport {
            id: row.get(0)?,
            kind: row.get(1)?,
            before_snapshot: row.get(2)?,
            after_snapshot: row.get(3)?,
            created_at: row.get(4)?,
            message: row.get(5)?,
        })
    })?;
    for row in rows {
        operations.push(row?);
    }

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
            remote: config.remote.as_ref().map(|remote| RemoteConfig {
                url: remote.url.clone(),
            }),
            large: LargeConfig {
                enabled: config.large.enabled,
                min_size: config.large.min_size,
                binary_min_size: config.large.binary_min_size,
                default_chunking: config.large.default_chunking.clone(),
                chunk_size: config.large.chunk_size,
                always: config.large.always.clone(),
                never: config.large.never.clone(),
                compression: config.large.compression.clone(),
            },
            security: config.security.clone(),
        },
        roots: roots(conn)?,
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
            "insert or replace into operations(id, kind, before_snapshot, after_snapshot, created_at, message)
             values (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                op.id,
                op.kind,
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

fn update_root_status(conn: &Connection, id: &str, status: &str) -> Result<()> {
    let data: String = conn
        .query_row(
            "select data_json from roots where id=?1",
            params![id],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| anyhow!("unknown root: {id}"))?;
    let mut root: RootConfig = serde_json::from_str(&data)?;
    root.status = status.to_string();
    conn.execute(
        "update roots set data_json=?2 where id=?1",
        params![id, serde_json::to_string(&root)?],
    )?;
    Ok(())
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
        conn.query_row(
            "select id from snapshots where created_at <= ?1 order by created_at desc limit 1",
            params![parse_time(at)?],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| anyhow!("no snapshot at or before {at}"))?
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
    conn.execute(
        "insert into operations(id, kind, before_snapshot, after_snapshot, created_at, message)
         values (?1, ?2, ?3, ?4, ?5, ?6)",
        params![id, kind, before, after, Utc::now().to_rfc3339(), message],
    )?;
    Ok(())
}

fn query_operation(conn: &Connection, op_id: &str) -> Result<OperationExport> {
    conn.query_row(
        "select id, kind, before_snapshot, after_snapshot, created_at, message from operations where id=?1",
        params![op_id],
        |row| {
            Ok(OperationExport {
                id: row.get(0)?,
                kind: row.get(1)?,
                before_snapshot: row.get(2)?,
                after_snapshot: row.get(3)?,
                created_at: row.get(4)?,
                message: row.get(5)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| anyhow!("unknown operation: {op_id}"))
}

fn read_config(paths: &Paths) -> Result<Config> {
    Ok(toml::from_str(&fs::read_to_string(&paths.config)?)?)
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

fn run_pre_snapshot_hook(paths: &Paths, root: &RootConfig) -> Result<()> {
    if root.snapshot_mode == "transactional" {
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
    }
    Ok(())
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

fn classify_large(config: &Config, rel: &Path, size: u64, binary: bool) -> bool {
    if !config.large.enabled {
        return false;
    }
    let name = rel.file_name().and_then(OsStr::to_str).unwrap_or_default();
    if config.large.never.iter().any(|p| glob_match(p, name)) {
        return false;
    }
    if config.large.always.iter().any(|p| glob_match(p, name)) {
        return true;
    }
    size >= config.large.min_size || (binary && size >= config.large.binary_min_size)
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

fn default_chunk_compression() -> String {
    "none".into()
}

impl Default for LargeCompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: "zstd".into(),
            level: 3,
            min_gain_ratio: 0.05,
            skip_extensions: vec![
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
            ],
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

fn pid_alive(pid: u32) -> bool {
    ProcessCommand::new("kill")
        .arg("-0")
        .arg(pid.to_string())
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

fn stable_read(path: &Path, mode: &str) -> Result<Vec<u8>> {
    let attempts = if mode == "strict" { 8 } else { 3 };
    let mut last_error = None;
    for attempt in 0..attempts {
        let before = fs::metadata(path)?;
        let bytes = fs::read(path)?;
        let after = fs::metadata(path)?;
        if before.len() == after.len() && before.modified().ok() == after.modified().ok() {
            return Ok(bytes);
        }
        last_error = Some(anyhow!("file changed while reading: {}", path.display()));
        std::thread::sleep(std::time::Duration::from_millis(25 * (attempt + 1) as u64));
    }
    Err(last_error.unwrap_or_else(|| anyhow!("file did not become stable: {}", path.display())))
}

fn store_bytes(paths: &Paths, base: &Path, oid: &str, bytes: &[u8]) -> Result<String> {
    let (a, b) = oid.split_at(2);
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

fn write_atomic(dest: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = dest.with_extension("mjtmp");
    let mut f = File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    fs::rename(tmp, dest)?;
    Ok(())
}

const ENC_MAGIC: &[u8] = b"MJENC1\n";

fn encode_object(paths: &Paths, bytes: &[u8]) -> Result<Vec<u8>> {
    let config = if paths.config.exists() {
        Some(read_config(paths)?)
    } else {
        None
    };
    if config
        .as_ref()
        .map(|config| config.security.encryption.as_str() == "chacha20poly1305")
        .unwrap_or(false)
    {
        let key_hex = read_master_key(paths)?;
        let key_bytes = hex::decode(key_hex.trim())?;
        let key = Key::from_slice(&key_bytes);
        let cipher = ChaCha20Poly1305::new(key);
        let mut nonce_bytes = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce_bytes), bytes)
            .map_err(|_| anyhow!("object encryption failed"))?;
        let mut out = Vec::with_capacity(ENC_MAGIC.len() + nonce_bytes.len() + ciphertext.len());
        out.extend_from_slice(ENC_MAGIC);
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
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
    if !bytes.starts_with(ENC_MAGIC) {
        return Ok(bytes.to_vec());
    }
    let start = ENC_MAGIC.len();
    if bytes.len() < start + 12 {
        bail!("encrypted object is truncated");
    }
    let nonce = &bytes[start..start + 12];
    let ciphertext = &bytes[start + 12..];
    let key_hex = read_master_key(paths)?;
    let key_bytes = hex::decode(key_hex.trim())?;
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key_bytes));
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| anyhow!("object decryption failed"))
}

fn random_key_hex() -> Result<String> {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    Ok(hex::encode(key))
}

fn validate_key_hex(hex_key: &str) -> Result<()> {
    let bytes = hex::decode(hex_key.trim())?;
    if bytes.len() != 32 {
        bail!("master key must be 32 bytes encoded as 64 hex characters");
    }
    Ok(())
}

fn read_master_key(paths: &Paths) -> Result<String> {
    if let Ok(key) = env::var("MAJUTSU_MASTER_KEY") {
        validate_key_hex(&key)?;
        return Ok(key);
    }
    let key = fs::read_to_string(&paths.master_key)
        .with_context(|| format!("missing master key: {}", paths.master_key.display()))?;
    validate_key_hex(key.trim())?;
    Ok(key.trim().to_string())
}

fn write_master_key(paths: &Paths, hex_key: &str) -> Result<()> {
    validate_key_hex(hex_key)?;
    if let Some(parent) = paths.master_key.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&paths.master_key, format!("{}\n", hex_key.trim()))?;
    Ok(())
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
    if input == "now" {
        return Ok(Utc::now().to_rfc3339());
    }
    bail!("time must be RFC3339 for now, got: {input}");
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
    client: Client,
}

fn open_remote(config: &RemoteConfig) -> Result<RemoteStore> {
    if let Some(path) = config.url.strip_prefix("file://") {
        return Ok(RemoteStore::File(FileRemote {
            root: PathBuf::from(path),
        }));
    }
    if config.url.starts_with("s3://") {
        let url = Url::parse(&config.url)?;
        let bucket = url
            .host_str()
            .ok_or_else(|| anyhow!("s3 remote is missing bucket: {}", config.url))?
            .to_string();
        let prefix = url.path().trim_matches('/').to_string();
        return Ok(RemoteStore::S3(S3Remote {
            bucket,
            prefix,
            endpoint: env::var("AWS_ENDPOINT_URL")
                .unwrap_or_else(|_| "https://storage.googleapis.com".into()),
            region: env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into()),
            signature_version: env::var("AWS_SIGNATURE_VERSION").unwrap_or_else(|_| "s3v4".into()),
            access_key: env::var("AWS_ACCESS_KEY_ID")
                .context("AWS_ACCESS_KEY_ID is required for s3 remote")?,
            secret_key: env::var("AWS_SECRET_ACCESS_KEY")
                .context("AWS_SECRET_ACCESS_KEY is required for s3 remote")?,
            client: Client::new(),
        }));
    }
    bail!("unsupported remote URL: {}", config.url);
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

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        match self {
            RemoteStore::File(remote) => Ok(fs::read(remote.root.join(key))?),
            RemoteStore::S3(remote) => remote.get(key),
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
}

impl S3Remote {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        if !self.uses_sigv2() && bytes.len() >= self.multipart_threshold() {
            return self.put_multipart(key, bytes);
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
            let auth = self.auth_v4("PUT", &remote_key, "", &payload_hash, &[])?;
            self.client
                .put(url)
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(bytes.to_vec())
                .send()?
        };
        if !response.status().is_success() {
            bail!("s3 put failed for {key}: HTTP {}", response.status());
        }
        Ok(())
    }

    fn put_multipart(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let remote_key = self.remote_key(key);
        let upload_id = self.initiate_multipart(&remote_key)?;
        let result = (|| {
            let mut parts = Vec::new();
            for (idx, chunk) in bytes.chunks(MIN_MULTIPART_PART_SIZE).enumerate() {
                let part_number = idx + 1;
                let etag = self.upload_part(&remote_key, &upload_id, part_number, chunk)?;
                parts.push(CompletedPart { part_number, etag });
            }
            self.complete_multipart(&remote_key, &upload_id, &parts)
        })();
        if result.is_err() {
            let _ = self.abort_multipart(&remote_key, &upload_id);
        }
        result.with_context(|| format!("multipart upload failed for {key}"))
    }

    fn initiate_multipart(&self, remote_key: &str) -> Result<String> {
        let query = "uploads=".to_string();
        let payload_hash = sha256_hex(b"");
        let auth = self.auth_v4("POST", remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .post(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .body(Vec::new())
            .send()?;
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
