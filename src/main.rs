use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use hmac::{Hmac, Mac};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, DATE};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use url::Url;
use uuid::Uuid;
use walkdir::WalkDir;

const DEFAULT_LARGE_MIN_SIZE: u64 = 64 * 1024 * 1024;
const DEFAULT_LARGE_BINARY_MIN_SIZE: u64 = 16 * 1024 * 1024;
const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

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
    Restore {
        #[command(subcommand)]
        command: RestoreCommand,
    },
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
    Clone(CloneArgs),
    Watch(WatchArgs),
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    Fsck,
}

#[derive(Args)]
struct InitArgs {
    #[arg(long)]
    remote: Option<String>,
    #[arg(long)]
    host_name: Option<String>,
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
}

#[derive(Subcommand)]
enum RestoreCommand {
    Plan(RestoreArgs),
    Apply(RestoreArgs),
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

#[derive(Subcommand)]
enum LargeCommand {
    List,
    Stat,
    Verify,
}

#[derive(Subcommand)]
enum SyncCommand {
    Status,
}

#[derive(Subcommand)]
enum RemoteCommand {
    Check,
    Fsck,
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

#[derive(Debug)]
struct Paths {
    home: PathBuf,
    db: PathBuf,
    config: PathBuf,
    host: PathBuf,
    objects: PathBuf,
    large_chunks: PathBuf,
    large_manifests: PathBuf,
    logs: PathBuf,
    runtime: PathBuf,
    daemon_pid: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    host: HostConfig,
    remote: Option<RemoteConfig>,
    large: LargeConfig,
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
struct LargeConfig {
    enabled: bool,
    min_size: u64,
    binary_min_size: u64,
    chunk_size: usize,
    always: Vec<String>,
    never: Vec<String>,
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
    roots: BTreeMap<String, Vec<FileRecord>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LargeManifest {
    version: u32,
    oid: String,
    size: u64,
    chunk_size: usize,
    chunks: Vec<LargeChunk>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LargeChunk {
    index: usize,
    offset: u64,
    len: u64,
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
        Command::Restore { command } => restore_cmd(&paths, command),
        Command::Large { command } => large_cmd(&paths, command),
        Command::Sync { command } => sync_cmd(&paths, command),
        Command::Remote { command } => remote_cmd(&paths, command),
        Command::Clone(args) => clone_cmd(&paths, args),
        Command::Watch(args) => watch_cmd(&paths, args),
        Command::Daemon { command } => daemon_cmd(&paths, command),
        Command::Fsck => fsck(&paths),
    }
}

fn resolve_paths(home_arg: Option<PathBuf>) -> Result<Paths> {
    let home = if let Some(home) = home_arg {
        home
    } else if let Ok(home) = env::var("MAJUTSU_HOME") {
        PathBuf::from(home)
    } else {
        let user_home = env::var("HOME").context("HOME is not set")?;
        PathBuf::from(user_home).join(".majutsu")
    };
    Ok(Paths {
        db: home.join("db/majutsu.sqlite"),
        config: home.join("config.toml"),
        host: home.join("host.toml"),
        objects: home.join("objects/blobs"),
        large_chunks: home.join("objects/large/chunks/fixed"),
        large_manifests: home.join("objects/large/manifests"),
        logs: home.join("logs"),
        runtime: home.join("runtime"),
        daemon_pid: home.join("runtime/daemon.pid"),
        home,
    })
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
            },
        }
    };
    write_config(paths, &config)?;
    fs::write(&paths.host, toml::to_string_pretty(&config.host)?)?;
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
    let config = read_config(paths)?;
    let conn = open_db(paths)?;
    let parent = current_snapshot(&conn)?;
    let op_id = new_id("op");
    let snapshot_id = new_id("snap");
    let mut by_root = BTreeMap::new();
    let mut total_files = 0usize;
    let mut large_files = 0usize;
    for root in roots(&conn)? {
        if root.status != "active" {
            eprintln!("root {}, skipped: status={}", root.id, root.status);
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
            continue;
        }
        let records = scan_root(paths, &config, &root)?;
        large_files += records
            .iter()
            .filter(|r| matches!(r.payload, Payload::Large { .. }))
            .count();
        total_files += records.len();
        by_root.insert(root.id, records);
    }
    let manifest = SnapshotManifest {
        snapshot_id: snapshot_id.clone(),
        parent: parent.clone(),
        op_id: op_id.clone(),
        timestamp: Utc::now(),
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
        println!(
            "{id}\t{created}\t{kind}\t{} -> {}\t{}",
            before.unwrap_or_default(),
            after.unwrap_or_default(),
            message.unwrap_or_default()
        );
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
    }
    Ok(())
}

fn large_cmd(paths: &Paths, command: LargeCommand) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    match command {
        LargeCommand::List => {
            let mut stmt = conn.prepare("select oid, size, chunk_count, manifest_key from large_objects order by rowid desc")?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, usize>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?;
            for row in rows {
                let (oid, size, chunks, key) = row?;
                println!("{oid}\t{size}\t{chunks}\t{key}");
            }
        }
        LargeCommand::Stat => {
            let count: i64 =
                conn.query_row("select count(*) from large_objects", [], |r| r.get(0))?;
            let bytes: Option<u64> =
                conn.query_row("select sum(size) from large_objects", [], |r| r.get(0))?;
            let chunks: i64 = conn.query_row("select count(*) from chunks", [], |r| r.get(0))?;
            println!("large_objects {count}");
            println!("logical_bytes {}", bytes.unwrap_or(0));
            println!("chunks {chunks}");
        }
        LargeCommand::Verify => fsck(paths)?,
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
    remote.put("metadata/export.json", &export_json)?;
    remote.put("config.toml", toml::to_string_pretty(&config)?.as_bytes())?;
    remote.put(
        "host.toml",
        toml::to_string_pretty(&config.host)?.as_bytes(),
    )?;
    if let Some(current) = export.refs.get("current") {
        remote.put("hosts/current", current.as_bytes())?;
    }

    let mut uploaded = 3usize;
    for key in local_object_keys(&export) {
        let local = paths.home.join(&key);
        if local.exists() {
            remote.put(
                &key,
                &fs::read(&local).with_context(|| format!("read {}", local.display()))?,
            )?;
            uploaded += 1;
        }
    }
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
            println!("stopped daemon pid {pid}");
        }
        DaemonCommand::Status => {
            if let Some(pid) = read_pid(&paths.daemon_pid)? {
                if pid_alive(pid) {
                    println!("running pid {pid}");
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

fn fsck(paths: &Paths) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let mut missing = 0usize;
    let mut stmt = conn.prepare("select oid, object_key from blobs")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (oid, key) = row?;
        if !paths.home.join(&key).exists() {
            missing += 1;
            eprintln!("missing blob {oid} {key}");
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

fn apply_restore_plan(paths: &Paths, plan: &RestorePlan) -> Result<()> {
    for record in &plan.files {
        let dest = plan.to.join(&record.root_id).join(&record.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        match &record.payload {
            Payload::Blob { object_key, .. } => {
                copy_atomic(&paths.home.join(object_key), &dest)?;
            }
            Payload::Large { manifest_key, .. } => {
                let manifest: LargeManifest =
                    serde_json::from_slice(&fs::read(paths.home.join(manifest_key))?)?;
                let tmp = dest.with_extension("mjtmp");
                let mut out = File::create(&tmp)?;
                for chunk in manifest.chunks {
                    let mut input = File::open(paths.home.join(chunk.object_key))?;
                    std::io::copy(&mut input, &mut out)?;
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
                store_large_file(paths, entry.path(), config.large.chunk_size)?;
            Payload::Large {
                oid,
                manifest_key,
                chunk_count,
            }
        } else {
            let bytes = stable_read(entry.path())?;
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

fn store_large_file(
    paths: &Paths,
    path: &Path,
    chunk_size: usize,
) -> Result<(String, String, usize)> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut chunks = Vec::new();
    let mut offset = 0u64;
    let mut index = 0usize;
    loop {
        let mut buf = vec![0u8; chunk_size];
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        buf.truncate(read);
        hasher.update(&buf);
        let chunk_oid = blake3_hex(&buf);
        let object_key = store_bytes(paths, &paths.large_chunks, &chunk_oid, &buf)?;
        chunks.push(LargeChunk {
            index,
            offset,
            len: read as u64,
            oid: chunk_oid.clone(),
            object_key: object_key.clone(),
        });
        let conn = open_db(paths)?;
        conn.execute(
            "insert or ignore into chunks(oid, size, object_key) values (?1, ?2, ?3)",
            params![chunk_oid, read as u64, object_key],
        )?;
        offset += read as u64;
        index += 1;
    }
    let oid = hasher.finalize().to_hex().to_string();
    let manifest = LargeManifest {
        version: 1,
        oid: oid.clone(),
        size: offset,
        chunk_size,
        chunks,
    };
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let manifest_oid = blake3_hex(&manifest_json);
    let manifest_key = store_bytes(paths, &paths.large_manifests, &manifest_oid, &manifest_json)?;
    let conn = open_db(paths)?;
    conn.execute(
        "insert or ignore into large_objects(oid, size, chunk_count, manifest_key) values (?1, ?2, ?3, ?4)",
        params![oid, offset, manifest.chunks.len(), manifest_key],
    )?;
    Ok((oid, manifest_key, manifest.chunks.len()))
}

fn create_layout(paths: &Paths) -> Result<()> {
    fs::create_dir_all(paths.db.parent().unwrap())?;
    fs::create_dir_all(&paths.objects)?;
    fs::create_dir_all(&paths.large_chunks)?;
    fs::create_dir_all(&paths.large_manifests)?;
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
        create table if not exists large_objects(oid text primary key, size integer not null, chunk_count integer not null, manifest_key text not null);
        create table if not exists chunks(oid text primary key, size integer not null, object_key text not null);
        ",
    )?;
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
                chunk_size: config.large.chunk_size,
                always: config.large.always.clone(),
                never: config.large.never.clone(),
            },
        },
        roots: roots(conn)?,
        snapshots,
        operations,
        refs,
        blobs: query_blobs(conn)?,
        large_objects: query_large_objects(conn)?,
        chunks: query_chunks(conn)?,
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
            "insert or replace into blobs(oid, size, object_key) values (?1, ?2, ?3)",
            params![blob.oid, blob.size, blob.object_key],
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
    Ok(())
}

fn query_blobs(conn: &Connection) -> Result<Vec<BlobExport>> {
    let mut stmt = conn.prepare("select oid, size, object_key from blobs order by oid")?;
    let rows = stmt.query_map([], |row| {
        Ok(BlobExport {
            oid: row.get(0)?,
            size: row.get(1)?,
            object_key: row.get(2)?,
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

fn local_object_keys(export: &MetadataExport) -> Vec<String> {
    let mut keys = Vec::new();
    for snapshot in &export.snapshots {
        keys.push(snapshot.manifest_key.clone());
    }
    for blob in &export.blobs {
        keys.push(blob.object_key.clone());
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

fn looks_binary(path: &Path) -> Result<bool> {
    let mut f = File::open(path)?;
    let mut buf = [0u8; 8192];
    let n = f.read(&mut buf)?;
    Ok(buf[..n].contains(&0))
}

fn stable_read(path: &Path) -> Result<Vec<u8>> {
    let before = fs::metadata(path)?;
    let bytes = fs::read(path)?;
    let after = fs::metadata(path)?;
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        bail!("file changed while reading: {}", path.display());
    }
    Ok(bytes)
}

fn store_bytes(paths: &Paths, base: &Path, oid: &str, bytes: &[u8]) -> Result<String> {
    let (a, b) = oid.split_at(2);
    let dir = base.join(a);
    fs::create_dir_all(&dir)?;
    let path = dir.join(b);
    if !path.exists() {
        let tmp = path.with_extension("tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        fs::rename(tmp, &path)?;
    }
    let rel = path.strip_prefix(&paths.home).unwrap_or(&path);
    Ok(path_to_slash(rel))
}

fn copy_atomic(src: &Path, dest: &Path) -> Result<()> {
    let tmp = dest.with_extension("mjtmp");
    fs::copy(src, &tmp)?;
    File::open(&tmp)?.sync_all()?;
    fs::rename(tmp, dest)?;
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
}

impl S3Remote {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let remote_key = self.remote_key(key);
        let date = http_date();
        let path = format!("/{}/{}", self.bucket, remote_key);
        let auth = self.auth("PUT", "", "application/octet-stream", &date, &path)?;
        let url = self.object_url(&remote_key);
        let response = self
            .client
            .put(url)
            .header(DATE, date)
            .header(CONTENT_TYPE, "application/octet-stream")
            .header(AUTHORIZATION, auth)
            .body(bytes.to_vec())
            .send()?;
        if !response.status().is_success() {
            bail!("s3 put failed for {key}: HTTP {}", response.status());
        }
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let remote_key = self.remote_key(key);
        let date = http_date();
        let path = format!("/{}/{}", self.bucket, remote_key);
        let auth = self.auth("GET", "", "", &date, &path)?;
        let response = self
            .client
            .get(self.object_url(&remote_key))
            .header(DATE, date)
            .header(AUTHORIZATION, auth)
            .send()?;
        if !response.status().is_success() {
            bail!("s3 get failed for {key}: HTTP {}", response.status());
        }
        Ok(response.bytes()?.to_vec())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let remote_key = self.remote_key(key);
        let date = http_date();
        let path = format!("/{}/{}", self.bucket, remote_key);
        let auth = self.auth("HEAD", "", "", &date, &path)?;
        let response = self
            .client
            .head(self.object_url(&remote_key))
            .header(DATE, date)
            .header(AUTHORIZATION, auth)
            .send()?;
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
        let date = http_date();
        let resource = format!("/{}/", self.bucket);
        let auth = self.auth("GET", "", "", &date, &resource)?;
        let url = format!(
            "{}/{}/?prefix={}",
            self.endpoint.trim_end_matches('/'),
            self.bucket,
            remote_prefix
        );
        let response = self
            .client
            .get(url)
            .header(DATE, date)
            .header(AUTHORIZATION, auth)
            .send()?;
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

    fn auth(
        &self,
        method: &str,
        md5: &str,
        content_type: &str,
        date: &str,
        resource: &str,
    ) -> Result<String> {
        let canonical = format!("{method}\n{md5}\n{content_type}\n{date}\n{resource}");
        let mut mac = Hmac::<Sha1>::new_from_slice(self.secret_key.as_bytes())?;
        mac.update(canonical.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        Ok(format!("AWS {}:{}", self.access_key, signature))
    }

    fn object_url(&self, remote_key: &str) -> String {
        format!(
            "{}/{}/{}",
            self.endpoint.trim_end_matches('/'),
            self.bucket,
            remote_key
        )
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
