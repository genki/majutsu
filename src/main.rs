use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
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
}

#[derive(Args)]
struct RootAddArgs {
    id: String,
    path: PathBuf,
    #[arg(long)]
    name: Option<String>,
    #[arg(long = "exclude")]
    exclude: Vec<String>,
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
    exclude: Vec<String>,
    follow_symlinks: bool,
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
        if !root.path.exists() {
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
        if is_ignored(&ignore, &rel, entry.file_type().is_dir()) {
            if entry.file_type().is_dir() {
                continue;
            }
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

fn roots(conn: &Connection) -> Result<Vec<RootConfig>> {
    let mut stmt = conn.prepare("select data_json from roots order by id")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(serde_json::from_str(&row?)?);
    }
    Ok(out)
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
