use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "mj",
    version,
    about = "Host-level multi-root snapshot history agent"
)]
pub(crate) struct Cli {
    #[arg(long, global = true)]
    pub(crate) home: Option<PathBuf>,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
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
pub(crate) struct InitArgs {
    #[arg(long)]
    pub(crate) remote: Option<String>,
    #[arg(long)]
    pub(crate) host_name: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) encrypt: bool,
}

#[derive(Subcommand)]
pub(crate) enum RootCommand {
    Add(RootAddArgs),
    Set(RootSetArgs),
    List,
    Remove { id: String },
    Pause { id: String },
    Resume { id: String },
    MarkDeleted { id: String },
}

#[derive(Args)]
pub(crate) struct RootAddArgs {
    pub(crate) id: String,
    pub(crate) path: PathBuf,
    #[arg(long)]
    pub(crate) name: Option<String>,
    #[arg(long = "exclude")]
    pub(crate) exclude: Vec<String>,
    #[arg(long = "include")]
    pub(crate) include: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) follow_symlinks: bool,
    #[arg(long, default_value_t = false)]
    pub(crate) require_mount: bool,
    #[arg(long, default_value = "default")]
    pub(crate) snapshot_mode: String,
    #[arg(long)]
    pub(crate) pre_snapshot: Option<String>,
    #[arg(long)]
    pub(crate) post_snapshot: Option<String>,
    #[arg(long)]
    pub(crate) snapshot_source: Option<PathBuf>,
    #[arg(long)]
    pub(crate) application_plugin: Option<String>,
    #[arg(long)]
    pub(crate) large_min_size: Option<u64>,
    #[arg(long)]
    pub(crate) large_binary_min_size: Option<u64>,
    #[arg(long)]
    pub(crate) large_chunk_size: Option<usize>,
    #[arg(long)]
    pub(crate) large_chunking: Option<String>,
    #[arg(long = "large-always")]
    pub(crate) large_always: Vec<String>,
    #[arg(long = "large-never")]
    pub(crate) large_never: Vec<String>,
}

#[derive(Args)]
pub(crate) struct RootSetArgs {
    pub(crate) id: String,
    #[arg(long)]
    pub(crate) path: Option<PathBuf>,
    #[arg(long)]
    pub(crate) name: Option<String>,
    #[arg(long = "include")]
    pub(crate) include: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_include: bool,
    #[arg(long = "exclude")]
    pub(crate) exclude: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_exclude: bool,
    #[arg(long, default_value_t = false)]
    pub(crate) follow_symlinks: bool,
    #[arg(long, default_value_t = false)]
    pub(crate) no_follow_symlinks: bool,
    #[arg(long, default_value_t = false)]
    pub(crate) require_mount: bool,
    #[arg(long, default_value_t = false)]
    pub(crate) no_require_mount: bool,
    #[arg(long)]
    pub(crate) snapshot_mode: Option<String>,
    #[arg(long)]
    pub(crate) pre_snapshot: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_pre_snapshot: bool,
    #[arg(long)]
    pub(crate) post_snapshot: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_post_snapshot: bool,
    #[arg(long)]
    pub(crate) snapshot_source: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_snapshot_source: bool,
    #[arg(long)]
    pub(crate) application_plugin: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_application_plugin: bool,
    #[arg(long)]
    pub(crate) large_min_size: Option<u64>,
    #[arg(long)]
    pub(crate) large_binary_min_size: Option<u64>,
    #[arg(long)]
    pub(crate) large_chunk_size: Option<usize>,
    #[arg(long)]
    pub(crate) large_chunking: Option<String>,
    #[arg(long = "large-always")]
    pub(crate) large_always: Vec<String>,
    #[arg(long = "large-never")]
    pub(crate) large_never: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_large_policy: bool,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_large_always: bool,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_large_never: bool,
}

#[derive(Args)]
pub(crate) struct SnapshotArgs {
    #[arg(long)]
    pub(crate) message: Option<String>,
}

#[derive(Args)]
pub(crate) struct LogArgs {
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: usize,
    #[arg(long)]
    pub(crate) root: Option<String>,
}

#[derive(Args)]
pub(crate) struct DiffArgs {
    pub(crate) from: Option<String>,
    pub(crate) to: Option<String>,
    #[arg(long)]
    pub(crate) at: Option<String>,
    #[arg(long)]
    pub(crate) root: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum RestoreCommand {
    Plan(RestoreArgs),
    Apply(RestoreArgs),
    Prepare(RestoreArgs),
    Resume { job_id: String },
}

#[derive(Args)]
pub(crate) struct RestoreTopArgs {
    #[command(subcommand)]
    pub(crate) command: Option<RestoreCommand>,
    #[command(flatten)]
    pub(crate) args: RestoreArgs,
}

#[derive(Args, Clone)]
pub(crate) struct RestoreArgs {
    #[arg(long)]
    pub(crate) snapshot: Option<String>,
    #[arg(long)]
    pub(crate) at: Option<String>,
    #[arg(long)]
    pub(crate) root: Option<String>,
    #[arg(long)]
    pub(crate) path: Option<PathBuf>,
    #[arg(long)]
    pub(crate) to: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    pub(crate) force: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) check_conflicts: bool,
}

#[derive(Args, Clone)]
pub(crate) struct MountArgs {
    #[arg(long)]
    pub(crate) snapshot: Option<String>,
    #[arg(long)]
    pub(crate) at: Option<String>,
    #[arg(long)]
    pub(crate) root: Option<String>,
    #[arg(long)]
    pub(crate) path: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    pub(crate) hydrate_large: bool,
    #[arg(long, default_value = "materialized")]
    pub(crate) backend: String,
    pub(crate) mountpoint: PathBuf,
}

#[derive(Args)]
pub(crate) struct UnmountArgs {
    pub(crate) mountpoint: PathBuf,
}

#[derive(Args)]
pub(crate) struct HydrateArgs {
    pub(crate) view: PathBuf,
    #[arg(long)]
    pub(crate) root: Option<String>,
    #[arg(long)]
    pub(crate) path: Option<PathBuf>,
}

#[derive(Subcommand)]
pub(crate) enum LargeCommand {
    List,
    Stat,
    Verify,
    Pin(LargePinArgs),
    Unpin(LargeUnpinArgs),
}

#[derive(Subcommand)]
pub(crate) enum SyncCommand {
    Status,
}

#[derive(Args)]
pub(crate) struct LargePinArgs {
    #[arg(long)]
    pub(crate) root: Option<String>,
    #[arg(long)]
    pub(crate) since: Option<String>,
}

#[derive(Args)]
pub(crate) struct LargeUnpinArgs {
    #[arg(long)]
    pub(crate) older_than: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum RemoteCommand {
    Check,
    Fsck,
    Capabilities,
    Hosts,
    Host { id: String },
}

#[derive(Subcommand)]
pub(crate) enum OpCommand {
    Log(LogArgs),
    Show { op_id: String },
    Restore { op_id: String },
}

#[derive(Subcommand)]
pub(crate) enum LifecycleCommand {
    Policy {
        #[arg(long, default_value = "gcs")]
        provider: String,
    },
}

#[derive(Args)]
pub(crate) struct CloneArgs {
    #[arg(long)]
    pub(crate) remote: String,
    #[arg(long)]
    pub(crate) host: Option<String>,
}

#[derive(Args)]
pub(crate) struct WatchArgs {
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) foreground: bool,
    #[arg(long)]
    pub(crate) mode: Option<String>,
    #[arg(long)]
    pub(crate) interval_secs: Option<u64>,
    #[arg(long)]
    pub(crate) debounce_ms: Option<u64>,
    #[arg(long)]
    pub(crate) settle_ms: Option<u64>,
    #[arg(long)]
    pub(crate) periodic_rescan_secs: Option<u64>,
    #[arg(long)]
    pub(crate) backend: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) once: bool,
}

#[derive(Clone)]
pub(crate) struct ResolvedWatchArgs {
    pub(crate) foreground: bool,
    pub(crate) mode: String,
    pub(crate) interval_secs: u64,
    pub(crate) debounce_ms: u64,
    pub(crate) settle_ms: u64,
    pub(crate) periodic_rescan_secs: u64,
    pub(crate) backend: String,
    pub(crate) once: bool,
}

#[derive(Subcommand)]
pub(crate) enum DaemonCommand {
    Start {
        #[arg(long)]
        backend: Option<String>,
        #[arg(long)]
        mode: Option<String>,
        #[arg(long)]
        interval_secs: Option<u64>,
        #[arg(long)]
        settle_ms: Option<u64>,
        #[arg(long)]
        periodic_rescan_secs: Option<u64>,
    },
    Service {
        #[arg(long, default_value = "systemd")]
        provider: String,
    },
    Stop,
    Status,
}

#[derive(Subcommand)]
pub(crate) enum KeyCommand {
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
pub(crate) struct PruneArgs {
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) dry_run: bool,
    #[arg(long, default_value_t = 90)]
    pub(crate) keep_daily: u32,
    #[arg(long, default_value_t = 36)]
    pub(crate) keep_monthly: u32,
}

#[derive(Args)]
pub(crate) struct PackArgs {
    #[arg(long, default_value_t = false)]
    pub(crate) compact: bool,
}
