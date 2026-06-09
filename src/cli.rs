use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

const CLI_LONG_ABOUT: &str = r#"majutsu snapshots multiple directories on a development host so local data loss can be recovered.

majutsu is not a Git- or jujutsu-compatible VCS. It records file state under configured roots in its own state directory. When adding a Git working tree as a root, normally exclude Git internals with `--exclude '**/.git/**'`.

Basic flow:
  mj init
  mj root add notes ~/notes --exclude '**/.git/**'
  mj snapshot --message 'first snapshot'
  mj log
  mj restore plan --root notes --to /tmp/majutsu-restore
  mj restore apply --root notes --to /tmp/majutsu-restore

Remote recovery flow:
  mj init --remote file:///mnt/backup/majutsu
  mj sync
  mj remote fsck
  mj clone --remote file:///mnt/backup/majutsu --home /tmp/recovered-majutsu

State home is resolved in this order: `--home`, `MAJUTSU_HOME`, XDG config, then `$HOME/.majutsu`."#;

#[derive(Parser)]
#[command(
    name = "mj",
    version,
    about = "Host-level multi-root snapshot history tool",
    long_about = CLI_LONG_ABOUT,
    after_help = "See README.md and docs/OPERATIONS.md for detailed usage."
)]
pub(crate) struct Cli {
    #[arg(
        long,
        global = true,
        value_name = "DIR",
        help = "Use a specific majutsu state directory",
        long_help = "Use a specific majutsu state directory containing `config.toml`, the SQLite database, local objects, and the operation log. If omitted, majutsu checks MAJUTSU_HOME, XDG config, and $HOME/.majutsu in that order."
    )]
    pub(crate) home: Option<PathBuf>,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    #[command(about = "Initialize the majutsu state directory")]
    Init(InitArgs),
    #[command(about = "Add, update, list, or pause snapshot roots")]
    Root {
        #[command(subcommand)]
        command: RootCommand,
    },
    #[command(about = "Create, list, switch, or delete logical history branches")]
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
    },
    #[command(about = "Snapshot the current state of configured roots")]
    Snapshot(SnapshotArgs),
    #[command(about = "Show roots, current snapshot, queues, and daemon state")]
    Status,
    #[command(about = "Inspect state home paths, refs, branches, and metadata")]
    State(StateArgs),
    #[command(about = "Show snapshot history")]
    Log(LogArgs),
    #[command(about = "Inspect or restore operation-log entries")]
    Op {
        #[command(subcommand)]
        command: OpCommand,
    },
    #[command(about = "Show differences between snapshots or points in time")]
    Diff(DiffArgs),
    #[command(about = "Plan, apply, prepare, or resume a restore")]
    Restore(RestoreTopArgs),
    #[command(about = "Mount a read-only restore view")]
    Mount(MountArgs),
    #[command(about = "Unmount a restore view")]
    Unmount(UnmountArgs),
    #[command(about = "Hydrate large objects in a materialized restore view")]
    Hydrate(HydrateArgs),
    #[command(about = "List, verify, and pin large objects")]
    Large {
        #[command(subcommand)]
        command: LargeCommand,
    },
    #[command(about = "Sync metadata and objects to the configured remote")]
    Sync {
        #[command(subcommand)]
        command: Option<SyncCommand>,
    },
    #[command(about = "Check remote reachability, integrity, and host timelines")]
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
    #[command(about = "Generate, inspect, or apply S3/GCS lifecycle policy")]
    Lifecycle {
        #[command(subcommand)]
        command: LifecycleCommand,
    },
    #[command(about = "Bootstrap an empty state directory from remote metadata")]
    Clone(CloneArgs),
    #[command(about = "Watch the filesystem and snapshot detected changes")]
    Watch(WatchArgs),
    #[command(about = "Start, stop, inspect, or export metrics for the watch daemon")]
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    #[command(about = "Export, import, or rotate the encryption master key")]
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
    #[command(about = "Pack or compact normal blob objects")]
    Pack(PackArgs),
    #[command(about = "Prune old history according to retention settings")]
    Prune(PruneArgs),
    #[command(about = "Remove unreferenced local loose objects")]
    Gc,
    #[command(about = "Check local metadata, objects, queues, and refs")]
    Fsck,
}

#[derive(Args)]
pub(crate) struct InitArgs {
    #[arg(
        long,
        value_name = "URL",
        help = "Configure the remote URL used by `mj sync`",
        long_help = "Configure a remote URL such as file:// or s3://. `mj sync` uploads metadata and objects to this remote, and `mj clone` can bootstrap recovery from it."
    )]
    pub(crate) remote: Option<String>,
    #[arg(long, value_name = "NAME", help = "Set the display name for this host")]
    pub(crate) host_name: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Enable age encryption and create a master key"
    )]
    pub(crate) encrypt: bool,
}

#[derive(Subcommand)]
pub(crate) enum RootCommand {
    #[command(about = "Add a new snapshot root")]
    Add(RootAddArgs),
    #[command(about = "Update an existing root")]
    Set(RootSetArgs),
    #[command(about = "List configured roots")]
    List,
    #[command(about = "Remove a root from the configuration")]
    Remove { id: String },
    #[command(about = "Temporarily pause snapshots for a root")]
    Pause { id: String },
    #[command(about = "Resume a paused root")]
    Resume { id: String },
    #[command(about = "Record a root as deleted")]
    MarkDeleted { id: String },
}

#[derive(Subcommand)]
pub(crate) enum BranchCommand {
    #[command(about = "List logical history branches")]
    List,
    #[command(about = "Show the active branch")]
    Current,
    #[command(about = "Create a branch at the current, specified, or time-selected snapshot")]
    Create(BranchCreateArgs),
    #[command(about = "Switch the active branch and optionally restore its files")]
    Switch(BranchSwitchArgs),
    #[command(about = "Move an existing branch head to another snapshot")]
    SetHead(BranchSetHeadArgs),
    #[command(about = "Delete a branch ref")]
    Delete(BranchDeleteArgs),
    #[command(about = "Rename a branch ref")]
    Rename(BranchRenameArgs),
}

#[derive(Args)]
pub(crate) struct BranchCreateArgs {
    #[arg(help = "Branch name")]
    pub(crate) name: String,
    #[arg(
        long,
        alias = "from",
        value_name = "SNAPSHOT",
        help = "Create the branch at this snapshot"
    )]
    pub(crate) snapshot: Option<String>,
    #[arg(
        long,
        value_name = "TIME",
        help = "Create the branch at the latest snapshot at or before this time"
    )]
    pub(crate) at: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Switch to the new branch after creating it"
    )]
    pub(crate) switch: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Restore files from the branch head after switching"
    )]
    pub(crate) restore: bool,
    #[arg(
        long,
        value_name = "DIR",
        help = "Restore into this directory instead of configured roots"
    )]
    pub(crate) to: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = false,
        help = "Move an existing branch or allow destructive restore"
    )]
    pub(crate) force: bool,
}

#[derive(Args)]
pub(crate) struct BranchSwitchArgs {
    #[arg(help = "Branch name")]
    pub(crate) name: String,
    #[arg(
        long,
        default_value_t = false,
        help = "Restore configured roots to the branch head"
    )]
    pub(crate) restore: bool,
    #[arg(
        long,
        value_name = "DIR",
        help = "Restore into this directory instead of configured roots"
    )]
    pub(crate) to: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = false,
        help = "Allow overwriting/deleting during restore"
    )]
    pub(crate) force: bool,
}

#[derive(Args)]
pub(crate) struct BranchSetHeadArgs {
    #[arg(help = "Branch name")]
    pub(crate) name: String,
    #[arg(
        long,
        alias = "to",
        value_name = "SNAPSHOT",
        help = "Move the branch to this snapshot"
    )]
    pub(crate) snapshot: Option<String>,
    #[arg(
        long,
        value_name = "TIME",
        help = "Move the branch to the latest snapshot at or before this time"
    )]
    pub(crate) at: Option<String>,
}

#[derive(Args)]
pub(crate) struct BranchDeleteArgs {
    #[arg(help = "Branch name")]
    pub(crate) name: String,
    #[arg(
        long,
        default_value_t = false,
        help = "Allow deleting the active branch"
    )]
    pub(crate) force: bool,
}

#[derive(Args)]
pub(crate) struct BranchRenameArgs {
    #[arg(help = "Old branch name")]
    pub(crate) old: String,
    #[arg(help = "New branch name")]
    pub(crate) new: String,
    #[arg(
        long,
        default_value_t = false,
        help = "Overwrite an existing destination branch"
    )]
    pub(crate) force: bool,
}

#[derive(Args)]
pub(crate) struct StateArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Emit machine-readable JSON instead of the terminal summary"
    )]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct RootAddArgs {
    #[arg(help = "Stable root ID used by restore and filters")]
    pub(crate) id: String,
    #[arg(help = "Directory to snapshot")]
    pub(crate) path: PathBuf,
    #[arg(long, help = "Human-readable root name")]
    pub(crate) name: Option<String>,
    #[arg(
        long = "exclude",
        value_name = "GLOB",
        help = "Exclude a glob from snapshots. Use `**/.git/**` for Git working trees"
    )]
    pub(crate) exclude: Vec<String>,
    #[arg(
        long = "preset",
        value_name = "NAME",
        help = "Apply exclude presets such as git-working-tree, rust, or node"
    )]
    pub(crate) presets: Vec<String>,
    #[arg(
        long = "include",
        value_name = "GLOB",
        help = "Include a glob in snapshots"
    )]
    pub(crate) include: Vec<String>,
    #[arg(long, default_value_t = false, help = "Follow symlink targets")]
    pub(crate) follow_symlinks: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Require the root path to be a mountpoint"
    )]
    pub(crate) require_mount: bool,
    #[arg(
        long,
        default_value = "default",
        value_name = "MODE",
        help = "Set the snapshot mode",
        long_help = "Set the snapshot mode. Use default for normal roots, strict when missing or unstable roots should fail the snapshot, and transactional when pre/post hooks create a consistent point-in-time view."
    )]
    pub(crate) snapshot_mode: String,
    #[arg(
        long,
        value_name = "COMMAND",
        help = "Command to run before a transactional snapshot"
    )]
    pub(crate) pre_snapshot: Option<String>,
    #[arg(
        long,
        value_name = "COMMAND",
        help = "Command to run after a transactional snapshot"
    )]
    pub(crate) post_snapshot: Option<String>,
    #[arg(
        long,
        value_name = "DIR",
        help = "Read snapshot data from this directory instead of the root path"
    )]
    pub(crate) snapshot_source: Option<PathBuf>,
    #[arg(
        long,
        value_name = "NAME",
        help = "Application-specific snapshot plugin name"
    )]
    pub(crate) application_plugin: Option<String>,
    #[arg(
        long,
        value_name = "BYTES",
        help = "Minimum size treated as a large object"
    )]
    pub(crate) large_min_size: Option<u64>,
    #[arg(
        long,
        value_name = "BYTES",
        help = "Minimum binary-file size treated as a large object"
    )]
    pub(crate) large_binary_min_size: Option<u64>,
    #[arg(long, value_name = "BYTES", help = "Large object chunk size")]
    pub(crate) large_chunk_size: Option<usize>,
    #[arg(
        long,
        value_name = "MODE",
        help = "Large object chunking mode: fixed or fastcdc"
    )]
    pub(crate) large_chunking: Option<String>,
    #[arg(
        long = "large-always",
        value_name = "GLOB",
        help = "Always treat matching files as large objects"
    )]
    pub(crate) large_always: Vec<String>,
    #[arg(
        long = "large-never",
        value_name = "GLOB",
        help = "Never treat matching files as large objects"
    )]
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
    #[arg(
        long = "preset",
        value_name = "NAME",
        help = "Apply exclude presets such as git-working-tree, rust, or node"
    )]
    pub(crate) presets: Vec<String>,
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
    Fsck {
        #[arg(
            long,
            help = "Run full payload verification. Default remote fsck is quick metadata/existence verification."
        )]
        deep: bool,
    },
    Capabilities,
    Hosts,
    Host {
        id: String,
        #[arg(long)]
        snapshots: bool,
        #[arg(long, alias = "ops")]
        operations: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum OpCommand {
    #[command(about = "List recent operations")]
    Log(LogArgs),
    #[command(about = "Show operation metadata and optionally file changes")]
    Show(OpShowArgs),
    #[command(about = "Show file changes caused by one operation")]
    Diff(OpDiffArgs),
    #[command(about = "Move current to the snapshot referenced by an operation")]
    Restore { op_id: String },
}

#[derive(Args)]
pub(crate) struct OpShowArgs {
    #[arg(help = "Operation id from `mj op log`")]
    pub(crate) op_id: String,
    #[arg(
        long,
        default_value_t = false,
        help = "Show file changes for this operation"
    )]
    pub(crate) files: bool,
    #[arg(long, help = "Limit --files output to one root id")]
    pub(crate) root: Option<String>,
}

#[derive(Args)]
pub(crate) struct OpDiffArgs {
    #[arg(help = "Operation id from `mj op log`")]
    pub(crate) op_id: String,
    #[arg(long, help = "Limit output to one root id")]
    pub(crate) root: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum LifecycleCommand {
    Policy {
        #[arg(long, default_value = "gcs")]
        provider: String,
    },
    Status,
    Apply {
        #[arg(long, default_value = "s3")]
        provider: String,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        dry_run: bool,
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
        debounce_ms: Option<u64>,
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
    Metrics,
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
    #[arg(
        long,
        default_value_t = false,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set
    )]
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
