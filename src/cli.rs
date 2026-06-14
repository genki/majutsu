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
    version = env!("MAJUTSU_VERSION"),
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
    Status(StatusArgs),
    #[command(about = "Inspect state home paths, refs, branches, and metadata")]
    State(StateArgs),
    #[command(about = "Show recent managed file changes")]
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
    #[command(about = "Inspect or prune synced local payload cache")]
    Cache {
        #[command(subcommand)]
        command: CacheCommand,
    },
    #[command(about = "Inspect or compact the local filesystem event journal")]
    Event {
        #[command(subcommand)]
        command: EventCommand,
    },
    #[command(about = "Sync metadata and objects to the configured remote")]
    Sync(SyncArgs),
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
    Fsck(FsckArgs),
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
pub(crate) struct StatusArgs {
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "pager",
        help = "Print directly without using a pager even when output is taller than the terminal"
    )]
    pub(crate) no_pager: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Force pager output even when stdout is not a terminal or output fits on screen"
    )]
    pub(crate) pager: bool,
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
    #[arg(
        long,
        value_name = "BYTES",
        help = "Minimum non-large file size stored as chunked blobs"
    )]
    pub(crate) large_chunked_min_size: Option<u64>,
    #[arg(
        long,
        value_name = "BYTES",
        help = "Chunk size for non-large files stored as chunked blobs"
    )]
    pub(crate) large_chunked_chunk_size: Option<usize>,
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
    pub(crate) large_chunked_min_size: Option<u64>,
    #[arg(long)]
    pub(crate) large_chunked_chunk_size: Option<usize>,
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
    #[arg(
        long,
        default_value_t = false,
        help = "Show internal operation records instead of managed file changes"
    )]
    pub(crate) operations: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Show every changed file instead of folding large change sets"
    )]
    pub(crate) full: bool,
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
    #[command(about = "Build a restore plan without changing files")]
    Plan(RestoreArgs),
    #[command(about = "Apply a restore plan to the configured root paths or --to directory")]
    Apply(RestoreArgs),
    #[command(about = "Prepare missing remote objects and archive restores before apply")]
    Prepare(RestoreArgs),
    #[command(about = "Resume a prepared restore job after missing objects become available")]
    Resume {
        #[arg(help = "Restore job id from `mj restore prepare`")]
        job_id: String,
    },
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
    #[command(about = "List large objects known to majutsu")]
    List,
    #[command(about = "Show large-object counts, chunk counts, and logical sizes")]
    Stat,
    #[command(about = "Verify large-object manifests, chunks, and pins")]
    Verify,
    #[command(about = "Pin large objects so retention does not prune them")]
    Pin(LargePinArgs),
    #[command(about = "Remove large-object pins, optionally by age")]
    Unpin(LargeUnpinArgs),
}

#[derive(Subcommand)]
pub(crate) enum CacheCommand {
    #[command(about = "Show synced local payload cache that can be evicted")]
    Stat(CachePruneArgs),
    #[command(about = "Remove synced local payload cache that can be hydrated from remote")]
    Prune(CachePruneArgs),
}

#[derive(Subcommand)]
pub(crate) enum EventCommand {
    #[command(about = "Show local filesystem event journal retention state")]
    Stat,
    #[command(about = "Remove processed event journal records older than the latest snapshot")]
    Compact {
        #[arg(
            long,
            default_value_t = false,
            help = "Report removable event records without deleting files"
        )]
        dry_run: bool,
    },
}

#[derive(Args)]
pub(crate) struct CachePruneArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Report removable payload cache without deleting files"
    )]
    pub(crate) dry_run: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Also prune synced local metadata cache such as tree manifests"
    )]
    pub(crate) metadata: bool,
}

#[derive(Subcommand)]
pub(crate) enum SyncCommand {
    #[command(about = "Show sync status; quick by default, use --deep for object availability")]
    Status(SyncStatusArgs),
}

#[derive(Args)]
pub(crate) struct SyncStatusArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Check every referenced remote object instead of only refs and queues"
    )]
    pub(crate) deep: bool,
    #[arg(
        long,
        value_name = "N",
        help = "Limit deep object availability checks to the first N local objects"
    )]
    pub(crate) sample: Option<usize>,
    #[arg(
        long,
        value_name = "SECONDS",
        help = "Stop deep object availability checks after this many seconds"
    )]
    pub(crate) timeout_secs: Option<u64>,
    #[arg(
        long,
        default_value_t = false,
        help = "Print deep object availability check progress to stderr"
    )]
    pub(crate) progress: bool,
}

#[derive(Args)]
pub(crate) struct SyncArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Wait until the configured remote catches up with the local current snapshot"
    )]
    pub(crate) wait: bool,
    #[arg(
        long,
        default_value_t = 300,
        value_name = "SECONDS",
        help = "Maximum time to wait with --wait"
    )]
    pub(crate) timeout_secs: u64,
    #[command(subcommand)]
    pub(crate) command: Option<SyncCommand>,
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
    #[command(about = "Check that the configured remote is reachable and supports required APIs")]
    Check,
    #[command(
        about = "Check remote metadata integrity; use --objects or --deep for heavier checks"
    )]
    Fsck {
        #[arg(
            long,
            help = "Check every referenced remote object without payload decoding. Default remote fsck checks only critical metadata."
        )]
        objects: bool,
        #[arg(
            long,
            default_value_t = 16,
            value_name = "N",
            help = "Parallel remote probes for --objects"
        )]
        parallelism: usize,
        #[arg(
            long,
            value_name = "N",
            help = "Limit --objects probes or --deep payload verification to the first N objects"
        )]
        sample: Option<usize>,
        #[arg(
            long,
            value_name = "SECONDS",
            help = "Stop --objects probes or --deep payload verification after this many seconds"
        )]
        timeout_secs: Option<u64>,
        #[arg(
            long,
            help = "Run full payload verification. Default remote fsck checks only critical metadata."
        )]
        deep: bool,
        #[arg(
            long,
            requires = "deep",
            help = "With --deep, verify payload decode/hash for this host without full metadata graph audit"
        )]
        payload_only: bool,
    },
    #[command(about = "Re-upload referenced local objects that are missing from the remote")]
    Repair {
        #[arg(
            long,
            default_value_t = false,
            help = "Report repair candidates without uploading"
        )]
        dry_run: bool,
        #[arg(
            long,
            default_value_t = 16,
            value_name = "N",
            help = "Parallel remote probes before repair"
        )]
        parallelism: usize,
        #[arg(
            long,
            value_name = "N",
            help = "Inspect only the first N referenced objects"
        )]
        sample: Option<usize>,
        #[arg(
            long,
            value_name = "SECONDS",
            help = "Stop scanning after this many seconds and repair only discovered missing objects"
        )]
        timeout_secs: Option<u64>,
    },
    #[command(about = "Show the remote backend capability matrix")]
    Capabilities,
    #[command(about = "List hosts published in the shared remote backend")]
    Hosts,
    #[command(about = "Inspect one remote host timeline")]
    Host {
        #[arg(help = "Remote host id or unambiguous host name")]
        id: String,
        #[arg(long)]
        snapshots: bool,
        #[arg(long, alias = "ops")]
        operations: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum OpCommand {
    #[command(about = "List recent internal operations")]
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
    #[command(about = "Render the lifecycle policy for a provider")]
    Policy {
        #[arg(long, default_value = "gcs")]
        provider: String,
    },
    #[command(about = "Show lifecycle policy state and configured provider behavior")]
    Status,
    #[command(about = "Apply or dry-run lifecycle policy configuration")]
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
    #[arg(
        long,
        help = "Quiet time after the last filesystem event before flushing a watch snapshot"
    )]
    pub(crate) debounce_ms: Option<u64>,
    #[arg(
        long,
        help = "Additional quiet time folded into watch snapshot buffering"
    )]
    pub(crate) settle_ms: Option<u64>,
    #[arg(
        long,
        help = "Maximum time to buffer filesystem events before forcing a watch snapshot"
    )]
    pub(crate) buffer_max_ms: Option<u64>,
    #[arg(
        long,
        help = "Maximum number of buffered filesystem events before forcing a watch snapshot"
    )]
    pub(crate) buffer_max_events: Option<usize>,
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
    pub(crate) buffer_max_ms: u64,
    pub(crate) buffer_max_events: usize,
    pub(crate) periodic_rescan_secs: u64,
    pub(crate) backend: String,
    pub(crate) once: bool,
}

#[derive(Subcommand)]
pub(crate) enum DaemonCommand {
    #[command(about = "Start the background watch daemon")]
    Start {
        #[arg(long)]
        backend: Option<String>,
        #[arg(long)]
        mode: Option<String>,
        #[arg(long)]
        interval_secs: Option<u64>,
        #[arg(
            long,
            help = "Quiet time after the last filesystem event before flushing a watch snapshot"
        )]
        debounce_ms: Option<u64>,
        #[arg(
            long,
            help = "Additional quiet time folded into watch snapshot buffering"
        )]
        settle_ms: Option<u64>,
        #[arg(
            long,
            help = "Maximum time to buffer filesystem events before forcing a watch snapshot"
        )]
        buffer_max_ms: Option<u64>,
        #[arg(
            long,
            help = "Maximum number of buffered filesystem events before forcing a watch snapshot"
        )]
        buffer_max_events: Option<usize>,
        #[arg(long)]
        periodic_rescan_secs: Option<u64>,
    },
    #[command(about = "Restart the watch daemon, cleaning stale runtime files if needed")]
    Restart {
        #[arg(long)]
        backend: Option<String>,
        #[arg(long)]
        mode: Option<String>,
    },
    #[command(about = "Diagnose daemon health and show recovery guidance")]
    Doctor,
    #[command(about = "Render a user service definition for systemd or launchd")]
    Service {
        #[arg(long, default_value = "systemd")]
        provider: String,
    },
    #[command(about = "Stop the background watch daemon")]
    Stop,
    #[command(about = "Show daemon pid, IPC, queue, and journal health")]
    Status,
    #[command(about = "Export daemon health metrics in text form")]
    Metrics,
}

#[derive(Args, Clone, Default)]
pub(crate) struct FsckArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Run lightweight metadata, ref, queue, and current-state checks"
    )]
    pub(crate) quick: bool,
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "quick",
        help = "Run full object and historical manifest verification. This is the default unless --quick is used."
    )]
    pub(crate) deep: bool,
    #[arg(long, default_value_t = false, help = "Show phase progress on stderr")]
    pub(crate) progress: bool,
    #[arg(
        long,
        value_name = "SECONDS",
        help = "Stop after this many seconds and report an incomplete check"
    )]
    pub(crate) timeout_secs: Option<u64>,
    #[arg(
        long,
        value_name = "N",
        help = "In full checks, inspect at most N objects per heavy payload or manifest phase"
    )]
    pub(crate) sample: Option<usize>,
    #[arg(
        long,
        value_name = "TIME",
        help = "In full checks, inspect heavy payload and manifest phases only for snapshots at or after this time"
    )]
    pub(crate) since: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum KeyCommand {
    #[command(about = "Print the current encryption master key in export form")]
    Export,
    #[command(about = "Import an encryption master key into this state home")]
    Import {
        #[arg(help = "64-character hex master key")]
        hex: String,
    },
    #[command(about = "Rotate encrypted local and remote metadata to a new master key")]
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
