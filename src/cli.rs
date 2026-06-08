use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

const CLI_LONG_ABOUT: &str = r#"majutsu は、開発ホストのデータ喪失に備えて複数ディレクトリをまとめて履歴化するスナップショットツールです。

Git や jujutsu 互換の VCS ではなく、指定した root 配下のファイル状態を majutsu 独自の状態ディレクトリへ保存します。Git 管理中のリポジトリを root にする場合は、通常 `--exclude '**/.git/**'` を指定して作業ツリーだけを保護します。

基本手順:
  mj init
  mj root add notes ~/notes --exclude '**/.git/**'
  mj snapshot --message 'first snapshot'
  mj log
  mj restore plan --root notes --to /tmp/majutsu-restore
  mj restore apply --root notes --to /tmp/majutsu-restore

remote を使う場合:
  mj init --remote file:///mnt/backup/majutsu
  mj sync
  mj remote fsck
  mj clone --remote file:///mnt/backup/majutsu --home /tmp/recovered-majutsu

状態ディレクトリは `--home`、`MAJUTSU_HOME`、XDG config、`$HOME/.majutsu` の順で解決されます。"#;

#[derive(Parser)]
#[command(
    name = "mj",
    version,
    about = "ホスト単位で複数rootを履歴化するスナップショットツール",
    long_about = CLI_LONG_ABOUT,
    after_help = "詳細な使い方は README.md と docs/OPERATIONS.md を参照してください。"
)]
pub(crate) struct Cli {
    #[arg(
        long,
        global = true,
        value_name = "DIR",
        help = "majutsu の状態ディレクトリを指定する",
        long_help = "`config.toml`、SQLite DB、ローカルオブジェクト、operation log を置く状態ディレクトリを指定します。未指定時は MAJUTSU_HOME、XDG config、$HOME/.majutsu の順で解決します。"
    )]
    pub(crate) home: Option<PathBuf>,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    #[command(about = "状態ディレクトリを初期化する")]
    Init(InitArgs),
    #[command(about = "スナップショット対象rootを追加・変更・一覧表示する")]
    Root {
        #[command(subcommand)]
        command: RootCommand,
    },
    #[command(about = "登録済みrootの現在状態をスナップショットする")]
    Snapshot(SnapshotArgs),
    #[command(about = "root、current snapshot、queue などの現在状態を表示する")]
    Status,
    #[command(about = "スナップショット履歴を表示する")]
    Log(LogArgs),
    #[command(about = "operation logを表示・復元する")]
    Op {
        #[command(subcommand)]
        command: OpCommand,
    },
    #[command(about = "スナップショット間または指定時点との差分を表示する")]
    Diff(DiffArgs),
    #[command(about = "スナップショットを計画・復元・resumeする")]
    Restore(RestoreTopArgs),
    #[command(about = "復元ビューをディレクトリへmountする")]
    Mount(MountArgs),
    #[command(about = "mountした復元ビューを解除する")]
    Unmount(UnmountArgs),
    #[command(about = "materialized viewのlarge objectを実体化する")]
    Hydrate(HydrateArgs),
    #[command(about = "large objectの一覧・検証・pinを管理する")]
    Large {
        #[command(subcommand)]
        command: LargeCommand,
    },
    #[command(about = "remoteへmetadataとobjectを同期する")]
    Sync {
        #[command(subcommand)]
        command: Option<SyncCommand>,
    },
    #[command(about = "remoteの到達性・整合性・host情報を確認する")]
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
    #[command(about = "S3/GCS向けlifecycle policyを生成・確認する")]
    Lifecycle {
        #[command(subcommand)]
        command: LifecycleCommand,
    },
    #[command(about = "remote metadataから空の状態ディレクトリへ復旧する")]
    Clone(CloneArgs),
    #[command(about = "filesystem watchで変更を検知してsnapshotする")]
    Watch(WatchArgs),
    #[command(about = "watch daemonの起動・停止・状態・metricsを扱う")]
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    #[command(about = "暗号化master keyをexport/import/rotateする")]
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
    #[command(about = "通常blobをpack化またはcompactする")]
    Pack(PackArgs),
    #[command(about = "保持ポリシーに従って古い履歴の削除候補を処理する")]
    Prune(PruneArgs),
    #[command(about = "参照されないlocal loose objectを削除する")]
    Gc,
    #[command(about = "local metadata/object/queue/refの整合性を検査する")]
    Fsck,
}

#[derive(Args)]
pub(crate) struct InitArgs {
    #[arg(
        long,
        value_name = "URL",
        help = "同期先remote URLを設定する",
        long_help = "file://、s3:// などのremote URLを設定します。metadataとobjectは `mj sync` でremoteへ送信され、`mj clone` による復旧に使われます。"
    )]
    pub(crate) remote: Option<String>,
    #[arg(long, value_name = "NAME", help = "このホストの表示名を指定する")]
    pub(crate) host_name: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "age暗号化を有効化してmaster keyを生成する"
    )]
    pub(crate) encrypt: bool,
}

#[derive(Subcommand)]
pub(crate) enum RootCommand {
    #[command(about = "新しいrootを追加する")]
    Add(RootAddArgs),
    #[command(about = "既存rootの設定を変更する")]
    Set(RootSetArgs),
    #[command(about = "登録済みrootを一覧表示する")]
    List,
    #[command(about = "rootを設定から削除する")]
    Remove { id: String },
    #[command(about = "rootのsnapshot対象を一時停止する")]
    Pause { id: String },
    #[command(about = "一時停止中rootを再開する")]
    Resume { id: String },
    #[command(about = "rootを削除済みとして記録する")]
    MarkDeleted { id: String },
}

#[derive(Args)]
pub(crate) struct RootAddArgs {
    #[arg(help = "root ID。restoreやfilterで使う安定した名前")]
    pub(crate) id: String,
    #[arg(help = "snapshot対象のディレクトリ")]
    pub(crate) path: PathBuf,
    #[arg(long, help = "rootの表示名")]
    pub(crate) name: Option<String>,
    #[arg(
        long = "exclude",
        value_name = "GLOB",
        help = "snapshotから除外するglob。Git併用時は `**/.git/**` を推奨"
    )]
    pub(crate) exclude: Vec<String>,
    #[arg(long = "include", value_name = "GLOB", help = "snapshotに含めるglob")]
    pub(crate) include: Vec<String>,
    #[arg(long, default_value_t = false, help = "symlinkの参照先をたどる")]
    pub(crate) follow_symlinks: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "root pathがmountpointであることを要求する"
    )]
    pub(crate) require_mount: bool,
    #[arg(
        long,
        default_value = "default",
        value_name = "MODE",
        help = "snapshot modeを指定する",
        long_help = "snapshot modeを指定します。通常は default、欠落や不安定なrootを厳格に扱う場合は strict、pre/post hookで整合点を作る場合は transactional を使います。"
    )]
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
