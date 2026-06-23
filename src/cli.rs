use clap::{Args, Command as ClapCommand, CommandFactory, FromArgMatches, Parser, Subcommand};
use std::env;
use std::path::PathBuf;

const CLI_LONG_ABOUT: &str = r#"majutsu snapshots multiple directories on a development host so local data loss can be recovered.

majutsu is not a Git- or jujutsu-compatible VCS. It records file state under configured roots in its own state directory. New roots use best-practice excludes for VCS internals, dependency directories, build outputs, and caches. Use `--no-default-excludes` only when those generated files must be backed up too.

Basic flow:
  mj init
  mj root add notes ~/notes
  mj snapshot --message 'first snapshot'
  mj log
  mj restore plan --root notes --to /tmp/majutsu-restore
  mj restore apply --root notes --to /tmp/majutsu-restore

Remote recovery flow:
  mj init --remote file:///mnt/backup/majutsu
  mj sync --wait
  mj remote fsck
  mj clone --remote file:///mnt/backup/majutsu --home /tmp/recovered-majutsu

Command groups:
  Setup: init, root
  Daily use: status, health, state, log, diff, snapshot, commit, note, track, untrack
  History: branch, switch, op
  Recovery: restore, restore mount, restore unmount, restore hydrate, mount, unmount, hydrate, clone
  Remote: sync, remote, lifecycle
  Service: watch, daemon
  Security: key
  Storage maintenance: large, cache, pack, prune, gc, fsck
  Advanced/debug: event

State home is resolved in this order: `--home`, `MAJUTSU_HOME`, XDG config, then `$HOME/.majutsu`.
For host-level system protection, use `mj --system ...`; that reads `/etc/majutsu/config.toml` and falls back to `/var/lib/majutsu`."#;

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
    #[arg(
        long,
        global = true,
        default_value_t = false,
        help = "Use the system majutsu instance",
        long_help = "Use the system majutsu instance intended for root-owned host configuration such as /etc and systemd units. If --home is not also provided, majutsu checks /etc/majutsu/config.toml and then falls back to /var/lib/majutsu."
    )]
    pub(crate) system: bool,
    #[command(subcommand)]
    pub(crate) command: Command,
}

pub(crate) fn parse_cli() -> Cli {
    let command = localize_command(Cli::command());
    let matches = command.get_matches();
    Cli::from_arg_matches(&matches).unwrap_or_else(|err| err.exit())
}

fn localize_command(mut command: ClapCommand) -> ClapCommand {
    let locale = CliLocale::detect();
    let text = locale.text();
    command = command
        .about(text.top_about)
        .long_about(text.top_long_about)
        .after_help(text.after_help);
    command = command
        .mut_arg("home", |arg| {
            arg.help(text.home_help).long_help(text.home_long_help)
        })
        .mut_arg("system", |arg| {
            arg.help(text.system_help).long_help(text.system_long_help)
        });
    command
        .mut_subcommand("init", |cmd| cmd.about(text.init_about))
        .mut_subcommand("root", |cmd| cmd.about(text.root_about))
        .mut_subcommand("snapshot", |cmd| cmd.about(text.snapshot_about))
        .mut_subcommand("status", |cmd| cmd.about(text.status_about))
        .mut_subcommand("health", |cmd| cmd.about(text.health_about))
        .mut_subcommand("state", |cmd| cmd.about(text.state_about))
        .mut_subcommand("note", |cmd| cmd.about(text.note_about))
        .mut_subcommand("track", |cmd| cmd.about(text.track_about))
        .mut_subcommand("untrack", |cmd| cmd.about(text.untrack_about))
        .mut_subcommand("log", |cmd| cmd.about(text.log_about))
        .mut_subcommand("diff", |cmd| cmd.about(text.diff_about))
        .mut_subcommand("branch", |cmd| cmd.about(text.branch_about))
        .mut_subcommand("switch", |cmd| cmd.about(text.switch_about))
        .mut_subcommand("op", |cmd| cmd.about(text.op_about))
        .mut_subcommand("restore", |cmd| {
            cmd.about(text.restore_about)
                .mut_arg("snapshot", |arg| arg.help(text.restore_snapshot_help))
                .mut_arg("op", |arg| arg.help(text.restore_op_help))
                .mut_arg("at", |arg| arg.help(text.restore_at_help))
                .mut_arg("ago", |arg| arg.help(text.restore_ago_help))
                .mut_arg("root", |arg| arg.help(text.restore_root_help))
                .mut_arg("path", |arg| arg.help(text.restore_path_help))
                .mut_arg("to", |arg| arg.help(text.restore_to_help))
                .mut_arg("force", |arg| arg.help(text.restore_force_help))
                .mut_arg("check_conflicts", |arg| {
                    arg.help(text.restore_check_conflicts_help)
                })
                .mut_subcommand("plan", |sub| sub.about(text.restore_plan_about))
                .mut_subcommand("apply", |sub| sub.about(text.restore_apply_about))
                .mut_subcommand("prepare", |sub| sub.about(text.restore_prepare_about))
                .mut_subcommand("resume", |sub| sub.about(text.restore_resume_about))
                .mut_subcommand("mount", |sub| sub.about(text.mount_about))
                .mut_subcommand("unmount", |sub| sub.about(text.unmount_about))
                .mut_subcommand("hydrate", |sub| sub.about(text.hydrate_about))
        })
        .mut_subcommand("mount", |cmd| cmd.about(text.mount_about))
        .mut_subcommand("unmount", |cmd| cmd.about(text.unmount_about))
        .mut_subcommand("hydrate", |cmd| cmd.about(text.hydrate_about))
        .mut_subcommand("clone", |cmd| cmd.about(text.clone_about))
        .mut_subcommand("sync", |cmd| cmd.about(text.sync_about))
        .mut_subcommand("remote", |cmd| cmd.about(text.remote_about))
        .mut_subcommand("lifecycle", |cmd| cmd.about(text.lifecycle_about))
        .mut_subcommand("watch", |cmd| cmd.about(text.watch_about))
        .mut_subcommand("daemon", |cmd| cmd.about(text.daemon_about))
        .mut_subcommand("key", |cmd| cmd.about(text.key_about))
        .mut_subcommand("large", |cmd| cmd.about(text.large_about))
        .mut_subcommand("cache", |cmd| cmd.about(text.cache_about))
        .mut_subcommand("pack", |cmd| cmd.about(text.pack_about))
        .mut_subcommand("prune", |cmd| cmd.about(text.prune_about))
        .mut_subcommand("gc", |cmd| cmd.about(text.gc_about))
        .mut_subcommand("fsck", |cmd| cmd.about(text.fsck_about))
        .mut_subcommand("event", |cmd| cmd.about(text.event_about))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CliLocale {
    En,
    Ja,
    Zh,
    Es,
    Fr,
}

impl CliLocale {
    fn detect() -> Self {
        let locale = ["LC_ALL", "LC_MESSAGES", "LANG"]
            .into_iter()
            .find_map(|name| env::var(name).ok().filter(|value| !value.trim().is_empty()))
            .unwrap_or_default()
            .to_ascii_lowercase();
        if locale.starts_with("ja") {
            Self::Ja
        } else if locale.starts_with("zh") {
            Self::Zh
        } else if locale.starts_with("es") {
            Self::Es
        } else if locale.starts_with("fr") {
            Self::Fr
        } else {
            Self::En
        }
    }

    fn text(self) -> &'static CliText {
        match self {
            Self::En => &CLI_TEXT_EN,
            Self::Ja => &CLI_TEXT_JA,
            Self::Zh => &CLI_TEXT_ZH,
            Self::Es => &CLI_TEXT_ES,
            Self::Fr => &CLI_TEXT_FR,
        }
    }
}

struct CliText {
    top_about: &'static str,
    top_long_about: &'static str,
    after_help: &'static str,
    home_help: &'static str,
    home_long_help: &'static str,
    system_help: &'static str,
    system_long_help: &'static str,
    init_about: &'static str,
    root_about: &'static str,
    snapshot_about: &'static str,
    status_about: &'static str,
    health_about: &'static str,
    state_about: &'static str,
    note_about: &'static str,
    track_about: &'static str,
    untrack_about: &'static str,
    log_about: &'static str,
    diff_about: &'static str,
    branch_about: &'static str,
    switch_about: &'static str,
    op_about: &'static str,
    restore_about: &'static str,
    mount_about: &'static str,
    unmount_about: &'static str,
    hydrate_about: &'static str,
    clone_about: &'static str,
    sync_about: &'static str,
    remote_about: &'static str,
    lifecycle_about: &'static str,
    watch_about: &'static str,
    daemon_about: &'static str,
    key_about: &'static str,
    large_about: &'static str,
    cache_about: &'static str,
    pack_about: &'static str,
    prune_about: &'static str,
    gc_about: &'static str,
    fsck_about: &'static str,
    event_about: &'static str,
    restore_plan_about: &'static str,
    restore_apply_about: &'static str,
    restore_prepare_about: &'static str,
    restore_resume_about: &'static str,
    restore_snapshot_help: &'static str,
    restore_op_help: &'static str,
    restore_at_help: &'static str,
    restore_ago_help: &'static str,
    restore_root_help: &'static str,
    restore_path_help: &'static str,
    restore_to_help: &'static str,
    restore_force_help: &'static str,
    restore_check_conflicts_help: &'static str,
}

static CLI_TEXT_EN: CliText = CliText {
    top_about: "Host-level multi-root snapshot history tool",
    top_long_about: CLI_LONG_ABOUT,
    after_help: "See README.md and docs/OPERATIONS.md for detailed usage.",
    home_help: "Use a specific majutsu state directory",
    home_long_help: "Use a specific majutsu state directory containing `config.toml`, the SQLite database, local objects, and the operation log. If omitted, majutsu checks MAJUTSU_HOME, XDG config, and $HOME/.majutsu in that order.",
    system_help: "Use the system majutsu instance",
    system_long_help: "Use the system majutsu instance intended for root-owned host configuration such as /etc and systemd units. If --home is not also provided, majutsu checks /etc/majutsu/config.toml and then falls back to /var/lib/majutsu.",
    init_about: "Initialize the majutsu state directory",
    root_about: "Add, update, list, or pause snapshot roots",
    snapshot_about: "Create a commit-like checkpoint of configured roots",
    status_about: "Show roots, current snapshot, queues, and daemon state",
    health_about: "Report protection health for normal operation",
    state_about: "Show managed file changes against live roots",
    note_about: "Show or edit the note on an operation or snapshot",
    track_about: "Explicitly track paths even when excluded by root rules",
    untrack_about: "Stop tracking paths without deleting working files",
    log_about: "Show recent managed file changes",
    diff_about: "Show differences between snapshots or points in time",
    branch_about: "Create, list, switch, or delete logical history branches",
    switch_about: "Switch the active branch and optionally restore its files",
    op_about: "Inspect or restore jj-style operation-log entries",
    restore_about: "Plan, apply, prepare, or resume a restore",
    mount_about: "Mount a read-only restore view",
    unmount_about: "Unmount a restore view",
    hydrate_about: "Hydrate large objects in a materialized restore view",
    clone_about: "Bootstrap an empty state directory from remote metadata",
    sync_about: "Sync metadata and objects to the configured remote",
    remote_about: "Check remote reachability, integrity, and host timelines",
    lifecycle_about: "Generate, inspect, or apply S3/GCS lifecycle policy",
    watch_about: "Watch the filesystem and snapshot detected changes",
    daemon_about: "Start, stop, inspect, or export metrics for the watch daemon",
    key_about: "Export, import, or rotate the encryption master key",
    large_about: "List, verify, and pin large objects",
    cache_about: "Inspect or prune synced local payload cache",
    pack_about: "Pack or compact normal blob objects",
    prune_about: "Prune old history according to retention settings",
    gc_about: "Remove unreferenced local loose objects",
    fsck_about: "Check local metadata, objects, queues, and refs",
    event_about: "Inspect or compact the local filesystem event journal",
    restore_plan_about: "Build a restore plan without changing files",
    restore_apply_about: "Apply a restore plan to the configured root paths or --to directory",
    restore_prepare_about: "Prepare missing remote objects and archive restores before apply",
    restore_resume_about: "Resume a prepared restore job after missing objects become available",
    restore_snapshot_help: "Restore this snapshot id",
    restore_op_help: "Restore the state produced by this operation id or prefix",
    restore_at_help: "Restore the latest snapshot at or before this time",
    restore_ago_help: "Restore the latest snapshot at or before this relative duration, such as 2h",
    restore_root_help: "Limit restore to one configured root id",
    restore_path_help: "Limit restore to this relative path inside the selected root",
    restore_to_help: "Restore into this directory instead of the configured root paths",
    restore_force_help: "Allow overwrites and deletes during restore",
    restore_check_conflicts_help: "Refuse restore when destination files conflict with the selected snapshot",
};

static CLI_TEXT_JA: CliText = CliText {
    top_about: "ホスト単位のmulti-root snapshot履歴ツール",
    top_long_about: r#"majutsuは、ローカルデータ喪失から復旧できるように開発ホスト上の複数ディレクトリをsnapshot化します。

majutsuはGitやjujutsu互換のVCSではありません。設定したroot配下のファイル状態を独自の状態ディレクトリに記録します。新しいrootにはVCS内部、依存ディレクトリ、build出力、cacheを避けるbest-practice excludeが既定で適用されます。生成物も保護対象にする必要がある場合だけ `--no-default-excludes` を使ってください。

基本フロー:
  mj init
  mj root add notes ~/notes
  mj snapshot --message 'first snapshot'
  mj log
  mj restore plan --root notes --to /tmp/majutsu-restore
  mj restore apply --root notes --to /tmp/majutsu-restore

remote復旧フロー:
  mj init --remote file:///mnt/backup/majutsu
  mj sync --wait
  mj remote fsck
  mj clone --remote file:///mnt/backup/majutsu --home /tmp/recovered-majutsu

コマンド分類:
  Setup: init, root
  Daily use: status, health, state, log, diff, snapshot, commit, note, track, untrack
  History: branch, switch, op
  Recovery: restore, restore mount, restore unmount, restore hydrate, mount, unmount, hydrate, clone
  Remote: sync, remote, lifecycle
  Service: watch, daemon
  Security: key
  Storage maintenance: large, cache, pack, prune, gc, fsck
  Advanced/debug: event

状態ディレクトリは `--home`、`MAJUTSU_HOME`、XDG config、`$HOME/.majutsu` の順に解決されます。
ホスト単位のsystem保護には `mj --system ...` を使います。これは `/etc/majutsu/config.toml` を読み、なければ `/var/lib/majutsu` にフォールバックします。"#,
    after_help: "詳細な使い方は README.md と docs/OPERATIONS.md を参照してください。",
    home_help: "使用するmajutsu状態ディレクトリを指定します",
    home_long_help: "`config.toml`、SQLite DB、ローカルobject、operation logを含むmajutsu状態ディレクトリを指定します。省略時は MAJUTSU_HOME、XDG config、$HOME/.majutsu の順に確認します。",
    system_help: "system用のmajutsuインスタンスを使います",
    system_long_help: "/etc や systemd unit などroot所有のホスト設定を保護するためのsystem用majutsuインスタンスを使います。--homeを同時指定しない場合は /etc/majutsu/config.toml を確認し、その後 /var/lib/majutsu にフォールバックします。",
    init_about: "majutsu状態ディレクトリを初期化します",
    root_about: "snapshot rootの追加、更新、一覧表示、一時停止を行います",
    snapshot_about: "設定済みrootのcommit相当のcheckpointを作成します",
    status_about: "root、current snapshot、queue、daemon状態を表示します",
    health_about: "通常運用に必要な保護状態を診断します",
    state_about: "live rootに対する管理対象ファイル変更を表示します",
    note_about: "operationまたはsnapshotのnoteを表示・編集します",
    track_about: "root規則で除外されるpathも明示的に管理対象へ戻します",
    untrack_about: "作業ファイルを削除せずにpathを管理対象から外します",
    log_about: "管理対象ファイルの最近の変更を表示します",
    diff_about: "snapshotや時点間の差分を表示します",
    branch_about: "論理履歴branchの作成、一覧、切替、削除を行います",
    switch_about: "active branchを切り替え、必要ならファイルも復元します",
    op_about: "jj風のoperation log entryを確認または復元します",
    restore_about: "復元の計画、適用、準備、再開を行います",
    mount_about: "読み取り専用の復元viewをmountします",
    unmount_about: "復元viewをunmountします",
    hydrate_about: "materialized restore view内のlarge objectをhydrateします",
    clone_about: "remote metadataから空の状態ディレクトリを復旧します",
    sync_about: "metadataとobjectを設定済みremoteへ同期します",
    remote_about: "remoteの到達性、整合性、host timelineを確認します",
    lifecycle_about: "S3/GCS lifecycle policyの生成、確認、適用を行います",
    watch_about: "filesystemを監視し、検出した変更をsnapshot化します",
    daemon_about: "watch daemonの起動、停止、確認、metrics出力を行います",
    key_about: "暗号化master keyのexport、import、rotationを行います",
    large_about: "large objectの一覧、検証、pin管理を行います",
    cache_about: "同期済みlocal payload cacheの確認や削除を行います",
    pack_about: "通常blob objectをpackまたはcompactします",
    prune_about: "retention設定に従って古い履歴をpruneします",
    gc_about: "参照されていないlocal loose objectを削除します",
    fsck_about: "local metadata、object、queue、refを検査します",
    event_about: "local filesystem event journalを確認またはcompactします",
    restore_plan_about: "ファイルを変更せず復元計画を作成します",
    restore_apply_about: "設定済みroot pathまたは--toディレクトリへ復元を適用します",
    restore_prepare_about: "適用前に不足remote objectやarchive restoreを準備します",
    restore_resume_about: "不足objectが利用可能になった後、準備済みrestore jobを再開します",
    restore_snapshot_help: "このsnapshot idを復元します",
    restore_op_help: "このoperation idまたはprefixが作った状態を復元します",
    restore_at_help: "この時刻以前の最新snapshotを復元します",
    restore_ago_help: "2hなどの相対時間以前の最新snapshotを復元します",
    restore_root_help: "復元対象を1つの設定済みroot idに限定します",
    restore_path_help: "選択root内の相対pathに復元対象を限定します",
    restore_to_help: "設定済みroot pathではなく、このディレクトリへ復元します",
    restore_force_help: "復元時の上書きや削除を許可します",
    restore_check_conflicts_help: "選択snapshotと復元先ファイルが衝突する場合は復元を拒否します",
};

static CLI_TEXT_ZH: CliText = CliText {
    top_about: "主机级 multi-root snapshot 历史工具",
    top_long_about: r#"majutsu 会为开发主机上的多个目录创建 snapshot，以便在本地数据丢失后恢复。

majutsu 不是 Git 或 jujutsu 兼容的 VCS。它把已配置 root 下的文件状态记录到自己的状态目录中。新 root 默认使用 best-practice excludes，排除 VCS 内部文件、依赖目录、build 输出和缓存。只有这些生成物也必须备份时，才使用 `--no-default-excludes`。

基本流程:
  mj init
  mj root add notes ~/notes
  mj snapshot --message 'first snapshot'
  mj log
  mj restore plan --root notes --to /tmp/majutsu-restore
  mj restore apply --root notes --to /tmp/majutsu-restore

remote 恢复流程:
  mj init --remote file:///mnt/backup/majutsu
  mj sync --wait
  mj remote fsck
  mj clone --remote file:///mnt/backup/majutsu --home /tmp/recovered-majutsu

命令分组:
  Setup: init, root
  Daily use: status, health, state, log, diff, snapshot, commit, note, track, untrack
  History: branch, switch, op
  Recovery: restore, restore mount, restore unmount, restore hydrate, mount, unmount, hydrate, clone
  Remote: sync, remote, lifecycle
  Service: watch, daemon
  Security: key
  Storage maintenance: large, cache, pack, prune, gc, fsck
  Advanced/debug: event

状态目录按 `--home`、`MAJUTSU_HOME`、XDG config、`$HOME/.majutsu` 的顺序解析。
主机级系统保护请使用 `mj --system ...`；它会读取 `/etc/majutsu/config.toml`，然后回退到 `/var/lib/majutsu`。"#,
    after_help: "详细用法请参阅 README.md 和 docs/OPERATIONS.md。",
    home_help: "使用指定的 majutsu 状态目录",
    home_long_help: "使用包含 `config.toml`、SQLite 数据库、本地对象和操作日志的 majutsu 状态目录。省略时按 MAJUTSU_HOME、XDG config、$HOME/.majutsu 的顺序查找。",
    system_help: "使用系统级 majutsu 实例",
    system_long_help: "使用面向 /etc 和 systemd unit 等 root 拥有的主机配置的系统级 majutsu 实例。若未同时指定 --home，则先检查 /etc/majutsu/config.toml，再回退到 /var/lib/majutsu。",
    init_about: "初始化 majutsu 状态目录",
    root_about: "添加、更新、列出或暂停 snapshot root",
    snapshot_about: "为已配置 root 创建类似 commit 的检查点",
    status_about: "显示 root、current snapshot、queue 和 daemon 状态",
    health_about: "报告正常运行所需的保护健康状态",
    state_about: "显示相对于 live root 的受管文件变更",
    note_about: "显示或编辑 operation 或 snapshot 的 note",
    track_about: "即使被 root 规则排除，也显式跟踪路径",
    untrack_about: "停止跟踪路径但不删除工作文件",
    log_about: "显示最近的受管文件变更",
    diff_about: "显示 snapshot 或时间点之间的差异",
    branch_about: "创建、列出、切换或删除逻辑历史 branch",
    switch_about: "切换 active branch，并可选择恢复文件",
    op_about: "检查或恢复 jj 风格的 operation log entry",
    restore_about: "规划、应用、准备或继续恢复",
    mount_about: "挂载只读恢复视图",
    unmount_about: "卸载恢复视图",
    hydrate_about: "在 materialized restore view 中 hydrate large object",
    clone_about: "从 remote metadata 引导空状态目录",
    sync_about: "将 metadata 和 object 同步到配置的 remote",
    remote_about: "检查 remote 可达性、完整性和 host timeline",
    lifecycle_about: "生成、检查或应用 S3/GCS lifecycle policy",
    watch_about: "监视 filesystem 并为检测到的变更创建 snapshot",
    daemon_about: "启动、停止、检查或导出 watch daemon metrics",
    key_about: "导出、导入或轮换加密 master key",
    large_about: "列出、验证和 pin large object",
    cache_about: "检查或清理已同步的本地 payload cache",
    pack_about: "打包或压缩普通 blob object",
    prune_about: "根据 retention 设置清理旧历史",
    gc_about: "删除未引用的本地 loose object",
    fsck_about: "检查本地 metadata、object、queue 和 ref",
    event_about: "检查或压缩本地 filesystem event journal",
    restore_plan_about: "在不修改文件的情况下构建恢复计划",
    restore_apply_about: "将恢复计划应用到配置的 root path 或 --to 目录",
    restore_prepare_about: "应用前准备缺失的 remote object 和 archive restore",
    restore_resume_about: "缺失对象可用后继续已准备的 restore job",
    restore_snapshot_help: "恢复此 snapshot id",
    restore_op_help: "恢复此 operation id 或 prefix 产生的状态",
    restore_at_help: "恢复此时间点之前的最新 snapshot",
    restore_ago_help: "恢复相对时长之前的最新 snapshot，例如 2h",
    restore_root_help: "仅恢复一个已配置的 root id",
    restore_path_help: "仅恢复所选 root 内的相对 path",
    restore_to_help: "恢复到此目录，而不是配置的 root path",
    restore_force_help: "允许恢复时覆盖和删除",
    restore_check_conflicts_help: "当目标文件与所选 snapshot 冲突时拒绝恢复",
};

static CLI_TEXT_ES: CliText = CliText {
    top_about: "Herramienta de historial snapshot multi-root a nivel de host",
    top_long_about: r#"majutsu crea snapshots de varios directorios en un host de desarrollo para poder recuperarse de una pérdida local de datos.

majutsu no es un VCS compatible con Git o jujutsu. Registra el estado de archivos bajo roots configurados en su propio directorio de estado. Los roots nuevos aplican excludes de buenas prácticas para internals de VCS, dependencias, salidas de build y cachés. Usa `--no-default-excludes` solo cuando esos generados también deban respaldarse.

Flujo básico:
  mj init
  mj root add notes ~/notes
  mj snapshot --message 'first snapshot'
  mj log
  mj restore plan --root notes --to /tmp/majutsu-restore
  mj restore apply --root notes --to /tmp/majutsu-restore

Flujo de recuperación remota:
  mj init --remote file:///mnt/backup/majutsu
  mj sync --wait
  mj remote fsck
  mj clone --remote file:///mnt/backup/majutsu --home /tmp/recovered-majutsu

Grupos de comandos:
  Setup: init, root
  Daily use: status, health, state, log, diff, snapshot, commit, note, track, untrack
  History: branch, switch, op
  Recovery: restore, restore mount, restore unmount, restore hydrate, mount, unmount, hydrate, clone
  Remote: sync, remote, lifecycle
  Service: watch, daemon
  Security: key
  Storage maintenance: large, cache, pack, prune, gc, fsck
  Advanced/debug: event

El directorio de estado se resuelve en este orden: `--home`, `MAJUTSU_HOME`, XDG config y `$HOME/.majutsu`.
Para protección de sistema a nivel de host, usa `mj --system ...`; lee `/etc/majutsu/config.toml` y luego usa `/var/lib/majutsu`."#,
    after_help: "Consulta README.md y docs/OPERATIONS.md para uso detallado.",
    home_help: "Usa un directorio de estado de majutsu específico",
    home_long_help: "Usa un directorio de estado de majutsu que contiene `config.toml`, la base SQLite, objetos locales y el registro de operaciones. Si se omite, majutsu revisa MAJUTSU_HOME, XDG config y $HOME/.majutsu en ese orden.",
    system_help: "Usa la instancia de majutsu del sistema",
    system_long_help: "Usa la instancia de majutsu del sistema para configuración del host propiedad de root, como /etc y unidades systemd. Si no se indica --home, revisa /etc/majutsu/config.toml y luego usa /var/lib/majutsu.",
    init_about: "Inicializa el directorio de estado de majutsu",
    root_about: "Agrega, actualiza, lista o pausa snapshot roots",
    snapshot_about: "Crea un checkpoint tipo commit de los roots configurados",
    status_about: "Muestra roots, current snapshot, queues y estado del daemon",
    health_about: "Informa la salud de protección para operación normal",
    state_about: "Muestra cambios de archivos gestionados contra roots vivos",
    note_about: "Muestra o edita la nota de una operación o snapshot",
    track_about: "Rastrea rutas explícitamente aunque las reglas del root las excluyan",
    untrack_about: "Deja de rastrear rutas sin borrar archivos de trabajo",
    log_about: "Muestra cambios recientes en archivos gestionados",
    diff_about: "Muestra diferencias entre snapshots o puntos en el tiempo",
    branch_about: "Crea, lista, cambia o elimina branches lógicos de historial",
    switch_about: "Cambia el active branch y opcionalmente restaura archivos",
    op_about: "Inspecciona o restaura entradas de operation log estilo jj",
    restore_about: "Planifica, aplica, prepara o reanuda una restauración",
    mount_about: "Monta una vista de restauración de solo lectura",
    unmount_about: "Desmonta una vista de restauración",
    hydrate_about: "Hydrate large objects en una materialized restore view",
    clone_about: "Inicializa un estado vacío desde remote metadata",
    sync_about: "Sincroniza metadata y objetos al remote configurado",
    remote_about: "Comprueba alcance, integridad y timelines de hosts remotos",
    lifecycle_about: "Genera, inspecciona o aplica políticas lifecycle S3/GCS",
    watch_about: "Vigila el filesystem y crea snapshots de cambios detectados",
    daemon_about: "Inicia, detiene, inspecciona o exporta metrics del watch daemon",
    key_about: "Exporta, importa o rota la master key de cifrado",
    large_about: "Lista, verifica y fija large objects",
    cache_about: "Inspecciona o limpia payload cache local ya sincronizada",
    pack_about: "Empaqueta o compacta objetos blob normales",
    prune_about: "Elimina historial antiguo según la configuración de retention",
    gc_about: "Elimina loose objects locales no referenciados",
    fsck_about: "Comprueba metadata, objetos, queues y refs locales",
    event_about: "Inspecciona o compacta el filesystem event journal local",
    restore_plan_about: "Construye un plan de restauración sin cambiar archivos",
    restore_apply_about: "Aplica un plan a los root paths configurados o a --to",
    restore_prepare_about: "Prepara objetos remotos faltantes y archive restores antes de aplicar",
    restore_resume_about: "Reanuda un restore job preparado cuando los objetos faltantes estén disponibles",
    restore_snapshot_help: "Restaura este snapshot id",
    restore_op_help: "Restaura el estado producido por este operation id o prefix",
    restore_at_help: "Restaura el snapshot más reciente en o antes de este momento",
    restore_ago_help: "Restaura el snapshot más reciente antes de una duración relativa, como 2h",
    restore_root_help: "Limita la restauración a un root id configurado",
    restore_path_help: "Limita la restauración a esta ruta relativa dentro del root",
    restore_to_help: "Restaura en este directorio en vez de los root paths configurados",
    restore_force_help: "Permite sobrescrituras y eliminaciones durante la restauración",
    restore_check_conflicts_help: "Rechaza la restauración si los archivos de destino entran en conflicto con el snapshot",
};

static CLI_TEXT_FR: CliText = CliText {
    top_about: "Outil d'historique snapshot multi-root au niveau hôte",
    top_long_about: r#"majutsu crée des snapshots de plusieurs répertoires sur un hôte de développement afin de permettre la récupération après une perte de données locale.

majutsu n'est pas un VCS compatible Git ou jujutsu. Il enregistre l'état des fichiers sous les roots configurés dans son propre répertoire d'état. Les nouveaux roots appliquent par défaut des excludes de bonnes pratiques pour les internals VCS, dépendances, sorties de build et caches. Utilisez `--no-default-excludes` uniquement si ces fichiers générés doivent aussi être sauvegardés.

Flux de base:
  mj init
  mj root add notes ~/notes
  mj snapshot --message 'first snapshot'
  mj log
  mj restore plan --root notes --to /tmp/majutsu-restore
  mj restore apply --root notes --to /tmp/majutsu-restore

Flux de récupération remote:
  mj init --remote file:///mnt/backup/majutsu
  mj sync --wait
  mj remote fsck
  mj clone --remote file:///mnt/backup/majutsu --home /tmp/recovered-majutsu

Groupes de commandes:
  Setup: init, root
  Daily use: status, health, state, log, diff, snapshot, commit, note, track, untrack
  History: branch, switch, op
  Recovery: restore, restore mount, restore unmount, restore hydrate, mount, unmount, hydrate, clone
  Remote: sync, remote, lifecycle
  Service: watch, daemon
  Security: key
  Storage maintenance: large, cache, pack, prune, gc, fsck
  Advanced/debug: event

Le répertoire d'état est résolu dans l'ordre suivant: `--home`, `MAJUTSU_HOME`, config XDG puis `$HOME/.majutsu`.
Pour la protection système au niveau hôte, utilisez `mj --system ...`; cela lit `/etc/majutsu/config.toml` puis utilise `/var/lib/majutsu`."#,
    after_help: "Consultez README.md et docs/OPERATIONS.md pour l'utilisation détaillée.",
    home_help: "Utilise un répertoire d'état majutsu spécifique",
    home_long_help: "Utilise un répertoire d'état majutsu contenant `config.toml`, la base SQLite, les objets locaux et le journal des opérations. S'il est omis, majutsu vérifie MAJUTSU_HOME, la config XDG puis $HOME/.majutsu.",
    system_help: "Utilise l'instance majutsu système",
    system_long_help: "Utilise l'instance majutsu système destinée à la configuration hôte appartenant à root, comme /etc et les unités systemd. Si --home n'est pas fourni, majutsu vérifie /etc/majutsu/config.toml puis utilise /var/lib/majutsu.",
    init_about: "Initialise le répertoire d'état majutsu",
    root_about: "Ajoute, met à jour, liste ou met en pause les snapshot roots",
    snapshot_about: "Crée un checkpoint de type commit des roots configurés",
    status_about: "Affiche roots, current snapshot, queues et état du daemon",
    health_about: "Rapporte la santé de protection pour l'exploitation normale",
    state_about: "Affiche les changements de fichiers suivis dans les roots actifs",
    note_about: "Affiche ou modifie la note d'une opération ou d'un snapshot",
    track_about: "Suit explicitement des chemins même exclus par les règles du root",
    untrack_about: "Arrête de suivre des chemins sans supprimer les fichiers de travail",
    log_about: "Affiche les changements récents des fichiers gérés",
    diff_about: "Affiche les différences entre snapshots ou instants",
    branch_about: "Crée, liste, change ou supprime des branches logiques d'historique",
    switch_about: "Change l'active branch et restaure éventuellement les fichiers",
    op_about: "Inspecte ou restaure des entrées d'operation log de style jj",
    restore_about: "Planifie, applique, prépare ou reprend une restauration",
    mount_about: "Monte une vue de restauration en lecture seule",
    unmount_about: "Démonte une vue de restauration",
    hydrate_about: "Hydrate les large objects dans une materialized restore view",
    clone_about: "Amorce un état vide depuis la remote metadata",
    sync_about: "Synchronise metadata et objets vers le remote configuré",
    remote_about: "Vérifie l'accessibilité, l'intégrité et les timelines d'hôtes distants",
    lifecycle_about: "Génère, inspecte ou applique une policy lifecycle S3/GCS",
    watch_about: "Surveille le filesystem et snapshot les changements détectés",
    daemon_about: "Démarre, arrête, inspecte ou exporte les metrics du watch daemon",
    key_about: "Exporte, importe ou renouvelle la master key de chiffrement",
    large_about: "Liste, vérifie et épingle les large objects",
    cache_about: "Inspecte ou nettoie le payload cache local synchronisé",
    pack_about: "Pack ou compacte les objets blob normaux",
    prune_about: "Élague l'ancien historique selon les paramètres de retention",
    gc_about: "Supprime les loose objects locaux non référencés",
    fsck_about: "Vérifie metadata, objets, queues et refs locaux",
    event_about: "Inspecte ou compacte le filesystem event journal local",
    restore_plan_about: "Construit un plan de restauration sans modifier les fichiers",
    restore_apply_about: "Applique un plan aux root paths configurés ou au répertoire --to",
    restore_prepare_about: "Prépare les remote objects manquants et archive restores avant application",
    restore_resume_about: "Reprend un restore job préparé lorsque les objets manquants sont disponibles",
    restore_snapshot_help: "Restaure ce snapshot id",
    restore_op_help: "Restaure l'état produit par cet operation id ou prefix",
    restore_at_help: "Restaure le snapshot le plus récent à cet instant ou avant",
    restore_ago_help: "Restaure le snapshot le plus récent avant une durée relative, comme 2h",
    restore_root_help: "Limite la restauration à un root id configuré",
    restore_path_help: "Limite la restauration à ce chemin relatif dans le root choisi",
    restore_to_help: "Restaure dans ce répertoire au lieu des root paths configurés",
    restore_force_help: "Autorise les écrasements et suppressions pendant la restauration",
    restore_check_conflicts_help: "Refuse la restauration si les fichiers de destination entrent en conflit avec le snapshot choisi",
};

#[derive(Subcommand)]
pub(crate) enum Command {
    #[command(about = "Initialize the majutsu state directory")]
    Init(InitArgs),
    #[command(about = "Add, update, list, or pause snapshot roots")]
    Root {
        #[command(subcommand)]
        command: RootCommand,
    },
    #[command(
        visible_alias = "commit",
        about = "Create a commit-like checkpoint of configured roots"
    )]
    Snapshot(SnapshotArgs),
    #[command(about = "Show roots, current snapshot, queues, and daemon state")]
    Status(StatusArgs),
    #[command(about = "Report protection health for normal operation")]
    Health(HealthArgs),
    #[command(about = "Show managed file changes against live roots")]
    State(StateArgs),
    #[command(about = "Show or edit the note on an operation or snapshot")]
    Note(NoteArgs),
    #[command(about = "Explicitly track paths even when excluded by root rules")]
    Track(PathTrackArgs),
    #[command(about = "Stop tracking paths without deleting working files")]
    Untrack(PathTrackArgs),
    #[command(about = "Show recent managed file changes")]
    Log(LogArgs),
    #[command(about = "Show differences between snapshots or points in time")]
    Diff(DiffArgs),
    #[command(about = "Create, list, switch, or delete logical history branches")]
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
    },
    #[command(about = "Switch the active branch and optionally restore its files")]
    Switch(BranchSwitchArgs),
    #[command(about = "Inspect or restore jj-style operation-log entries")]
    Op {
        #[command(subcommand)]
        command: OpCommand,
    },
    #[command(about = "Plan, apply, prepare, or resume a restore")]
    Restore(RestoreTopArgs),
    #[command(about = "Mount a read-only restore view")]
    Mount(MountArgs),
    #[command(about = "Unmount a restore view")]
    Unmount(UnmountArgs),
    #[command(about = "Hydrate large objects in a materialized restore view")]
    Hydrate(HydrateArgs),
    #[command(about = "Bootstrap an empty state directory from remote metadata")]
    Clone(CloneArgs),
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
    #[command(about = "Pack or compact normal blob objects")]
    Pack(PackArgs),
    #[command(about = "Prune old history according to retention settings")]
    Prune(PruneArgs),
    #[command(about = "Remove unreferenced local loose objects")]
    Gc,
    #[command(about = "Check local metadata, objects, queues, and refs")]
    Fsck(FsckArgs),
    #[command(about = "Inspect or compact the local filesystem event journal")]
    Event {
        #[command(subcommand)]
        command: EventCommand,
    },
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
    List(RootListArgs),
    #[command(about = "Show client and backend sizes for current roots")]
    Size(RootSizeArgs),
    #[command(about = "Remove a root from the configuration")]
    Remove { id: String },
    #[command(about = "Temporarily pause snapshots for a root")]
    Pause { id: String },
    #[command(about = "Resume a paused root")]
    Resume { id: String },
    #[command(about = "Record a root as deleted")]
    MarkDeleted { id: String },
}

#[derive(Args)]
pub(crate) struct RootListArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Emit machine-readable JSON instead of the aligned table"
    )]
    pub(crate) json: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Do not shorten columns to fit the terminal width"
    )]
    pub(crate) no_truncate: bool,
}

#[derive(Args)]
pub(crate) struct RootSizeArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Emit machine-readable JSON instead of the aligned table"
    )]
    pub(crate) json: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Show retained historical payloads that are not referenced by the current snapshot"
    )]
    pub(crate) history: bool,
    #[arg(
        long,
        default_value_t = 30,
        help = "Limit rows in the retained historical payload report"
    )]
    pub(crate) history_limit: usize,
    #[arg(
        long,
        default_value_t = false,
        help = "Ignore the cached remote object listing and scan the backend directly"
    )]
    pub(crate) no_remote_cache: bool,
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
pub(crate) struct HealthArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Emit machine-readable JSON instead of text"
    )]
    pub(crate) json: bool,
    #[arg(
        short = 'v',
        long,
        default_value_t = false,
        help = "Include per-root health details in text output"
    )]
    pub(crate) verbose: bool,
}

#[derive(Args)]
pub(crate) struct StateArgs {
    #[arg(
        value_name = "REF",
        help = "Show managed file changes since a reference such as 1h, 03:40, op-..., or snap-...; omitted means since the first snapshot"
    )]
    pub(crate) reference: Option<String>,
    #[arg(
        short = 'j',
        long,
        default_value_t = false,
        help = "Emit machine-readable JSON"
    )]
    pub(crate) json: bool,
    #[arg(
        short = 'r',
        long,
        value_name = "ID",
        help = "Limit reference-based file status to one configured root"
    )]
    pub(crate) root: Option<String>,
    #[arg(
        short = 'g',
        long = "global",
        default_value_t = false,
        help = "Show reference-based file status for all active roots even when run inside a root"
    )]
    pub(crate) global: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Show colored line diffs after changed file lines"
    )]
    pub(crate) diff: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Show only paths that are managed in the basis snapshot but missing from the live root"
    )]
    pub(crate) deleted: bool,
    #[arg(
        short = 's',
        long = "status",
        value_name = "MARK",
        help = "Filter by change status. May be repeated or comma-separated; valid marks are A, M, D, and m"
    )]
    pub(crate) status: Vec<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Include metadata-only changes such as directory mtime, mode, owner, or xattrs"
    )]
    pub(crate) meta: bool,
}

#[derive(Args, Clone)]
pub(crate) struct PathTrackArgs {
    #[arg(
        short = 'r',
        long = "root",
        value_name = "ID",
        help = "Apply the path operation within this root"
    )]
    pub(crate) root: Option<String>,
    #[arg(
        required = true,
        value_name = "PATH",
        help = "Path to track or untrack; relative paths are resolved from the current directory"
    )]
    pub(crate) paths: Vec<PathBuf>,
}

#[derive(Args, Clone)]
pub(crate) struct NoteArgs {
    #[arg(
        value_name = "REF",
        help = "Operation id/prefix or snapshot id/prefix to show or edit"
    )]
    pub(crate) reference: String,
    #[arg(
        short = 'm',
        long = "message",
        value_name = "TEXT",
        conflicts_with_all = ["stdin", "clear"],
        help = "Replace the note with this text"
    )]
    pub(crate) message: Option<String>,
    #[arg(
        long = "stdin",
        default_value_t = false,
        conflicts_with_all = ["message", "clear"],
        help = "Read the replacement note from stdin"
    )]
    pub(crate) stdin: bool,
    #[arg(
        long,
        default_value_t = false,
        conflicts_with_all = ["message", "stdin"],
        help = "Clear the note"
    )]
    pub(crate) clear: bool,
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
        help = "Add an exclude glob on top of the default best-practice excludes"
    )]
    pub(crate) exclude: Vec<String>,
    #[arg(
        long = "preset",
        value_name = "NAME",
        help = "Apply additional exclude presets such as git-working-tree, rust, node, or default"
    )]
    pub(crate) presets: Vec<String>,
    #[arg(
        long = "no-default-excludes",
        default_value_t = false,
        help = "Do not apply best-practice excludes for VCS internals, dependencies, build outputs, and caches"
    )]
    pub(crate) no_default_excludes: bool,
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
    #[arg(
        long = "volatile",
        value_name = "GLOB",
        help = "Mark matching high-frequency paths as volatile"
    )]
    pub(crate) volatile: Vec<String>,
    #[arg(
        long = "volatile-mode",
        default_value = "checkpoint",
        value_name = "MODE",
        help = "Volatile path handling: checkpoint or exclude"
    )]
    pub(crate) volatile_mode: String,
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
        help = "Apply exclude presets such as default, git-working-tree, rust, or node"
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
    #[arg(long = "volatile", value_name = "GLOB")]
    pub(crate) volatile: Vec<String>,
    #[arg(long = "volatile-mode", value_name = "MODE")]
    pub(crate) volatile_mode: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_volatile: bool,
}

#[derive(Args)]
pub(crate) struct SnapshotArgs {
    #[arg(long)]
    pub(crate) message: Option<String>,
    #[arg(skip)]
    pub(crate) origin: Option<crate::operation_log::OperationOriginOverride>,
}

#[derive(Args, Clone)]
pub(crate) struct LogArgs {
    #[arg(
        long,
        value_name = "N",
        help = "Maximum number of operations to show; file detail lines do not count toward this limit"
    )]
    pub(crate) limit: Option<usize>,
    #[arg(long)]
    pub(crate) root: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Show internal operation records, including sync/fsck/config operations, instead of only managed file changes"
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
    #[command(about = "Mount a read-only restore view")]
    Mount(MountArgs),
    #[command(about = "Unmount a restore view")]
    Unmount(UnmountArgs),
    #[command(about = "Hydrate large objects in a materialized restore view")]
    Hydrate(HydrateArgs),
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
    #[arg(long, help = "Restore this snapshot id")]
    pub(crate) snapshot: Option<String>,
    #[arg(
        long,
        help = "Restore the state produced by this operation id or prefix"
    )]
    pub(crate) op: Option<String>,
    #[arg(long, help = "Restore the latest snapshot at or before this time")]
    pub(crate) at: Option<String>,
    #[arg(
        long,
        help = "Restore the latest snapshot at or before this relative duration, such as 2h"
    )]
    pub(crate) ago: Option<String>,
    #[arg(long, help = "Limit restore to one configured root id")]
    pub(crate) root: Option<String>,
    #[arg(
        long,
        help = "Limit restore to this relative path inside the selected root"
    )]
    pub(crate) path: Option<PathBuf>,
    #[arg(
        long,
        help = "Restore into this directory instead of the configured root paths"
    )]
    pub(crate) to: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = false,
        help = "Allow overwrites and deletes during restore"
    )]
    pub(crate) force: bool,
    #[arg(
        long,
        default_value_t = true,
        action = clap::ArgAction::Set,
        help = "Refuse restore when destination files conflict with the selected snapshot"
    )]
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
    #[command(about = "Initialize an empty remote with majutsu host index metadata")]
    Init {
        #[arg(
            long,
            default_value_t = false,
            help = "Overwrite an existing empty or host index object"
        )]
        force: bool,
    },
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
    #[arg(
        long,
        help = "Trust executable hooks and application plugins from remote metadata"
    )]
    pub(crate) trust_remote_hooks: bool,
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
    #[command(about = "Render a user or system service definition for systemd or launchd")]
    Service {
        #[arg(long, default_value = "systemd")]
        provider: String,
        #[arg(
            long,
            default_value = "foreground",
            value_parser = ["foreground", "forking"],
            help = "Service style: foreground supervises `mj watch`, forking delegates lifecycle to `mj daemon start/stop`"
        )]
        style: String,
        #[arg(
            long,
            default_value = "user",
            value_parser = ["user", "system"],
            help = "Render a user service or root-owned system service"
        )]
        scope: String,
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
    #[arg(
        long,
        default_value_t = false,
        help = "Backfill local snapshot payload indexes used by scoped fsck, then exit"
    )]
    pub(crate) backfill_index: bool,
    #[arg(
        long,
        default_value_t = false,
        requires = "backfill_index",
        help = "Allow index backfill to hydrate missing metadata objects from the remote"
    )]
    pub(crate) hydrate_index_objects: bool,
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
    #[arg(
        long,
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set,
        help = "After deleting snapshots, sync metadata and prune remote objects that became unreachable"
    )]
    pub(crate) remote_cleanup: bool,
}

#[derive(Args)]
pub(crate) struct PackArgs {
    #[arg(long, default_value_t = false)]
    pub(crate) compact: bool,
}
