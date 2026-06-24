# majutsu 運用 runbook

## 日次確認

```sh
mj status
mj sync status
mj fsck --quick
mj remote check
mj remote fsck
mj daemon status
mj daemon metrics
```

`mj status` は運用上の要点確認に使う。`mj state` は Git の `status -s` に近い
管理対象ファイル差分の確認に使う。

既存の operation / snapshot に運用上の説明を後から付けたい場合は `mj note` を使う。
引数なしの表示ではなく、`mj note REF` で現在の note を表示し、`-m`、`--stdin`、
`--clear` で更新する。`snap-...` は、その snapshot を作成した operation に解決されるため、
checkpoint として作った snapshot に後から説明を付けられる。

```sh
mj note snap-12345678
mj note snap-12345678 -m 'migration前のcheckpoint'
mj note op-12345678 --clear
```

`mj state` を引数なしで実行すると、初回 snapshot から現在の live filesystem までの
全変更を表示する。指定時点からの差分は `mj state <ref>` を使う。root を絞る場合は
`-r/--root`、全root表示を明示する場合は `-g/--global` を使う。

```sh
mj state
mj state 1d -r moon
mj state 03:40 -r moon --diff
mj state op-123456789abc -g
mj state --deleted
mj state --status A,M
mj track path/to/file
mj untrack path/to/file
```

通常表示は `A/M/D` のファイル変更だけを出す。directory mtime、mode、owner、xattrs など
metadata-only の変更は既定では隠し、必要な場合だけ `--meta` で小文字 `m` として表示する。
`--diff` は text file の変更行を `@@` / `-` / `+` の色付き diff 形式で file row の直後に表示する。
binary、special file、1 MiB 超のファイルは status row のみ表示し、diff body は省略する。
実体が削除済みの管理対象だけを確認する場合は `--deleted` または `--status D` を使う。
任意の状態で絞る場合は `--status A,M` や `-s A -s D` のように指定する。

`rm` と `untrack` は別の操作である。root配下で管理対象になったファイルを `rm` した場合、
そのpathは管理対象のまま削除状態として残り、`mj state --deleted` や `mj log` の対象になる。
一方で `mj untrack <path>` は、そのpathを明示的に管理対象から外す操作であり、以後の
snapshot対象から除外され、retention / prune / gc / remote cleanup によって backend から
やがて除去される。除外規則に当たるpathを明示的に保護したい場合は `mj track <path>` を使う。

```sh
mj track memo/important.md
mj untrack tmp/local.db
mj track -r moon excluded/keep.md
mj untrack -r moon old/generated.bin
```

大量の tracked path を整理する場合は、shell で小分けにせず `--path-file` または
`--stdin` を使う。root の exclude を締めた後に「現在のroot rulesでは管理対象外になるはずの
既存 tracked path」を監査・除去するには `--excluded` を使う。これは作業treeのファイルを
削除せず、tracking metadata だけを更新する。

```sh
mj untrack -r moon --path-file paths.txt --summary
find . -name '*.tmp' -print | mj untrack -r moon --stdin --summary
mj untrack -r moon --excluded --dry-run --summary
mj untrack -r moon --excluded --summary
```

古い履歴objectが壊れていて cleanup rewrite まで完了できない場合でも、current metadata を
先に直す逃げ道として `mj root set --skip-history-rewrite` と
`mj untrack --continue-on-history-error` を使える。これは通常運用の既定ではなく、`mj fsck` /
`mj remote fsck --objects` で履歴を修復するまでの復旧用モードである。

```sh
mj root set moon --exclude '*.tmp' --skip-history-rewrite
mj untrack -r moon --excluded --continue-on-history-error --summary
```

`mj status` の先頭には remote head の同期状態が表示される。`Remote head synced (cached)`
なら local current snapshot が最後に確認した remote current ref と一致している。
これは quick signal であり、全 object の存在確認ではない。`lagging (cached)`、
`not synced (cached)`、`remote unavailable` の場合は `mj sync status` と `mj sync --wait`
で詳細確認と追従を行う。全 object の監査は `mj sync status --deep` や
`mj remote fsck --objects` を使う。

`mj status` は出力が端末高さを超える場合に pager を使う。ログ採取や cron などで
明示的に標準出力へ出したい場合は `--no-pager`、短い出力でも pager で確認したい場合は
`--pager` を使う。

```sh
mj status --no-pager
mj status --pager
```

`Local Storage` は apparent size と disk usage を分けて表示する。小ファイル多数の
event journal や queue では、apparent size が小さくても disk usage が大きくなることがある。

## user/system インスタンス分離

通常の開発repoやユーザー作業領域は、ユーザー権限の `mj` で保護する。

```sh
mj init --encrypt --remote s3://bucket/prefix/user
mj root add moon ~/moon
mj daemon service --provider systemd --scope user
```

新規 root は VCS 内部、依存物、build output、cache を既定で除外する。完全な
ファイルシステム像を保存したい場合だけ `mj root add <id> <path> --no-default-excludes`
を使う。root 固有に追加で外すものがある場合は `--exclude`、`--preset`、`mj root set`
で明示する。

`/etc`、systemd system unit、root所有のenvファイル、`/usr/local/sbin` の手製スクリプトなど、
ホスト復旧に必要で通常ユーザーから読めない構成は、root権限のsystemインスタンスで保護する。
systemインスタンスはユーザー用 `~/.majutsu` とstateを共有しない。

```sh
sudo mj --system init --encrypt --remote s3://bucket/prefix/system
sudo mj --system root add systemd-system /etc/systemd/system --include '**'
sudo mj --system root add etc-service-config /etc --include 'stackchan-control.env'
sudo mj --system daemon service --provider systemd --scope system > /etc/systemd/system/majutsu.service
sudo systemctl enable --now majutsu.service
```

`mj --system` は `--home` がない場合に `/etc/majutsu/config.toml` の `[state].home` を読み、
未設定なら `/var/lib/majutsu` を使う。root用backend prefixと暗号鍵はユーザー用と分ける。
単一のroot権限daemonでユーザーrepoまで管理すると、復元時の所有権や誤操作時の影響範囲が
大きくなるため避ける。

## event journal retention

filesystem event journal は daemon の crash recovery と運用調査に使う。通常は snapshot 後の
best-effort compact に任せればよい。保持数や削除候補を確認するには `mj event stat` を使う。

```sh
mj event stat
```

処理済み event record を明示的に整理したい場合は `mj event compact` を使う。直近 snapshot
cycle の因果関係を調査できるように、前回 snapshot 完了より古い event だけを削除し、
pending event は残す。削除前の確認には `--dry-run` を使う。snapshot 後の自動 compact も
同じ保持方針で処理済み record を整理する。

```sh
mj event compact --dry-run
mj event compact
```

## unchanged snapshot

直前 snapshot から root tree が変化していない場合、`mj snapshot` は既定で新しい
snapshot metadata を作らない。定期 daemon snapshot や手動確認で remote metadata が
増え続けることを避けるためである。この場合も event journal には `snapshot-noop` と
`snapshot-finish` が残り、watch / daemon の進捗判定には使われる。no-op 判定のために
一時的に再生成した payload cache と tree metadata は、remote に同期済みであれば同じ経路で
prune される。

監査上の checkpoint として、変化がなくても snapshot を明示的に残したい場合だけ
`MAJUTSU_SNAPSHOT_ALLOW_NOOP=1` を付ける。

```sh
MAJUTSU_SNAPSHOT_ALLOW_NOOP=1 mj snapshot
```

## local fsck

通常運用の保護状態確認では full fsck ではなく `mj health` を使う。

```sh
mj health
mj health --verbose
mj health --json
```

`mj health` は daemon、active root、remote、cached remote head、upload queue、
pending event journal、sync lock、暗号化key fileの基本状態から、`protected` / `degraded` /
`unprotected` を返す。クラッシュ対策として日常的に監視すべきなのは full fsck ではなくこの
軽量 health signal である。
通常のテキスト出力は要約のみを表示し、root別の詳細行は `mj health --verbose` で表示する。
`mj health --json` には root 別に `present`、`current_snapshot_includes`、
`current_file_count`、`current_tree_id`、`last_changed_snapshot`、`last_changed_at` も含まれる。
`mj status` の Roots 表も current snapshot 上の file count、tree id、最終変更時刻を表示する。
root が `permission-denied` など degraded 状態になった場合は、root別に `degraded_kind`、
`degraded_at`、`degraded_message` も出力される。`mj status` の Roots 表では `ISSUE` 列で
degraded kind と発生時刻を確認できる。
cached remote current または compact head の root ack がある場合は root 別に
`remote_snapshot_includes`、`remote_tree_id`、`remote_synced`、`remote_synced_snapshot`、
`remote_synced_at` も出力される。S3互換 backend では compact head の `root_acks` を優先し、
古い backend/cache では remote current snapshot から導出する。これは host単位 remote current が
lagging していても、変更されていない root が remote 側に保全済みかを判定するための軽量 signal である。

`protected` は active root が daemon に監視され、upload queue が空で、local current と
cached remote head が一致している状態を表す。`degraded` は一時的なsync中やpending eventなど、
復旧可能だが注意が必要な状態、`unprotected` は daemon停止、remote未設定、remote head遅延など
クラッシュ時の保全目的を満たさない状態を表す。

daemon は watch loop の起動時、periodic rescan 後、filesystem event snapshot 後に
`$MAJUTSU_HOME/runtime/health.json` を更新する。外部監視はこのファイルを読むことで、
full fsck を回さずに直近の保護状態を確認できる。

health が `degraded` / `unprotected` になった時に通知したい場合は
`MAJUTSU_HEALTH_NOTICE_CMD` を設定する。通知コマンドには `MAJUTSU_HOME`、
`MAJUTSU_HEALTH_STATE`、`MAJUTSU_HEALTH_ISSUE_COUNT`、
`MAJUTSU_HEALTH_ISSUE_CODES`、`MAJUTSU_HEALTH_CURRENT_SNAPSHOT` が渡される。
同一 state / issue set の通知は
`MAJUTSU_HEALTH_NOTICE_RATE_LIMIT_SECS`（default: 3600）で rate limit される。

fsckによる軽量診断では `--quick` を使う。

```sh
mj fsck --quick
```

`--quick` は DB、refs、queue、operation log、config drift などを確認し、全履歴の
payload decode / tree manifest 検査は省く。大きな root を多数管理している host でも
短時間で異常を検出するためのモードである。

全履歴と payload まで検査する場合は通常の `mj fsck` または明示的に `--deep` を使う。
長時間化する場合は `--progress` と `--timeout-secs` を指定する。大規模 root の smoke
検査では `--sample` で重い payload / manifest phase の検査件数を制限できる。
最近の snapshot から参照される payload / manifest だけを確認したい場合は `--since` を使う。
`--since` は heavy phase を絞るための指定で、refs、queue、oplog、history graph の基本整合性は
引き続き全体を確認する。`--sample` または `--since` を使う場合、全履歴を前提にした
metadata dangling 検査は false positive を避けるため省略される。metadata dangling まで含む
完全監査は `--sample` / `--since` なしの `mj fsck --deep` で実行する。

新しい snapshot では snapshot 到達 payload index と large object chunk index をDBに保持する。
`mj fsck --since` は index が揃っている場合、tree manifest や large manifest を再hydrateせず
DB queryでscopeを構築する。古いsnapshotやindex不足がある場合は従来通りmanifest読み込みへ
フォールバックし、読み込めたsnapshot / large objectのindexを補完する。

```sh
mj fsck --deep --progress --timeout-secs 300
mj fsck --deep --sample 1000 --progress
mj fsck --deep --since "24h ago" --progress
```

`--timeout-secs` で停止した場合は state 破損を意味しない。進捗の遅い phase を確認し、
必要なら大きい timeout で再実行する。

remote backend が S3 / GCS S3互換の場合、HTTP request は既定で connect timeout 10秒、
request timeout 300秒を使う。provider 側の一時遅延を切り分けたい場合は次で調整できる。

```sh
MAJUTSU_S3_CONNECT_TIMEOUT_SECS=10 mj fsck --since "24h ago" --progress
MAJUTSU_S3_REQUEST_TIMEOUT_SECS=300 mj sync status --deep --progress
```

`mj sync` の S3/GCS request 数と転送 body bytes を確認したい場合は次を使う。

```sh
MAJUTSU_TRACE_REMOTE=1 mj sync
```

stderr に `remote_trace` が1行出力される。`requests` は S3互換 API の HTTP request 数、
`upload_bytes` / `download_bytes` は HTTP body の合計で、TLS や HTTP header の overhead は含まない。
`MAJUTSU_TRACE_S3=1` も同じ意味で使える。

`mj fsck --progress` は remote payload key の一括取得を `remote-payload-index` phase として表示する。
`--sample` または `--since` で検査範囲を絞る場合は全 payload prefix の LIST を避け、
欠落 local object に遭遇した時だけ対象 key を HEAD probe する。短時間 smoke 検査では
GCS backend 全体の listing latency に引きずられにくい。
`--since` と `--sample` を併用する場合、対象 snapshot は新しい順に選び、scope 構築に
payload index を使う。scoped sample では深い snapshot / large / pack manifest object 検査を
省略し、直近 payload availability の smoke を優先する。
large object は object 件数だけでなく chunk 検査件数にも `--sample` を適用する。

古い snapshot の payload index を補完する場合は `--backfill-index` を使う。通常は
ローカルに残っている inline metadata のみを処理し、compact tree manifest の展開や remote hydrate は
行わない。missing metadata も含めて補完したいメンテナンス時だけ `--hydrate-index-objects` を明示する。

```sh
mj fsck --backfill-index --since "24h ago" --sample 100 --progress
mj fsck --backfill-index --hydrate-index-objects --timeout-secs 600 --progress
```

## daemon recovery

`mj status` で `Daemon stale pid` または `running, ipc unavailable` が出た場合は
`daemon doctor` で状態と直近ログを確認する。

```sh
mj daemon doctor
```

stale pid や IPC 不通は `daemon restart` で runtime pid/socket を掃除して起動し直せる。

```sh
mj daemon restart
mj daemon status
```

daemon の stdout/stderr と起動記録は `$MAJUTSU_HOME/logs/majutsu.log` に追記される。

## remote object 監査と修復

通常の `mj remote fsck` は metadata health を高速に確認する。
payload object の存在確認まで行う場合は `--objects` を使う。
compact head がある場合、通常の `mj remote fsck` と `--deep` は `root_acks` も
current snapshot の root tree と照合し、root別 `snapshot_id`、`tree_id`、`tree_key`、
`file_count`、`synced_at` の不一致を検出する。

```sh
mj remote fsck --objects --parallelism 32 --timeout-secs 300
```

短時間の sample 確認だけをしたい場合は `--sample` を使う。

```sh
mj remote fsck --objects --sample 1000 --parallelism 32
```

payload の decode / hash まで確認する場合は `--deep` を使う。`--deep` は metadata graph
検証の後に payload 検証を行う。短時間の smoke では `--sample`、長時間化を避ける場合は
`--timeout-secs` を指定する。

```sh
mj remote fsck --deep --sample 1000
mj remote fsck --deep --timeout-secs 300
```

metadata graph の全量監査を避け、現在 host の payload decode / hash だけを smoke したい場合は
`--payload-only` を併用する。

```sh
mj remote fsck --deep --payload-only --sample 1000
```

remote に欠けている referenced object があり、local state に payload が残っている場合は
`mj remote repair` で再送できる。実行前の確認には `--dry-run` を使う。

```sh
mj remote repair --dry-run --parallelism 32
mj remote repair --parallelism 32 --timeout-secs 300
```

`remote repair` は local に残っている object だけを再送する。local payload cache が
すでに prune 済みの object は修復できないため、別 host や remote backup からの復旧、
または履歴 prune/gc の判断が必要。

## 災害復旧 drill

```sh
export MAJUTSU_MASTER_KEY=<暗号化時に export した key>
mj --home /tmp/recovered-majutsu clone --remote s3://bucket/prefix
mj --home /tmp/recovered-majutsu fsck
mj --home /tmp/recovered-majutsu restore plan --to /tmp/restore
mj --home /tmp/recovered-majutsu restore apply --to /tmp/restore
```

## large object 検証

```sh
mj large stat
mj large verify
mj large pin --root photos --since 30d
mj large unpin --older-than 180d
```

## lifecycle 管理

policy を生成する。

```sh
mj lifecycle policy --provider s3
mj lifecycle policy --provider gcs
```

S3 lifecycle policy を適用する。

```sh
mj lifecycle apply --provider s3 --dry-run true
mj lifecycle apply --provider s3 --dry-run false
```

metadata は hot に維持し、archive 対象は pack や large chunk などの payload に限定する。


## sync status mode

通常確認では高速な status を使う。

```sh
mj sync status
```

deep status は参照 object の remote 存在確認を行う。通常は remote の key index を
`LIST` でまとめて取得し、local object key と canonical alias を set membership で確認する。
調査中に待ち時間を制限したい場合は `--sample`、`--timeout-secs`、`--progress` を使う。

```sh
mj sync status --deep
mj sync status --deep --sample 1000 --timeout-secs 30 --progress
```

## local cache

root 上の実体ファイルと remote backend が復旧用の正である運用では、
`$MAJUTSU_HOME/objects` の payload object や古い tree manifest は二重保持になりうる。

`mj cache stat` は remote に存在するため安全に削除できる local payload cache を表示する。

```sh
mj cache stat
```

`mj cache prune` は pack 本体、large chunk、loose blob などの payload cache だけを削除する。
metadata は既定では保持する。

```sh
mj cache prune --dry-run
mj cache prune
```

古い tree manifest も remote に同期済みであることを確認して削除したい場合は
`--metadata` を付ける。tree manifest は restore / diff / fsck が必要になった時点で
remote から on-demand hydrate される。

```sh
mj cache stat --metadata
mj cache prune --metadata --dry-run
mj cache prune --metadata
```

cache prune 後も `mj restore`、`mj hydrate`、`mj mount` は必要に応じて remote から object を
hydrate する。`mj fsck` は remote に存在する payload cache 欠落を正常扱いする。
metadata cache を prune した場合、full fsck は必要な tree manifest を remote から hydrate して検査する。
remote 未同期の payload / metadata 欠落は引き続き異常として扱う。

`mj sync` は同期完了後、remote に存在することを確認できる payload cache と tree metadata cache を
既定で自動 prune する。調査目的で同期後の cache を残したい場合は
`MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE=0` または
`MAJUTSU_SYNC_LOCAL_METADATA_CACHE_PRUNE=0` を指定する。metadata cache prune は明示指定がない場合、
payload cache prune の有効/無効に追従する。

remote が設定されている通常の `mj fsck` は、検査のため一時的に hydrate した同期済み
payload cache と tree metadata を成功後に自動 prune する。未同期、または remote に存在しない
object は削除しない。調査目的で検査後の cache を残したい場合は
`MAJUTSU_FSCK_PRUNE_PAYLOAD_CACHE=0` や `MAJUTSU_FSCK_PRUNE_METADATA_CACHE=0` を指定する。

新規に作成される tree manifest は compact JSON として保存される。既存の pretty JSON tree
manifest も引き続き読み取り可能で、restore / diff / fsck の互換性は維持される。

```sh
MAJUTSU_FSCK_PRUNE_PAYLOAD_CACHE=0 mj fsck --since "24h ago" --progress
MAJUTSU_FSCK_PRUNE_METADATA_CACHE=0 mj fsck --since "24h ago" --progress
```

## watch daemon memory mode

watch daemon は既定で snapshot / sync を子 process で実行する。大きな snapshot や
sync 後も長寿命 daemon の RSS を低く保つための設定である。調査目的で旧来の
inline mode を使う場合は次の環境変数を指定する。

```sh
MAJUTSU_WATCH_INLINE_SNAPSHOT=1 mj watch --foreground true
```

## queue backpressure

`mj daemon metrics` は次の値を公開する。

- `majutsu_daemon_queued_uploads`
- `majutsu_daemon_queued_uploads_retrying`
- `majutsu_daemon_queued_uploads_delayed`
- `majutsu_daemon_upload_queue_backpressure`

backpressure が続く場合は、remote credential、network、object store permission、lifecycle / tagging 互換性を確認する。

## stalled pending notice

watch daemon は periodic rescan のタイミングで pending journal の滞留を確認できる。
通知コマンドを連携する場合は `MAJUTSU_STALLED_NOTICE_CMD` を設定する。

```sh
export MAJUTSU_STALLED_NOTICE_CMD='../websh/scripts/notice.sh -t majutsu -m "mj pending journal stalled"'
export MAJUTSU_STALLED_NOTICE_AFTER_SECS=300
export MAJUTSU_STALLED_NOTICE_RATE_LIMIT_SECS=3600
mj daemon start
```

通知コマンドには次の環境変数が渡される。

- `MAJUTSU_HOME`
- `MAJUTSU_PENDING_JOURNAL_COUNT`
- `MAJUTSU_PENDING_OLDEST_AGE_SECS`

通知は `$MAJUTSU_HOME/runtime/stalled-notice.sent` で rate limit される。

## archive restore

```sh
mj restore prepare --at '2026-06-06 10:30:00' --root photos --to /tmp/photos-restore
mj restore resume <restore-job-id>
```

offset を含まない `YYYY-MM-DD HH:MM:SS` は実行環境の local timezone として扱う。
ホスト間で手順を共有する場合は `2026-06-06T10:30:00+09:00` のように
RFC3339 offset を明示する。
相対時刻で復元する場合は `--ago 2h` のように指定できる。operation log 上の
特定操作が作った状態を復元する場合は、曖昧でない prefix を使って
`mj restore plan --op op-e0b88514 --root photos --to /tmp/photos-restore`
のように指定する。

provider が非同期 restore window を必要とする場合は、provider 側の restore 完了を待ってから resume する。

## restore views

restore view 操作は `restore` namespace 配下で実行できる。

```sh
mj restore mount /tmp/majutsu-view
mj restore hydrate /tmp/majutsu-view --root photos --path sample.raw
mj restore unmount /tmp/majutsu-view
```

互換性のため `mj mount`、`mj hydrate`、`mj unmount` も引き続き利用できる。

## branch / timeline operation

```sh
mj branch list
mj branch create recovery-test --at '2026-06-06 10:30:00' --switch --restore --force
mj snapshot --message 'recovery-test branch'
mj branch switch main --restore --force
mj switch main --restore --force
```

working directory を上書きせずに古い branch を確認したい場合は、configured roots への
`--restore` ではなく `--to <dir>` を指定する。

## operation file diff

snapshot operation が管理対象ファイルに与えた差分を確認する。

```sh
mj op log
mj op diff <op-id>
mj op show <op-id> --files
mj op diff <op-id> --root moon
```

`mj op log` は操作単位の履歴、`mj op diff` はその操作の before / after snapshot から
導出した file-level diff を表示する。

`mj op log` と `mj op show` は、変更発生源の推定情報と、操作を記録した
session / process の手掛かりを分けて表示する。通常の `mj log` / `mj op log`
では `origin_*` を優先して表示し、daemon が観測して記録しただけの変更を
`daemon:daemon-pid-...` が編集したようには見せない。

複数の coding agent や terminal が同じ root を操作する場合は、次の環境変数を設定しておくと
op log 上で識別しやすい。

```sh
export MAJUTSU_SESSION_ID=codex-20260615-a
export MAJUTSU_SESSION_LABEL=codex
```

`session_id` は `MAJUTSU_SESSION_ID` を優先し、未設定の場合は Codex / Claude / Cursor /
terminal 系の session 環境変数、最後に記録元 pid へフォールバックする。`session_label` は
`MAJUTSU_SESSION_LABEL` または `MAJUTSU_AGENT_NAME` を優先する。

`process_id` は操作を記録した `mj` process の pid、`process_path` は OS の root process から
その pid に至る枝だけを pid 列として保存する。全 process tree は保存しない。これらは
`recorded_by` 相当の監査情報であり、互換性のため既存の `actor` / `session_*` /
`process_*` フィールドとして保存し続ける。

`origin_label`、`origin_session_id`、`origin_process_id`、`origin_process_path`、
`origin_exe`、`origin_confidence` は実変更者またはその推定値を表す。通常CLI操作では
現在の `mj` process を `origin_confidence=self` として保存する。Linux root daemon では
`fanotify` が既定backendになり、取得できたイベント元pidを `origin_confidence=fanotify`
として保存する。fanotifyが使えない場合は `watch-backend-fallback` を記録してinotifyへ縮退する。
inotify / notify 経由のファイル変更では、kernel の filesystem event から元の editor pid は
通常得られないため、明示的な origin hint がない限り `origin` は不明として表示される。
daemon 自体は `session_label=daemon` と `session_id=daemon-pid-<pid>` で `recorded_by`
として残る。

外部ラッパーが変更元を特定できる場合は、次のhintを渡せる。

```sh
export MAJUTSU_ORIGIN_LABEL=codex
export MAJUTSU_ORIGIN_SESSION_ID=codex-20260623-a
export MAJUTSU_ORIGIN_PID=12345
export MAJUTSU_ORIGIN_EXE=/usr/bin/code
export MAJUTSU_ORIGIN_CONFIDENCE=observed
```


## remote metadata storage efficiency

S3 互換 remote では、index file から埋め込み snapshot manifest を省略した compact
metadata export を使う。詳細は `docs/REMOTE_METADATA_STORAGE.md` を参照する。

## publish compaction

S3/GCS 互換 remote では、最新 head 情報を `hosts/<host-id>/head.cbor.zst.enc`
へ集約し、小変更時の canonical ref publish 数を削減する。詳細は
`docs/PUBLISH_COMPACTION.md` を参照する。
