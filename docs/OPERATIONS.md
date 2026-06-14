# majutsu 運用 runbook

## 日次確認

```sh
mj status
mj state
mj sync status
mj fsck --quick
mj remote check
mj remote fsck
mj daemon status
mj daemon metrics
```

`mj status` は運用上の要点確認、`mj state` は state home の paths、refs、
branch heads、metadata 件数を確認する用途で使い分ける。自動確認では
`mj state --json` を使う。

`mj status` の先頭には remote head の同期状態が表示される。`Remote head synced`
なら local current snapshot が最後に確認した remote current ref と一致している。
`lagging`、`not synced`、`remote unavailable` の場合は `mj sync status` と `mj sync --wait`
で詳細確認と追従を行う。

`mj status` は出力が端末高さを超える場合に pager を使う。ログ採取や cron などで
明示的に標準出力へ出したい場合は `--no-pager`、短い出力でも pager で確認したい場合は
`--pager` を使う。

```sh
mj status --no-pager
mj status --pager
```

`Local Storage` は apparent size と disk usage を分けて表示する。小ファイル多数の
event journal や queue では、apparent size が小さくても disk usage が大きくなることがある。

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

日次の軽量確認では `--quick` を使う。

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

```sh
mj fsck --deep --progress --timeout-secs 300
mj fsck --deep --sample 1000 --progress
mj fsck --deep --since "24h ago" --progress
```

`--timeout-secs` で停止した場合は state 破損を意味しない。進捗の遅い phase を確認し、
必要なら大きい timeout で再実行する。

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

deep status は参照 object ごとに remote 存在確認 request を発行しうるため、
監査目的の場合だけ使う。

```sh
mj sync status --deep
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

remote が設定されている通常の `mj fsck` は、検査のため一時的に hydrate した同期済み
payload cache と tree metadata を成功後に自動 prune する。未同期、または remote に存在しない
object は削除しない。調査目的で検査後の cache を残したい場合は
`MAJUTSU_FSCK_PRUNE_PAYLOAD_CACHE=0` や `MAJUTSU_FSCK_PRUNE_METADATA_CACHE=0` を指定する。

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

provider が非同期 restore window を必要とする場合は、provider 側の restore 完了を待ってから resume する。


## branch / timeline operation

```sh
mj branch list
mj branch create recovery-test --at '2026-06-06 10:30:00' --switch --restore --force
mj snapshot --message 'recovery-test branch'
mj branch switch main --restore --force
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


## remote metadata storage efficiency

S3 互換 remote では、index file から埋め込み snapshot manifest を省略した compact
metadata export を使う。詳細は `docs/REMOTE_METADATA_STORAGE.md` を参照する。

## publish compaction

S3/GCS 互換 remote では、最新 head 情報を `hosts/<host-id>/head.cbor.zst.enc`
へ集約し、小変更時の canonical ref publish 数を削減する。詳細は
`docs/PUBLISH_COMPACTION.md` を参照する。
