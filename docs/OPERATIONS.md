# majutsu 運用 runbook

## 日次確認

```sh
mj status
mj state
mj sync status
mj fsck
mj remote check
mj remote fsck
mj daemon status
mj daemon metrics
```

`mj status` は運用上の要点確認、`mj state` は state home の paths、refs、
branch heads、metadata 件数を確認する用途で使い分ける。自動確認では
`mj state --json` を使う。

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

## local payload cache

root 上の実体ファイルと remote backend が復旧用の正である運用では、
`$MAJUTSU_HOME/objects` の payload object は二重保持になりうる。

`mj cache stat` は remote に存在するため安全に削除できる local payload cache を表示する。

```sh
mj cache stat
```

`mj cache prune` は pack 本体、large chunk、loose blob などの payload cache だけを削除する。
snapshot manifest、tree manifest、pack index、large manifest などの metadata は保持する。

```sh
mj cache prune --dry-run
mj cache prune
```

cache prune 後も `mj restore`、`mj hydrate`、`mj mount` は必要に応じて remote から object を
hydrate する。`mj fsck` は remote に存在する payload cache 欠落を正常扱いし、metadata 欠落や
remote 未同期の payload 欠落は引き続き異常として扱う。

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
