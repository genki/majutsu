# majutsu 運用 runbook

## 日次確認

```sh
mj status
mj sync status
mj fsck
mj remote check
mj remote fsck
mj daemon status
mj daemon metrics
```

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
