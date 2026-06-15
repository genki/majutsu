# publish compaction

2026-06-12 の通信効率レビューで確認した、小変更同期時の固定 request 数を削るための設計メモ。

## 背景

build 11 時点で、小さなファイル追加・編集・削除の転送量は約 9 KiB まで下がった。一方で
S3/GCS 互換 backend では小さな metadata/ref object の PUT が複数残り、同期完了時間は
GCS request latency に支配されていた。

## compact head object

S3/GCS 互換 backend では、最新 head 情報を次の単一 object として publish する。

```text
hosts/<host-id>/head.cbor.zst.enc
```

この object には current snapshot、last-synced、host metadata key、host index key、
GC mark key、最新 snapshot/operation export key を含める。
あわせて current snapshot に含まれる root 別の `tree_id`、`tree_key`、`file_count`、
`synced_at` を `root_acks` として同梱する。これにより host単位 current が同じでも、
root 別にどの tree が remote 側で保全済みかを追加 request なしで確認できる。

`mj sync status` と `mj sync --wait` は compact head を優先して読み、存在しない古い
remote では従来の canonical ref にフォールバックする。
compact head から読んだ root ack は local `remote_refs` cache に保存され、`mj status` と
`mj health` は通常 remote へ通信せずに root 別 remote 同期状態を表示する。
`mj remote fsck` と `mj remote fsck --deep` は compact head の `root_acks` を current snapshot
manifest の root tree と照合し、head 自体の破損や古い root ack を検出する。

## ref object の扱い

file remote では従来通り canonical ref を毎回 publish する。S3/GCS 互換 backend では
compact head を正とし、canonical current / last-synced ref の毎回 publish は既定で省く。

互換確認や移行時に従来挙動へ戻したい場合は次を使う。

```sh
MAJUTSU_SYNC_REMOTE_REF_OBJECTS=1 mj sync
MAJUTSU_SYNC_LEGACY_CURRENT_REFS=1 mj sync
```

## GC mark

S3/GCS 互換 backend では compact head を通常復旧の正とするため、GC mark は初回だけ seed し、
通常の小変更 sync では毎回 publish しない。GC mark は remote prune の保護集合として使うため、
remote prune を実行する同期では最新状態を強制 publish してから prune する。

GC mark を互換確認などで毎回更新したい場合は次を使う。

```sh
MAJUTSU_SYNC_GC_MARK_EVERY_TIME=1 mj sync
```

file remote は従来通り、GC mark を毎 sync publish し、fsck でも current snapshot と live object
集合の一致を厳密に検証する。S3/GCS 互換 backend では compact head が存在する場合、古い GC mark
は remote prune 用の補助情報として扱い、version、host id、重複 key などの構造破損だけを検出する。
