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

`mj sync status` と `mj sync --wait` は compact head を優先して読み、存在しない古い
remote では従来の canonical ref にフォールバックする。

## ref object の扱い

file remote では従来通り canonical ref を毎回 publish する。S3/GCS 互換 backend では
compact head を正とし、canonical current / last-synced ref の毎回 publish は既定で省く。

互換確認や移行時に従来挙動へ戻したい場合は次を使う。

```sh
MAJUTSU_SYNC_REMOTE_REF_OBJECTS=1 mj sync
MAJUTSU_SYNC_LEGACY_CURRENT_REFS=1 mj sync
```

## GC mark

GC mark は remote prune の安全性に関わるため、この段階では毎 sync publish を維持する。
低頻度化・差分化は別の設計課題として扱う。
