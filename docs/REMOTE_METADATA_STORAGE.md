# remote metadata storage efficiency

Majutsu は snapshot payload を content-addressed object として保存する。S3/GCS 互換
remote では、remote URL の path を Majutsu 用 remote root とし、その直下に
`<host-id>/...` を配置する。host-id は remote 側の最上位境界であり、payload object や
metadata はこの境界を越えて共有しない。

現在の remote layout では、host ごとの metadata と payload は次のように保存する。

- `<host-id>/metadata/export.json.zst`
- `<host-id>/head.cbor.zst.enc`
- `<host-id>/refs/...`
- `<host-id>/snapshots/...`
- `<host-id>/ops/...`
- `<host-id>/objects/...`
- `<host-id>/blobs/...`
- `<host-id>/trees/...`
- `<host-id>/packs/...`
- `<host-id>/large/...`
- `<host-id>/gc/...`

履歴が長い場合、payload data が小さくても metadata が remote storage の大半を
占める。compact remote metadata layout では、これらの file に `manifest_key` などの
index field は残すが、S3 互換 remote では埋め込み `manifest_json` を省略する。
full snapshot manifest は `<host-id>/objects/` 以下の content-addressed object を正とする。

## 期待される効果

upgrade 後の次回 `mj sync` で host metadata file は compact 版で上書きされる。
per-snapshot export file も compact に再生成される。既存の content-addressed manifest
object は復元時の正本なので保持する。

旧 sync cache は format version 付き fingerprint で無効化されるため、論理 timeline が
変わっていなくても、この変更後の初回 sync で対象 metadata object を再書き込みする。

## 互換性

- file remote は単純な offline inspection のため full metadata format を維持する。
- S3 互換 remote は既定で compact metadata を使う。
- `mj clone` は SQLite へ metadata を import する前に、各 `manifest_key` object を
  download して compact snapshot metadata を hydrate する。
- validation は、意図的に `manifest_json` が空の compact snapshot export を許容する。

## 確認コマンド

```sh
mj sync
mj remote check
mj remote fsck
```

既存の大きな remote では、upgrade 後に `mj sync` を一度実行すれば、重複していた
metadata / index object が compact 版に書き換わる。
