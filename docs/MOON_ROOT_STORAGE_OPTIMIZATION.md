# moon root / S3 互換ストレージ最適化

`~/moon` を実際に root として検証した結果、GCS の S3 互換 endpoint では小さな object が多い場合に request latency が支配的になることが分かった。この文書は、その観測を受けて追加した挙動と調整項目をまとめる。

## root exclude

`/**` で終わる exclude pattern は、配下のファイルだけでなく directory entry 自体も除外する。たとえば `**/.git/**` は `.git` directory record も抑止する。

Git working tree を root にする場合は preset を使う。

```sh
mj root add moon ~/moon --preset git-working-tree
mj root set moon --preset git-working-tree
```

`git-working-tree` preset は `.git`、`node_modules`、`target`、`tmp`、`.infracost`、`.backup-kubeconfig`、`.kubeconfig*`、`etc/keys` を除外する。root に代表的な sensitive path があり、対応する exclude が無い場合は warning を表示する。

## sync

`mj sync` は未 pack の小さな blob が多い場合に、自動的に `mj pack` 相当の処理を実行してから upload queue を作る。これにより remote への PUT / HEAD request 数を減らす。

自動 pack は環境変数で調整できる。

```sh
MAJUTSU_SYNC_AUTO_PACK=0 mj sync
MAJUTSU_SYNC_AUTO_PACK_MIN_BLOBS=512 mj sync
```

S3 multipart upload の part size は endpoint に応じて選ぶ。

- MinIO / localhost: 16 MiB
- cloud S3 互換 endpoint: 64 MiB
- 10,000 part 上限を超えないように必要なら自動的に引き上げる

調整する場合は次を使う。

```sh
MAJUTSU_S3_MULTIPART_PART_SIZE=$((32 * 1024 * 1024)) mj sync
MAJUTSU_S3_MAX_MULTIPART_PARTS=10000 mj sync
```

## remote check / fsck

S3 `ListObjectsV2` の pagination を処理するため、`mj remote check` の object 数は 1000 件境界で止まらない。

`mj remote fsck` は quick mode をデフォルトにし、metadata と object existence を確認する。payload の decode / hash 検証まで行う場合は明示的に deep mode を使う。

```sh
mj remote fsck
mj remote fsck --deep
```

quick mode は通常運用の疎通確認向け、deep mode は release 検証、定期監査、remote corruption が疑われる場合向けとする。
