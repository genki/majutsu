# moon root / S3 互換ストレージ最適化

`~/moon` を実際に root として検証した結果、GCS の S3 互換 endpoint では小さな object が多い場合に request latency が支配的になることが分かった。この文書は、その観測を受けて追加した挙動と調整項目をまとめる。

## root exclude

`/**` で終わる exclude pattern は、配下のファイルだけでなく directory entry 自体も除外する。たとえば `**/.git/**` は `.git` directory record も抑止する。

新規 root では、復旧価値が低く巨大化しやすい再生成物を既定で除外する。対象は VCS 内部
（`.git`、`.jj`、`.hg`、`.svn`）、依存物（`node_modules`、virtualenv）、build output
（`target`、`build`、`dist`、`out`）、代表的な cache / tmp 系である。Git working tree を
root にするだけなら通常は追加指定不要。

完全なファイルシステム像を意図的に保存したい場合だけ、root add 時に既定除外を無効化する。

```sh
mj root add full-root /path/to/root --no-default-excludes
```

moon 固有の sensitive path までまとめて外したい場合は preset を追加する。

```sh
mj root add moon ~/moon --preset git-working-tree
mj root set moon --preset git-working-tree
```

`git-working-tree` preset は既定除外に加え、`.infracost`、`.backup-kubeconfig`、`.kubeconfig*`、`etc/keys` などを除外する。root に代表的な sensitive path があり、対応する exclude が無い場合は warning を表示する。`.env` や kubeconfig のような authored secret は、既定では黙って除外しない。暗号化 remote で保護するか、復旧対象外にする場合だけ明示 exclude を追加する。

## sync

`mj sync` は未 pack の小さな blob が多い場合に、自動的に `mj pack` 相当の処理を実行してから upload queue を作る。これにより remote への PUT / HEAD request 数を減らす。

自動 pack は環境変数で調整できる。

```sh
MAJUTSU_SYNC_AUTO_PACK=0 mj sync
MAJUTSU_SYNC_AUTO_PACK_MIN_BLOBS=512 mj sync
```

sync 後は、current retention metadata から参照されない remote content object を削除する。対象には旧 loose blob object、canonical loose blob alias、pack、pack index、tree node、large object manifest / chunk を含める。削除対象は remote の `gc/marks/` にある全 host の live object set と照合し、別 host が参照している object は削除しない。

ファイルを作業treeから削除しただけの場合、そのファイルは履歴上の復元対象として残る。一方で `mj root set --exclude ...` や include 変更によって majutsu の管理対象から外した場合は、保持中snapshotの該当root metadataからも対象外pathを忘却する。これにより、そのpayloadは通常の履歴retentionを待たずに metadata prune / remote cleanup の削除対象になれる。

この remote cleanup は通常有効で、環境変数で無効化できる。

```sh
MAJUTSU_SYNC_REMOTE_PRUNE=0 mj sync
MAJUTSU_SYNC_REMOTE_OBJECT_PRUNE=0 mj sync
```

remote cleanup の成功後は `~/.majutsu/cache/remote-prune-state.json` に状態を記録し、同じ remote / state fingerprint の no-op sync では backend prefix 全体の list を繰り返さない。強制的に remote cleanup を再実行する場合は次を使う。

```sh
MAJUTSU_SYNC_REMOTE_PRUNE_FORCE=1 mj sync
MAJUTSU_SYNC_REMOTE_PRUNE_INTERVAL_SECS=3600 mj sync
```

cleanup は remote list で存在する content object を絞り込み、S3 upload 並列度の設定を使って delete を並列化する。削除された旧 object は current retention metadata から到達不能なため、通常の clone / restore には不要。

`mj sync` 成功後は、ローカルに pack と pack index が揃っている pack 済み blob の旧 loose file も削除する。これにより auto pack 後の `~/.majutsu` 使用量が blob と pack の二重保持で増え続けることを抑える。pack から読める状態を確認してから削除するため、restore / fsck / remote sync の参照先は維持される。

この local cleanup は環境変数で無効化できる。

```sh
MAJUTSU_SYNC_LOCAL_OBJECT_PRUNE=0 mj sync
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
