# majutsu 性能・効率完了メモ 2026-06-16

このメモは、storage format の大きな変更を伴わずに実施できる残りの性能改善を整理する。

## P1 小変更 sync

現在の設計では、S3/GCS 経路で compact head object を使い、legacy current ref や GC mark の毎回更新を避けている。今回の変更では、さらに次の bounded metadata object を追加した。

```text
hosts/<host-id>/root-size-summary.cbor.zst.enc
```

これは current snapshot の root 別集計で、`mj root size` の cold path が remote prefix 全体を list せずに表示できるようにするためのもの。

同じ current snapshot では内容が変わらないようにし、remote sync cache と組み合わせて no-op sync で毎回再送しない。

## P2 root size cold path

`mj root size` は、local current snapshot と一致する root-size summary が remote にあればそれを優先する。summary が存在しない、古い、または `MAJUTSU_ROOT_SIZE_FORCE_SCAN=1` が指定された場合は、従来の正確な scan にフォールバックする。

summary には root 別の client bytes、payload / metadata bytes、object count、missing local object count を含める。root 数に比例する小さな object なので、履歴全体の長さには依存しない。

## P2 pack / sync RSS

pack と upload の streaming 化は既に実装済み。今後の regression 検出には次を使う。

```sh
scripts/traffic-regression.sh
```

高メモリ条件の確認には platform の `time` を使う。

```sh
/usr/bin/time -v mj sync --wait
```

## P2 subtree / delta tree design

次の storage-format 改良候補は content-addressed subtree reuse。これは restore、diff、fsck、clone の migration path を伴うため、この hotfix には混ぜない。設計メモは `docs/SUBTREE_AND_RESTORE_INDEX_DESIGN.md` に置く。

## P3 restore GET と traffic regression

traffic benchmark script は小さな root を作成し、root add / noop / add / edit / delete の各経路について、`synced`、`synced_bytes`、elapsed time を表示する。

S3/GCS では `MAJUTSU_TRACE_REMOTE=1` を付けると、request count は従来通り stderr に表示される。

`scripts/traffic-regression.sh` は、既定では object 数と転送 bytes の上限を超えた場合に非ゼロ終了する。elapsed は実行環境差が大きいため既定では判定に使わない。必要な場合は `MAJUTSU_TRAFFIC_MAX_ELAPSED_MS` を指定する。

主な調整用環境変数:

```sh
MAJUTSU_TRAFFIC_MAX_ROOT_ADD_SYNCED=400
MAJUTSU_TRAFFIC_MAX_ROOT_ADD_BYTES=500000
MAJUTSU_TRAFFIC_MAX_NOOP_SYNCED=0
MAJUTSU_TRAFFIC_MAX_NOOP_BYTES=0
MAJUTSU_TRAFFIC_MAX_SMALL_SYNCED=80
MAJUTSU_TRAFFIC_MAX_SMALL_BYTES=300000
MAJUTSU_TRAFFIC_MAX_ELAPSED_MS=0
```

履歴を残す場合は TSV 出力先を指定する。

```sh
MAJUTSU_TRAFFIC_REPORT=target/traffic-regression.tsv scripts/traffic-regression.sh
```
