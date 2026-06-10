# head root 性能 hardening

`head` root の検証後に追加した運用上の変更点を記録する。

## 高速な sync status

`mj sync status` は日常確認向けの高速表示とし、object ごとの remote 存在確認は
既定では行わない。refs、queue 状態、backpressure、sync lock owner を表示する。
全 object の remote 監査が必要な場合だけ deep mode を使う。

```sh
mj sync status
mj sync status --deep
```

## sync wait target の追従

`mj sync --wait` は待機中に local current snapshot が進んだ場合、最新の
current を待機対象として追従する。daemon が新しい snapshot を作った後に、すでに
local current ではない古い snapshot を待ち続けて timeout する状態を避ける。

## watch daemon のメモリ分離

watch daemon は重い snapshot / sync 処理を既定で子 `mj` process に委譲する。
daemon 本体は watch / event dispatch に集中させ、snapshot や sync 後に大きな
manifest/blob buffer を保持し続ける状態を避ける。旧来の inline path を調査する場合だけ
`MAJUTSU_WATCH_INLINE_SNAPSHOT=1` を設定する。

## event journal counters

daemon status / metrics は journal record 全体と pending record を分けて表示する。

```text
journal_events N
processed_journal_events N
pending_journal_event_count N
pending_journal_events true|false
```

`journal_events` が大きくても `pending_journal_events false` であれば、
未処理 work ではなく履歴として処理済みの観測 record が残っている状態を意味する。
