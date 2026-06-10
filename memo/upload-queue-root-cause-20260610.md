# S3 upload queue 肥大化の原因調査 2026-06-10

## 結論

小変更でも upload queue が大きくなる主因は、`mj sync` が差分同期ではなく「現時点の全 export と全 live object の再 queue 化」を行う設計になっていること。S3 backend では既存 object への `put_if_absent` も小さい object では body を送ってから `412/409` を受けるため、既存 object でも通信負荷が残る。

加えて、queue item の inline payload が JSON の数値配列として保存されるため、実 payload より queue 上の JSON が数倍大きくなる。暗号化時の canonical export は age のランダム暗号化で毎回 bytes が変わるため、同じ key でも inline payload が再生成される。

## 実装上の該当箇所

- `src/sync_runtime.rs`
  - `enqueue_and_drain_sync` が毎回 `export_metadata` 全体を作り直す。
  - 全 snapshot / 全 operation / host metadata / oplog / gc mark / chunk index shard を毎回 `enqueue_inline_upload` する。
  - `local_object_keys(&export)` の全 key と canonical alias を毎回 queue に積む。
- `src/queue_runtime.rs`
  - `UploadQueueItem.inline: Vec<u8>` を `serde_json::to_vec_pretty` で保存するため、大きい inline payload が JSON 数値配列になる。
- `src/remote_store.rs`
  - S3 multipart の `put_if_absent` は事前 `exists()` で送信を避ける。
  - 小さい object の `put_if_absent` は `If-None-Match: *` 付き PUT で body を送る。
- `crates/majutsu-crypto`
  - age encryption は非決定的なので、同じ plaintext でも canonical inline export の ciphertext は毎回異なる。

## 再現

通常の `~/.majutsu` に触れない一時 home/root/remote で確認した。

- 初期状態: 500 小ファイル
- 初回 sync: 34 objects
- 1 ファイルだけ変更
- remote を意図的に失敗する file path に変更して、enqueue 後に drain で停止させた

結果:

| 種別 | 件数 | payload bytes |
| --- | ---: | ---: |
| host metadata/ref | 6 | 743,984 |
| metadata/export.json | 1 | 743,434 |
| host snapshot export | 4 | 650,715 |
| canonical/legacy loose blob | 6 | 1,031,902 |
| legacy tree | 2 | 504,419 |
| canonical tree | 2 | 79,493 |
| legacy/canonical pack | 2 | 315,752 |
| legacy/canonical pack index | 2 | 89,666 |
| host op export | 16 | 9,351 |
| その他 | 5 | 4,377 |

queue item は合計 46 件で、payload 合計は数 MiB 規模だが、JSON ファイル合計は 18,665,653 bytes になった。inline bytes を JSON 数値配列にしているため、payload より大きく膨らむ。

## 実環境での観測

調査時点の live object set は 79 key、約 103 MiB。

| 種別 | 件数 | サイズ |
| --- | ---: | ---: |
| loose blob | 49 | 41.5 MiB |
| large chunk | 3 | 22.7 MiB |
| pack | 4 | 22.1 MiB |
| tree | 18 | 13.5 MiB |
| pack index | 4 | 0.4 MiB |
| large manifest | 1 | 1.5 KiB |

一方で `queue/events` は 25,912 件残っており、論理サイズは約 8.4 MiBだが小ファイル多数のため disk usage は約 104 MiB だった。処理済み event journal を削除または圧縮する仕組みがない。

## 改善方針

優先度順:

1. `UploadQueueItem.inline` を JSON 数値配列ではなく、queue payload file 参照にする。小さい inline metadata も `queue/payloads/<id>` に bytes として保存し、JSON には key/source/attempts だけを置く。
2. remote refs / local remote cache を使い、既に remote にある content-addressed key と canonical alias は enqueue しない。
3. S3 の `put_if_absent` は小さい object でも必要に応じて事前 `HEAD` を使い、既存 key への body upload を避ける。HEAD 数と PUT body の trade-off は backend latency と object size で閾値化する。
4. snapshot / operation export は新規分だけ immutable key に enqueue し、host metadata / refs / oplog など可変 key だけ毎回更新する。
5. age 暗号化済み canonical export は同一 plaintext key に対して再生成しない cache を持つ。key rotation 時だけ無効化する。
6. 処理済み event journal は snapshot-finish より古い file event を compact する。
7. `mj sync` に enqueue 件数、payload bytes、uploaded/skipped、現在 key を表示する進捗出力を追加する。

## 実施した改善

2026-06-10 に以下を実装した。

- 新規 inline upload queue は、JSON の `inline: [byte, ...]` に payload を直接入れず、`queue/upload-payloads/<upload-id>.bin` に bytes として保存し、queue JSON は `source` 参照にする。
- 既存の inline JSON queue は後方互換として読み続ける。
- upload 成功時に対応する `queue/upload-payloads/<upload-id>.bin` を削除する。
- file source 型の queue item を再 enqueue するときは、同じ key の古い payload file を削除する。
- content-addressed remote key と canonical alias は、remote に既に存在する場合は best-effort で enqueue を省略する。
- host snapshot / operation export は同じ ID でも key rotation や operation status 更新で内容が変わり得るため、skip 対象から外した。

## 改善後の効果

500 小ファイルを同期済みにした後、1 ファイルだけ変更する条件で確認した。

remote 失敗時に queue を残すケース:

| 指標 | 改善前 | 改善後 |
| --- | ---: | ---: |
| queue item | 46 | 46 |
| upload queue JSON 合計 | 18,665,653 bytes | 18,638 bytes |
| payload 保存 | JSON inline | 2,253,548 bytes |
| upload queue disk usage | 18 MiB 超 | 188 KiB + payload 2.3 MiB |

reachable file remote で 1 ファイル変更を同期するケース:

| 指標 | 改善前 | 改善後 |
| --- | ---: | ---: |
| synced objects | 46 | 38 |
| sync elapsed | 未測定 | 0.58 秒 |

queue JSON 肥大化はほぼ解消した。重複 upload は content-addressed object について削減したが、host metadata、snapshot export、operation export、oplog、refs は可変 key として毎回更新するため、完全な差分同期にはまだなっていない。

## 検証

- `cargo check --locked`
- `cargo test --locked --test remote_file`
- `cargo test --locked`

## event journal compact と sync 進捗 2026-06-10

追加対応:

- `compact_event_journal` を追加し、snapshot 完了後に best-effort で実行する。
- デフォルトでは event journal が 1024 件以下なら compact しない。少数イベントを調査・テストで確認しやすくするため。
- 閾値は `MAJUTSU_EVENT_COMPACT_MIN_RECORDS` で変更可能。
- compact 対象は最新 `snapshot-finish` より古い event。pending 判定に必要な最新 snapshot 完了後の `fs-event` / `periodic-rescan` は残る。
- `drain_upload_queue` に stderr 進捗を追加した。16 件以上の queue で開始時、25 件ごと、または 5 秒ごと、完了時に表示する。

検証:

- 一時 home で 202 件の event を作成し、`MAJUTSU_EVENT_COMPACT_MIN_RECORDS=10` の snapshot 後に 1 件まで compact されることを確認。
- 同じ検証で `mj sync` が `sync upload progress 0/68 ...`、`25/68`、`50/68`、`68/68 done` を stderr に出すことを確認。
- 実環境では `queue/events` が 38,720 件 / 155 MiB から、snapshot 後に 1 件、daemon 再起動後の確認で 4 件 / 3.1 MiB になった。
- `cargo test --locked` は成功。

## 注意

今回の snapshot/pack SQLite autocommit 改善とは別系統の問題。ローカル snapshot/pack は改善済みだが、remote sync はまだ「全体を再 queue 化して直列 upload する」ため、S3 backend では小変更でも時間がかかる。
