# 検証用 root 性能測定と改善 2026-06-10

## 目的

環境保全用途で多数の小ファイルを管理する場合に、`mj snapshot` と `mj sync` が実用的な速度で完了するかを確認した。検証用 root は `/tmp` 配下に作成し、通常の `~/.majutsu` には触れない一時 home と file remote を使った。

## 検証条件

- 検証 root: 50 ディレクトリ x 40 ファイル、合計 2000 小ファイル
- 各ファイル: 約 530 バイト
- backend: `file://` remote
- binary: `target/release/mj`
- 測定: `/usr/bin/time`

## 初期測定

最初の実装では以下のように壁時計時間が大きかった。

| 処理 | 経過時間 | 備考 |
| --- | ---: | --- |
| snapshot | 32.27 秒 | 小 blob ごとに DB open と insert が発生 |
| sync | 20.39 秒 | auto pack 経路で DB 更新が blob 単位の autocommit |
| pack 単体 | 18.79 秒 | 2000 object を 1 pack にまとめるだけで遅い |

CPU 使用率が低く、I/O 待ちと SQLite commit 待ちが支配的だった。

## 修正内容

### snapshot の blob 登録一括化

`scan_root` で小さい通常 blob を見つけるたびに `open_db()` と `insert into blobs` を実行していた。これを `BlobInsert` として scan 結果に積み、snapshot 作成時の既存 transaction 内で一括 insert するように変更した。

これにより snapshot は 32.27 秒から約 1 秒まで改善した。

### pack metadata 更新の transaction 化

`finish_pack` で pack ファイルを書き出した後、`packs` への insert と `blobs` の pack 参照更新を object ごとに autocommit していた。pack/index ファイルの書き出しと DB 更新を分離し、`persist_written_packs` で 1 transaction にまとめた。

これにより 2000 object の pack 単体は 18.79 秒から 0.40 秒まで改善した。

### local object/file remote の fsync 過多の抑制

local object 書き込みと file remote 書き込みで `sync_all()` を毎回実行していた。小ファイル多数の検証ではこれも壁時計時間を悪化させるため、以下の環境変数で opt-in する挙動に変更した。

- `MAJUTSU_FSYNC_OBJECTS=1`
- `MAJUTSU_FSYNC_REMOTE_FILE=1`

S3 backend はこの file remote の挙動変更とは別経路で動作する。

## 改善後測定

| 処理 | 経過時間 | user | sys | CPU | max RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| snapshot | 1.01 秒 | 0.69 秒 | 0.30 秒 | 98% | 17324 KB |
| sync auto-pack | 0.78 秒 | 0.57 秒 | 0.13 秒 | 90% | 39148 KB |
| pack 単体 | 0.40 秒 | 0.29 秒 | 0.08 秒 | 93% | 13816 KB |
| sync 単体(pack 済み) | 0.43 秒 | 0.26 秒 | 0.09 秒 | 83% | 42364 KB |

sync auto-pack の出力例:

```text
auto_pack unpacked_small_blobs 2000
packed 2000 objects into 1 pack(s)
synced 34 objects to file:///tmp/majutsu-perf-sync-opt.gFVssX/remote
pruned_remote_exports 0
pruned_remote_objects 0
pruned_local_objects 2000
```

## 検証

- `cargo check --locked`
- `cargo test --locked --test e2e_local multi_root_snapshot_sync_clone_restore_file_remote`
- `cargo test --locked`

## 残る注意点

- file remote を実バックアップ先として使う場合、`MAJUTSU_FSYNC_REMOTE_FILE=1` を指定するとより保守的になる。ただし小ファイル多数では遅くなる。
- 今回の測定は `/tmp` 上の file remote であり、S3/GCS 互換 backend では通信レイテンシ、multipart 設定、bucket 側 throttling を別途測る必要がある。
- pack/index ファイル書き出し後、DB 更新前にプロセスが落ちると未参照 pack が残り得る。参照前の孤立 object と同じく `fsck`/`gc` で扱う対象として整理できる。
