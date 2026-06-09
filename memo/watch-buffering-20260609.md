# watch event buffering improvement 2026-06-09

## 背景

`mj` は厳密なリアルタイム性より、remote通信量とstorage利用効率を重視する。
従来のwatch実装はfilesystem eventを受けるたびに短いdebounce/settle後にsnapshotし、
小さな変更でもmetadata exportやpack/ref更新に由来するupload queueが数百件出ることがあった。

## 実装方針

固定ウィンドウではなく、sliding quiet windowでoperationをまとめる。

- event発生時にbufferへ追加する。
- eventが追加されるたびにquiet timerをリセットする。
- 次のいずれかでflushし、1回のwatch snapshotを作る。
  - `debounce_ms + settle_ms` の無操作時間が経過した。
  - `buffer_max_ms` に到達した。
  - `buffer_max_events` に到達した。
- `strict` modeは従来通りeventごとにsnapshotする。
- `poll` backendは従来通りinterval snapshotで、buffering対象外。

## 追加設定

`[watch]` に以下を追加した。既存configにはdefaultが適用される。

```toml
buffer_max = "60s"
buffer_max_events = 1000
```

CLIでも指定できる。

```sh
mj watch --buffer-max-ms 60000 --buffer-max-events 1000
mj daemon start --buffer-max-ms 60000 --buffer-max-events 1000
```

`settle_ms` は互換性のため残し、sliding quiet windowへ合算する。
既定値では `debounce_ms=1500`、`settle_ms=500` なので、実際のquiet時間は2秒。

## 実測

検証用home/root/remoteのみを `/tmp/majutsu-buffer-test.*` に作成して確認した。

```text
watch:
  backend=inotify
  once=true
  debounce_ms=300
  settle_ms=0
  buffer_max_ms=5000
  buffer_max_events=100

操作:
  5ファイルを100ms間隔で作成

結果:
  snapshot files 5
  mj log: file-events-batch A:5 M:0 D:0
  watch-buffer-flush: reason=quiet events=10 elapsed_ms=728
  sync status: local_current == remote_current
  missing_remote_objects 0
  queued_uploads 0
```

`events=10` はinotifyが1ファイル作成にcreate/modifyなど複数eventを出すため。
最終operationはファイル状態として5件の追加に畳み込まれている。

## 確認したテスト

```sh
cargo check --locked
cargo test --locked
cargo test --locked --bin mj watch_runtime
cargo test --locked --test remote_file watch
cargo test --locked -p majutsu-watch -p majutsu-daemon
```

## 残る改善余地

- 小変更時にsnapshot後のupload queueがまだ数百件になる場合がある。
  今回の変更はoperation数を減らすもので、metadata/pack/refのqueue粒度は別途改善が必要。
- daemon稼働中にrootを追加した時のwatch再登録問題は今回の範囲外。
- `mj status` にbuffer中の件数、最古event時刻、flush予定理由を出すと運用しやすい。
