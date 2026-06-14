# majutsu 実装状況確認 2026-06-14

## 結論

`majutsu` はローカル completion gate と MinIO S3 互換 E2E を通過しており、仕様実装の主要機能は概ね揃っている。

一方で、実環境 `/home/vagrant/.majutsu` の運用状態を見ると、daemon が stale pid 状態で止まっており、active root が watch daemon に保護されていない。さらに local state の tree metadata が大きく、`mj fsck` が大規模 state で数分以上無出力のまま完了しなかった。完成宣言に近い実装ではあるが、実運用品質としてはこの 3 点を優先課題として扱う。

## 確認したバージョンと差分

- `mj 0.3.0+build.20`
- 作業ツリーには未コミット差分がある。
  - `BUILD_NUMBER`
  - `docs/OPERATIONS.md`
  - `src/cli.rs`
  - `src/fsck_runtime.rs`
  - `src/remote_runtime.rs`
  - `src/watch_runtime.rs`
  - `tests/e2e_local.rs`

差分の主な内容:

- `mj remote fsck --deep` に `--sample` / `--timeout-secs` / `--payload-only` を反映。
- pending journal 滞留時の notice command 実行を追加。
- `BUILD_NUMBER` を 20 に更新。

## 通過した検証

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
scripts/check-completion.sh
scripts/e2e-minio.sh
```

結果:

- `cargo fmt` 成功。
- `cargo clippy -D warnings` 成功。
- `cargo test --workspace --all-targets --locked` 成功。
- `scripts/check-completion.sh` 成功。
- `scripts/e2e-minio.sh` 成功。

`scripts/check-completion.sh` は `MAJUTSU_RUN_MINIO_E2E=1` を付けない場合、MinIO E2E をスキップする。今回は別途 `scripts/e2e-minio.sh` を実行して通過を確認した。

## 実装済みと判断できる主な機能

- `mj --help` はサブコマンド名だけでなく、用途説明と基本フローを表示する。
- `mj status` は端末幅を考慮し、概要、daemon、remote、設定、root、metadata、storage、queue をグループ表示する。
- `mj log` は通信ログではなく、管理対象 root のファイル追加、変更、削除の実変化ログを表示する。
- Linux では watch backend が inotify 既定になっている。
- `root add` や `watch` は daemon 起動を試みるテストがあり、自動起動の基本挙動は検証済み。
- file remote と MinIO S3 互換 remote の sync / fsck / clone / restore E2E は通過している。
- 暗号化 state の key export / clone / restore E2E は通過している。
- branch switch / restore 系の E2E は通過している。
- prune / gc が current restore を壊さない E2E は通過している。
- local payload cache prune と restore hydrate の E2E は通過している。

## 実環境 `/home/vagrant/.majutsu` の状態

`mj status --home /home/vagrant/.majutsu` の要点:

- root 数: 6
  - `head`
  - `help`
  - `jotter`
  - `moon`
  - `stackchan`
  - `websh`
- remote: GCS S3 互換 backend 設定済み。
- encryption: `age`
- queued uploads: 0
- `mj sync status` では `local_current` と `remote_current` が一致。
- daemon: `stale pid`
- event journal: 406 件、pending 1 件。
- local state usage: 約 747.7 MiB。
- local objects: 約 728.5 MiB。
- tree metadata: 約 714.1 MiB。

同期状態は正常に見えるが、daemon が停止しているため、今後のファイル変更が自動 snapshot / sync されない状態になっている。

## 優先課題

### 1. daemon stale pid の根本対策

現状:

- `mj status` は `Daemon stale pid` と表示し、異常に気付ける。
- しかし、実環境では実際に daemon が止まっていた。
- queued upload は 0 なので過去の同期は完了しているが、停止後の変更は保護されない。

改善案:

- `mj status` に daemon 復旧コマンドを明示するだけでなく、必要なら `mj daemon doctor` / `mj daemon restart` を追加する。
- stale pid 検出時に stale pid file / socket を安全に掃除できる処理を daemon start 側に集約する。
- systemd user service などの supervisor 前提の導入手順を completion / operations に含める。
- daemon 終了理由を残すため、watch daemon の stdout/stderr と panic を runtime log に保存する。
- `root add` の自動起動だけでなく、長時間稼働後の restart / crash recovery E2E を追加する。

### 2. local tree metadata の肥大

現状:

- 実環境 state は約 747.7 MiB。
- そのうち tree metadata が約 714.1 MiB を占める。
- root 実体ファイルがローカルにある前提では、ローカル state に巨大な tree manifest を長期保持し続ける設計は効率面で疑問が残る。

推定原因:

- snapshot 数が 1462 あり、tree manifest が snapshot ごとに大きく残っている。
- 小変更でも大きな root tree を再 materialize している可能性がある。
- pack payload はローカル prune 済みだが、tree metadata は同等の compaction 対象になっていない。

改善案:

- tree manifest を content-addressed にし、同一 subtree を snapshot 間で再利用する。
- root tree 全体ではなく変更 subtree と parent pointer で表現する差分 tree を検討する。
- current / recent restore に必要な metadata と、remote から再取得可能な古い metadata を分離し、古い tree manifest を local prune 対象にする。
- `mj status` に tree metadata の肥大警告と、推奨 compaction コマンドを表示する。

### 3. `mj fsck` の大規模 state での進捗不足

現状:

- `/home/vagrant/.majutsu` に対して `mj fsck` を実行したところ、数分以上無出力で完了しなかったため中断した。
- テスト上の `fsck` は通っているが、実運用サイズでは進捗や時間見積もりがなく、止まっているのか重いだけなのか判断しづらい。

推定原因:

- `fsck` が `export_metadata`、local object 検査、blob/chunk/large manifest 検査、snapshot/tree manifest 検査、pack 検査を一括で実行する。
- tree metadata が大きい実環境では snapshot/tree manifest 検査が律速になりやすい。

改善案:

- `mj fsck --quick` を既定にし、DB/ref/queue/current snapshot 周辺を短時間で確認する。
- payload hash / 全 snapshot tree 検査は `mj fsck --deep` に分離する。
- phase ごとの進捗表示、件数、経過秒、現在検査対象を stderr に出す。
- `--timeout-secs` / `--sample` / `--since` を local fsck にも追加する。
- `mj status` から実行される軽量 health check と、full fsck を明確に分ける。

## 中優先課題

### provider matrix の表現整理

`docs/PROVIDER_MATRIX.md` の GCS S3-compatible endpoint は `verified 2026-06-13` の証跡がある一方、Status 欄に `experimental until provider drill` という文言が残っている。release 判定時に supported / experimental の扱いが曖昧になるため、現在 release での扱いを明確にする。

### completion gate の重複

`scripts/check-completion.sh` は `cargo test --workspace --all-targets` の後に `cargo test --test e2e_local` を再実行する。証跡としては分かりやすいが、時間は重複する。release gate と開発時 fast gate を分けてもよい。

### 未コミット差分の整理

現在の未コミット差分には、remote fsck 改善、stalled notice、build 番号更新が混在している。実装単位としては分けて commit するのが望ましい。

## 次に実施すべき順序

1. daemon stale pid の復旧と、停止原因を runtime log に残す実装を追加する。
2. local tree metadata の肥大原因を測定し、tree manifest の再利用または compaction 方針を決める。
3. `mj fsck` を quick/deep に分離し、進捗表示と timeout を追加する。
4. provider matrix の GCS status を整理する。
5. 未コミット差分を実装単位で commit する。

## 改善実施 2026-06-14

実施済み:

- `mj daemon doctor` を追加し、daemon health、pid file、socket、log path、復旧 action、直近 log を表示するようにした。
- `mj daemon restart` を追加し、running daemon の停止、stale pid/socket の掃除、再起動を一つの操作で行えるようにした。
- `mj daemon start` が stale pid を見つけた場合、失敗せず runtime pid/socket を掃除して起動できるようにした。
- daemon 起動時に `$MAJUTSU_HOME/logs/majutsu.log` へ起動記録を残すようにした。
- `mj fsck --quick` を追加し、DB、refs、queue、operation log、config drift などの軽量確認を短時間で実行できるようにした。
- `mj fsck --progress` と `--timeout-secs` を追加し、大規模 state の full check が長時間無出力にならないようにした。
- `remote repair` が記録する `remote-repair` operation kind を core model の正規 kind に追加した。
- `docs/OPERATIONS.md` に local fsck と daemon recovery の運用手順を追記した。

検証:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

補足:

- 1 回目の全体テストで `restore_resume_uses_s3_range_get_for_packed_blobs` がローカル HTTP mock の `Connection reset by peer` で失敗したが、単独再実行では成功した。
- その後、全体テストを再実行して成功した。

残り:

- tree metadata 肥大そのものは未解決。今回の `fsck --quick` で運用確認は短時間化したが、local state の 700 MiB 級 tree metadata を減らすには subtree reuse / metadata compaction の設計実装が必要。
- GCS provider matrix の表現整理は未実施。
- 未コミット差分の整理と commit / push は未実施。

## 実環境反映 2026-06-14

`cargo install --path . --locked --force` で `/home/vagrant/.cargo/bin/mj` を build 21 に更新した。

```text
mj 0.3.0+build.21
```

実環境 `/home/vagrant/.majutsu` で確認した状態:

- `mj fsck --quick --progress` は約 8 秒で成功。
- `mj daemon restart` で stale pid を掃除し、daemon を起動。
- daemon status は `running pid ... ipc ok`。
- restart 後、pending journal が replay され、新しい snapshot `snap-be6b72ea-35e3-48d8-9bb4-48721f0b9e36` が作成された。
- `mj sync --wait --timeout-secs 300` を実行し、remote current を同 snapshot まで進めた。
- 最終 `mj sync status` は `local_current` と `remote_current` が一致し、`queued_uploads 0`。
- 最終 `mj daemon status` は `pending_journal_event_count 0`、`upload_queue_backpressure false`。

注意:

- daemon replay snapshot 直後に確認した時点では remote current が古かった。watch daemon は snapshot 後に外部 sync を呼ぶ実装だが、今回の作業では手動 `mj sync --wait` で保全状態を明示的に戻した。
- 実環境 state は replay 後に約 1.1 GiB へ増えた。large chunks が local に再作成されており、tree metadata も約 724 MiB のまま大きい。local metadata / payload cache 削減は引き続き課題。

## 残課題改善 2026-06-14

追加実施:

- `mj cache stat --metadata` / `mj cache prune --metadata` を追加した。
- 既定の `cache prune` は従来通り payload cache だけを対象にし、`--metadata` 指定時だけ remote 同期済み tree manifest を削除対象にする。
- pruned tree manifest は `read_object` 経由の restore / diff / fsck 時に remote から on-demand hydrate される。
- cache prune 出力を payload / metadata の件数と bytes に分離した。
- GCS provider matrix の Status 欄を `verified 2026-06-13` に整理し、archive restore は `experimental` のままとした。
- build number を 22 に更新した。

検証:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

1 回目の全体テストで `remote_check_uses_s3_range_get_probe` がローカル HTTP mock の connection close で失敗したが、単独再実行で成功し、全体テスト再実行も成功した。

実環境 `/home/vagrant/.majutsu` への反映:

```text
mj 0.3.0+build.22
metadata_cache_candidates 285
metadata_cache_bytes 798951793
removed_metadata_cache_objects 285
removed_metadata_cache_bytes 798951793
```

削減結果:

- prune 前 state usage: 796.9 MiB
- prune 後 state usage: 34.9 MiB
- tree manifest local cache: 761.9 MiB / 285 files から 0 B / 0 files
- `mj fsck --quick`: 成功
- `mj sync status`: `local_current` と `remote_current` が `snap-22b19b5d-dbf3-4b4a-a74b-a002458400e6` で一致
- `mj daemon status`: running、pending journal 0、queued upload 0

残り:

- subtree reuse / 差分 tree 表現は未実装。ただし、現実に問題になっていた local tree metadata の二重保持は metadata cache prune で解消した。
- full `mj fsck` は metadata prune 後に remote hydrate を行うため、監査時には時間と通信が発生する。日次確認は `mj fsck --quick` を使う。

## 再確認 2026-06-14 build 22

`../majutsu` は `main` / `origin/main` ともに `6a2163062e2f1a77f95c3c8ae46427349e786081` を指し、作業ツリーは clean。
installed CLI は `mj 0.3.0+build.22`。

検証:

```sh
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
mj fsck --home /home/vagrant/.majutsu --quick
```

いずれも成功した。

実環境 `/home/vagrant/.majutsu`:

- current snapshot: `snap-b18528a0-090c-4af5-a00f-79b60a984200`
- `local_current` と `remote_current` は一致。
- daemon は running、IPC ok。
- pending journal は 0。
- queued uploads は 0。
- remote は GCS S3 互換 backend の encrypted prefix。
- state usage は約 35.0 MiB。
- tree manifest local cache は 0 B。

現時点では daemon 稼働、remote 同期、quick fsck、local cache 削減はいずれも正常。

### 残る改善候補

1. `mj status --no-pager` が未対応。自動化や採取時には `MJ_PAGER=cat` などで回避できるが、CLI として明示オプションがある方が自然。
2. トップレベル `mj --help` は改善済みだが、`mj daemon --help` など一部サブコマンド配下の説明が薄い。
3. full `mj fsck` は metadata prune 後に remote hydrate を伴う可能性がある。大規模 root 向けには `--sample` / `--since` / phase 別対象件数などの追加余地がある。
4. `cache prune --metadata` はローカル二重保持を解消するが、snapshot ごとの tree manifest 生成量そのものは減らしていない。remote 長期効率と deep fsck コストをさらに詰めるなら、content-addressed subtree reuse や差分 tree 表現を検討する。
5. 過去にローカル HTTP mock テストが 1 回だけ connection reset / close で失敗し、単独再実行と全体再実行では成功している。再現していないが flaky 予防として mock server の待受開始・接続終了条件を点検対象に残す。

## 残改善実施 2026-06-14 build 23

実施:

- `mj status --no-pager` を追加し、出力が端末高さを超えても pager を使わず標準出力へ出せるようにした。
- `mj status --pager` を追加し、短い出力や非 TTY でも明示的に pager を使えるようにした。
- `mj daemon --help`、`mj lifecycle --help`、`mj large --help`、`mj remote --help`、`mj restore --help`、`mj key --help` 配下のサブコマンド説明を補強した。
- `mj fsck --sample N` を追加し、full fsck の重い payload / manifest / pack / metadata reference phase を phase ごとに最大 N 件で smoke できるようにした。
- `docs/OPERATIONS.md` に status pager 制御と local fsck sample 運用を追記した。

検証:

```sh
cargo test --test remote_file cli_help_describes_status_and_daemon_subcommands --locked
cargo test --test remote_file fsck_quick_and_timeout_are_available --locked
cargo test --test remote_file status_reports_configured_root_state --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

全て成功。

実環境反映:

- `cargo install --path . --locked --force`
- installed CLI: `mj 0.3.0+build.23`
- `mj daemon restart --home /home/vagrant/.majutsu`
- `mj cache prune --home /home/vagrant/.majutsu --metadata`
- `mj fsck --home /home/vagrant/.majutsu --quick`
- `mj sync status --home /home/vagrant/.majutsu`

結果:

- current snapshot は daemon により随時進むため、最終確認では `local_current` と `remote_current` の一致を確認した。
- daemon: running、IPC ok、pending journal 0。
- `local_current` と `remote_current` は一致。
- queued uploads 0。
- state usage は metadata prune 後に約 35.0 MiB。

残る設計課題:

- `mj fsck --since` は未実装。履歴 graph 全体の整合性検査と snapshot 時刻フィルタの意味づけが絡むため、partial fsck の仕様を決めてから入れる。
- tree manifest 生成量そのものを減らす subtree reuse / 差分 tree 表現は未実装。現状は local metadata cache prune でローカル二重保持を避ける。
- 過去に観測された HTTP mock の一時的な connection reset / close は今回再現していない。全体テストで再発する場合に server lifecycle を点検する。

## 残改善実施 2026-06-14 build 24

実施:

- `mj fsck --since TIME` を追加した。
  - `--since` は full fsck の heavy phase を指定時刻以降の snapshot から到達する payload / manifest に絞る。
  - refs、queue、oplog、history graph などの基本整合性は partial 指定でも全体を確認する。
  - 対象 snapshot から tree manifest を辿り、blob、large object、chunk、pack の検査対象を絞る。
  - `--sample` と併用した場合は scope 構築自体も sample 件数で打ち切る。
  - `--sample` または `--since` 指定時は、全履歴を前提にした metadata dangling 検査を false positive 回避のため省略する。
- 過去に一時失敗した S3 range GET 系テストの mock HTTP response を hardening した。
  - `Content-Length` と `Connection: close` を明示する。
  - response write 後に flush / shutdown し、reqwest 側の response boundary を安定させる。
- `BUILD_NUMBER` を 24 に更新した。
- `docs/OPERATIONS.md` に `fsck --since` の運用を追記した。

検証:

```sh
cargo test --test remote_file fsck_since_limits_heavy_checks_to_recent_snapshots --locked
cargo test --test remote_file remote_check_uses_s3_range_get_probe --locked
cargo test --test remote_file restore_resume_uses_s3_range_get_for_packed_blobs --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

残る設計課題:

- tree manifest 生成量そのものを減らす subtree reuse / 差分 tree 表現は未実装。影響範囲が大きいため、別設計タスクとして扱う。

### build 24 追補: daemon replay 後の自動 sync

実施:

- daemon restart 時などに pending event journal を replay して snapshot が作成された場合も、watch event snapshot と同じ外部 `mj sync --wait --timeout-secs 300` 経路へ流すようにした。
- remote が未設定の場合は何もしない。
- upload queue に delayed item がある場合は従来どおり `watch-sync-deferred` を記録し、retry 時刻を尊重する。
- `notify_watch_replay_syncs_current_snapshot_when_remote_is_configured` を追加し、remote 設定ありの `watch --once` が replay だけで current snapshot を remote metadata まで同期することを確認した。

検証:

```sh
cargo test --test remote_file notify_watch_replays_pending_event_journal --locked
cargo test --test remote_file notify_watch_replay_syncs_current_snapshot_when_remote_is_configured --locked
cargo test --test remote_file daemon_watch_snapshot_can_sync_clone_and_restore --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

全て成功。

実環境反映:

- `cargo install --path . --locked --force`
- installed CLI: `mj 0.3.0+build.24`
- `mj daemon restart --home /home/vagrant/.majutsu`
- `mj fsck --home /home/vagrant/.majutsu --since '24h ago' --sample 10 --timeout-secs 60 --progress`
- `mj fsck --home /home/vagrant/.majutsu --quick`
- `mj cache prune --home /home/vagrant/.majutsu --metadata`
- `mj sync status --home /home/vagrant/.majutsu`

結果:

- daemon: running、IPC ok、pending journal 0。
- `local_current` と `remote_current` の一致を確認。
- queued uploads 0、sync lock なし。
- scoped fsck は 25 秒程度で成功し、quick fsck も成功。
- metadata cache prune 後の state usage は約 35.0 MiB。

## 実装状況再確認 2026-06-14 build 24

確認した状態:

- repository: `main` / `ecff0e4 Harden fsck scope and daemon replay sync`
- 作業ツリー: clean
- installed CLI: `mj 0.3.0+build.24`
- 実環境 root:
  - `head`
  - `help`
  - `jotter`
  - `moon`
  - `stackchan`
  - `websh`
- daemon: running、IPC ok。
- pending journal: 0。
- queued uploads: 0。
- `local_current` と `remote_current` は一致。
- sync lock: なし。
- `mj fsck --home /home/vagrant/.majutsu --quick`: 成功、約 10 秒。
- `mj fsck --home /home/vagrant/.majutsu --since '24h ago' --sample 10 --timeout-secs 60 --progress`: 成功、約 24 秒。

ローカル state サイズ:

| 項目 | 値 |
| --- | ---: |
| state apparent size | 48,808,853 B |
| state disk usage | 64,421,888 B |
| queue apparent size | 127,851 B |
| queue disk usage | 6,836,224 B |
| event journal files | 545 |
| upload queue files | 0 |
| upload payload files | 0 |

`mj status --no-pager` の表示では state usage は apparent size 寄りに見える。一方、`queue/events`
のような小ファイル多数の領域では、実ディスク使用量が apparent size よりかなり大きくなる。

現時点の評価:

- daemon 稼働、自動 snapshot / sync、remote 同期、暗号化 backend、quick / scoped fsck は運用可能な状態。
- build 20 時点で問題だった stale daemon、巨大 local tree metadata、無進捗 fsck は、build 24 時点では実環境上のブロッカーではなくなっている。
- ただし、長期運用品質と大規模 root での効率をさらに上げる余地は残る。

残る改善候補:

1. `mj status` の storage 表示を apparent size と disk usage の両方に分ける。
   - 現状は queue apparent size が 127 KiB 程度でも disk usage は 6.8 MiB あり、小ファイル多数の実コストを見落としやすい。
   - `Local Storage` に `logical/apparent` と `disk` の列を追加するか、差が大きい場合に警告を出す。
2. 処理済み event journal の保持方針をもう少し明確にする。
   - pending は 0 だが、処理済み event file が 545 件残っている。
   - 現在は閾値を超えた snapshot 後に compact される設計だが、status 上は「正常な保持」なのか「compact 推奨」なのか判断しづらい。
   - `mj event compact` のような明示操作、または `mj cache prune` への event journal prune 統合を検討する。
3. `mj status` に sync head の一致状況を直接表示する。
   - `mj sync status` では `local_current == remote_current` を確認できるが、通常の `mj status` は remote configured / queued uploads 中心。
   - クラッシュ対策ツールとしては、通常 status の上部に `Remote head synced` / `Remote lagging` を出す方が異常に気付きやすい。
4. daemon の watch 対象 root 更新を明示する。
   - `root add` 後の自動 daemon 起動はあるが、既存 daemon が動作中の場合に root set 変更をどう反映したかが status から分かりづらい。
   - root 追加後に restart が必要ない設計ならその証跡を status / log に出し、必要な設計なら自動 reload を追加する。
5. `fsck --since` の scope 構築が tree manifest 読み込みに依存しており、sample 10 でも 8 秒程度かかる。
   - 現状は 60 秒制限内で成功しているためブロッカーではない。
   - 大規模 root では snapshot から到達する payload set を DB/index に保持し、scope 構築をさらに短縮する余地がある。
6. subtree reuse / 差分 tree 表現は未実装。
   - 現状は metadata cache prune によりローカル二重保持を抑えている。
   - remote 長期効率、deep fsck コスト、履歴がさらに増えた場合の metadata 量を詰めるなら別設計タスクとして扱う。

## 残改善実施 2026-06-14 build 25

実施:

- `mj status` の上部と overview に remote head 同期状態を表示するようにした。
  - local current と cached remote current ref が一致すれば `synced`。
  - 不一致、未同期、remote unavailable などは status で判別できる。
- `mj status` / `mj state` の storage 表示に apparent size と disk usage の両方を出すようにした。
  - 小ファイル多数の queue / event journal で実ディスク使用量を見落としにくくするため。
  - 狭い端末幅では列幅を収縮し、非末尾列を切り詰めて表示崩れを避ける。
- `mj event stat` と `mj event compact [--dry-run]` を追加した。
  - `stat` は総数、processed、pending、removable、oldest/newest、last snapshot finish を表示する。
  - `compact` は最新 snapshot finish より古い処理済み event record だけを削除する。
  - 自動 compact の閾値付き挙動は維持し、明示 compact だけ force 実行できる。
- `BUILD_NUMBER` を 25 に更新した。
- `docs/OPERATIONS.md` に remote head 表示、storage apparent/disk 表示、event journal retention を追記した。

検証:

```sh
cargo fmt --all -- --check
cargo test --test remote_file status_reports_configured_root_state --locked
cargo test --test remote_file event_stat_and_compact_report_processed_journal_records --locked
cargo test --test remote_file cli_help_describes_status_and_daemon_subcommands --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

全て成功。

## 実装状況再確認 2026-06-14 build 25

確認した状態:

- repository: `main` / `14cb8d7 Improve status storage and event journal operations`
- 作業ツリー: clean
- installed CLI: `mj 0.3.0+build.25`
- daemon: running、IPC ok。
- active roots: 6。
- pending journal: 0。
- queued uploads: 0。
- `local_current` と `remote_current` は一致。
- sync lock: なし。
- `mj fsck --home /home/vagrant/.majutsu --quick`: 成功、約 9 秒。
- `mj fsck --home /home/vagrant/.majutsu --since '24h ago' --sample 10 --timeout-secs 60 --progress`: 成功、約 20 秒。
- `mj status` で `Remote head synced`、storage apparent/disk、event journal removable が表示されることを確認。
- `mj event stat` / `mj event compact --dry-run` が動作することを確認。

観測値:

| 項目 | 値 |
| --- | ---: |
| snapshots | 1489 |
| operations | 2996 |
| logical blobs | 785.2 MiB |
| large objects | 739.4 MiB |
| chunks | 812.6 MiB |
| state apparent size | 85.3 MiB |
| state disk usage | 89.7 MiB |
| objects apparent size | 64.7 MiB |
| objects disk usage | 68.7 MiB |
| tree metadata apparent size | 50.3 MiB |
| queue apparent size | 19.9 KiB |
| queue disk usage | 348.0 KiB |
| event journal records | 87 |
| event journal removable | 84 |
| upload queue | 0 |

追加で `mj cache stat --metadata` を確認したところ、metadata cache candidates は 28 件 / 52,695,289 B
だった。つまり build 25 の event journal compact と status 表示改善は有効だが、scoped fsck や
daemon snapshot 後に tree metadata cache が再び数十 MiB 規模へ増える状態は残っている。

現時点の評価:

- データ保全の観点では、daemon 稼働、remote head 同期、upload queue 0、quick/scoped fsck 成功により正常。
- build 25 により、remote head の同期状態と actual disk usage は通常 status で確認しやすくなった。
- 一方で、local metadata cache の再増加と event journal removable の蓄積は、手動 prune/compact に頼る運用要素として残っている。

残る改善候補:

1. metadata cache の自動 prune 方針を追加する。
   - `cache prune --metadata` は手動では有効だが、scoped fsck や通常 snapshot 後に tree metadata が再増加する。
   - sync 完了後や fsck 完了後に synced metadata cache を best-effort prune する、または上限サイズを設定する余地がある。
2. `fsck --since` が tree manifest を hydrate し、その後の local state を増やす点を抑える。
   - 検査に必要な tree manifest を temporary cache として扱い、成功後に自動削除する設計を検討する。
   - あるいは snapshot から到達する payload set を DB/index に持ち、scope 構築時に tree manifest の full hydrate を避ける。
3. event journal compact の定期化または `cache prune` への統合。
   - build 25 で `mj event compact` は追加済みだが、短時間でも removable が再蓄積する。
   - daemon snapshot 後の自動 compact 閾値、または `mj cache prune --events` のような運用導線を検討する。
4. `Remote head synced` は cached remote refs に基づくため、remote 実体の完全確認とは別であることをさらに表示する。
   - deep object availability は `mj sync status --deep` や `mj remote fsck --objects` の担当。
   - status 上に `quick` / `cached` の明示を加えると誤解を減らせる。
5. subtree reuse / 差分 tree 表現は引き続き未実装。
   - tree metadata の長期増加を根本的に抑えるには、同一 subtree の再利用や差分 tree 表現が必要。

## 残改善実施 2026-06-14 build 26

`/tmp/websh-drop-mqe4ctsn-fzts7t-majutsu_residual_efficiency_fix.zip` の提案を確認し、
既存実装と重複しない改善を取り込んだ。

実施:

- 直前 snapshot から root tree が変化していない場合、既定では新しい snapshot metadata を作らないようにした。
  - daemon の定期 snapshot や手動確認で remote metadata が増え続ける問題を抑えるため。
  - checkpoint として無変化 snapshot を残したい場合は `MAJUTSU_SNAPSHOT_ALLOW_NOOP=1` を使う。
  - no-op 判定のために一時的に再生成された payload cache と tree metadata は、remote 同期済みなら同じ経路で prune する。
- snapshot 完了後の event journal compact を強制実行に変更した。
  - ただし直近 snapshot cycle の watch / plugin / snapshot event は調査用に残し、前回 snapshot 完了より古い record だけを削除する。
- `mj fsck` が検査のために hydrate した同期済み payload cache と tree metadata cache を、成功後に自動 prune するようにした。
  - `--quick` は対象外。
  - 調査目的で残したい場合は `MAJUTSU_FSCK_PRUNE_PAYLOAD_CACHE=0` または `MAJUTSU_FSCK_PRUNE_METADATA_CACHE=0` を指定する。
- sync の GC mark publish 頻度改善案は確認したが、現行実装は canonical S3/GCS では remote mark 欠落時や remote prune 指定時だけ publish するため、新規変更は不要と判断した。
- `BUILD_NUMBER` を 26 に更新した。

期待される効果:

- 無変化 snapshot による snapshot manifest / export metadata の増加を止める。
- event journal の処理済み record が短時間で再蓄積する量を抑える。
- scoped/full fsck 後に local payload / tree metadata cache が再び肥大化する問題を抑える。

残る設計課題:

- 同一 subtree reuse / 差分 tree 表現は未実装。
  - 無変化 snapshot は抑制したが、巨大 root の一部だけが変わる場合の tree metadata 生成量はまだ削減余地がある。
- fsck scope 構築と payload 検査は remote hydrate に依存する。
  - 成功後に自動 prune するため local state 肥大は抑えられるが、scope 構築時間をさらに短縮するには payload reachability index が必要。
- `Remote head synced` は cached remote ref に基づく quick signal であり、object availability の完全監査ではない。
  - 完全確認は引き続き `mj sync status --deep`、`mj remote fsck --objects`、`mj fsck` を使う。

検証:

- `cargo fmt --all -- --check` 成功。
- `cargo clippy --workspace --all-targets --locked -- -D warnings` 成功。
- `cargo test --test e2e_local cache_prune_evicts_synced_payload_cache_and_restore_hydrates --locked` 成功。
- `cargo test --workspace --all-targets --locked` 成功。

実環境反映:

- installed CLI: `mj 0.3.0+build.26`
- daemon を restart 済み、PID `3046496`、IPC ok。
- `mj fsck --home /home/vagrant/.majutsu --since '24h ago' --sample 10 --timeout-secs 90 --progress` 成功。
  - 検査で hydrate された metadata cache は終了時に自動 prune された。
  - payload cache は同期済み残留候補 0 のまま。
- `mj snapshot` の no-op 経路で `snapshot unchanged ...` を確認。
- no-op snapshot 後に再生成された同期済み payload cache / tree metadata が local に残らないように追加修正した。
- event journal は records 15、pending 0、removable 0。
- `mj cache stat --metadata` は payload / metadata cache candidates ともに 0。
- `mj sync status` は `local_current == remote_current == snap-866a7454-4d8e-4237-9dd4-82725705a804`、queued uploads 0、sync lock なし。
- `mj status --no-pager` は state apparent size 35.1 MiB、disk 39.2 MiB、trees 0 B。

## 残改善実施 2026-06-14 build 27

実施:

- `mj status` の `Remote head` 表示を `synced (cached)` / `lagging (cached)` のように変更した。
  - remote head は local DB に最後に観測した remote ref に基づく quick signal であり、object availability の完全監査ではないため。
- snapshot 到達 payload index をDBに追加した。
  - `snapshot_payload_index(snapshot_id, indexed_at)`
  - `snapshot_payloads(snapshot_id, kind, oid)`
  - `large_object_chunks(large_oid, chunk_oid)`
- 新規snapshot作成時に、snapshotから到達する blob / large object と、large object から到達する chunk をindexへ保存するようにした。
- `mj fsck --since` のscope構築で、対象snapshotすべてにindexが揃っている場合はDB indexからscopeを構築するようにした。
  - index不足、古いsnapshot、large object chunk index不足があれば従来のmanifest読み込みへフォールバックし、読み込めたindexをbackfillする。
- `mj sync` 完了後に同期済み tree metadata cache も自動 prune するようにした。
  - `MAJUTSU_SYNC_LOCAL_METADATA_CACHE_PRUNE=0` で無効化できる。
  - 明示指定がない場合は `MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE` に追従し、調査時にpayload cacheを残す指定ではmetadata cacheも残す。
- `BUILD_NUMBER` を 27 に更新した。

期待される効果:

- 今後作成されるsnapshotについて、`mj fsck --since` のscope構築時にtree manifest / large manifestをremoteからhydrateする頻度を下げる。
- 実snapshot後も、sync完了後にpayload / metadata cache候補が残らない状態へ戻す。
- statusのremote head表示がquick/cached signalであることが明確になり、`sync status --deep` / `remote fsck --objects` との役割分担が分かりやすくなる。

残る設計課題:

- 全履歴を先行して埋める専用backfillコマンドは未実装。
  - ただし `mj fsck --since` で対象になった古いsnapshotは、fallback後にindexが補完される。
- 同一 subtree reuse / 差分 tree 表現は未実装。
  - 今回はfsck scope構築コストを下げたが、巨大rootの一部変更時に生成されるtree metadataそのものを減らすには別設計が必要。

検証:

- `cargo fmt --all -- --check` 成功。
- `cargo clippy --workspace --all-targets --locked -- -D warnings` 成功。
- `cargo test --test remote_file fsck_since_limits_heavy_checks_to_recent_snapshots --locked` 成功。
- `cargo test --test remote_file status_reports_configured_root_state --locked` 成功。
- `cargo test -p majutsu-db --locked` 成功。
- `cargo test --workspace --all-targets --locked` 成功。

実環境反映:

- installed CLI: `mj 0.3.0+build.27`
- daemon を restart 済み、PID `3121584`、IPC ok。
- `mj status --no-pager` で `Remote head synced (cached)` を確認。
- `snapshot_payload_index` / `snapshot_payloads` / `large_object_chunks` table 作成を確認。
- 最新snapshot `snap-65f37d66-314e-42ad-ad3a-2d18e5113492` まで index が作成された。
  - `snapshot_payload_index`: 3
  - `snapshot_payloads`: 48008
  - `large_object_chunks`: 10169
- `mj fsck --since <latest-created-at> --sample 10 --timeout-secs 90 --progress` は `source=index` でscope構築し成功。
- `mj cache stat --metadata` は payload / metadata cache candidates ともに 0。
- `mj sync status` は `local_current == remote_current == snap-65f37d66-314e-42ad-ad3a-2d18e5113492`、queued uploads 0、sync lock なし。
- `mj daemon status` は pending journal 0、queued uploads 0。
- `mj status --no-pager` は state apparent size 48.0 MiB、disk 52.1 MiB、trees 0 B。
- sync-time metadata prune 追加後の実環境確認:
  - installed CLI: `mj 0.3.0+build.27`
  - daemon を restart 済み、PID `3186617`、IPC ok。
  - `mj sync --wait` は `synced 0 objects`、`pruned_metadata_cache_objects 9`、`pruned_metadata_cache_bytes 13572803`。
  - `mj cache stat --metadata` は payload / metadata cache candidates ともに 0。
  - `mj sync status` は `local_current == remote_current == snap-4e9edf8c-6045-4df7-830f-2c41fce2638c`、queued uploads 0、sync lock なし。
  - `mj status --no-pager` は state apparent size 62.0 MiB、disk 66.2 MiB、trees 0 B。

## 再点検 2026-06-14

現状:

- `../majutsu` は `origin/main` と一致。HEAD は `9318e89 Prune synced metadata cache during sync`。
- installed CLI は `mj 0.3.0+build.27`。
- daemon は running、PID `3186617`、IPC ok、RSS 約 31 MiB。
- 実環境 root は 6 件すべて active。
- `mj sync status` quick は `local_current == remote_current == snap-3eb37c2f-32e0-41bf-9384-fc3f1dc0cf0e`、queued uploads 0、sync lock なし。
- `mj cache stat --metadata` は payload / metadata cache candidates ともに 0。
- `mj status --no-pager` は state apparent size 66.0 MiB、disk 70.1 MiB、trees 0 B。
- DB index 状況:
  - `snapshots`: 1516
  - `snapshot_payload_index`: 7
  - `snapshot_payloads`: 112020
  - `large_object_chunks`: 10169

検証:

- `cargo fmt --all -- --check` 成功。
- `cargo clippy --workspace --all-targets --locked -- -D warnings` 成功。
- `cargo test --workspace --all-targets --locked` 成功。

残課題:

1. `mj sync status --deep` が実環境のGCS S3互換backendで数分以上完了しない。
   - 10,000件超のlocal objectについて `remote_object_available` を逐次実行している。
   - `remote_object_available` はlocal keyとcanonical aliasの両方に `HEAD` を投げる可能性があり、最大でobject数の約2倍のrequestになる。
   - `sync status --deep` には `--sample` / `--timeout-secs` / progress 表示がないため、通常運用で使うには重すぎる。
2. `mj fsck --since '24h ago' --sample 10 --timeout-secs 90 --progress` が、scope構築後のobject検査で90秒を超えて継続した。
   - `since-scope` は即時に進み、index利用は機能している。
   - その後のremote/local object確認で無出力になり、プロセスは一時 D 状態のI/O待ちになった。
   - `--timeout-secs` はループ境界でのみ確認され、個別のS3/HTTP requestには伝播しない。
   - `reqwest::blocking::Client::new()` を使っており、remote store のconnect/read timeoutを明示設定していない。

改善案:

- `sync status --deep` に `--sample`、`--timeout-secs`、`--progress` を追加する。
- deep status / fsck のremote object確認は、逐次 `HEAD` ではなくremote key indexの一括 `LIST` を優先し、canonical alias判定もset membershipで処理する。
- S3 remote clientにconnect/read/request timeoutを設定し、CLI側のtimeout budgetをremote operationへ渡せる形にする。
- timeout時は「確認未完了」として終了し、quick statusが正常か、何件まで確認したか、残り件数を表示する。
- `snapshot_payload_index` の全履歴backfillコマンドを追加すると、古いsnapshotを含むscoped検査でmanifest hydrateを避けやすくなる。

## 残改善実施 2026-06-14 build 28

実施:

- `BUILD_NUMBER` を 28 に更新した。
- `mj sync status --deep` に次を追加した。
  - `--sample <N>`
  - `--timeout-secs <SECONDS>`
  - `--progress`
- `mj sync status --deep` のremote object確認を、objectごとの逐次 `HEAD` から、remote key indexの一括 `LIST` とset membership判定へ変更した。
  - `remote_payload_key_index` に `objects/indexes/pack/`、`indexes/pack-index/`、`objects/large/manifests/`、`large/manifests/` も含め、local object key全体のcanonical alias判定に使えるようにした。
  - 一括 `LIST` が失敗した場合だけ従来の `HEAD` 確認へfallbackする。
- deep status出力に次を追加した。
  - `remote_objects_checked`
  - `missing_remote_objects_limited`
  - `remote_object_check_source`
- `mj sync --wait` の完了条件を修正し、`local_current == remote_current` かつ queue 0 だけでなく、`sync_lock_pid` が消えるまで待つようにした。
  - sync本体の後処理として実行されるlocal cache pruneが完了する前に `--wait` が返る状態を防ぐため。

期待される効果:

- GCS S3互換backendでの `mj sync status --deep` が、10,000件超のobjectに逐次 `HEAD` する状態を避けられる。
- `--sample` / `--timeout-secs` / `--progress` により、運用中でも確認範囲と待ち時間を制御できる。

検証:

- `cargo fmt --all -- --check` 成功。
- `cargo clippy --workspace --all-targets --locked -- -D warnings` 成功。
- `cargo test --test e2e_local sync_status_quick_and_wait_target_advancement --locked` 成功。
- `cargo test --test remote_file clone_can_restore_from_canonical_object_aliases --locked` 成功。
- `cargo test --test remote_file sync_wait_reports_status_when_existing_sync_lock_is_held --locked` 成功。
- `cargo test --workspace --all-targets --locked` 成功。

実環境反映:

- installed CLI: `mj 0.3.0+build.28`
- daemon を restart 済み、最終PID `3288157`、IPC ok。
- `mj sync status --deep --progress`
  - elapsed: 約 12.0 秒。
  - `local_objects 12043`
  - `remote_objects_checked 12043`
  - `missing_remote_objects 0`
  - `missing_remote_objects_limited false`
  - `remote_object_check_source list`
- `mj sync status --deep --sample 10 --timeout-secs 5`
  - elapsed: 約 5.35 秒。
  - `remote_objects_checked 7`
  - `missing_remote_objects 0`
  - `missing_remote_objects_limited true`
  - `remote_object_check_source head`
- 最終状態:
  - `mj sync --wait` は `sync_lock_pid (none)` まで待って返る。
  - `local_current == remote_current == snap-a48a3401-da75-4e71-99e6-8a1d5f0a54da`
  - queued uploads 0。
  - `mj cache stat --metadata` は payload / metadata cache candidates ともに 0。
  - daemon journal pending 0。

残課題:

- `mj fsck --since` のremote object検査はscope構築後にまだ重くなる可能性がある。
  - fsck自体はremote key indexを利用しているが、検査対象の種類とtimeout粒度をさらに見直す余地がある。
- S3 remote clientのconnect/read/request timeout明示設定は未実装。

## 残改善実施 2026-06-14 build 29

実施:

- `BUILD_NUMBER` を 29 に更新した。
- S3 / GCS S3互換 remote の reqwest blocking client にtimeoutを明示設定した。
  - connect timeout: 既定 10秒。
  - request timeout: 既定 300秒。
  - `MAJUTSU_S3_CONNECT_TIMEOUT_SECS` と `MAJUTSU_S3_REQUEST_TIMEOUT_SECS` で上書き可能。
- `mj fsck --progress` で remote payload key index 取得を `remote-payload-index` phase として表示するようにした。
  - `fsck --since` でscope構築後に待つ場合、remote index取得待ちなのか後続検査なのかを判別しやすくするため。

期待される効果:

- GCS/S3側の一時的な接続停滞でCLIが無期限に待ち続ける状態を避ける。
- `fsck --since` の重いremote確認で、少なくとも進捗上どのphaseにいるかを確認できる。

残課題:

- 現在のreqwest blocking ClientBuilderではread timeoutを使っていない。
  - request全体timeoutで上限は付くが、細かなread timeoutが必要ならreqwest更新または別設定の調査が必要。
- `snapshot_payload_index` の全履歴backfillコマンドは未実装。
- subtree reuse / 差分tree表現は未実装。

## 残改善実施 2026-06-14 build 30

build 29 を実環境に入れて `mj fsck --since '24h ago' --sample 10 --timeout-secs 90 --progress`
を確認したところ、`since-scope` 後に 90秒を超えて継続した。

原因:

- build 29 では `remote-payload-index` phase は見えるようになったが、`--sample` / `--since` の
  scoped fsck でも全 payload prefix の remote LIST を実行していた。
- `--timeout-secs` は Rust 側の loop / phase 境界では効くが、remote LIST の一連の blocking
  request 中には割り込めない。
- sampled smoke の目的に対して、backend 全体の payload key index を作るのは過剰だった。

実施:

- `BUILD_NUMBER` 30。
- `mj fsck` は `--sample` または `--since` 指定時に remote payload key index の一括 LIST を行わない。
- local cache が欠落し、かつ remote 設定がある payload だけを timeout 確認後に HEAD probe する。
- full `mj fsck --deep` は従来通り remote key index の一括 LIST を使い、大量 object の逐次 HEAD を避ける。
- `mj fsck --since --sample` は対象 snapshot を作成時刻の新しい順に選ぶ。
- scoped sample では snapshot / large / pack manifest object の deep 検査を省略し、payload record /
  availability の smoke を優先する。
- `docs/OPERATIONS.md` に scoped/sample fsck の remote 確認方針を追記した。

検証:

- `cargo test --test remote_file fsck_since_limits_heavy_checks_to_recent_snapshots --locked` 成功。
- `cargo test --test remote_file fsck_quick_and_timeout_are_available --locked` 成功。
- `cargo fmt --all -- --check` 成功。
- `cargo clippy --workspace --all-targets --locked -- -D warnings` 成功。
- `cargo test --workspace --all-targets --locked` 成功。
- 実環境 debug binary で `mj fsck --since '24h ago' --sample 10 --timeout-secs 90 --progress` が
  60.7秒で成功。
  - `source=index snapshots=10`
  - `object-manifests skipped=scoped-sample`

残り:

- full `mj fsck --deep` の remote LIST は request timeout 依存。全履歴監査では意図的に時間枠を取る。
- `large-payloads sampled=10` は実環境で約54秒かかっており、さらに短い smoke が必要なら
  large chunk 単位の sample 上限を別途追加する余地がある。
- `snapshot_payload_index` の全履歴backfillコマンドは未実装。
- subtree reuse / 差分tree表現は未実装。

## 残改善実施 2026-06-14 build 31

実施:

- `BUILD_NUMBER` 31。
- `mj fsck --sample` を large object 内の chunk 検査にも適用した。
  - build 30 では large object 件数にだけ sample が効き、1つの large object が多数 chunk を持つと
    `large-payloads sampled=10` でも時間が伸びていた。
  - build 31 では `large-chunks sampled=N` で chunk 検査も打ち切る。
- `mj fsck --backfill-index` を追加した。
  - scoped fsck が使う `snapshot_payload_index` / `snapshot_payloads` を補完する。
  - 通常は remote hydrate や巨大 compact tree manifest 展開を避け、短時間に戻る。
  - missing metadata も含めて深く補完する場合は `--hydrate-index-objects` を明示する。
- `docs/OPERATIONS.md` に large chunk sample と index backfill の運用を追記した。

検証:

- `cargo test --test remote_file fsck_backfill_index_rebuilds_missing_payload_index --locked` 成功。
- `cargo test --test remote_file fsck_since_limits_heavy_checks_to_recent_snapshots --locked` 成功。
- 実環境 debug binary で `mj fsck --since '24h ago' --sample 10 --timeout-secs 90 --progress` が
  27.5秒で成功。
  - build 30 release binary の同条件は約63秒。
  - `large-chunks sampled=10` を確認。
- 実環境 debug binary で `mj fsck --backfill-index --since '24h ago' --sample 2 --timeout-secs 30 --progress`
  が短時間で戻ることを確認。
  - remote hydrate が必要な古い compact snapshot は `backfill_skipped_missing_local_snapshots` として skip。
- `cargo fmt --all -- --check` 成功。
- `cargo clippy --workspace --all-targets --locked -- -D warnings` 成功。
- `cargo test --workspace --all-targets --locked` 成功。

実環境反映:

- installed CLI: `mj 0.3.0+build.31`
- daemon restart 済み、PID `3430326`、IPC ok。
- `mj sync --wait --timeout-secs 300` 成功。
- `local_current == remote_current == snap-17dbae62-568f-438f-9841-185abd36f1ab`
- queued uploads 0、sync lockなし。
- release binary で `mj fsck --since '24h ago' --sample 10 --timeout-secs 90 --progress` が
  17.1秒で成功。
- release binary で `mj fsck --backfill-index --since '24h ago' --sample 2 --timeout-secs 30 --progress`
  が短時間で成功。

残り:

- full `mj fsck --deep` の remote LIST は request timeout 依存。全履歴監査では意図的に時間枠を取る。
- `mj fsck --backfill-index --hydrate-index-objects` は巨大 tree manifest を読むためメンテナンス用途。
- subtree reuse / 差分tree表現は未実装。

## 方針変更 2026-06-14

`fsck` を定期的に実行し続ける設計ではなく、通常運用で `fsck` が必要になる状況を作らないことを
優先する方針に変更した。

通常運用で重要な signal:

- daemon が running かつ IPC ok。
- active root が欠落していない。
- pending event journal が 0。
- upload queue が 0。
- local current と cached remote head が一致。
- sync lock が残っていない。
- remote が設定され、openできる。
- 暗号化時にkey materialの基本状態が確認できる。

`fsck` は異常時、復旧前後、release前、メンテナンス時の診断ツールとして扱う。

## 残改善実施 2026-06-14 build 32

実施:

- `BUILD_NUMBER` 32。
- `mj health` を追加した。
  - text出力と `--json` を提供する。
  - `protected` / `degraded` / `unprotected` の保護状態を返す。
  - 判定材料は daemon、root、remote、cached remote head、upload queue、pending event journal、
    sync lock、encryption key file。
- `mj status` の最上段に `Protection` と `Health issues` を追加した。
- `mj status` に `Protection` セクションを追加し、health issueを重要度付きで表示するようにした。
- `docs/OPERATIONS.md` に、通常運用では full fsck ではなく `mj health` を見る方針を追記した。

検証:

- `cargo test --test remote_file health_reports_unprotected_when_active_root_has_no_daemon_or_remote --locked` 成功。
- `cargo test --test remote_file status_reports_configured_root_state --locked` 成功。
- `cargo fmt --all -- --check` 成功。
- `cargo clippy --workspace --all-targets --locked -- -D warnings` 成功。
- 実環境 debug binary で `mj health` / `mj health --json` が `state protected` を返すことを確認。
- 実環境 debug binary の `mj status --no-pager` 先頭に `Protection protected` が出ることを確認。

残り:

- daemon自身が定期的にhealthを記録する `runtime/health.json` は未実装。
- healthが degraded / unprotected に変わった場合の notice 通知は未実装。
- root別の最終snapshot/sync時刻やpermission degradedの詳細表示は未実装。
