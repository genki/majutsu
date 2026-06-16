# majutsu 完成度スコアカード

このファイルは majutsu を完成と呼ぶための受け入れ条件を定義する。

## 自動完了ゲート

次を通過した release candidate は、ローカル機能、file remote、Podman
MinIO S3 互換 remote、release package smoke について完成判定できる。

```sh
MAJUTSU_RUN_MINIO_E2E=1 scripts/check-completion.sh
```

このゲートには次が含まれる。

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked`
- `cargo test --workspace --all-targets --locked`
- local file-remote E2E
- encrypted disaster recovery E2E
- large object manifest/chunk E2E
- prune/gc safety E2E
- daemon status/metrics smoke E2E
- release package smoke
- Podman MinIO S3-compatible E2E

## 機能受け入れ条件

- [x] `mj init`、`root add`、`snapshot`、`status`、`log`、`diff`、`restore plan`、`restore apply` がローカル E2E を通過する。検証: `scripts/check-completion.sh`
- [x] 複数 root の timeline を空の state directory に clone し、別 target へ restore できる。検証: `tests/e2e_local.rs`
- [x] large object が pointer manifest と chunk 経由で保存され、byte-for-byte で復元できる。検証: `tests/e2e_local.rs`, `scripts/e2e-minio.sh`
- [x] normal blob の multipart upload が S3 互換 remote で通過する。検証: `scripts/e2e-minio.sh`
- [x] file remote で `sync`、`remote check`、`remote fsck`、`clone`、`restore apply` が通過する。検証: `tests/e2e_local.rs`
- [x] S3 互換 remote が Podman ベースの MinIO E2E スクリプトを通過する。検証: `MAJUTSU_RUN_MINIO_E2E=1 scripts/check-completion.sh`
- [x] 暗号化 state を remote metadata と export 済み master key だけで clone / restore できる。検証: `tests/e2e_local.rs`, `scripts/verify-encrypted-remote-recovery.sh`
- [x] `fsck` が metadata graph、local object、pack、chunk、manifest、queue、ref、operation log を検査する。検証: `scripts/check-completion.sh`
- [x] `prune --dry-run` と `gc` が live data を削除しない。検証: `tests/e2e_local.rs`
- [x] daemon status / metrics が root、current snapshot、event journal、upload queue、restore queue の状態を公開する。検証: `tests/e2e_local.rs`

## 運用受け入れ条件

- [x] GitHub Actions は使わず、ローカル completion gate を release 判定の唯一の自動ゲートにする。検証: `MAJUTSU_RUN_MINIO_E2E=1 scripts/check-completion.sh`
- [x] release artifact はローカルで生成し、展開後の smoke test まで completion gate で確認する。検証: `scripts/check-completion.sh`
- [x] provider matrix で supported とした provider がすべて検証済みである。対象: File remote, MinIO via Podman, GCS S3-compatible endpoint。詳細: `docs/PROVIDER_MATRIX.md`
- [x] archive / cold tier restore は、実 provider drill が通った provider だけ supported とし、未検証 provider は experimental として扱う。検証: `docs/PROVIDER_MATRIX.md`
- [x] `scripts/package-release.sh` が `target/dist/` に `mj`、README、docs を含む自己完結 archive を生成する。検証: `scripts/check-completion.sh`

## 完成宣言

機能受け入れ条件と supported provider matrix は満たしている。100% complete
を宣言する release では、さらに次のローカル証跡を `docs/RELEASE_EVIDENCE.md`
または release note に記録する。

1. 最新 commit で `MAJUTSU_RUN_MINIO_E2E=1 scripts/check-completion.sh` が成功した結果。
2. ローカル生成した release artifact 名。
3. artifact 展開後の `mj --version` / `mj --help` 結果。
4. archive restore を supported とする場合、その provider の実 restore drill 結果。

Archive restore 未検証の release では、archive restore 対応 provider を
supported とせず experimental として扱う。
