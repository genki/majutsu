# majutsu 完成度スコアカード

このファイルは majutsu を完成と呼ぶための受け入れ条件を定義する。

## 機能受け入れ条件

- [ ] `mj init`、`root add`、`snapshot`、`status`、`log`、`diff`、`restore plan`、`restore apply` がローカル E2E を通過する。
- [ ] 複数 root の timeline を空の state directory に clone し、別 target へ restore できる。
- [ ] large object が pointer manifest と chunk 経由で保存され、byte-for-byte で復元できる。
- [ ] file remote で `sync`、`remote check`、`remote fsck`、`clone`、`restore apply` が通過する。
- [ ] S3 互換 remote が MinIO E2E スクリプトを通過する。
- [ ] 暗号化 state を remote metadata と export 済み master key だけで clone / restore できる。
- [ ] `fsck` が metadata graph、local object、pack、chunk、manifest、queue、ref、operation log を検査する。
- [ ] `prune --dry-run` と `gc` が live data を削除しない。
- [ ] daemon status / metrics が root、current snapshot、event journal、upload queue、restore queue の状態を公開する。

## 運用受け入れ条件

- [ ] CI が Linux と macOS で通過する。
- [ ] release workflow がダウンロード可能な artifact を生成する。
- [ ] provider matrix で supported とした provider がすべて検証済みである。
- [ ] archive / cold tier からの restore を、archive restore 対応の実 S3 互換 provider で少なくとも 1 つ検証している。
- [ ] `scripts/package-release.sh` が `mj`、README、docs を含む自己完結 archive を生成する。

## 完成宣言

上記のすべての項目が完了し、release tag の CI が green になった時点で、該当 release を 100% complete と宣言できる。
