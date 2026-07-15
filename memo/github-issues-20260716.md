# GitHub Issues 対応メモ 2026-07-16

## 対象

`genki/majutsu` の open issue #17、#18、#19 を確認した。

## 対応結果

- #17: 永続化された PID の再利用を daemon と誤認する問題は、既存の PID、
  実行ファイル、コマンドライン、起動時刻の検証と stale runtime の掃除で解決済み
  だった。関連する daemon 起動・doctor・restart の回帰テストを再確認した。
- #19: `status` が暗黙に daemon を起動する問題を修正した。既定の `mj status`
  は読み取り専用で、必要な利用者だけが `mj status --start-daemon` を指定する。
  `fsck --quick`、`sync status`、`remote check`、`remote repair --dry-run` が
  daemon を起動しないことも回帰テストで確認した。
- #18: 旧 `metadata/export.json*` および `hosts/<uuid>/...` layout から現行の
  host-name prefix へ移す `mj remote migrate-legacy --host <id-or-name>` と
  `--dry-run` を追加した。S3 ではメタデータを `.zst` 付きの現行キーへ保存し、
  canonical content の形式も現行仕様へ変換する。旧データは検証完了まで残す。

## 検証

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- 診断系、migration、daemon identity の対象テスト
- `cargo test --locked --test remote_file -- --test-threads=1`（304 passed）

全体テストを既定の並列設定で実行すると、既存の Linux inotify テストが環境依存で
長時間待機した。対象テスト単独では成功し、全体は test-threads=1 で完走したため、
今後は inotify 子プロセスに明示的なテストタイムアウトを設ける余地がある。

## 運用上の注意

旧 remote は自動読込しない方針を維持する。移行時は dry-run、実移行、`mj remote
check`、clone/restore の順に確認し、問題がないことを確認してから旧 prefix を
backend 側で削除する。
