# 残存課題 2026-06-09 解消方針

`memo/remaining-issues-20260609.md` の課題に対する対応方針。

## 対応済みとして扱うもの

- completion gate 通過済み機能は `docs/COMPLETION_SCORECARD.md` の機能受け入れ条件で `[x]` に更新する。
- GCS S3-compatible endpoint の `moon` root 検証結果を `docs/PROVIDER_MATRIX.md` に記録する。
- AWS S3 / Cloudflare R2 は実検証前に supported と呼ばず、experimental として扱う。
- production encryption の運用手順を `docs/ENCRYPTED_PRODUCTION_STATE.md` に分離する。

## 外部証跡が必要なもの

- ローカル release artifact の展開 / `mj --version` 確認。
- archive / cold tier restore を supported とする場合の実 provider drill。

GitHub Actions は利用しない。ローカル artifact の証跡は `scripts/check-completion.sh`
と `scripts/package-release.sh`、archive restore の証跡は
`scripts/e2e-aws-archive-restore.sh` で取得する。
