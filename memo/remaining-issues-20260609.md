# majutsu 残存課題 2026-06-09

このメモは、現時点で majutsu を「100% complete」と宣言する前に残っている課題をまとめる。コード上の主要仕様は `MAJUTSU_RUN_MINIO_E2E=1 scripts/check-completion.sh` で通過済みだが、release 完成宣言には外部 CI、実 provider、release artifact の証跡がまだ必要。

## 現時点で確認済みの範囲

- `MAJUTSU_RUN_MINIO_E2E=1 scripts/check-completion.sh` はワークスペース環境で通過済み。
- Podman MinIO E2E は通過済み。
- file remote E2E、encrypted disaster recovery、large object roundtrip、prune/gc safety、daemon status/metrics smoke は completion gate 内で通過済み。
- GCS S3 互換 backend で `~/moon` root の `sync`、`remote check`、`remote fsck`、remote clone / restore は通過済み。
- pack 化後の remote loose object cleanup と local loose blob cleanup は実装済み。
- `~/.majutsu` の実測では local loose blob が `1246` files から `2` files に減り、state 使用量は `26M` から `14M` に減った。

## 完成宣言前のブロッカー

### 1. GitHub Actions の最新 CI 結果確認

最新 commit `85a7edd` について、GitHub Actions の Linux / macOS CI が green であることを確認する必要がある。

現状、ワークスペース環境の `gh` はインストール済みだが認証情報が見えておらず、`gh run list` で確認できなかった。

必要作業:

```sh
GH_TOKEN=... gh run list --limit 10
GH_TOKEN=... gh run view <run-id>
```

または GitHub UI で `genki/majutsu` の最新 `main` CI を確認する。

### 2. release workflow artifact の確認

release workflow が生成する artifact を実際にダウンロードし、展開後の `mj --version` が動くことを確認する必要がある。

ローカルの `scripts/package-release.sh smoke` は completion gate 内で通過済みだが、GitHub release workflow の artifact は未確認。

必要作業:

- tag または `workflow_dispatch` で release workflow を実行する。
- Linux / macOS artifact をダウンロードする。
- 展開後の `mj --version` と `mj --help` を確認する。

### 3. 実 provider matrix の記録更新

`docs/PROVIDER_MATRIX.md` では AWS S3、GCS S3-compatible endpoint、Cloudflare R2 が manual validation required のまま。

GCS S3-compatible endpoint は今回の実 backend で主要操作を確認済みだが、matrix には検証日、backend、確認コマンド、結果がまだ反映されていない。

必要作業:

- GCS S3-compatible endpoint の検証済み項目を matrix に反映する。
- AWS S3 と Cloudflare R2 を supported として維持するなら、release candidate ごとに実検証する。
- 未検証 provider は supported ではなく experimental / unverified として扱うか判断する。

### 4. archive / cold tier restore の実 provider 検証

archive restore については実装とテストはあるが、archive restore 対応の実 S3 provider で cold tier から復元する検証は未完了。

MinIO は Glacier-style archive restore を持たないため、この受け入れ条件の代替にはならない。

必要作業:

- AWS S3 Glacier / Deep Archive など、restore request を実際に受ける provider で検証する。
- lifecycle または手動で対象 object を archive tier に移す。
- `mj restore prepare` で archive restore request が出ることを確認する。
- provider 側の restore 完了後、`mj restore apply` で復元できることを確認する。

### 5. production 用 state encryption 方針

現在のワークスペース環境の `~/.majutsu` は `security.encryption = "none"` のまま。これは仕様実装の未達ではないが、secret を含む root を実運用で守るには不適切。

必要作業:

- `~/moon` を継続的に守る運用では encryption enabled state へ移行する。
- master key export / 保管手順を運用メモに明記する。
- encrypted state で GCS backend clone / restore を再検証する。

## 非ブロッカーだが残す課題

### 1. Clippy warning の整理

`cargo clippy --workspace --all-targets --locked` は exit code 0 で通過するが、既存 warning が残っている。

completion gate は warning を failure として扱っていないため現時点では非ブロッカー。ただし release 品質としては、以下のような warning は順次解消した方がよい。

- `collapsible_if`
- `needless_question_mark`
- `large_enum_variant`
- `too_many_arguments`
- `items_after_test_module`

### 2. `docs/COMPLETION_SCORECARD.md` のチェック状態更新

機能受け入れ条件の多くは completion gate と実環境検証で満たしているが、scorecard の checkbox は未チェックのまま。

ただし、外部 CI / release artifact / provider matrix / archive restore が未完了なので、全項目をチェック済みにするのはまだ早い。

必要作業:

- 自動 completion gate で満たした機能項目だけをチェック済みにする。
- 外部検証が必要な運用項目は未チェックのまま残す。
- 各項目に検証コマンドまたは commit / run id を併記する。

### 3. GitHub CLI 認証の確認

`gh` 自体は導入済みだが、このワークスペース環境では `GITHUB_TOKEN` / `GH_TOKEN` が見えていなかった。

CI 状態や workflow artifact を CLI で確認するには、認証情報を shell に渡す必要がある。

## 次の推奨順序

1. GitHub Actions の最新 `main` CI を確認する。
2. `docs/COMPLETION_SCORECARD.md` の機能項目を、通過済み gate に合わせて更新する。
3. GCS S3-compatible endpoint の実検証結果を `docs/PROVIDER_MATRIX.md` に反映する。
4. release workflow artifact を確認する。
5. archive / cold tier restore を AWS S3 などで実検証する。
6. production 用 `~/.majutsu` の encryption 移行を計画する。

