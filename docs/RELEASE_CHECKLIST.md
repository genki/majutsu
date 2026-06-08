# release checklist

1. ローカル completion check を実行する。

   ```sh
   scripts/check-completion.sh
   ```

2. Podman で S3 互換 MinIO E2E まで含めた completion check を実行する。

   ```sh
   podman info
   MAJUTSU_RUN_MINIO_E2E=1 scripts/check-completion.sh
   ```

3. 実 provider を release ごとに検証し、`docs/PROVIDER_MATRIX.md` に provider 名、検証日、結果を追記する。

   最低限、release candidate ごとに以下を確認する。

   ```sh
   mj remote check
   mj remote fsck
   mj remote fsck --deep
   mj restore prepare --at now --to /tmp/majutsu-restore-smoke
   ```

4. release package を生成する。

   ```sh
   scripts/package-release.sh
   ```

5. release tag を作成する。

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

6. release workflow artifact をダウンロードでき、展開後の `mj --version` が動作することを確認する。
