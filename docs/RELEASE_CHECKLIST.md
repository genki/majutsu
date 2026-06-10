# release checklist

1. ローカル completion check を実行する。

   ```sh
   scripts/check-completion.sh
   MAJUTSU_RUN_MINIO_E2E=1 scripts/check-completion.sh
   ```

2. production と同じ種類の remote で暗号化 disaster recovery を検証する。

   ```sh
   MAJUTSU_ENCRYPTED_REMOTE=s3://bucket/prefix scripts/verify-encrypted-remote-recovery.sh
   ```

3. Podman で S3 互換 MinIO E2E を実行する。

   ```sh
   podman info
   scripts/e2e-minio.sh
   ```

4. release package を生成する。

   ```sh
   scripts/package-release.sh
   ```

5. `BUILD_NUMBER` を確認し、ある程度まとまった実装単位・検証単位ごとにインクリメントする。

   ```sh
   cat BUILD_NUMBER
   mj --version
   ```

6. release tag を作成する。

   ```sh
   git tag v0.3.0
   git push origin v0.3.0
   ```

7. release artifact をローカルで展開して smoke test を実行する。

   ```sh
   tar -tf dist/majutsu-*.tar.gz
   tmp=$(mktemp -d)
   tar -xzf dist/majutsu-*.tar.gz -C "$tmp"
   "$tmp"/majutsu-*/mj --version
   "$tmp"/majutsu-*/mj --help
   ```

8. provider matrix を更新する。

   - File remote と MinIO はローカル completion gate の evidence を記録する。
   - GCS S3-compatible endpoint を supported にする場合は実 backend の検証日とコマンドを記録する。
   - AWS S3 / Cloudflare R2 は、その release candidate で実検証していない限り experimental のままにする。

9. archive restore を supported とする場合だけ、実 provider で cold-tier drill を実行する。

   ```sh
   MAJUTSU_AWS_ARCHIVE_BUCKET=... scripts/e2e-aws-archive-restore.sh
   # provider restore 完了後
   MAJUTSU_AWS_ARCHIVE_BUCKET=... MAJUTSU_AWS_ARCHIVE_PREFIX=... scripts/e2e-aws-archive-restore.sh --resume
   ```

10. ローカル生成した release artifact で `mj --version` と `mj --help` が動作することを release note に記録する。
