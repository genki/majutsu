# release checklist

1. ローカル completion check を実行する。

   ```sh
   scripts/check-completion.sh
   ```

2. Podman で S3 互換 MinIO E2E を実行する。

   ```sh
   podman info
   scripts/e2e-minio.sh
   ```

3. 暗号化 disaster recovery を検証する。

   ```sh
   mj init --encrypt --remote file:///tmp/majutsu-remote
   mj key export > /tmp/majutsu-master-key.txt
   # snapshot、sync、MAJUTSU_MASTER_KEY を使った clone、restore、fsck を確認する
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

6. release workflow artifact をダウンロードでき、`mj --version` が動作することを確認する。
