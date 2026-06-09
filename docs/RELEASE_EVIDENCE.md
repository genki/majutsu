# release 証跡テンプレート

このファイルは release ごとにコピーして、`docs/releases/<tag>.md` または
release note に貼り付ける。

## release

- tag:
- commit:
- date UTC:

## local completion gate

```sh
MAJUTSU_RUN_MINIO_E2E=1 scripts/check-completion.sh
```

- 結果:
- host:
- 補足:

## GitHub Actions

```sh
GH_TOKEN=... MAJUTSU_RELEASE_TAG=<tag> scripts/verify-release-artifacts.sh
```

- CI workflow run id:
- release workflow run id:
- Linux artifact:
- macOS artifact:
- `mj --version`:
- `mj --help` 確認: yes/no

## provider validation

| Provider | Status | Evidence |
|---|---|---|
| File remote | CI verified | |
| MinIO via Podman | CI verified | |
| GCS S3-compatible endpoint | verified / not used | |
| AWS S3 | supported / experimental | |
| Cloudflare R2 | supported / experimental | |

## archive restore

- provider:
- storage class:
- restore tier:
- object key:
- restore requested at:
- restore completed at:
- `mj restore prepare` 結果:
- `mj restore apply` 結果:

## encryption

- production state encryption: enabled / disabled
- master key export 手順記録: yes/no
- encrypted clone / restore drill: pass/fail
