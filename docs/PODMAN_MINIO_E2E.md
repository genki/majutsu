# Podman MinIO E2E

`scripts/e2e-minio.sh` は Docker Compose ではなく、Podman だけで MinIO を起動して S3 互換 remote を検証する。

## 前提

```sh
podman info
curl --version
cargo --version
```

rootless Podman が使えない環境では、次のように sudo 経由で実行できる。

```sh
MAJUTSU_PODMAN_SUDO=1 scripts/e2e-minio.sh
```

## 実行内容

スクリプトは次を実行する。

1. 一時 Podman network を作成する。
2. `docker.io/minio/minio:latest` を起動する。
3. `docker.io/minio/mc:latest` で `majutsu` bucket を作成する。
4. `mj init --remote s3://majutsu/e2e` を実行する。
5. 通常ファイルと large object を snapshot / sync する。
6. `remote check` / `remote fsck` を実行する。
7. 空の state directory へ clone する。
8. restore 結果を byte-for-byte で検証する。

## 便利な環境変数

| 変数 | 既定値 | 用途 |
|---|---:|---|
| `MAJUTSU_PODMAN_BIN` | `podman` | Podman binary path |
| `MAJUTSU_PODMAN_SUDO` | `0` | `1` で `sudo podman` を使う |
| `MAJUTSU_MINIO_PORT` | `9000` | host 側 S3 API port |
| `MAJUTSU_MINIO_CONSOLE_PORT` | `9001` | host 側 console port |
| `MAJUTSU_MINIO_IMAGE` | `docker.io/minio/minio:latest` | MinIO image |
| `MAJUTSU_MC_IMAGE` | `docker.io/minio/mc:latest` | MinIO client image |
| `MJ_BIN` | `target/debug/mj` | 検証対象 binary |

port が既に使われている場合:

```sh
MAJUTSU_MINIO_PORT=19000 MAJUTSU_MINIO_CONSOLE_PORT=19001 scripts/e2e-minio.sh
```

CI と同じ sudo 実行に寄せたい場合:

```sh
MAJUTSU_PODMAN_SUDO=1 scripts/e2e-minio.sh
```
