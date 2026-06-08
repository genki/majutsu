#!/usr/bin/env bash
set -euo pipefail

PODMAN_BIN="${MAJUTSU_PODMAN_BIN:-podman}"
PODMAN_SUDO="${MAJUTSU_PODMAN_SUDO:-0}"
MINIO_PORT="${MAJUTSU_MINIO_PORT:-9000}"
MINIO_CONSOLE_PORT="${MAJUTSU_MINIO_CONSOLE_PORT:-9001}"
MINIO_IMAGE="${MAJUTSU_MINIO_IMAGE:-docker.io/minio/minio:latest}"
MC_IMAGE="${MAJUTSU_MC_IMAGE:-docker.io/minio/mc:latest}"
E2E_ID="${MAJUTSU_E2E_ID:-$$}"
network="majutsu-e2e-${E2E_ID}"
minio_name="majutsu-minio-${E2E_ID}"
work=""

if ! command -v "$PODMAN_BIN" >/dev/null 2>&1; then
  echo "MinIO E2E には podman が必要です" >&2
  echo "例: sudo apt-get install -y podman / brew install podman" >&2
  exit 2
fi

podman_cmd() {
  if [[ "$PODMAN_SUDO" == "1" ]]; then
    sudo "$PODMAN_BIN" "$@"
  else
    "$PODMAN_BIN" "$@"
  fi
}

cleanup() {
  local status=$?
  if [[ "$status" -ne 0 ]]; then
    echo "MinIO E2E が失敗しました。Podman logs を表示します。" >&2
    podman_cmd logs "$minio_name" >&2 2>/dev/null || true
  fi
  if [[ -n "$work" && -d "$work" ]]; then
    rm -rf "$work"
  fi
  podman_cmd rm -f "$minio_name" >/dev/null 2>&1 || true
  podman_cmd network rm "$network" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT INT TERM

if ! podman_cmd info >/dev/null 2>&1; then
  echo "podman が利用可能ですが、現在のユーザーでは実行できません" >&2
  echo "rootless 設定を確認するか、MAJUTSU_PODMAN_SUDO=1 scripts/e2e-minio.sh を試してください" >&2
  exit 2
fi

podman_cmd network create "$network" >/dev/null
podman_cmd run \
  --detach \
  --replace \
  --name "$minio_name" \
  --network "$network" \
  --publish "127.0.0.1:${MINIO_PORT}:9000" \
  --publish "127.0.0.1:${MINIO_CONSOLE_PORT}:9001" \
  --env MINIO_ROOT_USER=minioadmin \
  --env MINIO_ROOT_PASSWORD=minioadmin \
  "$MINIO_IMAGE" \
  server /data --console-address ':9001' >/dev/null

ready=0
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "$ready" != "1" ]]; then
  echo "MinIO が ready になりませんでした" >&2
  exit 3
fi

podman_cmd run \
  --rm \
  --network "$network" \
  --entrypoint /bin/sh \
  "$MC_IMAGE" \
  -c "mc alias set local http://${minio_name}:9000 minioadmin minioadmin && mc mb --ignore-existing local/majutsu" >/dev/null

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL="http://127.0.0.1:${MINIO_PORT}"
export AWS_DEFAULT_REGION=us-east-1
export AWS_SIGNATURE_VERSION=s3v4
export MAJUTSU_S3_MULTIPART_THRESHOLD=$((5 * 1024 * 1024))

cargo build --locked
mj_bin="${MJ_BIN:-target/debug/mj}"
work="$(mktemp -d)"

home="$work/home"
recovered="$work/recovered"
root="$work/root"
restore="$work/restore"
mkdir -p "$root"
printf 'hello from minio e2e\n' > "$root/file.txt"
dd if=/dev/zero of="$root/large.bin" bs=1M count=6 status=none

"$mj_bin" --home "$home" init --remote s3://majutsu/e2e --host-name minio-e2e
"$mj_bin" --home "$home" root add sample "$root" \
  --large-min-size $((1 * 1024 * 1024)) \
  --large-binary-min-size $((1 * 1024 * 1024)) \
  --large-chunk-size $((1 * 1024 * 1024))
"$mj_bin" --home "$home" snapshot --message minio-e2e
"$mj_bin" --home "$home" sync
"$mj_bin" --home "$home" remote check
"$mj_bin" --home "$home" remote fsck
"$mj_bin" --home "$home" remote hosts
"$mj_bin" --home "$home" remote host minio-e2e --snapshots --operations
"$mj_bin" --home "$recovered" clone --remote s3://majutsu/e2e
"$mj_bin" --home "$recovered" restore apply --to "$restore"

diff -u "$root/file.txt" "$restore/sample/file.txt"
cmp "$root/large.bin" "$restore/sample/large.bin"
echo "MinIO E2E が通過しました"
