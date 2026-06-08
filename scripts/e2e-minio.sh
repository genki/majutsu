#!/usr/bin/env bash
set -euo pipefail

compose_file="docker-compose.minio.yml"
if ! command -v docker >/dev/null 2>&1; then
  echo "MinIO E2E には docker が必要です" >&2
  exit 2
fi

docker compose -f "$compose_file" up -d
cleanup() {
  docker compose -f "$compose_file" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:9000/minio/health/ready >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
export AWS_DEFAULT_REGION=us-east-1
export AWS_SIGNATURE_VERSION=s3v4
export MAJUTSU_S3_MULTIPART_THRESHOLD=$((5 * 1024 * 1024))

cargo build --locked
mj_bin="target/debug/mj"
work="$(mktemp -d)"
trap 'rm -rf "$work"; cleanup' EXIT

home="$work/home"
recovered="$work/recovered"
root="$work/root"
restore="$work/restore"
mkdir -p "$root"
printf 'hello from minio e2e\n' > "$root/file.txt"

"$mj_bin" --home "$home" init --remote s3://majutsu/e2e --host-name minio-e2e
"$mj_bin" --home "$home" root add sample "$root"
"$mj_bin" --home "$home" snapshot --message minio-e2e
"$mj_bin" --home "$home" sync
"$mj_bin" --home "$home" remote check
"$mj_bin" --home "$home" remote fsck
"$mj_bin" --home "$recovered" clone --remote s3://majutsu/e2e
"$mj_bin" --home "$recovered" restore apply --to "$restore"

diff -u "$root/file.txt" "$restore/sample/file.txt"
echo "MinIO E2E が通過しました"
