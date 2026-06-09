#!/usr/bin/env bash
set -euo pipefail

# Glacier 系 archive restore の実 provider 検証。
# 実 AWS S3 credential、bucket、archive restore 待ち時間が必要なため、
# default completion gate には含めない。
#
# 必須 env:
#   MAJUTSU_AWS_ARCHIVE_BUCKET=<bucket>
# 任意 env:
#   MAJUTSU_AWS_ARCHIVE_PREFIX=majutsu-archive-e2e-<timestamp>
#   MAJUTSU_AWS_ARCHIVE_STORAGE_CLASS=GLACIER_IR|GLACIER|DEEP_ARCHIVE
#   MAJUTSU_AWS_ARCHIVE_RESTORE_TIER=Expedited|Standard|Bulk
#   MAJUTSU_AWS_ARCHIVE_RESTORE_DAYS=2

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "必要なコマンドが見つかりません: $1" >&2
    exit 2
  fi
}

need aws
need cargo

bucket="${MAJUTSU_AWS_ARCHIVE_BUCKET:?set MAJUTSU_AWS_ARCHIVE_BUCKET}"
prefix="${MAJUTSU_AWS_ARCHIVE_PREFIX:-majutsu-archive-e2e-$(date -u +%Y%m%dT%H%M%SZ)}"
storage_class="${MAJUTSU_AWS_ARCHIVE_STORAGE_CLASS:-GLACIER_IR}"
restore_tier="${MAJUTSU_AWS_ARCHIVE_RESTORE_TIER:-Standard}"
restore_days="${MAJUTSU_AWS_ARCHIVE_RESTORE_DAYS:-2}"
region="${AWS_DEFAULT_REGION:-us-east-1}"
remote="s3://${bucket}/${prefix}"
work="$(mktemp -d)"
resume="${1:-}"
cleanup() {
  local status=$?
  if [[ "${MAJUTSU_KEEP_AWS_ARCHIVE_E2E:-0}" != "1" ]]; then
    rm -rf "$work"
  else
    echo "workdir を保持しました: $work" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

cargo build --locked
mj_bin="${MJ_BIN:-target/debug/mj}"

home="$work/home"
recovered="$work/recovered"
root="$work/root"
restore="$work/restore"
mkdir -p "$root"

export AWS_SIGNATURE_VERSION=s3v4
export AWS_DEFAULT_REGION="$region"

write_large_fixture() {
  local path="$1"
  local block="majutsu aws archive restore deterministic payload 2026-06-09\n"
  : > "$path"
  for _ in $(seq 1 160000); do
    printf '%s' "$block" >> "$path"
  done
}

if [[ "$resume" == "--resume" ]]; then
  write_large_fixture "$root/archive-large.bin"
  "$mj_bin" --home "$recovered" clone --remote "$remote"
  "$mj_bin" --home "$recovered" restore prepare --to "$restore"
  "$mj_bin" --home "$recovered" restore apply --to "$restore"
  cmp "$root/archive-large.bin" "$restore/sample/archive-large.bin"
  echo "AWS archive restore E2E が通過しました"
  exit 0
elif [[ -n "$resume" ]]; then
  echo "未知の引数です: $resume" >&2
  exit 2
fi

printf 'archive restore smoke\n' > "$root/file.txt"
write_large_fixture "$root/archive-large.bin"

"$mj_bin" --home "$home" init --remote "$remote" --host-name aws-archive-e2e
"$mj_bin" --home "$home" root add sample "$root" \
  --large-min-size $((1 * 1024 * 1024)) \
  --large-binary-min-size $((1 * 1024 * 1024)) \
  --large-chunk-size $((1 * 1024 * 1024))
"$mj_bin" --home "$home" snapshot --message aws-archive-e2e
"$mj_bin" --home "$home" sync
"$mj_bin" --home "$home" remote check
"$mj_bin" --home "$home" remote fsck

first_s3_key() {
  aws s3api list-objects-v2 \
    --bucket "$bucket" \
    --prefix "$1" \
    --query 'Contents[0].Key' \
    --output text
}

local_key="$(first_s3_key "${prefix}/objects/large/chunks/fixed/")"
canonical_key="$(first_s3_key "${prefix}/large/chunks/")"
archive_keys=()
if [[ -n "$local_key" && "$local_key" != "None" ]]; then
  archive_keys+=("$local_key")
fi
if [[ -n "$canonical_key" && "$canonical_key" != "None" && "$canonical_key" != "$local_key" ]]; then
  archive_keys+=("$canonical_key")
fi
if [[ "${#archive_keys[@]}" -eq 0 ]]; then
  echo "${prefix} 配下に archive 対象の large chunk key が見つかりません" >&2
  exit 1
fi

for key in "${archive_keys[@]}"; do
  echo "payload object を archive storage class へ移動します: s3://${bucket}/${key} -> ${storage_class}"
  aws s3api copy-object \
    --bucket "$bucket" \
    --key "$key" \
    --copy-source "${bucket}/${key}" \
    --metadata-directive COPY \
    --storage-class "$storage_class" >/dev/null
done

# provider が Glacier 形式の restore request を受けることを確認する。
# 通常の復旧経路では、この後の --resume で mj restore prepare / apply を実行する。
restore_request="{\"Days\":${restore_days},\"GlacierJobParameters\":{\"Tier\":\"${restore_tier}\"}}"
for key in "${archive_keys[@]}"; do
  aws s3api restore-object --bucket "$bucket" --key "$key" --restore-request "$restore_request" || true
done

cat <<EOM
Archive restore request を送信しました。
provider 側の restore 完了後、次を実行してください:

  MAJUTSU_AWS_ARCHIVE_BUCKET=$bucket \\
  MAJUTSU_AWS_ARCHIVE_PREFIX=$prefix \\
  AWS_DEFAULT_REGION=$region \\
  $0 --resume

EOM
