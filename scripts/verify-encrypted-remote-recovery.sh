#!/usr/bin/env bash
set -euo pipefail

# encrypted disaster recovery を file:// または s3:// remote で検証する。
# MAJUTSU_ENCRYPTED_REMOTE が未指定なら一時 file remote を使う。

cargo build --locked
mj_bin="${MJ_BIN:-target/debug/mj}"
work="$(mktemp -d)"
cleanup() {
  local status=$?
  if [[ "${MAJUTSU_KEEP_ENCRYPTED_E2E:-0}" != "1" ]]; then
    rm -rf "$work"
  else
    echo "workdir を保持しました: $work" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

remote="${MAJUTSU_ENCRYPTED_REMOTE:-file://${work}/remote}"
home="$work/encrypted-home"
recovered="$work/recovered"
root="$work/root"
restore="$work/restore"
mkdir -p "$root"
printf 'encrypted recovery\n' > "$root/secret.txt"

"$mj_bin" --home "$home" init --encrypt --remote "$remote" --host-name encrypted-e2e
"$mj_bin" --home "$home" root add secret "$root"
"$mj_bin" --home "$home" snapshot --message encrypted-e2e
"$mj_bin" --home "$home" sync
key="$($mj_bin --home "$home" key export)"
MAJUTSU_MASTER_KEY="$key" "$mj_bin" --home "$recovered" clone --remote "$remote"
"$mj_bin" --home "$recovered" fsck
"$mj_bin" --home "$recovered" restore apply --to "$restore"
cmp "$root/secret.txt" "$restore/secret/secret.txt"
echo "encrypted remote recovery が通過しました: $remote"
