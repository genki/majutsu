#!/usr/bin/env bash
set -euo pipefail

mj_bin="${MJ_BIN:-target/debug/mj}"
if [[ ! -x "$mj_bin" ]]; then
  cargo build --locked
fi
mj_bin="${MJ_BIN:-target/debug/mj}"

work="$(mktemp -d)"
cleanup() {
  chmod -R u+w "$work" 2>/dev/null || true
  rm -rf "$work" 2>/dev/null || true
}
trap cleanup EXIT

home="$work/home"
root="$work/root"
remote="$work/remote"
mkdir -p "$root"

run_case() {
  local name="$1"
  shift
  local start end out
  start=$(date +%s%3N)
  out=$("$@" 2>&1)
  end=$(date +%s%3N)
  local synced synced_bytes skipped
  synced=$(grep -E '^synced [0-9]+' <<<"$out" | tail -1 | awk '{print $2}' || true)
  synced_bytes=$(grep -E '^synced_bytes ' <<<"$out" | tail -1 | awk '{print $2}' || true)
  skipped=$(grep -E '^skipped_uploads ' <<<"$out" | tail -1 | awk '{print $2}' || true)
  printf '%-18s elapsed_ms=%s synced=%s synced_bytes=%s skipped=%s\n' \
    "$name" "$((end-start))" "${synced:-?}" "${synced_bytes:-?}" "${skipped:-?}"
}

"$mj_bin" --home "$home" init --remote "file://$remote" --host-name traffic-regression >/dev/null
for i in $(seq 1 100); do printf 'line %04d\n' "$i" > "$root/file-$i.txt"; done
"$mj_bin" --home "$home" root add sample "$root" >/dev/null
"$mj_bin" --home "$home" snapshot --message root-add >/dev/null
run_case root-add "$mj_bin" --home "$home" sync --wait

"$mj_bin" --home "$home" snapshot --message noop >/dev/null || true
run_case noop "$mj_bin" --home "$home" sync --wait

printf 'new small\n' > "$root/new-small.txt"
"$mj_bin" --home "$home" snapshot --message add-small >/dev/null
run_case add-small "$mj_bin" --home "$home" sync --wait

printf 'edited small\n' >> "$root/new-small.txt"
"$mj_bin" --home "$home" snapshot --message edit-small >/dev/null
run_case edit-small "$mj_bin" --home "$home" sync --wait

rm "$root/new-small.txt"
"$mj_bin" --home "$home" snapshot --message delete-small >/dev/null
run_case delete-small "$mj_bin" --home "$home" sync --wait

"$mj_bin" --home "$home" root size --json >/dev/null
