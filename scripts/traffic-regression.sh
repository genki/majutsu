#!/usr/bin/env bash
set -euo pipefail

mj_bin="${MJ_BIN:-target/debug/mj}"
if [[ ! -x "$mj_bin" ]]; then
  cargo build --locked
fi
mj_bin="${MJ_BIN:-target/debug/mj}"

: "${MAJUTSU_TRAFFIC_MAX_ROOT_ADD_SYNCED:=400}"
: "${MAJUTSU_TRAFFIC_MAX_ROOT_ADD_BYTES:=500000}"
: "${MAJUTSU_TRAFFIC_MAX_NOOP_SYNCED:=0}"
: "${MAJUTSU_TRAFFIC_MAX_NOOP_BYTES:=0}"
: "${MAJUTSU_TRAFFIC_MAX_SMALL_SYNCED:=80}"
: "${MAJUTSU_TRAFFIC_MAX_SMALL_BYTES:=300000}"
: "${MAJUTSU_TRAFFIC_MAX_ELAPSED_MS:=0}"

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
failures=0

case_limit() {
  local name="$1"
  local metric="$2"
  case "$name:$metric" in
    root-add:synced) printf '%s\n' "$MAJUTSU_TRAFFIC_MAX_ROOT_ADD_SYNCED" ;;
    root-add:bytes) printf '%s\n' "$MAJUTSU_TRAFFIC_MAX_ROOT_ADD_BYTES" ;;
    noop:synced) printf '%s\n' "$MAJUTSU_TRAFFIC_MAX_NOOP_SYNCED" ;;
    noop:bytes) printf '%s\n' "$MAJUTSU_TRAFFIC_MAX_NOOP_BYTES" ;;
    *:synced) printf '%s\n' "$MAJUTSU_TRAFFIC_MAX_SMALL_SYNCED" ;;
    *:bytes) printf '%s\n' "$MAJUTSU_TRAFFIC_MAX_SMALL_BYTES" ;;
  esac
}

record_case() {
  local name="$1"
  local elapsed_ms="$2"
  local synced="$3"
  local synced_bytes="$4"
  local skipped="$5"
  if [[ -n "${MAJUTSU_TRAFFIC_REPORT:-}" ]]; then
    mkdir -p "$(dirname "$MAJUTSU_TRAFFIC_REPORT")"
    if [[ ! -e "$MAJUTSU_TRAFFIC_REPORT" ]]; then
      printf 'timestamp\tcase\telapsed_ms\tsynced\tsynced_bytes\tskipped\n' \
        > "$MAJUTSU_TRAFFIC_REPORT"
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      "$name" "$elapsed_ms" "$synced" "$synced_bytes" "$skipped" \
      >> "$MAJUTSU_TRAFFIC_REPORT"
  fi
}

check_case() {
  local name="$1"
  local elapsed_ms="$2"
  local synced="$3"
  local synced_bytes="$4"
  local max_synced max_bytes
  max_synced="$(case_limit "$name" synced)"
  max_bytes="$(case_limit "$name" bytes)"
  if (( synced > max_synced )); then
    printf 'traffic regression: %s synced=%s exceeds max=%s\n' \
      "$name" "$synced" "$max_synced" >&2
    failures=$((failures + 1))
  fi
  if (( synced_bytes > max_bytes )); then
    printf 'traffic regression: %s synced_bytes=%s exceeds max=%s\n' \
      "$name" "$synced_bytes" "$max_bytes" >&2
    failures=$((failures + 1))
  fi
  if (( MAJUTSU_TRAFFIC_MAX_ELAPSED_MS > 0 && elapsed_ms > MAJUTSU_TRAFFIC_MAX_ELAPSED_MS )); then
    printf 'traffic regression: %s elapsed_ms=%s exceeds max=%s\n' \
      "$name" "$elapsed_ms" "$MAJUTSU_TRAFFIC_MAX_ELAPSED_MS" >&2
    failures=$((failures + 1))
  fi
}

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
  synced="${synced:-0}"
  synced_bytes="${synced_bytes:-0}"
  skipped="${skipped:-0}"
  printf '%-18s elapsed_ms=%s synced=%s synced_bytes=%s skipped=%s\n' \
    "$name" "$((end-start))" "$synced" "$synced_bytes" "$skipped"
  record_case "$name" "$((end-start))" "$synced" "$synced_bytes" "$skipped"
  check_case "$name" "$((end-start))" "$synced" "$synced_bytes"
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

if (( failures > 0 )); then
  printf 'traffic regression check failed: failures=%s\n' "$failures" >&2
  exit 1
fi
