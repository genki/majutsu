#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/publish-crates-io.sh [--execute] [--allow-dirty]

Publish majutsu workspace crates to crates.io in dependency order.

Default mode is dry-run. Use --execute only after local release checks pass.
Set CARGO_REGISTRY_TOKEN in the environment before --execute.
USAGE
}

execute=0
allow_dirty=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --execute)
      execute=1
      ;;
    --allow-dirty)
      allow_dirty=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [ "$execute" -eq 1 ] && [ -z "${CARGO_REGISTRY_TOKEN:-}" ]; then
  echo "CARGO_REGISTRY_TOKEN is required for --execute" >&2
  exit 2
fi

packages=(
  majutsu-core
  majutsu-cli
  majutsu-crypto
  majutsu-daemon
  majutsu-db
  majutsu-policy
  majutsu-watch
  majutsu-large
  majutsu-pack
  majutsu-restore
  majutsu-store
  majutsu
)

publish_one() {
  local pkg="$1"
  local args=(-p "$pkg")
  if [ "$allow_dirty" -eq 1 ]; then
    args+=(--allow-dirty)
  fi
  if [ "$execute" -eq 0 ]; then
    args+=(--dry-run)
  fi

  while true; do
    echo "== cargo publish ${args[*]} =="
    local tmp
    tmp="$(mktemp)"
    if cargo publish "${args[@]}" >"$tmp" 2>&1; then
      cat "$tmp"
      rm -f "$tmp"
      return 0
    fi
    local status="$?"
    cat "$tmp"
    if [ "$execute" -eq 1 ] && grep -q "Too Many Requests" "$tmp"; then
      rm -f "$tmp"
      echo "crates.io rate limited $pkg; sleeping ${PUBLISH_RETRY_SECS:-610}s" >&2
      sleep "${PUBLISH_RETRY_SECS:-610}"
      continue
    fi
    rm -f "$tmp"
    return "$status"
  done
}

for pkg in "${packages[@]}"; do
  publish_one "$pkg"
done
