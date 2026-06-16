#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/publish-crates-io.sh [--execute] [--allow-dirty] [--no-skip-existing]

Publish the public majutsu crate to crates.io.

Default mode is dry-run. Use --execute only after local release checks pass.
Set CARGO_REGISTRY_TOKEN in the environment before --execute.
Already-published package versions are skipped by default.
USAGE
}

execute=0
allow_dirty=0
skip_existing=1

while [ "$#" -gt 0 ]; do
  case "$1" in
    --execute)
      execute=1
      ;;
    --allow-dirty)
      allow_dirty=1
      ;;
    --no-skip-existing)
      skip_existing=0
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

packages=(majutsu)

package_version() {
  local pkg="$1"
  cargo metadata --no-deps --format-version 1 \
    | jq -r --arg pkg "$pkg" '.packages[] | select(.name == $pkg) | .version' \
    | head -n 1
}

crate_version_exists() {
  local pkg="$1"
  local version="$2"
  cargo search "$pkg" --limit 1 2>/dev/null \
    | grep -Eq "^${pkg} = \"${version}\"([[:space:]]|$)"
}

publish_one() {
  local pkg="$1"
  local version
  version="$(package_version "$pkg")"
  if [ -z "$version" ]; then
    echo "could not resolve package version for $pkg" >&2
    return 2
  fi
  if [ "$skip_existing" -eq 1 ] && crate_version_exists "$pkg" "$version"; then
    echo "== skip $pkg $version: already published =="
    return 0
  fi

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
