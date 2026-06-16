#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/sync-internal-crates.sh [--check]

Synchronize the publish-facing src/internal/*.rs files from the private
workspace support crates under crates/majutsu-*/src/lib.rs.

Default mode updates src/internal and removes stale generated files. Use
--check in release gates to fail when the generated files are stale or when
unexpected mirror files remain.
USAGE
}

check=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --check)
      check=1
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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

modules=(
  "majutsu-cli:majutsu_cli"
  "majutsu-core:majutsu_core"
  "majutsu-crypto:majutsu_crypto"
  "majutsu-daemon:majutsu_daemon"
  "majutsu-db:majutsu_db"
  "majutsu-large:majutsu_large"
  "majutsu-pack:majutsu_pack"
  "majutsu-policy:majutsu_policy"
  "majutsu-restore:majutsu_restore"
  "majutsu-store:majutsu_store"
  "majutsu-watch:majutsu_watch"
)

mkdir -p src/internal

stale=0
expected_files=()
for module in "${modules[@]}"; do
  crate="${module%%:*}"
  target="${module##*:}"
  src="crates/${crate}/src/lib.rs"
  dst="src/internal/${target}.rs"
  expected_files+=("$dst")

  if [ ! -f "$src" ]; then
    echo "missing source: $src" >&2
    stale=1
    continue
  fi

  if [ "$check" -eq 1 ]; then
    if [ ! -f "$dst" ]; then
      echo "missing generated file: $dst" >&2
      stale=1
      continue
    fi
    if ! cmp -s "$src" "$dst"; then
      echo "stale generated file: $dst differs from $src" >&2
      stale=1
    fi
  else
    cp "$src" "$dst"
  fi
done

for generated in src/internal/*.rs; do
  [ -e "$generated" ] || continue
  expected=0
  for dst in "${expected_files[@]}"; do
    if [ "$generated" = "$dst" ]; then
      expected=1
      break
    fi
  done
  if [ "$expected" -eq 0 ]; then
    if [ "$check" -eq 1 ]; then
      echo "unexpected generated file: $generated" >&2
      stale=1
    else
      rm -f "$generated"
      echo "removed unexpected generated file: $generated"
    fi
  fi
done

if [ "$stale" -ne 0 ]; then
  echo "run scripts/sync-internal-crates.sh to refresh src/internal" >&2
  exit 1
fi

if [ "$check" -eq 1 ]; then
  echo "src/internal support crate mirror is up to date"
else
  echo "src/internal support crate mirror refreshed"
fi
