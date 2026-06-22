#!/usr/bin/env bash
set -euo pipefail

CONTAINER_BIN="${MAJUTSU_CONTAINER_BIN:-}"
IMAGE="${MAJUTSU_DEMO_IMAGE:-docker.io/library/ubuntu:24.04}"
MJ_BIN="${MJ_BIN:-target/release/mj}"
WORK=""

if [[ -z "$CONTAINER_BIN" ]]; then
  if command -v podman >/dev/null 2>&1; then
    CONTAINER_BIN=podman
  elif command -v docker >/dev/null 2>&1; then
    CONTAINER_BIN=docker
  else
    echo "podman または docker が必要です" >&2
    exit 2
  fi
fi

if [[ ! -x "$MJ_BIN" ]]; then
  cargo build --locked --release
fi

MJ_BIN_ABS="$(cd "$(dirname "$MJ_BIN")" && pwd)/$(basename "$MJ_BIN")"
WORK="$(mktemp -d)"

cleanup() {
  local status=$?
  if [[ -n "$WORK" && -d "$WORK" ]]; then
    rm -rf "$WORK"
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

cat >"$WORK/demo.sh" <<'DEMO'
set -eu

HOME=/demo/home
export HOME

mkdir -p /demo/root /demo/restore
printf '# Demo notes\n\nfirst line\n' >/demo/root/README.md

echo '$ mj --home /demo/state init --remote file:///demo/remote --host-name clean-container-demo'
mj --home /demo/state init --remote file:///demo/remote --host-name clean-container-demo

echo
echo '$ mj --home /demo/state root add notes /demo/root'
mj --home /demo/state root add notes /demo/root

echo
echo '$ mj --home /demo/state snapshot --message initial'
mj --home /demo/state snapshot --message initial

printf '\nsecond line\n' >>/demo/root/README.md
printf 'temporary idea\n' >/demo/root/idea.txt

echo
echo '$ mj --home /demo/state snapshot --message edited'
mj --home /demo/state snapshot --message edited

sleep 2

echo
echo '$ mj --home /demo/state log --root notes --limit 5'
mj --home /demo/state log --root notes --limit 5

echo
echo '$ mj --home /demo/state state 1s -r notes'
mj --home /demo/state state 1s -r notes || true

echo
echo '$ mj --home /demo/state sync --wait'
mj --home /demo/state sync --wait

echo
echo '$ mj --home /demo/state restore plan --root notes --ago 1s --to /demo/restore'
mj --home /demo/state restore plan --root notes --ago 1s --to /demo/restore

echo
echo '$ mj --home /demo/state restore apply --root notes --ago 1s --to /demo/restore'
mj --home /demo/state restore apply --root notes --ago 1s --to /demo/restore

echo
echo '$ find /demo/restore -maxdepth 3 -type f -print -exec sed -n "1,5p" {} \;'
find /demo/restore -maxdepth 3 -type f -print -exec sed -n '1,5p' {} \;
DEMO

"$CONTAINER_BIN" run --rm \
  --volume "$MJ_BIN_ABS:/usr/local/bin/mj:ro" \
  --volume "$WORK:/demo:rw" \
  --workdir /demo \
  "$IMAGE" \
  /bin/sh /demo/demo.sh
