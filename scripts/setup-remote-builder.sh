#!/usr/bin/env bash
set -euo pipefail

remote="${MAJUTSU_BUILD_REMOTE:-machinaai}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ssh "$remote" 'mkdir -p "$HOME/build-service/bin" "$HOME/build-service/workspaces" "$HOME/build-service/locks" "$HOME/build-service/docker-config" "$HOME/Library/Caches/majutsu-sccache"; printf "%s\n" "{\"auths\":{}}" > "$HOME/build-service/docker-config/config.json"'
scp "$script_dir/remote-build-worker.sh" "$remote:build-service/bin/remote-build-worker.sh"
ssh "$remote" 'mkdir -p "$HOME/build-service/docker"'
scp "$script_dir/remote-builder/Dockerfile" "$remote:build-service/docker/Dockerfile"
ssh "$remote" '
  set -eu
  export PATH="/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"
  chmod 755 "$HOME/build-service/bin/remote-build-worker.sh"
  command -v rustc >/dev/null
  command -v cargo >/dev/null
  command -v sccache >/dev/null
  command -v zig >/dev/null
  command -v cargo-zigbuild >/dev/null
  command -v docker >/dev/null
  DOCKER_HOST="unix://$HOME/.colima/default/docker.sock" DOCKER_CONFIG="$HOME/build-service/docker-config" docker info >/dev/null
'

echo "$remote のremote builderを準備しました"
