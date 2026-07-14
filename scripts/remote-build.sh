#!/usr/bin/env bash
set -euo pipefail

remote="${MAJUTSU_BUILD_REMOTE:-machinaai}"
target="${1:-aarch64-apple-darwin}"
mode="${2:-release}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
job_id="majutsu-$(date -u +%Y%m%dT%H%M%SZ)-$$"
remote_job="build-service/workspaces/$target"
local_out="${MAJUTSU_REMOTE_BUILD_OUT:-$repo_dir/target/remote-dist/$job_id}"

case "$target" in
  aarch64-apple-darwin|x86_64-apple-darwin|aarch64-unknown-linux-gnu|x86_64-unknown-linux-gnu) ;;
  *) echo "未対応のtargetです: $target" >&2; exit 2 ;;
esac

lock_acquired=false

cleanup() {
  if [[ "$lock_acquired" == true ]]; then
    ssh "$remote" "rm -rf -- \"\$HOME/$remote_job/target\" \"\$HOME/$remote_job/out\" \"\$HOME/build-service/locks/$target\"" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

"$repo_dir/scripts/setup-remote-builder.sh"
ssh "$remote" "
  deadline=\$((SECONDS + 1800))
  until mkdir \"\$HOME/build-service/locks/$target\" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      echo 'target lockの待機がtimeoutしました: $target' >&2
      exit 1
    fi
    sleep 2
  done
  printf '%s\\n' '$job_id' > \"\$HOME/build-service/locks/$target/token\"
"
lock_acquired=true
ssh "$remote" "mkdir -p \"\$HOME/$remote_job/source\""
rsync -a --delete \
  --exclude /.git/ \
  --exclude /target/ \
  --exclude '*.swp' \
  --exclude '.DS_Store' \
  "$repo_dir/" "$remote:$remote_job/source/"

ssh "$remote" "\$HOME/build-service/bin/remote-build-worker.sh \"\$HOME/$remote_job\" '$target' '$mode' '$job_id'"

if [[ "$mode" == "release" ]]; then
  mkdir -p "$local_out"
  rsync -a "$remote:$remote_job/out/" "$local_out/"
  echo "成果物: $local_out/mj"
fi
