#!/usr/bin/env bash
set -euo pipefail

job_dir="${1:?job directory is required}"
target="${2:?target is required}"
mode="${3:-release}"
lock_token="${4:?lock token is required}"

case "$job_dir" in
  "$HOME/build-service/workspaces/"*) ;;
  *) echo "不正なjob directoryです: $job_dir" >&2; exit 2 ;;
esac

case "$target" in
  aarch64-apple-darwin|x86_64-apple-darwin|aarch64-unknown-linux-gnu|x86_64-unknown-linux-gnu) ;;
  *) echo "未対応のtargetです: $target" >&2; exit 2 ;;
esac

case "$mode" in
  check|clippy|release|test) ;;
  *) echo "未対応のmodeです: $mode" >&2; exit 2 ;;
esac

export PATH="/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"
export RUSTC_WRAPPER="$(command -v sccache)"
export SCCACHE_DIR="$HOME/Library/Caches/majutsu-sccache"
export SCCACHE_CACHE_SIZE="20G"
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$job_dir/target"
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export DOCKER_CONFIG="$HOME/build-service/docker-config"

source_dir="$job_dir/source"
out_dir="$job_dir/out"
lock_dir="$HOME/build-service/locks/$target"
mkdir -p "$out_dir" "$SCCACHE_DIR"
if [[ ! -f "$lock_dir/token" ]] || [[ "$(cat "$lock_dir/token")" != "$lock_token" ]]; then
  echo "target lockを所有していません: $target" >&2
  exit 2
fi

cd "$source_dir"

if [[ "$target" == *-unknown-linux-gnu ]]; then
  case "$target" in
    aarch64-unknown-linux-gnu) arch="arm64" ;;
    x86_64-unknown-linux-gnu) arch="amd64" ;;
  esac
  platform="linux/arm64"
  image="majutsu-builder:linux-cross-v3"
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    docker build --platform "$platform" -t "$image" "$HOME/build-service/docker"
  fi
  cargo_home="$HOME/Library/Caches/majutsu-cargo-linux"
  mkdir -p "$CARGO_TARGET_DIR" "$HOME/Library/Caches/majutsu-sccache-linux-$arch" "$cargo_home"
  chmod 777 "$CARGO_TARGET_DIR" "$HOME/Library/Caches/majutsu-sccache-linux-$arch" "$cargo_home"
  docker_args=(
    run --rm --platform "$platform"
    --user "$(id -u):$(id -g)"
    --hostname vagrant
    -e HOME=/tmp/majutsu-builder-home
    -e CARGO_HOME=/cargo
    -v "$source_dir:/workspace:ro"
    -v "$CARGO_TARGET_DIR:/target"
    -v "$cargo_home:/cargo"
    -v "$HOME/Library/Caches/majutsu-sccache-linux-$arch:/sccache"
    -w /workspace
    "$image"
  )
  if [[ "$mode" == "test" ]]; then
    if [[ "$target" != "aarch64-unknown-linux-gnu" ]]; then
      echo "Linux testはnative ARM64 targetだけで利用できます" >&2
      exit 2
    fi
    docker "${docker_args[@]}" env RUST_TEST_THREADS=1 cargo test --workspace --all-targets --locked
  elif [[ "$mode" == "clippy" ]]; then
    if [[ "$target" != "aarch64-unknown-linux-gnu" ]]; then
      echo "Linux clippyはnative ARM64 targetだけで利用できます" >&2
      exit 2
    fi
    docker "${docker_args[@]}" cargo clippy --workspace --all-targets --locked -- -D warnings
  elif [[ "$mode" == "check" ]]; then
    docker "${docker_args[@]}" cargo zigbuild --target "$target" --locked
  else
    docker "${docker_args[@]}" cargo zigbuild --release --target "$target" --locked
    cp "$CARGO_TARGET_DIR/$target/release/mj" "$out_dir/mj"
    chmod 755 "$out_dir/mj"
    (cd "$out_dir" && shasum -a 256 mj > mj.sha256)
  fi
elif [[ "$mode" == "test" ]]; then
  if [[ "$target" != "aarch64-apple-darwin" ]]; then
    echo "test modeはmachinaaiのnative targetだけで利用できます" >&2
    exit 2
  fi
  rustup target add "$target" >/dev/null
  HOSTNAME=vagrant TMPDIR=/private/tmp RUST_TEST_THREADS=1 cargo test --workspace --all-targets --locked
elif [[ "$mode" == "clippy" ]]; then
  if [[ "$target" != "aarch64-apple-darwin" ]]; then
    echo "macOS clippyはnative ARM64 targetだけで利用できます" >&2
    exit 2
  fi
  cargo clippy --workspace --all-targets --locked -- -D warnings
elif [[ "$mode" == "check" ]]; then
  rustup target add "$target" >/dev/null
  cargo build --target "$target" --locked
else
  rustup target add "$target" >/dev/null
  cargo build --release --target "$target" --locked
  cp "$CARGO_TARGET_DIR/$target/release/mj" "$out_dir/mj"
  chmod 755 "$out_dir/mj"
  (cd "$out_dir" && shasum -a 256 mj > mj.sha256)
  sccache --show-stats
fi
