#!/usr/bin/env bash
set -euo pipefail

mode="${1:-release}"
target_dir="${CARGO_TARGET_DIR:-target}"
dist_dir="${MAJUTSU_DIST_DIR:-${target_dir}/dist}"
mkdir -p "$dist_dir"

if [[ -n "${MAJUTSU_PREBUILT_BIN:-}" ]]; then
  bin="$MAJUTSU_PREBUILT_BIN"
elif [[ "$mode" == "dev" ]]; then
  MAJUTSU_DEV_BUILD=1 cargo build --locked
  bin="$target_dir/debug/mj"
elif [[ "$mode" == "smoke" ]]; then
  cargo build --locked
  bin="$target_dir/debug/mj"
else
  cargo build --release --locked
  bin="$target_dir/release/mj"
fi

if [[ ! -x "$bin" ]]; then
  echo "build 済み binary が見つかりません: $bin" >&2
  exit 1
fi

version="$($bin --version | awk '{print $2}')"
package_version="${version/+/-}"
platform="${MAJUTSU_PACKAGE_PLATFORM:-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m)}"
stage="${dist_dir}/majutsu-${package_version}-${platform}"
rm -rf "$stage"
mkdir -p "$stage"
cp "$bin" "$stage/mj"
cp README.md "$stage/README.md"
if [[ -f LICENSE ]]; then cp LICENSE "$stage/LICENSE"; fi
if [[ -d docs ]]; then cp -R docs "$stage/docs"; fi

if command -v tar >/dev/null 2>&1; then
  tar -C "$dist_dir" -czf "${stage}.tar.gz" "$(basename "$stage")"
fi
if command -v zip >/dev/null 2>&1; then
  (cd "$dist_dir" && zip -qr "$(basename "$stage").zip" "$(basename "$stage")")
fi

echo "release artifact を ${dist_dir}/ に出力しました"
