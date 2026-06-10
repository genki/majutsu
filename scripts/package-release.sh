#!/usr/bin/env bash
set -euo pipefail

mode="${1:-release}"
mkdir -p dist

if [[ "$mode" == "smoke" ]]; then
  cargo build --locked
  bin="target/debug/mj"
else
  cargo build --release --locked
  bin="target/release/mj"
fi

if [[ ! -x "$bin" ]]; then
  echo "build 済み binary が見つかりません: $bin" >&2
  exit 1
fi

version="$($bin --version | awk '{print $2}')"
package_version="${version/+/-}"
platform="$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m)"
stage="dist/majutsu-${package_version}-${platform}"
rm -rf "$stage"
mkdir -p "$stage"
cp "$bin" "$stage/mj"
cp README.md "$stage/README.md"
if [[ -f LICENSE ]]; then cp LICENSE "$stage/LICENSE"; fi
if [[ -d docs ]]; then cp -R docs "$stage/docs"; fi

if command -v tar >/dev/null 2>&1; then
  tar -C dist -czf "${stage}.tar.gz" "$(basename "$stage")"
fi
if command -v zip >/dev/null 2>&1; then
  (cd dist && zip -qr "$(basename "$stage").zip" "$(basename "$stage")")
fi

echo "release artifact を dist/ に出力しました"
