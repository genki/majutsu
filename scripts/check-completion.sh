#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo clippy --workspace --all-targets
RUST_TEST_THREADS=1 cargo test --workspace --all-targets -- --nocapture
RUST_TEST_THREADS=1 cargo test --test e2e_local -- --nocapture
scripts/package-release.sh smoke

echo "majutsu completion check が通過しました"
