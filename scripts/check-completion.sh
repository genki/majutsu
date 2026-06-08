#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo clippy --workspace --all-targets
RUST_TEST_THREADS=1 cargo test --workspace --all-targets -- --nocapture
RUST_TEST_THREADS=1 cargo test --test e2e_local -- --nocapture
scripts/package-release.sh smoke

if [[ "${MAJUTSU_RUN_MINIO_E2E:-0}" == "1" ]]; then
  scripts/e2e-minio.sh
else
  echo "MinIO Podman E2E は MAJUTSU_RUN_MINIO_E2E=1 のときだけ実行します"
fi

echo "majutsu completion check が通過しました"
