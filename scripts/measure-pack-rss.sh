#!/usr/bin/env bash
set -euo pipefail

if ! command -v /usr/bin/time >/dev/null 2>&1; then
  echo '/usr/bin/time is required' >&2
  exit 2
fi

: "${MAJUTSU_HOME:?set MAJUTSU_HOME to the majutsu state directory to measure}"

/usr/bin/time -v mj pack "$@"
/usr/bin/time -v mj sync --wait
