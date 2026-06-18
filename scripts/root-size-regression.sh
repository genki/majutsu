#!/usr/bin/env bash
set -euo pipefail

mj_bin="${MJ_BIN:-mj}"
if [[ "${MJ_BIN:-}" == "" && -x target/debug/mj ]]; then
  mj_bin="target/debug/mj"
fi

: "${MAJUTSU_ROOT_SIZE_EXACT:=0}"
: "${MAJUTSU_ROOT_SIZE_MAX_CURRENT_CLIENT_RATIO:=5.0}"
: "${MAJUTSU_ROOT_SIZE_MAX_PREFIX_CURRENT_RATIO:=10.0}"
: "${MAJUTSU_ROOT_SIZE_MIN_CLIENT_BYTES:=16777216}"

mj_args=()
if [[ -n "${MAJUTSU_HOME:-}" ]]; then
  mj_args+=(--home "$MAJUTSU_HOME")
fi

if [[ "$MAJUTSU_ROOT_SIZE_EXACT" == "1" ]]; then
  json="$(env MAJUTSU_ROOT_SIZE_FORCE_SCAN=1 "$mj_bin" "${mj_args[@]}" root size --json)"
else
  json="$("$mj_bin" "${mj_args[@]}" root size --json)"
fi

ROOT_SIZE_JSON="$json" python3 - "$MAJUTSU_ROOT_SIZE_MAX_CURRENT_CLIENT_RATIO" \
  "$MAJUTSU_ROOT_SIZE_MAX_PREFIX_CURRENT_RATIO" \
  "$MAJUTSU_ROOT_SIZE_MIN_CLIENT_BYTES" \
  "${MAJUTSU_ROOT_SIZE_REPORT:-}" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

max_current_client = float(sys.argv[1])
max_prefix_current = float(sys.argv[2])
min_client_bytes = int(sys.argv[3])
report_path = sys.argv[4]

report = json.loads(os.environ["ROOT_SIZE_JSON"])
roots = report.get("roots", [])
totals = report.get("totals", {})
client_bytes = sum(int(row.get("client_bytes") or 0) for row in roots)
current_backend = int(totals.get("current_backend_bytes") or 0)
prefix_bytes = int(totals.get("backend_prefix_bytes") or 0)
prefix_exact = bool(totals.get("backend_prefix_exact", True))
prefix_scope = totals.get("backend_prefix_scope") or ("exact" if prefix_exact else "unknown")

failures = []
current_client_ratio = None
if client_bytes >= min_client_bytes and client_bytes > 0:
    current_client_ratio = current_backend / client_bytes
    if current_client_ratio > max_current_client:
        failures.append(
            f"current_backend/client ratio {current_client_ratio:.3f} exceeds {max_current_client:.3f}"
        )

prefix_current_ratio = None
if prefix_exact and current_backend > 0:
    prefix_current_ratio = prefix_bytes / current_backend
    if prefix_current_ratio > max_prefix_current:
        failures.append(
            f"backend_prefix/current_backend ratio {prefix_current_ratio:.3f} exceeds {max_prefix_current:.3f}"
        )

print(
    "root-size-regression "
    f"roots={len(roots)} "
    f"client_bytes={client_bytes} "
    f"current_backend_bytes={current_backend} "
    f"current_client_ratio={current_client_ratio if current_client_ratio is not None else 'skipped'} "
    f"backend_prefix_bytes={prefix_bytes} "
    f"backend_prefix_exact={str(prefix_exact).lower()} "
    f"backend_prefix_scope={prefix_scope} "
    f"prefix_current_ratio={prefix_current_ratio if prefix_current_ratio is not None else 'skipped'}"
)

if report_path:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with path.open("a", encoding="utf-8") as handle:
        if new_file:
            handle.write(
                "timestamp\troots\tclient_bytes\tcurrent_backend_bytes\t"
                "current_client_ratio\tbackend_prefix_bytes\tbackend_prefix_exact\t"
                "backend_prefix_scope\tprefix_current_ratio\tfailures\n"
            )
        handle.write(
            "\t".join(
                [
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    str(len(roots)),
                    str(client_bytes),
                    str(current_backend),
                    "" if current_client_ratio is None else f"{current_client_ratio:.6f}",
                    str(prefix_bytes),
                    str(prefix_exact).lower(),
                    str(prefix_scope),
                    "" if prefix_current_ratio is None else f"{prefix_current_ratio:.6f}",
                    "|".join(failures),
                ]
            )
            + "\n"
        )

if failures:
    for failure in failures:
        print(f"root size regression: {failure}", file=sys.stderr)
    sys.exit(1)
PY
