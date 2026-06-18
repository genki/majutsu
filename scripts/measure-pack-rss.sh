#!/usr/bin/env bash
set -euo pipefail

if ! command -v /usr/bin/time >/dev/null 2>&1; then
  echo '/usr/bin/time is required' >&2
  exit 2
fi

: "${MAJUTSU_HOME:?set MAJUTSU_HOME to the majutsu state directory to measure}"
: "${MAJUTSU_MEASURE_MAX_RSS_KB:=0}"

mj_bin="${MJ_BIN:-mj}"
if [[ "${MJ_BIN:-}" == "" && -x target/debug/mj ]]; then
  mj_bin="target/debug/mj"
fi

failures=0

write_report() {
  local phase="$1"
  local status="$2"
  local elapsed="$3"
  local max_rss_kb="$4"
  local command="$5"
  if [[ -z "${MAJUTSU_MEASURE_RSS_REPORT:-}" ]]; then
    return 0
  fi
  mkdir -p "$(dirname "$MAJUTSU_MEASURE_RSS_REPORT")"
  if [[ ! -e "$MAJUTSU_MEASURE_RSS_REPORT" ]]; then
    printf 'timestamp\tphase\texit_status\telapsed\tmax_rss_kb\tcommand\n' \
      > "$MAJUTSU_MEASURE_RSS_REPORT"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$phase" "$status" "$elapsed" "$max_rss_kb" "$command" \
    >> "$MAJUTSU_MEASURE_RSS_REPORT"
}

run_phase() {
  local phase="$1"
  shift
  local metrics
  metrics="$(mktemp)"
  set +e
  /usr/bin/time -v -o "$metrics" "$@"
  local status=$?
  set -e

  local elapsed max_rss_kb
  elapsed="$(
    awk -F': ' '/Elapsed .*wall clock.* time/ {print $2}' "$metrics" | tail -1
  )"
  max_rss_kb="$(
    awk -F': ' '/Maximum resident set size/ {print $2}' "$metrics" | tail -1
  )"
  rm -f "$metrics"
  elapsed="${elapsed:-unknown}"
  max_rss_kb="${max_rss_kb:-0}"

  printf 'measure phase=%s exit_status=%s elapsed=%s max_rss_kb=%s\n' \
    "$phase" "$status" "$elapsed" "$max_rss_kb"
  write_report "$phase" "$status" "$elapsed" "$max_rss_kb" "$*"

  if (( MAJUTSU_MEASURE_MAX_RSS_KB > 0 && max_rss_kb > MAJUTSU_MEASURE_MAX_RSS_KB )); then
    printf 'rss regression: %s max_rss_kb=%s exceeds max=%s\n' \
      "$phase" "$max_rss_kb" "$MAJUTSU_MEASURE_MAX_RSS_KB" >&2
    failures=$((failures + 1))
  fi
  if (( status != 0 )); then
    failures=$((failures + 1))
  fi
}

run_phase pack "$mj_bin" pack "$@"
run_phase sync "$mj_bin" sync --wait

if (( failures > 0 )); then
  printf 'measure-pack-rss failed: failures=%s\n' "$failures" >&2
  exit 1
fi
