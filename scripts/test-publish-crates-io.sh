#!/usr/bin/env bash
# 外部公開を行わず、publish失敗の伝播とrate limit後の再試行を検証する。
set -euo pipefail
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin"
cat > "$work/bin/cargo" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  metadata) printf '%s\n' '{"packages":[{"name":"majutsu","version":"0.8.4"}]}' ;;
  publish)
    case "$PUBLISH_TEST_MODE" in
      success) exit 0 ;;
      failure) echo '模擬した公開失敗' >&2; exit 37 ;;
      rate-limit)
        if [[ ! -e "$PUBLISH_TEST_STATE" ]]; then
          touch "$PUBLISH_TEST_STATE"
          echo 'Too Many Requests' >&2
          exit 101
        fi
        exit 0 ;;
    esac ;;
  *) exit 99 ;;
esac
STUB
chmod 755 "$work/bin/cargo"

check() {
  local mode="$1" expected="$2" status=0
  # これはfake cargo専用の非秘密fixtureであり、実際の公開先へは送信しない。
  PATH="$work/bin:$PATH" CARGO_REGISTRY_TOKEN=publish-test-fixture \
    PUBLISH_TEST_MODE="$mode" PUBLISH_TEST_STATE="$work/retried" PUBLISH_RETRY_SECS=0 \
    bash scripts/publish-crates-io.sh --execute --no-skip-existing > "$work/output" 2>&1 || status="$?"
  if [[ "$status" != "$expected" ]]; then
    echo "publish helper test失敗: mode=$mode expected=$expected actual=$status" >&2
    exit 1
  fi
}
check success 0
check failure 37
check rate-limit 0
test -e "$work/retried"
echo 'publish helperの3ケースが成功しました'
