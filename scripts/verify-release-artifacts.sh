#!/usr/bin/env bash
set -euo pipefail

# majutsu release を 100% complete と宣言する前に必要な外部証跡を確認する。
# 必要コマンド: gh, tar。artifact が zip の場合は unzip も必要。
# 認証: GH_TOKEN または GITHUB_TOKEN に Actions artifact の読み取り権限が必要。

REPO="${MAJUTSU_REPO:-genki/majutsu}"
COMMIT="${MAJUTSU_VERIFY_COMMIT:-}"
CI_WORKFLOW="${MAJUTSU_CI_WORKFLOW:-ci}"
RELEASE_WORKFLOW="${MAJUTSU_RELEASE_WORKFLOW:-release}"
TAG="${MAJUTSU_RELEASE_TAG:-}"
WORK="${MAJUTSU_VERIFY_WORK:-$(mktemp -d)}"
KEEP_WORK="${MAJUTSU_KEEP_VERIFY_WORK:-0}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "必要なコマンドが見つかりません: $1" >&2
    exit 2
  fi
}

cleanup() {
  local status=$?
  if [[ "$KEEP_WORK" != "1" ]]; then
    rm -rf "$WORK"
  else
    echo "検証用 workdir を保持しました: $WORK" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

need gh
need jq
need awk
need sed

if ! gh auth status -h github.com >/dev/null 2>&1; then
  if [[ -z "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ]]; then
    echo "gh is not authenticated. Set GH_TOKEN or GITHUB_TOKEN, or run gh auth login." >&2
    exit 2
  fi
fi

mkdir -p "$WORK"

echo "repository: $REPO"
if [[ -n "$COMMIT" ]]; then
  echo "target commit: $COMMIT"
else
  COMMIT="$(gh api "repos/$REPO/commits/main" --jq .sha)"
  echo "target commit: $COMMIT (main)"
fi

ci_run_id="$(gh run list \
  --repo "$REPO" \
  --workflow "$CI_WORKFLOW" \
  --commit "$COMMIT" \
  --limit 1 \
  --json databaseId,conclusion,status \
  --jq '.[0].databaseId // empty')"

if [[ -z "$ci_run_id" ]]; then
  echo "commit $COMMIT / workflow $CI_WORKFLOW の CI run が見つかりません" >&2
  exit 1
fi

ci_status="$(gh run view --repo "$REPO" "$ci_run_id" --json status --jq .status)"
ci_conclusion="$(gh run view --repo "$REPO" "$ci_run_id" --json conclusion --jq .conclusion)"
echo "ci_run_id: $ci_run_id status=$ci_status conclusion=$ci_conclusion"
if [[ "$ci_status" != "completed" || "$ci_conclusion" != "success" ]]; then
  gh run view --repo "$REPO" "$ci_run_id" --json jobs --jq '.jobs[] | [.name,.status,.conclusion] | @tsv' || true
  echo "CI が green ではありません" >&2
  exit 1
fi

if [[ -n "$TAG" ]]; then
  release_run_id="$(gh run list \
    --repo "$REPO" \
    --workflow "$RELEASE_WORKFLOW" \
    --branch "$TAG" \
    --limit 1 \
    --json databaseId,conclusion,status \
    --jq '.[0].databaseId // empty')"
else
  release_run_id="$(gh run list \
    --repo "$REPO" \
    --workflow "$RELEASE_WORKFLOW" \
    --limit 1 \
    --json databaseId,conclusion,status \
    --jq '.[0].databaseId // empty')"
fi

if [[ -z "$release_run_id" ]]; then
  echo "release workflow run が見つかりません。先に v* tag push または workflow_dispatch を実行してください。" >&2
  exit 1
fi

release_status="$(gh run view --repo "$REPO" "$release_run_id" --json status --jq .status)"
release_conclusion="$(gh run view --repo "$REPO" "$release_run_id" --json conclusion --jq .conclusion)"
echo "release_run_id: $release_run_id status=$release_status conclusion=$release_conclusion"
if [[ "$release_status" != "completed" || "$release_conclusion" != "success" ]]; then
  gh run view --repo "$REPO" "$release_run_id" --json jobs --jq '.jobs[] | [.name,.status,.conclusion] | @tsv' || true
  echo "release workflow が green ではありません" >&2
  exit 1
fi

artifacts_json="$WORK/artifacts.json"
gh api "repos/$REPO/actions/runs/$release_run_id/artifacts" > "$artifacts_json"
artifact_count="$(jq '.total_count' "$artifacts_json")"
if [[ "$artifact_count" -lt 1 ]]; then
  echo "release workflow が artifact を生成していません" >&2
  exit 1
fi

echo "release artifacts: $artifact_count"
jq -r '.artifacts[] | [.id,.name,.size_in_bytes,.expired] | @tsv' "$artifacts_json"

mkdir -p "$WORK/artifacts"
while read -r artifact_id artifact_name expired; do
  if [[ "$expired" == "true" ]]; then
    echo "artifact が expire しています: $artifact_name" >&2
    exit 1
  fi
  echo "artifact をダウンロードします: $artifact_name ($artifact_id)"
  gh run download --repo "$REPO" "$release_run_id" --name "$artifact_name" --dir "$WORK/artifacts/$artifact_name"
done < <(jq -r '.artifacts[] | [.id,.name,.expired] | @tsv' "$artifacts_json")

verified_linux=0
verified_any=0
while IFS= read -r archive; do
  echo "checking archive: $archive"
  stage="$WORK/extract/$(basename "$archive")"
  mkdir -p "$stage"
  case "$archive" in
    *.tar.gz|*.tgz)
      tar -C "$stage" -xzf "$archive"
      ;;
    *.zip)
      unzip -q "$archive" -d "$stage"
      ;;
    *)
      echo "archive ではない payload を skip します: $archive"
      continue
      ;;
  esac
  mj_path="$(find "$stage" -type f -name mj -perm -111 | head -n 1 || true)"
  if [[ -z "$mj_path" ]]; then
    echo "artifact archive に実行可能な mj が含まれていません: $archive" >&2
    exit 1
  fi
  verified_any=1
  if file "$mj_path" | grep -qiE 'ELF|Linux'; then
    "$mj_path" --version
    "$mj_path" --help >/dev/null
    verified_linux=1
  else
    echo "Linux 以外の mj binary です。archive 構造のみ確認しました: $mj_path"
  fi
done < <(find "$WORK/artifacts" -type f \( -name '*.tar.gz' -o -name '*.tgz' -o -name '*.zip' \))

if [[ "$verified_any" != "1" ]]; then
  echo "ダウンロードした artifact 内に release archive が見つかりません" >&2
  exit 1
fi
if [[ "$(uname -s)" == "Linux" && "$verified_linux" != "1" ]]; then
  echo "この host で実行可能な Linux release artifact が見つかりません" >&2
  exit 1
fi

cat > "$WORK/release-evidence.md" <<EOM
# majutsu release 検証証跡

- repo: $REPO
- commit: $COMMIT
- ci_run_id: $ci_run_id
- release_run_id: $release_run_id
- artifacts: $artifact_count
- verified_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)

結果: PASS
EOM

cat "$WORK/release-evidence.md"
echo "release 検証が通過しました"
