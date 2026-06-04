#!/usr/bin/env bash
set -euo pipefail

CHANNEL=""
BUMP_TYPE="patch"
PLATFORM="all"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--channel) CHANNEL="$2"; shift 2 ;;
    -p|--platform) PLATFORM="$2"; shift 2 ;;
    --major|major) BUMP_TYPE="major"; shift ;;
    --minor|minor) BUMP_TYPE="minor"; shift ;;
    --patch|patch) BUMP_TYPE="patch"; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$PLATFORM" in
  all|linux-x64|linux-arm64|darwin-x64|darwin-arm64|windows-x64) ;;
  *) echo "Invalid platform: $PLATFORM" >&2; exit 2 ;;
esac

export GIT_COMMITTER_NAME="${GIT_COMMITTER_NAME:-Ascii Release Bot}"
export GIT_COMMITTER_EMAIL="${GIT_COMMITTER_EMAIL:-release-bot@ascii.local}"

get_latest_version() {
  git fetch --tags -q 2>/dev/null || true
  {
    git tag -l "game-cli-v*" | sed -nE 's/^game-cli-v([0-9]+\.[0-9]+\.[0-9]+)(-.+)?$/\1/p'
    sed -nE 's/^version = "([0-9]+\.[0-9]+\.[0-9]+)(-.+)?"$/\1/p' cli/Cargo.toml | head -n1
  } | sort -V | tail -n1
}

increment_version() {
  local version="$1" part="$2"
  IFS='.' read -r major minor patch <<< "$version"
  case "$part" in
    major) echo "$((major + 1)).0.0" ;;
    minor) echo "$major.$((minor + 1)).0" ;;
    *) echo "$major.$minor.$((patch + 1))" ;;
  esac
}

latest="$(get_latest_version)"
if [ -z "$latest" ]; then latest="0.0.0"; fi

version="$(increment_version "$latest" "$BUMP_TYPE")"
if [ -n "$CHANNEL" ]; then
  last_num="$(git tag -l "game-cli-v${version}-${CHANNEL}*" | grep -oE "${CHANNEL}[0-9]+" | sed "s/${CHANNEL}//" | sort -n | tail -n1 || echo "0")"
  num="$((${last_num:-0} + 1))"
  version="${version}-${CHANNEL}${num}"
fi

tag="game-cli-v${version}"
head_sha="$(git rev-parse HEAD)"
git tag -a "$tag" -m "game cli release
version=$version
platform=$PLATFORM" "$head_sha"
git push origin "$tag"

echo "Done: $tag"
echo "Platform: $PLATFORM"
echo "Commit: $head_sha"

