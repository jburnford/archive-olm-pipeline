#!/usr/bin/env bash
set -euo pipefail

# Bootstrap external components to pinned SHAs from _manifests/versions.json
# Requires: jq, git

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VERSIONS_FILE="$ROOT_DIR/_manifests/versions.json"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required. Install jq and re-run." >&2
  exit 1
fi

if [ ! -f "$VERSIONS_FILE" ]; then
  echo "Versions file not found: $VERSIONS_FILE" >&2
  exit 1
fi

mapfile -t NODES < <(jq -c '.components[]' "$VERSIONS_FILE")

for node in "${NODES[@]}"; do
  name=$(jq -r '.name' <<<"$node")
  path=$(jq -r '.repo_path' <<<"$node")
  remote=$(jq -r '.remote_url' <<<"$node")
  commit=$(jq -r '.commit' <<<"$node")

  echo "==> $name"

  if [[ "$remote" == "null" || -z "$remote" ]]; then
    echo "   Skipping (no remote_url set)"
    continue
  fi

  if [[ "$commit" == "null" || -z "$commit" || "$commit" == <* ]]; then
    echo "   Warning: commit not pinned for $name; please update _manifests/versions.json"
  fi

  if [ ! -d "$path/.git" ]; then
    mkdir -p "$path"
    git clone "$remote" "$path"
  fi

  (cd "$path" && git fetch --all --tags && {
     if [ -n "$commit" ] && [[ ! "$commit" =~ ^< ]]; then
       git checkout "$commit"
     else
       echo "   No commit pinned; leaving repo at current branch"
     fi
   })
done

echo "\nBootstrap complete."
