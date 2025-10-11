#!/usr/bin/env bash
set -euo pipefail

# Periodically split new JSONL results into per-PDF JSON and finalize outputs.
#
# Usage:
#   tools/run_split_finalize_loop.sh --base-dir /path/to/caribbean_pipeline [--interval 1800]
#
# Notes:
# - Designed to be run from repo root (so relative Python scripts resolve).
# - Safe to run repeatedly; steps are idempotent.

BASE_DIR=""
INTERVAL="1800" # 30 minutes default

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-dir)
      BASE_DIR="$2"; shift 2 ;;
    --interval)
      INTERVAL="$2"; shift 2 ;;
    *)
      echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$BASE_DIR" ]]; then
  echo "Error: --base-dir is required" >&2
  exit 1
fi

if [[ ! -d "$BASE_DIR" ]]; then
  echo "Error: base dir not found: $BASE_DIR" >&2
  exit 1
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

echo "[loop] Starting split+finalize loop"
echo "[loop] Base dir: $BASE_DIR"
echo "[loop] Interval: ${INTERVAL}s"

tick() {
  local now
  now=$(date '+%F %T')
  echo "[loop] $now - tick"

  local changed=0
  # Split per batch if any JSONL present
  while IFS= read -r -d $'\0' B; do
    if find "$B/results" -type f -name '*.jsonl' 2>/dev/null | grep -q .; then
      echo "[loop] Splitting $(basename "$B")"
      python3 orchestration/split_jsonl_to_json.py "$B" || true
      changed=$((changed+1))
    fi
  done < <(find "$BASE_DIR/03_ocr_processing" -maxdepth 1 -type d -name 'batch_*' -print0 | sort -z)

  echo "[loop] Batches split: $changed"

  # Finalize and free space
  echo "[loop] Finalizing"
  python3 streaming/file_based_finalize.py --base-dir "$BASE_DIR" || true

  # Short summary
  local cnt_json cnt_ocr cnt_md
  cnt_json=$(find "$BASE_DIR/03_ocr_processing" -type f -path '*/results/json/*.json' 2>/dev/null | wc -l)
  cnt_ocr=$(find "$BASE_DIR/05_processed" -type f -name '*.ocr.json' 2>/dev/null | wc -l)
  cnt_md=$(find "$BASE_DIR/05_processed" -type f -name '*.md' 2>/dev/null | wc -l)
  echo "[loop] Summary: results/json: $cnt_json, processed OCR: $cnt_ocr, processed MD: $cnt_md"
}

trap 'echo "[loop] Caught signal, exiting"; exit 0' INT TERM

while true; do
  tick
  sleep "$INTERVAL"
done

