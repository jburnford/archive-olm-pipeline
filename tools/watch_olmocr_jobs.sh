#!/usr/bin/env bash
set -euo pipefail

# Lightweight watcher for OLMoCR jobs.
# Logs queue/summary and recent error snippets at a fixed interval.
#
# Usage:
#   tools/watch_olmocr_jobs.sh --base-dir /path/to/caribbean_pipeline [--interval 600]
#

BASE_DIR=""
INTERVAL=600

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-dir) BASE_DIR="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac

done

if [[ -z "$BASE_DIR" ]]; then
  echo "Error: --base-dir is required" >&2
  exit 1
fi

LOGFILE="$HOME/olmocr_watch.log"
echo "[watch] starting; base=$BASE_DIR interval=${INTERVAL}s log=$LOGFILE"

tick() {
  local now; now=$(date '+%F %T')
  echo "[watch] $now" | tee -a "$LOGFILE"

  # Queue snapshot
  squeue -u "$USER" -n olmocr_pdf_* -o "%A_%a %j %T %M %L %D %R" | sed -n '1,30p' | tee -a "$LOGFILE" || true

  # Today's sacct summary (last 20)
  sacct -X -S $(date +%Y-%m-%d) -u "$USER" -o JobID,JobName,State,ExitCode,Elapsed,Start,End -P     | grep olmocr_pdf_ | tail -n 20 | tee -a "$LOGFILE" || true

  # Recent logs with ERROR lines
  echo "[watch] recent errors (if any):" | tee -a "$LOGFILE"
  find "$BASE_DIR/03_ocr_processing" -type f -path '*/logs/slurm-*.out' -mmin -30 -print     | head -n 10     | while read -r f; do echo "-- $f" | tee -a "$LOGFILE"; grep -E "ERROR|ValueError" -n "$f" | tail -n 5 | tee -a "$LOGFILE" || true; done

  echo "[watch] tick done" | tee -a "$LOGFILE"
}

trap 'echo "[watch] stopping" | tee -a "$LOGFILE"; exit 0' INT TERM

while true; do
  tick
  sleep "$INTERVAL"
done
