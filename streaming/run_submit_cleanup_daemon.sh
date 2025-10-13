#!/bin/bash
#SBATCH --job-name=submit_cleanup_daemon
#SBATCH --time=144:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --account=def-jic823
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=jic823@usask.ca

# Dedicated submit/split/finalize daemon
# - Submits all unprocessed PDFs to OLMoCR (page-capped chunks)
# - Splits completed JSONL into per-PDF JSON
# - Finalizes results into 02_processed/ and deletes original PDFs

set -euo pipefail

echo "========================================="
echo "Submit + Cleanup Daemon"
echo "========================================="
echo "Job ID: ${SLURM_JOB_ID:-n/a}"
echo "Node: ${SLURM_NODELIST:-n/a}"
echo "Start time: $(date)"
echo "========================================="

# Load Python and activate venv
if command -v module >/dev/null 2>&1; then
  module load python/3.11 || true
fi

VENV_DIR="/home/jic823/projects/def-jic823/InternetArchive/venv"
source "$VENV_DIR/bin/activate"

cd "$HOME/projects/def-jic823/archive-olm-pipeline"

CONFIG="config/caribbean_filebased.yaml"
BASE="/home/jic823/projects/def-jic823/caribbean_pipeline"
PDFDIR="$BASE/01_downloaded"

while true; do
  echo "=== Phase: Submit ==="
  python3 streaming/simple_batch_submitter.py --config "$CONFIG" --batch-size 1 || true

  echo "=== Phase: Split ==="
  find "$PDFDIR" -type f \( -path "*/results/*.jsonl" -o -path "*/results/results/*.jsonl" \) -printf '%h\n' 2>/dev/null \
    | sed 's#/results$##' | sort -u \
    | while read -r B; do [ -d "$B" ] && python3 orchestration/split_jsonl_to_json.py "$B" || true; done

  echo "=== Phase: Finalize ==="
  find "$PDFDIR" -type d -path "*/results/json" -print 2>/dev/null \
    | while read -r JDIR; do
        if ls "$JDIR"/*.json >/dev/null 2>&1; then
          python3 streaming/simplified_finalizer.py --base-dir "$BASE" --results-dir "$JDIR" --auto-delete-pdfs || true
        fi
      done

  echo "=== Sleep 5m ==="
  sleep 300
done

echo "========================================="
echo "Submit + Cleanup daemon finished"
echo "End time: $(date)"
echo "========================================="

