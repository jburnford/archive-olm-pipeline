#!/bin/bash
#SBATCH --job-name=simple_pipeline
#SBATCH --time=144:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --account=def-jic823
#SBATCH --mail-type=ALL
#SBATCH --mail-user=jic823@usask.ca

# Simple Pipeline Orchestrator SLURM Job
# Runs the simplified 2-directory pipeline with smart backpressure

set -euo pipefail

echo "========================================="
echo "Simple Pipeline Orchestrator"
echo "========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Start time: $(date)"
echo "========================================="

# Configuration
CONFIG_FILE="${CONFIG_FILE:-config/caribbean_filebased.yaml}"
CHECK_INTERVAL="${CHECK_INTERVAL:-60}"

# Load Python and activate virtual environment used by downloader/olmocr tools
if command -v module >/dev/null 2>&1; then
  module load python/3.11 || {
    echo "ERROR: Failed to load Python module" >&2
    exit 1
  }
fi

VENV_DIR="${VENV_DIR:-/home/jic823/projects/def-jic823/InternetArchive/venv}"

if [ ! -d "$VENV_DIR" ]; then
  echo "ERROR: Virtual environment not found at: $VENV_DIR" >&2
  exit 1
fi

echo "Activating virtual environment: $VENV_DIR"
# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate" || {
  echo "ERROR: Failed to activate virtual environment" >&2
  exit 1
}

# Change to repo directory
cd ~/projects/def-jic823/archive-olm-pipeline

# Run orchestrator
python3 streaming/simple_orchestrator.py \
  --config "$CONFIG_FILE" \
  --check-interval "$CHECK_INTERVAL" \
  --max-iterations -1

echo ""
echo "========================================="
echo "Pipeline complete"
echo "End time: $(date)"
echo "========================================="
