#!/bin/bash
#SBATCH --job-name=file_pipeline
#SBATCH --account=def-jic823
#SBATCH --time=144:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=jic823@usask.ca

# File-Based Streaming Pipeline - SLURM Wrapper
# No database copying - all tracking via JSON files

set -e

# Load configuration
REPO_DIR="${SLURM_SUBMIT_DIR:-/home/jic823/projects/def-jic823/archive-olm-pipeline}"

echo "========================================="
echo "File-Based Streaming Archive-OLM Pipeline"
echo "========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Repository: $REPO_DIR"
echo ""

# Load Python module
module load python/3.11 || {
    echo "ERROR: Failed to load Python module"
    exit 1
}

# Activate virtual environment
VENV_DIR="${VENV_DIR:-/home/jic823/projects/def-jic823/InternetArchive/venv}"

if [ ! -d "$VENV_DIR" ]; then
    echo "ERROR: Virtual environment not found at: $VENV_DIR"
    exit 1
fi

echo "Activating virtual environment: $VENV_DIR"
source "$VENV_DIR/bin/activate" || {
    echo "ERROR: Failed to activate virtual environment"
    exit 1
}

# Parse command line arguments
CONFIG_FILE="${CONFIG_FILE:-config/caribbean_filebased.yaml}"
START_FROM="${START_FROM:-0}"
MAX_ITEMS="${MAX_ITEMS:-100000}"

echo ""
echo "Configuration:"
echo "  Config file: $CONFIG_FILE"
echo "  Start from: $START_FROM"
echo "  Max items: $MAX_ITEMS"
echo ""

# Run the file-based streaming pipeline orchestrator
cd "$REPO_DIR"

python3 streaming/file_based_orchestrator.py \
    --config "$CONFIG_FILE" \
    --start-from "$START_FROM" \
    --max-items "$MAX_ITEMS"

EXIT_CODE=$?

echo ""
echo "========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ File-based streaming pipeline completed successfully"
else
    echo "✗ File-based streaming pipeline failed with exit code: $EXIT_CODE"
fi
echo "========================================="

exit $EXIT_CODE
