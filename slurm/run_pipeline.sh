#!/bin/bash
#SBATCH --job-name=archive_pipeline
#SBATCH --account=def-jic823
#SBATCH --time=72:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=jic823@usask.ca

# Archive-OLM Pipeline - SLURM Wrapper
# Runs the full pipeline in batches on NIBI cluster

set -e

# Load configuration
# Use SLURM_SUBMIT_DIR (directory where sbatch was run) or hardcoded path
REPO_DIR="${SLURM_SUBMIT_DIR:-/home/jic823/projects/def-jic823/archive-olm-pipeline}"

echo "========================================="
echo "Archive-OLM Pipeline"
echo "========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Repository: $REPO_DIR"
echo ""

# Load Python module
module load python/3.11 || {
    echo "ERROR: Failed to load Python module"
    exit 1
}

# Activate virtual environment (from IA_downloader_cluster)
# Assumes venv was created in InternetArchive repo
VENV_DIR="${VENV_DIR:-/home/jic823/projects/def-jic823/InternetArchive/venv}"

if [ ! -d "$VENV_DIR" ]; then
    echo "ERROR: Virtual environment not found at: $VENV_DIR"
    echo "Please run setup_venv.sh from InternetArchive repo first"
    exit 1
fi

echo "Activating virtual environment: $VENV_DIR"
source "$VENV_DIR/bin/activate" || {
    echo "ERROR: Failed to activate virtual environment"
    exit 1
}

# Parse command line arguments
CONFIG_FILE="${CONFIG_FILE:-config/pipeline_config.yaml}"
TOTAL_ITEMS="${TOTAL_ITEMS:-100000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
START_BATCH="${START_BATCH:-1}"
NO_CLEANUP="${NO_CLEANUP:-false}"

# Allow override via command line
while [[ $# -gt 0 ]]; do
    case "$1" in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --total-items)
            TOTAL_ITEMS="$2"
            shift 2
            ;;
        --batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --start-batch)
            START_BATCH="$2"
            shift 2
            ;;
        --no-cleanup)
            NO_CLEANUP=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "Configuration:"
echo "  Config file: $CONFIG_FILE"
echo "  Total items: $TOTAL_ITEMS"
echo "  Batch size: $BATCH_SIZE"
echo "  Start batch: $START_BATCH"
echo "  Auto cleanup: $([ "$NO_CLEANUP" = "true" ] && echo "No" || echo "Yes")"
echo ""

# Run the pipeline orchestrator
cd "$REPO_DIR"

CLEANUP_FLAG=""
if [ "$NO_CLEANUP" = "true" ]; then
    CLEANUP_FLAG="--no-cleanup"
fi

python3 orchestration/pipeline_orchestrator.py \
    --config "$CONFIG_FILE" \
    run-batches \
    --total-items "$TOTAL_ITEMS" \
    --batch-size "$BATCH_SIZE" \
    --start-batch "$START_BATCH" \
    $CLEANUP_FLAG

EXIT_CODE=$?

echo ""
echo "========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Pipeline completed successfully"
else
    echo "✗ Pipeline failed with exit code: $EXIT_CODE"
fi
echo "========================================="

exit $EXIT_CODE
