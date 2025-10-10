#!/bin/bash
#SBATCH --job-name=stream_pipeline
#SBATCH --account=def-jic823
#SBATCH --time=144:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=jic823@usask.ca

# Streaming Pipeline - SLURM Wrapper
# Runs downloader, dispatcher, and cleanup worker concurrently

set -e

# Load configuration
REPO_DIR="${SLURM_SUBMIT_DIR:-/home/jic823/projects/def-jic823/archive-olm-pipeline}"

echo "========================================="
echo "Streaming Archive-OLM Pipeline"
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

# Database copy strategy for NFS reliability
DB_ORIGINAL="${DB_PATH:-/home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db}"
DB_LOCAL="$SLURM_TMPDIR/archive_tracking.db"

echo ""
echo "========================================="
echo "Database Setup (Local Copy Strategy)"
echo "========================================="
echo "Original DB: $DB_ORIGINAL"
echo "Local DB: $DB_LOCAL"

# Function to copy database back to NFS
copy_db_back() {
    local exit_code=$?
    echo ""
    echo "========================================="
    echo "Copying database back to NFS storage..."
    echo "========================================="
    if [ -f "$DB_LOCAL" ]; then
        # Get sizes for verification
        LOCAL_SIZE=$(stat -f%z "$DB_LOCAL" 2>/dev/null || stat -c%s "$DB_LOCAL" 2>/dev/null)
        echo "Local DB size: $(du -h "$DB_LOCAL" | cut -f1)"

        # Copy back with verification
        cp "$DB_LOCAL" "$DB_ORIGINAL.tmp" || {
            echo "ERROR: Failed to copy database back to NFS"
            return 1
        }

        # Verify copy
        COPY_SIZE=$(stat -f%z "$DB_ORIGINAL.tmp" 2>/dev/null || stat -c%s "$DB_ORIGINAL.tmp" 2>/dev/null)
        if [ "$LOCAL_SIZE" = "$COPY_SIZE" ]; then
            mv "$DB_ORIGINAL.tmp" "$DB_ORIGINAL"
            echo "✓ Database successfully copied back to NFS"
        else
            echo "ERROR: Database copy size mismatch!"
            echo "  Local: $LOCAL_SIZE bytes"
            echo "  Copy: $COPY_SIZE bytes"
            return 1
        fi
    else
        echo "WARNING: Local database not found at $DB_LOCAL"
    fi
    return $exit_code
}

# Register trap to copy database back on exit (success or failure)
trap copy_db_back EXIT

# Copy database to local storage
if [ -f "$DB_ORIGINAL" ]; then
    echo "Copying database to local storage..."
    cp "$DB_ORIGINAL" "$DB_LOCAL" || {
        echo "ERROR: Failed to copy database to local storage"
        exit 1
    }
    echo "✓ Database copied to local storage"
    echo "  Size: $(du -h "$DB_LOCAL" | cut -f1)"
else
    echo "WARNING: Original database not found, will create new local database"
    touch "$DB_LOCAL"
fi

# Export local database path for pipeline to use
export PIPELINE_DB_PATH="$DB_LOCAL"
echo "✓ Pipeline will use local database"
echo ""

# Parse command line arguments
CONFIG_FILE="${CONFIG_FILE:-config/pipeline_config.yaml}"
START_FROM="${START_FROM:-0}"
MAX_ITEMS="${MAX_ITEMS:-100000}"

# Allow override via command line
while [[ $# -gt 0 ]]; do
    case "$1" in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --start-from)
            START_FROM="$2"
            shift 2
            ;;
        --max-items)
            MAX_ITEMS="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "Configuration:"
echo "  Config file: $CONFIG_FILE"
echo "  Start from: $START_FROM"
echo "  Max items: $MAX_ITEMS"
echo ""

# Run the streaming pipeline orchestrator
cd "$REPO_DIR"

python3 streaming/stream_orchestrator.py \
    --config "$CONFIG_FILE" \
    --start-from "$START_FROM" \
    --max-items "$MAX_ITEMS"

EXIT_CODE=$?

echo ""
echo "========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Streaming pipeline completed successfully"
else
    echo "✗ Streaming pipeline failed with exit code: $EXIT_CODE"
fi
echo "========================================="

exit $EXIT_CODE
