#!/bin/bash
#SBATCH --account=def-jic823
#SBATCH --gres=gpu:h100:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=jic823@mail.usask.ca
# NOTE: Time, job name, and output file are now set by the sbatch command in the manager script.

echo "--- OLMoCR PDF Chunk Processor ---"
echo "Job ID: $SLURM_JOB_ID, Array Task ID: $SLURM_ARRAY_TASK_ID"

# === CONFIGURATION (reads from environment) ===
if [ -z "$PDF_DIR" ]; then
    echo "❌ ERROR: PDF_DIR environment variable is not set."
    exit 1
fi

# === APPTAINER/SYSTEM TEMP SETUP ===
# Large arrays can exhaust or race on node /tmp. Use per-task scratch tmp.
JOB_TMP_BASE="${SLURM_TMPDIR:-/scratch/$USER/tmp}"
JOB_TASK_TMP="$JOB_TMP_BASE/olmocr_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$JOB_TASK_TMP" "$JOB_TASK_TMP/runtime" || { echo "❌ ERROR: Unable to create job temp dir: $JOB_TASK_TMP"; exit 1; }
export TMPDIR="$JOB_TASK_TMP"
export APPTAINER_TMPDIR="$JOB_TASK_TMP"
export SINGULARITY_TMPDIR="$JOB_TASK_TMP"  # backward compatibility
export XDG_RUNTIME_DIR="$JOB_TASK_TMP/runtime"

COMPLETED_LOG="$PDF_DIR/processed_files.log"
CHUNK_DIR="$PDF_DIR/chunks"
LOCK_DIR="$PDF_DIR/log.lock"
FINAL_RESULTS_DIR="$PDF_DIR/results"

# === DYNAMIC PATHS ===
CHUNK_FILE="$CHUNK_DIR/chunk_${SLURM_ARRAY_TASK_ID}.txt"
# Use per-task output to avoid collisions across array tasks
OUTPUT_DIR="/scratch/$USER/olmocr_pdf_results/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"

# (The rest of the script remains identical to the previous version)

# 1. Check if the chunk file exists
if [ ! -f "$CHUNK_FILE" ]; then
    echo "❌ ERROR: Chunk file not found: $CHUNK_FILE"
    exit 1
fi

# ... (rest of the script: read files, run olmocr, save results, update log) ...
# (No changes are needed from the previous correct version for the rest of this file)
mapfile -t PDF_FILENAMES < "$CHUNK_FILE"
echo "Processing ${#PDF_FILENAMES[@]} PDFs from $PDF_DIR"
mkdir -p "$OUTPUT_DIR"
PROCESS_ARGS=""
for pdf in "${PDF_FILENAMES[@]}"; do
    PROCESS_ARGS="$PROCESS_ARGS \"$PDF_DIR/$pdf\""
done
OLMOCR_DIR="/home/jic823/projects/def-jic823/olmocr"
CONTAINER_PATH="$OLMOCR_DIR/olmocr.sif"
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
module load apptainer
# Optional olmOCR flags for controlling processing (STABLE defaults)
# For stability, we use minimal flags. Only enable advanced options if needed.
OLMOCR_FLAGS="--markdown"
if [ -n "$WORKERS" ] && [ "$WORKERS" != "0" ]; then
  OLMOCR_FLAGS="$OLMOCR_FLAGS --workers $WORKERS"
fi
if [ -n "$PAGES_PER_GROUP" ] && [ "$PAGES_PER_GROUP" != "0" ]; then
  OLMOCR_FLAGS="$OLMOCR_FLAGS --pages_per_group $PAGES_PER_GROUP"
fi

echo "Using TMPDIR: $TMPDIR"
echo "Chunk file: $CHUNK_FILE"
echo "Output dir: $OUTPUT_DIR"
echo "Container: $CONTAINER_PATH"
echo "OLMoCR flags: ${OLMOCR_FLAGS:-<none - using defaults for stability>}"

eval "apptainer run --nv \
  --env REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
  --env SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
  $CONTAINER_PATH \
  python -m olmocr.pipeline $OUTPUT_DIR --pdfs $PROCESS_ARGS $OLMOCR_FLAGS"
if [ $? -ne 0 ]; then
    echo "❌ ERROR: OLMoCR process failed for this chunk. Not updating log."
    exit 1
fi
echo "✅ OLMoCR complete. Saving results and updating log..."
mkdir -p "$FINAL_RESULTS_DIR"
cp -r $OUTPUT_DIR/* "$FINAL_RESULTS_DIR/"
while ! mkdir "$LOCK_DIR" 2>/dev/null; do
    echo "Waiting for log file lock..."
    sleep 2
done
printf "%s\n" "${PDF_FILENAMES[@]}" >> "$COMPLETED_LOG"
rmdir "$LOCK_DIR"
echo "Log updated. Cleaning up temporary scratch space."
rm -rf "$OUTPUT_DIR"
rm -rf "$JOB_TASK_TMP"
echo "--- Task $SLURM_ARRAY_TASK_ID Complete ---"
