#!/bin/bash
#SBATCH --job-name=sarah496_ocr
#SBATCH --account=def-jic823
#SBATCH --time=08:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=8
#SBATCH --gres=gpu:h100:1
#SBATCH --output=%x-%j.out

# OLMoCR Processing Script for Sarah496 Collection
# 33 PDFs, ~2.5GB, estimated ~3000-4000 pages total

set -e

echo "========================================="
echo "Sarah496 OLMoCR Processing"
echo "========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Start time: $(date)"
echo ""

# Configuration (env-overridable)
PDF_DIR="${PDF_DIR:-$HOME/projects/def-jic823/sarah496_ocr/pdfs}"
OUTPUT_DIR="${OUTPUT_DIR:-$HOME/projects/def-jic823/sarah496_ocr/results}"
CONTAINER="${CONTAINER:-$HOME/projects/def-jic823/olmocr/olmocr.sif}"

# Load Apptainer/Singularity if modules are available (Nibi usage)
if command -v module >/dev/null 2>&1; then
    module load apptainer 2>/dev/null || module load singularity 2>/dev/null || true
fi

# Verify setup
echo "Checking environment..."
if [ ! -f "$CONTAINER" ]; then
    echo "ERROR: OLMoCR container not found at $CONTAINER"
    exit 1
fi

if [ ! -d "$PDF_DIR" ]; then
    echo "ERROR: PDF directory not found at $PDF_DIR"
    exit 1
fi

PDF_COUNT=$(ls "$PDF_DIR"/*.pdf 2>/dev/null | wc -l)
echo "Found $PDF_COUNT PDFs to process"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Set up temp directory (and apptainer tmp/runtime)
export TMPDIR="${SLURM_TMPDIR:-$TMPDIR}"
mkdir -p "$TMPDIR" "$TMPDIR/runtime"
export APPTAINER_TMPDIR="$TMPDIR"
export SINGULARITY_TMPDIR="$TMPDIR"
export XDG_RUNTIME_DIR="$TMPDIR/runtime"

# TLS certs (defensive for network calls inside container)
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

echo "Using temp directory: $TMPDIR"
echo ""

# Run OLMoCR
echo "Starting OLMoCR processing..."
echo "========================================="

# Build per-PDF arguments for the pipeline entrypoint
PDF_ARGS=""
shopt -s nullglob
for f in "$PDF_DIR"/*.pdf; do
    base=$(basename "$f")
    PDF_ARGS="$PDF_ARGS --pdfs \"/pdfs/$base\""
done
shopt -u nullglob

# Execute using the same pipeline entrypoint used by our chunk scripts
eval "apptainer run --nv \
    --bind \"$PDF_DIR:/pdfs:ro\" \
    --bind \"$OUTPUT_DIR:/output:rw\" \
    --bind \"$TMPDIR:/tmp\" \
    \"$CONTAINER\" \
    python -m olmocr.pipeline /output $PDF_ARGS --workers 6"

echo ""
echo "========================================="
echo "Processing Complete"
echo "========================================="
echo "End time: $(date)"
echo ""

# Count results
RESULT_COUNT=$(ls "$OUTPUT_DIR"/*.jsonl 2>/dev/null | wc -l)
echo "Results: $RESULT_COUNT JSONL files created"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Calculate statistics
if [ -d "$OUTPUT_DIR" ]; then
    TOTAL_SIZE=$(du -sh "$OUTPUT_DIR" | cut -f1)
    echo "Total output size: $TOTAL_SIZE"
fi

echo ""
echo "To view results, check: $OUTPUT_DIR"
