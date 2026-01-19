#!/bin/bash
# Saskatchewan Canadiana Pipeline - Directory Setup Script
#
# Run this on Nibi to set up the required directory structure.
#
# Usage:
#   cd ~/projects/def-jic823/archive-olm-pipeline
#   bash scripts/setup_sask_canadiana.sh

set -e

echo "=========================================="
echo "Saskatchewan Canadiana Pipeline Setup"
echo "=========================================="

BASE_DIR="/home/$USER/projects/def-jic823/saskatchewan_canadiana"

echo "Setting up directories at: $BASE_DIR"

# Create required directories
mkdir -p "$BASE_DIR/01_downloaded"
mkdir -p "$BASE_DIR/02_processed"
mkdir -p "$BASE_DIR/_manifests"
mkdir -p "$BASE_DIR/export"

echo "Created:"
echo "  $BASE_DIR/01_downloaded   - Source PDFs (pre-downloaded)"
echo "  $BASE_DIR/02_processed    - OCR results by identifier"
echo "  $BASE_DIR/_manifests      - State tracking files"
echo "  $BASE_DIR/export          - DuckDB catalog"

# Check if PDFs exist
PDF_COUNT=$(find "$BASE_DIR/01_downloaded" -name "*.pdf" 2>/dev/null | wc -l)
echo ""
echo "Current PDF count: $PDF_COUNT"

if [ "$PDF_COUNT" -eq 0 ]; then
    echo ""
    echo "WARNING: No PDFs found in 01_downloaded/"
    echo "Copy or symlink your pre-downloaded PDFs before running the pipeline."
    echo ""
    echo "Example:"
    echo "  ln -s /path/to/canadiana_pdfs/*.pdf $BASE_DIR/01_downloaded/"
    echo "  # OR"
    echo "  cp /path/to/canadiana_pdfs/*.pdf $BASE_DIR/01_downloaded/"
fi

# Check for metadata files
META_COUNT=$(find "$BASE_DIR/01_downloaded" -name "*.meta.json" 2>/dev/null | wc -l)
echo "Current metadata file count: $META_COUNT"

if [ "$META_COUNT" -eq 0 ] && [ "$PDF_COUNT" -gt 0 ]; then
    echo ""
    echo "WARNING: No metadata files found."
    echo "The pipeline requires .meta.json files alongside PDFs."
    echo "Run the metadata generator if needed."
fi

echo ""
echo "=========================================="
echo "Next Steps"
echo "=========================================="
echo ""
echo "1. Ensure PDFs are in $BASE_DIR/01_downloaded/"
echo ""
echo "2. Test run (single iteration):"
echo "   cd ~/projects/def-jic823/archive-olm-pipeline"
echo "   python3 streaming/sask_orchestrator.py \\"
echo "     --config config/sask_canadiana.yaml \\"
echo "     --max-iterations 1"
echo ""
echo "3. Production run:"
echo "   sbatch streaming/run_sask_pipeline.sh"
echo ""
echo "4. Monitor progress:"
echo "   squeue -u $USER"
echo "   ls $BASE_DIR/02_processed/ | wc -l"
echo ""
echo "5. After main processing, retry failed PDFs:"
echo "   python3 streaming/retry_failed.py \\"
echo "     --config config/sask_canadiana.yaml"
echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
