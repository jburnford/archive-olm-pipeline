#!/bin/bash
# Setup File-Based Pipeline
# Creates directory structure and initializes manifests

set -e

echo "========================================="
echo "File-Based Pipeline Setup"
echo "========================================="
echo ""

# Configuration
BASE_DIR="/home/jic823/projects/def-jic823/caribbean_pipeline"

echo "Base directory: $BASE_DIR"
echo ""

# Create directory structure
echo "Creating directory structure..."
mkdir -p "$BASE_DIR"/{01_downloaded,02_ocr_pending,03_ocr_processing,04_ocr_completed,05_processed,99_errors/{download_failed,ocr_failed,processing_failed},_manifests}

echo "✓ Created directories:"
tree -L 2 "$BASE_DIR" 2>/dev/null || ls -lR "$BASE_DIR" | head -30

echo ""
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
echo "Directory structure created at: $BASE_DIR"
echo ""
echo "Next steps:"
echo "1. Review configuration: config/caribbean_filebased.yaml"
echo "2. Submit job: CONFIG_FILE=config/caribbean_filebased.yaml sbatch streaming/run_filebased_pipeline.sh"
echo "3. Monitor progress:"
echo "   - Download queue: ls $BASE_DIR/02_ocr_pending/*.pdf | wc -l"
echo "   - OCR batches: ls -d $BASE_DIR/03_ocr_processing/batch_*"
echo "   - Completed: ls $BASE_DIR/04_ocr_completed/*.jsonl | wc -l"
echo ""
echo "View manifests:"
echo "   - Download progress: cat $BASE_DIR/_manifests/download_progress.json"
echo "   - Batch registry: cat $BASE_DIR/_manifests/batches.json"
echo ""
