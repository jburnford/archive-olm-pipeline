#!/bin/bash
# Test Pipeline with Existing 372 PDFs
#
# This script helps test the simplified pipeline with the PDFs already
# downloaded in 01_downloaded/

set -euo pipefail

BASE_DIR="${BASE_DIR:-/home/jic823/projects/def-jic823/caribbean_pipeline}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "========================================="
echo "Pipeline Test Script"
echo "========================================="
echo "Base directory: $BASE_DIR"
echo "Repository: $REPO_DIR"
echo ""

# Step 1: Extract processed IDs
echo "Step 1: Extract processed IDs from 02_processed"
echo "-----------------------------------------"
python3 "$REPO_DIR/tools/extract_processed_ids.py" \
  --processed-dir "$BASE_DIR/02_processed" \
  --output "$BASE_DIR/_manifests/processed_pdfs.json"

echo ""
echo "Step 2: Check unprocessed PDF count"
echo "-----------------------------------------"
TOTAL_PDFS=$(ls "$BASE_DIR/01_downloaded"/*.pdf 2>/dev/null | wc -l)
PROCESSED=$(jq -r '.count' "$BASE_DIR/_manifests/processed_pdfs.json")
UNPROCESSED=$((TOTAL_PDFS - PROCESSED))

echo "Total PDFs: $TOTAL_PDFS"
echo "Processed: $PROCESSED"
echo "Unprocessed: $UNPROCESSED"

if [ "$UNPROCESSED" -lt 10 ]; then
    echo ""
    echo "⚠️  Only $UNPROCESSED unprocessed PDFs"
    echo "   Need at least 10 for a test batch"
    exit 1
fi

echo ""
echo "Step 3: Submit test batch (dry run)"
echo "-----------------------------------------"
python3 "$REPO_DIR/streaming/simple_batch_submitter.py" \
  --config "$REPO_DIR/config/caribbean_filebased.yaml" \
  --batch-size 200 \
  --dry-run

echo ""
read -p "Submit batch for real? (y/N) " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled"
    exit 0
fi

echo ""
echo "Step 4: Submit batch (LIVE)"
echo "-----------------------------------------"
python3 "$REPO_DIR/streaming/simple_batch_submitter.py" \
  --config "$REPO_DIR/config/caribbean_filebased.yaml" \
  --batch-size 200

echo ""
echo "========================================="
echo "✅ Test batch submitted!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Monitor OLMoCR job:"
echo "   squeue -u \$USER"
echo ""
echo "2. Check OLMoCR output:"
echo "   tail -f $BASE_DIR/slurm-*.out | tail -100"
echo ""
echo "3. Wait for results, then run splitter:"
echo "   python3 orchestration/split_jsonl_to_json.py $BASE_DIR/01_downloaded"
echo ""
echo "4. Run finalizer:"
echo "   python3 streaming/simplified_finalizer.py --base-dir $BASE_DIR --auto-delete-pdfs"
echo ""
echo "5. Verify results:"
echo "   ls $BASE_DIR/02_processed/ | wc -l"
echo ""
echo "========================================="
