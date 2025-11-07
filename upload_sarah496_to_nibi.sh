#!/bin/bash
# Upload Sarah496 PDFs to Nibi cluster and set up for OLMoCR processing

set -e

# Configuration
LOCAL_DIR="/mnt/c/Users/jic823/Documents/Sarah496"
NIBI_BASE="nibi:projects/def-jic823"
NIBI_DIR="sarah496_ocr"

echo "========================================="
echo "Sarah496 Upload to Nibi Cluster"
echo "========================================="
echo "Local directory: $LOCAL_DIR"
echo "Nibi destination: $NIBI_BASE/$NIBI_DIR"
echo ""

# Count PDFs
PDF_COUNT=$(ls "$LOCAL_DIR"/*.pdf 2>/dev/null | wc -l)
echo "Found $PDF_COUNT PDF files to upload"
echo ""

# Create directory on Nibi
echo "Creating directory on Nibi..."
ssh nibi "mkdir -p projects/def-jic823/$NIBI_DIR/pdfs"
ssh nibi "mkdir -p projects/def-jic823/$NIBI_DIR/results"
echo "✓ Directories created"
echo ""

# Upload PDFs using rsync (resumable, shows progress)
echo "Uploading PDFs..."
rsync -avP --info=progress2 "$LOCAL_DIR"/*.pdf "$NIBI_BASE/$NIBI_DIR/pdfs/"
echo "✓ Upload complete"
echo ""

# Verify upload
echo "Verifying upload..."
REMOTE_COUNT=$(ssh nibi "ls projects/def-jic823/$NIBI_DIR/pdfs/*.pdf 2>/dev/null | wc -l")
echo "Local PDFs: $PDF_COUNT"
echo "Remote PDFs: $REMOTE_COUNT"

if [ "$PDF_COUNT" -eq "$REMOTE_COUNT" ]; then
    echo "✓ All PDFs uploaded successfully"
else
    echo "⚠ Warning: Count mismatch!"
fi

echo ""
echo "========================================="
echo "Next Steps:"
echo "========================================="
echo "1. PDFs are now on Nibi at: ~/projects/def-jic823/$NIBI_DIR/pdfs/"
echo "2. Ready to submit OLMoCR job"
echo ""
echo "To process with OLMoCR, run on Nibi:"
echo "  cd ~/projects/def-jic823/olmocr"
echo "  ./smart_submit_pdf_jobs.sh --pdf-dir ~/projects/def-jic823/$NIBI_DIR/pdfs"
