#!/bin/bash
# Migrate PDFs from old SQLite pipeline to new file-based structure

set -e

echo "========================================="
echo "Migrating Existing PDFs"
echo "========================================="
echo ""

# Directories
OLD_QUEUE="/home/jic823/projects/def-jic823/pdfs_caribbean/download_queue"
NEW_BASE="/home/jic823/projects/def-jic823/caribbean_pipeline"
NEW_DOWNLOADED="$NEW_BASE/01_downloaded"
NEW_PENDING="$NEW_BASE/02_ocr_pending"

# Check if old queue exists
if [ ! -d "$OLD_QUEUE" ]; then
    echo "ERROR: Old download queue not found at: $OLD_QUEUE"
    exit 1
fi

# Count PDFs
PDF_COUNT=$(ls "$OLD_QUEUE"/*.pdf 2>/dev/null | wc -l)

if [ "$PDF_COUNT" -eq 0 ]; then
    echo "No PDFs found in old queue"
    exit 0
fi

echo "Found $PDF_COUNT PDFs in old queue"
echo "Old location: $OLD_QUEUE"
echo "New location: $NEW_DOWNLOADED"
echo ""

# Create new directories if needed
mkdir -p "$NEW_DOWNLOADED"
mkdir -p "$NEW_PENDING"

echo "Migrating PDFs..."
MIGRATED=0
SKIPPED=0

for pdf_file in "$OLD_QUEUE"/*.pdf; do
    if [ ! -f "$pdf_file" ]; then
        continue
    fi

    filename=$(basename "$pdf_file")
    identifier="${filename%.pdf}"

    # Check if already migrated
    if [ -f "$NEW_DOWNLOADED/$filename" ]; then
        ((SKIPPED++))
        continue
    fi

    # Copy PDF to new location
    cp "$pdf_file" "$NEW_DOWNLOADED/"

    # Create minimal metadata file
    cat > "$NEW_DOWNLOADED/${identifier}.meta.json" <<EOF
{
  "identifier": "$identifier",
  "collection": "caribbean_collection",
  "downloaded_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "filename": "$filename",
  "file_path": "01_downloaded/$filename",
  "file_size": $(stat -c%s "$pdf_file" 2>/dev/null || stat -f%z "$pdf_file" 2>/dev/null),
  "source": "migrated_from_old_pipeline",
  "note": "Metadata recreated during migration"
}
EOF

    # Create symlink in pending directory
    if [ ! -e "$NEW_PENDING/$filename" ]; then
        ln -s "../01_downloaded/$filename" "$NEW_PENDING/$filename"
    fi

    ((MIGRATED++))

    # Progress indicator
    if [ $((MIGRATED % 100)) -eq 0 ]; then
        echo "  Migrated: $MIGRATED PDFs..."
    fi
done

echo ""
echo "========================================="
echo "Migration Complete!"
echo "========================================="
echo "  Migrated: $MIGRATED PDFs"
echo "  Skipped (already exist): $SKIPPED"
echo "  Total: $PDF_COUNT"
echo ""
echo "PDFs are now in:"
echo "  Downloaded: $NEW_DOWNLOADED"
echo "  Pending OCR: $NEW_PENDING"
echo ""
echo "Next steps:"
echo "1. Verify migration: ls $NEW_PENDING/*.pdf | wc -l"
echo "2. Start pipeline: CONFIG_FILE=config/caribbean_filebased.yaml sbatch streaming/run_filebased_pipeline.sh"
echo "3. Dispatcher will batch these PDFs for OCR"
echo ""
echo "Optional: Delete old queue after verifying:"
echo "  rm -rf $OLD_QUEUE/*.pdf"
echo ""
