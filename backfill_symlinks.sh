#!/bin/bash
# Backfill symlinks for existing PDFs that don't have them

BASE="/home/jic823/projects/def-jic823/caribbean_pipeline"
DOWNLOADED="$BASE/01_downloaded"
PENDING="$BASE/02_ocr_pending"

echo "=================================="
echo "Backfilling Symlinks"
echo "=================================="
echo ""

CREATED=0
SKIPPED=0

for pdf in "$DOWNLOADED"/*.pdf; do
    if [ ! -f "$pdf" ]; then
        continue
    fi

    filename=$(basename "$pdf")
    symlink="$PENDING/$filename"

    if [ -L "$symlink" ]; then
        ((SKIPPED++))
    else
        ln -s "$pdf" "$symlink"
        ((CREATED++))

        if [ $((CREATED % 50)) -eq 0 ]; then
            echo "Created $CREATED symlinks..."
        fi
    fi
done

echo ""
echo "=================================="
echo "Summary"
echo "=================================="
echo "Created: $CREATED symlinks"
echo "Skipped (already exist): $SKIPPED"
echo "Total pending: $(ls "$PENDING"/*.pdf 2>/dev/null | wc -l)"
echo "=================================="
