#!/usr/bin/env python3
"""Analyze Saskatchewan collection in the database."""

import sqlite3
import json
from collections import defaultdict

db_path = "/home/jic823/archive-olm-pipeline/archive_tracking.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

print("=" * 80)
print("DATABASE ANALYSIS - Saskatchewan Collection")
print("=" * 80)

# 1. Overall statistics
print("\n1. OVERALL STATISTICS")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        COUNT(DISTINCT i.identifier) as total,
        COUNT(DISTINCT CASE WHEN p.subcollection = 'saskatchewan_1808_1946' THEN i.identifier END) as sask_items
    FROM items i
    LEFT JOIN pdf_files p ON i.identifier = p.identifier
""")
row = cursor.fetchone()
print(f"Total items in database: {row['total']}")
print(f"Saskatchewan items: {row['sask_items']}")

# 2. Saskatchewan PDF download status
print("\n2. SASKATCHEWAN PDF DOWNLOAD STATUS")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        download_status,
        COUNT(*) as count
    FROM pdf_files p
    WHERE p.subcollection = 'saskatchewan_1808_1946'
    GROUP BY download_status
    ORDER BY count DESC
""")
for row in cursor:
    print(f"  {row['download_status']:15s}: {row['count']:5d}")

# 3. Saskatchewan OCR processing status
print("\n3. SASKATCHEWAN OCR PROCESSING STATUS")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        o.status,
        COUNT(*) as count
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    WHERE p.subcollection = 'saskatchewan_1808_1946'
    GROUP BY o.status
    ORDER BY count DESC
""")
for row in cursor:
    print(f"  {row['status']:15s}: {row['count']:5d}")

# 4. Check OCR data storage (are we actually storing the OCR text?)
print("\n4. OCR DATA STORAGE CHECK")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        COUNT(*) as total_ocr,
        COUNT(CASE WHEN ocr_data IS NOT NULL THEN 1 END) as has_ocr_data,
        COUNT(CASE WHEN json_output_path IS NOT NULL THEN 1 END) as has_output_path,
        SUM(LENGTH(ocr_data)) as total_ocr_bytes
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND o.status = 'completed'
""")
row = cursor.fetchone()
print(f"  Completed OCR jobs: {row['total_ocr']}")
print(f"  With OCR data in DB: {row['has_ocr_data']}")
print(f"  With output path: {row['has_output_path']}")
if row['total_ocr_bytes']:
    print(f"  Total OCR data: {row['total_ocr_bytes'] / 1024 / 1024:.1f} MB")

# 5. Look at some examples beyond the first 112
print("\n5. SAMPLE DATA (items 113-120)")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        p.identifier,
        p.filename,
        p.download_status,
        o.status as ocr_status,
        CASE WHEN o.ocr_data IS NOT NULL THEN 'YES' ELSE 'NO' END as has_data,
        LENGTH(o.ocr_data) as data_size
    FROM pdf_files p
    LEFT JOIN ocr_processing o ON p.id = o.pdf_file_id
    WHERE p.subcollection = 'saskatchewan_1808_1946'
    ORDER BY p.id
    LIMIT 8 OFFSET 112
""")

for row in cursor:
    print(f"\n  Identifier: {row['identifier']}")
    print(f"    PDF: {row['filename'] or 'N/A'}")
    print(f"    Download status: {row['download_status'] or 'N/A'}")
    print(f"    OCR status: {row['ocr_status'] or 'N/A'}")
    print(f"    Has OCR data: {row['has_data']}")
    if row['data_size']:
        print(f"    Data size: {row['data_size']:,} bytes")

# 6. Check for failed or problematic items
print("\n6. FAILED/PROBLEMATIC ITEMS")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        p.identifier,
        p.filename,
        p.download_status,
        o.status as ocr_status,
        o.error_message
    FROM pdf_files p
    LEFT JOIN ocr_processing o ON p.id = o.pdf_file_id
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND (p.download_status = 'failed' OR o.status = 'failed')
    LIMIT 10
""")

failed = list(cursor)
if failed:
    for row in failed:
        print(f"\n  {row['identifier']}: {row['filename']}")
        print(f"    Download: {row['download_status']}, OCR: {row['ocr_status']}")
        if row['error_message']:
            print(f"    Error: {row['error_message']}")
else:
    print("  No failed items found!")

# 7. Deletion status
print("\n7. PDF DELETION STATUS")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN deleted_date IS NOT NULL THEN 1 END) as deleted,
        COUNT(CASE WHEN deleted_date IS NULL THEN 1 END) as not_deleted
    FROM pdf_files p
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND p.download_status = 'downloaded'
""")
row = cursor.fetchone()
print(f"  Total downloaded PDFs: {row['total']}")
print(f"  Deleted (freed space): {row['deleted']}")
print(f"  Still on disk: {row['not_deleted']}")

# 8. Pipeline runs tracking
print("\n8. PIPELINE BATCH RUNS")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        batch_number,
        phase,
        status,
        items_processed,
        started_date,
        completed_date,
        error_message
    FROM pipeline_runs
    ORDER BY batch_number,
        CASE phase
            WHEN 'download' THEN 1
            WHEN 'ocr' THEN 2
            WHEN 'ingest' THEN 3
            WHEN 'cleanup' THEN 4
        END
""")

current_batch = None
for row in cursor:
    if row['batch_number'] != current_batch:
        current_batch = row['batch_number']
        print(f"\n  Batch {current_batch}:")

    status_mark = "✓" if row['status'] == 'completed' else "✗" if row['status'] == 'failed' else "→"
    print(f"    {status_mark} {row['phase']:10s}: {row['status']:10s}", end="")
    if row['items_processed']:
        print(f" ({row['items_processed']} items)", end="")
    if row['error_message']:
        print(f" - ERROR: {row['error_message'][:50]}", end="")
    print()

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)

conn.close()
