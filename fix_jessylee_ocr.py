#!/usr/bin/env python3
"""
Fix jessylee OCR data - replace bad JSONL references with correct JSON files.

Problem: Multiple PDFs incorrectly mapped to the same JSONL files
Solution: Clear bad data, ingest the split JSON files (which match PDF names)
"""

import sqlite3
import json
import sys
from pathlib import Path
from datetime import datetime

db_path = "/home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db"
json_dir = Path("/home/jic823/projects/def-jic823/pdfs_jessylee/results/json")

print("=" * 80)
print("JESSYLEE OCR DATA FIX")
print("=" * 80)

# Step 1: Analyze the problem
print("\n1. ANALYZING CURRENT STATE")
print("-" * 80)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

cursor = conn.execute("""
    SELECT COUNT(*) as total,
           COUNT(DISTINCT json_output_path) as unique_jsonl
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    WHERE p.filepath LIKE '%pdfs_jessylee%'
      AND o.status = 'completed'
""")
stats = cursor.fetchone()
print(f"  Current OCR records: {stats['total']}")
print(f"  Unique JSONL files: {stats['unique_jsonl']}")
print(f"  Duplication ratio: {stats['total'] / stats['unique_jsonl']:.1f}x")

# Step 2: Count available JSON files
json_files = list(json_dir.glob("*.json"))
print(f"\n  Available JSON files: {len(json_files)}")

# Step 3: Show action plan
print("\n2. ACTION PLAN")
print("-" * 80)
print(f"  1. Delete {stats['total']} bad OCR records")
print(f"  2. Ingest {len(json_files)} correct JSON files")
print(f"  3. Match JSON files to PDFs by filename")

# Ask for confirmation
response = input("\nProceed with fix? [yes/no]: ").lower()
if response != "yes":
    print("Aborted.")
    sys.exit(0)

# Step 4: Delete bad OCR records
print("\n3. DELETING BAD OCR RECORDS")
print("-" * 80)

cursor = conn.execute("""
    DELETE FROM ocr_processing
    WHERE pdf_file_id IN (
        SELECT id FROM pdf_files
        WHERE filepath LIKE '%pdfs_jessylee%'
    )
""")
deleted = cursor.rowcount
conn.commit()
print(f"  Deleted {deleted} bad OCR records")

# Step 5: Ingest correct JSON files
print("\n4. INGESTING CORRECT JSON FILES")
print("-" * 80)

# Get all jessylee PDFs
cursor = conn.execute("""
    SELECT id, filename, filepath
    FROM pdf_files
    WHERE filepath LIKE '%pdfs_jessylee%'
""")
pdfs = {row['filename']: row for row in cursor}

print(f"  Found {len(pdfs)} PDFs in database")

ingested = 0
matched = 0
unmatched = []

for json_file in sorted(json_files):
    json_filename = json_file.name

    # Try to match to PDF by removing .json extension
    pdf_filename = json_filename.replace('.json', '.pdf')

    if pdf_filename in pdfs:
        pdf_record = pdfs[pdf_filename]

        # Load JSON data
        try:
            with open(json_file) as f:
                ocr_data = json.load(f)

            # Count pages
            num_pages = len(ocr_data) if isinstance(ocr_data, list) else 1

            # Calculate text length for estimate
            if isinstance(ocr_data, list):
                total_text = ' '.join([page.get('text', '') for page in ocr_data if isinstance(page, dict)])
            else:
                total_text = ocr_data.get('text', '')

            # Insert OCR record
            conn.execute("""
                INSERT INTO ocr_processing
                (pdf_file_id, status, ocr_engine, json_output_path,
                 started_date, completed_date, processing_time_seconds, ocr_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pdf_record['id'],
                'completed',
                'olmocr',
                str(json_file),
                datetime.now(),
                datetime.now(),
                0,  # Unknown processing time
                json.dumps(ocr_data)
            ))

            matched += 1
            ingested += 1

            if ingested % 100 == 0:
                conn.commit()
                print(f"  Ingested {ingested} files...")

        except Exception as e:
            print(f"  Error processing {json_filename}: {e}")
    else:
        unmatched.append(json_filename)

conn.commit()

print(f"\n  Total ingested: {ingested}")
print(f"  Matched to PDFs: {matched}")
print(f"  Unmatched JSON files: {len(unmatched)}")

if unmatched and len(unmatched) <= 20:
    print(f"\n  Unmatched files:")
    for fname in unmatched[:20]:
        print(f"    - {fname}")

# Step 6: Verify
print("\n5. VERIFICATION")
print("-" * 80)

cursor = conn.execute("""
    SELECT COUNT(*) as total,
           COUNT(DISTINCT json_output_path) as unique_json
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    WHERE p.filepath LIKE '%pdfs_jessylee%'
      AND o.status = 'completed'
""")
stats = cursor.fetchone()
print(f"  New OCR records: {stats['total']}")
print(f"  Unique JSON files: {stats['unique_json']}")
print(f"  Expected: {len(json_files)} JSON files")

if stats['total'] == len(json_files):
    print("\n  ✓ All JSON files successfully ingested!")
else:
    print(f"\n  ⚠ Mismatch: {len(json_files) - stats['total']} files not ingested")

conn.close()

print("\n" + "=" * 80)
print("FIX COMPLETE")
print("=" * 80)
