#!/usr/bin/env python3
"""
Fix OCR duplication for all collections.

Steps:
1. Split existing JSONL files into individual JSON files per PDF
2. Clear bad OCR records
3. Re-ingest from correct JSON files
"""

import subprocess
import sqlite3
import sys
from pathlib import Path

# Collection directories
COLLECTIONS = {
    'saskatchewan_1808_1946': Path('/home/jic823/projects/def-jic823/pdfs_sask_test'),
    'jacob': Path('/home/jic823/projects/def-jic823/pdfs_jacob'),
    'main': Path('/home/jic823/projects/def-jic823/pdf'),
    'india': Path('/home/jic823/projects/def-jic823/pdf_india'),
}

db_path = "/home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db"
split_script = Path("/home/jic823/projects/def-jic823/archive-olm-pipeline/orchestration/split_jsonl_to_json.py")
ingest_script = Path("/home/jic823/projects/def-jic823/InternetArchive/ingest_ocr_results.py")

print("=" * 80)
print("FIX ALL COLLECTIONS - OCR DUPLICATION")
print("=" * 80)

# Step 1: Analyze current state
print("\n1. CURRENT STATE")
print("-" * 80)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

for subcollection, pdf_dir in COLLECTIONS.items():
    cursor = conn.execute("""
        SELECT
            COUNT(*) as ocr_records,
            COUNT(DISTINCT json_output_path) as unique_files
        FROM ocr_processing o
        JOIN pdf_files p ON o.pdf_file_id = p.id
        WHERE p.subcollection = ?
          AND o.status = 'completed'
    """, (subcollection,))
    stats = cursor.fetchone()

    if stats and stats['ocr_records'] > 0:
        ratio = stats['ocr_records'] / stats['unique_files'] if stats['unique_files'] > 0 else 0
        print(f"  {subcollection:25s}: {stats['ocr_records']:4d} records, {stats['unique_files']:4d} files ({ratio:.1f}x dup)")

conn.close()

# Step 2: Split JSONL files for each collection
print("\n2. SPLITTING JSONL FILES")
print("-" * 80)

for subcollection, pdf_dir in COLLECTIONS.items():
    if not pdf_dir.exists():
        print(f"  ⊘ {subcollection}: directory not found")
        continue

    jsonl_dir = pdf_dir / "results" / "results"
    if not jsonl_dir.exists() or not list(jsonl_dir.glob("*.jsonl")):
        print(f"  ⊘ {subcollection}: no JSONL files")
        continue

    print(f"\n  Processing {subcollection}...")
    try:
        result = subprocess.run(
            ["python3", str(split_script), str(pdf_dir)],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"    ✓ Split completed")

        # Count JSON files created
        json_dir = pdf_dir / "results" / "json"
        if json_dir.exists():
            json_count = len(list(json_dir.glob("*.json")))
            print(f"    ✓ Created {json_count} JSON files")
    except subprocess.CalledProcessError as e:
        print(f"    ✗ Split failed: {e}")
        print(f"      STDERR: {e.stderr[:200]}")
        continue

# Step 3: Clear bad OCR records
print("\n3. CLEARING BAD OCR RECORDS")
print("-" * 80)
print("  Proceeding with deletion (auto-confirmed)...")

conn = sqlite3.connect(db_path)

for subcollection in COLLECTIONS.keys():
    cursor = conn.execute("""
        DELETE FROM ocr_processing
        WHERE pdf_file_id IN (
            SELECT id FROM pdf_files WHERE subcollection = ?
        )
    """, (subcollection,))
    deleted = cursor.rowcount
    if deleted > 0:
        print(f"  {subcollection:25s}: Deleted {deleted} bad records")

conn.commit()
conn.close()

# Step 4: Re-ingest from JSON files
print("\n4. RE-INGESTING FROM JSON FILES")
print("-" * 80)

for subcollection, pdf_dir in COLLECTIONS.items():
    json_dir = pdf_dir / "results" / "json"
    if not json_dir.exists() or not list(json_dir.glob("*.json")):
        print(f"  ⊘ {subcollection}: no JSON files to ingest")
        continue

    print(f"\n  Ingesting {subcollection}...")
    try:
        result = subprocess.run([
            "python3", str(ingest_script),
            str(pdf_dir),
            "--db-path", db_path,
            "--ocr-dir", str(json_dir),
            "--no-parse-jsonl",  # Use filename-based matching (JSON files match PDF names)
        ], capture_output=True, text=True, check=True)

        # Parse output for summary
        if "Summary:" in result.stdout:
            summary_lines = result.stdout.split("Summary:")[-1].split("\n")[:5]
            for line in summary_lines:
                if line.strip():
                    print(f"    {line.strip()}")
    except subprocess.CalledProcessError as e:
        print(f"    ✗ Ingestion failed: {e}")
        print(f"      STDERR: {e.stderr[:200]}")

# Step 5: Verify
print("\n5. VERIFICATION")
print("-" * 80)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

for subcollection, pdf_dir in COLLECTIONS.items():
    cursor = conn.execute("""
        SELECT
            COUNT(*) as ocr_records,
            COUNT(DISTINCT json_output_path) as unique_files
        FROM ocr_processing o
        JOIN pdf_files p ON o.pdf_file_id = p.id
        WHERE p.subcollection = ?
          AND o.status = 'completed'
    """, (subcollection,))
    stats = cursor.fetchone()

    if stats and stats['ocr_records'] > 0:
        ratio = stats['ocr_records'] / stats['unique_files'] if stats['unique_files'] > 0 else 0
        status = "✓" if ratio <= 1.1 else "⚠"
        print(f"  {status} {subcollection:25s}: {stats['ocr_records']:4d} records, {stats['unique_files']:4d} files ({ratio:.1f}x)")

conn.close()

print("\n" + "=" * 80)
print("FIX COMPLETE")
print("=" * 80)
