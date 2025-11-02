#!/usr/bin/env python3
"""
Phase 2: Import ground truth documents.

Reads all ground truth text files and populates the documents table.
Also creates OCR results for the gold_standard validation system.
"""

import sqlite3
from pathlib import Path
import re


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace for consistency."""
    return re.sub(r'\s+', ' ', text).strip()


def import_ground_truth(db_path: Path, gt_dir: Path):
    """Import all ground truth files into database."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=" * 100)
    print("IMPORTING GROUND TRUTH DOCUMENTS")
    print("=" * 100)
    print(f"Source directory: {gt_dir}")
    print()

    # Get all ground truth files
    gt_files = sorted([f for f in gt_dir.glob('*.txt') if not ':com.dropbox' in f.name])

    print(f"Found {len(gt_files)} ground truth files")
    print()

    imported = 0
    errors = []

    for gt_file in gt_files:
        short_id = gt_file.stem

        try:
            # Read ground truth text
            with gt_file.open('r', encoding='utf-8') as f:
                text = f.read()

            # Normalize whitespace for counting
            normalized = normalize_whitespace(text)

            # Calculate statistics
            char_count = len(normalized)
            word_count = len(normalized.split())
            line_count = len([line for line in text.split('\n') if line.strip()])

            # Insert into documents table
            cursor.execute("""
                INSERT OR REPLACE INTO documents
                (short_id, ground_truth_path, ground_truth_chars,
                 ground_truth_words, ground_truth_lines)
                VALUES (?, ?, ?, ?, ?)
            """, (short_id, str(gt_file), char_count, word_count, line_count))

            # Also insert into ocr_results as gold_standard system
            cursor.execute("""
                INSERT OR REPLACE INTO ocr_results
                (short_id, system_id, text_content, character_count,
                 word_count, line_count, file_path)
                VALUES (?, 'gold_standard', ?, ?, ?, ?, ?)
            """, (short_id, text, char_count, word_count, line_count, str(gt_file)))

            imported += 1

            if imported % 100 == 0:
                print(f"  Imported {imported}/{len(gt_files)} documents...")

        except Exception as e:
            errors.append((short_id, str(e)))
            print(f"  ✗ Error importing {short_id}: {e}")

    conn.commit()

    print()
    print("=" * 100)
    print(f"✓ Import complete")
    print("=" * 100)
    print(f"  Total files: {len(gt_files)}")
    print(f"  Imported: {imported}")
    print(f"  Errors: {len(errors)}")
    print()

    # Statistics
    cursor.execute("""
        SELECT
            COUNT(*) as num_docs,
            AVG(ground_truth_chars) as avg_chars,
            AVG(ground_truth_words) as avg_words,
            AVG(ground_truth_lines) as avg_lines,
            MIN(ground_truth_chars) as min_chars,
            MAX(ground_truth_chars) as max_chars
        FROM documents
    """)

    stats = cursor.fetchone()

    print("Ground Truth Statistics:")
    print("-" * 100)
    print(f"  Documents: {stats[0]:,}")
    print(f"  Avg characters: {stats[1]:.0f}")
    print(f"  Avg words: {stats[2]:.0f}")
    print(f"  Avg lines: {stats[3]:.0f}")
    print(f"  Character range: {stats[4]:,} - {stats[5]:,}")
    print()

    # Check gold_standard system
    cursor.execute("""
        SELECT COUNT(*) FROM ocr_results WHERE system_id = 'gold_standard'
    """)

    gold_count = cursor.fetchone()[0]

    print("=" * 100)
    print("Gold Standard Validation System:")
    print("-" * 100)
    print(f"  Documents: {gold_count:,}")
    print(f"  Purpose: Metric calculation validation")
    print(f"  Expected result: CER=0%, WER=0% when evaluated against itself")
    print()
    print("=" * 100)

    if errors:
        print()
        print("ERRORS:")
        print("-" * 100)
        for short_id, error in errors:
            print(f"  {short_id}: {error}")
        print()

    conn.close()
    return imported, len(errors)


def main():
    """Main import function."""

    db_path = Path('/home/jic823/archive-olm-pipeline/evaluation/ocr_evaluation_corrected.db')
    gt_dir = Path('/home/jic823/ocr_bldata/25439023/BLN600/Ground Truth')

    if not db_path.exists():
        print("ERROR: Database not found. Run setup_corrected_database.py first.")
        return 1

    if not gt_dir.exists():
        print(f"ERROR: Ground truth directory not found: {gt_dir}")
        return 1

    imported, errors = import_ground_truth(db_path, gt_dir)

    print()
    print("=" * 100)
    print("✓ PHASE 2 COMPLETE")
    print("=" * 100)
    print()
    print(f"Imported {imported} documents")
    print(f"Errors: {errors}")
    print()
    print("Next step: Run import_ocr_systems.py")
    print("=" * 100)

    return 0 if errors == 0 else 1


if __name__ == '__main__':
    exit(main())
