#!/usr/bin/env python3
"""
Phase 4: Extract OCR text from old database and files.

Extracts text for each OCR system from:
- Old database (text_content column)
- File system (GALE, OLMoCR JSON, Chandra markdown)
"""

import sqlite3
from pathlib import Path
import json
import re


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace for consistency."""
    return re.sub(r'\s+', ' ', text).strip()


def extract_from_old_database(old_db_path: Path, new_db_path: Path, system_id: str):
    """Extract OCR text from old database."""

    old_conn = sqlite3.connect(old_db_path)
    new_conn = sqlite3.connect(new_db_path)

    old_cursor = old_conn.cursor()
    new_cursor = new_conn.cursor()

    print(f"  Extracting from old database: {system_id}")

    # Get text from old database
    old_cursor.execute("""
        SELECT or_.short_id, or_.text_content
        FROM ocr_results or_
        WHERE or_.system_id = ?
        AND or_.text_content IS NOT NULL
        AND or_.text_content != ''
    """, (system_id,))

    rows = old_cursor.fetchall()

    imported = 0
    skipped = 0

    for short_id, text_content in rows:
        # Check if document exists in ground truth
        new_cursor.execute("SELECT 1 FROM documents WHERE short_id = ?", (short_id,))
        if not new_cursor.fetchone():
            skipped += 1
            continue

        # Calculate statistics
        normalized = normalize_whitespace(text_content)
        char_count = len(normalized)
        word_count = len(normalized.split())
        line_count = len([line for line in text_content.split('\n') if line.strip()])

        # Insert into new database
        new_cursor.execute("""
            INSERT OR REPLACE INTO ocr_results
            (short_id, system_id, text_content, character_count,
             word_count, line_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (short_id, system_id, text_content, char_count, word_count, line_count))

        imported += 1

    new_conn.commit()
    old_conn.close()
    new_conn.close()

    print(f"    ✓ Imported {imported} documents (skipped {skipped} not in ground truth)")
    return imported


def extract_gale_files(new_db_path: Path, gale_dir: Path):
    """Extract GALE OCR from text files."""

    conn = sqlite3.connect(new_db_path)
    cursor = conn.cursor()

    print(f"  Extracting GALE from files: {gale_dir}")

    gale_files = sorted([f for f in gale_dir.glob('*.txt') if ':com.dropbox' not in f.name])

    imported = 0
    skipped = 0

    for gale_file in gale_files:
        short_id = gale_file.stem

        # Check if document exists
        cursor.execute("SELECT 1 FROM documents WHERE short_id = ?", (short_id,))
        if not cursor.fetchone():
            skipped += 1
            continue

        # Read text
        with gale_file.open('r', encoding='utf-8') as f:
            text_content = f.read()

        # Calculate statistics
        normalized = normalize_whitespace(text_content)
        char_count = len(normalized)
        word_count = len(normalized.split())
        line_count = len([line for line in text_content.split('\n') if line.strip()])

        # Insert
        cursor.execute("""
            INSERT OR REPLACE INTO ocr_results
            (short_id, system_id, text_content, character_count,
             word_count, line_count, file_path)
            VALUES (?, 'gale', ?, ?, ?, ?, ?)
        """, (short_id, text_content, char_count, word_count, line_count, str(gale_file)))

        imported += 1

    conn.commit()
    conn.close()

    print(f"    ✓ Imported {imported} documents (skipped {skipped})")
    return imported


def extract_olmocr_files(new_db_path: Path, olmocr_dir: Path):
    """Extract OLMoCR from JSON files."""

    conn = sqlite3.connect(new_db_path)
    cursor = conn.cursor()

    print(f"  Extracting OLMoCR from files: {olmocr_dir}")

    json_files = sorted(olmocr_dir.glob('*.json'))

    imported = 0
    skipped = 0
    errors = 0

    for json_file in json_files:
        short_id = json_file.stem

        # Check if document exists
        cursor.execute("SELECT 1 FROM documents WHERE short_id = ?", (short_id,))
        if not cursor.fetchone():
            skipped += 1
            continue

        try:
            # Read JSON
            with json_file.open('r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract text (handle different formats)
            text_content = None
            if isinstance(data, list) and len(data) > 0:
                if 'text' in data[0]:
                    text_content = data[0]['text']
                elif 'markdown' in data[0]:
                    text_content = data[0]['markdown']

            if not text_content:
                errors += 1
                continue

            # Calculate statistics
            normalized = normalize_whitespace(text_content)
            char_count = len(normalized)
            word_count = len(normalized.split())
            line_count = len([line for line in text_content.split('\n') if line.strip()])

            # Insert
            cursor.execute("""
                INSERT OR REPLACE INTO ocr_results
                (short_id, system_id, text_content, character_count,
                 word_count, line_count, file_path)
                VALUES (?, 'olmocr_v0_3_4', ?, ?, ?, ?, ?)
            """, (short_id, text_content, char_count, word_count, line_count, str(json_file)))

            imported += 1

        except Exception as e:
            print(f"    ✗ Error reading {json_file.name}: {e}")
            errors += 1

    conn.commit()
    conn.close()

    print(f"    ✓ Imported {imported} documents (skipped {skipped}, errors {errors})")
    return imported


def extract_chandra_files(new_db_path: Path, chandra_dir: Path):
    """Extract Chandra from markdown files."""

    conn = sqlite3.connect(new_db_path)
    cursor = conn.cursor()

    print(f"  Extracting Chandra from files: {chandra_dir}")

    # Chandra structure: {doc_id}/{doc_id}.md
    md_files = sorted(chandra_dir.glob('*/*.md'))

    imported = 0
    skipped = 0
    errors = 0

    for md_file in md_files:
        short_id = md_file.stem

        # Check if document exists
        cursor.execute("SELECT 1 FROM documents WHERE short_id = ?", (short_id,))
        if not cursor.fetchone():
            skipped += 1
            continue

        try:
            # Read markdown
            with md_file.open('r', encoding='utf-8') as f:
                text_content = f.read()

            # Calculate statistics
            normalized = normalize_whitespace(text_content)
            char_count = len(normalized)
            word_count = len(normalized.split())
            line_count = len([line for line in text_content.split('\n') if line.strip()])

            # Insert
            cursor.execute("""
                INSERT OR REPLACE INTO ocr_results
                (short_id, system_id, text_content, character_count,
                 word_count, line_count, file_path)
                VALUES (?, 'chandra', ?, ?, ?, ?, ?)
            """, (short_id, text_content, char_count, word_count, line_count, str(md_file)))

            imported += 1

        except Exception as e:
            print(f"    ✗ Error reading {md_file.name}: {e}")
            errors += 1

    conn.commit()
    conn.close()

    print(f"    ✓ Imported {imported} documents (skipped {skipped}, errors {errors})")
    return imported


def main():
    """Main extraction function."""

    new_db_path = Path('/home/jic823/archive-olm-pipeline/evaluation/ocr_evaluation_corrected.db')
    old_db_path = Path('/home/jic823/ocr_bldata/ocr_results/database/ocr_evaluation.db')

    gale_dir = Path('/home/jic823/ocr_bldata/25439023/BLN600/OCR Text')
    olmocr_dir = Path('/home/jic823/ocr_bldata/results')
    chandra_dir = Path('/home/jic823/british_library_chandra')

    print("=" * 100)
    print("EXTRACTING OCR TEXT FROM MULTIPLE SOURCES")
    print("=" * 100)
    print()

    results = {}

    # Extract from old database
    print("Extracting from old database:")
    print("-" * 100)
    for system_id in ['gemini_2.5_pro', 'mistral_small_32_24b', 'olmocr_v0_3_4',
                      'tesseract_v4_newspapers', 'deepseek_ocr', 'paddleocr_v3', 'effocr']:
        count = extract_from_old_database(old_db_path, new_db_path, system_id)
        results[system_id] = count

    print()

    # Extract from files
    print("Extracting from file systems:")
    print("-" * 100)

    if gale_dir.exists():
        results['gale'] = extract_gale_files(new_db_path, gale_dir)
    else:
        print(f"  ⚠️  GALE directory not found: {gale_dir}")

    if olmocr_dir.exists():
        # OLMoCR is also in database, but files might have more
        olmocr_count = extract_olmocr_files(new_db_path, olmocr_dir)
        results['olmocr_v0_3_4'] = max(results.get('olmocr_v0_3_4', 0), olmocr_count)
    else:
        print(f"  ⚠️  OLMoCR directory not found: {olmocr_dir}")

    if chandra_dir.exists():
        results['chandra'] = extract_chandra_files(new_db_path, chandra_dir)
    else:
        print(f"  ⚠️  Chandra directory not found: {chandra_dir}")

    print()
    print("=" * 100)
    print("EXTRACTION SUMMARY")
    print("=" * 100)
    print()

    # Get final counts from database
    conn = sqlite3.connect(new_db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT system_id, COUNT(*) as doc_count
        FROM ocr_results
        WHERE system_id != 'gold_standard'
        GROUP BY system_id
        ORDER BY doc_count DESC
    """)

    print(f"{'System':<30} {'Documents':<15} {'Status'}")
    print("-" * 100)

    for system_id, count in cursor.fetchall():
        status = "✓ Ready" if count >= 580 else f"⚠️  Only {count} docs"
        print(f"{system_id:<30} {count:<15} {status}")

    print()
    print("=" * 100)
    print("✓ PHASE 4 COMPLETE")
    print("=" * 100)
    print()
    print("Next step: Run calculate_corrected_metrics.py")
    print("=" * 100)

    conn.close()


if __name__ == '__main__':
    main()
