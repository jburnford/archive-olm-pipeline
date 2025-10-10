#!/usr/bin/env python3
"""
Spot check OCR quality for residential school documents and newspapers.
"""

import sqlite3
import json
import random

db_path = "/home/jic823/archive-olm-pipeline/archive_tracking.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

print("=" * 80)
print("OCR QUALITY SPOT CHECK")
print("=" * 80)

# 1. RESIDENTIAL SCHOOL DOCUMENTS
print("\n1. RESIDENTIAL SCHOOL DOCUMENTS (School Files Series)")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        i.identifier,
        i.title,
        i.year,
        p.filename,
        o.ocr_data,
        LENGTH(o.ocr_data) as data_size
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    JOIN items i ON p.identifier = i.identifier
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND o.status = 'completed'
      AND o.ocr_data IS NOT NULL
      AND (i.subject LIKE '%Residential School%' OR i.subject LIKE '%School Files%')
    ORDER BY RANDOM()
    LIMIT 3
""")

for idx, row in enumerate(cursor, 1):
    print(f"\n--- Sample {idx} ---")
    print(f"Title: {row['title'][:70]}...")
    print(f"Year: {row['year']}")
    print(f"File: {row['filename']}")
    print(f"Size: {row['data_size']:,} bytes\n")

    try:
        ocr_data = json.loads(row['ocr_data'])

        # Get a middle page to avoid boilerplate
        if len(ocr_data) > 5:
            sample_page = ocr_data[len(ocr_data) // 2]
        else:
            sample_page = ocr_data[0]

        if 'text' in sample_page:
            text = sample_page['text']
            print(f"Page {sample_page.get('page_num', 'unknown')} excerpt:")
            print("-" * 70)
            # Show first 800 characters
            print(text[:800])
            print("-" * 70)

            # Quality metrics
            words = text.split()
            avg_word_len = sum(len(w) for w in words) / len(words) if words else 0
            uppercase_ratio = sum(1 for c in text if c.isupper()) / len(text) if text else 0
            digit_ratio = sum(1 for c in text if c.isdigit()) / len(text) if text else 0

            print(f"\nQuality indicators:")
            print(f"  Words on page: {len(words)}")
            print(f"  Avg word length: {avg_word_len:.1f}")
            print(f"  Uppercase ratio: {uppercase_ratio:.1%}")
            print(f"  Digit ratio: {digit_ratio:.1%}")

            # Check for common OCR errors
            ocr_errors = []
            if 'rn' in text.lower() and 'm' not in text.lower():
                ocr_errors.append("'rn' confusion (might be 'm')")
            if '|' in text or '!' in text[:200]:  # ! at start might be l or I
                ocr_errors.append("vertical bar/exclamation confusion")
            if uppercase_ratio > 0.5:
                ocr_errors.append("excessive uppercase (might be recognition issue)")

            if ocr_errors:
                print(f"  Potential OCR issues: {', '.join(ocr_errors)}")
            else:
                print(f"  ✓ No obvious OCR issues detected")
    except Exception as e:
        print(f"Error parsing OCR: {e}")

# 2. NEWSPAPERS
print("\n\n2. NEWSPAPER DOCUMENTS")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        i.identifier,
        i.title,
        i.year,
        p.filename,
        o.ocr_data,
        LENGTH(o.ocr_data) as data_size
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    JOIN items i ON p.identifier = i.identifier
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND o.status = 'completed'
      AND o.ocr_data IS NOT NULL
      AND (i.subject LIKE '%Prince Albert times%'
           OR i.subject LIKE '%newspaper%'
           OR i.collection LIKE '%newspaper%')
    ORDER BY RANDOM()
    LIMIT 3
""")

for idx, row in enumerate(cursor, 1):
    print(f"\n--- Sample {idx} ---")
    print(f"Title: {row['title'][:70]}...")
    print(f"Year: {row['year']}")
    print(f"File: {row['filename']}")
    print(f"Size: {row['data_size']:,} bytes\n")

    try:
        ocr_data = json.loads(row['ocr_data'])

        # Get first real content page (skip front matter)
        sample_page = ocr_data[min(2, len(ocr_data)-1)]

        if 'text' in sample_page:
            text = sample_page['text']
            print(f"Page {sample_page.get('page_num', 'unknown')} excerpt:")
            print("-" * 70)
            # Show first 800 characters
            print(text[:800])
            print("-" * 70)

            # Quality metrics
            words = text.split()
            avg_word_len = sum(len(w) for w in words) / len(words) if words else 0
            uppercase_ratio = sum(1 for c in text if c.isupper()) / len(text) if text else 0

            print(f"\nQuality indicators:")
            print(f"  Words on page: {len(words)}")
            print(f"  Avg word length: {avg_word_len:.1f}")
            print(f"  Uppercase ratio: {uppercase_ratio:.1%}")

            # Newspaper-specific checks
            newspaper_features = []
            if any(word in text.lower() for word in ['advertisement', 'notices', 'for sale']):
                newspaper_features.append("advertisements detected")
            if any(word in text.lower() for word in ['editor', 'editorial', 'publisher']):
                newspaper_features.append("editorial content")
            if text.count('$') > 2 or text.count('¢') > 0:
                newspaper_features.append("price/currency symbols")

            if newspaper_features:
                print(f"  Newspaper features: {', '.join(newspaper_features)}")

            # Column detection (rough heuristic)
            lines = text.split('\n')
            short_lines = sum(1 for line in lines if 10 < len(line) < 60)
            if short_lines > len(lines) * 0.6:
                print(f"  ✓ Likely multi-column layout detected")

            # OCR quality check
            ocr_quality = "Good"
            if uppercase_ratio > 0.6:
                ocr_quality = "Fair - excessive uppercase"
            if avg_word_len < 3 or avg_word_len > 8:
                ocr_quality = "Fair - unusual word lengths"
            print(f"  Overall OCR quality: {ocr_quality}")

    except Exception as e:
        print(f"Error parsing OCR: {e}")

print("\n" + "=" * 80)
print("SPOT CHECK COMPLETE")
print("=" * 80)

conn.close()
