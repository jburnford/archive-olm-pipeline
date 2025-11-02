#!/usr/bin/env python3
"""
Phase 3: Import OCR systems metadata.

Adds metadata for all OCR systems to be evaluated.
"""

import sqlite3
from pathlib import Path


OCR_SYSTEMS = [
    # (system_id, name, version, model_type, description, cost_per_page, open_source)
    ('gale', 'GALE OCR', 'Unknown', 'Traditional OCR',
     'GALE consortium OCR system (legacy)', None, False),

    ('olmocr_v0_3_4', 'OLMoCR', 'v0.3.4', 'Vision-Language Model (Qwen2 VL 7B)',
     'Open-source OCR using Qwen2 VL 7B base model', 0.00019, True),

    ('gemini_2.5_pro', 'Google Gemini 2.5 Pro', '2.5-pro', 'Large Language Model',
     'Google\'s multimodal LLM for OCR tasks', None, False),

    ('mistral_small_32_24b', 'Mistral Small 3.2', '24B', 'Large Language Model',
     'Mistral Small 3.2 24B parameter model', None, False),

    ('tesseract_v4_newspapers', 'Tesseract v4', 'v4.1.1', 'Traditional OCR',
     'Tesseract v4 with newspaper-specific training', 0.0, True),

    ('deepseek_ocr', 'DeepSeek OCR', 'base_size_1024', 'Deep Learning OCR',
     'DeepSeek OCR model', None, True),

    ('paddleocr_v3', 'PaddleOCR', 'v3.2.0', 'Deep Learning OCR',
     'PaddleOCR v3 multilingual OCR', 0.0, True),

    ('effocr', 'EffOCR', 'latest', 'Deep Learning OCR',
     'Efficient OCR model', None, True),

    ('chandra', 'Chandra', 'Unknown', 'Large Language Model',
     'LLM-based OCR with markdown output and structure preservation', None, False),
]


def import_systems(db_path: Path):
    """Import OCR systems metadata."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=" * 100)
    print("IMPORTING OCR SYSTEMS METADATA")
    print("=" * 100)
    print()

    for system in OCR_SYSTEMS:
        system_id, name, version, model_type, description, cost, open_source = system

        cursor.execute("""
            INSERT OR REPLACE INTO ocr_systems
            (system_id, name, version, model_type, description,
             cost_per_page, open_source, is_validation)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        """, (system_id, name, version, model_type, description, cost, open_source))

        cost_str = f"${cost:.6f}/page" if cost else "Unknown"
        os_str = "✓ Open Source" if open_source else "✗ Proprietary"

        print(f"✓ {name:<35} {version:<15} {cost_str:<20} {os_str}")

    conn.commit()

    print()
    print("=" * 100)
    print("✓ Imported {} OCR systems".format(len(OCR_SYSTEMS)))
    print("=" * 100)
    print()

    # Summary
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN open_source = 1 THEN 1 ELSE 0 END) as open_source_count,
            SUM(CASE WHEN is_validation = 1 THEN 1 ELSE 0 END) as validation_count
        FROM ocr_systems
    """)

    total, os_count, val_count = cursor.fetchone()

    print("Summary:")
    print("-" * 100)
    print(f"  Total systems: {total}")
    print(f"  Open source: {os_count}")
    print(f"  Proprietary: {total - os_count - val_count}")
    print(f"  Validation systems: {val_count}")
    print()
    print("=" * 100)

    conn.close()


def main():
    """Main import function."""

    db_path = Path('/home/jic823/archive-olm-pipeline/evaluation/ocr_evaluation_corrected.db')

    if not db_path.exists():
        print("ERROR: Database not found. Run setup_corrected_database.py first.")
        return 1

    import_systems(db_path)

    print()
    print("✓ PHASE 3 COMPLETE")
    print()
    print("Next step: Run extract_ocr_text.py")
    print("=" * 100)

    return 0


if __name__ == '__main__':
    exit(main())
