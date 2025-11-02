#!/usr/bin/env python3
"""
Phase 1: Setup corrected evaluation database.

Creates new database with proper schema including:
- Separate tables for strict and semantic metrics
- Independent calculation of CER and WER
- Gold standard system for validation (should always be 0% error)
"""

import sqlite3
from pathlib import Path
from datetime import datetime


def create_database(db_path: Path):
    """Create new evaluation database with corrected schema."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=" * 100)
    print("CREATING CORRECTED EVALUATION DATABASE")
    print("=" * 100)
    print(f"Database: {db_path}")
    print()

    # 1. Documents table (ground truth)
    print("Creating table: documents")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            short_id TEXT PRIMARY KEY,
            doc_id TEXT,
            publication TEXT,
            date TEXT,
            image_path TEXT,
            ground_truth_path TEXT NOT NULL,
            ground_truth_chars INTEGER,
            ground_truth_words INTEGER,
            ground_truth_lines INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 2. OCR Systems table
    print("Creating table: ocr_systems")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ocr_systems (
            system_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            version TEXT,
            model_type TEXT,
            description TEXT,
            cost_per_page REAL,
            cost_currency TEXT DEFAULT 'USD',
            open_source BOOLEAN DEFAULT 0,
            is_validation BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 3. OCR Results table
    print("Creating table: ocr_results")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ocr_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            short_id TEXT NOT NULL,
            system_id TEXT NOT NULL,
            text_content TEXT NOT NULL,
            character_count INTEGER,
            word_count INTEGER,
            line_count INTEGER,
            file_path TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (short_id) REFERENCES documents(short_id),
            FOREIGN KEY (system_id) REFERENCES ocr_systems(system_id),
            UNIQUE(short_id, system_id)
        )
    """)

    # 4. Evaluation Metrics - STRICT (preserves case and punctuation)
    print("Creating table: evaluation_metrics_strict")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS evaluation_metrics_strict (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            short_id TEXT NOT NULL,
            system_id TEXT NOT NULL,

            -- Character-level metrics
            cer REAL NOT NULL,
            char_insertions INTEGER,
            char_deletions INTEGER,
            char_substitutions INTEGER,
            levenshtein_distance_chars INTEGER,

            -- Word-level metrics (INDEPENDENT of CER)
            wer REAL NOT NULL,
            word_insertions INTEGER,
            word_deletions INTEGER,
            word_substitutions INTEGER,
            levenshtein_distance_words INTEGER,

            -- Validation
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (short_id) REFERENCES documents(short_id),
            FOREIGN KEY (system_id) REFERENCES ocr_systems(system_id),
            UNIQUE(short_id, system_id),

            -- Sanity checks
            CHECK(cer >= 0),
            CHECK(wer >= 0),
            CHECK(char_insertions >= 0),
            CHECK(char_deletions >= 0),
            CHECK(char_substitutions >= 0),
            CHECK(word_insertions >= 0),
            CHECK(word_deletions >= 0),
            CHECK(word_substitutions >= 0)
        )
    """)

    # 5. Evaluation Metrics - SEMANTIC (lowercase, no punctuation)
    print("Creating table: evaluation_metrics_semantic")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS evaluation_metrics_semantic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            short_id TEXT NOT NULL,
            system_id TEXT NOT NULL,

            -- Character-level metrics (normalized)
            cer_semantic REAL NOT NULL,
            char_insertions_semantic INTEGER,
            char_deletions_semantic INTEGER,
            char_substitutions_semantic INTEGER,
            levenshtein_distance_chars_semantic INTEGER,

            -- Word-level metrics (normalized, INDEPENDENT)
            wer_semantic REAL NOT NULL,
            word_insertions_semantic INTEGER,
            word_deletions_semantic INTEGER,
            word_substitutions_semantic INTEGER,
            levenshtein_distance_words_semantic INTEGER,

            -- Validation
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (short_id) REFERENCES documents(short_id),
            FOREIGN KEY (system_id) REFERENCES ocr_systems(system_id),
            UNIQUE(short_id, system_id),

            -- Sanity checks
            CHECK(cer_semantic >= 0),
            CHECK(wer_semantic >= 0),
            CHECK(char_insertions_semantic >= 0),
            CHECK(char_deletions_semantic >= 0),
            CHECK(char_substitutions_semantic >= 0),
            CHECK(word_insertions_semantic >= 0),
            CHECK(word_deletions_semantic >= 0),
            CHECK(word_substitutions_semantic >= 0)
        )
    """)

    # Create indexes for performance
    print("Creating indexes...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ocr_results_short_id ON ocr_results(short_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ocr_results_system_id ON ocr_results(system_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_strict_short_id ON evaluation_metrics_strict(short_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_strict_system_id ON evaluation_metrics_strict(system_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_semantic_short_id ON evaluation_metrics_semantic(short_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_semantic_system_id ON evaluation_metrics_semantic(system_id)")

    conn.commit()

    print()
    print("=" * 100)
    print("✓ Database schema created successfully")
    print("=" * 100)
    print()
    print("Tables created:")
    print("  1. documents - Ground truth metadata")
    print("  2. ocr_systems - OCR system information")
    print("  3. ocr_results - Raw OCR text")
    print("  4. evaluation_metrics_strict - Strict metrics (case + punctuation)")
    print("  5. evaluation_metrics_semantic - Semantic metrics (normalized)")
    print()
    print("Indexes created on foreign keys for performance")
    print()

    # Show table info
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    print("Database tables:")
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table[0]})")
        columns = cursor.fetchall()
        print(f"\n  {table[0]} ({len(columns)} columns)")

    print()
    print("=" * 100)

    conn.close()
    return db_path


def add_validation_system(db_path: Path):
    """Add 'gold_standard' as a validation OCR system."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=" * 100)
    print("ADDING GOLD STANDARD VALIDATION SYSTEM")
    print("=" * 100)
    print()

    cursor.execute("""
        INSERT OR IGNORE INTO ocr_systems
        (system_id, name, version, model_type, description,
         cost_per_page, open_source, is_validation)
        VALUES
        ('gold_standard', 'Gold Standard (Validation)', 'N/A', 'Ground Truth',
         'Ground truth data used as validation - should always produce 0% error rates',
         0.0, 1, 1)
    """)

    conn.commit()

    print("✓ Added system: gold_standard")
    print("  Purpose: Sanity check - should produce CER=0%, WER=0%")
    print()
    print("This validates our metric calculation code is correct.")
    print("If gold_standard produces any errors, our code has bugs!")
    print()
    print("=" * 100)

    conn.close()


def main():
    """Main setup function."""

    db_path = Path('/home/jic823/archive-olm-pipeline/evaluation/ocr_evaluation_corrected.db')

    # Remove old database if exists (fresh start)
    if db_path.exists():
        print(f"⚠️  Existing database found: {db_path}")
        print("   Removing for fresh start...")
        db_path.unlink()
        print("   ✓ Removed")
        print()

    # Create database
    create_database(db_path)

    # Add gold standard validation system
    add_validation_system(db_path)

    print()
    print("=" * 100)
    print("✓ PHASE 1 COMPLETE")
    print("=" * 100)
    print()
    print(f"Database created: {db_path}")
    print(f"Size: {db_path.stat().st_size / 1024:.1f} KB")
    print()
    print("Next steps:")
    print("  1. Run import_ground_truth.py")
    print("  2. Run import_ocr_systems.py")
    print("  3. Run extract_ocr_text.py")
    print("  4. Run calculate_corrected_metrics.py")
    print("  5. Validate gold_standard system has 0% errors!")
    print()
    print("=" * 100)


if __name__ == '__main__':
    main()
