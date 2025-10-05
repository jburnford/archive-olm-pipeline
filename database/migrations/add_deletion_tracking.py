#!/usr/bin/env python3
"""
Database migration: Add PDF deletion tracking

Adds:
1. deleted_date column to pdf_files table
2. pipeline_runs table for tracking batch operations
3. Extended workflow view showing deletion status
"""

import sqlite3
import sys
from pathlib import Path


def migrate_database(db_path: str, dry_run: bool = False):
    """Add deletion tracking to database schema."""

    print(f"{'[DRY RUN] ' if dry_run else ''}Migrating database: {db_path}")
    print("=" * 70)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    changes_made = False

    # Check if deleted_date column already exists
    cursor.execute("PRAGMA table_info(pdf_files)")
    columns = [row[1] for row in cursor.fetchall()]

    if 'deleted_date' not in columns:
        print("✓ Adding deleted_date column to pdf_files table...")
        if not dry_run:
            cursor.execute("""
                ALTER TABLE pdf_files
                ADD COLUMN deleted_date TIMESTAMP
            """)
            changes_made = True
    else:
        print("  deleted_date column already exists")

    # Check if pipeline_runs table exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='pipeline_runs'
    """)
    if not cursor.fetchone():
        print("✓ Creating pipeline_runs table...")
        if not dry_run:
            cursor.execute("""
                CREATE TABLE pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT UNIQUE NOT NULL,
                    batch_number INTEGER,
                    phase TEXT CHECK(phase IN ('download', 'ocr', 'ingest', 'cleanup')),
                    status TEXT CHECK(status IN ('running', 'completed', 'failed')) DEFAULT 'running',
                    started_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_date TIMESTAMP,
                    items_processed INTEGER DEFAULT 0,
                    items_total INTEGER,
                    error_message TEXT,
                    config_snapshot TEXT  -- JSON of config used
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pipeline_run_id
                ON pipeline_runs(run_id)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pipeline_status
                ON pipeline_runs(status)
            """)

            changes_made = True
    else:
        print("  pipeline_runs table already exists")

    # Check if workflow_status_extended view exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='view' AND name='workflow_status_extended'
    """)
    if not cursor.fetchone():
        print("✓ Creating workflow_status_extended view...")
        if not dry_run:
            cursor.execute("""
                CREATE VIEW workflow_status_extended AS
                SELECT
                    w.*,
                    p.deleted_date,
                    CASE
                        WHEN p.deleted_date IS NOT NULL THEN 'deleted'
                        WHEN o.ocr_data IS NOT NULL THEN 'ready_for_cleanup'
                        WHEN o.status = 'completed' THEN 'pending_ingestion'
                        ELSE 'active'
                    END as storage_status
                FROM workflow_status w
                LEFT JOIN pdf_files p ON w.filename = p.filename
                LEFT JOIN ocr_processing o ON p.id = o.pdf_file_id
            """)
            changes_made = True
    else:
        print("  workflow_status_extended view already exists")

    if changes_made and not dry_run:
        conn.commit()
        print("\n✓ Migration completed successfully!")
    elif dry_run:
        print("\n✓ Dry run completed - no changes made")
    else:
        print("\n✓ Database already up to date")

    conn.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Add deletion tracking to database schema"
    )
    parser.add_argument(
        "db_path",
        help="Path to SQLite database file"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )

    args = parser.parse_args()

    if not Path(args.db_path).exists():
        print(f"Error: Database not found: {args.db_path}")
        sys.exit(1)

    migrate_database(args.db_path, args.dry_run)


if __name__ == "__main__":
    main()
