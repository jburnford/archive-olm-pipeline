#!/usr/bin/env python3
"""
Download PDFs from Archive.org using a pre-fetched identifiers file.

This bypasses the buggy Archive.org search API pagination by downloading
items directly by their identifiers using the internetarchive library.
"""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

try:
    from internetarchive import get_item
except ImportError:
    print("Error: internetarchive library not installed")
    print("Install with: pip install internetarchive")
    sys.exit(1)


def download_pdfs_from_identifiers(
    identifiers_file: Path,
    start_from: int,
    max_items: int,
    download_dir: Path,
    db_path: Path = None,
    delay: float = 0.05,
    download_all_pdfs: bool = False,
    subcollection: str = None
):
    """
    Download PDFs directly from Archive.org using identifiers.

    Args:
        identifiers_file: Path to identifiers JSON file
        start_from: Starting index in identifiers list
        max_items: Maximum number of items to download
        download_dir: Directory to download PDFs to
        db_path: Path to SQLite database for tracking
        delay: Delay between downloads (seconds)
        download_all_pdfs: Download all PDFs per item
        subcollection: Subcollection name for database tracking
    """
    # Load identifiers file
    if not identifiers_file.exists():
        print(f"Error: Identifiers file not found: {identifiers_file}")
        sys.exit(1)

    with open(identifiers_file, 'r') as f:
        data = json.load(f)

    identifiers = data["identifiers"]
    total_identifiers = len(identifiers)

    print("=" * 70)
    print("Archive.org Direct Download (by Identifier)")
    print("=" * 70)
    print(f"Identifiers file: {identifiers_file}")
    print(f"Total identifiers in file: {total_identifiers:,}")
    print(f"Starting from index: {start_from}")
    print(f"Max items to download: {max_items}")
    print(f"Download directory: {download_dir}")
    print()

    # Calculate slice
    end_index = min(start_from + max_items, total_identifiers)
    identifiers_to_download = identifiers[start_from:end_index]

    print(f"Will download identifiers {start_from} to {end_index - 1}")
    print(f"Total items: {len(identifiers_to_download)}")
    print()

    if not identifiers_to_download:
        print("No identifiers to download")
        return

    # Create download directory
    download_dir.mkdir(parents=True, exist_ok=True)

    # Initialize database connection if provided
    db_conn = None
    if db_path:
        db_conn = sqlite3.connect(db_path)
        # Ensure tables exist
        _ensure_db_tables(db_conn, subcollection)

    # Download statistics
    stats = {
        'downloaded': 0,
        'failed': 0,
        'skipped': 0,
        'no_pdf': 0
    }

    # Process each identifier
    for i, identifier in enumerate(identifiers_to_download, start=1):
        print(f"[{i}/{len(identifiers_to_download)}] Processing: {identifier}")

        try:
            # Get item from Archive.org
            item = get_item(identifier)

            # Save metadata to database
            if db_conn:
                _save_item_metadata(db_conn, identifier, item.metadata, subcollection)

            # Find PDF files (format can be 'PDF', 'Text PDF', 'Image PDF', etc.)
            pdf_files = [f for f in item.files if 'PDF' in f.get('format', '').upper() or f['name'].lower().endswith('.pdf')]

            if not pdf_files:
                print(f"  ⚠ No PDF files found")
                stats['no_pdf'] += 1
                continue

            # Decide which PDFs to download
            files_to_download = pdf_files if download_all_pdfs else [pdf_files[0]]

            if download_all_pdfs and len(pdf_files) > 1:
                print(f"  Found {len(pdf_files)} PDFs, downloading all")

            # Download each PDF
            for pdf_file in files_to_download:
                filename = pdf_file['name']
                # internetarchive library creates subdirectory named after identifier
                output_path = download_dir / identifier / filename

                # Check if already downloaded
                if output_path.exists():
                    print(f"  ⏭ Already exists: {filename}")
                    stats['skipped'] += 1
                    continue

                # Download the file
                print(f"  ⬇ Downloading: {filename}", flush=True)
                try:
                    # Use internetarchive library's download method
                    # This creates a subdirectory named after the identifier
                    item.download(
                        files=[filename],
                        destdir=str(download_dir),
                        ignore_existing=True,
                        verbose=False
                    )

                    # Check if file was downloaded successfully
                    if output_path.exists():
                        file_size = output_path.stat().st_size
                        print(f"  ✓ Downloaded: {filename} ({file_size:,} bytes)")
                        stats['downloaded'] += 1

                        # Save to database
                        if db_conn:
                            _save_file_download(
                                db_conn,
                                identifier,
                                filename,
                                str(output_path),
                                file_size,
                                subcollection
                            )
                    else:
                        print(f"  ✗ Download failed: {filename} not found after download")
                        stats['failed'] += 1

                except Exception as e:
                    print(f"  ✗ Error downloading {filename}: {e}")
                    stats['failed'] += 1

            # Delay between items
            if delay > 0 and i < len(identifiers_to_download):
                time.sleep(delay)

        except Exception as e:
            print(f"  ✗ Error processing {identifier}: {e}")
            stats['failed'] += 1

        # Commit database changes periodically
        if db_conn and i % 10 == 0:
            db_conn.commit()

    # Final database commit
    if db_conn:
        db_conn.commit()
        db_conn.close()

    # Print summary
    print()
    print("=" * 70)
    print("Download Summary")
    print("=" * 70)
    print(f"  Downloaded: {stats['downloaded']}")
    print(f"  Skipped (already exists): {stats['skipped']}")
    print(f"  No PDFs found: {stats['no_pdf']}")
    print(f"  Failed: {stats['failed']}")
    print("=" * 70)


def _ensure_db_tables(conn: sqlite3.Connection, subcollection: str = None):
    """Ensure database tables exist - matches existing InternetArchive schema."""
    cursor = conn.cursor()

    # Items table - use existing schema, don't recreate
    # The table already exists with this schema:
    # identifier, title, creator, publisher, date, year, language, subject,
    # collection, description, item_url, download_date, metadata_json, notes

    # Files table - check if exists, create if needed
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            identifier TEXT,
            filename TEXT,
            file_path TEXT,
            file_size INTEGER,
            download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(identifier, filename)
        )
    """)

    conn.commit()


def _save_item_metadata(conn: sqlite3.Connection, identifier: str, metadata: dict, subcollection: str = None):
    """Save item metadata to database - matches existing schema."""
    cursor = conn.cursor()

    # Extract year from date if possible
    year = None
    date_str = metadata.get('date')
    if date_str:
        import re
        year_match = re.search(r'\d{4}', str(date_str))
        if year_match:
            year = int(year_match.group())

    cursor.execute("""
        INSERT OR REPLACE INTO items
        (identifier, title, creator, publisher, date, year, language, subject,
         collection, description, item_url, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        identifier,
        _join_if_list(metadata.get('title')),
        _join_if_list(metadata.get('creator')),
        _join_if_list(metadata.get('publisher')),
        _join_if_list(metadata.get('date')),
        year,
        _join_if_list(metadata.get('language')),
        _join_if_list(metadata.get('subject')),
        _join_if_list(metadata.get('collection')),
        _join_if_list(metadata.get('description')),
        f"https://archive.org/details/{identifier}",
        json.dumps(metadata)
    ))


def _save_file_download(
    conn: sqlite3.Connection,
    identifier: str,
    filename: str,
    file_path: str,
    file_size: int,
    subcollection: str = None
):
    """Save file download record to database."""
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO files
        (identifier, filename, file_path, file_size)
        VALUES (?, ?, ?, ?)
    """, (identifier, filename, file_path, file_size))


def _join_if_list(value):
    """Join list values with semicolons, or return string as-is."""
    if isinstance(value, list):
        return '; '.join(str(v) for v in value)
    return value


def main():
    parser = argparse.ArgumentParser(
        description="Download PDFs using pre-fetched identifiers file"
    )
    parser.add_argument(
        "--identifiers-file",
        type=Path,
        required=True,
        help="Path to identifiers JSON file from fetch_identifiers.py"
    )
    parser.add_argument(
        "--start-from",
        type=int,
        default=0,
        help="Starting index in identifiers list"
    )
    parser.add_argument(
        "--max-items",
        type=int,
        required=True,
        help="Maximum number of items to download"
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        required=True,
        help="Directory to download PDFs to"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.05,
        help="Delay between downloads (seconds)"
    )
    parser.add_argument(
        "--download-all-pdfs",
        action="store_true",
        help="Download all PDFs per item"
    )
    parser.add_argument(
        "--subcollection",
        help="Subcollection name for database tracking"
    )

    args = parser.parse_args()

    download_pdfs_from_identifiers(
        identifiers_file=args.identifiers_file,
        start_from=args.start_from,
        max_items=args.max_items,
        download_dir=args.download_dir,
        db_path=args.db_path,
        delay=args.delay,
        download_all_pdfs=args.download_all_pdfs,
        subcollection=args.subcollection
    )


if __name__ == "__main__":
    main()
