#!/usr/bin/env python3
"""
Continuous PDF Downloader for Streaming Pipeline

Downloads PDFs continuously, monitoring disk space and pausing at 90% usage.
Writes downloaded PDFs to download_queue/ for OCR dispatcher to process.
"""

import argparse
import json
import os
import shutil
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


class ContinuousDownloader:
    """Continuously download PDFs with disk space monitoring."""

    def __init__(
        self,
        identifiers_file: Path,
        start_from: int,
        max_items: int,
        download_queue_dir: Path,
        db_path: Path = None,
        delay: float = 0.05,
        subcollection: str = None,
        disk_threshold: float = 0.90,
        state_file: Path = None
    ):
        self.identifiers_file = identifiers_file
        self.start_from = start_from
        self.max_items = max_items
        self.download_queue_dir = download_queue_dir
        self.db_path = db_path
        self.delay = delay
        self.subcollection = subcollection
        self.disk_threshold = disk_threshold
        self.state_file = state_file or download_queue_dir / ".downloader_state"

        # Create directories
        self.download_queue_dir.mkdir(parents=True, exist_ok=True)

        # Load identifiers
        with open(identifiers_file) as f:
            data = json.load(f)
        self.identifiers = data["identifiers"]

        # Initialize database
        self.db_conn = None
        if db_path:
            self.db_conn = sqlite3.connect(db_path, timeout=30.0)
            self.db_conn.row_factory = sqlite3.Row

        # Load state
        self.current_index = self._load_state()

        # Stats
        self.stats = {
            'downloaded': 0,
            'skipped': 0,
            'no_pdf': 0,
            'failed': 0,
            'paused_count': 0
        }

    def _load_state(self) -> int:
        """Load download progress from state file."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                state = json.load(f)
                return state.get('current_index', self.start_from)
        return self.start_from

    def _save_state(self):
        """Save download progress to state file."""
        state = {
            'current_index': self.current_index,
            'timestamp': time.time(),
            'stats': self.stats
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)

    def _get_disk_usage(self) -> float:
        """Get disk usage percentage for download directory."""
        stat = shutil.disk_usage(self.download_queue_dir)
        return stat.used / stat.total

    def _wait_for_space(self):
        """Wait until disk usage drops below threshold."""
        print(f"\n⚠ Disk usage ≥{self.disk_threshold*100:.0f}% - pausing downloads...")
        self.stats['paused_count'] += 1

        while self._get_disk_usage() >= self.disk_threshold:
            time.sleep(30)  # Check every 30 seconds

        print(f"✓ Disk space available - resuming downloads...")

    def download_pdf(self, identifier: str) -> bool:
        """
        Download a single PDF by identifier.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get item
            item = get_item(identifier)

            # Save metadata to database
            if self.db_conn:
                self._save_item_metadata(identifier, item.metadata)

            # Find PDF files
            pdf_files = [
                f for f in item.files
                if 'PDF' in f.get('format', '').upper() or f['name'].lower().endswith('.pdf')
            ]

            # Filter out _text.pdf versions
            pdf_files = [f for f in pdf_files if not f['name'].endswith('_text.pdf')]

            if not pdf_files:
                print(f"  ⚠ No PDF files found")
                self.stats['no_pdf'] += 1
                return False

            # Download first PDF
            pdf_file = pdf_files[0]
            filename = pdf_file['name']
            output_path = self.download_queue_dir / filename

            # Check if already downloaded
            if output_path.exists():
                print(f"  ⏭ Already exists: {filename}")
                self.stats['skipped'] += 1

                # Still save to database
                if self.db_conn:
                    file_size = output_path.stat().st_size
                    self._save_file_download(identifier, filename, str(output_path), file_size)

                return True

            # Download
            print(f"  ⬇ Downloading: {filename}", flush=True)
            item.download(
                files=[filename],
                destdir=str(self.download_queue_dir),
                ignore_existing=True,
                verbose=False,
                no_directory=True
            )

            if output_path.exists():
                file_size = output_path.stat().st_size
                print(f"  ✓ Downloaded: {filename} ({file_size:,} bytes)")
                self.stats['downloaded'] += 1

                # Save to database
                if self.db_conn:
                    self._save_file_download(identifier, filename, str(output_path), file_size)

                return True
            else:
                print(f"  ✗ Download failed: {filename} not found")
                self.stats['failed'] += 1
                return False

        except Exception as e:
            print(f"  ✗ Error: {e}")
            self.stats['failed'] += 1
            return False

    def _save_item_metadata(self, identifier: str, metadata: dict):
        """Save item metadata to database."""
        # Extract year
        year = None
        date_str = metadata.get('date')
        if date_str:
            import re
            year_match = re.search(r'\d{4}', str(date_str))
            if year_match:
                year = int(year_match.group())

        self.db_conn.execute("""
            INSERT OR REPLACE INTO items
            (identifier, title, creator, publisher, date, year, language, subject,
             collection, description, item_url, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            identifier,
            self._join_if_list(metadata.get('title')),
            self._join_if_list(metadata.get('creator')),
            self._join_if_list(metadata.get('publisher')),
            self._join_if_list(metadata.get('date')),
            year,
            self._join_if_list(metadata.get('language')),
            self._join_if_list(metadata.get('subject')),
            self._join_if_list(metadata.get('collection')),
            self._join_if_list(metadata.get('description')),
            f"https://archive.org/details/{identifier}",
            json.dumps(metadata)
        ))
        self.db_conn.commit()

    def _save_file_download(self, identifier: str, filename: str, file_path: str, file_size: int):
        """Save file download record to database."""
        cursor = self.db_conn.cursor()

        cursor.execute("""
            SELECT id FROM pdf_files WHERE identifier = ? AND filename = ?
        """, (identifier, filename))
        row = cursor.fetchone()

        if row:
            cursor.execute("""
                UPDATE pdf_files
                SET filepath = ?, filesize = ?, download_status = 'downloaded',
                    download_date = CURRENT_TIMESTAMP, subcollection = COALESCE(?, subcollection)
                WHERE id = ?
            """, (file_path, file_size, self.subcollection, row[0]))
        else:
            cursor.execute("""
                INSERT INTO pdf_files
                (identifier, filename, filepath, filesize, download_status, download_date, subcollection)
                VALUES (?, ?, ?, ?, 'downloaded', CURRENT_TIMESTAMP, ?)
            """, (identifier, filename, file_path, file_size, self.subcollection))

        self.db_conn.commit()

    def _join_if_list(self, value):
        """Join list values with semicolons."""
        if isinstance(value, list):
            return '; '.join(str(v) for v in value)
        return value

    def run(self):
        """Run continuous downloader."""
        print("=" * 70)
        print("Continuous PDF Downloader")
        print("=" * 70)
        print(f"Identifiers file: {self.identifiers_file}")
        print(f"Total identifiers: {len(self.identifiers):,}")
        print(f"Starting from: {self.current_index}")
        print(f"Max items: {self.max_items}")
        print(f"Download queue: {self.download_queue_dir}")
        print(f"Disk threshold: {self.disk_threshold*100:.0f}%")
        print("=" * 70)
        print()

        end_index = min(self.current_index + self.max_items, len(self.identifiers))

        try:
            while self.current_index < end_index:
                # Check disk space before downloading
                disk_usage = self._get_disk_usage()
                if disk_usage >= self.disk_threshold:
                    self._wait_for_space()

                # Download next item
                identifier = self.identifiers[self.current_index]
                print(f"[{self.current_index - self.start_from + 1}/{self.max_items}] {identifier}")

                self.download_pdf(identifier)

                # Update progress
                self.current_index += 1
                self._save_state()

                # Delay between downloads
                if self.delay > 0:
                    time.sleep(self.delay)

        except KeyboardInterrupt:
            print("\n\nDownload interrupted by user")
            self._save_state()

        finally:
            # Print summary
            print()
            print("=" * 70)
            print("Download Summary")
            print("=" * 70)
            print(f"  Downloaded: {self.stats['downloaded']}")
            print(f"  Skipped (already exists): {self.stats['skipped']}")
            print(f"  No PDFs found: {self.stats['no_pdf']}")
            print(f"  Failed: {self.stats['failed']}")
            print(f"  Times paused for disk space: {self.stats['paused_count']}")
            print(f"  Final index: {self.current_index}")
            print("=" * 70)

            if self.db_conn:
                self.db_conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Continuous PDF downloader with disk space monitoring"
    )
    parser.add_argument(
        "--identifiers-file",
        type=Path,
        required=True,
        help="Path to identifiers JSON file"
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
        "--download-queue",
        type=Path,
        required=True,
        help="Directory for download queue"
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
        "--subcollection",
        help="Subcollection name for database tracking"
    )
    parser.add_argument(
        "--disk-threshold",
        type=float,
        default=0.90,
        help="Disk usage threshold to pause downloads (default: 0.90)"
    )

    args = parser.parse_args()

    downloader = ContinuousDownloader(
        identifiers_file=args.identifiers_file,
        start_from=args.start_from,
        max_items=args.max_items,
        download_queue_dir=args.download_queue,
        db_path=args.db_path,
        delay=args.delay,
        subcollection=args.subcollection,
        disk_threshold=args.disk_threshold
    )

    downloader.run()


if __name__ == "__main__":
    main()
