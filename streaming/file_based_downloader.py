#!/usr/bin/env python3
"""
File-Based Continuous PDF Downloader

Downloads PDFs and tracks state using JSON files instead of SQLite.
Uses the directory structure: 01_downloaded/ for PDFs and metadata.
"""

import argparse
import json
import hashlib
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from internetarchive import get_item
except ImportError:
    print("Error: internetarchive library not installed")
    print("Install with: pip install internetarchive")
    sys.exit(1)


class FileBasedDownloader:
    """Download PDFs with file-based state tracking."""

    def __init__(
        self,
        identifiers_file: Path,
        start_from: int,
        max_items: int,
        base_dir: Path,
        delay: float = 0.05,
        collection: str = None,
        disk_threshold: float = 0.90
    ):
        self.identifiers_file = identifiers_file
        self.start_from = start_from
        self.max_items = max_items
        self.base_dir = base_dir
        self.delay = delay
        self.collection = collection
        self.disk_threshold = disk_threshold

        # Create directory structure
        self.downloaded_dir = base_dir / "01_downloaded"
        self.ocr_pending_dir = base_dir / "02_ocr_pending"
        self.errors_dir = base_dir / "99_errors" / "download_failed"
        self.manifests_dir = base_dir / "_manifests"

        for d in [self.downloaded_dir, self.ocr_pending_dir, self.errors_dir, self.manifests_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Load identifiers
        with open(identifiers_file) as f:
            data = json.load(f)
        self.identifiers = data["identifiers"]

        # Load progress
        self.progress_file = self.manifests_dir / "download_progress.json"
        self.current_index = self._load_progress()

        # Stats
        self.stats = {
            'downloaded': 0,
            'skipped': 0,
            'no_pdf': 0,
            'failed': 0,
            'paused_count': 0
        }

    def _load_progress(self) -> int:
        """Load download progress from manifest."""
        if self.progress_file.exists():
            with open(self.progress_file) as f:
                data = json.load(f)
                return data.get('current_index', self.start_from)
        return self.start_from

    def _save_progress(self):
        """Save download progress to manifest."""
        data = {
            'current_index': self.current_index,
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'stats': self.stats
        }
        with open(self.progress_file, 'w') as f:
            json.dump(data, f, indent=2)

    def _get_disk_usage(self) -> float:
        """Get disk usage percentage for base directory."""
        stat = shutil.disk_usage(self.base_dir)
        return stat.used / stat.total

    def _wait_for_space(self):
        """Wait until disk usage drops below threshold."""
        print(f"\n⚠ Disk usage ≥{self.disk_threshold*100:.0f}% - pausing downloads...")
        self.stats['paused_count'] += 1

        while self._get_disk_usage() >= self.disk_threshold:
            time.sleep(30)

        print(f"✓ Disk space available - resuming downloads...")

    def _compute_md5(self, file_path: Path) -> str:
        """Compute MD5 hash of file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _is_already_downloaded(self, identifier: str) -> bool:
        """Check if PDF already downloaded."""
        # Check for any PDF with this identifier
        pdf_pattern = f"{identifier}*.pdf"
        existing = list(self.downloaded_dir.glob(pdf_pattern))
        return len(existing) > 0

    def _save_download_metadata(self, identifier: str, item_metadata: dict,
                                filename: str, file_path: Path, file_size: int):
        """Save download metadata to JSON file."""
        # Extract useful metadata
        title = item_metadata.get('title')
        if isinstance(title, list):
            title = '; '.join(title)

        creator = item_metadata.get('creator')
        if isinstance(creator, list):
            creator = '; '.join(creator)

        # Extract year from date
        year = None
        date_str = item_metadata.get('date')
        if date_str:
            import re
            year_match = re.search(r'\d{4}', str(date_str))
            if year_match:
                year = int(year_match.group())

        metadata = {
            "identifier": identifier,
            "collection": self.collection,
            "title": title,
            "creator": creator,
            "year": year,
            "downloaded_at": datetime.utcnow().isoformat() + 'Z',
            "filename": filename,
            "file_path": str(file_path.relative_to(self.base_dir)),
            "file_size": file_size,
            "source_url": f"https://archive.org/details/{identifier}",
            "item_metadata": item_metadata
        }

        # Save to 01_downloaded/{identifier}.meta.json
        meta_file = self.downloaded_dir / f"{identifier}.meta.json"
        with open(meta_file, 'w') as f:
            json.dump(metadata, f, indent=2)

    def _save_error(self, identifier: str, error_type: str, error_message: str):
        """Save download error to JSON file."""
        error_data = {
            "identifier": identifier,
            "stage": "download",
            "error_type": error_type,
            "error_message": error_message,
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "collection": self.collection
        }

        error_file = self.errors_dir / f"{identifier}.error.json"
        with open(error_file, 'w') as f:
            json.dump(error_data, f, indent=2)

    def download_pdf(self, identifier: str) -> bool:
        """
        Download a single PDF by identifier.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if already downloaded
            if self._is_already_downloaded(identifier):
                existing = list(self.downloaded_dir.glob(f"{identifier}*.pdf"))
                print(f"  ⏭ Already exists: {existing[0].name}")
                self.stats['skipped'] += 1
                return True

            # Get item
            item = get_item(identifier)

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
                self._save_error(identifier, "no_pdf", "No PDF files found for this item")
                return False

            # Download first PDF
            pdf_file = pdf_files[0]
            filename = pdf_file['name']
            output_path = self.downloaded_dir / filename

            print(f"  ⬇ Downloading: {filename}", flush=True)
            item.download(
                files=[filename],
                destdir=str(self.downloaded_dir),
                ignore_existing=True,
                verbose=False,
                no_directory=True
            )

            if output_path.exists():
                file_size = output_path.stat().st_size
                print(f"  ✓ Downloaded: {filename} ({file_size:,} bytes)")
                self.stats['downloaded'] += 1

                # Save metadata
                self._save_download_metadata(
                    identifier,
                    item.metadata,
                    filename,
                    output_path,
                    file_size
                )

                # Create symlink in 02_ocr_pending
                pending_link = self.ocr_pending_dir / filename
                if not pending_link.exists():
                    pending_link.symlink_to(output_path)

                return True
            else:
                print(f"  ✗ Download failed: {filename} not found")
                self.stats['failed'] += 1
                self._save_error(identifier, "download_failed", "File not found after download")
                return False

        except Exception as e:
            error_msg = str(e)
            print(f"  ✗ Error: {error_msg}")
            self.stats['failed'] += 1
            self._save_error(identifier, "exception", error_msg)
            return False

    def run(self):
        """Run continuous downloader."""
        print("=" * 70)
        print("File-Based Continuous PDF Downloader")
        print("=" * 70)
        print(f"Identifiers file: {self.identifiers_file}")
        print(f"Total identifiers: {len(self.identifiers):,}")
        print(f"Starting from: {self.current_index}")
        print(f"Max items: {self.max_items}")
        print(f"Base directory: {self.base_dir}")
        print(f"Disk threshold: {self.disk_threshold*100:.0f}%")
        print("=" * 70)
        print()

        end_index = min(self.current_index + self.max_items, len(self.identifiers))

        try:
            while self.current_index < end_index:
                # Check disk space
                disk_usage = self._get_disk_usage()
                if disk_usage >= self.disk_threshold:
                    self._wait_for_space()

                # Download next item
                identifier = self.identifiers[self.current_index]
                print(f"[{self.current_index - self.start_from + 1}/{self.max_items}] {identifier}")

                self.download_pdf(identifier)

                # Update progress
                self.current_index += 1
                self._save_progress()

                # Delay between downloads
                if self.delay > 0:
                    time.sleep(self.delay)

        except KeyboardInterrupt:
            print("\n\nDownload interrupted by user")
            self._save_progress()

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


def main():
    parser = argparse.ArgumentParser(
        description="File-based continuous PDF downloader"
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
        "--base-dir",
        type=Path,
        required=True,
        help="Base directory for pipeline (contains 01_downloaded, 02_ocr_pending, etc.)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.05,
        help="Delay between downloads (seconds)"
    )
    parser.add_argument(
        "--collection",
        help="Collection name for tracking"
    )
    parser.add_argument(
        "--disk-threshold",
        type=float,
        default=0.90,
        help="Disk usage threshold to pause downloads (default: 0.90)"
    )

    args = parser.parse_args()

    downloader = FileBasedDownloader(
        identifiers_file=args.identifiers_file,
        start_from=args.start_from,
        max_items=args.max_items,
        base_dir=args.base_dir,
        delay=args.delay,
        collection=args.collection,
        disk_threshold=args.disk_threshold
    )

    downloader.run()


if __name__ == "__main__":
    main()
