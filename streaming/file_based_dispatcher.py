#!/usr/bin/env python3
"""
File-Based OCR Dispatcher

Monitors 02_ocr_pending/ for PDFs and dispatches them to OLMoCR
in batches when enough pages accumulate. Uses JSON files for state tracking.
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict

try:
    from PyPDF2 import PdfReader
except ImportError:
    print("Error: PyPDF2 library not installed")
    print("Install with: pip install PyPDF2")
    sys.exit(1)


class FileBasedDispatcher:
    """Monitor pending PDFs and dispatch OCR jobs when batches are ready."""

    def __init__(
        self,
        base_dir: Path,
        olmocr_submit_script: Path,
        pages_per_chunk: int = 1500,
        check_interval: int = 60
    ):
        self.base_dir = base_dir
        self.olmocr_submit_script = olmocr_submit_script
        self.pages_per_chunk = pages_per_chunk
        self.check_interval = check_interval

        # Directories
        self.pending_dir = base_dir / "02_ocr_pending"
        self.processing_dir = base_dir / "03_ocr_processing"
        self.manifests_dir = base_dir / "_manifests"

        for d in [self.pending_dir, self.processing_dir, self.manifests_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Batch tracking
        self.batches_file = self.manifests_dir / "batches.json"
        self.batches = self._load_batches()

    def _load_batches(self) -> List[Dict]:
        """Load batch registry from manifest."""
        if self.batches_file.exists():
            with open(self.batches_file) as f:
                data = json.load(f)
                return data.get('batches', [])
        return []

    def _save_batches(self):
        """Save batch registry to manifest."""
        data = {
            'batches': self.batches,
            'last_updated': datetime.utcnow().isoformat() + 'Z'
        }
        with open(self.batches_file, 'w') as f:
            json.dump(data, f, indent=2)

    def _get_next_batch_id(self) -> str:
        """Get next batch ID."""
        if not self.batches:
            return "batch_0001"

        last_id = self.batches[-1]['batch_id']
        num = int(last_id.split('_')[1]) + 1
        return f"batch_{num:04d}"

    def _get_page_count(self, pdf_path: Path) -> int:
        """Get page count from PDF file."""
        try:
            reader = PdfReader(pdf_path)
            return len(reader.pages)
        except Exception as e:
            print(f"  ⚠ Could not read {pdf_path.name}: {e}")
            return 0

    def _scan_pending_pdfs(self) -> List[Dict]:
        """
        Scan pending directory for PDFs.

        Returns:
            List of dicts with 'path', 'identifier', and 'pages' keys
        """
        pdfs = []

        for pdf_path in self.pending_dir.glob("*.pdf"):
            # Skip broken symlinks
            if not pdf_path.exists():
                print(f"  ⚠ Removing broken symlink: {pdf_path.name}")
                pdf_path.unlink()
                continue

            # Get identifier from filename (remove .pdf extension)
            identifier = pdf_path.stem

            # Get page count
            page_count = self._get_page_count(pdf_path)
            if page_count > 0:
                pdfs.append({
                    'path': pdf_path,
                    'identifier': identifier,
                    'pages': page_count
                })

        return pdfs

    def _create_batch(self, pdfs: List[Dict]) -> Path:
        """
        Create a batch directory and move PDFs into it.

        Returns:
            Path to batch directory
        """
        batch_id = self._get_next_batch_id()
        batch_dir = self.processing_dir / batch_id
        batch_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories for OLMoCR
        (batch_dir / "chunks").mkdir(exist_ok=True)
        (batch_dir / "results").mkdir(exist_ok=True)
        (batch_dir / "logs").mkdir(exist_ok=True)

        identifiers = []
        total_pages = 0

        # Move PDFs into batch directory
        for pdf_info in pdfs:
            src = pdf_info['path']
            dst = batch_dir / src.name

            # Resolve symlink and copy actual file
            if src.is_symlink():
                actual_file = src.resolve()
                import shutil
                shutil.copy2(actual_file, dst)
            else:
                src.rename(dst)

            # Remove symlink from pending
            if src.is_symlink():
                src.unlink()

            identifiers.append(pdf_info['identifier'])
            total_pages += pdf_info['pages']

        # Save batch metadata
        batch_meta = {
            "batch_id": batch_id,
            "created_at": datetime.utcnow().isoformat() + 'Z',
            "total_pdfs": len(pdfs),
            "total_pages": total_pages,
            "identifiers": identifiers,
            "status": "created"
        }

        batch_meta_file = batch_dir / "batch.meta.json"
        with open(batch_meta_file, 'w') as f:
            json.dump(batch_meta, f, indent=2)

        return batch_dir, batch_meta

    def _submit_ocr_job(self, batch_dir: Path, batch_meta: Dict) -> str:
        """
        Submit OCR job for batch directory.

        Returns:
            SLURM job ID
        """
        cmd = [
            str(self.olmocr_submit_script),
            "--pdf-dir",
            str(batch_dir)
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        # Parse job ID from output
        output = result.stdout + result.stderr
        for line in output.split('\n'):
            if 'Submitted batch job' in line:
                job_id = line.split()[-1]
                return job_id

        raise RuntimeError("Could not parse SLURM job ID from output")

    def _bundle_and_submit(self, pdfs: List[Dict]):
        """Bundle PDFs into batches and submit to OCR."""
        if not pdfs:
            return

        current_batch = []
        current_pages = 0

        for pdf_info in pdfs:
            current_batch.append(pdf_info)
            current_pages += pdf_info['pages']

            # Submit when we have enough pages
            if current_pages >= self.pages_per_chunk:
                self._submit_batch(current_batch, current_pages)
                current_batch = []
                current_pages = 0

        # Submit remaining PDFs if any
        if current_batch:
            self._submit_batch(current_batch, current_pages)

    def _submit_batch(self, batch_pdfs: List[Dict], total_pages: int):
        """Submit a single batch to OCR."""
        print()
        print(f"📦 Creating batch")
        print(f"   PDFs: {len(batch_pdfs)}")
        print(f"   Pages: {total_pages}")

        try:
            # Create batch directory and metadata
            batch_dir, batch_meta = self._create_batch(batch_pdfs)
            batch_id = batch_meta['batch_id']

            print(f"   Batch ID: {batch_id}")

            # Submit OCR job
            job_id = self._submit_ocr_job(batch_dir, batch_meta)
            print(f"   ✓ Submitted: Job {job_id}")

            # Update batch metadata with job ID
            batch_meta['slurm_job_id'] = job_id
            batch_meta['submitted_at'] = datetime.utcnow().isoformat() + 'Z'
            batch_meta['status'] = 'submitted'

            batch_meta_file = batch_dir / "batch.meta.json"
            with open(batch_meta_file, 'w') as f:
                json.dump(batch_meta, f, indent=2)

            # Add to batch registry
            self.batches.append(batch_meta)
            self._save_batches()

        except Exception as e:
            print(f"   ✗ Error submitting batch: {e}")
            # PDFs remain in batch directory for manual handling

    def run(self):
        """Run OCR dispatcher in continuous loop."""
        print("=" * 70)
        print("File-Based OCR Dispatcher")
        print("=" * 70)
        print(f"Pending directory: {self.pending_dir}")
        print(f"Processing directory: {self.processing_dir}")
        print(f"Pages per chunk: {self.pages_per_chunk}")
        print(f"Check interval: {self.check_interval}s")
        print("=" * 70)
        print()

        try:
            while True:
                # Scan for pending PDFs
                pdfs = self._scan_pending_pdfs()

                if pdfs:
                    total_pages = sum(p['pages'] for p in pdfs)
                    print(f"🔍 Found {len(pdfs)} PDFs ({total_pages} pages) in queue")

                    # Bundle and submit if we have enough pages
                    if total_pages >= self.pages_per_chunk:
                        self._bundle_and_submit(pdfs)
                    else:
                        print(f"   Waiting for more pages (need {self.pages_per_chunk - total_pages} more)")
                else:
                    print("🔍 No new PDFs in queue")

                # Wait before next check
                time.sleep(self.check_interval)

        except KeyboardInterrupt:
            print("\n\nDispatcher stopped by user")

        finally:
            # Print summary
            print()
            print("=" * 70)
            print("Dispatcher Summary")
            print("=" * 70)
            print(f"  Total batches created: {len(self.batches)}")
            total_pdfs = sum(b['total_pdfs'] for b in self.batches)
            total_pages = sum(b['total_pages'] for b in self.batches)
            print(f"  Total PDFs processed: {total_pdfs}")
            print(f"  Total pages: {total_pages}")
            print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="File-based OCR dispatcher for streaming pipeline"
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        required=True,
        help="Base directory for pipeline"
    )
    parser.add_argument(
        "--olmocr-script",
        type=Path,
        required=True,
        help="Path to olmOCR submission script"
    )
    parser.add_argument(
        "--pages-per-chunk",
        type=int,
        default=1500,
        help="Minimum pages per OCR chunk (default: 1500)"
    )
    parser.add_argument(
        "--check-interval",
        type=int,
        default=60,
        help="Seconds between queue checks (default: 60)"
    )

    args = parser.parse_args()

    dispatcher = FileBasedDispatcher(
        base_dir=args.base_dir,
        olmocr_submit_script=args.olmocr_script,
        pages_per_chunk=args.pages_per_chunk,
        check_interval=args.check_interval
    )

    dispatcher.run()


if __name__ == "__main__":
    main()
