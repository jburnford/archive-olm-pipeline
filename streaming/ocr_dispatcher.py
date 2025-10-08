#!/usr/bin/env python3
"""
OCR Dispatcher for Streaming Pipeline

Monitors download_queue/ for new PDFs and submits them to olmOCR
when enough pages have accumulated (≥1000 pages per chunk).
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Dict

try:
    from PyPDF2 import PdfReader
except ImportError:
    print("Error: PyPDF2 library not installed")
    print("Install with: pip install PyPDF2")
    sys.exit(1)


class OCRDispatcher:
    """Monitor downloads and dispatch OCR jobs when enough pages accumulated."""

    def __init__(
        self,
        download_queue_dir: Path,
        ocr_processing_dir: Path,
        olmocr_submit_script: Path,
        pages_per_chunk: int = 1000,
        check_interval: int = 60,
        state_file: Path = None
    ):
        self.download_queue_dir = download_queue_dir
        self.ocr_processing_dir = ocr_processing_dir
        self.olmocr_submit_script = olmocr_submit_script
        self.pages_per_chunk = pages_per_chunk
        self.check_interval = check_interval
        self.state_file = state_file or ocr_processing_dir / ".dispatcher_state"

        # Create directories
        self.ocr_processing_dir.mkdir(parents=True, exist_ok=True)

        # Load state
        self.processed_files = self._load_state()

        # Stats
        self.stats = {
            'chunks_submitted': 0,
            'pdfs_processed': 0,
            'total_pages': 0,
            'jobs_submitted': []
        }

    def _load_state(self) -> set:
        """Load already-processed files from state file."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                state = json.load(f)
                return set(state.get('processed_files', []))
        return set()

    def _save_state(self):
        """Save processed files to state file."""
        state = {
            'processed_files': list(self.processed_files),
            'timestamp': time.time(),
            'stats': self.stats
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)

    def _get_page_count(self, pdf_path: Path) -> int:
        """Get page count from PDF file."""
        try:
            reader = PdfReader(pdf_path)
            return len(reader.pages)
        except Exception as e:
            print(f"  ⚠ Could not read {pdf_path.name}: {e}")
            return 0

    def _scan_download_queue(self) -> List[Dict]:
        """
        Scan download queue for unprocessed PDFs.

        Returns:
            List of dicts with 'path' and 'pages' keys
        """
        pdfs = []

        for pdf_path in self.download_queue_dir.glob("*.pdf"):
            # Skip if already processed
            if str(pdf_path) in self.processed_files:
                continue

            # Get page count
            page_count = self._get_page_count(pdf_path)
            if page_count > 0:
                pdfs.append({
                    'path': pdf_path,
                    'pages': page_count
                })

        return pdfs

    def _create_chunk(self, pdfs: List[Dict], chunk_id: int) -> Path:
        """
        Create a chunk directory and move PDFs into it.

        Returns:
            Path to chunk directory
        """
        chunk_dir = self.ocr_processing_dir / f"chunk_{chunk_id:04d}"
        chunk_dir.mkdir(parents=True, exist_ok=True)

        for pdf_info in pdfs:
            src = pdf_info['path']
            dst = chunk_dir / src.name
            src.rename(dst)
            self.processed_files.add(str(src))

        return chunk_dir

    def _submit_ocr_job(self, chunk_dir: Path) -> str:
        """
        Submit OCR job for chunk directory.

        Returns:
            SLURM job ID
        """
        cmd = [
            str(self.olmocr_submit_script),
            "--pdf-dir",
            str(chunk_dir)
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
        """Bundle PDFs into chunks and submit to OCR."""
        if not pdfs:
            return

        current_chunk = []
        current_pages = 0

        for pdf_info in pdfs:
            current_chunk.append(pdf_info)
            current_pages += pdf_info['pages']

            # Submit when we have enough pages
            if current_pages >= self.pages_per_chunk:
                self._submit_chunk(current_chunk, current_pages)
                current_chunk = []
                current_pages = 0

        # Submit remaining PDFs if any
        if current_chunk:
            self._submit_chunk(current_chunk, current_pages)

    def _submit_chunk(self, chunk_pdfs: List[Dict], total_pages: int):
        """Submit a single chunk to OCR."""
        chunk_id = self.stats['chunks_submitted']

        print()
        print(f"📦 Creating chunk {chunk_id}")
        print(f"   PDFs: {len(chunk_pdfs)}")
        print(f"   Pages: {total_pages}")

        # Create chunk directory and move PDFs
        chunk_dir = self._create_chunk(chunk_pdfs, chunk_id)

        try:
            # Submit OCR job
            job_id = self._submit_ocr_job(chunk_dir)

            print(f"   ✓ Submitted: Job {job_id}")

            # Update stats
            self.stats['chunks_submitted'] += 1
            self.stats['pdfs_processed'] += len(chunk_pdfs)
            self.stats['total_pages'] += total_pages
            self.stats['jobs_submitted'].append({
                'job_id': job_id,
                'chunk_id': chunk_id,
                'chunk_dir': str(chunk_dir),
                'pdfs': len(chunk_pdfs),
                'pages': total_pages,
                'timestamp': time.time()
            })

            # Save state
            self._save_state()

        except Exception as e:
            print(f"   ✗ Error submitting chunk: {e}")
            # Move PDFs back to download queue on error
            for pdf_info in chunk_pdfs:
                dst = self.download_queue_dir / pdf_info['path'].name
                src = chunk_dir / pdf_info['path'].name
                if src.exists():
                    src.rename(dst)
                self.processed_files.discard(str(pdf_info['path']))

    def run(self):
        """Run OCR dispatcher in continuous loop."""
        print("=" * 70)
        print("OCR Dispatcher")
        print("=" * 70)
        print(f"Download queue: {self.download_queue_dir}")
        print(f"OCR processing: {self.ocr_processing_dir}")
        print(f"Pages per chunk: {self.pages_per_chunk}")
        print(f"Check interval: {self.check_interval}s")
        print("=" * 70)
        print()

        try:
            while True:
                # Scan for new PDFs
                pdfs = self._scan_download_queue()

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
            self._save_state()

        finally:
            # Print summary
            print()
            print("=" * 70)
            print("Dispatcher Summary")
            print("=" * 70)
            print(f"  Chunks submitted: {self.stats['chunks_submitted']}")
            print(f"  PDFs processed: {self.stats['pdfs_processed']}")
            print(f"  Total pages: {self.stats['total_pages']}")
            print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="OCR dispatcher for streaming pipeline"
    )
    parser.add_argument(
        "--download-queue",
        type=Path,
        required=True,
        help="Directory to monitor for downloaded PDFs"
    )
    parser.add_argument(
        "--ocr-processing",
        type=Path,
        required=True,
        help="Directory for OCR processing chunks"
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
        default=1000,
        help="Minimum pages per OCR chunk (default: 1000)"
    )
    parser.add_argument(
        "--check-interval",
        type=int,
        default=60,
        help="Seconds between queue checks (default: 60)"
    )

    args = parser.parse_args()

    dispatcher = OCRDispatcher(
        download_queue_dir=args.download_queue,
        ocr_processing_dir=args.ocr_processing,
        olmocr_submit_script=args.olmocr_script,
        pages_per_chunk=args.pages_per_chunk,
        check_interval=args.check_interval
    )

    dispatcher.run()


if __name__ == "__main__":
    main()
