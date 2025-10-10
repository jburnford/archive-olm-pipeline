#!/usr/bin/env python3
"""
File-Based Cleanup Worker

Monitors OCR batch directories for completed results and processes them:
1. Splits batch JSONL files into individual files
2. Moves results to 04_ocr_completed/
3. Updates batch status
4. Archives completed batches
"""

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict


class FileBasedCleanup:
    """Monitor OCR batches and cleanup completed results."""

    def __init__(
        self,
        base_dir: Path,
        split_script: Path,
        check_interval: int = 60
    ):
        self.base_dir = base_dir
        self.split_script = split_script
        self.check_interval = check_interval

        # Directories
        self.processing_dir = base_dir / "03_ocr_processing"
        self.completed_dir = base_dir / "04_ocr_completed"
        self.processed_dir = base_dir / "05_processed"
        self.manifests_dir = base_dir / "_manifests"

        for d in [self.completed_dir, self.processed_dir, self.manifests_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Stats
        self.stats = {
            'batches_processed': 0,
            'files_processed': 0,
            'errors': 0
        }

    def _load_batch_metadata(self, batch_dir: Path) -> Dict:
        """Load batch metadata from batch.meta.json."""
        meta_file = batch_dir / "batch.meta.json"
        if meta_file.exists():
            with open(meta_file) as f:
                return json.load(f)
        return None

    def _save_batch_metadata(self, batch_dir: Path, metadata: Dict):
        """Save batch metadata to batch.meta.json."""
        meta_file = batch_dir / "batch.meta.json"
        with open(meta_file, 'w') as f:
            json.dump(metadata, f, indent=2)

    def _is_batch_complete(self, batch_dir: Path) -> bool:
        """Check if batch OCR processing is complete."""
        results_dir = batch_dir / "results"
        if not results_dir.exists():
            return False

        # Check for combined JSONL file (OLMoCR output)
        jsonl_files = list(results_dir.glob("*.jsonl"))
        return len(jsonl_files) > 0

    def _get_batch_job_status(self, job_id: str) -> str:
        """
        Get SLURM job status.

        Returns:
            'RUNNING', 'COMPLETED', 'FAILED', or 'UNKNOWN'
        """
        try:
            result = subprocess.run(
                ['sacct', '-j', job_id, '--format=State', '--noheader', '--parsable2'],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if lines:
                    state = lines[0].strip()
                    if 'COMPLETED' in state:
                        return 'COMPLETED'
                    elif 'FAILED' in state or 'CANCELLED' in state:
                        return 'FAILED'
                    elif 'RUNNING' in state or 'PENDING' in state:
                        return 'RUNNING'

            return 'UNKNOWN'

        except Exception as e:
            print(f"  ⚠ Error checking job status: {e}")
            return 'UNKNOWN'

    def _split_jsonl(self, batch_dir: Path, metadata: Dict):
        """Split batch JSONL into individual identifier files."""
        results_dir = batch_dir / "results"
        jsonl_files = list(results_dir.glob("*.jsonl"))

        if not jsonl_files:
            print(f"  ⚠ No JSONL files found in {results_dir}")
            return

        jsonl_file = jsonl_files[0]  # Should be only one
        print(f"  📄 Splitting JSONL: {jsonl_file.name}")

        # Read JSONL and split by identifier
        identifier_data = {}

        try:
            with open(jsonl_file, 'r') as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        identifier = data.get('identifier', 'unknown')

                        if identifier not in identifier_data:
                            identifier_data[identifier] = []

                        identifier_data[identifier].append(data)

            # Write individual files to 04_ocr_completed/
            for identifier, pages in identifier_data.items():
                output_file = self.completed_dir / f"{identifier}.ocr.jsonl"

                with open(output_file, 'w') as f:
                    for page_data in pages:
                        f.write(json.dumps(page_data) + '\n')

                print(f"  ✓ Saved: {output_file.name} ({len(pages)} pages)")

                # Create OCR metadata file
                ocr_meta = {
                    "identifier": identifier,
                    "batch_id": metadata['batch_id'],
                    "ocr_completed_at": datetime.utcnow().isoformat() + 'Z',
                    "total_pages": len(pages),
                    "ocr_file": f"04_ocr_completed/{identifier}.ocr.jsonl"
                }

                meta_file = self.completed_dir / f"{identifier}.meta.json"
                with open(meta_file, 'w') as f:
                    json.dump(ocr_meta, f, indent=2)

                self.stats['files_processed'] += 1

        except Exception as e:
            print(f"  ✗ Error splitting JSONL: {e}")
            self.stats['errors'] += 1

    def _archive_batch(self, batch_dir: Path):
        """Archive completed batch (compress PDFs, keep results)."""
        print(f"  📦 Archiving batch: {batch_dir.name}")

        # Option 1: Delete PDFs to save space
        pdf_files = list(batch_dir.glob("*.pdf"))
        for pdf in pdf_files:
            pdf.unlink()
        print(f"  ✓ Deleted {len(pdf_files)} PDFs")

        # Option 2: Compress entire batch
        # (Commented out - uncomment if you want to keep compressed archives)
        # archive_name = batch_dir.parent / f"{batch_dir.name}.tar.gz"
        # shutil.make_archive(str(archive_name).replace('.tar.gz', ''), 'gztar', batch_dir.parent, batch_dir.name)
        # shutil.rmtree(batch_dir)
        # print(f"  ✓ Archived to {archive_name}")

    def _process_batch(self, batch_dir: Path):
        """Process a completed OCR batch."""
        batch_id = batch_dir.name
        print(f"\n🔧 Processing batch: {batch_id}")

        # Load metadata
        metadata = self._load_batch_metadata(batch_dir)
        if not metadata:
            print(f"  ⚠ No metadata found for {batch_id}")
            return

        # Check job status if we have a job ID
        job_id = metadata.get('slurm_job_id')
        if job_id:
            job_status = self._get_batch_job_status(job_id)
            print(f"  Job {job_id}: {job_status}")

            if job_status == 'RUNNING':
                print(f"  ⏳ Still running, skipping for now")
                return
            elif job_status == 'FAILED':
                print(f"  ✗ Job failed, marking batch as failed")
                metadata['status'] = 'failed'
                metadata['failed_at'] = datetime.utcnow().isoformat() + 'Z'
                self._save_batch_metadata(batch_dir, metadata)
                return

        # Check if results are available
        if not self._is_batch_complete(batch_dir):
            print(f"  ⏳ Results not ready yet")
            return

        # Split JSONL into individual files
        self._split_jsonl(batch_dir, metadata)

        # Update batch metadata
        metadata['status'] = 'completed'
        metadata['completed_at'] = datetime.utcnow().isoformat() + 'Z'
        self._save_batch_metadata(batch_dir, metadata)

        # Archive batch
        self._archive_batch(batch_dir)

        self.stats['batches_processed'] += 1
        print(f"  ✓ Batch {batch_id} completed")

    def run(self):
        """Run cleanup worker in continuous loop."""
        print("=" * 70)
        print("File-Based Cleanup Worker")
        print("=" * 70)
        print(f"Processing directory: {self.processing_dir}")
        print(f"Completed directory: {self.completed_dir}")
        print(f"Check interval: {self.check_interval}s")
        print("=" * 70)
        print()

        try:
            while True:
                # Find all batch directories
                batch_dirs = [d for d in self.processing_dir.iterdir() if d.is_dir() and d.name.startswith('batch_')]

                if batch_dirs:
                    print(f"🔍 Found {len(batch_dirs)} batches to check")

                    for batch_dir in sorted(batch_dirs):
                        metadata = self._load_batch_metadata(batch_dir)
                        if metadata and metadata.get('status') != 'completed':
                            self._process_batch(batch_dir)
                else:
                    print("🔍 No batches to process")

                # Wait before next check
                time.sleep(self.check_interval)

        except KeyboardInterrupt:
            print("\n\nCleanup worker stopped by user")

        finally:
            # Print summary
            print()
            print("=" * 70)
            print("Cleanup Worker Summary")
            print("=" * 70)
            print(f"  Batches processed: {self.stats['batches_processed']}")
            print(f"  Files processed: {self.stats['files_processed']}")
            print(f"  Errors: {self.stats['errors']}")
            print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="File-based cleanup worker for streaming pipeline"
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        required=True,
        help="Base directory for pipeline"
    )
    parser.add_argument(
        "--split-script",
        type=Path,
        help="Path to JSONL split script (not currently used)"
    )
    parser.add_argument(
        "--check-interval",
        type=int,
        default=60,
        help="Seconds between checks (default: 60)"
    )

    args = parser.parse_args()

    cleanup = FileBasedCleanup(
        base_dir=args.base_dir,
        split_script=args.split_script,
        check_interval=args.check_interval
    )

    cleanup.run()


if __name__ == "__main__":
    main()
