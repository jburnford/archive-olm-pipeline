#!/usr/bin/env python3
"""
Cleanup Worker for Streaming Pipeline

Monitors completed OCR jobs, ingests results to database,
and deletes source PDFs to free disk space.
"""

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Dict, Set


class CleanupWorker:
    """Monitor OCR jobs and clean up completed work."""

    def __init__(
        self,
        ocr_processing_dir: Path,
        ocr_completed_dir: Path,
        split_script: Path,
        ingest_script: Path,
        db_path: Path,
        check_interval: int = 60,
        dispatcher_state_file: Path = None,
        state_file: Path = None
    ):
        self.ocr_processing_dir = ocr_processing_dir
        self.ocr_completed_dir = ocr_completed_dir
        self.split_script = split_script
        self.ingest_script = ingest_script
        self.db_path = db_path
        self.check_interval = check_interval
        self.dispatcher_state_file = dispatcher_state_file
        self.state_file = state_file or ocr_completed_dir / ".cleanup_state"

        # Create directories
        self.ocr_completed_dir.mkdir(parents=True, exist_ok=True)

        # Load state
        self.completed_jobs = self._load_state()

        # Stats
        self.stats = {
            'jobs_completed': 0,
            'pdfs_deleted': 0,
            'space_freed_gb': 0
        }

    def _load_state(self) -> set:
        """Load already-completed job IDs."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                state = json.load(f)
                return set(state.get('completed_jobs', []))
        return set()

    def _save_state(self):
        """Save completed job IDs to state file."""
        state = {
            'completed_jobs': list(self.completed_jobs),
            'timestamp': time.time(),
            'stats': self.stats
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)

    def _get_pending_jobs(self) -> List[Dict]:
        """
        Get list of pending OCR jobs from dispatcher state.

        Returns:
            List of job dicts with job_id, chunk_dir, etc.
        """
        if not self.dispatcher_state_file or not self.dispatcher_state_file.exists():
            return []

        with open(self.dispatcher_state_file) as f:
            state = json.load(f)

        # Filter out already-completed jobs
        pending = []
        for job in state.get('stats', {}).get('jobs_submitted', []):
            if job['job_id'] not in self.completed_jobs:
                pending.append(job)

        return pending

    def _check_job_status(self, job_id: str) -> str:
        """
        Check SLURM job status.

        Returns:
            'COMPLETED', 'FAILED', 'RUNNING', or 'PENDING'
        """
        try:
            result = subprocess.run(
                ['sacct', '-j', job_id, '--format=State', '--noheader', '--parsable2'],
                capture_output=True,
                text=True,
                check=True
            )

            # Get first non-empty line (main job state)
            for line in result.stdout.strip().split('\n'):
                state = line.strip()
                if state:
                    return state

            return 'UNKNOWN'

        except subprocess.CalledProcessError:
            return 'UNKNOWN'

    def _split_jsonl(self, chunk_dir: Path) -> bool:
        """Split JSONL files to individual JSON files."""
        try:
            print(f"   Splitting JSONL files...")
            result = subprocess.run(
                ['python3', str(self.split_script), str(chunk_dir)],
                capture_output=True,
                text=True,
                check=True
            )
            print(f"   ✓ Split completed")
            return True

        except subprocess.CalledProcessError as e:
            print(f"   ✗ Split failed: {e}")
            print(f"   STDERR: {e.stderr}")
            return False

    def _ingest_results(self, chunk_dir: Path) -> bool:
        """Ingest OCR results to database."""
        try:
            print(f"   Ingesting OCR results...")

            # OCR results are in chunk_dir/results/json/
            ocr_dir = chunk_dir / "results" / "json"

            result = subprocess.run(
                [
                    'python3',
                    str(self.ingest_script),
                    str(chunk_dir),
                    '--db-path', str(self.db_path),
                    '--ocr-dir', str(ocr_dir)
                ],
                capture_output=True,
                text=True,
                check=True
            )
            print(f"   ✓ Ingest completed")
            return True

        except subprocess.CalledProcessError as e:
            print(f"   ✗ Ingest failed: {e}")
            print(f"   STDERR: {e.stderr}")
            return False

    def _delete_pdfs(self, chunk_dir: Path) -> int:
        """
        Delete PDF files from chunk directory.

        Returns:
            Number of bytes freed
        """
        pdfs = list(chunk_dir.glob("*.pdf"))
        total_size = sum(p.stat().st_size for p in pdfs)

        for pdf in pdfs:
            pdf.unlink()

        return total_size

    def _move_to_completed(self, chunk_dir: Path):
        """Move chunk directory to completed directory."""
        dest = self.ocr_completed_dir / chunk_dir.name
        chunk_dir.rename(dest)

    def _process_completed_job(self, job_info: Dict):
        """Process a single completed OCR job."""
        job_id = job_info['job_id']
        chunk_dir = Path(job_info['chunk_dir'])

        print(f"\n📥 Processing completed job {job_id}")
        print(f"   Chunk: {chunk_dir.name}")
        print(f"   PDFs: {job_info['pdfs']}")
        print(f"   Pages: {job_info['pages']}")

        try:
            # Split JSONL
            if not self._split_jsonl(chunk_dir):
                return False

            # Ingest to database
            if not self._ingest_results(chunk_dir):
                return False

            # Delete PDFs
            space_freed = self._delete_pdfs(chunk_dir)
            space_freed_gb = space_freed / (1024**3)
            print(f"   ✓ Deleted {job_info['pdfs']} PDFs ({space_freed_gb:.2f} GB freed)")

            # Move to completed
            self._move_to_completed(chunk_dir)
            print(f"   ✓ Moved to completed")

            # Update stats
            self.stats['jobs_completed'] += 1
            self.stats['pdfs_deleted'] += job_info['pdfs']
            self.stats['space_freed_gb'] += space_freed_gb

            # Mark as completed
            self.completed_jobs.add(job_id)
            self._save_state()

            return True

        except Exception as e:
            print(f"   ✗ Error processing job: {e}")
            return False

    def run(self):
        """Run cleanup worker in continuous loop."""
        print("=" * 70)
        print("Cleanup Worker")
        print("=" * 70)
        print(f"OCR processing: {self.ocr_processing_dir}")
        print(f"OCR completed: {self.ocr_completed_dir}")
        print(f"Database: {self.db_path}")
        print(f"Check interval: {self.check_interval}s")
        print("=" * 70)
        print()

        try:
            while True:
                # Get pending jobs
                pending_jobs = self._get_pending_jobs()

                if pending_jobs:
                    print(f"🔍 Checking {len(pending_jobs)} pending jobs...")

                    for job_info in pending_jobs:
                        job_id = job_info['job_id']
                        status = self._check_job_status(job_id)

                        if status == 'COMPLETED':
                            self._process_completed_job(job_info)
                        elif status == 'FAILED':
                            print(f"   ⚠ Job {job_id} failed")
                            self.completed_jobs.add(job_id)
                            self._save_state()

                else:
                    print("🔍 No pending jobs")

                # Wait before next check
                time.sleep(self.check_interval)

        except KeyboardInterrupt:
            print("\n\nCleanup worker stopped by user")
            self._save_state()

        finally:
            # Print summary
            print()
            print("=" * 70)
            print("Cleanup Summary")
            print("=" * 70)
            print(f"  Jobs completed: {self.stats['jobs_completed']}")
            print(f"  PDFs deleted: {self.stats['pdfs_deleted']}")
            print(f"  Space freed: {self.stats['space_freed_gb']:.2f} GB")
            print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Cleanup worker for streaming pipeline"
    )
    parser.add_argument(
        "--ocr-processing",
        type=Path,
        required=True,
        help="Directory with OCR processing chunks"
    )
    parser.add_argument(
        "--ocr-completed",
        type=Path,
        required=True,
        help="Directory for completed chunks"
    )
    parser.add_argument(
        "--split-script",
        type=Path,
        required=True,
        help="Path to split JSONL script"
    )
    parser.add_argument(
        "--ingest-script",
        type=Path,
        required=True,
        help="Path to ingest OCR results script"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        required=True,
        help="Path to SQLite database"
    )
    parser.add_argument(
        "--check-interval",
        type=int,
        default=60,
        help="Seconds between job checks (default: 60)"
    )
    parser.add_argument(
        "--dispatcher-state",
        type=Path,
        help="Path to dispatcher state file"
    )

    args = parser.parse_args()

    worker = CleanupWorker(
        ocr_processing_dir=args.ocr_processing,
        ocr_completed_dir=args.ocr_completed,
        split_script=args.split_script,
        ingest_script=args.ingest_script,
        db_path=args.db_path,
        check_interval=args.check_interval,
        dispatcher_state_file=args.dispatcher_state
    )

    worker.run()


if __name__ == "__main__":
    main()
