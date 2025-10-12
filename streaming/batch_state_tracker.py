#!/usr/bin/env python3
"""
Batch State Tracker

Tracks the state of batch submissions through their lifecycle:
  submitted → processing → completed/failed

This prevents resubmission of PDFs already in the queue and handles
failed jobs by returning PDFs to the unprocessed pool.
"""

import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional


class BatchStateTracker:
    """Track batch submission state."""

    def __init__(self, manifest_path: Path):
        self.manifest_path = manifest_path
        self.state = self._load()

    def _load(self) -> dict:
        """Load state from manifest."""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path) as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️  Error loading batch state: {e}")
                return {"batches": {}}
        return {"batches": {}}

    def _save(self):
        """Save state to manifest."""
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_path, 'w') as f:
            json.dump(self.state, f, indent=2)

    def get_submitted_pdfs(self) -> Set[str]:
        """
        Get set of PDF filenames in submitted/processing batches.

        Returns filenames (not full paths) of PDFs currently in the pipeline.
        """
        submitted = set()

        for batch_id, batch_info in self.state["batches"].items():
            status = batch_info.get("status")

            # Include PDFs from batches that are submitted or processing
            if status in ["submitted", "processing"]:
                submitted.update(batch_info.get("pdf_filenames", []))

        return submitted

    def register_batch(
        self,
        batch_number: int,
        job_id: str,
        pdf_filenames: List[str],
        chunk_count: int
    ):
        """Register a newly submitted batch."""
        batch_id = f"batch_{batch_number:04d}"

        self.state["batches"][batch_id] = {
            "batch_number": batch_number,
            "job_id": job_id,
            "pdf_filenames": pdf_filenames,
            "chunk_count": chunk_count,
            "status": "submitted",
            "submitted_at": datetime.now().isoformat(),
            "pdf_count": len(pdf_filenames)
        }

        self._save()
        print(f"  Registered {batch_id} with {len(pdf_filenames)} PDFs")

    def update_batch_states(self) -> Dict[str, str]:
        """
        Check SLURM job status and update batch states.

        Returns dict of batch_id -> new_status for batches that changed.
        """
        changed = {}

        for batch_id, batch_info in list(self.state["batches"].items()):
            current_status = batch_info.get("status")

            # Skip already-completed batches
            if current_status in ["completed", "failed"]:
                continue

            job_id = batch_info.get("job_id")
            if not job_id:
                continue

            # Check SLURM status
            new_status = self._check_job_status(job_id)

            if new_status != current_status:
                batch_info["status"] = new_status
                batch_info["status_updated_at"] = datetime.now().isoformat()
                changed[batch_id] = new_status

        if changed:
            self._save()

        return changed

    def _check_job_status(self, job_id: str) -> str:
        """
        Check SLURM job array status.

        Returns:
            "submitted" - job is pending
            "processing" - at least one task is running
            "completed" - all tasks completed successfully
            "failed" - some tasks failed
        """
        try:
            # Check with sacct for completed/failed jobs
            result = subprocess.run(
                ['sacct', '-j', job_id, '--format=State', '--noheader', '-X'],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0 and result.stdout.strip():
                states = [line.strip() for line in result.stdout.strip().split('\n')]

                # Check for failures
                if any(s in ['FAILED', 'CANCELLED', 'TIMEOUT', 'OUT_OF_MEMORY'] for s in states):
                    return "failed"

                # Check if all completed
                if all(s == 'COMPLETED' for s in states):
                    return "completed"

            # Check squeue for running/pending jobs
            result = subprocess.run(
                ['squeue', '-j', job_id, '--noheader', '--format=%T'],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                if not result.stdout.strip():
                    # Not in queue - check sacct again
                    return "completed"  # Assume completed if not in queue and not failed

                states = [line.strip() for line in result.stdout.strip().split('\n')]

                # If any running, status is processing
                if any(s == 'RUNNING' for s in states):
                    return "processing"

                # If all pending, status is submitted
                if all(s in ['PENDING', 'CONFIGURING'] for s in states):
                    return "submitted"

        except Exception as e:
            print(f"  ⚠️  Error checking job {job_id}: {e}")

        return "submitted"  # Default to submitted if we can't determine

    def get_failed_batch_pdfs(self) -> List[str]:
        """
        Get list of PDF filenames from failed batches.

        These should be returned to the unprocessed pool.
        """
        failed_pdfs = []

        for batch_id, batch_info in self.state["batches"].items():
            if batch_info.get("status") == "failed":
                # Check if we've already requeued these
                if not batch_info.get("requeued", False):
                    failed_pdfs.extend(batch_info.get("pdf_filenames", []))
                    batch_info["requeued"] = True

        if failed_pdfs:
            self._save()

        return failed_pdfs

    def mark_batch_completed(self, batch_id: str):
        """Mark a batch as completed."""
        if batch_id in self.state["batches"]:
            self.state["batches"][batch_id]["status"] = "completed"
            self.state["batches"][batch_id]["completed_at"] = datetime.now().isoformat()
            self._save()

    def get_summary(self) -> dict:
        """Get summary of batch states."""
        summary = {
            "submitted": 0,
            "processing": 0,
            "completed": 0,
            "failed": 0,
            "total_pdfs_submitted": 0,
            "total_pdfs_processing": 0
        }

        for batch_info in self.state["batches"].values():
            status = batch_info.get("status", "unknown")
            pdf_count = batch_info.get("pdf_count", 0)

            if status in summary:
                summary[status] += 1

            if status in ["submitted", "processing"]:
                summary["total_pdfs_submitted"] += pdf_count
                if status == "processing":
                    summary["total_pdfs_processing"] += pdf_count

        return summary


if __name__ == "__main__":
    # Test the tracker
    import sys
    from pathlib import Path

    if len(sys.argv) < 2:
        print("Usage: python3 batch_state_tracker.py <base_dir>")
        sys.exit(1)

    base_dir = Path(sys.argv[1])
    tracker = BatchStateTracker(base_dir / "_manifests" / "batch_state.json")

    print("Updating batch states...")
    changed = tracker.update_batch_states()

    if changed:
        print("\nStatus changes:")
        for batch_id, new_status in changed.items():
            print(f"  {batch_id}: {new_status}")

    print("\nSummary:")
    summary = tracker.get_summary()
    for key, value in summary.items():
        print(f"  {key}: {value}")

    failed = tracker.get_failed_batch_pdfs()
    if failed:
        print(f"\n⚠️  {len(failed)} PDFs from failed batches need reprocessing")
