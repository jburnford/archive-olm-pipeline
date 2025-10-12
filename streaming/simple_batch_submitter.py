#!/usr/bin/env python3
"""
Simple Batch Submitter with Page-based Chunking

Scans 01_downloaded/ for unprocessed PDFs, packs them into 1500-page chunks,
and submits each chunk as a separate SLURM job. Includes backpressure monitoring.

Usage:
  python3 streaming/simple_batch_submitter.py \
    --config config/caribbean_filebased.yaml \
    [--batch-size 200] \
    [--max-unprocessed 500] \
    [--dry-run]

Features:
  - Filters PDFs using _manifests/processed_pdfs.json
  - Packs PDFs into chunks of ≤1500 pages (conservative for SLURM)
  - Submits each chunk as separate job with dynamic walltime
  - Reports backlog status for orchestrator
  - Prevents submission when backlog too large
  - Dynamic walltime: 300s startup + 6s/page + 20% buffer
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Set, Tuple
import yaml

# Import batch state tracker
sys.path.insert(0, str(Path(__file__).parent))
from batch_state_tracker import BatchStateTracker


class ProcessedPDFTracker:
    """Track which PDFs have been completely processed."""

    def __init__(self, manifest_path: Path):
        self.manifest_path = manifest_path
        self.processed = self._load()

    def _load(self) -> Set[str]:
        """Load processed identifiers from manifest."""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path) as f:
                    data = json.load(f)
                return set(data.get('processed', []))
            except Exception as e:
                print(f"⚠️  Error loading tracker: {e}")
                return set()
        return set()

    def is_processed(self, identifier: str) -> bool:
        """Check if identifier has been processed."""
        return identifier in self.processed

    def count(self) -> int:
        """Return count of processed items."""
        return len(self.processed)


def get_metadata_mapping(downloaded_dir: Path) -> dict:
    """Create mapping from PDF filename to identifier."""
    mapping = {}

    for meta_file in downloaded_dir.glob("*.meta.json"):
        try:
            with open(meta_file) as f:
                meta = json.load(f)

            identifier = meta.get("identifier")
            filename = meta.get("filename")

            if identifier and filename:
                mapping[filename] = identifier

        except Exception:
            continue

    return mapping


def get_unprocessed_pdfs(
    downloaded_dir: Path,
    processed_tracker: ProcessedPDFTracker,
    batch_tracker: BatchStateTracker
) -> Tuple[List[Path], int, int]:
    """
    Get list of unprocessed PDFs (not completed AND not already submitted).

    Returns:
        (unprocessed_pdfs, total_pdfs, processed_pdfs)
    """
    # Get all PDFs
    all_pdfs = sorted(downloaded_dir.glob("*.pdf"))

    # Get metadata mapping
    filename_to_id = get_metadata_mapping(downloaded_dir)

    # Get PDFs already in submitted/processing batches
    submitted_filenames = batch_tracker.get_submitted_pdfs()

    # Filter unprocessed
    unprocessed = []

    for pdf in all_pdfs:
        identifier = filename_to_id.get(pdf.name)

        # Skip if already processed
        if identifier and processed_tracker.is_processed(identifier):
            continue

        # Skip if already in a submitted batch
        if pdf.name in submitted_filenames:
            continue

        if identifier:
            unprocessed.append(pdf)

    return unprocessed, len(all_pdfs), processed_tracker.count()


def count_pdf_pages(pdf_path: Path) -> int:
    """Count pages in PDF using pdfinfo."""
    try:
        result = subprocess.run(
            ['pdfinfo', str(pdf_path)],
            capture_output=True,
            text=True,
            timeout=20
        )

        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if line.startswith('Pages:'):
                    parts = line.split()
                    if len(parts) >= 2 and parts[1].isdigit():
                        return int(parts[1])
    except Exception:
        pass

    return 1  # Default to 1 page


def estimate_walltime(total_pages: int) -> str:
    """
    Estimate SLURM walltime based on page count.

    Formula: 300s startup + 6s per page + 20% buffer
    """
    base_seconds = 300
    seconds_per_page = 6
    buffer = 0.20

    total_seconds = int((base_seconds + total_pages * seconds_per_page) * (1 + buffer))

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def pack_pdfs_into_chunks(
    pdfs: List[Path],
    max_pages_per_chunk: int = 1500
) -> List[Tuple[List[Path], int]]:
    """
    Pack PDFs into chunks where each chunk has <= max_pages_per_chunk.

    Returns:
        List of (pdf_list, total_pages) tuples
    """
    chunks = []
    current_chunk = []
    current_pages = 0

    print(f"  Packing PDFs into {max_pages_per_chunk}-page chunks...")

    for pdf in pdfs:
        pages = count_pdf_pages(pdf)

        # If adding this PDF would exceed limit, start new chunk
        if current_pages > 0 and current_pages + pages > max_pages_per_chunk:
            chunks.append((current_chunk, current_pages))
            current_chunk = []
            current_pages = 0

        # Add PDF to current chunk
        current_chunk.append(pdf)
        current_pages += pages

    # Add final chunk
    if current_chunk:
        chunks.append((current_chunk, current_pages))

    return chunks


def submit_batch_to_olmocr(
    pdf_dir: Path,
    chunks: List[Tuple[List[Path], int]],
    olmocr_script: Path,
    batch_number: int
) -> str:
    """
    Submit all chunks as a SLURM job array.

    Returns job ID.
    """
    # Create batch-specific directories
    batch_dir = pdf_dir / f"batch_{batch_number:04d}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    results_dir = batch_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    chunks_dir = batch_dir / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)

    # Write chunk files named chunk_1.txt, chunk_2.txt, etc. (OLMoCR expects this)
    for i, (chunk_pdfs, _) in enumerate(chunks, 1):
        chunk_file = chunks_dir / f"chunk_{i}.txt"
        chunk_file.write_text('\n'.join(pdf.name for pdf in chunk_pdfs) + '\n')

    # Find max walltime needed
    max_pages = max(pages for _, pages in chunks)
    walltime_seconds = int((300 + max_pages * 6) * 1.2)  # 300s startup + 6s/page + 20% buffer
    walltime_minutes = (walltime_seconds + 59) // 60  # Round up to minutes

    # Submit as job array
    cmd = [
        'sbatch',
        '--account', 'def-jic823_gpu',
        '--gres', 'gpu:h100:1',
        '--cpus-per-task', '8',
        '--mem', '64G',
        '--time', str(walltime_minutes),  # Minutes format
        '--array', f'1-{len(chunks)}',
        '--export', f'ALL,PDF_DIR={batch_dir}',
        '--job-name', f'olmocr_batch_{batch_number:04d}',
        '--output', str(batch_dir / 'slurm-%A_%a.out'),
        '--parsable',
        str(olmocr_script)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed: {result.stderr}")

    job_id = result.stdout.strip()
    return job_id


def check_backpressure(
    unprocessed_count: int,
    max_unprocessed: int
) -> Tuple[bool, str]:
    """
    Check if backpressure should be applied.

    Returns:
        (should_pause_downloads, status_message)
    """
    if unprocessed_count >= max_unprocessed:
        return True, f"HIGH_BACKLOG ({unprocessed_count} >= {max_unprocessed})"
    elif unprocessed_count >= max_unprocessed * 0.8:
        return False, f"APPROACHING_LIMIT ({unprocessed_count}/{max_unprocessed})"
    else:
        return False, f"OK ({unprocessed_count}/{max_unprocessed})"


def main():
    parser = argparse.ArgumentParser(
        description="Submit unprocessed PDFs to OLMoCR with backpressure"
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Pipeline YAML config"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="PDFs per batch (default: 200)"
    )
    parser.add_argument(
        "--max-unprocessed",
        type=int,
        default=500,
        help="Max unprocessed PDFs before backpressure (default: 500)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done"
    )

    args = parser.parse_args()

    # Load config
    with open(args.config) as f:
        config = yaml.safe_load(f)

    base_dir = Path(config['directories']['base_dir'])
    pdf_dir = base_dir / "01_downloaded"
    olmocr_repo = Path(config['components']['olmocr_repo'])
    olmocr_script = olmocr_repo / "smart_process_pdf_chunks.slurm"

    print("=" * 70)
    print("Simple Batch Submitter")
    print("=" * 70)
    print(f"Base directory: {base_dir}")
    print(f"PDF directory: {pdf_dir}")
    print(f"Batch size: {args.batch_size}")
    print(f"Max unprocessed: {args.max_unprocessed}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("-" * 70)

    # Load trackers
    processed_tracker_path = base_dir / "_manifests" / "processed_pdfs.json"
    processed_tracker = ProcessedPDFTracker(processed_tracker_path)

    batch_tracker_path = base_dir / "_manifests" / "batch_state.json"
    batch_tracker = BatchStateTracker(batch_tracker_path)

    # Update batch states from SLURM
    print("Checking batch states...")
    changed = batch_tracker.update_batch_states()
    if changed:
        for batch_id, new_status in changed.items():
            print(f"  {batch_id}: {new_status}")

    # Get unprocessed PDFs (excluding submitted/processing)
    unprocessed, total_pdfs, processed_count = get_unprocessed_pdfs(
        pdf_dir, processed_tracker, batch_tracker
    )

    print(f"Total PDFs: {total_pdfs}")
    print(f"Processed: {processed_count}")
    print(f"Unprocessed: {len(unprocessed)}")
    print("-" * 70)

    # Check backpressure
    should_pause, status = check_backpressure(len(unprocessed), args.max_unprocessed)

    print(f"\nBackpressure status: {status}")

    if should_pause:
        print("⚠️  BACKLOG TOO HIGH - Downloads should pause")
        print(f"   Wait for processing to catch up before downloading more")
        return 2  # Exit code 2 = backpressure active

    # Check if we have enough for a batch
    if len(unprocessed) < args.batch_size:
        print(f"\n⏳ Only {len(unprocessed)} unprocessed PDFs")
        print(f"   Need {args.batch_size} to submit a batch")
        print("   Continue downloading...")
        return 0

    # Take batch
    batch_pdfs = unprocessed[:args.batch_size]

    print(f"\n📦 Ready to submit batch of {len(batch_pdfs)} PDFs")

    # Pack into 1500-page chunks
    chunks = pack_pdfs_into_chunks(batch_pdfs, max_pages_per_chunk=1500)

    print(f"  Created {len(chunks)} chunks:")
    for i, (chunk_pdfs, pages) in enumerate(chunks, 1):
        print(f"    Chunk {i}: {len(chunk_pdfs)} PDFs, {pages:,} pages")

    if args.dry_run:
        print("\n[DRY RUN] Would submit these chunks to SLURM")
        return 0

    # Determine batch number
    existing_batches = sorted(pdf_dir.glob("batch_*"))
    if existing_batches:
        # Extract highest batch number from directory name
        last_batch = existing_batches[-1].name
        batch_num_str = last_batch.split('_')[1]
        batch_number = int(batch_num_str) + 1
    else:
        batch_number = 1

    print(f"\nBatch number: {batch_number:04d}")
    print("-" * 70)

    # Submit all chunks as a single job array
    try:
        print(f"\nSubmitting job array for {len(chunks)} chunks...")

        job_id = submit_batch_to_olmocr(
            pdf_dir,
            chunks,
            olmocr_script,
            batch_number
        )

        # Register batch in state tracker
        batch_tracker.register_batch(
            batch_number=batch_number,
            job_id=job_id,
            pdf_filenames=[pdf.name for pdf in batch_pdfs],
            chunk_count=len(chunks)
        )

        print()
        print("=" * 70)
        print("✅ Batch Submitted")
        print("=" * 70)
        print(f"Batch: batch_{batch_number:04d}")
        print(f"Total chunks: {len(chunks)}")
        print(f"Total PDFs: {len(batch_pdfs)}")
        print(f"Job Array ID: {job_id}")
        print(f"Results will appear in: {pdf_dir}/batch_{batch_number:04d}/results/")
        print("=" * 70)

        return 0

    except Exception as e:
        print()
        print("=" * 70)
        print("❌ Submission Failed")
        print("=" * 70)
        print(f"Error: {e}")
        print("=" * 70)
        return 1


if __name__ == "__main__":
    exit(main())
