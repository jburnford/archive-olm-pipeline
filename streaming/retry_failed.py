#!/usr/bin/env python3
"""
Retry Failed PDFs Script

Resubmits failed PDFs with extended walltime (10x original).
Tracks second failures for fallback OCR processing.

Usage:
  python3 streaming/retry_failed.py \
    --config config/sask_canadiana.yaml \
    [--dry-run]

Workflow:
  1. Reads _manifests/failed_pdfs.json
  2. Filters PDFs that haven't been retried yet
  3. Submits batch with 10x walltime
  4. Marks PDFs as retry_attempted
  5. Second failures go to _manifests/needs_fallback_ocr.json
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import yaml

# Import batch state tracker
sys.path.insert(0, str(Path(__file__).parent))
from batch_state_tracker import BatchStateTracker


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


def submit_retry_batch(
    pdf_dir: Path,
    chunks: List[Tuple[List[Path], int]],
    olmocr_script: Path,
    batch_number: int,
    time_multiplier: int = 10
) -> str:
    """
    Submit retry batch with extended walltime.

    Returns job ID.
    """
    # Create batch-specific directories
    batch_dir = pdf_dir / f"batch_retry_{batch_number:04d}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    results_dir = batch_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    chunks_dir = batch_dir / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)

    # Write chunk files
    for i, (chunk_pdfs, _) in enumerate(chunks, 1):
        chunk_file = chunks_dir / f"chunk_{i}.txt"
        chunk_file.write_text('\n'.join(pdf.name for pdf in chunk_pdfs) + '\n')

    # Calculate extended walltime (10x normal)
    max_pages = max(pages for _, pages in chunks)
    base_seconds = 300 + (max_pages * 6)
    extended_seconds = int(base_seconds * time_multiplier * 1.2)  # 10x + 20% buffer
    walltime_minutes = (extended_seconds + 59) // 60

    print(f"  Extended walltime: {walltime_minutes} minutes ({time_multiplier}x normal)")

    # Submit as job array
    export_env = f"ALL,PDF_DIR={pdf_dir},BATCH_DIR={batch_dir}"

    cmd = [
        'sbatch',
        '--account', 'def-jic823_gpu',
        '--gres', 'gpu:h100:1',
        '--cpus-per-task', '8',
        '--mem', '64G',
        '--time', str(walltime_minutes),
        '--array', f'1-{len(chunks)}',
        '--export', export_env,
        '--job-name', f'olmocr_retry_{batch_number:04d}',
        '--output', str(batch_dir / 'slurm-%A_%a.out'),
        '--parsable',
        str(olmocr_script)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed: {result.stderr}")

    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(
        description="Retry failed PDFs with extended walltime"
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Pipeline YAML config"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done"
    )
    parser.add_argument(
        "--time-multiplier",
        type=int,
        default=10,
        help="Walltime multiplier for retry (default: 10)"
    )

    args = parser.parse_args()

    # Load config
    with open(args.config) as f:
        config = yaml.safe_load(f)

    base_dir = Path(config['directories']['base_dir'])
    pdf_dir = base_dir / "01_downloaded"
    olmocr_repo = Path(config['components']['olmocr_repo'])
    olmocr_script = olmocr_repo / "smart_process_pdf_chunks.slurm"

    # Use config time multiplier if set
    time_multiplier = config.get('slurm', {}).get('retry_time_multiplier', args.time_multiplier)

    print("=" * 70)
    print("Retry Failed PDFs")
    print("=" * 70)
    print(f"Base directory: {base_dir}")
    print(f"Time multiplier: {time_multiplier}x")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("-" * 70)

    # Load failed PDFs manifest
    failures_path = base_dir / "_manifests" / "failed_pdfs.json"

    if not failures_path.exists():
        print("\nNo failed PDFs manifest found.")
        print("Run the main pipeline first to identify failures.")
        return 0

    with open(failures_path) as f:
        failures_data = json.load(f)

    # Filter PDFs that haven't been retried
    to_retry = []
    already_retried = []

    for failure in failures_data.get('failed', []):
        if failure.get('retry_attempted', False):
            already_retried.append(failure['filename'])
        else:
            to_retry.append(failure['filename'])

    print(f"Total failures: {len(failures_data.get('failed', []))}")
    print(f"Already retried: {len(already_retried)}")
    print(f"To retry: {len(to_retry)}")
    print("-" * 70)

    if not to_retry:
        print("\nNo PDFs need retry.")

        # Check for second-time failures
        second_failures_path = base_dir / "_manifests" / "needs_fallback_ocr.json"
        if second_failures_path.exists():
            with open(second_failures_path) as f:
                second_data = json.load(f)
            print(f"\n{len(second_data.get('failed', []))} PDFs need fallback OCR")
            print(f"See: {second_failures_path}")

        return 0

    # Find actual PDF files
    pdf_files = []
    missing = []

    for filename in to_retry:
        pdf_path = pdf_dir / filename
        if pdf_path.exists():
            pdf_files.append(pdf_path)
        else:
            missing.append(filename)

    if missing:
        print(f"\nWarning: {len(missing)} PDFs not found in {pdf_dir}")

    if not pdf_files:
        print("\nNo PDF files found to retry.")
        return 0

    print(f"\nFound {len(pdf_files)} PDFs to retry")

    # Pack into chunks
    chunks = pack_pdfs_into_chunks(pdf_files)
    print(f"Packed into {len(chunks)} chunks:")
    for i, (chunk_pdfs, pages) in enumerate(chunks, 1):
        print(f"  Chunk {i}: {len(chunk_pdfs)} PDFs, {pages:,} pages")

    if args.dry_run:
        print("\n[DRY RUN] Would submit these chunks with extended walltime")
        return 0

    # Determine retry batch number
    existing_retry_batches = sorted(pdf_dir.glob("batch_retry_*"))
    if existing_retry_batches:
        last_batch = existing_retry_batches[-1].name
        batch_num_str = last_batch.split('_')[-1]
        batch_number = int(batch_num_str) + 1
    else:
        batch_number = 1

    print(f"\nRetry batch number: {batch_number:04d}")
    print("-" * 70)

    # Submit retry batch
    try:
        print(f"\nSubmitting retry batch...")

        job_id = submit_retry_batch(
            pdf_dir,
            chunks,
            olmocr_script,
            batch_number,
            time_multiplier
        )

        # Mark PDFs as retry_attempted
        for failure in failures_data.get('failed', []):
            if failure['filename'] in to_retry:
                failure['retry_attempted'] = True
                failure['retry_batch'] = f"batch_retry_{batch_number:04d}"
                failure['retry_job_id'] = job_id
                failure['retry_submitted_at'] = datetime.now().isoformat()

        failures_data['retry_count'] = failures_data.get('retry_count', 0) + len(pdf_files)
        failures_data['last_retry_at'] = datetime.now().isoformat()

        with open(failures_path, 'w') as f:
            json.dump(failures_data, f, indent=2)

        # Register with batch tracker
        batch_tracker = BatchStateTracker(base_dir / "_manifests" / "batch_state.json")
        batch_tracker.register_batch(
            batch_number=10000 + batch_number,  # Use high numbers for retry batches
            job_id=job_id,
            pdf_filenames=[pdf.name for pdf in pdf_files],
            chunk_count=len(chunks)
        )

        print()
        print("=" * 70)
        print("Retry Batch Submitted")
        print("=" * 70)
        print(f"Batch: batch_retry_{batch_number:04d}")
        print(f"Total chunks: {len(chunks)}")
        print(f"Total PDFs: {len(pdf_files)}")
        print(f"Job Array ID: {job_id}")
        print(f"Walltime: {time_multiplier}x normal")
        print(f"Results: {pdf_dir}/batch_retry_{batch_number:04d}/results/")
        print("=" * 70)

        return 0

    except Exception as e:
        print()
        print("=" * 70)
        print("Retry Submission Failed")
        print("=" * 70)
        print(f"Error: {e}")
        print("=" * 70)
        return 1


def check_retry_results(config_path: Path):
    """
    Check results of retry batches and move second failures to fallback list.

    This should be called after retry batches complete.
    """
    with open(config_path) as f:
        config = yaml.safe_load(f)

    base_dir = Path(config['directories']['base_dir'])

    # Load failures manifest
    failures_path = base_dir / "_manifests" / "failed_pdfs.json"
    if not failures_path.exists():
        return

    with open(failures_path) as f:
        failures_data = json.load(f)

    # Check batch tracker for completed retry batches
    batch_tracker = BatchStateTracker(base_dir / "_manifests" / "batch_state.json")
    batch_tracker.update_batch_states()

    # Find PDFs that failed twice (in retry batch that failed)
    second_failures = []

    for failure in failures_data.get('failed', []):
        if not failure.get('retry_attempted'):
            continue

        retry_batch = failure.get('retry_batch')
        if not retry_batch:
            continue

        # Check if retry batch failed
        batch_info = batch_tracker.state.get('batches', {}).get(retry_batch, {})
        if batch_info.get('status') == 'failed':
            second_failures.append({
                'filename': failure['filename'],
                'original_failure_at': failure.get('failed_at'),
                'retry_failure_at': datetime.now().isoformat(),
                'retry_batch': retry_batch
            })

    if second_failures:
        # Save to fallback OCR manifest
        fallback_path = base_dir / "_manifests" / "needs_fallback_ocr.json"

        if fallback_path.exists():
            with open(fallback_path) as f:
                fallback_data = json.load(f)
        else:
            fallback_data = {'failed': [], 'count': 0}

        existing_names = {f['filename'] for f in fallback_data['failed']}

        for failure in second_failures:
            if failure['filename'] not in existing_names:
                fallback_data['failed'].append(failure)

        fallback_data['count'] = len(fallback_data['failed'])
        fallback_data['last_updated'] = datetime.now().isoformat()

        with open(fallback_path, 'w') as f:
            json.dump(fallback_data, f, indent=2)

        print(f"\n{len(second_failures)} PDFs need fallback OCR")
        print(f"Saved to: {fallback_path}")


if __name__ == "__main__":
    exit(main())
