#!/usr/bin/env python3
"""
Saskatchewan Canadiana Pipeline Orchestrator

Coordinates processing of pre-downloaded PDFs:
  Batch Submit (queue-limited) -> Split -> Finalize -> Export

Key differences from simple_orchestrator.py:
  - No download phase (PDFs already complete)
  - Rolling batch submission (keeps N batches in queue)
  - No PDF deletion (preserves originals)
  - Tracks failed PDFs for retry

Usage:
  python3 streaming/sask_orchestrator.py \
    --config config/sask_canadiana.yaml \
    [--check-interval 120] \
    [--max-iterations -1]
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

import yaml

# Import batch state tracker
sys.path.insert(0, str(Path(__file__).parent))
from batch_state_tracker import BatchStateTracker


def run_command(cmd: list, description: str) -> int:
    """
    Run a command and return exit code.

    Returns:
        Exit code (0 = success, 1 = error)
    """
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except Exception as e:
        print(f"  x Error running {description}: {e}")
        return 1


def get_queue_status(base_dir: Path) -> Dict[str, int]:
    """
    Get current queue status from batch state tracker.

    Returns dict with counts by status.
    """
    tracker = BatchStateTracker(base_dir / "_manifests" / "batch_state.json")
    tracker.update_batch_states()
    return tracker.get_summary()


def batch_submit_phase(config_path: Path, batch_size: int, max_queued: int) -> int:
    """
    Run batch submit phase with queue limit check.

    Only submits if queued batches < max_queued.

    Returns:
        0 = success (batch submitted or queue full)
        1 = error
    """
    print("\n" + "-" * 70)
    print("PHASE: Batch Submit")
    print("-" * 70)

    # Load config to get base_dir
    with open(config_path) as f:
        config = yaml.safe_load(f)

    base_dir = Path(config['directories']['base_dir'])

    # Check current queue status
    queue_status = get_queue_status(base_dir)
    queued_batches = queue_status['submitted'] + queue_status['processing']

    print(f"  Queue status: {queued_batches} batches in queue")
    print(f"    Submitted: {queue_status['submitted']}")
    print(f"    Processing: {queue_status['processing']}")
    print(f"    Completed: {queue_status['completed']}")
    print(f"    Failed: {queue_status['failed']}")
    print(f"  Max queued: {max_queued}")

    if queued_batches >= max_queued:
        print(f"\n  >> Queue at capacity ({queued_batches}/{max_queued})")
        print("     Waiting for batches to complete...")
        return 0

    print(f"\n  Queue has space ({queued_batches}/{max_queued})")
    print("  Submitting new batch...")

    cmd = [
        'python3',
        str(Path(__file__).parent / 'simple_batch_submitter.py'),
        '--config', str(config_path),
        '--batch-size', str(batch_size),
        '--max-unprocessed', '0'  # No backpressure cap
    ]

    exit_code = run_command(cmd, "batch submitter")

    if exit_code == 0:
        print("  > Batch phase complete")
    else:
        print("  ! Batch submit encountered issues")

    return exit_code


def split_phase(base_dir: Path) -> int:
    """
    Run split phase.

    Returns:
        0 = success
        1 = error
    """
    print("\n" + "-" * 70)
    print("PHASE: Split JSONL")
    print("-" * 70)

    downloaded_dir = base_dir / "01_downloaded"

    # Find all batch directories with results
    # Note: OLMoCR creates results in batch_*/results/results/ (nested)
    batch_dirs = sorted(downloaded_dir.glob("batch_*/results/results"))

    if not batch_dirs:
        print("  >> No batch results directories yet")
        return 0

    # Count total JSONL files across all batches
    total_jsonl = 0
    for batch_result_dir in batch_dirs:
        jsonl_count = len(list(batch_result_dir.glob("*.jsonl")))
        if jsonl_count > 0:
            print(f"  Batch {batch_result_dir.parent.parent.name}: {jsonl_count} JSONL files")
            total_jsonl += jsonl_count

    if total_jsonl == 0:
        print("  >> No JSONL files to split")
        return 0

    print(f"  Total: {total_jsonl} JSONL files")

    # Process each batch directory separately
    split_script = Path(__file__).parent.parent / 'orchestration' / 'split_jsonl_to_json.py'
    all_success = True

    for batch_result_dir in batch_dirs:
        batch_dir = batch_result_dir.parent.parent  # Go up from results/results to batch_NNNN
        batch_name = batch_dir.name

        print(f"\n  Processing {batch_name}...")

        cmd = [
            'python3',
            str(split_script),
            str(batch_dir)
        ]

        exit_code = run_command(cmd, f"splitter ({batch_name})")

        if exit_code != 0:
            all_success = False

    if all_success:
        print("\n  > Split phase complete")
    else:
        print("\n  ! Split phase encountered issues")

    return 0 if all_success else 1


def finalize_phase(config_path: Path) -> int:
    """
    Run finalize phase WITHOUT PDF deletion.

    Returns:
        0 = success
        1 = error
    """
    print("\n" + "-" * 70)
    print("PHASE: Finalize")
    print("-" * 70)

    # Load config to get base_dir
    with open(config_path) as f:
        config = yaml.safe_load(f)

    base_dir = Path(config['directories']['base_dir'])
    downloaded_dir = base_dir / "01_downloaded"

    # Find all batch directories with JSON results
    batch_json_dirs = sorted(downloaded_dir.glob("batch_*/results/json"))

    if not batch_json_dirs:
        print("  >> No batch JSON directories yet")
        return 0

    # Count total JSON files across all batches
    total_json = 0
    for json_dir in batch_json_dirs:
        json_count = len(list(json_dir.glob("*.json")))
        if json_count > 0:
            print(f"  Batch {json_dir.parent.parent.name}: {json_count} JSON files")
            total_json += json_count

    if total_json == 0:
        print("  >> No JSON files to finalize")
        return 0

    print(f"  Total: {total_json} JSON files")

    # Process each batch directory separately
    finalizer_script = Path(__file__).parent / 'simplified_finalizer.py'
    all_success = True

    for json_dir in batch_json_dirs:
        batch_dir = json_dir.parent.parent  # Go up from results/json to batch_NNNN
        batch_name = batch_dir.name

        # Check if this batch has JSON files
        if not list(json_dir.glob("*.json")):
            continue

        print(f"\n  Finalizing {batch_name}...")

        # NOTE: No --auto-delete-pdfs flag - preserve original PDFs
        cmd = [
            'python3',
            str(finalizer_script),
            '--base-dir', str(base_dir),
            '--results-dir', str(json_dir)
        ]

        exit_code = run_command(cmd, f"finalizer ({batch_name})")

        if exit_code != 0:
            all_success = False

    if all_success:
        print("\n  > Finalize phase complete")
    else:
        print("\n  ! Finalize phase encountered issues")

    return 0 if all_success else 1


def track_failures_phase(base_dir: Path) -> int:
    """
    Track failed PDFs from failed batches.

    Saves to _manifests/failed_pdfs.json for later retry.

    Returns:
        0 = success
    """
    print("\n" + "-" * 70)
    print("PHASE: Track Failures")
    print("-" * 70)

    tracker = BatchStateTracker(base_dir / "_manifests" / "batch_state.json")

    # Get PDFs from failed batches
    failed_pdfs = tracker.get_failed_batch_pdfs()

    if not failed_pdfs:
        print("  No new failures to track")
        return 0

    # Load existing failures manifest
    failures_path = base_dir / "_manifests" / "failed_pdfs.json"

    if failures_path.exists():
        with open(failures_path) as f:
            failures_data = json.load(f)
    else:
        failures_data = {
            "failed": [],
            "count": 0,
            "retry_count": 0
        }

    # Add new failures
    existing_names = {f['filename'] for f in failures_data['failed']}
    new_failures = []

    for pdf_name in failed_pdfs:
        if pdf_name not in existing_names:
            new_failures.append({
                "filename": pdf_name,
                "failed_at": datetime.now().isoformat(),
                "retry_attempted": False
            })

    if new_failures:
        failures_data['failed'].extend(new_failures)
        failures_data['count'] = len(failures_data['failed'])
        failures_data['last_updated'] = datetime.now().isoformat()

        # Save
        failures_path.parent.mkdir(parents=True, exist_ok=True)
        with open(failures_path, 'w') as f:
            json.dump(failures_data, f, indent=2)

        print(f"  Added {len(new_failures)} new failures")
        print(f"  Total tracked failures: {failures_data['count']}")
    else:
        print("  No new unique failures")

    return 0


def export_phase(config_path: Path) -> int:
    """
    Run export phase (DuckDB catalog).

    Returns:
        0 = success
        1 = error
    """
    print("\n" + "-" * 70)
    print("PHASE: Export Catalog")
    print("-" * 70)

    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)

    base_dir = Path(config['directories']['base_dir'])

    cmd = [
        'python3',
        str(Path(__file__).parent.parent / 'tools' / 'export_catalog_duckdb.py'),
        '--base-dir', str(base_dir),
        '--fast'  # Skip page counting for speed
    ]

    exit_code = run_command(cmd, "exporter")

    if exit_code == 0:
        print("  > Export phase complete")
    else:
        print("  ! Export phase encountered issues")

    return exit_code


def print_status(iteration: int, base_dir: Path):
    """Print status header with queue info."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get counts
    processed_dir = base_dir / "02_processed"
    processed_count = len(list(processed_dir.iterdir())) if processed_dir.exists() else 0

    pdf_dir = base_dir / "01_downloaded"
    pdf_count = len(list(pdf_dir.glob("*.pdf"))) if pdf_dir.exists() else 0

    print("\n" + "=" * 70)
    print(f"Iteration {iteration} | {timestamp}")
    print(f"PDFs: {pdf_count:,} total | {processed_count:,} processed | {pdf_count - processed_count:,} remaining")
    print("=" * 70)


def print_progress(base_dir: Path):
    """Print overall progress summary."""
    # Get counts
    processed_dir = base_dir / "02_processed"
    processed_count = len(list(processed_dir.iterdir())) if processed_dir.exists() else 0

    pdf_dir = base_dir / "01_downloaded"
    pdf_count = len(list(pdf_dir.glob("*.pdf"))) if pdf_dir.exists() else 0

    if pdf_count > 0:
        pct = (processed_count / pdf_count) * 100
        print(f"\nProgress: {processed_count:,}/{pdf_count:,} ({pct:.1f}%)")

        # Estimate remaining
        remaining = pdf_count - processed_count
        print(f"Remaining: {remaining:,} PDFs")


def main():
    parser = argparse.ArgumentParser(
        description="Saskatchewan Canadiana pipeline orchestrator"
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Pipeline YAML config"
    )
    parser.add_argument(
        "--check-interval",
        type=int,
        default=120,
        help="Seconds between checks (default: 120)"
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=-1,
        help="Max iterations (-1 = infinite, default: -1)"
    )
    parser.add_argument(
        "--export-interval",
        type=int,
        default=10,
        help="Export catalog every N iterations (default: 10)"
    )

    args = parser.parse_args()

    # Load config
    with open(args.config) as f:
        config = yaml.safe_load(f)

    base_dir = Path(config['directories']['base_dir'])
    batch_size = config.get('ocr', {}).get('pdfs_per_batch', 500)
    max_queued = config.get('ocr', {}).get('max_queued_batches', 8)

    print("=" * 70)
    print("Saskatchewan Canadiana Pipeline Orchestrator")
    print("=" * 70)
    print(f"Config: {args.config}")
    print(f"Base dir: {base_dir}")
    print(f"Batch size: {batch_size}")
    print(f"Max queued batches: {max_queued}")
    print(f"Check interval: {args.check_interval}s")
    print(f"Max iterations: {'infinite' if args.max_iterations < 0 else args.max_iterations}")
    print("=" * 70)

    # Initial progress
    print_progress(base_dir)

    iteration = 0

    try:
        while args.max_iterations < 0 or iteration < args.max_iterations:
            iteration += 1

            print_status(iteration, base_dir)

            # Phase 1: Batch Submit (with queue limit)
            batch_submit_phase(args.config, batch_size, max_queued)

            # Phase 2: Split (process any completed JSONL)
            split_phase(base_dir)

            # Phase 3: Finalize (move to 02_processed, NO PDF deletion)
            finalize_phase(args.config)

            # Phase 4: Track failures
            track_failures_phase(base_dir)

            # Phase 5: Export (periodic)
            if iteration % args.export_interval == 0:
                export_phase(args.config)

            # Progress summary
            print_progress(base_dir)

            # Sleep
            print(f"\n>> Sleeping {args.check_interval}s...")
            time.sleep(args.check_interval)

    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print("Orchestrator interrupted by user")
        print("=" * 70)
        print_progress(base_dir)
        return 0

    print("\n" + "=" * 70)
    print(f"Orchestrator complete ({iteration} iterations)")
    print("=" * 70)
    print_progress(base_dir)
    return 0


if __name__ == "__main__":
    exit(main())
