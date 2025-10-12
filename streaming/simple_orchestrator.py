#!/usr/bin/env python3
"""
Simple Pipeline Orchestrator

Coordinates the complete pipeline with smart backpressure:
  Download → Batch Submit → Split → Finalize → Export

Usage:
  python3 streaming/simple_orchestrator.py \
    --config config/caribbean_filebased.yaml \
    [--check-interval 60] \
    [--max-iterations -1]

Features:
  - Smart backpressure: pauses downloads when unprocessed > 500
  - Continuous split/finalize of completed results
  - Periodic DuckDB export
  - Graceful shutdown on interrupt
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
import yaml


def run_command(cmd: list, description: str) -> int:
    """
    Run a command and return exit code.

    Returns:
        Exit code (0 = success, 1 = error, 2 = backpressure)
    """
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except Exception as e:
        print(f"  ✗ Error running {description}: {e}")
        return 1


def download_phase(config_path: Path, batch_size: int = 200) -> int:
    """
    Run download phase.

    Returns:
        0 = downloaded items
        1 = error
        2 = backpressure (should skip)
    """
    print("\n" + "─" * 70)
    print("PHASE: Download")
    print("─" * 70)

    # For now, downloads are managed externally
    # In future, can integrate downloader here
    print("⏭  Skipping download (run manually or via separate job)")
    return 0


def batch_submit_phase(config_path: Path, batch_size: int = 200) -> int:
    """
    Run batch submit phase.

    Returns:
        0 = success (batch submitted or not needed)
        1 = error
        2 = backpressure active
    """
    print("\n" + "─" * 70)
    print("PHASE: Batch Submit")
    print("─" * 70)

    cmd = [
        'python3',
        str(Path(__file__).parent / 'simple_batch_submitter.py'),
        '--config', str(config_path),
        '--batch-size', str(batch_size)
    ]

    exit_code = run_command(cmd, "batch submitter")

    if exit_code == 0:
        print("  ✓ Batch phase complete")
    elif exit_code == 2:
        print("  ⚠️  Backpressure active - downloads should pause")
    else:
        print("  ⚠️  Batch submit encountered issues")

    return exit_code


def split_phase(base_dir: Path) -> int:
    """
    Run split phase.

    Returns:
        0 = success
        1 = error
    """
    print("\n" + "─" * 70)
    print("PHASE: Split JSONL")
    print("─" * 70)

    downloaded_dir = base_dir / "01_downloaded"
    results_dir = downloaded_dir / "results" / "results"

    # Check if there are JSONL files to split
    if not results_dir.exists():
        print("  ⏭  No results directory yet")
        return 0

    jsonl_files = list(results_dir.glob("*.jsonl"))

    if not jsonl_files:
        print("  ⏭  No JSONL files to split")
        return 0

    print(f"  Found {len(jsonl_files)} JSONL files")

    cmd = [
        'python3',
        str(Path(__file__).parent.parent / 'orchestration' / 'split_jsonl_to_json.py'),
        str(downloaded_dir)
    ]

    exit_code = run_command(cmd, "splitter")

    if exit_code == 0:
        print("  ✓ Split phase complete")
    else:
        print("  ⚠️  Split phase encountered issues")

    return exit_code


def finalize_phase(config_path: Path) -> int:
    """
    Run finalize phase.

    Returns:
        0 = success
        1 = error
    """
    print("\n" + "─" * 70)
    print("PHASE: Finalize")
    print("─" * 70)

    # Load config to get base_dir
    with open(config_path) as f:
        config = yaml.safe_load(f)

    base_dir = Path(config['directories']['base_dir'])
    results_json_dir = base_dir / "01_downloaded" / "results" / "json"

    # Check if there are JSON files to finalize
    if not results_json_dir.exists():
        print("  ⏭  No results/json directory yet")
        return 0

    json_files = list(results_json_dir.glob("*.json"))

    if not json_files:
        print("  ⏭  No JSON files to finalize")
        return 0

    print(f"  Found {len(json_files)} JSON files")

    cmd = [
        'python3',
        str(Path(__file__).parent / 'simplified_finalizer.py'),
        '--base-dir', str(base_dir),
        '--auto-delete-pdfs'  # Delete PDFs after successful finalization
    ]

    exit_code = run_command(cmd, "finalizer")

    if exit_code == 0:
        print("  ✓ Finalize phase complete")
    else:
        print("  ⚠️  Finalize phase encountered issues")

    return exit_code


def export_phase(config_path: Path) -> int:
    """
    Run export phase (DuckDB catalog).

    Returns:
        0 = success
        1 = error
    """
    print("\n" + "─" * 70)
    print("PHASE: Export Catalog")
    print("─" * 70)

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
        print("  ✓ Export phase complete")
    else:
        print("  ⚠️  Export phase encountered issues")

    return exit_code


def print_status(iteration: int, backpressure: bool):
    """Print status header."""
    status = "🔴 BACKPRESSURE" if backpressure else "🟢 RUNNING"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n" + "=" * 70)
    print(f"Iteration {iteration} | {timestamp} | {status}")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Simple pipeline orchestrator with backpressure"
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
        default=60,
        help="Seconds between checks (default: 60)"
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
    batch_size = config.get('ocr', {}).get('pdfs_per_batch', 200)

    print("=" * 70)
    print("Simple Pipeline Orchestrator")
    print("=" * 70)
    print(f"Config: {args.config}")
    print(f"Base dir: {base_dir}")
    print(f"Batch size: {batch_size}")
    print(f"Check interval: {args.check_interval}s")
    print(f"Max iterations: {'∞' if args.max_iterations < 0 else args.max_iterations}")
    print("=" * 70)

    iteration = 0
    backpressure_active = False

    try:
        while args.max_iterations < 0 or iteration < args.max_iterations:
            iteration += 1

            print_status(iteration, backpressure_active)

            # Phase 1: Batch Submit (checks backpressure internally)
            batch_exit = batch_submit_phase(args.config, batch_size)
            backpressure_active = (batch_exit == 2)

            # Phase 2: Split (process any completed JSONL)
            split_phase(base_dir)

            # Phase 3: Finalize (move to 02_processed and delete PDFs)
            finalize_phase(args.config)

            # Phase 4: Export (periodic)
            if iteration % args.export_interval == 0:
                export_phase(args.config)

            # Status
            if backpressure_active:
                print("\n⚠️  BACKPRESSURE ACTIVE")
                print("   Unprocessed PDFs > 500")
                print("   Waiting for OCR to catch up...")
            else:
                print("\n✓ Iteration complete")

            # Sleep
            print(f"\n⏱️  Sleeping {args.check_interval}s...")
            time.sleep(args.check_interval)

    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print("Orchestrator interrupted by user")
        print("=" * 70)
        return 0

    print("\n" + "=" * 70)
    print(f"Orchestrator complete ({iteration} iterations)")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    exit(main())
