#!/usr/bin/env python3
"""
Batch Reconcile: detect PDFs not completed within a batch and optionally resubmit them.

Compares batch_XXXX/chunks/chunk_*.txt with batch_XXXX/processed_files.log to
identify missing PDFs. Optionally submits a new OLMoCR array containing only
the missing PDFs, using existing submitter utilities.

Usage:
  python3 streaming/batch_reconcile.py \
    --base-dir /home/jic823/projects/def-jic823/caribbean_pipeline \
    [--config config/caribbean_filebased.yaml] \
    [--submit] [--workers 1] [--pages-per-group 1]
"""

import argparse
from pathlib import Path
from typing import List, Set, Tuple

import yaml


def read_chunk_pdfs(chunks_dir: Path) -> List[str]:
    pdfs: List[str] = []
    for chunk_file in sorted(chunks_dir.glob("chunk_*.txt")):
        for line in chunk_file.read_text(encoding="utf-8").splitlines():
            name = line.strip()
            if name:
                pdfs.append(name)
    return pdfs


def read_processed_log(batch_dir: Path) -> Set[str]:
    log = batch_dir / "processed_files.log"
    done: Set[str] = set()
    if log.exists():
        for line in log.read_text(encoding="utf-8").splitlines():
            name = line.strip()
            if name:
                done.add(name)
    return done


def pack_by_pages(pdf_paths: List[Path], max_pages_per_chunk: int = 1500) -> List[Tuple[List[Path], int]]:
    # Local import from submitter to reuse counting logic
    from .simple_batch_submitter import count_pdf_pages

    chunks: List[Tuple[List[Path], int]] = []
    current: List[Path] = []
    pages_acc = 0

    for pdf in pdf_paths:
        pages = count_pdf_pages(pdf)
        if pages_acc > 0 and pages_acc + pages > max_pages_per_chunk:
            chunks.append((current, pages_acc))
            current, pages_acc = [], 0
        current.append(pdf)
        pages_acc += pages

    if current:
        chunks.append((current, pages_acc))

    return chunks


def submit_missing(
    base_dir: Path,
    missing_pdfs: List[Path],
    config_path: Path,
    workers: int | None,
    pages_per_group: int | None,
) -> str:
    # Load config to locate OLMoCR script
    with open(config_path) as f:
        config = yaml.safe_load(f)
    olmocr_repo = Path(config['components']['olmocr_repo'])
    olmocr_script = olmocr_repo / "smart_process_pdf_chunks.slurm"

    # Determine next batch number
    pdf_dir = base_dir / "01_downloaded"
    existing = sorted(pdf_dir.glob("batch_*"))
    if existing:
        last = existing[-1].name
        batch_number = int(last.split('_')[1]) + 1
    else:
        batch_number = 1

    # Pack into chunks (keep 1500 page cap)
    chunks = pack_by_pages(missing_pdfs, max_pages_per_chunk=1500)

    # Submit via submitter utility
    from .simple_batch_submitter import submit_batch_to_olmocr
    job_id = submit_batch_to_olmocr(
        pdf_dir,
        chunks,
        olmocr_script,
        batch_number,
        workers=workers,
        pages_per_group=pages_per_group,
    )
    return job_id


def main():
    ap = argparse.ArgumentParser(description="Reconcile OLMoCR batch completions and resubmit missing PDFs")
    ap.add_argument("--base-dir", type=Path, required=True, help="Pipeline base directory")
    ap.add_argument("--config", type=Path, default=Path("config/caribbean_filebased.yaml"), help="Pipeline config for component paths")
    ap.add_argument("--submit", action="store_true", help="Submit retries for missing PDFs")
    ap.add_argument("--workers", type=int, default=None, help="WORKERS env for OLMoCR retries")
    ap.add_argument("--pages-per-group", type=int, default=None, help="PAGES_PER_GROUP env for retries")

    args = ap.parse_args()

    base = args.base_dir
    pdf_dir = base / "01_downloaded"

    total_missing: List[Path] = []
    batches_checked = 0

    for batch_dir in sorted(pdf_dir.glob("batch_*")):
        chunks_dir = batch_dir / "chunks"
        if not chunks_dir.exists():
            continue
        batches_checked += 1

        requested = read_chunk_pdfs(chunks_dir)
        done = read_processed_log(batch_dir)

        missing_names = [n for n in requested if n not in done]
        missing_paths = [pdf_dir / n for n in missing_names if (pdf_dir / n).exists()]

        print(f"{batch_dir.name}: requested={len(requested)} done={len(done)} missing={len(missing_paths)}")
        total_missing.extend(missing_paths)

    print("-" * 70)
    print(f"Batches checked: {batches_checked}")
    print(f"Total missing PDFs: {len(total_missing)}")

    if not total_missing:
        return 0

    if args.submit:
        print("Submitting retries for missing PDFs...")
        job_id = submit_missing(base, total_missing, args.config, args.workers, args.pages_per_group)
        print(f"Retry job array: {job_id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

