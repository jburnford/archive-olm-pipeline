#!/usr/bin/env python3
"""
Report split coverage for OLMoCR batches.

For each batch under 03_ocr_processing, reads chunk lists in chunks/chunk_*.txt
and checks for the presence of per-PDF JSON files in results/json/<pdf>.json.
Writes a CSV report with rows (batch_id, chunk_idx, pdf, split_present) and
prints a short summary.

Usage:
  python3 tools/report_split_status.py --base-dir /path/to/caribbean_pipeline       [--out analysis_output/split_status.csv]
"""

import argparse
import csv
from pathlib import Path
from typing import List


def read_chunk_file(chunk_path: Path) -> List[str]:
    try:
        return [ln.strip() for ln in chunk_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    except Exception:
        return []


def main():
    ap = argparse.ArgumentParser(description="Report split coverage for OLMoCR batches")
    ap.add_argument("--base-dir", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=Path("analysis_output/split_status.csv"))
    args = ap.parse_args()

    base = args.base_dir
    batches_dir = base / "03_ocr_processing"
    out_path = args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    total_pdfs = 0
    total_split = 0
    per_batch_summary = []

    for batch_dir in sorted(p for p in batches_dir.glob("batch_*") if p.is_dir()):
        batch_id = batch_dir.name
        chunks_dir = batch_dir / "chunks"
        json_out_dir = batch_dir / "results" / "json"
        if not chunks_dir.exists():
            continue
        chunk_files = sorted(chunks_dir.glob("chunk_*.txt"))
        complete_chunks = 0
        partial_chunks = 0
        missing_files = 0

        for cf in chunk_files:
            try:
                idx = int(cf.stem.split("_")[1])
            except Exception:
                idx = -1
            pdfs = read_chunk_file(cf)
            if not pdfs:
                continue
            present_flags = []
            for pdf in pdfs:
                stem = Path(pdf).stem
                target = json_out_dir / f"{stem}.json"
                present = target.exists()
                rows.append((batch_id, idx, pdf, 1 if present else 0))
                total_pdfs += 1
                total_split += 1 if present else 0
                present_flags.append(present)
                if not present:
                    missing_files += 1
            if all(present_flags):
                complete_chunks += 1
            else:
                partial_chunks += 1

        if complete_chunks or partial_chunks:
            per_batch_summary.append((batch_id, complete_chunks, partial_chunks, missing_files))

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["batch_id", "chunk_idx", "pdf", "split_present"])
        for r in rows:
            w.writerow(r)

    print("=" * 70)
    print("Split Coverage Report")
    print("=" * 70)
    print(f"Output: {out_path}")
    print(f"Batches covered: {len(per_batch_summary)}")
    print(f"PDFs total in chunk lists: {total_pdfs}")
    print(f"PDFs with split JSON present: {total_split}")
    missing = total_pdfs - total_split
    print(f"Missing per-PDF JSON: {missing}")
    if per_batch_summary:
        print("
Per-batch summary (batch_id, complete_chunks, partial_chunks, missing_files):")
        for b in per_batch_summary[:20]:
            print("  ", b)


if __name__ == "__main__":
    main()
