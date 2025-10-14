#!/usr/bin/env python3
"""
Generate minimal metadata files for locally provided PDFs in 01_downloaded/.

This is intended for priority runs where PDFs are uploaded directly (not via
the downloader). The simplified finalizer and submitter expect per-PDF
metadata JSON files in 01_downloaded/ so they can match results to identifiers.

Metadata written per PDF (<identifier>.meta.json):
  - identifier: stem of the PDF filename
  - filename: original PDF filename
  - file_path: relative path within base_dir (01_downloaded/<filename>)
  - collection: label for this batch (default: local_upload)
  - title: derived from filename (optional best-effort)
  - downloaded_at: UTC timestamp

Usage:
  python3 tools/make_local_meta.py \
    --base-dir /home/jic823/projects/def-jic823/caribbean_pipeline \
    [--collection local_priority] [--pdf-dir <override>] [--dry-run]
"""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path


def infer_title(stem: str) -> str:
    # Replace separators with spaces and tidy
    t = re.sub(r"[_-]+", " ", stem)
    # Collapse multiple spaces
    t = re.sub(r"\s+", " ", t).strip()
    return t


def main():
    ap = argparse.ArgumentParser(description="Create minimal meta files for PDFs in 01_downloaded")
    ap.add_argument("--base-dir", type=Path, required=True, help="Pipeline base directory")
    ap.add_argument("--pdf-dir", type=Path, help="PDF directory (default: <base>/01_downloaded)")
    ap.add_argument("--collection", default="local_upload", help="Collection label for these PDFs")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be written without creating files")
    args = ap.parse_args()

    base = args.base_dir
    pdf_dir = args.pdf_dir or (base / "01_downloaded")
    pdf_dir = pdf_dir.resolve()

    if not pdf_dir.exists():
        print(f"PDF directory not found: {pdf_dir}")
        return 1

    written = 0
    skipped = 0

    for pdf in sorted(pdf_dir.glob("*.pdf")):
        identifier = pdf.stem
        meta_path = pdf_dir / f"{identifier}.meta.json"

        if meta_path.exists():
            skipped += 1
            continue

        meta = {
            "identifier": identifier,
            "collection": args.collection,
            "title": infer_title(identifier),
            "downloaded_at": datetime.utcnow().isoformat() + "Z",
            "filename": pdf.name,
            "file_path": f"01_downloaded/{pdf.name}",
            "source": "local_upload"
        }

        if args.dry_run:
            print(f"[DRY RUN] Would write: {meta_path}")
        else:
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
            written += 1

    print("=" * 70)
    print("Local PDF Metadata Generation")
    print("=" * 70)
    print(f"PDF directory: {pdf_dir}")
    print(f"Collection: {args.collection}")
    print(f"Written: {written}")
    print(f"Skipped (already had meta): {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

