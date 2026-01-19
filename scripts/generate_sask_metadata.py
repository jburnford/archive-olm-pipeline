#!/usr/bin/env python3
"""
Generate metadata files for Saskatchewan Canadiana PDFs.

Creates .meta.json files alongside PDFs for the pipeline to track identifiers.

Usage:
  python3 scripts/generate_sask_metadata.py \
    --pdf-dir /path/to/01_downloaded \
    [--dry-run]
"""

import argparse
import json
from datetime import datetime
from pathlib import Path


def generate_metadata(pdf_path: Path) -> dict:
    """
    Generate metadata for a PDF file.

    Identifier is derived from filename (without .pdf extension).
    """
    filename = pdf_path.name
    identifier = pdf_path.stem  # filename without extension

    return {
        "identifier": identifier,
        "filename": filename,
        "collection": "saskatchewan_canadiana",
        "source": "canadiana.org",
        "metadata_generated_at": datetime.utcnow().isoformat() + "Z"
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate metadata files for Saskatchewan Canadiana PDFs"
    )
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        required=True,
        help="Directory containing PDFs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done"
    )

    args = parser.parse_args()

    pdf_dir = args.pdf_dir

    print("=" * 70)
    print("Saskatchewan Canadiana Metadata Generator")
    print("=" * 70)
    print(f"PDF directory: {pdf_dir}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("-" * 70)

    # Find all PDFs
    pdfs = sorted(pdf_dir.glob("*.pdf"))
    print(f"Found {len(pdfs):,} PDF files")

    # Check existing metadata
    existing_meta = set(p.stem for p in pdf_dir.glob("*.meta.json"))
    print(f"Existing metadata files: {len(existing_meta):,}")

    # Find PDFs needing metadata
    need_meta = [p for p in pdfs if p.stem not in existing_meta]
    print(f"PDFs needing metadata: {len(need_meta):,}")
    print("-" * 70)

    if not need_meta:
        print("\nAll PDFs already have metadata files.")
        return 0

    # Generate metadata
    created = 0
    errors = 0

    for i, pdf_path in enumerate(need_meta, 1):
        meta = generate_metadata(pdf_path)
        meta_path = pdf_path.with_suffix('.meta.json')

        if args.dry_run:
            if i <= 5:
                print(f"  [DRY RUN] Would create: {meta_path.name}")
            elif i == 6:
                print(f"  ... and {len(need_meta) - 5} more")
        else:
            try:
                with open(meta_path, 'w') as f:
                    json.dump(meta, f, indent=2)
                created += 1

                if created % 5000 == 0:
                    print(f"  Created {created:,} / {len(need_meta):,} metadata files...")

            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  Error creating {meta_path.name}: {e}")

    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)

    if args.dry_run:
        print(f"Would create: {len(need_meta):,} metadata files")
    else:
        print(f"Created: {created:,} metadata files")
        if errors:
            print(f"Errors: {errors}")

    print("=" * 70)

    return 0


if __name__ == "__main__":
    exit(main())
