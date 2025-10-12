#!/usr/bin/env python3
"""
Delete PDFs from 01_downloaded for items already in 02_processed.

This is a one-time cleanup script to remove PDFs that were already
processed but not deleted by the old pipeline.

Usage:
  # Dry run (safe, shows what would be deleted)
  python3 tools/cleanup_processed_pdfs.py \
    --base-dir /path/to/caribbean_pipeline \
    --dry-run

  # Actually delete
  python3 tools/cleanup_processed_pdfs.py \
    --base-dir /path/to/caribbean_pipeline
"""

import argparse
import json
from pathlib import Path


def load_metadata_index(downloaded_dir: Path) -> dict:
    """Index metadata files by identifier."""
    index = {}

    for meta_file in downloaded_dir.glob("*.meta.json"):
        try:
            with open(meta_file) as f:
                meta = json.load(f)

            identifier = meta.get("identifier")
            filename = meta.get("filename")

            if identifier:
                index[identifier] = {
                    "filename": filename,
                    "meta_path": meta_file
                }
        except Exception as e:
            print(f"⚠️  Error reading {meta_file.name}: {e}")

    return index


def main():
    parser = argparse.ArgumentParser(
        description="Clean up PDFs for already-processed items"
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        required=True,
        help="Pipeline base directory"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without deleting"
    )

    args = parser.parse_args()

    downloaded_dir = args.base_dir / "01_downloaded"
    processed_dir = args.base_dir / "02_processed"

    print("=" * 70)
    print("Cleanup Processed PDFs")
    print("=" * 70)
    print(f"Downloaded: {downloaded_dir}")
    print(f"Processed: {processed_dir}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("-" * 70)

    # Get processed identifiers
    if not processed_dir.exists():
        print("Error: 02_processed directory not found")
        return 1

    processed_ids = {d.name for d in processed_dir.iterdir() if d.is_dir()}
    print(f"Processed items: {len(processed_ids)}")

    # Load metadata index
    meta_index = load_metadata_index(downloaded_dir)
    print(f"Metadata files: {len(meta_index)}")
    print("-" * 70)

    # Find PDFs to delete
    to_delete = []

    for identifier, info in meta_index.items():
        if identifier in processed_ids:
            filename = info["filename"]
            if filename:
                pdf_path = downloaded_dir / filename
                if pdf_path.exists() and pdf_path.suffix.lower() == ".pdf":
                    to_delete.append((identifier, pdf_path))

    print(f"\nFound {len(to_delete)} PDFs to delete")

    if not to_delete:
        print("Nothing to delete!")
        return 0

    # Show what will be deleted
    print("\nPDFs to delete:")
    for i, (identifier, pdf_path) in enumerate(to_delete[:10], 1):
        size_mb = pdf_path.stat().st_size / (1024 * 1024)
        print(f"  {i}. {pdf_path.name} ({size_mb:.1f} MB) [{identifier}]")

    if len(to_delete) > 10:
        print(f"  ... and {len(to_delete) - 10} more")

    # Calculate total size
    total_size = sum(p.stat().st_size for _, p in to_delete)
    total_size_mb = total_size / (1024 * 1024)
    total_size_gb = total_size / (1024 * 1024 * 1024)

    print(f"\nTotal size: {total_size_gb:.2f} GB ({total_size_mb:.1f} MB)")

    if args.dry_run:
        print("\n[DRY RUN] No files deleted")
        return 0

    # Delete files
    print("\nDeleting...")
    deleted = 0
    failed = 0

    for identifier, pdf_path in to_delete:
        try:
            pdf_path.unlink()
            deleted += 1
            if deleted % 50 == 0:
                print(f"  Deleted {deleted}/{len(to_delete)}...")
        except Exception as e:
            print(f"  ✗ Failed to delete {pdf_path.name}: {e}")
            failed += 1

    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Deleted: {deleted}")
    print(f"Failed: {failed}")
    print(f"Space freed: {total_size_gb:.2f} GB")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    exit(main())
