#!/usr/bin/env python3
"""
Simplified Finalizer

Moves completed OCR results from 01_downloaded/results/json/ to
02_processed/{identifier}/, updates tracking, and deletes original PDFs.

Usage:
  python3 streaming/simplified_finalizer.py \
    --base-dir /path/to/caribbean_pipeline \
    [--auto-delete-pdfs]

Features:
  - Matches JSON files to metadata by filename
  - Creates consolidated metadata in 02_processed
  - Tracks completion in _manifests/processed_pdfs.json
  - Optionally deletes PDFs after successful finalization
"""

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set


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

    def _save(self):
        """Save processed identifiers to manifest."""
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            'processed': sorted(list(self.processed)),
            'count': len(self.processed),
            'last_updated': datetime.utcnow().isoformat() + 'Z'
        }

        with open(self.manifest_path, 'w') as f:
            json.dump(data, f, indent=2)

    def mark_processed(self, identifier: str):
        """Mark identifier as processed and save."""
        self.processed.add(identifier)
        self._save()

    def is_processed(self, identifier: str) -> bool:
        """Check if identifier has been processed."""
        return identifier in self.processed


def load_metadata_index(downloaded_dir: Path) -> Dict[str, Dict]:
    """
    Create index of metadata files.

    Returns dict mapping:
      - PDF filename -> metadata
      - identifier -> metadata
    """
    index = {}

    for meta_file in downloaded_dir.glob("*.meta.json"):
        try:
            with open(meta_file) as f:
                meta = json.load(f)

            identifier = meta.get("identifier")
            filename = meta.get("filename")

            if identifier:
                meta["__meta_file"] = str(meta_file)
                index[identifier] = meta

                if filename:
                    index[filename] = meta

        except Exception as e:
            print(f"⚠️  Error loading {meta_file.name}: {e}")

    return index


def finalize_one(
    json_file: Path,
    meta_index: Dict[str, Dict],
    downloaded_dir: Path,
    processed_dir: Path,
    auto_delete_pdfs: bool = False
) -> Optional[str]:
    """
    Finalize one OCR result.

    Returns identifier if successful, None otherwise.
    """
    # Try to match metadata by PDF filename
    pdf_filename = json_file.stem + ".pdf"
    meta = meta_index.get(pdf_filename)

    if not meta:
        # Try by identifier
        meta = meta_index.get(json_file.stem)

    if not meta:
        return None

    identifier = meta.get("identifier")
    if not identifier:
        return None

    # Create processed directory
    dest_dir = processed_dir / identifier
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Copy OCR JSON
    dest_json = dest_dir / "olmocr_results.json"
    shutil.copy2(json_file, dest_json)

    # Look for markdown file (OLMoCR creates these in 01_downloaded/)
    md_file = downloaded_dir / f"{json_file.stem}.md"
    dest_md = None

    if md_file.exists():
        dest_md = dest_dir / "olmocr_results.md"
        shutil.copy2(md_file, dest_md)

    # Create consolidated metadata
    merged = dict(meta)
    merged["ocr_json"] = "olmocr_results.json"

    if dest_md:
        merged["ocr_markdown"] = "olmocr_results.md"

    merged["ocr_consolidated_at"] = datetime.utcnow().isoformat() + "Z"
    merged["original_filename"] = pdf_filename

    # Remove internal tracking fields
    merged.pop("__meta_file", None)

    # Write metadata
    dest_meta = dest_dir / "metadata.json"
    with open(dest_meta, 'w') as f:
        json.dump(merged, f, indent=2)

    # Verify all required files are present
    has_meta = dest_meta.exists()
    has_json = dest_json.exists()

    # Consider successful if meta + json exist
    # (markdown is optional - olmOCR may not generate it)
    if has_meta and has_json:
        # Delete original PDF and markdown if requested
        if auto_delete_pdfs:
            pdf_path = downloaded_dir / pdf_filename
            md_source = downloaded_dir / f"{json_file.stem}.md"

            if pdf_path.exists():
                try:
                    pdf_path.unlink()
                except Exception as e:
                    print(f"  ⚠️  Could not delete {pdf_filename}: {e}")

            if md_source.exists():
                try:
                    md_source.unlink()
                except Exception as e:
                    print(f"  ⚠️  Could not delete {md_source.name}: {e}")

        return identifier

    return None


def main():
    parser = argparse.ArgumentParser(
        description="Finalize OCR results and track completion"
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        required=True,
        help="Pipeline base directory"
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        help="Results JSON directory (default: 01_downloaded/results/json)"
    )
    parser.add_argument(
        "--auto-delete-pdfs",
        action="store_true",
        help="Automatically delete PDFs after successful finalization"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done"
    )

    args = parser.parse_args()

    base_dir = args.base_dir
    downloaded_dir = base_dir / "01_downloaded"
    processed_dir = base_dir / "02_processed"

    # Use provided results directory or default
    if args.results_dir:
        results_json_dir = args.results_dir
    else:
        results_json_dir = downloaded_dir / "results" / "json"

    print("=" * 70)
    print("Simplified Finalizer")
    print("=" * 70)
    print(f"Base: {base_dir}")
    print(f"Results: {results_json_dir}")
    print(f"Output: {processed_dir}")
    print(f"Auto-delete PDFs: {args.auto_delete_pdfs}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("-" * 70)

    # Load tracker
    tracker_path = base_dir / "_manifests" / "processed_pdfs.json"
    tracker = ProcessedPDFTracker(tracker_path)

    # Load metadata
    meta_index = load_metadata_index(downloaded_dir)
    print(f"Metadata entries: {len(meta_index)}")

    # Find JSON files
    if not results_json_dir.exists():
        print("\n⏳ No results directory yet")
        print("   Waiting for OLMoCR to complete...")
        return 0

    json_files = sorted(results_json_dir.glob("*.json"))
    print(f"OCR JSON files: {len(json_files)}")
    print("-" * 70)

    if not json_files:
        print("\n⏳ No JSON files to process")
        return 0

    # Process files
    finalized = 0
    already_processed = 0
    missing_meta = 0
    deleted_pdfs = 0

    for json_file in json_files:
        # Check if already processed
        identifier = json_file.stem
        if tracker.is_processed(identifier):
            already_processed += 1
            continue

        if args.dry_run:
            # In dry run, just check if we can match metadata
            pdf_filename = json_file.stem + ".pdf"
            meta = meta_index.get(pdf_filename) or meta_index.get(json_file.stem)

            if meta:
                print(f"  [DRY RUN] Would finalize: {json_file.name}")
                finalized += 1
            else:
                print(f"  [DRY RUN] No metadata: {json_file.name}")
                missing_meta += 1
            continue

        # Actually finalize
        result_id = finalize_one(
            json_file,
            meta_index,
            downloaded_dir,
            processed_dir,
            args.auto_delete_pdfs
        )

        if result_id is None:
            missing_meta += 1
            print(f"  ⚠️  No metadata for {json_file.name}")
            continue

        # Mark as processed
        tracker.mark_processed(result_id)
        finalized += 1

        if args.auto_delete_pdfs:
            deleted_pdfs += 1
            print(f"  ✅ {result_id} (PDF deleted)")
        else:
            print(f"  ✅ {result_id}")

    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Finalized: {finalized}")
    print(f"Already processed: {already_processed}")
    print(f"Missing metadata: {missing_meta}")

    if args.auto_delete_pdfs and not args.dry_run:
        print(f"PDFs deleted: {deleted_pdfs}")

    print(f"Total processed: {tracker.count()}")
    print(f"Output: {processed_dir}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    exit(main())
