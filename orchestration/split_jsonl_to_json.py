#!/usr/bin/env python3
"""
Split olmOCR JSONL files into individual JSON files per PDF.

This processes files from results/results/*.jsonl and creates individual
JSON files in results/json/<pdf_name>.json for easier ingestion.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


PDF_SOURCE_KEYS = (
    "Source-File",
    "source_file",
    "source",
    "filename",
    "file_name",
    "path",
    "filepath",
    "pdf",
    "pdf_name",
    "document",
    "document_name",
)


def _safe_parse_metadata(md: Any) -> Optional[Dict[str, Any]]:
    """Return metadata as dict when possible (handles stringified JSON)."""
    if md is None:
        return None
    if isinstance(md, dict):
        return md
    if isinstance(md, str):
        try:
            parsed = json.loads(md)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def _extract_source_file(obj: Dict[str, Any]) -> Optional[str]:
    """Try to extract a source PDF path/filename from an object.

    Looks into both top-level and metadata fields using a broad set of keys.
    """
    md = _safe_parse_metadata(obj.get("metadata")) or {}
    # Prefer metadata keys
    for k in PDF_SOURCE_KEYS:
        v = md.get(k)
        if isinstance(v, str) and v:
            return v
    # Fallback to top-level keys
    for k in PDF_SOURCE_KEYS:
        v = obj.get(k)
        if isinstance(v, str) and v:
            return v
    return None


def _iter_records(obj: Any, inherited_source: Optional[str] = None) -> Iterable[Tuple[Dict[str, Any], str]]:
    """Recursively traverse nested JSON, yielding (record, source_file) pairs.

    - Propagates nearest-known source file down to children.
    - Emits dicts that have a known or inherited source and contain more than just metadata.
    """
    if isinstance(obj, dict):
        current_source = _extract_source_file(obj) or inherited_source

        # Consider this dict a record if it has a source and more than only metadata
        is_record = current_source is not None and (
            any(k for k in obj.keys() if k != "metadata")
        )
        if is_record:
            yield obj, current_source

        for v in obj.values():
            yield from _iter_records(v, current_source)

    elif isinstance(obj, list):
        for v in obj:
            yield from _iter_records(v, inherited_source)


def parse_jsonl_file(jsonl_file: Path) -> Tuple[Dict[str, List[dict]], List[Tuple[int, str]]]:
    """Parse a JSONL file and group records by PDF filename.

    Handles nested JSON by recursively extracting sub-records that inherit
    metadata.Source-File (or equivalent) from ancestors.
    """
    grouped: Dict[str, List[dict]] = defaultdict(list)
    issues: List[Tuple[int, str]] = []

    with jsonl_file.open('r', encoding='utf-8') as f:
        for line_no, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                issues.append((line_no, f"JSON decode error: {e}"))
                continue

            found_any = False
            for record, source_file in _iter_records(obj):
                found_any = True
                pdf_filename = Path(source_file).name
                grouped[pdf_filename].append(record)

            if not found_any:
                issues.append((line_no, "No records with a source file found in line"))

    return dict(grouped), issues


def split_jsonl_files(pdf_dir: Path, dry_run: bool = False):
    """
    Split JSONL files from results/results/ into individual JSON files in results/json/.

    Args:
        pdf_dir: Base PDF directory containing results/results/
        dry_run: If True, only show what would be done
    """
    # Determine JSONL directory (OLMoCR may nest as results/results)
    candidates = [
        pdf_dir / "results" / "results",
        pdf_dir / "results",
    ]
    jsonl_dir = next((c for c in candidates if c.exists()), candidates[0])
    json_output_dir = pdf_dir / "results" / "json"

    if not jsonl_dir.exists():
        print(f"Error: JSONL directory not found: {jsonl_dir}")
        return False

    # Search recursively for JSONL files
    jsonl_files = list(jsonl_dir.rglob("*.jsonl"))

    if not jsonl_files:
        print(f"No JSONL files found in {jsonl_dir}")
        return False

    print("=" * 70)
    print("Split JSONL to Individual JSON Files")
    print("=" * 70)
    print(f"Input directory:  {jsonl_dir}")
    print(f"Output directory: {json_output_dir}")
    print(f"JSONL files found: {len(jsonl_files)}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("-" * 70)

    all_grouped = {}
    total_issues = 0

    for jsonl_file in jsonl_files:
        print(f"\nProcessing: {jsonl_file.name}")
        grouped, issues = parse_jsonl_file(jsonl_file)

        print(f"  Records: {sum(len(records) for records in grouped.values())}")
        print(f"  PDFs: {len(grouped)}")

        if issues:
            print(f"  Issues: {len(issues)}")
            for line_no, msg in issues[:3]:
                print(f"    Line {line_no}: {msg}")
            if len(issues) > 3:
                print(f"    ... and {len(issues) - 3} more")

        # Merge results
        for pdf_name, records in grouped.items():
            if pdf_name in all_grouped:
                print(f"  ⚠ Warning: {pdf_name} already seen, appending records")
                all_grouped[pdf_name].extend(records)
            else:
                all_grouped[pdf_name] = records

        total_issues += len(issues)

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"JSONL files processed: {len(jsonl_files)}")
    print(f"Unique PDFs found: {len(all_grouped)}")
    print(f"Total issues: {total_issues}")

    if not all_grouped:
        print("\n⚠ No records could be extracted")
        return False

    # Create output directory
    if not dry_run:
        json_output_dir.mkdir(parents=True, exist_ok=True)

    # Save individual JSON files
    print(f"\n{'Would save' if dry_run else 'Saving'} JSON files:")
    print("-" * 70)

    saved_count = 0
    for pdf_filename, records in sorted(all_grouped.items()):
        json_filename = pdf_filename.replace('.pdf', '.json')
        json_path = json_output_dir / json_filename

        if not dry_run:
            with json_path.open('w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)

        print(f"  {'[DRY RUN] ' if dry_run else '✓ '}{json_filename}: {len(records)} records")
        saved_count += 1

    print("\n" + "=" * 70)
    if dry_run:
        print(f"DRY RUN: Would create {saved_count} JSON files")
    else:
        print(f"✓ Successfully created {saved_count} JSON files in {json_output_dir}")
    print("=" * 70)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Split olmOCR JSONL files into individual JSON files per PDF"
    )
    parser.add_argument(
        "pdf_dir",
        type=Path,
        help="Base PDF directory containing results/results/"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without creating files"
    )

    args = parser.parse_args()

    if not args.pdf_dir.exists():
        print(f"Error: PDF directory not found: {args.pdf_dir}")
        return 1

    success = split_jsonl_files(args.pdf_dir, args.dry_run)
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
