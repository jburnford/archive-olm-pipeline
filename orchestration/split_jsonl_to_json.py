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
from typing import Dict, List, Tuple


def parse_jsonl_file(jsonl_file: Path) -> Tuple[Dict[str, List[dict]], List[Tuple[int, str]]]:
    """Parse a JSONL file and group records by PDF filename."""
    grouped = defaultdict(list)
    issues = []

    with jsonl_file.open('r', encoding='utf-8') as f:
        for line_no, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                issues.append((line_no, f"JSON decode error: {e}"))
                continue

            # Try to find the source PDF filename
            metadata = record.get('metadata', {})
            source_file = (
                metadata.get('Source-File') or
                metadata.get('source_file') or
                record.get('Source-File') or
                record.get('source_file') or
                record.get('source')
            )

            if not source_file:
                issues.append((line_no, f"No source file found"))
                continue

            # Extract just the filename from the full path
            pdf_filename = Path(source_file).name
            grouped[pdf_filename].append(record)

    return dict(grouped), issues


def split_jsonl_files(pdf_dir: Path, dry_run: bool = False):
    """
    Split JSONL files from results/results/ into individual JSON files in results/json/.

    Args:
        pdf_dir: Base PDF directory containing results/results/
        dry_run: If True, only show what would be done
    """
    jsonl_dir = pdf_dir / "results" / "results"
    json_output_dir = pdf_dir / "results" / "json"

    if not jsonl_dir.exists():
        print(f"Error: JSONL directory not found: {jsonl_dir}")
        return False

    jsonl_files = list(jsonl_dir.glob("*.jsonl"))

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
