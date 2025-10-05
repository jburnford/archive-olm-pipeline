#!/usr/bin/env python3
"""
Test script to parse JSONL files and extract individual JSON files per PDF.

This will help debug the parsing logic before running on NIBI.
"""

import json
from collections import defaultdict
from pathlib import Path


def parse_jsonl_file(jsonl_file: Path):
    """Parse a JSONL file and group records by PDF filename."""
    grouped = defaultdict(list)
    issues = []

    print(f"\nProcessing: {jsonl_file.name}")
    print("-" * 70)

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
            # Check multiple possible locations
            metadata = record.get('metadata', {})
            source_file = (
                metadata.get('Source-File') or
                metadata.get('source_file') or
                record.get('Source-File') or
                record.get('source_file') or
                record.get('source')
            )

            if not source_file:
                issues.append((line_no, f"No source file found. Keys: {list(record.keys())}, Metadata keys: {list(metadata.keys())}"))
                continue

            # Extract just the filename from the full path
            pdf_filename = Path(source_file).name

            if line_no == 1:
                print(f"  First record source: {source_file}")
                print(f"  Extracted filename: {pdf_filename}")

            grouped[pdf_filename].append(record)

    print(f"\n  Total records: {sum(len(records) for records in grouped.values())}")
    print(f"  Unique PDFs: {len(grouped)}")
    print(f"  Issues: {len(issues)}")

    if issues:
        print("\n  Issues found:")
        for line_no, msg in issues[:5]:  # Show first 5
            print(f"    Line {line_no}: {msg}")
        if len(issues) > 5:
            print(f"    ... and {len(issues) - 5} more")

    return dict(grouped), issues


def save_json_files(grouped_records: dict, output_dir: Path):
    """Save grouped records as individual JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nSaving JSON files to: {output_dir}")
    print("-" * 70)

    for pdf_filename, records in grouped_records.items():
        # Create output filename: same as PDF but with .json extension
        json_filename = pdf_filename.replace('.pdf', '.json')
        json_path = output_dir / json_filename

        # Save records as JSON array
        with json_path.open('w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        print(f"  ✓ {json_filename}: {len(records)} records")


def main():
    # Test with local files
    test_dir = Path("/Users/jimclifford/Library/CloudStorage/GoogleDrive-cljim22@gmail.com/My Drive/archive-olm-pipeline")
    output_dir = test_dir / "test_json_output"

    jsonl_files = list(test_dir.glob("output_*.jsonl"))

    if not jsonl_files:
        print("No JSONL files found in current directory")
        return

    print("=" * 70)
    print("JSONL Parser Test")
    print("=" * 70)
    print(f"Found {len(jsonl_files)} JSONL files")

    all_grouped = {}
    total_issues = 0

    for jsonl_file in jsonl_files:
        grouped, issues = parse_jsonl_file(jsonl_file)

        # Merge results
        for pdf_name, records in grouped.items():
            if pdf_name in all_grouped:
                print(f"  ⚠ Warning: {pdf_name} found in multiple JSONL files")
            all_grouped[pdf_name] = records

        total_issues += len(issues)

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Total JSONL files processed: {len(jsonl_files)}")
    print(f"Total unique PDFs found: {len(all_grouped)}")
    print(f"Total issues: {total_issues}")

    if all_grouped:
        print(f"\nPDF files found:")
        for pdf_name in sorted(all_grouped.keys())[:10]:
            print(f"  - {pdf_name}: {len(all_grouped[pdf_name])} records")
        if len(all_grouped) > 10:
            print(f"  ... and {len(all_grouped) - 10} more")

        # Save individual JSON files
        save_json_files(all_grouped, output_dir)

        print("\n" + "=" * 70)
        print(f"✓ Successfully created {len(all_grouped)} JSON files in {output_dir}")
        print("=" * 70)
    else:
        print("\n⚠ No records could be extracted")


if __name__ == "__main__":
    main()
