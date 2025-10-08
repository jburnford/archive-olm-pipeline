#!/usr/bin/env python3
"""
Convert search results CSV to identifiers.json format for pipeline input.

This allows using pre-saved search results from Internet Archive
instead of re-running searches.
"""

import argparse
import csv
import json
import sys
from pathlib import Path


def csv_to_identifiers(csv_path: Path, output_path: Path = None) -> dict:
    """
    Convert CSV with 'identifier' column to identifiers.json format.

    Args:
        csv_path: Path to CSV file with search results
        output_path: Optional path for output JSON (defaults to same dir as CSV)

    Returns:
        Dictionary with identifiers data
    """
    if not csv_path.exists():
        print(f"ERROR: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    # Read identifiers from CSV
    identifiers = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # Check for identifier column
        if 'identifier' not in reader.fieldnames:
            print(f"ERROR: CSV must have 'identifier' column", file=sys.stderr)
            print(f"Found columns: {reader.fieldnames}", file=sys.stderr)
            sys.exit(1)

        for row in reader:
            identifier = row['identifier'].strip()
            if identifier:
                identifiers.append(identifier)

    if not identifiers:
        print(f"ERROR: No identifiers found in CSV", file=sys.stderr)
        sys.exit(1)

    # Create identifiers.json structure
    data = {
        "query": f"CSV import from {csv_path.name}",
        "sort_order": "CSV order",
        "total_count": len(identifiers),
        "identifiers": identifiers
    }

    # Determine output path
    if output_path is None:
        output_path = csv_path.parent / "identifiers.json"

    # Write JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

    print(f"✓ Converted {len(identifiers)} identifiers from CSV")
    print(f"  Input:  {csv_path}")
    print(f"  Output: {output_path}")

    return data


def main():
    parser = argparse.ArgumentParser(
        description="Convert search results CSV to identifiers.json format"
    )
    parser.add_argument(
        "csv_file",
        type=Path,
        help="Path to CSV file with 'identifier' column"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output path for identifiers.json (default: same dir as CSV)"
    )

    args = parser.parse_args()

    csv_to_identifiers(args.csv_file, args.output)


if __name__ == "__main__":
    main()
