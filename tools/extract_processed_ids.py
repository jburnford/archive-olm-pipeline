#!/usr/bin/env python3
"""
Extract list of processed identifiers from 02_processed directory.

This creates the initial processed_pdfs.json tracking file from
existing completed items.

Usage:
  python3 tools/extract_processed_ids.py \
    --processed-dir /path/to/02_processed \
    --output _manifests/processed_pdfs.json
"""

import argparse
import json
from datetime import datetime
from pathlib import Path


def extract_processed_identifiers(processed_dir: Path) -> list:
    """Scan processed directory and extract all identifiers."""
    identifiers = []

    if not processed_dir.exists():
        print(f"Warning: {processed_dir} does not exist")
        return identifiers

    for item_dir in sorted(processed_dir.iterdir()):
        if item_dir.is_dir():
            identifiers.append(item_dir.name)

    return identifiers


def main():
    parser = argparse.ArgumentParser(
        description="Extract processed identifiers for tracking"
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        required=True,
        help="Path to 02_processed directory"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output path for processed_pdfs.json"
    )

    args = parser.parse_args()

    print("=" * 70)
    print("Extract Processed Identifiers")
    print("=" * 70)
    print(f"Scanning: {args.processed_dir}")

    identifiers = extract_processed_identifiers(args.processed_dir)

    print(f"Found: {len(identifiers)} processed items")

    # Create output
    output_data = {
        "processed": sorted(identifiers),
        "count": len(identifiers),
        "extracted_at": datetime.utcnow().isoformat() + "Z"
    }

    # Save to file
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, 'w') as f:
        json.dump(output_data, f, indent=2)

    print(f"\n✅ Saved to: {args.output}")
    print("=" * 70)

    # Show first few
    if identifiers:
        print("\nFirst 10 identifiers:")
        for i, ident in enumerate(identifiers[:10], 1):
            print(f"  {i}. {ident}")
        if len(identifiers) > 10:
            print(f"  ... and {len(identifiers) - 10} more")


if __name__ == "__main__":
    main()
