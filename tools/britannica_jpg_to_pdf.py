#!/usr/bin/env python3
"""
Convert NLS Britannica JPG images to PDFs for OLMoCR processing.

Reads the inventory CSV, filters to specified editions, and creates
one PDF per volume from the sorted JPG images.

Usage:
  python3 tools/britannica_jpg_to_pdf.py \
    --nls-dir /path/to/nls-data-encyclopaediaBritannica \
    --output-dir /path/to/britannica_pipeline/01_downloaded \
    --editions EB.4 EB.7 EB.11 EB.12 EB.15 EB.16 \
    [--dry-run]

Requirements:
  - img2pdf (pip install img2pdf) - preferred, fast, no re-encoding
  - OR ImageMagick convert as fallback
"""

import argparse
import csv
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Set

# Try to import img2pdf for efficient conversion
try:
    import img2pdf
    HAS_IMG2PDF = True
except ImportError:
    HAS_IMG2PDF = False


def parse_inventory(inventory_path: Path) -> List[Dict]:
    """Parse the NLS inventory CSV file."""
    volumes = []

    with open(inventory_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                volume_id = row[0].strip()
                description = row[1].strip()

                # Extract EB code from description
                eb_match = re.search(r'EB\.(\d+)', description)
                eb_code = f"EB.{eb_match.group(1)}" if eb_match else None

                # Extract edition info
                edition_match = re.search(r'(First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth) edition', description)
                edition = edition_match.group(1) if edition_match else None

                # Extract volume number
                vol_match = re.search(r'Volume (\d+)', description)
                volume_num = int(vol_match.group(1)) if vol_match else None

                # Extract year if present
                year_match = re.search(r', (\d{4}),', description)
                year = year_match.group(1) if year_match else None

                volumes.append({
                    'volume_id': volume_id,
                    'description': description,
                    'eb_code': eb_code,
                    'edition': edition,
                    'volume_num': volume_num,
                    'year': year
                })

    return volumes


def get_sorted_images(image_dir: Path) -> List[Path]:
    """Get JPG images sorted by their numeric ID."""
    images = list(image_dir.glob('*.jpg'))

    # Sort by the numeric part of the filename
    def extract_num(p: Path) -> int:
        match = re.search(r'(\d+)', p.stem)
        return int(match.group(1)) if match else 0

    return sorted(images, key=extract_num)


def convert_with_img2pdf(images: List[Path], output_pdf: Path) -> bool:
    """Convert images to PDF using img2pdf (fast, no re-encoding)."""
    try:
        with open(output_pdf, 'wb') as f:
            f.write(img2pdf.convert([str(img) for img in images]))
        return True
    except Exception as e:
        print(f"  img2pdf error: {e}")
        return False


def convert_with_imagemagick(images: List[Path], output_pdf: Path) -> bool:
    """Convert images to PDF using ImageMagick convert (fallback)."""
    try:
        # ImageMagick can handle many images but may need memory limits
        cmd = ['convert'] + [str(img) for img in images] + [str(output_pdf)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"  ImageMagick timeout")
        return False
    except Exception as e:
        print(f"  ImageMagick error: {e}")
        return False


def create_metadata(volume: Dict, pdf_path: Path, page_count: int) -> Dict:
    """Create metadata JSON for a volume."""
    # Create a clean identifier from volume_id
    identifier = f"britannica_nls_{volume['volume_id']}"

    return {
        "identifier": identifier,
        "collection": "britannica_nls",
        "title": volume['description'],
        "downloaded_at": datetime.utcnow().isoformat() + "Z",
        "filename": pdf_path.name,
        "file_path": f"01_downloaded/{pdf_path.name}",
        "source": "nls_foundry",
        "nls_volume_id": volume['volume_id'],
        "eb_code": volume['eb_code'],
        "edition": volume['edition'],
        "volume_num": volume['volume_num'],
        "year": volume['year'],
        "page_count": page_count
    }


def main():
    parser = argparse.ArgumentParser(
        description="Convert NLS Britannica JPGs to PDFs for OLMoCR"
    )
    parser.add_argument(
        "--nls-dir",
        type=Path,
        required=True,
        help="Path to nls-data-encyclopaediaBritannica directory"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for PDFs (e.g., britannica_pipeline/01_downloaded)"
    )
    parser.add_argument(
        "--editions",
        nargs='+',
        default=['EB.4', 'EB.7', 'EB.11', 'EB.12', 'EB.15', 'EB.16'],
        help="Edition codes to process (default: EB.4 EB.7 EB.11 EB.12 EB.15 EB.16)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without creating files"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing PDFs"
    )

    args = parser.parse_args()

    # Validate paths
    nls_dir = args.nls_dir.resolve()
    if not nls_dir.exists():
        print(f"ERROR: NLS directory not found: {nls_dir}")
        return 1

    inventory_path = nls_dir / "encyclopaediaBritannica-inventory.csv"
    if not inventory_path.exists():
        print(f"ERROR: Inventory CSV not found: {inventory_path}")
        return 1

    # Create output directory
    output_dir = args.output_dir.resolve()
    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    # Check conversion method
    if HAS_IMG2PDF:
        print("Using img2pdf for conversion (fast, no re-encoding)")
        convert_func = convert_with_img2pdf
    else:
        print("img2pdf not available, using ImageMagick (slower)")
        convert_func = convert_with_imagemagick

    # Parse inventory
    print(f"\nParsing inventory: {inventory_path}")
    volumes = parse_inventory(inventory_path)
    print(f"Found {len(volumes)} total volumes")

    # Filter to requested editions
    target_editions = set(args.editions)
    filtered = [v for v in volumes if v['eb_code'] in target_editions]
    print(f"Filtered to {len(filtered)} volumes for editions: {', '.join(sorted(target_editions))}")

    # Process each volume
    print("\n" + "=" * 70)
    print("Processing Volumes")
    print("=" * 70)

    success = 0
    skipped = 0
    failed = 0
    total_pages = 0

    for i, volume in enumerate(filtered, 1):
        volume_id = volume['volume_id']
        eb_code = volume['eb_code']

        # Check if image directory exists
        image_dir = nls_dir / volume_id / "image"
        if not image_dir.exists():
            print(f"[{i}/{len(filtered)}] {volume_id} - SKIP (no image dir)")
            skipped += 1
            continue

        # Get sorted images
        images = get_sorted_images(image_dir)
        if not images:
            print(f"[{i}/{len(filtered)}] {volume_id} - SKIP (no images)")
            skipped += 1
            continue

        # Output paths
        pdf_name = f"britannica_nls_{volume_id}.pdf"
        pdf_path = output_dir / pdf_name
        meta_path = output_dir / f"britannica_nls_{volume_id}.meta.json"

        # Check if already exists
        if pdf_path.exists() and not args.force:
            print(f"[{i}/{len(filtered)}] {volume_id} - EXISTS ({len(images)} pages)")
            skipped += 1
            total_pages += len(images)
            continue

        print(f"[{i}/{len(filtered)}] {volume_id} ({eb_code}) - {len(images)} pages...", end=" ", flush=True)

        if args.dry_run:
            print("DRY RUN")
            success += 1
            total_pages += len(images)
            continue

        # Convert to PDF
        if convert_func(images, pdf_path):
            # Create metadata
            meta = create_metadata(volume, pdf_path, len(images))
            meta_path.write_text(json.dumps(meta, indent=2), encoding='utf-8')

            print(f"OK ({pdf_path.stat().st_size / 1024 / 1024:.1f} MB)")
            success += 1
            total_pages += len(images)
        else:
            print("FAILED")
            failed += 1

    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Success: {success}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"Total pages: {total_pages:,}")
    print(f"Output directory: {output_dir}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
