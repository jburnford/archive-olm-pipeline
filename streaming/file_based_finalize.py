#!/usr/bin/env python3
"""
File-Based Finalizer

Links OCR JSON outputs with original download metadata, moves them
to a safe location (05_processed), and deletes original PDFs to free space.

Usage:
  python3 streaming/file_based_finalize.py --base-dir /path/to/caribbean_pipeline

Behavior:
  - Scans 03_ocr_processing/batch_*/results/json/*.json
  - Matches each JSON to 01_downloaded/* .meta.json by filename
  - Creates 05_processed/<identifier>/ with:
      - <pdf_basename>.ocr.json  (copied from results)
      - <identifier>.meta.json   (original meta + OCR pointers)
  - Deletes original PDFs in 01_downloaded/ after successful consolidation
"""

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional


def load_download_metadata(downloaded_dir: Path) -> Dict[str, Dict]:
    """Index download metadata by original PDF filename.

    Returns mapping: pdf_filename -> metadata dict with path to meta file.
    """
    index: Dict[str, Dict] = {}
    for meta_path in downloaded_dir.glob("*.meta.json"):
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
            filename = data.get("filename")
            if isinstance(filename, str) and filename:
                data["__meta_path"] = str(meta_path)
                index[filename] = data
        except Exception:
            continue
    return index


def consolidate_one(json_file: Path, meta_index: Dict[str, Dict], processed_dir: Path) -> Optional[Path]:
    """Write consolidated output to 05_processed and return destination path.

    Returns the path to the copied JSON file on success, None on failure.
    """
    # Derive original PDF filename from OCR JSON name
    pdf_filename = json_file.name.replace(".json", ".pdf")
    meta = meta_index.get(pdf_filename)
    if not meta:
        return None

    identifier = meta.get("identifier") or json_file.stem
    dest_dir = processed_dir / identifier
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Copy OCR JSON
    dest_json = dest_dir / f"{json_file.stem}.ocr.json"
    shutil.copy2(json_file, dest_json)

    # Also copy corresponding markdown, if present next to PDFs in batch dir
    # json_file path is .../batch_X/results/json/<pdf_stem>.json
    # batch_dir is two parents up from results/json
    try:
        batch_dir = json_file.parent.parent.parent
        pdf_stem = json_file.stem
        md_candidates = [
            batch_dir / f"{pdf_stem}.md",
            batch_dir / "results" / f"{pdf_stem}.md",
            batch_dir / "results" / "results" / f"{pdf_stem}.md",
        ]
        dest_md = None
        for md in md_candidates:
            if md.exists():
                dest_md = dest_dir / f"{pdf_stem}.md"
                shutil.copy2(md, dest_md)
                break
    except Exception:
        dest_md = None

    # Write merged metadata
    merged = dict(meta)
    merged["ocr_json"] = str(dest_json)
    if dest_md:
        merged["ocr_markdown"] = str(dest_md)
    merged["ocr_consolidated_at"] = datetime.utcnow().isoformat() + "Z"
    merged["original_filename"] = pdf_filename
    merged["source_pdf"] = meta.get("filepath")

    # Provide relative path to batch output for traceability
    merged["batch_ocr_source"] = str(json_file)

    # Remove helper path from saved metadata
    merged.pop("__meta_path", None)

    dest_meta = dest_dir / f"{identifier}.meta.json"
    dest_meta.write_text(json.dumps(merged, indent=2), encoding="utf-8")

    return dest_json


def delete_original_pdf(downloaded_dir: Path, meta: Dict):
    """Delete original PDF in 01_downloaded when safe to remove."""
    # Preferred explicit filepath if present
    pdf_path = meta.get("filepath")
    if pdf_path:
        p = Path(pdf_path)
        if p.exists() and p.suffix.lower() == ".pdf":
            try:
                p.unlink()
            except Exception:
                pass
        return

    # Fallback to identifier + filename heuristic
    filename = meta.get("filename")
    if filename:
        p = downloaded_dir / filename
        if p.exists() and p.suffix.lower() == ".pdf":
            try:
                p.unlink()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="Finalize OCR outputs and cleanup PDFs")
    parser.add_argument("--base-dir", type=Path, required=True, help="Pipeline base directory")
    args = parser.parse_args()

    base = args.base_dir
    downloaded = base / "01_downloaded"
    ocr_batches = base / "03_ocr_processing"
    processed = base / "05_processed"

    print("=" * 70)
    print("File-Based Finalizer")
    print("=" * 70)
    print(f"Base: {base}")

    # Build metadata index by original filename
    meta_index = load_download_metadata(downloaded)
    print(f"Indexed {len(meta_index)} download metadata files")

    # Scan batch OCR JSON outputs
    json_files = list(ocr_batches.glob("batch_*/results/json/*.json"))
    print(f"Found {len(json_files)} OCR JSON files to consolidate")

    consolidated = 0
    deleted = 0
    missing_meta = 0

    for jf in sorted(json_files):
        dest = consolidate_one(jf, meta_index, processed)
        if dest is None:
            missing_meta += 1
            print(f"  ⚠ No download metadata for {jf.name}")
            continue

        # After successful consolidation, delete original PDF
        pdf_name = jf.name.replace('.json', '.pdf')
        meta = meta_index.get(pdf_name)
        if meta:
            delete_original_pdf(downloaded, meta)
            deleted += 1

        consolidated += 1

    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Consolidated JSONs: {consolidated}")
    print(f"Original PDFs deleted: {deleted}")
    print(f"Missing metadata: {missing_meta}")
    print(f"Output: {processed}")


if __name__ == "__main__":
    main()
