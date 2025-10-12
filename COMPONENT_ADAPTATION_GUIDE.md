# Component Adaptation Guide

## Migration Strategy

**Good News:** We can reuse existing `01_downloaded/` and just rename `05_processed/` to `02_processed/`!

### Quick Migration
```bash
cd /home/jic823/projects/def-jic823/caribbean_pipeline

# Rename processed directory
mv 05_processed 02_processed

# Remove intermediate directories (after backing up if needed)
rm -rf 02_ocr_pending 03_ocr_processing 04_ocr_completed

# Keep: 01_downloaded, 02_processed, _manifests, 99_errors
```

## Component-by-Component Changes

### 1. Downloader: `streaming/file_based_downloader.py`

**Current behavior:**
- Downloads to `01_downloaded/{filename}.pdf`
- Creates metadata `01_downloaded/{identifier}.meta.json`
- Creates symlink in `02_ocr_pending/`

**Required changes:**
```python
# REMOVE lines 49-50 (ocr_pending_dir creation)
# REMOVE lines 192-195 (symlink creation in download_pdf)
# REMOVE lines 246-248 (symlink creation in download_pdf)
```

**Why:** No longer need `02_ocr_pending/` directory or symlinks.

**Modified code snippet:**
```python
# OLD:
self.ocr_pending_dir = base_dir / "02_ocr_pending"
for d in [self.downloaded_dir, self.ocr_pending_dir, self.errors_dir, ...]:
    d.mkdir(parents=True, exist_ok=True)

# Create symlink in 02_ocr_pending
pending_link = self.ocr_pending_dir / filename
if not pending_link.exists():
    pending_link.symlink_to(output_path)

# NEW:
# Just remove these lines - no symlinks needed
```

---

### 2. Batch Submitter: NEW `streaming/simple_batch_submitter.py`

**Purpose:** Submit batches directly from `01_downloaded/` to OLMoCR

**Key features:**
- Scans `01_downloaded/*.pdf`
- Filters using `_manifests/processed_pdfs.json`
- When 200+ unprocessed PDFs, submit batch
- OLMoCR processes in place, outputs to `01_downloaded/results/`

**Implementation:**
```python
#!/usr/bin/env python3
"""
Simple Batch Submitter - Submit unprocessed PDFs to OLMoCR

Scans 01_downloaded/ for PDFs, checks processed_pdfs.json,
and submits batches of 200 unprocessed PDFs directly to OLMoCR.
"""

import argparse
import json
import subprocess
from pathlib import Path
from typing import List, Set
import yaml


class ProcessedPDFTracker:
    """Track which PDFs have been processed."""

    def __init__(self, manifest_path: Path):
        self.manifest_path = manifest_path
        self.processed = self._load()

    def _load(self) -> Set[str]:
        if self.manifest_path.exists():
            data = json.loads(self.manifest_path.read_text())
            return set(data.get('processed', []))
        return set()

    def is_processed(self, identifier: str) -> bool:
        return identifier in self.processed

    def get_unprocessed_pdfs(self, pdf_dir: Path) -> List[Path]:
        """Get list of unprocessed PDFs."""
        all_pdfs = sorted(pdf_dir.glob("*.pdf"))
        unprocessed = []

        for pdf in all_pdfs:
            # Get identifier from filename (without extension)
            identifier = pdf.stem

            # Check if already processed
            if not self.is_processed(identifier):
                unprocessed.append(pdf)

        return unprocessed


def count_pdf_pages(pdf_path: Path) -> int:
    """Count pages in a PDF using pdfinfo."""
    try:
        result = subprocess.run(
            ['pdfinfo', str(pdf_path)],
            capture_output=True,
            text=True,
            timeout=20
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if line.startswith('Pages:'):
                    pages = line.split()[1]
                    if pages.isdigit():
                        return int(pages)
    except Exception:
        pass
    return 1  # Default to 1 page if we can't determine


def submit_batch_to_olmocr(
    pdf_dir: Path,
    pdfs: List[Path],
    olmocr_repo: Path,
    batch_number: int
) -> List[str]:
    """
    Submit a batch of PDFs to OLMoCR.

    Returns list of SLURM job IDs.
    """
    # OLMoCR expects PDF_DIR to point to directory with PDFs
    # Results will be written to PDF_DIR/results/

    # Create results directory if needed
    results_dir = pdf_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Calculate total pages for walltime estimation
    total_pages = sum(count_pdf_pages(pdf) for pdf in pdfs)

    # Estimate walltime: 300s startup + 6s per page + 20% buffer
    walltime_seconds = int((300 + total_pages * 6) * 1.2)
    walltime_hours = walltime_seconds // 3600
    walltime_minutes = (walltime_seconds % 3600) // 60
    walltime = f"{walltime_hours:02d}:{walltime_minutes:02d}:00"

    # Use direct_submit_batches logic
    slurm_script = olmocr_repo / "smart_process_pdf_chunks.slurm"

    # Create chunk list file
    chunk_dir = pdf_dir / "chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)

    chunk_file = chunk_dir / f"batch_{batch_number:04d}.txt"
    chunk_file.write_text('\n'.join(pdf.name for pdf in pdfs) + '\n')

    # Submit to SLURM
    cmd = [
        'sbatch',
        '--export', f'ALL,PDF_DIR={pdf_dir}',
        '--job-name', f'olmocr_batch_{batch_number}',
        '--output', str(pdf_dir / f'slurm-%j_batch_{batch_number}.out'),
        '--time', walltime,
        '--chdir', str(pdf_dir),
        '--parsable',
        str(slurm_script)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed: {result.stderr}")

    job_id = result.stdout.strip()
    return [job_id]


def main():
    parser = argparse.ArgumentParser(
        description="Submit unprocessed PDFs to OLMoCR"
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Pipeline YAML config"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Number of PDFs per batch"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done"
    )

    args = parser.parse_args()

    # Load config
    with open(args.config) as f:
        config = yaml.safe_load(f)

    base_dir = Path(config['directories']['base_dir'])
    pdf_dir = base_dir / "01_downloaded"
    olmocr_repo = Path(config['components']['olmocr_repo'])

    # Load tracker
    tracker = ProcessedPDFTracker(base_dir / "_manifests" / "processed_pdfs.json")

    # Get unprocessed PDFs
    unprocessed = tracker.get_unprocessed_pdfs(pdf_dir)

    print("=" * 70)
    print("Simple Batch Submitter")
    print("=" * 70)
    print(f"PDF directory: {pdf_dir}")
    print(f"Total PDFs in directory: {len(list(pdf_dir.glob('*.pdf')))}")
    print(f"Unprocessed PDFs: {len(unprocessed)}")
    print(f"Batch size: {args.batch_size}")
    print("=" * 70)
    print()

    if len(unprocessed) < args.batch_size:
        print(f"⏳ Only {len(unprocessed)} unprocessed PDFs (need {args.batch_size})")
        print("   Waiting for more downloads...")
        return

    # Take first batch_size PDFs
    batch_pdfs = unprocessed[:args.batch_size]

    print(f"📦 Ready to submit batch of {len(batch_pdfs)} PDFs")

    if args.dry_run:
        print("\n[DRY RUN] Would submit:")
        for pdf in batch_pdfs[:10]:
            print(f"  - {pdf.name}")
        if len(batch_pdfs) > 10:
            print(f"  ... and {len(batch_pdfs) - 10} more")
        return

    # Determine batch number
    existing_jobs = list((pdf_dir / "chunks").glob("batch_*.txt")) if (pdf_dir / "chunks").exists() else []
    batch_number = len(existing_jobs) + 1

    # Submit batch
    try:
        job_ids = submit_batch_to_olmocr(
            pdf_dir,
            batch_pdfs,
            olmocr_repo,
            batch_number
        )

        print(f"\n✅ Submitted batch {batch_number}")
        print(f"   Job IDs: {', '.join(job_ids)}")
        print(f"   PDFs: {len(batch_pdfs)}")
        print(f"   Results will appear in: {pdf_dir}/results/")

    except Exception as e:
        print(f"\n❌ Failed to submit batch: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
```

---

### 3. Splitter: `orchestration/split_jsonl_to_json.py`

**Current behavior:**
- Looks for `batch_dir/results/results/*.jsonl`
- Outputs to `batch_dir/results/json/`

**Required changes:**
Update to work with `01_downloaded/results/` instead of batch directories.

**Modified usage:**
```bash
# OLD:
python3 orchestration/split_jsonl_to_json.py /path/to/batch_0001

# NEW:
python3 orchestration/split_jsonl_to_json.py /path/to/01_downloaded
```

**Code changes:**
```python
# In split_jsonl_files function, update paths:

# OLD:
candidates = [
    pdf_dir / "results" / "results",
    pdf_dir / "results",
]
json_output_dir = pdf_dir / "results" / "json"

# NEW (same code works! just called differently):
# When pdf_dir = 01_downloaded, it looks in 01_downloaded/results/results/
# and outputs to 01_downloaded/results/json/
# No changes needed! Just call it with 01_downloaded as argument
```

**Status:** ✅ Works as-is with correct argument!

---

### 4. Finalizer: `streaming/file_based_finalize.py`

**Current behavior:**
- Scans `03_ocr_processing/batch_*/results/json/*.json`
- Matches to `01_downloaded/*.meta.json`
- Outputs to `05_processed/{identifier}/`

**Required changes:**
- Look in `01_downloaded/results/json/`
- Output to `02_processed/`
- Add processed PDF tracking

**Modified code:**
```python
#!/usr/bin/env python3
"""
Simplified Finalizer

Moves completed OCR results from 01_downloaded/results/json/
to 02_processed/{identifier}/ and deletes original PDFs.
"""

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set


class ProcessedPDFTracker:
    """Track processed PDFs."""

    def __init__(self, manifest_path: Path):
        self.manifest_path = manifest_path
        self.processed = self._load()

    def _load(self) -> Set[str]:
        if self.manifest_path.exists():
            data = json.loads(self.manifest_path.read_text())
            return set(data.get('processed', []))
        return set()

    def _save(self):
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            'processed': sorted(list(self.processed)),
            'last_updated': datetime.utcnow().isoformat() + 'Z'
        }
        self.manifest_path.write_text(json.dumps(data, indent=2))

    def mark_processed(self, identifier: str):
        self.processed.add(identifier)
        self._save()


def load_download_metadata(downloaded_dir: Path) -> Dict[str, Dict]:
    """Index metadata by PDF filename."""
    index: Dict[str, Dict] = {}
    for meta_path in downloaded_dir.glob("*.meta.json"):
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
            filename = data.get("filename")
            if isinstance(filename, str) and filename:
                data["__meta_path"] = str(meta_path)
                # Also index by identifier
                identifier = data.get("identifier")
                if identifier:
                    index[identifier] = data
                index[filename] = data
        except Exception:
            continue
    return index


def consolidate_one(
    json_file: Path,
    meta_index: Dict[str, Dict],
    downloaded_dir: Path,
    processed_dir: Path
) -> Optional[str]:
    """
    Consolidate one OCR result to 02_processed.

    Returns identifier if successful, None otherwise.
    """
    # Try to match by filename first
    pdf_filename = json_file.stem + ".pdf"
    meta = meta_index.get(pdf_filename)

    if not meta:
        # Try to match by identifier (json filename might be identifier.json)
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
    dest_json = dest_dir / f"{json_file.stem}.json"
    shutil.copy2(json_file, dest_json)

    # Look for markdown file
    results_dir = json_file.parent
    md_file = results_dir / f"{json_file.stem}.md"
    dest_md = None

    if md_file.exists():
        dest_md = dest_dir / f"{json_file.stem}.md"
        shutil.copy2(md_file, dest_md)

    # Create consolidated metadata
    merged = dict(meta)
    merged["ocr_json"] = f"{json_file.stem}.json"
    if dest_md:
        merged["ocr_markdown"] = f"{json_file.stem}.md"
    merged["ocr_consolidated_at"] = datetime.utcnow().isoformat() + "Z"
    merged["original_filename"] = pdf_filename
    merged.pop("__meta_path", None)

    dest_meta = dest_dir / f"{identifier}.meta.json"
    dest_meta.write_text(json.dumps(merged, indent=2), encoding="utf-8")

    # Check if all 3 files are present
    has_meta = dest_meta.exists()
    has_json = dest_json.exists()
    has_md = (dest_md and dest_md.exists()) or not md_file.exists()

    if has_meta and has_json and has_md:
        # Safe to delete original PDF
        pdf_path = downloaded_dir / pdf_filename
        if pdf_path.exists():
            try:
                pdf_path.unlink()
                print(f"  🗑️  Deleted: {pdf_filename}")
            except Exception as e:
                print(f"  ⚠️  Could not delete {pdf_filename}: {e}")

    return identifier


def main():
    parser = argparse.ArgumentParser(
        description="Finalize OCR results and cleanup PDFs"
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        required=True,
        help="Pipeline base directory"
    )

    args = parser.parse_args()

    base = args.base_dir
    downloaded = base / "01_downloaded"
    processed = base / "02_processed"
    results_json = downloaded / "results" / "json"

    print("=" * 70)
    print("Simplified Finalizer")
    print("=" * 70)
    print(f"Base: {base}")
    print(f"Results: {results_json}")
    print(f"Output: {processed}")
    print("-" * 70)

    # Load tracker
    tracker = ProcessedPDFTracker(base / "_manifests" / "processed_pdfs.json")

    # Load metadata
    meta_index = load_download_metadata(downloaded)
    print(f"Indexed {len(meta_index)} metadata entries")

    # Find JSON files
    if not results_json.exists():
        print("No results directory found")
        return

    json_files = list(results_json.glob("*.json"))
    print(f"Found {len(json_files)} OCR JSON files")
    print("-" * 70)

    consolidated = 0
    already_processed = 0
    missing_meta = 0

    for json_file in sorted(json_files):
        # Check if already processed
        identifier = json_file.stem
        if tracker.is_processed(identifier):
            already_processed += 1
            continue

        # Consolidate
        result_id = consolidate_one(json_file, meta_index, downloaded, processed)

        if result_id is None:
            missing_meta += 1
            print(f"  ⚠️  No metadata for {json_file.name}")
            continue

        # Mark as processed
        tracker.mark_processed(result_id)
        consolidated += 1
        print(f"  ✅ {result_id}")

    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Consolidated: {consolidated}")
    print(f"Already processed: {already_processed}")
    print(f"Missing metadata: {missing_meta}")
    print(f"Output: {processed}")


if __name__ == "__main__":
    main()
```

---

### 5. DuckDB Exporter: `tools/export_catalog_duckdb.py`

**Current behavior:**
- Scans `05_processed/`

**Required change:**
```python
# Line 78: Change directory name
processed = base_dir / "02_processed"  # was "05_processed"
```

**Status:** ✅ One-line change!

---

## Summary of Changes

| Component | Lines Changed | Complexity | Status |
|-----------|---------------|------------|--------|
| Downloader | ~10 lines | Simple (remove symlinks) | ✅ Ready |
| Batch Submitter | New file (~200 lines) | Medium | 📝 Need to write |
| Splitter | 0 lines | None (use as-is!) | ✅ Ready |
| Finalizer | Major rewrite (~150 lines) | Medium | 📝 Need to write |
| DuckDB Exporter | 1 line | Trivial | ✅ Ready |

## Testing Checklist

- [ ] Download 10 PDFs to `01_downloaded/`
- [ ] Check metadata files created
- [ ] Submit batch (should process 10 PDFs)
- [ ] Verify OLMoCR writes to `01_downloaded/results/`
- [ ] Run splitter on `01_downloaded/`
- [ ] Check JSON files in `01_downloaded/results/json/`
- [ ] Run finalizer
- [ ] Verify files in `02_processed/{identifier}/`
- [ ] Confirm PDFs deleted from `01_downloaded/`
- [ ] Check `processed_pdfs.json` updated
- [ ] Verify second batch submission skips processed PDFs
- [ ] Run DuckDB exporter
- [ ] Query catalog.duckdb

## Migration Commands

```bash
# On Nibi cluster
cd /home/jic823/projects/def-jic823/caribbean_pipeline

# Backup existing structure
tar -czf caribbean_pipeline_backup_$(date +%Y%m%d).tar.gz \
  02_ocr_pending 03_ocr_processing 04_ocr_completed

# Rename processed directory
mv 05_processed 02_processed

# Clean up intermediate directories
rm -rf 02_ocr_pending 03_ocr_processing 04_ocr_completed

# Initialize processed tracker from existing data
python3 /path/to/archive-olm-pipeline/tools/init_processed_tracker.py \
  --base-dir /home/jic823/projects/def-jic823/caribbean_pipeline

# Done! Ready to use new simplified pipeline
```
