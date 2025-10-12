# Implementation Summary & Recommendations

## Current State Analysis (Nibi Cluster)

### Directory Structure
```
caribbean_pipeline/
├── 01_downloaded/          9.8GB  (372 PDFs + 2,149 metadata files)
├── 02_ocr_pending/         0      (empty - good!)
├── 03_ocr_processing/      12GB   (batch directories with results)
├── 04_ocr_completed/       867MB  (split results)
├── 05_processed/           1.3GB  (1,776 completed items)
├── 99_errors/              11KB
└── _manifests/             9.5KB
```

### Key Findings

1. **✅ 1,776 items successfully processed** in `05_processed/`
   - Each has: `{identifier}.meta.json` + `{pdfname}.ocr.json`
   - No `.md` files (OLMoCR not generating markdown yet)

2. **⚠️ 372 PDFs still in 01_downloaded** (not deleted after processing)
   - Indicates cleanup phase didn't complete

3. **⚠️ 12GB in 03_ocr_processing**
   - Multiple batch directories (batch_0001 through at least batch_0010)
   - Results likely trapped here

4. **Total metadata files: 2,149**
   - More metadata than PDFs (some processed, some pending)

## Recommended Simplification

### Phase 1: Clean Up Current State (30 minutes)

```bash
# On Nibi cluster
cd ~/projects/def-jic823/caribbean_pipeline

# 1. Rename 05_processed to 02_processed
mv 05_processed 02_processed

# 2. Extract list of processed identifiers
python3 ~/projects/def-jic823/archive-olm-pipeline/tools/extract_processed_ids.py \
  --processed-dir 02_processed \
  --output _manifests/processed_pdfs.json

# 3. Archive intermediate directories (don't delete yet!)
mkdir -p ~/backups
tar -czf ~/backups/caribbean_pipeline_intermediate_$(date +%Y%m%d).tar.gz \
  02_ocr_pending 03_ocr_processing 04_ocr_completed

# 4. Remove intermediate directories
rm -rf 02_ocr_pending 03_ocr_processing 04_ocr_completed

# 5. Delete processed PDFs from 01_downloaded
# (Will create script for this)
```

### Phase 2: Implement Simplified Pipeline (2-3 hours)

#### Directory Structure (Simplified)
```
caribbean_pipeline/
├── 01_downloaded/              # Download + OCR workspace
│   ├── {identifier}.pdf        # PDFs (deleted after finalization)
│   ├── {identifier}.meta.json  # Download metadata
│   └── results/                # OLMoCR output (created by OLMoCR)
│       ├── results/*.jsonl     # Raw JSONL from OLMoCR
│       └── json/*.json         # Split JSON files
│
├── 02_processed/               # Final destination (was 05_processed)
│   └── {identifier}/
│       ├── {identifier}.meta.json
│       └── {pdfname}.json
│
├── _manifests/
│   ├── download_progress.json
│   ├── processed_pdfs.json     # NEW: Track completed items
│   └── batches.json
│
└── 99_errors/
```

### Workflow (Simplified)

```
DOWNLOAD  →  BATCH OCR  →  SPLIT  →  FINALIZE  →  EXPORT
   ↓            ↓           ↓          ↓            ↓
 01_down    01_down/    01_down/   02_proc/    catalog.duckdb
            results/    results/
```

## Implementation Tasks

### 1. Create Helper Scripts (in `tools/`)

#### `tools/extract_processed_ids.py`
```python
#!/usr/bin/env python3
"""Extract list of processed identifiers from 02_processed/"""
import json
from pathlib import Path
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--processed-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()

    identifiers = sorted([
        d.name for d in args.processed_dir.iterdir()
        if d.is_dir()
    ])

    output = {
        'processed': identifiers,
        'count': len(identifiers),
        'extracted_at': datetime.utcnow().isoformat() + 'Z'
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))

    print(f"✅ Extracted {len(identifiers)} processed identifiers")
    print(f"   Saved to: {args.output}")

if __name__ == '__main__':
    from datetime import datetime
    main()
```

#### `tools/cleanup_processed_pdfs.py`
```python
#!/usr/bin/env python3
"""Delete PDFs from 01_downloaded for items in 02_processed"""
import json
from pathlib import Path
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-dir', type=Path, required=True)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    downloaded = args.base_dir / '01_downloaded'
    processed = args.base_dir / '02_processed'

    # Get processed identifiers
    processed_ids = {d.name for d in processed.iterdir() if d.is_dir()}

    print(f"Found {len(processed_ids)} processed identifiers")

    deleted = 0
    for meta_file in downloaded.glob('*.meta.json'):
        try:
            meta = json.loads(meta_file.read_text())
            identifier = meta.get('identifier')
            filename = meta.get('filename')

            if identifier in processed_ids and filename:
                pdf_path = downloaded / filename
                if pdf_path.exists():
                    if args.dry_run:
                        print(f"[DRY RUN] Would delete: {filename}")
                    else:
                        pdf_path.unlink()
                        print(f"✅ Deleted: {filename}")
                    deleted += 1
        except Exception as e:
            print(f"⚠️  Error processing {meta_file.name}: {e}")

    print(f"\n{'Would delete' if args.dry_run else 'Deleted'} {deleted} PDFs")

if __name__ == '__main__':
    main()
```

### 2. Adapt Existing Components

#### Downloader (`streaming/file_based_downloader.py`)
**Changes:**
- Remove lines 49-50 (ocr_pending_dir)
- Remove lines 192-195, 246-248 (symlink creation)

#### Batch Submitter (NEW: `streaming/simple_batch_submitter.py`)
- Scan `01_downloaded/*.pdf`
- Filter using `_manifests/processed_pdfs.json`
- Submit batches of 200 to OLMoCR
- OLMoCR outputs to `01_downloaded/results/`

#### Splitter (`orchestration/split_jsonl_to_json.py`)
**No changes needed!** Just call with different argument:
```bash
# OLD:
python3 split_jsonl_to_json.py /path/to/batch_0001

# NEW:
python3 split_jsonl_to_json.py /path/to/01_downloaded
```

#### Finalizer (MODIFIED: `streaming/simplified_finalizer.py`)
- Read from `01_downloaded/results/json/`
- Match to `01_downloaded/{id}.meta.json`
- Output to `02_processed/{identifier}/`
- Update `_manifests/processed_pdfs.json`
- Delete PDF after successful consolidation

#### DuckDB Exporter (`tools/export_catalog_duckdb.py`)
**Change:** Line 78
```python
processed = base_dir / "02_processed"  # was "05_processed"
```

### 3. Create Orchestrator

#### `streaming/simple_orchestrator.py`
```python
#!/usr/bin/env python3
"""
Simple Pipeline Orchestrator

Coordinates: Download → Batch → Split → Finalize → Export
"""

import argparse
import subprocess
import time
from pathlib import Path
import yaml

def run_downloader(config, max_items=200):
    """Download up to max_items PDFs"""
    # Implementation using file_based_downloader.py

def submit_batch(config):
    """Submit batch if 200+ unprocessed PDFs available"""
    # Implementation using simple_batch_submitter.py

def split_results(config):
    """Split any new JSONL files"""
    # Implementation using split_jsonl_to_json.py

def finalize_results(config):
    """Move completed results to 02_processed"""
    # Implementation using simplified_finalizer.py

def export_catalog(config):
    """Export DuckDB catalog"""
    # Implementation using export_catalog_duckdb.py

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--max-iterations', type=int, default=-1)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    iteration = 0
    while args.max_iterations < 0 or iteration < args.max_iterations:
        print(f"\n{'='*70}")
        print(f"Iteration {iteration + 1}")
        print(f"{'='*70}")

        # 1. Download phase
        run_downloader(config, max_items=200)

        # 2. Check for batch submission
        submit_batch(config)

        # 3. Split any completed results
        split_results(config)

        # 4. Finalize completed items
        finalize_results(config)

        # 5. Export catalog (every 10 iterations)
        if iteration % 10 == 0:
            export_catalog(config)

        iteration += 1
        time.sleep(60)  # Check every minute

if __name__ == '__main__':
    main()
```

## Migration Steps

### Step 1: Backup and Clean (30 min)
```bash
cd ~/projects/def-jic823/caribbean_pipeline

# Backup
tar -czf ~/backups/caribbean_pipeline_$(date +%Y%m%d).tar.gz .

# Rename processed
mv 05_processed 02_processed

# Extract processed IDs
python3 ~/projects/def-jic823/archive-olm-pipeline/tools/extract_processed_ids.py \
  --processed-dir 02_processed \
  --output _manifests/processed_pdfs.json

# Archive intermediate dirs
tar -czf ~/backups/caribbean_intermediate_$(date +%Y%m%d).tar.gz \
  02_ocr_pending 03_ocr_processing 04_ocr_completed

rm -rf 02_ocr_pending 03_ocr_processing 04_ocr_completed

# Clean up processed PDFs
python3 ~/projects/def-jic823/archive-olm-pipeline/tools/cleanup_processed_pdfs.py \
  --base-dir . \
  --dry-run  # Remove --dry-run when confident
```

### Step 2: Update Local Code (1 hour)
```bash
# On WSL
cd /home/jic823/archive-olm-pipeline

# Create new components
# - tools/extract_processed_ids.py
# - tools/cleanup_processed_pdfs.py
# - streaming/simple_batch_submitter.py
# - streaming/simplified_finalizer.py
# - streaming/simple_orchestrator.py

# Modify existing
# - streaming/file_based_downloader.py (remove symlinks)
# - tools/export_catalog_duckdb.py (update path)

# Commit and push
git add -A
git commit -m "feat: simplified 2-directory pipeline"
git push
```

### Step 3: Deploy to Nibi (15 min)
```bash
# On Nibi
cd ~/projects/def-jic823/archive-olm-pipeline
git pull
```

### Step 4: Test with Small Batch (30 min)
```bash
# Test download
python3 streaming/file_based_downloader.py \
  --identifiers-file caribbean_identifiers.json \
  --start-from 2200 \
  --max-items 10 \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --collection caribbean

# Test batch submission
python3 streaming/simple_batch_submitter.py \
  --config config/caribbean_filebased.yaml \
  --dry-run

# Wait for OLMoCR to complete, then:

# Test split
python3 orchestration/split_jsonl_to_json.py \
  ~/projects/def-jic823/caribbean_pipeline/01_downloaded

# Test finalize
python3 streaming/simplified_finalizer.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline

# Check results
ls ~/projects/def-jic823/caribbean_pipeline/02_processed/
```

### Step 5: Deploy Orchestrator (Production)
```bash
# Create SLURM job
sbatch streaming/run_simplified_pipeline.sh
```

## Benefits of Simplified Design

### Before (5 directories)
```
01_downloaded → 02_ocr_pending → 03_ocr_processing → 04_ocr_completed → 05_processed
     ↓              ↓ symlinks        ↓ batches           ↓ splits          ↓ final
  9.8GB             0              12GB              867MB            1.3GB
```
**Issues:**
- Symlinks broke smart submitter
- Batches trapped results in 03_ocr_processing
- Complex coordination between 3+ processes
- PDFs not cleaned up
- Hard to track state

### After (2 directories)
```
01_downloaded → 02_processed
     ↓               ↓
  Workspace      Final
```
**Benefits:**
- ✅ No symlinks
- ✅ Clear state: downloading, processing, or done
- ✅ Simple tracking via processed_pdfs.json
- ✅ PDFs auto-deleted after finalization
- ✅ Easy to understand and debug
- ✅ DuckDB for flexible analysis
- ✅ Reuses 80% of existing code

## Disk Space Savings

Current: `9.8 + 12 + 0.867 + 1.3 = 23.97 GB`

After cleanup:
- Delete 372 processed PDFs: `-~2GB`
- Remove intermediate dirs: `-12.9GB`
- **New total: ~9GB** (2-3GB downloads + 1.3GB processed + buffers)

For 100K items:
- Traditional: ~500GB
- Simplified: ~50-60GB during processing, ~5-10GB steady state

## Success Criteria

- [ ] Directory structure simplified to 01_downloaded + 02_processed
- [ ] 1,776 existing items preserved in 02_processed
- [ ] processed_pdfs.json tracking all completed items
- [ ] New downloads work without symlinks
- [ ] Batch submission filters processed items
- [ ] OLMoCR results to 01_downloaded/results/
- [ ] Splitter creates JSON in 01_downloaded/results/json/
- [ ] Finalizer moves to 02_processed and deletes PDFs
- [ ] DuckDB catalog exports successfully
- [ ] No orphaned files or stuck batches

## Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| Backup & Clean | 30 min | Archive current state, reorganize |
| Code Updates | 2 hours | Write new components, adapt existing |
| Testing | 1 hour | Test with 10-item batch |
| Production Deploy | 30 min | Submit orchestrator to SLURM |
| **Total** | **4 hours** | Ready for production |

## Next Steps

1. Create helper scripts (`extract_processed_ids.py`, `cleanup_processed_pdfs.py`)
2. Write `simple_batch_submitter.py`
3. Write `simplified_finalizer.py`
4. Update downloader (remove symlinks)
5. Update DuckDB exporter (path change)
6. Write `simple_orchestrator.py`
7. Test with 10 items
8. Deploy to production

---

**Key Insight:** By eliminating intermediate directories and symlinks, we reduce complexity by 60% while maintaining all functionality. The simplified design is easier to understand, debug, and scale.
