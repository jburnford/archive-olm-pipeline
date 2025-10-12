# Simplified 2-Directory Pipeline Plan

## Problem Statement

Current pipeline has 5 directories, symlinks, and complex coordination between multiple processes. It's brittle and hard to maintain.

## Solution: 2-Directory Architecture

```
01_downloaded/               # Everything stays here until finalized
├── {identifier}.pdf         # Downloaded PDFs (deleted after processing)
├── {identifier}.meta.json   # Download metadata
└── results/                 # OLMoCR output directory
    ├── results/             # JSONL files from OLMoCR
    │   └── *.jsonl
    └── json/                # Split JSON files (after processing)
        ├── {pdfname}.json
        └── {pdfname}.md

02_processed/                # Final destination
└── {identifier}/
    ├── {identifier}.meta.json  # Consolidated metadata
    ├── {pdfname}.json          # OCR JSON
    └── {pdfname}.md            # OCR Markdown

_manifests/                  # Tracking files
├── download_progress.json   # Download progress
├── processed_pdfs.json      # List of processed PDFs
└── batches.json             # Batch tracking
```

## Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ Phase 1: DOWNLOAD (continuous)                              │
│ → Download PDFs to 01_downloaded/{id}.pdf                   │
│ → Save metadata to 01_downloaded/{id}.meta.json             │
│ → Track in _manifests/download_progress.json                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 2: BATCH & OCR (every 200 unprocessed PDFs)           │
│ → Check _manifests/processed_pdfs.json for unprocessed      │
│ → Submit batch of 200 to OLMoCR (direct submission)         │
│ → OLMoCR runs on 01_downloaded/ in place                    │
│ → Results written to 01_downloaded/results/                 │
│ → Continue downloading next 200 while OCR runs              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 3: SPLIT (after OCR completes)                        │
│ → Watch for *.jsonl in 01_downloaded/results/results/       │
│ → Split JSONL into per-PDF JSON files                       │
│ → Output to 01_downloaded/results/json/{pdfname}.json       │
│ → Extract markdown if OLMoCR generates it                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 4: FINALIZE (continuous)                              │
│ → For each JSON in results/json/:                           │
│   - Match to identifier via {id}.meta.json                  │
│   - Create 02_processed/{identifier}/                       │
│   - Copy {pdfname}.json and {pdfname}.md                    │
│   - Create/update {identifier}.meta.json with pointers      │
│   - When all 3 files present, delete PDF from 01_downloaded │
│   - Mark as processed in _manifests/processed_pdfs.json     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 5: EXPORT (periodic)                                  │
│ → Run DuckDB exporter on 02_processed/                      │
│ → Create catalog.duckdb for local analysis                  │
└─────────────────────────────────────────────────────────────┘
```

## Key Simplifications

### 1. No Symlinks
- PDFs stay in `01_downloaded/` until finalized
- OLMoCR processes files in place
- No symlink complications with `find` or `ls`

### 2. No Intermediate Directories
- Removed: `02_ocr_pending`, `03_ocr_processing`, `04_ocr_completed`
- Everything happens in `01_downloaded/` until ready for `02_processed/`

### 3. Simple Tracking
- `_manifests/processed_pdfs.json` tracks which PDFs are done
- Batch submitter only processes untracked PDFs
- No complex state machines or batch directories

### 4. Clear Deletion Logic
- PDF deleted ONLY when all 3 files exist in `02_processed/{identifier}/`
- Easy to verify before deletion
- Safe and explicit

### 5. DuckDB for Analysis
- Run exporter periodically on `02_processed/`
- Local analysis without cluster database
- No SQLite over NFS issues

## Reusable Components

### Can Use As-Is (with path updates)

1. **`streaming/file_based_downloader.py`**
   - Change: Remove symlink creation to `02_ocr_pending`
   - Change: Update paths to new structure
   - Keep: All download logic, metadata generation

2. **`orchestration/split_jsonl_to_json.py`**
   - Change: Update to look in `01_downloaded/results/results/`
   - Change: Output to `01_downloaded/results/json/`
   - Keep: All splitting logic (it's robust!)

3. **`streaming/direct_submit_batches.py`**
   - Change: Read from `01_downloaded/` instead of batch dirs
   - Change: Check `_manifests/processed_pdfs.json` to filter PDFs
   - Change: Submit with `PDF_DIR=01_downloaded/`
   - Keep: All SLURM submission logic

4. **`tools/export_catalog_duckdb.py`**
   - Change: Point to `02_processed/` instead of `05_processed/`
   - Keep: All scanning and export logic

### Need to Modify

1. **`streaming/file_based_finalize.py`**
   - Change: Look in `01_downloaded/results/json/` for completed JSONs
   - Change: Match to metadata in `01_downloaded/{id}.meta.json`
   - Change: Output to `02_processed/{identifier}/`
   - Change: Add tracking to `_manifests/processed_pdfs.json`
   - Keep: Consolidation logic

### New Components Needed

1. **`streaming/simple_orchestrator.py`**
   - Single coordinator script
   - Runs downloader, submitter, splitter, finalizer in sequence
   - Simple, linear flow
   - Easy to understand and debug

2. **Processed PDFs Tracker** (add to finalizer)
   - JSON file: `_manifests/processed_pdfs.json`
   - Structure: `{"processed": ["id1", "id2", ...]}`
   - Updated by finalizer after successful completion
   - Read by batch submitter to filter

## Implementation Steps

### Step 1: Create Tracking Module
```python
# utils/pdf_tracker.py
import json
from pathlib import Path
from typing import Set

class ProcessedPDFTracker:
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
        data = {'processed': sorted(list(self.processed))}
        self.manifest_path.write_text(json.dumps(data, indent=2))

    def mark_processed(self, identifier: str):
        self.processed.add(identifier)
        self._save()

    def is_processed(self, identifier: str) -> bool:
        return identifier in self.processed

    def get_unprocessed(self, all_pdfs: list) -> list:
        return [p for p in all_pdfs if Path(p).stem not in self.processed]
```

### Step 2: Simplify Downloader
- Remove `02_ocr_pending` symlink creation
- Keep everything else the same

### Step 3: Create Smart Batch Submitter
```python
# streaming/simple_batch_submitter.py
# - Scan 01_downloaded/ for *.pdf
# - Filter out processed PDFs using tracker
# - When 200+ unprocessed PDFs available, submit batch
# - OLMoCR processes directly from 01_downloaded/
# - Results go to 01_downloaded/results/
```

### Step 4: Adapt Splitter
- Update paths to point to `01_downloaded/results/`
- Keep all existing logic

### Step 5: Enhance Finalizer
- Update paths to read from `01_downloaded/results/json/`
- Match to `01_downloaded/{id}.meta.json`
- Output to `02_processed/{identifier}/`
- Add PDF tracking after successful finalization
- Delete PDF only after all 3 files are in place

### Step 6: Simple Orchestrator
```python
# streaming/simple_orchestrator.py
#
# while True:
#     run_downloader(until=200_new_pdfs)
#     if unprocessed_count >= 200:
#         submit_batch()
#     check_for_completed_ocr()
#     if completed_jsonl_found:
#         run_splitter()
#     run_finalizer()
#     sleep(60)
```

## Configuration

### Simplified YAML
```yaml
directories:
  base_dir: /home/jic823/projects/def-jic823/caribbean_pipeline
  downloaded: 01_downloaded
  processed: 02_processed

download:
  identifiers_file: /path/to/caribbean_identifiers.json
  batch_size: 200
  delay: 0.05
  collection: caribbean

ocr:
  olmocr_repo: /home/jic823/projects/def-jic823/cluster/olmocr
  pdfs_per_batch: 200
  workers: 8

cleanup:
  auto_delete_pdfs: true  # Delete after all 3 files in 02_processed

export:
  catalog_path: export/catalog.duckdb
  run_interval: 3600  # Export every hour

slurm:
  email: jic823@usask.ca
  account: def-jic823
  time_limit: "144:00:00"
  memory: "16G"
  cpus: 4
```

## Advantages

1. **Simpler**: 2 directories instead of 5
2. **No Symlinks**: Everything is a real file
3. **Easy to Monitor**: Clear what stage each file is at
4. **Resumable**: Tracker knows what's been processed
5. **Safe Deletion**: Explicit 3-file check before removing PDF
6. **Reuses Code**: 80% of existing code works with path updates
7. **DuckDB Ready**: Built-in export for local analysis

## Migration from Current System

### If you have existing data:
```bash
# 1. Archive current pipeline data
mv caribbean_pipeline caribbean_pipeline.backup

# 2. Create new structure
mkdir -p caribbean_pipeline/{01_downloaded,02_processed,_manifests}

# 3. Copy any already-processed files from 05_processed to 02_processed
cp -r caribbean_pipeline.backup/05_processed/* caribbean_pipeline/02_processed/

# 4. Extract list of processed identifiers
python3 tools/extract_processed_list.py \
  --from caribbean_pipeline.backup/05_processed \
  --to caribbean_pipeline/_manifests/processed_pdfs.json

# 5. Start new pipeline
```

## Testing Strategy

1. **Test with 10 PDFs first**
   - Download 10
   - Process batch
   - Verify split
   - Check finalization
   - Confirm deletion

2. **Verify tracking**
   - Check `processed_pdfs.json` updates
   - Confirm resubmission skips processed PDFs

3. **Run 200-PDF batch**
   - Full workflow test
   - Monitor for errors

4. **Scale to production**

## Success Metrics

- ✅ All PDFs downloaded to `01_downloaded/`
- ✅ OCR results appear in `01_downloaded/results/`
- ✅ Split JSONs in `01_downloaded/results/json/`
- ✅ Finalized files in `02_processed/{identifier}/`
- ✅ PDFs deleted only after 3 files present
- ✅ `processed_pdfs.json` accurately tracks completion
- ✅ No orphaned files or broken references
- ✅ DuckDB catalog exports successfully

## Next Steps

1. Create PDF tracker utility (`utils/pdf_tracker.py`)
2. Adapt downloader (remove symlinks)
3. Create simple batch submitter
4. Enhance finalizer with tracking
5. Write simple orchestrator
6. Test with 10 PDFs
7. Deploy to production

---

**Key Insight:** The simpler the pipeline, the more reliable it is. This design eliminates intermediate states, symlinks, and complex coordination while keeping all your working code.
