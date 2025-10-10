# Pipeline Fixes Summary

## Current Status (October 10, 2025)

### Situation
- **2,148 PDFs** downloaded to 01_downloaded/
- **2,053 PDFs** stuck in batch_0001 (never successfully submitted to OLMoCR by dispatcher)
- **46 OLMoCR jobs** manually submitted earlier - COMPLETED successfully
- **154 JSONL result files** created by OLMoCR in `batch_0001/results/results/`
- **0 files** processed by cleanup worker (couldn't find results due to path mismatch)

### Problems Identified

#### 1. Dispatcher Cannot Submit Jobs ✅ DIAGNOSED
**Issue**: Dispatcher creates batches but fails when calling OLMoCR script with error:
```
✗ Error submitting batch: Could not parse SLURM job ID from output
```

**Root Cause**: The dispatcher's `_submit_ocr_job()` function runs the OLMoCR script but cannot parse the job ID from the output.

**Fix Applied**: Added debug logging to print the full script output when parsing fails.

**Next Steps**: Need to test actual submission to see what output format OLMoCR script produces.

#### 2. Cleanup Worker Cannot Find Results ✅ FIXED
**Issue**: OLMoCR creates results in `batch_0001/results/results/` but cleanup worker looks in `batch_0001/results/`

**Fix Applied**:
- Changed `_is_batch_complete()` to use `results_dir.glob("**/*.jsonl")` (recursive search)
- Changed `_split_jsonl()` to process all JSONL files in nested directories

#### 3. Missing Symlinks for Existing PDFs ✅ FIXED
**Issue**: Downloader skipped creating symlinks for already-downloaded PDFs, making them invisible to dispatcher.

**Fix Applied**:
- Modified downloader to create symlinks even for already-downloaded PDFs
- Created `backfill_symlinks.sh` script for bulk symlink creation

#### 4. Cleanup Worker Identifier Extraction ✅ FIXED
**Issue**: Cleanup worker aggregated all 154 JSONL files into single "unknown.ocr.jsonl" because it couldn't extract Archive.org identifiers from OLMoCR metadata.

**Root Cause**: Worker was looking for top-level `identifier` field, but OLMoCR stores the filename in nested `metadata.Source-File` (or `source_file`, `source`).

**Fix Applied**:
- Updated `_split_jsonl()` to use same logic as existing `orchestration/split_jsonl_to_json.py`
- Now checks multiple metadata fields: `Source-File`, `source_file`, `source`
- Extracts filename from path and removes `.pdf` extension to get identifier

**Current Situation**:
- Cleanup worker already ran on batch_0001 (before this fix)
- Created `unknown.ocr.jsonl` with all 154 JSONL files aggregated
- Deleted all 2,053 PDFs (freed ~80GB disk space)
- Marked batch as completed

**Recovery Plan**:
Since the original JSONL files still exist in `batch_0001/results/results/`, use the existing split script:
```bash
python3 orchestration/split_jsonl_to_json.py \
  /home/jic823/projects/def-jic823/caribbean_pipeline/03_ocr_processing/batch_0001
```
This will create properly split JSON files in `batch_0001/results/json/` that can be moved to `04_ocr_completed/`.

#### 5. Markdown Output Not Generated ✅ FIXED
**Issue**: OLMoCR needs flag to generate markdown files alongside JSONL.

**Fix Applied**:
- Added `--markdown` flag to `OLMOCR_FLAGS` in `smart_process_pdf_chunks.slurm`
- Pushed to git repository for consistency

#### 6. Batch Metadata Corruption ⚠️ ISSUE
**Issue**: batch_0001 metadata shows only 6 PDFs but directory contains 2,053 PDFs.

**Cause**: Dispatcher kept calling `_create_batch()` which overwrites metadata instead of checking for existing batch.

**Impact**: Metadata unreliable for tracking batch contents.

## Recommended Action Plan

### Phase 1: Test Fixed Cleanup Worker (PRIORITY)
1. Commit and push cleanup worker fixes
2. Pull on Nibi
3. Manually run cleanup worker against batch_0001:
   ```bash
   python3 streaming/file_based_cleanup.py \
     --base-dir /home/jic823/projects/def-jic823/caribbean_pipeline \
     --split-script orchestration/split_jsonl_to_json.py
   ```
4. Verify it processes the 154 JSONL files
5. Check that PDFs are deleted from batch_0001
6. Check results in 04_ocr_completed/

### Phase 2: Fix Dispatcher Submission
**Option A: Debug Existing Submission**
1. Test manual submission to understand output format
2. Fix parsing logic in dispatcher

**Option B: Simplify Dispatcher**
1. Remove submission code from dispatcher
2. Have dispatcher just create batch directories
3. Submit batches manually or via separate script
4. Cleanup worker handles everything else

### Phase 3: Add Markdown Generation
1. Research OLMoCR markdown flag (check olmocr --help or docs)
2. Add flag to `OLMOCR_FLAGS` in submission script
3. Modify cleanup worker to also move .md files to 04_ocr_completed/

### Phase 4: Handle Backlog
Once fixes are working:
1. Clear batch_0001 after cleanup worker processes it
2. Backfill symlinks for all 2,148 downloaded PDFs
3. Let dispatcher create new batches from pending queue
4. Submit batches (manually or automatically)

## Files Modified

### Fixed
- `streaming/file_based_cleanup.py` - Recursive JSONL search, identifier extraction from metadata
- `streaming/file_based_downloader.py` - Create symlinks for existing PDFs
- `streaming/file_based_dispatcher.py` - Debug output logging
- `streaming/smart_process_pdf_chunks.slurm` - Added `--markdown` flag for OLMoCR
- `backfill_symlinks.sh` - Bulk symlink creation script

## Testing Checklist

- [ ] Cleanup worker processes batch_0001 results
- [ ] Cleanup worker deletes PDFs after processing
- [ ] Results appear in 04_ocr_completed/
- [ ] Markdown files generated by OLMoCR
- [ ] Dispatcher successfully submits new batch
- [ ] Full pipeline flows from download → OCR → cleanup
- [ ] Disk space freed after cleanup

## Key Metrics

- **Downloaded**: 2,148 PDFs (~100 GB)
- **Pending OCR**: 94 symlinks (after batch_0001 consumed most)
- **In batch_0001**: 2,053 PDFs (need OCR submission)
- **OCR completed**: 154 JSONL files ready for cleanup
- **Final processed**: 0 (cleanup worker never ran)

## Next Immediate Steps

1. ✅ Commit cleanup worker identifier extraction fix
2. ✅ Add markdown flag to OLMoCR SLURM script
3. ⏳ Pull latest changes on Nibi cluster
4. ⏳ Run split script on batch_0001 to properly extract identifiers:
   ```bash
   cd /home/jic823/projects/def-jic823/cluster
   python3 orchestration/split_jsonl_to_json.py \
     /home/jic823/projects/def-jic823/caribbean_pipeline/03_ocr_processing/batch_0001
   ```
5. ⏳ Move split JSON files from `batch_0001/results/json/` to `04_ocr_completed/`
6. ⏳ Backfill symlinks for all downloaded PDFs
7. ⏳ Test dispatcher submission (or submit new batch manually)
8. ⏳ Verify cleanup worker works correctly on new batches
