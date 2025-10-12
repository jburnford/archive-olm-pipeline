# Testing Guide - Simplified Pipeline

## Overview

This guide walks through testing the simplified 2-directory pipeline with the 372 PDFs already downloaded on Nibi.

## Prerequisites

1. **On Nibi cluster** - All commands run there
2. **372 PDFs** in `01_downloaded/` ready for testing
3. **1,776 items** already in `05_processed/` (will become `02_processed/`)

## Phase 1: Migration (30 minutes)

### Step 1: Backup Current State

```bash
cd ~/projects/def-jic823/caribbean_pipeline

# Full backup
tar -czf ~/backups/caribbean_pipeline_$(date +%Y%m%d_%H%M).tar.gz .

# Quick verification
ls -lh ~/backups/
```

### Step 2: Rename Processed Directory

```bash
cd ~/projects/def-jic823/caribbean_pipeline

# Rename 05_processed to 02_processed
mv 05_processed 02_processed

# Verify
ls -l | grep processed
```

### Step 3: Extract Processed IDs

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

python3 tools/extract_processed_ids.py \
  --processed-dir ~/projects/def-jic823/caribbean_pipeline/02_processed \
  --output ~/projects/def-jic823/caribbean_pipeline/_manifests/processed_pdfs.json

# Verify
cat ~/projects/def-jic823/caribbean_pipeline/_manifests/processed_pdfs.json | jq '.count'
# Should show 1776
```

### Step 4: Clean Up Old Structure

```bash
cd ~/projects/def-jic823/caribbean_pipeline

# Archive intermediate directories
tar -czf ~/backups/caribbean_intermediate_$(date +%Y%m%d).tar.gz \
  02_ocr_pending 03_ocr_processing 04_ocr_completed

# Remove them
rm -rf 02_ocr_pending 03_ocr_processing 04_ocr_completed

# Verify structure
ls -l
# Should see: 01_downloaded, 02_processed, _manifests, 99_errors
```

### Step 5: Clean Up Already-Processed PDFs (Optional)

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

# Dry run first
python3 tools/cleanup_processed_pdfs.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --dry-run

# If looks good, run for real
python3 tools/cleanup_processed_pdfs.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline

# This will free ~2GB of space
```

## Phase 2: Code Deployment (10 minutes)

### Step 1: Push from WSL

```bash
# On WSL
cd /home/jic823/archive-olm-pipeline

git add -A
git commit -m "feat: simplified 2-directory pipeline with smart backpressure"
git push
```

### Step 2: Pull on Nibi

```bash
# On Nibi
cd ~/projects/def-jic823/archive-olm-pipeline
git pull
```

## Phase 3: Test Batch Submission (1 hour)

### Step 1: Check Status

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

# Check unprocessed count
python3 streaming/simple_batch_submitter.py \
  --config config/caribbean_filebased.yaml \
  --dry-run

# Should show:
# - Total PDFs: 372
# - Processed: 1776
# - Unprocessed: 372 (if cleanup not run) or ~200 (if cleanup run)
```

### Step 2: Submit Test Batch

```bash
# Submit batch of 200 PDFs
python3 streaming/simple_batch_submitter.py \
  --config config/caribbean_filebased.yaml \
  --batch-size 200

# Note the job ID and batch number
```

### Step 3: Monitor OLMoCR Job

```bash
# Check job queue
squeue -u $USER

# Watch job output
watch -n 30 squeue -u $USER

# View SLURM output (replace JOBID)
tail -f ~/projects/def-jic823/caribbean_pipeline/slurm-JOBID_batch_0001.out
```

**Expected behavior:**
- Job should start processing PDFs
- Results will appear in `01_downloaded/results/results/*.jsonl`
- Should take 1-2 hours for 200 PDFs

### Step 4: Verify Results Location

```bash
# Check for JSONL results
ls ~/projects/def-jic823/caribbean_pipeline/01_downloaded/results/results/

# Should see *.jsonl files appearing
```

## Phase 4: Test Splitting (15 minutes)

### Wait for OLMoCR to Complete

```bash
# Check if job is done
squeue -u $USER

# When no jobs running, proceed
```

### Run Splitter

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

python3 orchestration/split_jsonl_to_json.py \
  ~/projects/def-jic823/caribbean_pipeline/01_downloaded

# Should see:
# - Scanning results/results/*.jsonl
# - Creating results/json/*.json files
```

### Verify Split Results

```bash
# Check JSON files created
ls ~/projects/def-jic823/caribbean_pipeline/01_downloaded/results/json/*.json | wc -l

# Should match number of PDFs processed (~200)
```

## Phase 5: Test Finalization (10 minutes)

### Run Finalizer

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

python3 streaming/simplified_finalizer.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --auto-delete-pdfs

# Should see:
# - Matching JSON files to metadata
# - Creating directories in 02_processed/{identifier}/
# - Copying JSON files
# - Deleting original PDFs
# - Updating processed_pdfs.json
```

### Verify Results

```bash
BASE=~/projects/def-jic823/caribbean_pipeline

# Count processed items (should increase)
ls $BASE/02_processed/ | wc -l

# Check processed tracking updated
cat $BASE/_manifests/processed_pdfs.json | jq '.count'

# Verify PDFs deleted
ls $BASE/01_downloaded/*.pdf | wc -l
# Should be ~172 (372 - 200)

# Check a processed item
ls $BASE/02_processed/*/
# Should see: {identifier}.meta.json and {pdfname}.json files
```

## Phase 6: Test Orchestrator (Optional)

### Submit Orchestrator Job

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

sbatch streaming/run_simplified_pipeline.sh

# Note job ID
```

### Monitor

```bash
# Check job
squeue -u $USER

# Watch output
tail -f ~/projects/def-jic823/slurm-*.out

# Should see:
# - Iteration messages
# - Batch submission (if 200+ unprocessed)
# - Split phase
# - Finalize phase
# - Backpressure status
```

## Phase 7: Test DuckDB Export (5 minutes)

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

python3 tools/export_catalog_duckdb.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --fast

# Verify
ls -lh ~/projects/def-jic823/caribbean_pipeline/export/catalog.duckdb

# Should see DuckDB file created
```

## Verification Checklist

After testing, verify:

- [ ] `02_processed/` has 1776+ items
- [ ] `_manifests/processed_pdfs.json` shows correct count
- [ ] Each item in `02_processed/` has:
  - [ ] `{identifier}.meta.json`
  - [ ] `{pdfname}.json`
- [ ] PDFs deleted from `01_downloaded/` after finalization
- [ ] `export/catalog.duckdb` created successfully
- [ ] OLMoCR results appeared in `01_downloaded/results/`
- [ ] Splitter created files in `01_downloaded/results/json/`
- [ ] Finalizer moved files to `02_processed/`

## Troubleshooting

### "No metadata" errors in finalizer

**Cause:** JSON filename doesn't match any metadata file

**Fix:**
```bash
# Check metadata files
ls ~/projects/def-jic823/caribbean_pipeline/01_downloaded/*.meta.json | head

# Check JSON files
ls ~/projects/def-jic823/caribbean_pipeline/01_downloaded/results/json/*.json | head

# Filenames should match (except .pdf vs .json)
```

### OLMoCR results not appearing

**Check:**
1. Job is still running: `squeue -u $USER`
2. Job output for errors: `tail -100 ~/projects/def-jic823/caribbean_pipeline/slurm-*.out`
3. Results directory exists: `ls ~/projects/def-jic823/caribbean_pipeline/01_downloaded/results/`

### Backpressure always active

**Check:**
```bash
python3 streaming/simple_batch_submitter.py \
  --config config/caribbean_filebased.yaml \
  --dry-run

# If unprocessed > 500, run finalizer to clear backlog
python3 streaming/simplified_finalizer.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --auto-delete-pdfs
```

## Success Indicators

✅ **Pipeline is working if:**

1. Batch submitter reports correct unprocessed count
2. OLMoCR job completes and writes results
3. Splitter creates JSON files
4. Finalizer moves files to 02_processed
5. PDFs are deleted after finalization
6. processed_pdfs.json tracks correctly
7. DuckDB export succeeds

## Next Steps After Testing

1. **Resume downloads** (if paused)
2. **Deploy orchestrator** for continuous operation
3. **Monitor backpressure** to ensure downloads pause appropriately
4. **Check disk usage** regularly
5. **Export catalog** periodically for local analysis

## Quick Test Command

Run all phases in sequence:

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

# Step 1: Extract processed IDs
python3 tools/extract_processed_ids.py \
  --processed-dir ~/projects/def-jic823/caribbean_pipeline/02_processed \
  --output ~/projects/def-jic823/caribbean_pipeline/_manifests/processed_pdfs.json

# Step 2: Submit batch
python3 streaming/simple_batch_submitter.py \
  --config config/caribbean_filebased.yaml \
  --batch-size 200

# Step 3: Wait for OLMoCR (monitor with squeue)

# Step 4: Split
python3 orchestration/split_jsonl_to_json.py \
  ~/projects/def-jic823/caribbean_pipeline/01_downloaded

# Step 5: Finalize
python3 streaming/simplified_finalizer.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --auto-delete-pdfs

# Step 6: Export
python3 tools/export_catalog_duckdb.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --fast
```

## Contact

If issues arise, check:
- SLURM job output files
- `_manifests/` directory for state
- `99_errors/` for error logs
