# Production Pipeline Run Guide

## Saskatchewan Collection: 16,075 Items

This guide explains how to run the full pipeline to process all 16,075 Saskatchewan items from Archive.org.

## Prerequisites

✅ Identifiers file created: `/home/jic823/projects/def-jic823/pdfs_sask_test/identifiers.json` (16,075 identifiers)
✅ Download phase tested successfully
⚠️ **Need to test**: OCR, Ingest, and Cleanup phases

## Testing Remaining Phases (Do First!)

Before running the full production pipeline, test the remaining phases with your current 100 downloaded PDFs:

### 1. Test OCR Phase
```bash
python3 orchestration/pipeline_orchestrator.py \
    --config config/test_saskatchewan.yaml \
    ocr
```
**Expected**: Submits olmOCR SLURM jobs, waits for completion, OCR results generated

### 2. Test Ingest Phase
```bash
python3 orchestration/pipeline_orchestrator.py \
    --config config/test_saskatchewan.yaml \
    ingest
```
**Expected**: OCR data added to database, check `ocr_processing` table

### 3. Test Cleanup Phase
```bash
# Dry run first
python3 orchestration/pipeline_orchestrator.py \
    --config config/test_saskatchewan.yaml \
    cleanup --dry-run

# Then actually delete
python3 orchestration/pipeline_orchestrator.py \
    --config config/test_saskatchewan.yaml \
    cleanup
```
**Expected**: PDFs deleted after successful OCR and ingest

## Production Run

Once testing is complete, run the full pipeline:

### Step 1: Submit Production Job

```bash
sbatch slurm/run_pipeline.sh \
    --config config/production_saskatchewan.yaml \
    --total-items 16075 \
    --batch-size 1000 \
    --start-batch 1
```

**What This Does:**
- Runs orchestrator as a SLURM job (72-hour walltime)
- Processes 16,075 items in 17 batches of 1,000 items each
- For each batch:
  1. Downloads 1,000 PDFs (directly to `pdfs_sask_production/`)
  2. Submits olmOCR jobs and waits for completion
  3. Ingests OCR results into database
  4. Deletes PDFs to save space
- Repeats until all 16,075 items are processed

### Step 2: Monitor Progress

**Check SLURM job status:**
```bash
squeue -u $USER
```

**Watch logs in real-time:**
```bash
tail -f logs/pipeline_*.log
```

**Check database progress:**
```bash
sqlite3 /home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db \
    "SELECT batch_number, phase, status, items_processed
     FROM pipeline_runs
     ORDER BY batch_number DESC
     LIMIT 20;"
```

### Step 3: Resume if Needed

If the job fails or times out, check which batch it stopped at and resume:

```bash
# Check last completed batch
tail -100 logs/pipeline_*.log | grep "Batch.*completed"

# Resume from batch N (e.g., batch 8)
sbatch slurm/run_pipeline.sh \
    --config config/production_saskatchewan.yaml \
    --total-items 16075 \
    --batch-size 1000 \
    --start-batch 8
```

## Resource Requirements

- **Walltime**: ~4-5 hours per 1,000-item batch = ~3 days total for 16,075 items
- **Disk Space**: Max ~50GB (1,000 PDFs at peak, auto-deleted after each batch)
- **Memory**: 16GB (specified in SLURM script)
- **CPUs**: 4 (specified in SLURM script)

## Configuration Files

### Test Config: `config/test_saskatchewan.yaml`
- Small batch sizes (10 items)
- Manual confirmation for cleanup
- Debug logging
- Use for testing individual phases

### Production Config: `config/production_saskatchewan.yaml`
- Large batch sizes (1,000 items)
- Auto-cleanup enabled
- INFO logging
- Optimized for full run

## Troubleshooting

### Job Times Out
- Check which batch failed: `tail -100 logs/pipeline_*.log`
- Resume with `--start-batch N`
- May need to increase SLURM walltime for very large OCR jobs

### OCR Jobs Fail
- Check olmOCR logs in olmocr repo
- Verify PDFs are valid: Check `pdf_files` table in database
- May need to adjust `max_wait_hours` in config

### Disk Space Issues
- Cleanup is automatic, but check if it's running: `SELECT * FROM pipeline_runs WHERE phase='cleanup'`
- Manually clean if needed: `rm -rf pdfs_sask_production/*.pdf`

### Database Errors
- Check database isn't locked: `sqlite3 archive_tracking.db "PRAGMA busy_timeout=30000;"`
- Verify schema matches: Compare with existing working items

## Post-Processing

After completion, verify results:

```bash
# Count processed items
sqlite3 /home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db \
    "SELECT COUNT(*) FROM items WHERE collection LIKE '%saskatchewan%';"

# Check OCR completion
sqlite3 /home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db \
    "SELECT COUNT(DISTINCT p.identifier)
     FROM pdf_files p
     JOIN ocr_processing o ON p.id = o.pdf_file_id
     WHERE o.status = 'completed'
     AND p.subcollection = 'saskatchewan_1808_1946';"

# Expected: Both should be ~16,075
```

## Next Steps

Once Saskatchewan collection is complete:
1. Export data for analysis
2. Apply same approach to other collections
3. Scale up batch sizes if needed (e.g., 2,000 items per batch)
