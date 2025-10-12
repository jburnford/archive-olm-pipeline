# Monitoring the Simplified Pipeline

## Current Batch Status

**Batch 0001**: 200 PDFs split into 31 chunks
- **Job IDs**: 2743796-2743826
- **Submitted**: 2025-10-11
- **Status**: All queued (pending)
- **Remaining**: 171 PDFs waiting for batch_0001 to complete

**Note**: Cannot submit remaining 171 PDFs until batch_0001 is finalized to avoid duplicates.

## Quick Status Checks

### Check SLURM Queue
```bash
# View all your jobs
squeue -u $USER

# Count running vs pending
squeue -u $USER | grep " R " | wc -l  # Running
squeue -u $USER | grep " PD " | wc -l  # Pending
```

### Check Pipeline Directories
```bash
BASE="/home/jic823/projects/def-jic823/caribbean_pipeline"

# Count files at each stage
echo "Downloaded PDFs: $(ls $BASE/01_downloaded/*.pdf 2>/dev/null | wc -l)"
echo "Processed items: $(ls $BASE/02_processed/ 2>/dev/null | wc -l)"
echo "Result JSONLs: $(ls $BASE/01_downloaded/results/results/*.jsonl 2>/dev/null | wc -l)"
echo "Split JSONs: $(ls $BASE/01_downloaded/results/json/*.json 2>/dev/null | wc -l)"
```

### Monitor Specific Chunk
```bash
# Watch a specific job's output (replace JOBID)
tail -f ~/projects/def-jic823/caribbean_pipeline/01_downloaded/slurm-2743796_batch_0001_chunk_001.out

# Check last 50 lines
tail -50 ~/projects/def-jic823/caribbean_pipeline/01_downloaded/slurm-2743796_batch_0001_chunk_001.out
```

### Check Results Directory
```bash
BASE="/home/jic823/projects/def-jic823/caribbean_pipeline"

# List recent JSONL results
ls -lth $BASE/01_downloaded/results/results/*.jsonl | head -10

# Count pages processed (requires results split)
find $BASE/01_downloaded/results/json -name "*.json" | wc -l
```

## Expected Timeline

### Phase 1: Queuing (Current)
- All 31 jobs waiting for H100 GPUs
- Status: `PD` (pending)
- Depends on cluster load

### Phase 2: Processing (1-3 hours per chunk)
- Jobs start running: Status changes to `R`
- OLMoCR processes PDFs
- Results written to `01_downloaded/results/results/*.jsonl`

### Phase 3: Splitting
Run manually or via orchestrator:
```bash
cd ~/projects/def-jic823/archive-olm-pipeline
python3 orchestration/split_jsonl_to_json.py \
  ~/projects/def-jic823/caribbean_pipeline/01_downloaded
```

### Phase 4: Finalization
```bash
cd ~/projects/def-jic823/archive-olm-pipeline
python3 streaming/simplified_finalizer.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --auto-delete-pdfs
```

## Troubleshooting

### No jobs running after hours
```bash
# Check job priority and reason
squeue -u $USER -l

# Common reasons:
# - Priority: Normal queueing
# - Resources: No H100 GPUs available
# - QOSMaxGRESPerUser: GPU quota reached
```

### Results not appearing
```bash
# Check if jobs are actually running
squeue -u $USER | grep " R "

# Check SLURM output for errors
tail -100 ~/projects/def-jic823/caribbean_pipeline/01_downloaded/slurm-*_batch_0001_chunk_001.out
```

### Job failed
```bash
# Check exit status
sacct -j JOBID --format=JobID,State,ExitCode,DerivedExitCode

# View full output
cat ~/projects/def-jic823/caribbean_pipeline/01_downloaded/slurm-JOBID_batch_0001_chunk_NNN.out
```

## Success Indicators

✅ Jobs transition from `PD` → `R` → completed
✅ JSONL files appear in `01_downloaded/results/results/`
✅ No error messages in SLURM output files
✅ Page counts match expectations

## Next Steps After Completion

1. **Verify all chunks completed**:
   ```bash
   ls ~/projects/def-jic823/caribbean_pipeline/01_downloaded/results/results/*.jsonl | wc -l
   # Should see multiple JSONL files
   ```

2. **Split results**:
   ```bash
   python3 orchestration/split_jsonl_to_json.py \
     ~/projects/def-jic823/caribbean_pipeline/01_downloaded
   ```

3. **Finalize and clean up**:
   ```bash
   python3 streaming/simplified_finalizer.py \
     --base-dir ~/projects/def-jic823/caribbean_pipeline \
     --auto-delete-pdfs
   ```

4. **Check processed count**:
   ```bash
   cat ~/projects/def-jic823/caribbean_pipeline/_manifests/processed_pdfs.json | jq '.count'
   # Should increase from 1776 → 1976
   ```

5. **Submit remaining 171 PDFs**:
   ```bash
   cd ~/projects/def-jic823/archive-olm-pipeline

   # This will now select the NEXT 200 unprocessed PDFs
   python3 streaming/simple_batch_submitter.py \
     --config config/caribbean_filebased.yaml \
     --batch-size 200

   # Expected: ~27 chunks for remaining 171 PDFs
   ```

## Batch Workflow

The simplified pipeline processes batches sequentially to avoid duplicates:

1. **Submit batch** → Creates chunk files and SLURM jobs
2. **Wait for completion** → All chunk jobs finish
3. **Split results** → JSONL → per-PDF JSON files
4. **Finalize** → Move to 02_processed/ and update tracker
5. **Submit next batch** → Now sees remaining unprocessed PDFs

**Important**: Don't submit a new batch until the previous one is finalized!
