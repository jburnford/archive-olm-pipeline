# Claude Navigation Guide - Nibi Cluster and File-Based Pipeline

## Quick Reference

### SSH Access
```bash
# From WSL
ssh nibi
```

**Note**: If SSH hangs, check for stale multiplexer:
```bash
ps aux | grep ssh
kill [PID if hanging]
```

## Current Job Monitoring

### Job Status
```bash
# Check running jobs
squeue -u jic823

# Job details
squeue -u jic823 -l

# Cancel a job
scancel [JOBID]
```

### Current File-Based Pipeline Job
- **Job ID**: 2690464
- **Config**: config/caribbean_filebased.yaml
- **Time Limit**: 144 hours (6 days)
- **Resources**: 16GB RAM, 4 CPUs

### Monitoring Commands

#### Quick Status Check
```bash
BASE="/home/jic823/projects/def-jic823/caribbean_pipeline"

echo "Downloaded: $(ls $BASE/01_downloaded/*.pdf 2>/dev/null | wc -l)"
echo "Pending OCR: $(ls $BASE/02_ocr_pending/*.pdf 2>/dev/null | wc -l)"
echo "Processing: $(ls -d $BASE/03_ocr_processing/batch_* 2>/dev/null | wc -l)"
echo "Completed: $(ls $BASE/04_ocr_completed/*.jsonl 2>/dev/null | wc -l)"
echo "Final: $(ls $BASE/05_processed/*.json 2>/dev/null | wc -l)"
```

#### View Pipeline Progress
```bash
# Download progress
cat $BASE/_manifests/download_progress.json | jq '.'

# OCR batches
cat $BASE/_manifests/batches.json | jq '.batches[] | {batch_id, total_pdfs, status}'

# Check for errors
find $BASE/99_errors -name "*.error.json" 2>/dev/null | wc -l
```

#### Watch Job Output
```bash
# Find the output file (in one of these locations)
ls -lth ~/projects/def-jic823/slurm-*.out | head -5
ls -lth ~/projects/def-jic823/cluster/slurm-*.out | head -5
ls -lth ~/slurm-*.out | head -5

# Watch output in real-time
tail -f ~/projects/def-jic823/slurm-2690464.out

# View recent output
tail -100 ~/projects/def-jic823/slurm-2690464.out
```

#### Check Specific PDFs
```bash
# List downloaded PDFs
ls -lth $BASE/01_downloaded/*.pdf | head -10

# View metadata for a specific PDF
cat $BASE/01_downloaded/[identifier].meta.json | jq '.'

# Check symlinks in pending
ls -lh $BASE/02_ocr_pending/*.pdf | head -10
```

#### Monitor Disk Space
```bash
# Check pipeline directory size
du -sh $BASE/*

# Check total space
df -h /home/jic823/projects/def-jic823
```

## File-Based Pipeline Architecture

### Directory Structure
```
caribbean_pipeline/
├── 01_downloaded/      # Downloaded PDFs + .meta.json files
├── 02_ocr_pending/     # Symlinks to PDFs ready for OCR
├── 03_ocr_processing/  # Active OCR batch directories
│   └── batch_NNNN/
│       ├── *.pdf       # PDFs being processed
│       ├── batch.meta.json
│       ├── results/    # OCR output (.jsonl)
│       └── logs/       # OCR logs
├── 04_ocr_completed/   # Split OCR results (one .json per page)
├── 05_processed/       # Final processed documents
├── 99_errors/          # Failed items with .error.json
└── _manifests/         # Progress tracking JSON files
```

### State Flow
```
Archive.org → 01_downloaded → 02_ocr_pending → 03_ocr_processing →
04_ocr_completed → 05_processed
```

### Key Processes

1. **Downloader** (`file_based_downloader.py`)
   - Downloads PDFs from Archive.org
   - Creates rich metadata JSON files
   - Symlinks to 02_ocr_pending/
   - Tracks progress in `_manifests/download_progress.json`

2. **Dispatcher** (`file_based_dispatcher.py`)
   - Monitors 02_ocr_pending/ for PDFs
   - Creates batches of 200 PDFs
   - Submits SLURM jobs to OLMoCR
   - Tracks batches in `_manifests/batches.json`

3. **Cleanup Worker** (`file_based_cleanup.py`)
   - Monitors completed OCR batches
   - Splits JSONL by identifier
   - Moves results to 04_ocr_completed/
   - Optionally deletes PDFs to save space

## Common Operations

### Restart Pipeline After Failure
```bash
cd ~/projects/def-jic823/archive-olm-pipeline

# Pull latest code
git pull

# Resubmit job (fully resumable)
CONFIG_FILE=config/caribbean_filebased.yaml sbatch streaming/run_filebased_pipeline.sh
```

### Manual Batch Submission
```bash
# If dispatcher isn't working, submit manually
cd ~/projects/def-jic823/olmocr
./smart_submit_pdf_jobs.sh --pdf-dir ~/projects/def-jic823/caribbean_pipeline/03_ocr_processing/batch_0001
```

### Check OLMoCR Job Status
```bash
# List all running OCR jobs
squeue -u jic823 | grep olmocr

# Check specific batch output
cat ~/projects/def-jic823/caribbean_pipeline/03_ocr_processing/batch_0001/logs/*.log
```

### Clean Up After Completion
```bash
# PDFs are auto-deleted by cleanup worker if auto_delete_pdfs: true in config
# To manually clean:
rm -rf $BASE/01_downloaded/*.pdf
rm -rf $BASE/03_ocr_processing/batch_*/chunks/*.pdf

# Keep metadata and results!
```

## Troubleshooting

### Dispatcher Not Creating Batches
```bash
# Check pending count
ls $BASE/02_ocr_pending/*.pdf | wc -l

# Should trigger at 200 PDFs (configurable in caribbean_filebased.yaml)

# Check dispatcher is running
ps aux | grep file_based_dispatcher

# Look for dispatcher output in SLURM log
grep "Dispatcher" ~/projects/def-jic823/slurm-2690464.out | tail -20
```

### Download Stalled
```bash
# Check downloader progress
cat $BASE/_manifests/download_progress.json | jq '.current_index, .total_downloaded'

# Check disk space (may pause if >90% full)
df -h /home/jic823/projects/def-jic823
```

### OCR Jobs Failing
```bash
# Check batch status
cat $BASE/_manifests/batches.json | jq '.batches[] | select(.status != "completed")'

# Check error logs
find $BASE/99_errors -name "*.error.json" -exec cat {} \;

# Check SLURM logs for OCR jobs
ls ~/projects/def-jic823/cluster/slurm-*.out | grep -v 2690464
```

### Broken Symlinks
```bash
# Find and remove broken symlinks
find $BASE/02_ocr_pending -xtype l -delete
```

## Key Configuration

### Pipeline Config: `config/caribbean_filebased.yaml`
```yaml
directories:
  base_dir: /home/jic823/projects/def-jic823/caribbean_pipeline

download:
  identifiers_file: caribbean_identifiers.json
  delay: 0.05
  collection: caribbean_collection

ocr:
  pdfs_per_batch: 200  # Trigger batch every 200 PDFs
  check_interval: 60   # Check every 60 seconds

cleanup:
  check_interval: 60
  auto_delete_pdfs: true  # Save space after OCR
```

## Performance Expectations

### Download Phase
- **Speed**: ~5-10 PDFs/minute (with 0.05s delay)
- **Metadata**: Full Archive.org metadata in JSON
- **Disk Usage**: ~40-50 MB per PDF average

### OCR Phase
- **Batch Size**: 200 PDFs
- **Processing Time**: ~1-2 hours per batch (varies by page count)
- **Output**: JSONL with page-level OCR data

### Expected Timeline (100K PDFs)
- **Download**: ~7-14 days
- **OCR**: Concurrent with download
- **Total**: ~2-3 weeks for complete pipeline

## Repository Locations

### Local (WSL)
- **Archive Pipeline**: `/home/jic823/archive-olm-pipeline`
- **Git Remote**: `git.cs.usask.ca:jic823/cluster.git`

### Nibi Cluster
- **Archive Pipeline**: `~/projects/def-jic823/archive-olm-pipeline`
- **Pipeline Data**: `~/projects/def-jic823/caribbean_pipeline`
- **OLMoCR**: `~/projects/def-jic823/olmocr/`
- **Cluster Scripts**: `~/projects/def-jic823/cluster/`

## Emergency Commands

### Kill Everything
```bash
# Cancel all jobs
scancel -u jic823

# Kill SSH multiplexer if hung
pkill -f "ssh.*nibi.*ControlMaster"
```

### Reset Pipeline (Nuclear Option)
```bash
# DANGER: Deletes all progress!
rm -rf ~/projects/def-jic823/caribbean_pipeline
./setup_filebased_pipeline.sh
# Pipeline will restart from beginning
```

## Success Indicators

### Healthy Pipeline Shows:
1. ✅ All three processes visible in SLURM output
2. ✅ PDFs accumulating in 01_downloaded/
3. ✅ Symlinks created in 02_ocr_pending/
4. ✅ Batches submitted when count reaches 200
5. ✅ OCR jobs appearing in squeue
6. ✅ Results appearing in 04_ocr_completed/

### Warning Signs:
- ⚠️ "No new PDFs in queue" for >5 minutes with 200+ PDFs pending
- ⚠️ No new downloads for >10 minutes
- ⚠️ Growing error count in 99_errors/
- ⚠️ Disk usage >90%

## Notes for Future Claude Sessions

- **Architecture**: File-based (NO SQLite) - everything tracked via JSON + directories
- **Resumability**: Fully resumable from any point via `_manifests/download_progress.json`
- **Simplification**: Dispatcher triggers every 200 PDFs (not page counting)
- **Why File-Based**: SQLite corrupted on NFS at 3.5GB, would reach 100GB+ at scale
- **Git Workflow**: Develop locally, push to GitLab, pull on Nibi, submit jobs
