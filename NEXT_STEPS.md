# Next Steps - Ready to Test!

## ✅ What's Been Done

### Code Implemented
- ✅ Simplified 2-directory architecture (01_downloaded → 02_processed)
- ✅ Smart backpressure (pauses downloads when backlog > 500 PDFs)
- ✅ PDF tracking system (processed_pdfs.json)
- ✅ Batch submitter with unprocessed filtering
- ✅ Finalizer with auto-PDF deletion
- ✅ Orchestrator for continuous operation
- ✅ All helper scripts and tools
- ✅ Comprehensive documentation

### Changes Made
1. **Removed symlinks** from downloader
2. **Updated paths** in DuckDB exporter (05_processed → 02_processed)
3. **Created 7 new scripts** for simplified workflow
4. **Wrote 5 documentation files** with testing procedures

### Git Status
- ✅ Committed: `feat: simplified 2-directory pipeline with smart backpressure`
- ✅ Pushed to GitLab
- 📍 Ready to pull on Nibi

## 🚀 Next: Test on Nibi

### Step 1: Pull Code (2 minutes)

```bash
# SSH to Nibi
ssh nibi

# Navigate to repo
cd ~/projects/def-jic823/archive-olm-pipeline

# Pull latest code
git pull

# Verify new files
ls streaming/simple_*.py tools/extract_*.py
```

### Step 2: One-Time Migration (30 minutes)

```bash
cd ~/projects/def-jic823/caribbean_pipeline

# Rename directory
mv 05_processed 02_processed

# Extract processed IDs
cd ~/projects/def-jic823/archive-olm-pipeline
python3 tools/extract_processed_ids.py \
  --processed-dir ~/projects/def-jic823/caribbean_pipeline/02_processed \
  --output ~/projects/def-jic823/caribbean_pipeline/_manifests/processed_pdfs.json

# Clean up old structure
cd ~/projects/def-jic823/caribbean_pipeline
rm -rf 02_ocr_pending 03_ocr_processing 04_ocr_completed

# Optional: Delete already-processed PDFs to free space
cd ~/projects/def-jic823/archive-olm-pipeline
python3 tools/cleanup_processed_pdfs.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --dry-run  # Remove --dry-run to actually delete
```

### Step 3: Test Batch Submission (1-2 hours)

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

# Check status
python3 streaming/simple_batch_submitter.py \
  --config config/caribbean_filebased.yaml \
  --dry-run

# Submit test batch of 200 PDFs (will be split into ~31 chunks of ≤1500 pages each)
python3 streaming/simple_batch_submitter.py \
  --config config/caribbean_filebased.yaml \
  --batch-size 200

# Monitor all chunk jobs
squeue -u $USER

# Expected: 31 separate jobs, each processing ≤1500 pages
```

### Step 4: Check Where OLMoCR Results Go

**This is the key test you mentioned!**

After OLMoCR job completes:

```bash
BASE=~/projects/def-jic823/caribbean_pipeline

# Check results location
ls $BASE/01_downloaded/results/results/*.jsonl

# Should see JSONL files created by OLMoCR
```

Expected path: `01_downloaded/results/results/*.jsonl`

### Step 5: Split and Finalize

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

# Split JSONL into per-PDF JSON files
python3 orchestration/split_jsonl_to_json.py \
  ~/projects/def-jic823/caribbean_pipeline/01_downloaded

# Check split results
ls ~/projects/def-jic823/caribbean_pipeline/01_downloaded/results/json/*.json | wc -l

# Finalize (move to 02_processed and delete PDFs)
python3 streaming/simplified_finalizer.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --auto-delete-pdfs

# Verify
ls ~/projects/def-jic823/caribbean_pipeline/02_processed/ | wc -l
```

## 📋 Testing Checklist

Use this to verify everything works:

- [ ] Code pulled from GitLab
- [ ] 05_processed renamed to 02_processed
- [ ] processed_pdfs.json created with 1776 items
- [ ] Batch submitter shows correct unprocessed count
- [ ] Test batch submitted successfully
- [ ] OLMoCR job appears in queue
- [ ] Results appear in `01_downloaded/results/results/*.jsonl`
- [ ] Splitter creates files in `01_downloaded/results/json/`
- [ ] Finalizer moves files to `02_processed/{identifier}/`
- [ ] PDFs deleted from `01_downloaded/` after finalization
- [ ] processed_pdfs.json count increases
- [ ] DuckDB export works

## 📚 Documentation Quick Links

- **QUICK_START.md** - TL;DR commands
- **TESTING_GUIDE.md** - Detailed step-by-step testing
- **SIMPLIFIED_PIPELINE_PLAN.md** - Architecture overview
- **COMPONENT_ADAPTATION_GUIDE.md** - What changed in the code
- **IMPLEMENTATION_SUMMARY.md** - Migration analysis

## 🎯 Key Features to Test

### 1. Page-based Chunking

Each batch of 200 PDFs is split into chunks of ≤1500 pages:

- **Why**: Smaller jobs queue faster in SLURM and are more stable
- **How**: Dynamic packing algorithm groups PDFs by page count
- **Result**: 200 PDFs → ~31 chunks (varies by document size)
- **Walltime**: Calculated per chunk: 300s startup + 6s/page + 20% buffer

### 2. Smart Backpressure

The batch submitter will report status:

- `OK (372/500)` - Continue normally
- `APPROACHING_LIMIT (450/500)` - Getting close
- `HIGH_BACKLOG (550/500)` - Pause downloads (exit code 2)

### 3. PDF Deletion

After finalization, PDFs should be deleted from `01_downloaded/`:

```bash
# Before finalization
ls ~/projects/def-jic823/caribbean_pipeline/01_downloaded/*.pdf | wc -l
# Should show 372 (or less if cleanup ran)

# After finalization
ls ~/projects/def-jic823/caribbean_pipeline/01_downloaded/*.pdf | wc -l
# Should decrease by ~200
```

### 4. Tracking Updates

```bash
# Check tracking
cat ~/projects/def-jic823/caribbean_pipeline/_manifests/processed_pdfs.json | jq '.count'

# Should increase as items are finalized
```

## 🐛 What If Something Breaks?

### Results in wrong location

**Check:**
```bash
find ~/projects/def-jic823/caribbean_pipeline -name "*.jsonl" -o -name "*.json" | grep -v meta.json
```

This will show where all result files are.

### Can't find unprocessed PDFs

**Debug:**
```bash
cd ~/projects/def-jic823/archive-olm-pipeline

python3 -c "
import json
from pathlib import Path

base = Path('~/projects/def-jic823/caribbean_pipeline').expanduser()
tracker_file = base / '_manifests' / 'processed_pdfs.json'
pdfs = list((base / '01_downloaded').glob('*.pdf'))

with open(tracker_file) as f:
    processed = set(json.load(f)['processed'])

print(f'Total PDFs: {len(pdfs)}')
print(f'Processed: {len(processed)}')
print(f'Unprocessed: {len([p for p in pdfs if p.stem not in processed])}')
"
```

### OLMoCR job fails

**Check SLURM output:**
```bash
tail -100 ~/projects/def-jic823/caribbean_pipeline/slurm-*.out
```

Common issues:
- Container not found → Check `~/projects/def-jic823/olmocr/olmocr.sif`
- Out of memory → Reduce batch size
- Timeout → Increase walltime estimate

## 📞 Report Back

After testing, let me know:

1. ✅ Where OLMoCR results ended up
2. ✅ Whether splitter found them correctly
3. ✅ Whether finalizer worked
4. ✅ Whether PDFs got deleted
5. ✅ Any errors encountered

This will help tune the pipeline for production!

## 🎉 Success Looks Like

```bash
$ ls ~/projects/def-jic823/caribbean_pipeline/
01_downloaded/  02_processed/  _manifests/  99_errors/  export/

$ ls 01_downloaded/
chunks/  results/  *.meta.json  (few PDFs)

$ ls 01_downloaded/results/
json/  results/

$ ls 02_processed/ | wc -l
1976  # 1776 + 200 newly processed

$ cat _manifests/processed_pdfs.json | jq '.count'
1976
```

## 🚀 After Testing Works

Deploy the orchestrator for continuous operation:

```bash
cd ~/projects/def-jic823/archive-olm-pipeline
sbatch streaming/run_simplified_pipeline.sh
```

This will:
- Check for new batches every 60 seconds
- Submit when 200+ unprocessed PDFs available
- Split results as they arrive
- Finalize and delete PDFs
- Apply backpressure when needed
- Export catalog every 10 iterations

---

**Ready to test! 🚀**

Pull the code on Nibi and start with the migration steps above.
