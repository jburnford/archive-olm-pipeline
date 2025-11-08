# OLMoCR Batch Processing - Best Practices & Troubleshooting

This document provides best practices for using `olmocr/smart_process_pdf_chunks.slurm` to avoid common pitfalls that can cause jobs to silently process only a fraction of the intended PDFs.

## Critical Lessons Learned

### Problem: Job "Completes" in Minutes but Only Processes 1 PDF

**Symptoms:**
- Job completes with exit code 0 (success)
- Runtime is suspiciously short (2-5 minutes for what should be hours)
- Log shows "Completed pages: ~100" when you expected thousands
- olmocr reports "Found 1 total pdf paths to add" instead of N

**Root Causes:**

1. **Incorrect Argument Format** (Most Common)
2. **Filenames with Spaces in Chunk File**
3. **Quote Escaping Issues**

---

## Best Practice #1: Verify Chunk File Format

### ✅ Correct Format

Each PDF filename on **one line**, preserving spaces:
```
Document1.pdf
File with spaces.pdf
Another_Document.pdf
```

### ❌ Incorrect Format

Filenames split across lines (caused by improper ls usage):
```
File
with
spaces.pdf
```

### How to Create Chunk Files Correctly

**Method 1: Using find (Recommended)**
```bash
cd ~/projects/def-jic823/collection_name
find pdfs -name "*.pdf" -printf "%f\n" > batch_0001/chunks/chunk_0.txt
```

**Method 2: Using ls (with proper options)**
```bash
ls pdfs/*.pdf | xargs -n1 basename > batch_0001/chunks/chunk_0.txt
```

**Method 3: Multiple Chunks**
```bash
# Split into 3 chunks for parallel processing
find pdfs -name "*.pdf" -printf "%f\n" | split -n l/3 - batch_0001/chunks/chunk_
# Rename to chunk_0.txt, chunk_1.txt, chunk_2.txt
mv batch_0001/chunks/chunk_aa batch_0001/chunks/chunk_0.txt
mv batch_0001/chunks/chunk_ab batch_0001/chunks/chunk_1.txt
mv batch_0001/chunks/chunk_ac batch_0001/chunks/chunk_2.txt
```

### Verify Chunk File After Creation

```bash
# Count should match PDF count
wc -l batch_0001/chunks/chunk_0.txt
ls pdfs/*.pdf | wc -l

# Check for split filenames (should return nothing)
grep -E '^\(|^\[|^\.pdf$' batch_0001/chunks/chunk_0.txt

# View sample entries
head -10 batch_0001/chunks/chunk_0.txt
```

---

## Best Practice #2: Understanding olmocr Argument Format

### How olmocr Expects Arguments

The `--pdfs` flag uses argparse's `nargs='*'` which means:
```
--pdfs file1.pdf file2.pdf file3.pdf
```

**NOT** repeated flags:
```
--pdfs file1.pdf --pdfs file2.pdf --pdfs file3.pdf  # WRONG - only processes last file!
```

### How smart_process_pdf_chunks.slurm Handles This

The script builds arguments correctly:
```bash
PROCESS_ARGS=("--pdfs")  # Single flag
for pdf in "${PDF_FILENAMES[@]}"; do
    PROCESS_ARGS+=("$PDF_DIR/$pdf")  # Add each path
done

# Expands to: --pdfs path1 path2 path3 ...
python -m olmocr.pipeline "$OUTPUT_DIR" $OLMOCR_FLAGS "${PROCESS_ARGS[@]}"
```

### What NOT to Do

❌ **Don't use eval with quoted strings**
```bash
# BREAKS with multiple PDFs
ARGS="--pdfs \"$file1\" --pdfs \"$file2\""
eval "python -m olmocr.pipeline $ARGS"
```

❌ **Don't repeat --pdfs flag**
```bash
# Only processes last PDF
ARGS+=("--pdfs" "$file1" "--pdfs" "$file2")
```

---

## Best Practice #3: Pre-Flight Checklist

Before submitting an OLMoCR job, run these checks:

### 1. Verify PDF Count
```bash
PDF_DIR=~/projects/def-jic823/collection_name/pdfs
CHUNK_FILE=~/projects/def-jic823/collection_name/batch_0001/chunks/chunk_0.txt

echo "PDFs in directory: $(ls $PDF_DIR/*.pdf 2>/dev/null | wc -l)"
echo "Entries in chunk file: $(wc -l < $CHUNK_FILE)"
```

These numbers should match (or chunk file should be a subset).

### 2. Check for Problematic Filenames
```bash
# Find PDFs with spaces
find $PDF_DIR -name "* *.pdf" -exec basename {} \;

# Verify they appear correctly in chunk file (one per line)
grep " " $CHUNK_FILE
```

### 3. Calculate Expected Runtime
```bash
# Count total pages across all PDFs
for pdf in $PDF_DIR/*.pdf; do
    pdfinfo "$pdf" 2>/dev/null | grep "^Pages:" | awk '{print $2}'
done | awk '{sum += $1} END {print "Total pages:", sum}'

# At ~0.4-2.5 pages/second, estimate runtime
# 2500 pages ÷ 1.5 pages/sec = ~28 minutes
```

If your job completes in <5 minutes and you expected 30+ minutes, something went wrong.

### 4. Test with Small Subset First
```bash
# Create test chunk with 3 PDFs
head -3 batch_0001/chunks/chunk_0.txt > batch_test/chunks/chunk_0.txt

# Submit test job
sbatch --time=00:30:00 --array=0 \
  --export=ALL,PDF_DIR=$PDF_DIR,BATCH_DIR=$PWD/batch_test \
  olmocr/smart_process_pdf_chunks.slurm
```

---

## Best Practice #4: Monitor Job Progress

### During Execution

```bash
# Watch live output
tail -f ~/projects/def-jic823/collection_name/job_name-JOBID.out

# Look for this line early in the log:
grep "Found.*total pdf paths to add" job-JOBID.out
# Should show: "Found N total pdf paths to add" where N = your chunk file line count
```

### Key Indicators of Success

✅ **Good Signs:**
```
Processing 32 PDFs from /path/to/pdfs
Number of PDFs in args: 32
Found 32 total pdf paths to add
Calculated items_per_group: 6 based on average pages per PDF: 81.12
```

❌ **Warning Signs:**
```
Processing 32 PDFs from /path/to/pdfs  # Shell says 32...
Number of PDFs in args: 32              # Shell built 32 args...
Found 1 total pdf paths to add          # But olmocr only saw 1! ⚠️
```

### After Completion

```bash
BATCH_DIR=~/projects/def-jic823/collection_name/batch_0001

# Check processed count
wc -l $BATCH_DIR/processed_files.log

# Should equal chunk file count
wc -l $BATCH_DIR/chunks/chunk_0.txt

# Check for results
ls -lh $BATCH_DIR/results/results/*.jsonl
```

---

## Best Practice #5: Handling Job Failures

### If Job Processes Wrong Number of PDFs

1. **Check olmocr Detection**
```bash
grep "Found.*total pdf" slurm-JOBID.out
```

If this shows 1 but you expected 32, the chunk file likely has issues.

2. **Recreate Chunk File**
```bash
cd ~/projects/def-jic823/collection_name
rm batch_0001/chunks/chunk_0.txt
find pdfs -name "*.pdf" -printf "%f\n" > batch_0001/chunks/chunk_0.txt
```

3. **Clean and Resubmit**
```bash
rm -rf batch_0001/results/*
rm batch_0001/processed_files.log
sbatch --array=0 --export=ALL,PDF_DIR=$PWD/pdfs,BATCH_DIR=$PWD/batch_0001 \
  olmocr/smart_process_pdf_chunks.slurm
```

### If Job Crashes with ValueError

**Error:** `ValueError: pdfs argument needs to be either a local path, an s3 path, or an s3 glob pattern...`

**Cause:** Chunk file contains invalid entries (e.g., `"(1).pdf"` on its own line)

**Fix:** Recreate chunk file using find/basename method above

### If Some Pages Fail to Process

**Normal:** A few retries due to model issues
```
WARNING - ValueError on attempt 0 for file.pdf-15: Response did not finish with reason code 'stop'
```

olmocr will retry these pages (up to max_retries, default 3).

**Concerning:** >10% failure rate suggests memory/GPU issues
```bash
# Check final stats
grep "Page Failure rate" slurm-JOBID.out
```

---

## Example: Correct Workflow Start to Finish

```bash
# 1. Set up directory structure
COLLECTION=~/projects/def-jic823/my_collection
mkdir -p $COLLECTION/{pdfs,batch_0001/{chunks,results}}

# 2. Upload/copy PDFs to pdfs/
# (assume PDFs are already there)

# 3. Create chunk file correctly
cd $COLLECTION
find pdfs -name "*.pdf" -printf "%f\n" > batch_0001/chunks/chunk_0.txt

# 4. Verify setup
echo "PDF count: $(ls pdfs/*.pdf | wc -l)"
echo "Chunk count: $(wc -l < batch_0001/chunks/chunk_0.txt)"
echo "Sample entries:"
head -5 batch_0001/chunks/chunk_0.txt

# 5. Estimate pages and runtime
echo "Estimating total pages..."
for pdf in pdfs/*.pdf; do
    pdfinfo "$pdf" 2>/dev/null | grep "^Pages:" | awk '{print $2}'
done | awk '{sum += $1} END {printf "Total pages: %d\nEst. time at 1.5 pg/s: %.1f min\n", sum, sum/1.5/60}'

# 6. Submit job
cd ~/projects/def-jic823/archive-olm-pipeline
sbatch --job-name=my_collection \
  --time=04:00:00 \
  --output=$COLLECTION/ocr-%j.out \
  --array=0 \
  --export=ALL,PDF_DIR=$COLLECTION/pdfs,BATCH_DIR=$COLLECTION/batch_0001 \
  olmocr/smart_process_pdf_chunks.slurm

# 7. Monitor
tail -f $COLLECTION/ocr-JOBID.out

# 8. Watch for "Found N total pdf paths" in first minute
# If N != expected count, cancel and debug:
# scancel JOBID
```

---

## Debugging Reference

### Quick Diagnosis

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| "Found 1 total pdf paths" but expected 32 | Chunk file has split filenames | Recreate with `find -printf "%f\n"` |
| Job completes in 2 min instead of 30 min | Only processed 1 PDF | Check "Found N total pdf paths" line |
| ValueError about pdf paths | Invalid entry in chunk file | Recreate chunk file |
| Exit code 0 but no results | Script thinks it succeeded but olmocr failed silently | Check for errors in job log |

### Useful Grep Patterns

```bash
LOG=~/projects/def-jic823/collection/slurm-JOBID.out

# How many PDFs olmocr actually loaded
grep "Found.*total pdf" $LOG

# Final statistics
grep -A20 "FINAL METRICS" $LOG

# Failure rate
grep "Page Failure rate" $LOG

# Pages completed
grep "Completed pages:" $LOG

# Processing rate
grep "tokens/sec rate" $LOG
```

---

## When to Use This Script vs. Custom Scripts

### ✅ Use `smart_process_pdf_chunks.slurm` When:
- Processing 10-1000+ PDFs in one or more batches
- PDFs are locally available on Nibi filesystem
- You want automatic retry logic and progress tracking
- You need to split work across array jobs

### ❌ Create Custom Script When:
- Processing a single PDF (use direct olmocr command)
- PDFs are in S3 (use olmocr's native S3 support)
- Highly custom preprocessing needed per PDF
- Different OCR settings per document type

---

## Advanced: Parallel Processing with Array Jobs

For large collections (1000+ PDFs), split into multiple chunks:

```bash
# Split 1000 PDFs into 10 chunks of ~100 each
cd ~/projects/def-jic823/large_collection
find pdfs -name "*.pdf" -printf "%f\n" | split -n l/10 - batch_0001/chunks/chunk_

# Rename to chunk_0.txt through chunk_9.txt
i=0
for f in batch_0001/chunks/chunk_*; do
    mv "$f" "batch_0001/chunks/chunk_${i}.txt"
    ((i++))
done

# Submit as array job (10 parallel tasks)
sbatch --array=0-9 \
  --export=ALL,PDF_DIR=$PWD/pdfs,BATCH_DIR=$PWD/batch_0001 \
  olmocr/smart_process_pdf_chunks.slurm
```

Each array task processes its own chunk independently.

---

## Summary: The Golden Rules

1. **Always use `find -printf "%f\n"` to create chunk files** - handles spaces correctly
2. **Verify "Found N total pdf paths" matches expected count** - catch issues early
3. **Test with 3-5 PDFs first** - validate workflow before full batch
4. **Check runtime expectations** - if it's too fast, something failed silently
5. **Never use eval or quote manipulation** - bash arrays handle everything
6. **One --pdfs flag, multiple paths** - not multiple --pdfs flags

---

## Getting Help

If you encounter issues not covered here:

1. Check job log for "Found N total pdf paths" - this is the smoking gun
2. Verify chunk file format: `cat -A chunk_0.txt | head -20`
3. Count pages manually: `pdfinfo` on all PDFs
4. Review recent commits to `smart_process_pdf_chunks.slurm` - may have fixes
5. Test with single PDF first to isolate olmocr vs. script issues

---

**Document Version:** 1.0
**Last Updated:** 2025-11-07
**Based on:** Sarah496 debugging session (Jobs 4055141, 4055429, 4058470, 4064443, 4068720)
