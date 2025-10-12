# Quick Start - Simplified Pipeline

## TL;DR

```bash
# On Nibi cluster

# 1. One-time migration
cd ~/projects/def-jic823/caribbean_pipeline
mv 05_processed 02_processed
rm -rf 02_ocr_pending 03_ocr_processing 04_ocr_completed

cd ~/projects/def-jic823/archive-olm-pipeline
python3 tools/extract_processed_ids.py \
  --processed-dir ~/projects/def-jic823/caribbean_pipeline/02_processed \
  --output ~/projects/def-jic823/caribbean_pipeline/_manifests/processed_pdfs.json

# 2. Test with existing PDFs
python3 streaming/simple_batch_submitter.py \
  --config config/caribbean_filebased.yaml \
  --batch-size 200

# 3. Wait for OLMoCR, then split
python3 orchestration/split_jsonl_to_json.py \
  ~/projects/def-jic823/caribbean_pipeline/01_downloaded

# 4. Finalize
python3 streaming/simplified_finalizer.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --auto-delete-pdfs

# Done!
```

## What Changed

### Before (5 directories)
```
01_downloaded → 02_ocr_pending → 03_ocr_processing → 04_ocr_completed → 05_processed
```
- Symlinks everywhere
- Complex coordination
- PDFs not deleted
- Hard to track state

### After (2 directories)
```
01_downloaded → 02_processed
```
- No symlinks
- Simple tracking via JSON
- PDFs auto-deleted
- Clear workflow

## Key Components

| Script | Purpose | When to Run |
|--------|---------|-------------|
| `simple_batch_submitter.py` | Submit batches to OLMoCR | Every 200 unprocessed PDFs |
| `split_jsonl_to_json.py` | Split OLMoCR results | After OLMoCR completes |
| `simplified_finalizer.py` | Move to processed, delete PDFs | After splitting |
| `simple_orchestrator.py` | Coordinate everything | Continuous operation |

## Smart Backpressure

The system automatically manages download/OCR balance:

- **< 200 unprocessed PDFs**: Continue downloading
- **200-400 unprocessed**: Can still download
- **400-500 unprocessed**: Warning zone
- **> 500 unprocessed**: Pause downloads, wait for OCR

Exit codes from `simple_batch_submitter.py`:
- `0` = success
- `1` = error
- `2` = backpressure active (pause downloads)

## Testing

Use the existing 372 PDFs:

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

# Interactive test
bash tools/test_pipeline.sh

# Or follow TESTING_GUIDE.md for detailed steps
```

## Production

```bash
cd ~/projects/def-jic823/archive-olm-pipeline

# Submit orchestrator
sbatch streaming/run_simplified_pipeline.sh

# Monitor
squeue -u $USER
tail -f ~/projects/def-jic823/slurm-*.out
```

## File Locations (Nibi)

```
~/projects/def-jic823/
├── archive-olm-pipeline/           # Code repository
│   ├── streaming/                  # Pipeline scripts
│   ├── orchestration/              # Splitter
│   └── tools/                      # Helper scripts
│
└── caribbean_pipeline/             # Data directory
    ├── 01_downloaded/              # PDFs + metadata + results
    │   ├── *.pdf                   # Downloaded PDFs
    │   ├── *.meta.json             # Metadata
    │   └── results/
    │       ├── results/*.jsonl     # OLMoCR raw output
    │       └── json/*.json         # Split per-PDF files
    │
    ├── 02_processed/               # Final results
    │   └── {identifier}/
    │       ├── {id}.meta.json
    │       └── {pdfname}.json
    │
    ├── _manifests/
    │   ├── processed_pdfs.json     # Tracking
    │   └── download_progress.json
    │
    └── export/
        └── catalog.duckdb          # Local analysis
```

## Troubleshooting

### Batch submitter says "no metadata"
- Check `01_downloaded/*.meta.json` files exist
- Verify filenames match between PDFs and metadata

### OLMoCR job fails
- Check SLURM output: `tail -100 ~/projects/def-jic823/caribbean_pipeline/slurm-*.out`
- Verify OLMoCR container exists: `ls ~/projects/def-jic823/olmocr/olmocr.sif`

### Finalizer finds no files
- Check `01_downloaded/results/json/` has JSON files
- Run splitter first if results only has JSONL

### Backpressure always active
- Run finalizer to clear backlog
- Check processed count is updating: `cat _manifests/processed_pdfs.json | jq '.count'`

## Documentation

- **SIMPLIFIED_PIPELINE_PLAN.md** - Architecture and design
- **COMPONENT_ADAPTATION_GUIDE.md** - Code changes made
- **IMPLEMENTATION_SUMMARY.md** - Migration steps and benefits
- **TESTING_GUIDE.md** - Detailed testing procedure
- **QUICK_START.md** - This file

## Support

Check these if stuck:
1. SLURM output files (`slurm-*.out`)
2. `_manifests/` directory
3. `99_errors/` directory
4. Recent git commits for changes
