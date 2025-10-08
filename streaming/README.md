# Streaming Pipeline Architecture

**Status:** Experimental - Under Development

This directory contains a new streaming pipeline design that runs download, OCR, and cleanup concurrently for maximum throughput.

## Architecture

Three independent processes run in parallel:

### 1. Continuous Downloader (`continuous_downloader.py`)
- Downloads PDFs non-stop from identifiers list
- Monitors disk space, pauses at 90% usage
- Resumes automatically when space freed
- Writes to: `download_queue/`

### 2. OCR Dispatcher (`ocr_dispatcher.py`)
- Monitors `download_queue/` for new PDFs
- Counts total pages in pending PDFs
- When ≥1000 pages ready:
  - Bundles PDFs into chunk
  - Submits to olmOCR GPU job
  - Moves to `ocr_processing/{job_id}/`

### 3. Cleanup Worker (`cleanup_worker.py`)
- Polls SLURM for completed OCR jobs
- When job completes:
  - Splits JSONL → individual JSON files
  - Ingests OCR data to database
  - Deletes source PDFs
  - Frees disk space for more downloads

## Key Differences from Batch Pipeline

| Aspect | Batch Pipeline | Streaming Pipeline |
|--------|---------------|-------------------|
| Processing | Sequential batches | Concurrent processes |
| Idle time | High (waiting for OCR) | Low (always working) |
| Batch size | Fixed (500 items) | Dynamic (≥1000 pages) |
| Disk usage | Bursty | Steady ~85-90% |
| Throughput | ~30-40 items/hour | ~50-70 items/hour (est) |

## Usage

```bash
sbatch streaming/run_streaming_pipeline.sh \
  --config config/test_saskatchewan.yaml \
  --start-from 0 \
  --max-items 10000
```

## State Management

- `download_queue/` - Downloaded PDFs awaiting OCR
- `ocr_processing/{job_id}/` - PDFs submitted to OCR
- `ocr_completed/{job_id}/` - Completed OCR results
- Database tracks overall progress

## Configuration

Uses same config files as batch pipeline (`config/*.yaml`).

## Safety Features

- Disk space monitoring with 90% threshold
- Database on local SSD (same as batch)
- Auto-cleanup prevents disk overflow
- State tracking prevents duplicate processing
