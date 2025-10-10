# File-Based Streaming Pipeline

## Overview

The file-based pipeline eliminates SQLite database dependency, tracking all state through JSON files and directory structure. This solves the problems of:
- ❌ Database corruption from NFS issues
- ❌ Multi-GB database copying on every job
- ❌ SQLite locking with concurrent access
- ✅ Simple, resilient file operations
- ✅ Easy to inspect, debug, and resume
- ✅ Scales to millions of documents

## Directory Structure

```
caribbean_pipeline/
├── 01_downloaded/           # Downloaded PDFs + metadata
│   ├── identifier1.pdf
│   ├── identifier1.meta.json
│   └── ...
│
├── 02_ocr_pending/          # Symlinks to PDFs ready for OCR
│   ├── identifier1.pdf -> ../01_downloaded/identifier1.pdf
│   └── ...
│
├── 03_ocr_processing/       # Active OCR batches
│   ├── batch_0001/
│   │   ├── identifier1.pdf
│   │   ├── identifier2.pdf
│   │   ├── batch.meta.json
│   │   ├── results/
│   │   │   └── combined.ocr.jsonl
│   │   └── logs/
│   └── batch_0002/
│
├── 04_ocr_completed/        # OCR results split by identifier
│   ├── identifier1.ocr.jsonl
│   ├── identifier1.meta.json
│   └── ...
│
├── 05_processed/            # Final processed documents
│   ├── identifier1.json
│   └── ...
│
├── 99_errors/               # Failed items
│   ├── download_failed/
│   ├── ocr_failed/
│   └── processing_failed/
│
└── _manifests/              # Collection-level tracking
    ├── download_progress.json
    ├── batches.json
    └── collection.json
```

## Quick Start

### 1. Setup

```bash
cd /home/jic823/archive-olm-pipeline

# Create directory structure
./setup_filebased_pipeline.sh
```

### 2. Run Pipeline

```bash
# On Nibi cluster
cd ~/projects/def-jic823/archive-olm-pipeline

# Submit job
CONFIG_FILE=config/caribbean_filebased.yaml sbatch streaming/run_filebased_pipeline.sh
```

### 3. Monitor Progress

```bash
# Check job status
squeue -u $USER

# View job output
tail -f slurm-XXXXXX.out

# Count items at each stage
BASE="/home/jic823/projects/def-jic823/caribbean_pipeline"

echo "Downloaded: $(ls $BASE/01_downloaded/*.pdf 2>/dev/null | wc -l)"
echo "Pending OCR: $(ls $BASE/02_ocr_pending/*.pdf 2>/dev/null | wc -l)"
echo "Processing batches: $(ls -d $BASE/03_ocr_processing/batch_* 2>/dev/null | wc -l)"
echo "Completed: $(ls $BASE/04_ocr_completed/*.jsonl 2>/dev/null | wc -l)"

# View progress manifest
cat $BASE/_manifests/download_progress.json | jq '.'

# View batch registry
cat $BASE/_manifests/batches.json | jq '.batches[] | {batch_id, status, total_pdfs, total_pages}'
```

## Pipeline Components

### 1. File-Based Downloader
**Script:** `streaming/file_based_downloader.py`

**What it does:**
- Downloads PDFs from Archive.org
- Saves metadata to `01_downloaded/{identifier}.meta.json`
- Creates symlinks in `02_ocr_pending/`
- Tracks progress in `_manifests/download_progress.json`
- Pauses at 90% disk usage
- Saves errors to `99_errors/download_failed/`

**Metadata Example:**
```json
{
  "identifier": "historyofjamaica01long",
  "collection": "caribbean_collection",
  "title": "The History of Jamaica",
  "downloaded_at": "2025-10-10T03:00:00Z",
  "file_size": 45678901,
  "source_url": "https://archive.org/details/historyofjamaica01long"
}
```

### 2. File-Based OCR Dispatcher
**Script:** `streaming/file_based_dispatcher.py`

**What it does:**
- Monitors `02_ocr_pending/` for PDFs
- Counts pages using PyPDF2
- Bundles into ~1,500 page batches
- Moves PDFs to `03_ocr_processing/batch_XXXX/`
- Submits SLURM jobs to OLMoCR
- Updates `_manifests/batches.json`

**Batch Metadata Example:**
```json
{
  "batch_id": "batch_0001",
  "slurm_job_id": "2682150",
  "submitted_at": "2025-10-10T03:05:00Z",
  "total_pdfs": 15,
  "total_pages": 1523,
  "identifiers": ["id1", "id2", ...],
  "status": "submitted"
}
```

### 3. File-Based Cleanup Worker
**Script:** `streaming/file_based_cleanup.py`

**What it does:**
- Monitors `03_ocr_processing/` for completed batches
- Checks SLURM job status
- Splits combined JSONL by identifier
- Moves results to `04_ocr_completed/`
- Deletes PDFs to save space
- Updates batch status to "completed"

**OCR Metadata Example:**
```json
{
  "identifier": "historyofjamaica01long",
  "batch_id": "batch_0001",
  "ocr_completed_at": "2025-10-10T03:45:00Z",
  "total_pages": 234,
  "ocr_file": "04_ocr_completed/historyofjamaica01long.ocr.jsonl"
}
```

### 4. File-Based Orchestrator
**Script:** `streaming/file_based_orchestrator.py`

**What it does:**
- Launches all three components with unbuffered output
- Monitors for process failures
- Coordinates graceful shutdown

## Advantages Over SQLite

| Feature | SQLite + NFS | File-Based |
|---------|--------------|------------|
| **Corruption Risk** | High (NFS caching issues) | None (atomic file ops) |
| **Concurrent Access** | Lock contention | No locks needed |
| **Scalability** | 100GB+ database | Scales linearly |
| **Resume After Failure** | Complex state recovery | Simple: scan directories |
| **Debugging** | SQL queries needed | `ls`, `cat`, `jq` |
| **Backup** | Copy entire database | Copy specific files |
| **Migration** | Schema changes needed | Add new fields to JSON |

## Monitoring Commands

```bash
BASE="/home/jic823/projects/def-jic823/caribbean_pipeline"

# Real-time download progress
watch -n 5 "cat $BASE/_manifests/download_progress.json | jq '{current_index, stats}'"

# Check for errors
find $BASE/99_errors -name "*.error.json" | wc -l
cat $BASE/99_errors/download_failed/*.error.json | jq '{identifier, error_message}' | head -20

# List active batches
ls -lh $BASE/03_ocr_processing/

# Check batch status
for batch in $BASE/03_ocr_processing/batch_*; do
    echo "=== $(basename $batch) ==="
    cat $batch/batch.meta.json | jq '{status, total_pdfs, slurm_job_id}'
done

# Count completed OCR
find $BASE/04_ocr_completed -name "*.ocr.jsonl" | wc -l

# Disk usage
du -sh $BASE/{01_downloaded,02_ocr_pending,03_ocr_processing,04_ocr_completed}
```

## Resuming After Interruption

The pipeline is fully resumable:

```bash
# Check where you left off
cat $BASE/_manifests/download_progress.json | jq '.current_index'

# Resubmit job - it will continue from last checkpoint
CONFIG_FILE=config/caribbean_filebased.yaml sbatch streaming/run_filebased_pipeline.sh
```

## Cleanup and Archiving

```bash
# Archive completed batches
for batch in $BASE/03_ocr_processing/batch_*; do
    if grep -q '"status": "completed"' $batch/batch.meta.json 2>/dev/null; then
        tar -czf $(basename $batch).tar.gz -C $BASE/03_ocr_processing $(basename $batch)
        echo "Archived: $(basename $batch)"
    fi
done

# Delete completed batch PDFs (already done automatically)
# Results are preserved in 04_ocr_completed/
```

## Troubleshooting

### Dispatcher not creating batches?
```bash
# Check pending PDFs
ls $BASE/02_ocr_pending/*.pdf | wc -l

# Check if symlinks are broken
for f in $BASE/02_ocr_pending/*.pdf; do
    [ -e "$f" ] || echo "Broken: $f"
done

# Manually check page counts
python3 -c "
from PyPDF2 import PdfReader
from pathlib import Path
for p in Path('$BASE/02_ocr_pending').glob('*.pdf'):
    try:
        pages = len(PdfReader(p).pages)
        print(f'{p.name}: {pages} pages')
    except:
        print(f'{p.name}: ERROR')
"
```

### OLMoCR jobs failing?
```bash
# Check batch logs
tail -100 $BASE/03_ocr_processing/batch_0001/logs/*.out

# Check SLURM job status
BATCH_META="$BASE/03_ocr_processing/batch_0001/batch.meta.json"
JOB_ID=$(jq -r '.slurm_job_id' $BATCH_META)
sacct -j $JOB_ID --format=JobID,State,ExitCode,Elapsed
```

### Cleanup worker not processing?
```bash
# Check batch statuses
jq -r '.batches[] | "\(.batch_id): \(.status)"' $BASE/_manifests/batches.json

# Manually trigger cleanup for a batch
python3 streaming/file_based_cleanup.py \
    --base-dir $BASE \
    --check-interval 10
```

## Migration from SQLite

If you have existing data in SQLite:

```bash
# Export download metadata
python3 << 'EOF'
import sqlite3
import json
from pathlib import Path

conn = sqlite3.connect('/path/to/archive_tracking.db')
conn.row_factory = sqlite3.Row

base_dir = Path('/home/jic823/projects/def-jic823/caribbean_pipeline')
downloaded_dir = base_dir / '01_downloaded'
downloaded_dir.mkdir(parents=True, exist_ok=True)

for row in conn.execute('SELECT * FROM pdf_files WHERE download_status="downloaded"'):
    identifier = row['identifier']

    metadata = {
        'identifier': identifier,
        'downloaded_at': row['download_date'],
        'file_size': row['filesize'],
        'filename': row['filename']
    }

    meta_file = downloaded_dir / f"{identifier}.meta.json"
    with open(meta_file, 'w') as f:
        json.dump(metadata, f, indent=2)

print(f"Exported {conn.execute('SELECT COUNT(*) FROM pdf_files').fetchone()[0]} records")
EOF
```

## Configuration

Edit `config/caribbean_filebased.yaml`:

```yaml
directories:
  base_dir: /home/jic823/projects/def-jic823/caribbean_pipeline

download:
  identifiers_file: /path/to/caribbean_identifiers.json
  collection: caribbean_collection
  delay: 0.05

ocr:
  max_pages_per_chunk: 1500  # Adjust based on performance

slurm:
  time_limit: "144:00:00"  # 6 days
  memory: "16G"
  cpus: 4
```

## Performance Tuning

### Faster Downloads
```yaml
download:
  delay: 0.01  # Reduce delay (watch for rate limiting)
```

### Larger OCR Batches
```yaml
ocr:
  max_pages_per_chunk: 2000  # Larger batches (longer jobs)
```

### More Frequent Checks
```yaml
ocr:
  check_interval: 30  # Check every 30 seconds instead of 60
cleanup:
  check_interval: 30
```

## Next Steps

1. **Run setup**: `./setup_filebased_pipeline.sh`
2. **Submit job**: `CONFIG_FILE=config/caribbean_filebased.yaml sbatch streaming/run_filebased_pipeline.sh`
3. **Monitor**: Use commands above to track progress
4. **Scale**: Once working, can run multiple collections in parallel (different base_dir for each)

## Support

For issues or questions, check:
- SLURM output: `slurm-JOBID.out`
- Error files: `99_errors/*/`
- Manifests: `_manifests/*.json`
- Batch logs: `03_ocr_processing/batch_*/logs/`
