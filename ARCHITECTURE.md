# File-Based Pipeline Architecture

## Directory Structure

```
pdfs_caribbean/
├── 00_queue/                    # Files ready to download
│   └── {identifier}.json        # Minimal metadata (identifier, collection)
│
├── 01_downloaded/               # PDFs successfully downloaded
│   ├── {identifier}.pdf
│   └── {identifier}.meta.json   # Download metadata (timestamp, size, URL)
│
├── 02_ocr_pending/              # Ready for OCR processing
│   └── {identifier}.pdf         # Symlink or move from 01_downloaded
│
├── 03_ocr_processing/           # Currently being processed by OLMoCR
│   ├── batch_0001/              # OCR job batch directories
│   │   ├── {identifier}.pdf
│   │   ├── results/
│   │   │   └── {identifier}.jsonl
│   │   └── batch.meta.json      # Batch metadata (job_id, submitted_time, status)
│   └── batch_0002/
│
├── 04_ocr_completed/            # OCR successfully completed
│   ├── {identifier}.ocr.jsonl   # Raw OCR output from OLMoCR
│   └── {identifier}.meta.json   # OCR metadata (pages, processing_time)
│
├── 05_processed/                # Final processed results
│   ├── {identifier}.json        # Combined metadata + OCR text
│   └── {identifier}.pdf         # (Optional) Keep PDF or archive
│
├── 99_errors/                   # Failed items
│   ├── download_failed/
│   │   └── {identifier}.error.json
│   ├── ocr_failed/
│   │   └── {identifier}.error.json
│   └── processing_failed/
│       └── {identifier}.error.json
│
└── _manifests/                  # Collection-level tracking
    ├── collection.json          # Overall statistics
    ├── batches.json            # OCR batch registry
    └── progress.json           # Pipeline progress tracking
```

## File Formats

### Queue File: `00_queue/{identifier}.json`
```json
{
  "identifier": "historyofjamaica01long",
  "collection": "caribbean_collection",
  "queued_at": "2025-10-10T01:00:00Z"
}
```

### Download Metadata: `01_downloaded/{identifier}.meta.json`
```json
{
  "identifier": "historyofjamaica01long",
  "collection": "caribbean_collection",
  "downloaded_at": "2025-10-10T01:15:23Z",
  "file_size": 45678901,
  "file_path": "01_downloaded/historyofjamaica01long.pdf",
  "source_url": "https://archive.org/download/...",
  "md5": "abc123..."
}
```

### Batch Metadata: `03_ocr_processing/batch_0001/batch.meta.json`
```json
{
  "batch_id": "batch_0001",
  "slurm_job_id": "2682150",
  "submitted_at": "2025-10-10T02:00:00Z",
  "total_pdfs": 15,
  "total_pages": 1523,
  "identifiers": [
    "historyofjamaica01long",
    "voyagetoguinea1735",
    "..."
  ],
  "status": "running"
}
```

### OCR Metadata: `04_ocr_completed/{identifier}.meta.json`
```json
{
  "identifier": "historyofjamaica01long",
  "batch_id": "batch_0001",
  "ocr_completed_at": "2025-10-10T02:45:12Z",
  "total_pages": 234,
  "processing_seconds": 168.5,
  "ocr_file": "04_ocr_completed/historyofjamaica01long.ocr.jsonl"
}
```

### Final Result: `05_processed/{identifier}.json`
```json
{
  "identifier": "historyofjamaica01long",
  "collection": "caribbean_collection",
  "title": "The History of Jamaica",
  "year": 1774,
  "downloaded_at": "2025-10-10T01:15:23Z",
  "ocr_completed_at": "2025-10-10T02:45:12Z",
  "total_pages": 234,
  "file_size": 45678901,
  "ocr_text": "...",  // Or reference to .ocr.jsonl
  "metadata": {
    "source_url": "https://archive.org/...",
    "collection": "caribbean_collection"
  }
}
```

### Error File: `99_errors/download_failed/{identifier}.error.json`
```json
{
  "identifier": "brokenfile123",
  "stage": "download",
  "error_type": "404",
  "error_message": "Item not found",
  "timestamp": "2025-10-10T01:30:00Z",
  "retry_count": 3
}
```

## Pipeline State Tracking

**State = Directory Location**
- Item in `00_queue/` → needs downloading
- Item in `01_downloaded/` → ready for OCR
- Item in `03_ocr_processing/` → currently being processed
- Item in `04_ocr_completed/` → ready for final processing
- Item in `05_processed/` → complete

**State Transitions = File Moves**
```
00_queue → 01_downloaded → 02_ocr_pending → 03_ocr_processing/batch_XXXX/ → 04_ocr_completed → 05_processed
                                                                          ↓
                                                                   99_errors/
```

## Key Benefits

### 1. Atomic Operations
Each file operation is atomic - no partial states, no corruption

### 2. Resume-Friendly
Pipeline can restart at any point:
```bash
# Count items at each stage
ls 00_queue/*.json | wc -l        # Queued
ls 01_downloaded/*.pdf | wc -l    # Downloaded
ls 02_ocr_pending/*.pdf | wc -l   # Ready for OCR
ls 04_ocr_completed/*.jsonl | wc -l  # OCR complete
```

### 3. Easy Debugging
```bash
# Find all errors
ls 99_errors/*/*.error.json

# Check specific item status
find . -name "historyofjamaica01long.*"
```

### 4. Parallel Processing
No database locks - each worker processes different files independently

### 5. Simple Queries
```bash
# How many PDFs are waiting for OCR?
ls 02_ocr_pending/*.pdf | wc -l

# Which batches are still processing?
find 03_ocr_processing -name "batch.meta.json" -exec grep -l '"status": "running"' {} \;

# Total pages processed today
find 04_ocr_completed -name "*.meta.json" -mtime -1 -exec jq -r '.total_pages' {} \; | awk '{sum+=$1} END {print sum}'
```

### 6. Cleanup/Archiving
```bash
# Archive completed PDFs (free 52GB)
tar -czf completed_batch_0001.tar.gz 03_ocr_processing/batch_0001/
rm -rf 03_ocr_processing/batch_0001/*.pdf

# Keep only OCR results, delete PDFs
find 05_processed -name "*.pdf" -delete
```

## Migration from SQLite

1. Export current database to JSON files
2. Organize into directory structure
3. Update pipeline scripts to use file operations
4. Archive SQLite database as backup

## Performance Considerations

### Millions of Files
For very large collections, use subdirectories:
```
05_processed/
├── a/
│   ├── ab/
│   │   └── abc123.json
│   └── ad/
│       └── ade456.json
├── b/
│   └── ...
```

Hash-based sharding: `{identifier[0]}/{identifier[0:2]}/{identifier}.json`

### NFS Optimization
- Use `rsync` for bulk moves
- Batch operations when possible
- Avoid `ls` on huge directories (use `find` instead)

## Pipeline Implementation

Each component becomes simpler:
1. **Downloader**: Move from `00_queue/` → `01_downloaded/`
2. **Dispatcher**: Bundle files from `02_ocr_pending/` → `03_ocr_processing/batch_XXXX/`
3. **Cleanup**: Move from `04_ocr_completed/` → `05_processed/`

No database = no corruption, no locks, no copying, no risk.
