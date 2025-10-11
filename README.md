# Archive-OLM Pipeline

Automated pipeline for processing Internet Archive collections at scale with olmOCR.

## Overview

This pipeline coordinates a complete workflow:
1. **Download**: Fetch PDFs from Internet Archive with metadata
2. **OCR**: Batch process with olmOCR on NIBI cluster
3. **Ingest**: Store OCR results in SQLite database
4. **Cleanup**: Delete PDFs after successful ingestion to save space

**Key Feature**: Processes in batches of 1,000 items to manage disk space (1TB limit on NIBI).

## Architecture

```
┌─────────────┐
│  Download   │  Fetch 1,000 PDFs + metadata
│   1000      │  Store in database
└──────┬──────┘
       │
       ▼
┌─────────────┐
│     OCR     │  Process with olmOCR
│   (SLURM)   │  Generate JSONL results
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Ingest    │  Store OCR data in database
│  to Database│  Verify data integrity
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Cleanup   │  Delete PDFs (keep metadata + OCR)
│  Delete PDFs│  Free disk space
└──────┬──────┘
       │
       ▼
   Next 1,000 items
```

## Features

- **Batch Processing**: Handles 100,000+ items in 1,000-item chunks
- **Space Efficient**: Deletes PDFs after OCR (keeps metadata + OCR text)
- **Safe Deletion**: Multi-level verification before removing files
- **Fault Tolerant**: Resume from any phase if interrupted
- **SLURM Integrated**: Runs on NIBI cluster with job dependencies

## Quick Start

```bash
# On NIBI cluster
cd /home/jic823/projects/def-jic823/archive-olm-pipeline

# Process 1,000 items (download → OCR → ingest → cleanup)
sbatch slurm/run_pipeline.sh --batch-size 1000

# Process 100,000 items in batches
python3 orchestration/pipeline_orchestrator.py run-batches \
    --total-items 100000 \
    --batch-size 1000
```

## Input Formats

The pipeline supports two input formats for specifying which items to process:

### Option 1: identifiers.json (default)
```json
{
  "query": "Saskatchewan AND date:[1808-01-01 TO 1946-01-01]",
  "sort_order": "date asc",
  "total_count": 16075,
  "identifiers": ["id1", "id2", "id3", ...]
}
```

### Option 2: CSV file (automatic conversion)
You can directly use a CSV file with an `identifier` column. The pipeline will automatically convert it to JSON format.

**CSV Requirements:**
- Must have an `identifier` column
- One row per item
- Additional columns (title, date, etc.) are preserved in CSV but only identifier is used

**Usage:**
```yaml
# In config file (e.g., config/caribbean.yaml)
download:
  identifiers_file: /path/to/search_results.csv  # Can be CSV or JSON
```

The pipeline automatically detects CSV format and converts it on first run. The converted `identifiers.json` is saved in the same directory for future use.

**Manual conversion (optional):**
```bash
python3 orchestration/csv_to_identifiers.py input.csv -o output.json
```

## Components

- `orchestration/` - Python scripts for pipeline control
- `config/` - Configuration files
- `slurm/` - SLURM job submission scripts
- `database/` - Schema migrations
- `docs/` - Detailed documentation

## Space Savings

**Per 1,000 PDFs**:
- Before: ~5 GB (PDFs on disk)
- After: ~100 MB (database with OCR text)
- **Savings: 98%**

**For 100,000 items**:
- Traditional: ~500 GB
- Pipeline: ~10 GB
- **Savings: ~490 GB**

## Documentation

- [Installation Guide](INSTALLATION.md) - NIBI cluster setup
- [File-Based Pipeline Overview](FILE_BASED_PIPELINE.md) - Streaming file-based pipeline
- [Pipeline Plan](FILE_BASED_PIPELINE_PLAN.md) - Current stabilization and scale plan
- [Components Inventory](docs/COMPONENTS.md) - What we use and where it is
- [Interfaces](docs/INTERFACES.md) - Contracts for key scripts
- [Contributing](docs/CONTRIBUTING.md) - Reuse-first guidance
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues

### Version Pins / Bootstrap

- `_manifests/versions.json` holds pinned external repo commits.
- `tools/bootstrap_components.sh` clones/updates external repos to pinned SHAs.

## Requirements

- NIBI cluster access (account: def-jic823)
- [IA_downloader_cluster](https://github.com/jburnford/IA_downloader_cluster) - Download component
- [olmocr](https://git.cs.usask.ca/history-graphrag/olmocr.git) - OCR processing
- Python 3.11+ with dependencies from `requirements.txt`

## License

Academic research use only.

## Contact

- Jim Clifford (jic823@usask.ca)
- NIBI Cluster: Digital Research Alliance of Canada
