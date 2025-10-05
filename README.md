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
- [Workflow Details](docs/WORKFLOW.md) - How the pipeline works
- [Usage Examples](docs/EXAMPLES.md) - Common use cases
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues

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
