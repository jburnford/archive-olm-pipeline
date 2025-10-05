# Installation Guide - NIBI Cluster

Complete setup instructions for running the Archive-OLM Pipeline on NIBI.

## Prerequisites

- NIBI cluster account (def-jic823)
- Access to both component repositories:
  - [IA_downloader_cluster](https://github.com/jburnford/IA_downloader_cluster)
  - [olmocr](https://git.cs.usask.ca/history-graphrag/olmocr.git)

## Directory Structure

The pipeline expects this layout on NIBI:

```
/home/jic823/projects/def-jic823/
├── InternetArchive/              # IA_downloader_cluster repo
│   ├── archive_cluster_downloader.py
│   ├── ingest_ocr_results.py
│   ├── archive_tracking.db       # Database (created)
│   └── venv/                     # Python virtual environment
├── cluster/
│   └── olmocr/                   # olmOCR repo
│       ├── smart_submit_pdf_jobs.sh
│       └── smart_process_pdf_chunks.slurm
├── archive-olm-pipeline/         # This repo
│   ├── orchestration/
│   ├── slurm/
│   └── config/
└── pdfs_pipeline/                # Working directory for batches
    ├── results/                  # OCR outputs (created automatically)
    └── chunks/                   # Job chunks (created automatically)
```

## Step-by-Step Setup

### 1. Clone Repositories

```bash
cd /home/jic823/projects/def-jic823

# Clone this pipeline repo
git clone git@github.com:jburnford/archive-olm-pipeline.git

# Verify component repos are present
ls -l InternetArchive/archive_cluster_downloader.py
ls -l cluster/olmocr/smart_submit_pdf_jobs.sh
```

### 2. Create Configuration File

```bash
cd archive-olm-pipeline

# Copy example config
cp config/pipeline_config.yaml.example config/pipeline_config.yaml

# Edit configuration (use nano or vi)
nano config/pipeline_config.yaml
```

**Important settings to verify**:

```yaml
components:
  downloader_repo: /home/jic823/projects/def-jic823/InternetArchive
  olmocr_repo: /home/jic823/projects/def-jic823/cluster/olmocr

directories:
  pdf_dir: /home/jic823/projects/def-jic823/pdfs_pipeline
  database: /home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db

batching:
  batch_size: 1000
  total_items: 100000
```

### 3. Setup Python Environment

The pipeline uses the virtual environment from `InternetArchive` repo:

```bash
cd /home/jic823/projects/def-jic823/InternetArchive

# If venv doesn't exist yet, create it (on login node)
module load python/3.11
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install pyyaml  # Additional dependency for pipeline
```

### 4. Setup Database

Run the migration to add deletion tracking:

```bash
cd /home/jic823/projects/def-jic823/archive-olm-pipeline

# Activate venv
source /home/jic823/projects/def-jic823/InternetArchive/venv/bin/activate

# Run migration (dry-run first to preview)
python3 database/migrations/add_deletion_tracking.py \
    /home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db \
    --dry-run

# Apply migration
python3 database/migrations/add_deletion_tracking.py \
    /home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db
```

### 5. Create Working Directories

```bash
mkdir -p /home/jic823/projects/def-jic823/pdfs_pipeline
mkdir -p /home/jic823/projects/def-jic823/archive-olm-pipeline/logs
```

### 6. Verify Component Access

Test that all components are accessible:

```bash
# Test downloader
python3 /home/jic823/projects/def-jic823/InternetArchive/archive_cluster_downloader.py --help

# Test ingestion
python3 /home/jic823/projects/def-jic823/InternetArchive/ingest_ocr_results.py --help

# Test olmOCR
ls /home/jic823/projects/def-jic823/cluster/olmocr/smart_submit_pdf_jobs.sh
```

### 7. Test Cleanup Script (Dry Run)

```bash
cd /home/jic823/projects/def-jic823/archive-olm-pipeline

# Test cleanup in dry-run mode
python3 orchestration/cleanup_pdfs.py \
    --db-path /home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db \
    --dry-run
```

## Running the Pipeline

### Small Test (10 items)

Test with a small batch first:

```bash
cd /home/jic823/projects/def-jic823/archive-olm-pipeline

# Run 10 items manually (not via SLURM)
python3 orchestration/pipeline_orchestrator.py run-batches \
    --total-items 10 \
    --batch-size 10 \
    --no-cleanup  # Skip cleanup for testing
```

### Production Run (100,000 items in batches of 1,000)

Submit as SLURM job:

```bash
cd /home/jic823/projects/def-jic823/archive-olm-pipeline

# Submit pipeline job
sbatch slurm/run_pipeline.sh \
    --total-items 100000 \
    --batch-size 1000
```

### Monitor Progress

```bash
# Check SLURM job status
squeue -u $USER

# View log file (will be in current directory)
tail -f slurm-*.out

# Check database statistics
sqlite3 /home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db \
    "SELECT * FROM pipeline_runs ORDER BY started_date DESC LIMIT 10;"
```

### Resume from Interruption

If the pipeline stops, resume from a specific batch:

```bash
sbatch slurm/run_pipeline.sh \
    --total-items 100000 \
    --batch-size 1000 \
    --start-batch 15  # Resume from batch 15
```

## Troubleshooting

### Virtual Environment Not Found

```bash
cd /home/jic823/projects/def-jic823/InternetArchive
module load python/3.11
./setup_venv.sh
```

### Database Not Found

Make sure you've run the initial download at least once to create the database:

```bash
cd /home/jic823/projects/def-jic823/InternetArchive
sbatch run_archive_download.sh
```

### olmOCR Container Missing

```bash
cd /home/jic823/projects/def-jic823/cluster/olmocr
sbatch nibi_setup_official.sh
```

### Permission Errors

Make sure all scripts are executable:

```bash
cd /home/jic823/projects/def-jic823/archive-olm-pipeline
chmod +x orchestration/*.py
chmod +x slurm/*.sh
chmod +x database/migrations/*.py
```

## Verifying Setup

Run this checklist before starting production runs:

```bash
# 1. Check repos
[ -f /home/jic823/projects/def-jic823/InternetArchive/archive_cluster_downloader.py ] && echo "✓ Downloader repo" || echo "✗ Downloader repo missing"
[ -f /home/jic823/projects/def-jic823/cluster/olmocr/smart_submit_pdf_jobs.sh ] && echo "✓ olmOCR repo" || echo "✗ olmOCR repo missing"
[ -f /home/jic823/projects/def-jic823/archive-olm-pipeline/orchestration/pipeline_orchestrator.py ] && echo "✓ Pipeline repo" || echo "✓ Pipeline repo missing"

# 2. Check database
[ -f /home/jic823/projects/def-jic823/InternetArchive/archive_tracking.db ] && echo "✓ Database exists" || echo "✗ Database missing"

# 3. Check venv
[ -d /home/jic823/projects/def-jic823/InternetArchive/venv ] && echo "✓ Virtual environment" || echo "✗ Venv missing"

# 4. Check configuration
[ -f /home/jic823/projects/def-jic823/archive-olm-pipeline/config/pipeline_config.yaml ] && echo "✓ Configuration" || echo "✗ Config missing"
```

## Next Steps

After successful setup, see:
- [Workflow Guide](docs/WORKFLOW.md) - Detailed explanation of each phase
- [Usage Examples](docs/EXAMPLES.md) - Common use cases
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues and solutions
