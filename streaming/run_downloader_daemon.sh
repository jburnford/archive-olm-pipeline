#!/bin/bash
#SBATCH --job-name=downloader_daemon
#SBATCH --time=144:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=2
#SBATCH --account=def-jic823
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=jic823@usask.ca

# Dedicated downloader daemon
# - Continuously downloads identifiers into 01_downloaded/
# - Pauses automatically when disk usage >= 80%
# - Resumes when space frees after finalize cleanup

set -euo pipefail

echo "========================================="
echo "Downloader Daemon"
echo "========================================="
echo "Job ID: ${SLURM_JOB_ID:-n/a}"
echo "Node: ${SLURM_NODELIST:-n/a}"
echo "Start time: $(date)"
echo "========================================="

# Load Python and activate venv
if command -v module >/dev/null 2>&1; then
  module load python/3.11 || true
fi

VENV_DIR="/home/jic823/projects/def-jic823/InternetArchive/venv"
source "$VENV_DIR/bin/activate"

cd "$HOME/projects/def-jic823/archive-olm-pipeline"

python3 streaming/file_based_downloader.py \
  --identifiers-file "$HOME/projects/def-jic823/archive-olm-pipeline/caribbean_identifiers.json" \
  --start-from 0 \
  --max-items 100000000 \
  --base-dir "/home/jic823/projects/def-jic823/caribbean_pipeline" \
  --delay 0.05 \
  --collection caribbean_collection \
  --disk-threshold 0.80

echo "========================================="
echo "Downloader daemon finished"
echo "End time: $(date)"
echo "========================================="

