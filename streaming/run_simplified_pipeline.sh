#!/bin/bash
#SBATCH --job-name=simple_pipeline
#SBATCH --time=144:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --account=def-jic823
#SBATCH --mail-type=ALL
#SBATCH --mail-user=jic823@usask.ca

# Simple Pipeline Orchestrator SLURM Job
# Runs the simplified 2-directory pipeline with smart backpressure

set -euo pipefail

echo "========================================="
echo "Simple Pipeline Orchestrator"
echo "========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Start time: $(date)"
echo "========================================="

# Configuration
CONFIG_FILE="${CONFIG_FILE:-config/caribbean_filebased.yaml}"
CHECK_INTERVAL="${CHECK_INTERVAL:-60}"

# Load Python environment (if needed)
# module load python/3.11

# Change to repo directory
cd ~/projects/def-jic823/archive-olm-pipeline

# Run orchestrator
python3 streaming/simple_orchestrator.py \
  --config "$CONFIG_FILE" \
  --check-interval "$CHECK_INTERVAL" \
  --max-iterations -1

echo ""
echo "========================================="
echo "Pipeline complete"
echo "End time: $(date)"
echo "========================================="
