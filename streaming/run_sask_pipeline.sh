#!/bin/bash
#SBATCH --job-name=sask_pipeline
#SBATCH --time=168:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --account=def-jic823
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=jic823@usask.ca

# Saskatchewan Canadiana Pipeline Orchestrator
#
# Runs the orchestrator as a long-running job that:
#   - Submits batches to keep 5-10 in queue
#   - Processes completed results
#   - Tracks failures for retry
#
# Usage:
#   sbatch streaming/run_sask_pipeline.sh

set -e

# Change to repository directory
cd ~/projects/def-jic823/archive-olm-pipeline

# Log start time
echo "=========================================="
echo "Saskatchewan Canadiana Pipeline"
echo "Started: $(date)"
echo "=========================================="

# Activate Python environment if needed
# source ~/venv/bin/activate

# Run orchestrator
python3 streaming/sask_orchestrator.py \
    --config config/sask_canadiana.yaml \
    --check-interval 120 \
    --export-interval 10

# Log completion
echo "=========================================="
echo "Pipeline completed: $(date)"
echo "=========================================="
