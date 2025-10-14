# Pipeline Operations: Next Steps

## Current State (Context)
- Separate daemons running:
  - `downloader_daemon`: RUNNING, auto‑pauses at 80% disk.
  - `submit_cleanup_daemon`: RUNNING, loops submit → split → finalize every 5 minutes.
- Large OLMoCR array pending plus a retry array submitted with reduced concurrency (WORKERS=1).
- Disk trending downward as finalize reclaims space.

## Immediate Next Steps
- Let retry array (WORKERS=1) run and observe yield (JSONL files produced, finalized items, fewer vLLM "finish_reason != stop" warnings).
- Keep submit/cleanup daemon running; it will split and finalize results continuously and free disk.
- Keep downloader paused by disk threshold (80%); it will resume automatically when space allows.

## Monitoring
- Queue and priority
  - `squeue -u $USER -o "%18i %20j %8T %10M %9l %R"`
  - `sprio -l -j <ARRAY_JOBID>`
  - `squeue --start -j <ARRAY_JOBID>`
- Submit/Cleanup activity (finalize growth)
  - `tail -n 200 ~/projects/def-jic823/archive-olm-pipeline/slurm-*.out`
  - Watch for "Finalized:" and "Total processed" counters increasing.
- Disk usage
  - `df -h /home/jic823/projects/def-jic823`
- Results presence per batch
  - `find $BASE/01_downloaded/batch_XXXX -type f -name "*.jsonl" | wc -l`
  - `find $BASE/01_downloaded/batch_XXXX -type f -path "*/results/json/*.json" | wc -l`

## Measure WORKERS=1 Success Rate
Use this to decide whether to default to a single worker for stability.

- After a retry batch (e.g., `batch_0053`) completes:
  - `R=$BASE/01_downloaded/batch_0053`
  - `req=$(awk 'NF>0' "$R"/chunks/chunk_*.txt | wc -l)`
  - `done=$(wc -l < "$R/processed_files.log")`
  - `awk -v d="$done" -v r="$req" 'BEGIN{printf "batch_0053 success: %d/%d (%.2f%%)\n", d, r, (d/r)*100}'`

- Decision rule:
  - If success ≥ 98% and logs show few "finish_reason != stop" warnings → default to `WORKERS=1`.
  - If < 95% → keep `WORKERS=1` and try `--pages-per-group 1` for the next retry batch.
  - Optionally re‑trial `WORKERS=2` on a small batch later if warnings remain low.

## Reconcile & Retry (Targeted Recovery)
The reconcile tool detects PDFs requested by a batch that never made it to `processed_files.log`, and can resubmit only those PDFs.

- Dry run (no submit):
  - `python3 streaming/batch_reconcile.py --base-dir $BASE`
- Submit retries with reduced concurrency:
  - `python3 streaming/batch_reconcile.py --base-dir $BASE --submit --workers 1`
- Notes:
  - Skips in‑flight batches (from `_manifests/batch_state.json` and `squeue`) and the newest batch to avoid duplication.
  - Packs retries into ~1500‑page chunks to avoid extra model load overhead.

## Crash/Restart Playbook
If the node or job crashes, bring the pipeline back quickly:

1) Verify repo sync and scripts present
- `cd $REPO && git status -uno && git pull --rebase`

2) Relaunch daemons
- `sbatch streaming/run_downloader_daemon.sh`
- `sbatch streaming/run_submit_cleanup_daemon.sh`

3) Snapshot state
- Queue: `squeue -u $USER -o "%18i %20j %8T %10M %9l %R"`
- Disk: `df -h /home/jic823/projects/def-jic823`
- Counts: processed dirs, PDFs in 01_downloaded, JSONL, split JSON

4) Reconcile (analysis first)
- `python3 streaming/batch_reconcile.py --base-dir $BASE`
- If `Total missing PDFs > 0` and no conflicting in‑flight batches:
  - `python3 streaming/batch_reconcile.py --base-dir $BASE --submit --workers 1`

5) Monitor finalize loop
- `tail -n 200 $REPO/slurm-*.out` for "Finalized" and "Total processed" growth
- Verify disk drops as PDFs are deleted post‑finalize

6) Decide default WORKERS
- Apply the success‑rate rule above; if defaulting to `WORKERS=1`, update any wrapper scripts or orchestration to pass `--workers 1`.

## Tuning Knobs (if yield remains low)
- Reduce internal OLMoCR concurrency for hardest batches:
  - Use `--workers 1` (already enabled for retries).
  - Optionally `--pages-per-group 1` for especially fragile sets.
- Keep chunk cap at 1500 pages initially (avoids model reload cost). Revisit only if timeouts persist.

## Health Checks (Batch Completeness)
Per batch:
- Requested PDFs: `cat batch_XXXX/chunks/chunk_*.txt | wc -l`
- Done count: `wc -l < batch_XXXX/processed_files.log`
- JSONL produced: `find batch_XXXX -type f -name "*.jsonl" | wc -l`
- Warning density:
  - `grep -h "Response did not finish" batch_XXXX/slurm-*.out | wc -l`

## When to Intervene
- Finalize plateaus while arrays are RUNNING.
- Disk stops dropping while JSONL is arriving.
- Many tasks complete unusually fast with few/no JSONL outputs.
- Warnings spike per task; consider reruns with `--workers 1 --pages-per-group 1`.

## Operational Notes
- GPU PD state with reason `ReqNodeNotAvail` is normal on shared H100 partitions; use `sprio` and `squeue --start` for estimates.
- The downloader daemon remains RUNNING in Slurm even when internally paused by the 80% disk guard—that is expected.

## Handy Variables
```
BASE=/home/jic823/projects/def-jic823/caribbean_pipeline
REPO=/home/jic823/projects/def-jic823/archive-olm-pipeline
```
