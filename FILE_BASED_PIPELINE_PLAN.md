# File-Based Pipeline Completion Plan (Next 24–48 Hours)

## Objectives

- Stabilize and run the end-to-end file-based pipeline by tomorrow.
- Clear the current OCR backlog (batches 0002–0012) already submitted via direct submission.
- Ensure outputs are consolidated, linked with original metadata, and original PDFs are deleted to free space.
- Add basic monitoring to estimate throughput and runtime expectations for future runs.

## Current State (as of now)

- JSONL splitting: Fixed. `orchestration/split_jsonl_to_json.py` now searches recursively and extracts nested records by `metadata.Source-File` (and variants).
- Cleanup worker: Updated to use the same nested extraction and recursive search; can split combined JSONL into per-identifier `.ocr.jsonl`.
- Finalizer: New `streaming/file_based_finalize.py` consolidates per-PDF JSON with download metadata into `05_processed/<identifier>/` and deletes original PDFs on success.
- Direct submission: New `streaming/direct_submit_batches.py` chunks PDFs by ~1,500 pages and submits jobs directly via `sbatch` (bypasses smart submitter’s symlink-sensitive pending scan). Batches `0002`–`0012` have been submitted.
- Smart submit tool: `smart_submit_pdf_jobs.sh` in OLMoCR reports “No new PDFs to process” when files are symlinks; we worked around this using the direct submitter.

## High-Level Plan

1. Monitor the currently submitted backlog to completion and split/finalize results to free disk space.
2. Replace smart submit step in our orchestrator with the direct submit tool (or adjust smart submitter to follow symlinks) so future batches flow hands-off.
3. Keep cleanup worker running to detect completed results and generate per-identifier files and metadata.
4. Run the finalizer routinely to link outputs and delete PDFs.
5. Re-enable the downloader after significant backlog processed and disk space recovered.
6. Track runtime and throughput from SLURM to guide scale and scheduling.

## Tonight (Immediate Actions)

- Verify jobs are in the SLURM queue and running:
  - `squeue -u $USER`
  - `tail -f /home/jic823/projects/def-jic823/caribbean_pipeline/03_ocr_processing/batch_0002/logs/*.out`
- Keep the cleanup worker ready to process results as they appear:
  - `python3 streaming/file_based_cleanup.py --base-dir /home/jic823/projects/def-jic823/caribbean_pipeline --check-interval 60`
- If early results appear (partial batches), run the finalizer to free space:
  - `python3 streaming/file_based_finalize.py --base-dir /home/jic823/projects/def-jic823/caribbean_pipeline`

## Tomorrow Morning (Backlog > Finalization)

1. Confirm job status and count remaining chunks:
   - `sacct -u $USER --format=JobID,JobName,State,Elapsed,Start,End | grep olmocr_pdf_ | tail -100`
2. If results created (`results/results/*.jsonl`), run:
   - `python3 orchestration/split_jsonl_to_json.py /path/to/batch_XXXX`
   - or let cleanup worker run and generate per-identifier `.ocr.jsonl` in `04_ocr_completed/`.
3. Finalize outputs and delete PDFs:
   - `python3 streaming/file_based_finalize.py --base-dir /home/jic823/projects/def-jic823/caribbean_pipeline`
4. Validate counts and disk usage:
   - `find /home/jic823/projects/def-jic823/caribbean_pipeline/05_processed -type f -name "*.ocr.json" | wc -l`
   - `du -sh /home/jic823/projects/def-jic823/caribbean_pipeline/{01_downloaded,02_ocr_pending,03_ocr_processing,04_ocr_completed,05_processed}`

## Pipeline Stabilization (Daytime Work)

Two options to keep submission stable going forward:

- Option A (Recommended now): Integrate `direct_submit_batches.py` into the orchestrator.
  - Update `streaming/file_based_orchestrator.py` to launch the direct submitter instead of relying on the smart submitter.
  - Ensure it runs periodically or event-driven after batches are formed.

- Option B: Patch the OLMoCR submitter to follow symlinks.
  - In `smart_submit_pdf_jobs.sh`, modify the pending detection to follow symlinks:
    - Use `find -L "$PDF_DIR" -maxdepth 1 -type f -name "*.pdf"` or include `-xtype l`.
  - Keep our dispatcher unchanged.

We can do Option A now for speed, and later contribute a PR upstream for Option B.

## Monitoring & Runtime Estimation

Goal: estimate processing throughput (pages/hour) and walltime per chunk to plan future capacity.

- Quick per-chunk telemetry (from SLURM):
  - `sacct -u $USER --name olmocr_pdf_* --format=JobID,State,Elapsed,Start,End%20 | sed '1,2!b'`
  - `sacct -j <jobid> --format=JobID,State,Elapsed,CPUTimeRAW`
- Aggregate throughput (example Python snippet):
  ```bash
  python3 - << 'PY'
import subprocess, re
out = subprocess.run(['sacct','-u','$USER','--name','olmocr_pdf_*','--format','JobID%20,State%12,Elapsed'],
                     capture_output=True,text=True).stdout
elapsed_secs = 0
jobs = 0
for line in out.splitlines():
    if 'COMPLETED' in line or 'RUNNING' in line:
        m = re.search(r'(\d{2}):(\d{2}):(\d{2})', line)
        if m:
            h,mn,s = map(int,m.groups())
            elapsed_secs += h*3600+mn*60+s
            jobs += 1
print('Jobs:', jobs, 'Sum elapsed (h):', round(elapsed_secs/3600,2))
PY
  ```
- Compare against expected per-page estimate:
  - `walltime ≈ 300s + pages*6s + 20% buffer` (already encoded in direct submit tool).
- Track how many chunks complete per hour to estimate total time for a collection.

## Error Handling / Recovery

- If smart submit reports “No new PDFs to process” but PDFs exist:
  - Use `streaming/direct_submit_batches.py` to submit.
- If a batch produces `unknown` identifiers during split:
  - Re-run splitter (we now handle nested metadata and files); inspect one JSONL line to confirm `metadata.Source-File`.
- If cleanup worker marks a batch completed too early:
  - Re-run `split_jsonl_to_json.py` directly on that batch and finalize; correct metadata afterwards.

## Operational Runbook (Commands)

- Launch cleanup worker:
  - `python3 streaming/file_based_cleanup.py --base-dir /home/jic823/projects/def-jic823/caribbean_pipeline --check-interval 60`
- Submit backlog directly (already done for 0002–0012; use if needed):
  - `python3 streaming/direct_submit_batches.py --config config/caribbean_filebased.yaml --batches batch_0002 ...`
- Split JSONL (per-batch):
  - `python3 orchestration/split_jsonl_to_json.py /home/jic823/projects/def-jic823/caribbean_pipeline/03_ocr_processing/batch_000X`
- Finalize + delete PDFs:
  - `python3 streaming/file_based_finalize.py --base-dir /home/jic823/projects/def-jic823/caribbean_pipeline`
- Monitor SLURM:
  - `squeue -u $USER`
  - `sacct -u $USER --name olmocr_pdf_* --format JobID,State,Elapsed,Start,End`

## Definition of “Done” for Tomorrow

- [ ] All chunks for batches 0002–0012 are submitted and progressing/complete.
- [ ] Results are split into per-PDF JSON under each batch or per-identifier `.ocr.jsonl` under `04_ocr_completed/`.
- [ ] Finalizer moved outputs into `05_processed/<identifier>/` and deleted original PDFs from `01_downloaded` and batch dirs.
- [ ] Disk space recovered and confirmed.
- [ ] Orchestrator updated to use direct submission (or smart submit patched) so file-based pipeline runs end-to-end.
- [ ] Basic runtime metrics captured (average chunk walltime, throughput estimate).

## Follow-Ups (After Tomorrow)

- Integrate direct submission step into `streaming/file_based_orchestrator.py`.
- Add a small metrics collector to write chunk/job stats to `_manifests/metrics.json`.
- Contribute a PR to OLMoCR submitter to follow symlinks (fixing the `find` behavior) to align with our file layout.
- Optional: add a throttle to downloader to avoid overwhelming storage while backlog processes.

