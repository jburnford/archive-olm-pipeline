# Sarah496 OLMoCR on Nibi — Issues & Resolutions

This document records the problems we hit while running the Sarah496 OLMoCR batch on the Nibi cluster, how we diagnosed them, and what we changed to resolve them. It also includes a short runbook to avoid similar issues next time.

## Context

- Collection: Sarah496 (≈33 PDFs, ~2.5 GB)
- Submission script: `process_sarah496.sh`
- Upload helper: `upload_sarah496_to_nibi.sh`
- Container: `~/projects/def-jic823/olmocr/olmocr.sif`
- Target directories:
  - PDFs: `~/projects/def-jic823/sarah496_ocr/pdfs`
  - Output: `~/projects/def-jic823/sarah496_ocr/results`

## Issue 1 — SLURM couldn’t find the script

- Symptom: `sbatch: error: Unable to open file process_sarah496.sh`
- Root cause: Script was untracked locally and hadn’t been pushed to GitHub.
- Resolution:
  - `git add process_sarah496.sh upload_sarah496_to_nibi.sh && git commit && git push`
  - On Nibi: `git pull`

## Issue 2 — Environment alignment with existing Nibi scripts

- Gaps vs. older scripts:
  - Missing `module load apptainer` (or `singularity`) on compute nodes.
  - Only `TMPDIR` set; not exporting `APPTAINER_TMPDIR`, `SINGULARITY_TMPDIR`, or `XDG_RUNTIME_DIR`.
  - No TLS cert envs inside container (can cause TLS errors in some images).
  - Hardcoded paths, not env-overridable.
- Changes made in `process_sarah496.sh`:
  - Load Apptainer (no-op if modules aren’t used): `module load apptainer || module load singularity || true`.
  - Export scratch dirs: `TMPDIR`, `APPTAINER_TMPDIR`, `SINGULARITY_TMPDIR`, `XDG_RUNTIME_DIR`.
  - Add TLS envs: `REQUESTS_CA_BUNDLE`, `SSL_CERT_FILE`.
  - Make paths env-overridable: `PDF_DIR`, `OUTPUT_DIR`, `CONTAINER`.

## Issue 3 — Wrong Python entrypoint inside the container

- Symptom: `/usr/bin/python: No module named olmocr.__main__; 'olmocr' is a package and cannot be directly executed`
- Root cause: Invoked `python -m olmocr`; correct entrypoint is the pipeline module.
- Resolution:
  - Changed to `python -m olmocr.pipeline` and passed per-PDF arguments like our chunk script does.
  - Built args with `--pdfs "/pdfs/<filename>.pdf"` for each PDF.

## Issue 4 — vLLM rejected `--verbose` and never became ready

- Symptom (in log): `vllm: error: unrecognized arguments: --verbose`, repeated “Please wait for vllm server to become ready...” then server task ended.
- Root cause: `--verbose` was forwarded into vLLM’s serve process, which doesn’t accept it.
- Resolution:
  - Removed `--verbose` from the call. The pipeline starts normally afterward.

## Not a Failure — Queue wait (PENDING / Priority)

- Observation: Job `4051910` showed `PENDING (Priority)` with no log yet.
- Explanation: Waiting for an H100 slot; this is scheduling, not a crash.
- Optional acceleration:
  - Submit to backfill partition (e.g., `#SBATCH -p gpubackfill`) with a shorter walltime.
  - Relax GPU constraint to `--gres=gpu:1` if A100/H100 mix is acceptable.

## Verified Paths on Nibi

- Container: `~/projects/def-jic823/olmocr/olmocr.sif` — OK
- PDFs: `~/projects/def-jic823/sarah496_ocr/pdfs` — 32 PDFs detected
- Output: `~/projects/def-jic823/sarah496_ocr/results` — exists

## Current Status

- Script fixes pushed to `main`:
  - Hardened environment and temp handling.
  - Corrected entrypoint: `python -m olmocr.pipeline`.
  - Removed `--verbose` to avoid vLLM failure.
- Latest job `4051910` submitted; pending due to scheduler priority.

## Runbook — Preflight Checklist

1. Git
   - `git pull` (on Nibi) and confirm script presence.
2. Container & Paths
   - `[ -f ~/projects/def-jic823/olmocr/olmocr.sif ]`
   - `ls ~/projects/def-jic823/sarah496_ocr/pdfs/*.pdf | wc -l`
   - `mkdir -p ~/projects/def-jic823/sarah496_ocr/results`
3. GPU/Partition
   - `sinfo -o "%P %G %N" | grep -i gpu`
   - Adjust `#SBATCH --gres=` if needed (e.g., `gpu:h100:1` or `gpu:1`).
4. Submit
   - `sbatch process_sarah496.sh`
5. Monitor
   - `squeue -u $USER`
   - `tail -f sarah496_ocr-<jobid>.out`

## Troubleshooting Commands

```bash
# Latest log
ls -1t sarah496_ocr-*.out | head -n 1 | xargs tail -n 120

# Job accounting
sacct -j <jobid> --format=JobID,State,ExitCode,Elapsed,NodeList%30

# Environment checks
[ -f ~/projects/def-jic823/olmocr/olmocr.sif ] && echo container OK || echo container MISSING
ls ~/projects/def-jic823/sarah496_ocr/pdfs/*.pdf 2>/dev/null | wc -l
```

## Lessons Learned / Best Practices

- Always commit/push local script changes before submitting on Nibi.
- Align cluster scripts with shared patterns:
  - Load Apptainer modules, set scratch/temp variables.
  - Prefer env-overridable paths.
  - Use the correct entrypoint (`olmocr.pipeline`) and argument format.
- Treat PENDING as normal; only investigate when logs show actual errors.
- Keep GPU constraints realistic for available partitions; use backfill for quick tests.

