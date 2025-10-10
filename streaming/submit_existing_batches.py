#!/usr/bin/env python3
"""
Submit Existing Batches to OLMoCR

Finds batch directories in 03_ocr_processing that contain PDFs but were
never submitted, submits them using OLMoCR's smart_submit script, and
updates batch metadata and the batches manifest.

Usage:
  python3 streaming/submit_existing_batches.py --config config/caribbean_filebased.yaml
"""

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
import shutil
from typing import Dict, List

import yaml


def load_config(path: Path) -> Dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_batches(manifest: Path) -> List[Dict]:
    if manifest.exists():
        with open(manifest) as f:
            data = json.load(f)
            return data.get("batches", [])
    return []


def save_batches(manifest: Path, batches: List[Dict]):
    data = {
        "batches": batches,
        "last_updated": datetime.utcnow().isoformat() + "Z",
    }
    with open(manifest, "w") as f:
        json.dump(data, f, indent=2)


def parse_job_id(output: str) -> str:
    for line in output.split("\n"):
        if "Submitted batch job" in line:
            return line.split()[-1]
    raise RuntimeError("Could not parse SLURM job ID from output:\n" + output)


def submit_batch(olmocr_script: Path, batch_dir: Path) -> str:
    cmd = [str(olmocr_script), "--pdf-dir", str(batch_dir)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    output = (result.stdout or "") + (result.stderr or "")
    if result.returncode != 0:
        raise RuntimeError(f"Submit script failed (code {result.returncode})\n{output}")
    return parse_job_id(output)


def reset_batch_state(batch_dir: Path):
    """Clear flags that might make OLMoCR think PDFs are already processed."""
    proc_log = batch_dir / "processed_files.log"
    if proc_log.exists():
        try:
            proc_log.unlink()
        except Exception:
            pass
    # Clear done flags and worker locks if present
    for sub in [batch_dir / "results" / "done_flags", batch_dir / "results" / "worker_locks"]:
        if sub.exists() and sub.is_dir():
            try:
                shutil.rmtree(sub)
            except Exception:
                pass


def get_slurm_state(job_id: str) -> str:
    """Return a coarse job state using sacct/squeue.

    Returns one of: RUNNING, PENDING, COMPLETED, FAILED, UNKNOWN
    """
    try:
        # sacct shows historical; use parsable to simplify
        r = subprocess.run(
            ["sacct", "-j", job_id, "--format=State", "--noheader", "--parsable2"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if r.returncode == 0:
            for line in (r.stdout or "").splitlines():
                state = line.strip()
                if not state:
                    continue
                if "COMPLETED" in state:
                    return "COMPLETED"
                if any(x in state for x in ("FAILED", "CANCELLED", "TIMEOUT")):
                    return "FAILED"
                if any(x in state for x in ("RUNNING", "PENDING")):
                    return "RUNNING" if "RUNNING" in state else "PENDING"
        # Fallback to squeue (active only)
        r2 = subprocess.run(
            ["squeue", "-j", job_id, "-h", "-o", "%T"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if r2.returncode == 0:
            s = (r2.stdout or "").strip().upper()
            if s in {"RUNNING", "PENDING", "CONFIGURING", "COMPLETING"}:
                return "RUNNING" if s == "RUNNING" else "PENDING"
    except Exception:
        pass
    return "UNKNOWN"


def main():
    parser = argparse.ArgumentParser(description="Submit existing OCR batches to OLMoCR")
    parser.add_argument("--config", required=True, type=Path, help="Path to file-based YAML config")
    args = parser.parse_args()

    cfg = load_config(args.config)
    base_dir = Path(cfg["directories"]["base_dir"])  # e.g., /home/.../caribbean_pipeline
    olmocr_repo = Path(cfg["components"]["olmocr_repo"])
    olmocr_script = olmocr_repo / "smart_submit_pdf_jobs.sh"

    processing_dir = base_dir / "03_ocr_processing"
    manifest = base_dir / "_manifests" / "batches.json"
    batches = load_batches(manifest)

    # Index batches by id to simplify update or insert
    batch_index = {b.get("batch_id"): b for b in batches}

    print("=" * 70)
    print("Submit Existing Batches")
    print("=" * 70)
    print(f"Base: {base_dir}")
    print(f"OLMoCR: {olmocr_script}")

    candidates = sorted([d for d in processing_dir.glob("batch_*") if d.is_dir()])
    print(f"Found {len(candidates)} batch directories")

    submitted = 0
    skipped = 0
    errors = 0

    for batch_dir in candidates:
        batch_meta_file = batch_dir / "batch.meta.json"
        if batch_meta_file.exists():
            with open(batch_meta_file) as f:
                meta = json.load(f)
        else:
            # Create minimal meta if missing
            meta = {"batch_id": batch_dir.name, "status": "created"}

        job_id = str(meta.get("slurm_job_id") or "").strip()

        # If results already exist, skip (already processed)
        if list((batch_dir / "results").glob("**/*.jsonl")):
            print(f"  ↷ Skip {batch_dir.name}: results already present")
            skipped += 1
            continue

        # If we have a job id, verify with SLURM
        if job_id:
            state = get_slurm_state(job_id)
            if state in {"RUNNING", "PENDING"}:
                print(f"  ↷ Skip {batch_dir.name}: job {job_id} {state}")
                skipped += 1
                continue
            else:
                print(f"  → Resubmitting {batch_dir.name}: previous job {job_id} state={state}")

        else:
            # If meta says running but no job id, we should submit
            if meta.get("status") == "running":
                print(f"  → Submitting {batch_dir.name}: status=running but no job ID present")

        # Ensure there are PDFs to process
        pdfs = list(batch_dir.glob("*.pdf"))
        if not pdfs:
            print(f"  ↷ Skip {batch_dir.name}: no PDFs found")
            skipped += 1
            continue

        print(f"  → Submitting {batch_dir.name} with {len(pdfs)} PDFs")
        try:
            try:
                job_id = submit_batch(olmocr_script, batch_dir)
            except RuntimeError as e:
                msg = str(e)
                if "No new PDFs to process" in msg:
                    print(f"    ↻ Resetting state for {batch_dir.name} and retrying")
                    reset_batch_state(batch_dir)
                    job_id = submit_batch(olmocr_script, batch_dir)
                else:
                    raise
            print(f"    ✓ Submitted SLURM job {job_id}")

            # Update batch meta
            meta.update({
                "batch_id": batch_dir.name,
                "slurm_job_id": job_id,
                "submitted_at": datetime.utcnow().isoformat() + "Z",
                "total_pdfs": meta.get("total_pdfs", len(pdfs)),
                "status": "submitted",
            })
            with open(batch_meta_file, "w") as f:
                json.dump(meta, f, indent=2)

            # Update manifest
            batch_index[meta["batch_id"]] = meta
            batches = [batch_index[k] for k in sorted(batch_index.keys())]
            save_batches(manifest, batches)

            submitted += 1
        except Exception as e:
            print(f"    ✗ Error: {e}")
            errors += 1

    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Submitted: {submitted}")
    print(f"Skipped: {skipped}")
    print(f"Errors: {errors}")


if __name__ == "__main__":
    main()
