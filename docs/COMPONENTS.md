# Components Inventory

Single source of truth for what we use, where it lives, and how to call it. Keep this up to date to avoid reinventing solutions.

## Conventions
- repo_path: absolute path on Nibi/cluster where applicable.
- remote_url: git remote for external repos.
- commit: pinned commit SHA (update when intentionally upgrading).
- interface: CLI or file contract we rely on.
- notes: quirks, constraints, and reminders.

## Components

### archive-olm-pipeline (this repo)
- purpose: Orchestrates the pipeline; contains splitter, cleanup worker, direct submitter, finalizer, exporters, docs.
- repo_path: /home/jic823/projects/def-jic823/archive-olm-pipeline
- remote_url: https://github.com/jburnford/archive-olm-pipeline
- commit: pinned by normal git usage for this repo (use tags/branches as desired)
- interface:
  - Splitter: `python3 orchestration/split_jsonl_to_json.py <batch_dir> [--dry-run]`
  - Direct submit: `python3 streaming/direct_submit_batches.py --config <yaml> [--batches ...]`
  - Cleanup worker: `python3 streaming/file_based_cleanup.py --base-dir <dir>`
  - Finalizer: `python3 streaming/file_based_finalize.py --base-dir <dir>`
  - Exporters (planned): `tools/export_catalog_duckdb.py`, `tools/build_content_bundles.py`, `tools/metrics_summary.py`
- notes:
  - File-based pipeline; avoid symlinks in critical paths; prefer regular files or hard links.

### OLMoCR (external)
- purpose: OCR worker and submit scripts.
- repo_path: /home/jic823/projects/def-jic823/cluster/olmocr
- remote_url: (fill) e.g., https://github.com/<org>/olmocr
- commit: (fill) run `git -C /home/jic823/projects/def-jic823/cluster/olmocr rev-parse HEAD`
- interface:
  - SLURM worker script: `smart_process_pdf_chunks.slurm`
  - Submitter (not used directly now): `smart_submit_pdf_jobs.sh` (symlink-sensitive)
  - Environment: reads `PDF_DIR`, `WORKERS`, `PAGES_PER_GROUP` from exported env
- notes:
  - The submitter’s pending detection ignores symlinks (`find -type f`); direct submitter in this repo bypasses this.

### DuckDB (export/runtime)
- purpose: Local analytics for catalogs of documents and OCR content pointers.
- repo_path: N/A (binary/package)
- remote_url: https://duckdb.org
- commit: use packaged release
- interface: `duckdb export/catalog.duckdb` then SQL (see plan for example queries)
- notes: excellent for single-file local analysis of large datasets; no server needed.

## Version Pins
- See `_manifests/versions.json` for pinned commits and bootstrap instructions.
- Use `tools/bootstrap_components.sh` to clone/update external repos at pinned SHAs.

---

## Update Policy
- Prefer reuse of existing components.
- If you must modify behavior, update this file with:
  - what changed, why, pinned commit, interface and contract updates.
