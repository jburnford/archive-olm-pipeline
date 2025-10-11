# Interfaces and Contracts

Reference for calling conventions and file layouts. Keep this stable to avoid breakage.

## Splitter: `orchestration/split_jsonl_to_json.py`
- Input: `<batch_dir>/results/results/*.jsonl` (recursive) from OLMoCR.
- Behavior: parses JSONL lines, recursively extracts nested records; groups by `metadata.Source-File` (or variants) → filename.
- Output: `<batch_dir>/results/json/<pdf_name>.json` (array of records).
- Keys expected: `metadata.Source-File | source_file | source | filename | file_name | path | filepath`.

## Cleanup Worker: `streaming/file_based_cleanup.py`
- Input: `<base>/03_ocr_processing/batch_XXXX/results/**/*.jsonl`.
- Behavior: splits combined JSONL into per-identifier `.ocr.jsonl` files in `<base>/04_ocr_completed/`.
- Output: `<identifier>.ocr.jsonl` and `<identifier>.meta.json` in `04_ocr_completed/`.

## Direct Submit: `streaming/direct_submit_batches.py`
- Input: batch directory with `*.pdf` files.
- Behavior: uses `pdfinfo` to count pages, packs ~1,500 pages per chunk; writes `chunks/chunk_N.txt` with basenames.
- Submission: `sbatch smart_process_pdf_chunks.slurm` with `--export ALL,PDF_DIR=<batch_dir>,WORKERS=...,PAGES_PER_GROUP=...` and `--array N`.
- Output: job IDs recorded in `batch.meta.json` and `_manifests/batches.json`.

## Finalizer: `streaming/file_based_finalize.py`
- Input: `03_ocr_processing/batch_XXXX/results/json/*.json` and `01_downloaded/*.meta.json`.
- Behavior: links OCR JSON and download metadata; writes consolidated outputs and deletes source PDFs on success.
- Output: `05_processed/<identifier>/<pdf_basename>.ocr.json`, `05_processed/<identifier>/<identifier>.meta.json`.
- Safety: deletes PDFs only after writing outputs.

## Exporters (planned)
- Catalog: `tools/export_catalog_duckdb.py`
  - Reads `05_processed/`, writes `export/catalog.duckdb` with `documents` and `ocr_docs` tables.
- Bundles: `tools/build_content_bundles.py`
  - Packs `05_processed/<identifier>/*.ocr.json` into `export/bundles/<shard>.tar.gz`; writes checksums and `export/manifest.csv.gz`.
- Metrics: `tools/metrics_summary.py`
  - Summarizes `_manifests/metrics.ndjson` and sacct to `export/metrics_summary.json`.

## Directory Sharding (planned)
- Two-level shard by first 2 hex chars of a stable hash (e.g., SHA1(identifier)):
  - `01_downloaded/ab/<file>.pdf`
  - `05_processed/ab/<identifier>/...`

## Manifests
- `_manifests/batches.json` (current) → move to append-only `batches.ndjson` with periodic compaction.
- `_manifests/metrics.ndjson`: append-only. Summarize to `export/metrics_summary.json`.
