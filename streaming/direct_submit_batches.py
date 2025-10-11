#!/usr/bin/env python3
"""
Directly submit existing batch PDFs to OLMoCR via SLURM.

Bypasses the smart_submit script's pending detection (which ignores symlinks),
packs PDFs into ~1500-page chunks, computes walltime, writes chunk files, and
invokes sbatch on OLMoCR's smart_process_pdf_chunks.slurm.

Usage:
  python3 streaming/direct_submit_batches.py --config config/caribbean_filebased.yaml

Optional flags:
  --max-pages-per-chunk 1500
  --time-per-page-seconds 6
  --startup-seconds 300
  --batches batch_0002 batch_0003 ...  (limit to specific batches)
"""

import argparse
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import yaml


def load_config(path: Path) -> Dict:
    with open(path) as f:
        return yaml.safe_load(f)


def get_pdfs(batch_dir: Path) -> List[Path]:
    pdfs = sorted([p for p in batch_dir.glob('*.pdf') if p.is_file() or p.is_symlink()])
    return pdfs


def pdf_pages(pdf_path: Path) -> int:
    try:
        r = subprocess.run(['pdfinfo', str(pdf_path)], capture_output=True, text=True, timeout=20)
        if r.returncode == 0:
            for line in r.stdout.splitlines():
                if line.startswith('Pages:'):
                    parts = line.split()
                    if len(parts) >= 2 and parts[1].isdigit():
                        return int(parts[1])
        return 1
    except Exception:
        return 1


def pack_chunks(pdfs: List[Path], max_pages: int) -> List[Tuple[List[str], int]]:
    chunks: List[Tuple[List[str], int]] = []
    current: List[str] = []
    pages_sum = 0

    for p in pdfs:
        pages = pdf_pages(p)
        if pages <= 0:
            pages = 1
        if pages_sum > 0 and pages_sum + pages > max_pages:
            chunks.append((current, pages_sum))
            current = []
            pages_sum = 0
        current.append(p.name)
        pages_sum += pages

    if current:
        chunks.append((current, pages_sum))
    return chunks


def format_walltime(pages: int, per_page: int, startup: int, safety: float = 0.2) -> str:
    total = startup + pages * per_page
    total = int(total + total * safety)
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def parse_job_id(output: str) -> str:
    for line in output.splitlines():
        if 'Submitted batch job' in line:
            return line.split()[-1]
    raise RuntimeError(f"Could not parse SLURM job ID from output:\n{output}")


def submit_chunk(slurm_script: Path, batch_dir: Path, chunk_idx: int, walltime: str) -> str:
    logs_dir = batch_dir / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Build safe --export list without embedding literal quotes in values
    exports = [
        'ALL',
        f'PDF_DIR={batch_dir}',
    ]
    workers = os.environ.get('WORKERS')
    if workers:
        exports.append(f'WORKERS={workers}')
    pages_per_group = os.environ.get('PAGES_PER_GROUP')
    if pages_per_group:
        exports.append(f'PAGES_PER_GROUP={pages_per_group}')
    env_export = ','.join(exports)

    cmd = [
        'sbatch',
        '--export', env_export,
        '--job-name', f'olmocr_pdf_{chunk_idx}',
        '--output', str(logs_dir / f'slurm-%j_{chunk_idx}.out'),
        '--time', walltime,
        '--array', str(chunk_idx),
        '--chdir', str(batch_dir),
        '--parsable',
        str(slurm_script)
    ]

    r = subprocess.run(cmd, capture_output=True, text=True)
    output = (r.stdout or '') + (r.stderr or '')
    if r.returncode != 0:
        raise RuntimeError(f"sbatch failed (code {r.returncode})\n{output}")
    job_id = (r.stdout or '').strip()
    if not job_id:
        # Fallback to parser if cluster did not honor --parsable
        return parse_job_id(output)
    return job_id


def update_batch_meta(batch_dir: Path, job_ids: List[str], total_pdfs: int):
    meta_file = batch_dir / 'batch.meta.json'
    if meta_file.exists():
        with open(meta_file) as f:
            meta = json.load(f)
    else:
        meta = {"batch_id": batch_dir.name}

    meta.update({
        'batch_id': batch_dir.name,
        'submitted_at': datetime.utcnow().isoformat() + 'Z',
        'status': 'submitted',
        'total_pdfs': total_pdfs,
        'slurm_job_ids': job_ids,
    })

    with open(meta_file, 'w') as f:
        json.dump(meta, f, indent=2)


def update_manifest(base_dir: Path, meta: Dict):
    manif = base_dir / '_manifests' / 'batches.json'
    batches: List[Dict] = []
    if manif.exists():
        with open(manif) as f:
            data = json.load(f)
            batches = data.get('batches', [])

    idx = {b.get('batch_id'): b for b in batches}
    idx[meta['batch_id']] = meta
    new_list = [idx[k] for k in sorted(idx.keys())]

    with open(manif, 'w') as f:
        json.dump({'batches': new_list, 'last_updated': datetime.utcnow().isoformat() + 'Z'}, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Directly submit batch PDFs to OLMoCR')
    parser.add_argument('--config', required=True, type=Path, help='File-based YAML config')
    parser.add_argument('--max-pages-per-chunk', type=int, default=1500)
    parser.add_argument('--time-per-page-seconds', type=int, default=6)
    parser.add_argument('--startup-seconds', type=int, default=300)
    parser.add_argument('--batches', nargs='*', help='Specific batch IDs to submit')
    args = parser.parse_args()

    cfg = load_config(args.config)
    base_dir = Path(cfg['directories']['base_dir'])
    olmocr_repo = Path(cfg['components']['olmocr_repo'])
    slurm_script = olmocr_repo / 'smart_process_pdf_chunks.slurm'

    processing_dir = base_dir / '03_ocr_processing'

    if args.batches:
        candidates = [processing_dir / b for b in args.batches]
    else:
        candidates = sorted(d for d in processing_dir.glob('batch_*') if d.is_dir())

    print('=' * 70)
    print('Direct OLMoCR Submission')
    print('=' * 70)
    print(f'Base: {base_dir}')
    print(f'SLURM script: {slurm_script}')
    print(f'Batches: {", ".join(b.name for b in candidates)}')

    submitted = 0
    skipped = 0
    errors = 0

    for batch_dir in candidates:
        if list((batch_dir / 'results').glob('**/*.jsonl')):
            print(f'  ↷ Skip {batch_dir.name}: results already present')
            skipped += 1
            continue

        pdfs = get_pdfs(batch_dir)
        if not pdfs:
            print(f'  ↷ Skip {batch_dir.name}: no PDFs found')
            skipped += 1
            continue

        (batch_dir / 'chunks').mkdir(parents=True, exist_ok=True)
        (batch_dir / 'results').mkdir(parents=True, exist_ok=True)
        (batch_dir / 'logs').mkdir(parents=True, exist_ok=True)

        for old in (batch_dir / 'chunks').glob('chunk_*.txt'):
            try:
                old.unlink()
            except Exception:
                pass

        print(f'  • Packing {len(pdfs)} PDFs in {batch_dir.name}...')
        chunks = pack_chunks(pdfs, args.max_pages_per_chunk)
        print(f'    → {len(chunks)} chunks')

        job_ids: List[str] = []
        for idx, (basenames, pages) in enumerate(chunks, start=1):
            chunk_file = batch_dir / 'chunks' / f'chunk_{idx}.txt'
            chunk_file.write_text('\n'.join(basenames) + '\n', encoding='utf-8')
            wall = format_walltime(pages, args.time_per_page_seconds, args.startup_seconds)
            try:
                job_id = submit_chunk(slurm_script, batch_dir, idx, wall)
                print(f'    ✓ Submitted chunk {idx} ({pages} pages) as job {job_id}')
                job_ids.append(job_id)
            except Exception as e:
                print(f'    ✗ Failed to submit chunk {idx}: {e}')
                errors += 1

        if job_ids:
            update_batch_meta(batch_dir, job_ids, len(pdfs))
            with open(batch_dir / 'batch.meta.json') as f:
                meta = json.load(f)
            update_manifest(base_dir, meta)
            submitted += 1

    print('\n' + '=' * 70)
    print('Summary')
    print('=' * 70)
    print(f'Submitted batches: {submitted}')
    print(f'Skipped batches: {skipped}')
    print(f'Errors: {errors}')


if __name__ == '__main__':
    main()
