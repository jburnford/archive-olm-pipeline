#!/usr/bin/env python3
"""
Enhanced DuckDB Catalog Export

Creates comprehensive warehouse with:
- documents: One row per Archive.org item
- pages: One row per page (optional, for full-text search)
- batches: Processing batch tracking

Usage:
  python3 tools/export_catalog_enhanced.py \
    --base-dir ~/projects/def-jic823/caribbean_pipeline \
    [--include-pages]  # Extract page-level data (slower)
    [--fast]           # Skip page counting
"""

import argparse
import json
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
import os

import duckdb


@dataclass
class DocumentRow:
    identifier: str
    title: Optional[str]
    collection: Optional[str]
    creator: Optional[str]
    year: Optional[int]
    language: Optional[str]
    subjects: Optional[str]  # JSON array as string
    original_filename: Optional[str]
    pdf_size_bytes: Optional[int]
    ocr_json_path: Optional[str]
    ocr_markdown_path: Optional[str]
    total_pages: Optional[int]
    total_chars: Optional[int]
    processed_at: Optional[str]
    batch_id: Optional[int]
    processed_dir: str
    archive_url: Optional[str]


@dataclass
class PageRow:
    identifier: str
    page_number: int
    text_content: str
    char_count: int
    word_count: int


@dataclass
class BatchRow:
    batch_id: int
    submitted_at: Optional[str]
    pdf_count: int
    total_pages: Optional[int]
    chunk_count: int
    status: str


def load_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def extract_pages(ocr_json_path: Path) -> List[dict]:
    """Extract page-level data from OCR JSON."""
    try:
        data = json.loads(ocr_json_path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "pages" in data:
            return data["pages"]
    except Exception:
        pass
    return []


def scan_processed(
    base_dir: Path,
    include_pages: bool = False,
    fast: bool = False
) -> Tuple[List[DocumentRow], List[PageRow]]:
    """Scan 02_processed/ for documents and optionally extract pages."""
    processed_dir = base_dir / "02_processed"
    documents: List[DocumentRow] = []
    all_pages: List[PageRow] = []

    if not processed_dir.exists():
        return documents, all_pages

    for id_dir in sorted([d for d in processed_dir.iterdir() if d.is_dir()]):
        identifier = id_dir.name
        meta_path = id_dir / f"{identifier}.meta.json"
        meta = load_json(meta_path) if meta_path.exists() else {}

        # Find OCR JSON
        ocr_json_path: Optional[Path] = None
        json_files = list(id_dir.glob("*.json"))
        for jf in json_files:
            if jf.name != meta_path.name:
                ocr_json_path = jf
                break

        # Find markdown
        md_path: Optional[Path] = None
        md_files = list(id_dir.glob("*.md"))
        if md_files:
            md_path = md_files[0]

        # Extract metadata
        title = meta.get("title")
        collection = meta.get("collection")
        creator = meta.get("creator")
        year = meta.get("year")
        language = meta.get("language")
        subjects = json.dumps(meta.get("subject", [])) if meta.get("subject") else None
        original_filename = meta.get("filename") or meta.get("original_filename")
        processed_at = meta.get("ocr_consolidated_at")
        batch_id = meta.get("batch_id")

        # Archive.org URL
        archive_url = f"https://archive.org/details/{identifier}" if identifier else None

        # File sizes and page counts
        pdf_size_bytes = None
        total_pages = None
        total_chars = None

        if ocr_json_path and ocr_json_path.exists():
            try:
                pdf_size_bytes = ocr_json_path.stat().st_size
            except Exception:
                pass

            if not fast:
                pages_data = extract_pages(ocr_json_path)
                total_pages = len(pages_data)
                total_chars = sum(len(p.get("text", "")) for p in pages_data if isinstance(p, dict))

                # Extract page-level data if requested
                if include_pages:
                    for page_num, page_data in enumerate(pages_data, 1):
                        if isinstance(page_data, dict):
                            text = page_data.get("text", "")
                            all_pages.append(PageRow(
                                identifier=identifier,
                                page_number=page_num,
                                text_content=text,
                                char_count=len(text),
                                word_count=len(text.split())
                            ))

        documents.append(DocumentRow(
            identifier=identifier,
            title=title,
            collection=collection,
            creator=creator,
            year=year,
            language=language,
            subjects=subjects,
            original_filename=original_filename,
            pdf_size_bytes=pdf_size_bytes,
            ocr_json_path=str(ocr_json_path) if ocr_json_path else None,
            ocr_markdown_path=str(md_path) if md_path else None,
            total_pages=total_pages,
            total_chars=total_chars,
            processed_at=processed_at,
            batch_id=batch_id,
            processed_dir=str(id_dir),
            archive_url=archive_url
        ))

    return documents, all_pages


def scan_batches(base_dir: Path) -> List[BatchRow]:
    """Scan chunks/ directory to reconstruct batch information."""
    chunks_dir = base_dir / "01_downloaded" / "chunks"
    batches: List[BatchRow] = []

    if not chunks_dir.exists():
        return batches

    # Group chunk files by batch
    batch_files = {}
    for chunk_file in chunks_dir.glob("batch_*_chunk_*.txt"):
        # Extract batch number from filename like "batch_0001_chunk_003.txt"
        parts = chunk_file.stem.split('_')
        if len(parts) >= 4:
            batch_num = int(parts[1])
            if batch_num not in batch_files:
                batch_files[batch_num] = []
            batch_files[batch_num].append(chunk_file)

    for batch_num, chunk_list in sorted(batch_files.items()):
        # Count PDFs across all chunks
        pdf_count = 0
        for chunk_file in chunk_list:
            try:
                pdf_count += len(chunk_file.read_text().strip().split('\n'))
            except Exception:
                pass

        batches.append(BatchRow(
            batch_id=batch_num,
            submitted_at=None,  # Could extract from SLURM logs
            pdf_count=pdf_count,
            total_pages=None,
            chunk_count=len(chunk_list),
            status='unknown'
        ))

    return batches


def write_catalog(
    out_path: Path,
    documents: List[DocumentRow],
    pages: List[PageRow],
    batches: List[BatchRow]
):
    """Write enhanced catalog with documents, pages, and batches tables."""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpd:
        tmp_path = Path(tmpd) / out_path.name
        con = duckdb.connect(str(tmp_path))

        # Create documents table
        con.execute("""
            CREATE TABLE documents (
                identifier TEXT PRIMARY KEY,
                title TEXT,
                collection TEXT,
                creator TEXT,
                year INTEGER,
                language TEXT,
                subjects TEXT,
                original_filename TEXT,
                pdf_size_bytes BIGINT,
                ocr_json_path TEXT,
                ocr_markdown_path TEXT,
                total_pages INTEGER,
                total_chars BIGINT,
                processed_at TIMESTAMP,
                batch_id INTEGER,
                processed_dir TEXT,
                archive_url TEXT
            );
        """)

        # Create pages table (if data provided)
        if pages:
            con.execute("""
                CREATE TABLE pages (
                    identifier TEXT,
                    page_number INTEGER,
                    text_content TEXT,
                    char_count INTEGER,
                    word_count INTEGER,
                    PRIMARY KEY (identifier, page_number)
                );
            """)

        # Create batches table
        con.execute("""
            CREATE TABLE batches (
                batch_id INTEGER PRIMARY KEY,
                submitted_at TIMESTAMP,
                pdf_count INTEGER,
                total_pages INTEGER,
                chunk_count INTEGER,
                status TEXT
            );
        """)

        # Insert documents
        con.executemany("""
            INSERT INTO documents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [(
            d.identifier, d.title, d.collection, d.creator, d.year, d.language,
            d.subjects, d.original_filename, d.pdf_size_bytes, d.ocr_json_path,
            d.ocr_markdown_path, d.total_pages, d.total_chars, d.processed_at,
            d.batch_id, d.processed_dir, d.archive_url
        ) for d in documents])

        # Insert pages
        if pages:
            con.executemany("""
                INSERT INTO pages VALUES (?,?,?,?,?)
            """, [(
                p.identifier, p.page_number, p.text_content, p.char_count, p.word_count
            ) for p in pages])

        # Insert batches
        if batches:
            con.executemany("""
                INSERT INTO batches VALUES (?,?,?,?,?,?)
            """, [(
                b.batch_id, b.submitted_at, b.pdf_count, b.total_pages,
                b.chunk_count, b.status
            ) for b in batches])

        con.close()

        # Move to final location (use shutil.move for cross-filesystem support)
        if out_path.exists():
            out_path.unlink()
        shutil.move(str(tmp_path), str(out_path))


def main():
    parser = argparse.ArgumentParser(description="Export enhanced DuckDB catalog")
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--include-pages", action="store_true", help="Extract page-level text (slower)")
    parser.add_argument("--fast", action="store_true", help="Skip page counting")
    args = parser.parse_args()

    base_dir = args.base_dir
    out_path = args.out or (base_dir / "export" / "catalog.duckdb")

    print("=" * 70)
    print("Enhanced DuckDB Catalog Export")
    print("=" * 70)
    print(f"Base:          {base_dir}")
    print(f"Output:        {out_path}")
    print(f"Include pages: {args.include_pages}")
    print(f"Mode:          {'FAST' if args.fast else 'STANDARD'}")
    print("-" * 70)

    print("Scanning documents...")
    documents, pages = scan_processed(base_dir, args.include_pages, args.fast)
    print(f"  Documents: {len(documents)}")
    print(f"  Pages:     {len(pages)}")

    print("Scanning batches...")
    batches = scan_batches(base_dir)
    print(f"  Batches:   {len(batches)}")

    print("Writing catalog...")
    write_catalog(out_path, documents, pages, batches)

    print("-" * 70)
    print(f"✅ Catalog written: {out_path}")
    print(f"   Size: {out_path.stat().st_size / 1024 / 1024:.1f} MB")
    print()
    print("Query with: duckdb " + str(out_path))
    print("=" * 70)


if __name__ == "__main__":
    main()
