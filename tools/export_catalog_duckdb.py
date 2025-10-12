#!/usr/bin/env python3
"""
Export a lightweight DuckDB catalog of processed documents.

Scans 05_processed/<identifier>/ to collect document metadata and OCR pointers,
then writes tables to export/catalog.duckdb:

- documents(identifier PRIMARY KEY, title, collection, creator, year,
             original_filename, ocr_json, ocr_markdown, processed_dir,
             file_size_bytes, md_size_bytes, ocr_consolidated_at)
- ocr_docs(identifier, pdf_name, pages_count, text_bytes, ocr_json_path)

Usage:
  python3 tools/export_catalog_duckdb.py     --base-dir /home/USER/projects/.../caribbean_pipeline     [--out export/catalog.duckdb] [--fast]

Notes:
- "--fast" skips JSON parsing for page counts (sets pages_count to NULL).
- Safe to re-run; replaces the output file atomically.
"""

import argparse
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb  # type: ignore


@dataclass
class DocumentRow:
    identifier: str
    title: Optional[str]
    collection: Optional[str]
    creator: Optional[str]
    year: Optional[int]
    original_filename: Optional[str]
    ocr_json: Optional[str]
    ocr_markdown: Optional[str]
    processed_dir: str
    file_size_bytes: Optional[int]
    md_size_bytes: Optional[int]
    ocr_consolidated_at: Optional[str]


@dataclass
class OcrDocRow:
    identifier: str
    pdf_name: str
    pages_count: Optional[int]
    text_bytes: Optional[int]
    ocr_json_path: str


def load_meta(meta_path: Path) -> Optional[dict]:
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def count_pages_in_json(json_path: Path) -> Optional[int]:
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return len(data)
        # Some formats might be dict with pages field
        if isinstance(data, dict) and "pages" in data and isinstance(data["pages"], list):
            return len(data["pages"])
    except Exception:
        return None
    return None


def scan_processed(base_dir: Path, fast: bool = False) -> Tuple[List[DocumentRow], List[OcrDocRow]]:
    processed = base_dir / "02_processed"
    documents: List[DocumentRow] = []
    ocr_docs: List[OcrDocRow] = []

    if not processed.exists():
        return documents, ocr_docs

    for id_dir in sorted([d for d in processed.iterdir() if d.is_dir()]):
        identifier = id_dir.name
        meta_path = id_dir / f"{identifier}.meta.json"
        meta = load_meta(meta_path) if meta_path.exists() else None

        # Determine OCR JSON path
        ocr_json_path: Optional[Path] = None
        if meta and isinstance(meta.get("ocr_json"), str):
            ocr_json_path = Path(meta["ocr_json"]) if Path(meta["ocr_json"]).is_absolute() else (id_dir / Path(meta["ocr_json"]).name)
            if not ocr_json_path.exists():  # fallback to listing
                ocr_json_path = None
        if ocr_json_path is None:
            cands = list(id_dir.glob("*.ocr.json"))
            if cands:
                ocr_json_path = cands[0]

        # Determine Markdown path if present
        md_path: Optional[Path] = None
        if meta and isinstance(meta.get("ocr_markdown"), str):
            md_path = Path(meta["ocr_markdown"]) if Path(meta["ocr_markdown"]).is_absolute() else (id_dir / Path(meta["ocr_markdown"]).name)
            if not md_path.exists():
                md_path = None
        if md_path is None:
            cands_md = list(id_dir.glob("*.md"))
            if cands_md:
                md_path = cands_md[0]

        # Extract key fields from metadata
        title = meta.get("title") if meta else None
        collection = meta.get("collection") if meta else None
        creator = meta.get("creator") if meta else None
        year = meta.get("year") if meta else None
        ocr_consolidated_at = meta.get("ocr_consolidated_at") if meta else None
        original_filename = meta.get("original_filename") if meta else None

        file_size_bytes = None
        pages_count = None
        text_bytes = None

        if ocr_json_path and ocr_json_path.exists():
            try:
                file_size_bytes = ocr_json_path.stat().st_size
            except Exception:
                pass
            if not fast:
                pages_count = count_pages_in_json(ocr_json_path)
            text_bytes = file_size_bytes

        md_size_bytes = None
        if md_path and md_path.exists():
            try:
                md_size_bytes = md_path.stat().st_size
            except Exception:
                pass

        documents.append(
            DocumentRow(
                identifier=identifier,
                title=title,
                collection=collection,
                creator=creator,
                year=year,
                original_filename=original_filename,
                ocr_json=str(ocr_json_path) if ocr_json_path else None,
                ocr_markdown=str(md_path) if md_path else None,
                processed_dir=str(id_dir),
                file_size_bytes=file_size_bytes,
                md_size_bytes=md_size_bytes,
                ocr_consolidated_at=ocr_consolidated_at,
            )
        )

        if ocr_json_path:
            ocr_docs.append(
                OcrDocRow(
                    identifier=identifier,
                    pdf_name=(Path(original_filename).name if original_filename else ocr_json_path.stem.replace('.ocr','')),
                    pages_count=pages_count,
                    text_bytes=text_bytes,
                    ocr_json_path=str(ocr_json_path),
                )
            )

    return documents, ocr_docs


def write_duckdb(out_path: Path, documents: List[DocumentRow], ocr_docs: List[OcrDocRow]):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Write to temp then move for atomicity
    with tempfile.TemporaryDirectory() as tmpd:
        tmp_path = Path(tmpd) / out_path.name
        con = duckdb.connect(str(tmp_path))
        con.execute(
            """
            CREATE TABLE documents (
                identifier TEXT PRIMARY KEY,
                title TEXT,
                collection TEXT,
                creator TEXT,
                year INTEGER,
                original_filename TEXT,
                ocr_json TEXT,
                ocr_markdown TEXT,
                processed_dir TEXT,
                file_size_bytes BIGINT,
                md_size_bytes BIGINT,
                ocr_consolidated_at TEXT
            );
            """
        )
        con.execute(
            """
            CREATE TABLE ocr_docs (
                identifier TEXT,
                pdf_name TEXT,
                pages_count INTEGER,
                text_bytes BIGINT,
                ocr_json_path TEXT
            );
            """
        )

        con.executemany(
            """
            INSERT INTO documents (
              identifier,title,collection,creator,year,original_filename,
              ocr_json,ocr_markdown,processed_dir,file_size_bytes,md_size_bytes,ocr_consolidated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [(
                d.identifier, d.title, d.collection, d.creator, d.year, d.original_filename,
                d.ocr_json, d.ocr_markdown, d.processed_dir, d.file_size_bytes, d.md_size_bytes,
                d.ocr_consolidated_at
            ) for d in documents]
        )
        con.executemany(
            """
            INSERT INTO ocr_docs (
              identifier,pdf_name,pages_count,text_bytes,ocr_json_path
            ) VALUES (?,?,?,?,?)
            """,
            [(
                o.identifier, o.pdf_name, o.pages_count, o.text_bytes, o.ocr_json_path
            ) for o in ocr_docs]
        )
        con.close()
        # Move into place
        if out_path.exists():
            out_path.unlink()
        os.replace(tmp_path, out_path)


def main():
    ap = argparse.ArgumentParser(description="Export a DuckDB catalog of processed OCR outputs")
    ap.add_argument("--base-dir", type=Path, required=True, help="Pipeline base directory")
    ap.add_argument("--out", type=Path, default=None, help="Output DuckDB path (default: <base>/export/catalog.duckdb)")
    ap.add_argument("--fast", action="store_true", help="Skip page counting (faster, sets pages_count NULL)")
    args = ap.parse_args()

    base_dir = args.base_dir
    out_path = args.out or (base_dir / "export" / "catalog.duckdb")

    print("=" * 70)
    print("Export Catalog (DuckDB)")
    print("=" * 70)
    print(f"Base: {base_dir}")
    print(f"Out:  {out_path}")
    print(f"Mode: {'FAST' if args.fast else 'STANDARD'}")

    documents, ocr_docs = scan_processed(base_dir, fast=args.fast)
    print(f"Documents found: {len(documents)} | OCR docs: {len(ocr_docs)}")

    write_duckdb(out_path, documents, ocr_docs)
    print("✓ Wrote catalog:", out_path)


if __name__ == "__main__":
    main()
