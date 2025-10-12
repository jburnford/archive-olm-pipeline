#!/usr/bin/env python3
"""
Example DuckDB Queries for Archive Pipeline Catalog

Usage:
  python3 tools/query_examples.py ~/projects/def-jic823/caribbean_pipeline/export/catalog.duckdb
"""

import sys
import duckdb
from pathlib import Path


def query_summary(con):
    """Basic statistics."""
    print("=" * 70)
    print("CATALOG SUMMARY")
    print("=" * 70)

    result = con.execute("""
        SELECT
            COUNT(*) as docs,
            COUNT(DISTINCT collection) as collections,
            MIN(year) as earliest,
            MAX(year) as latest
        FROM documents
    """).fetchone()

    print(f"Documents:   {result[0]:,}")
    print(f"Collections: {result[1]:,}")
    print(f"Years:       {result[2]} - {result[3]}")
    print()


def query_collections(con):
    """Collection breakdown."""
    print("=" * 70)
    print("TOP COLLECTIONS")
    print("=" * 70)

    results = con.execute("""
        SELECT
            collection,
            COUNT(*) as count
        FROM documents
        WHERE collection IS NOT NULL
        GROUP BY collection
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    for coll, count in results:
        print(f"{count:6,} docs | {coll}")
    print()


def query_timeline(con):
    """Documents by decade."""
    print("=" * 70)
    print("TIMELINE (by decade)")
    print("=" * 70)

    results = con.execute("""
        SELECT
            (year / 10) * 10 as decade,
            COUNT(*) as count
        FROM documents
        WHERE year IS NOT NULL
        GROUP BY decade
        ORDER BY decade
    """).fetchall()

    for decade, count in results:
        print(f"{decade}s: {'█' * (count // 10)} {count:,} docs")
    print()


def query_batches(con):
    """Processing batches."""
    print("=" * 70)
    print("PROCESSING BATCHES")
    print("=" * 70)

    results = con.execute("""
        SELECT batch_id, pdf_count, chunk_count, status
        FROM batches
        ORDER BY batch_id
    """).fetchall()

    if not results:
        print("No batch data available")
    else:
        for batch_id, pdf_count, chunk_count, status in results:
            print(f"Batch {batch_id:04d}: {pdf_count:4} PDFs in {chunk_count:2} chunks [{status}]")
    print()


def export_subset_example(con):
    """Example: Export 18th century documents."""
    print("=" * 70)
    print("EXPORT EXAMPLE: 18th Century Documents")
    print("=" * 70)

    results = con.execute("""
        SELECT
            identifier,
            title,
            year,
            archive_url
        FROM documents
        WHERE year BETWEEN 1700 AND 1799
        ORDER BY year, title
        LIMIT 10
    """).fetchall()

    print("First 10 documents from 1700-1799:")
    for identifier, title, year, url in results:
        title_short = title[:50] if title else 'N/A'
        print(f"  {year}: {title_short}")
        print(f"       {url}")

    # Show export command
    print()
    print("To export all 18th century docs to CSV:")
    print("  COPY (SELECT * FROM documents WHERE year BETWEEN 1700 AND 1799)")
    print("  TO '18th_century.csv' (HEADER, DELIMITER ',');")
    print()


def search_example(con):
    """Example: Search by title."""
    print("=" * 70)
    print("SEARCH EXAMPLE: Find documents mentioning 'Jamaica'")
    print("=" * 70)

    results = con.execute("""
        SELECT identifier, title, year
        FROM documents
        WHERE title LIKE '%Jamaica%'
        ORDER BY year
        LIMIT 10
    """).fetchall()

    if not results:
        print("No matches found")
    else:
        for identifier, title, year in results:
            print(f"{year}: {title}")
    print()


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path-to-catalog.duckdb>")
        sys.exit(1)

    catalog_path = Path(sys.argv[1])
    if not catalog_path.exists():
        print(f"Error: Catalog not found: {catalog_path}")
        sys.exit(1)

    print(f"\nQuerying: {catalog_path}\n")

    con = duckdb.connect(str(catalog_path), read_only=True)

    query_summary(con)
    query_collections(con)
    query_timeline(con)
    query_batches(con)
    export_subset_example(con)
    search_example(con)

    con.close()

    print("=" * 70)
    print("MORE QUERIES")
    print("=" * 70)
    print()
    print("# Interactive SQL:")
    print(f"  duckdb {catalog_path}")
    print()
    print("# Python API:")
    print("  import duckdb")
    print(f"  con = duckdb.connect('{catalog_path}', read_only=True)")
    print("  df = con.execute('SELECT * FROM documents LIMIT 10').df()")
    print()


if __name__ == "__main__":
    main()
