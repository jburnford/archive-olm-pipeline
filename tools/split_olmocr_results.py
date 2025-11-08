#!/usr/bin/env python3
"""
Split OLMoCR JSONL results into individual JSON files per PDF.

Reads JSONL files from olmocr output and creates one JSON file per source PDF,
using the PDF filename (without .pdf extension) as the output filename.

Usage:
    python3 split_olmocr_results.py <results_dir> <output_dir>

Arguments:
    results_dir: Directory containing output_*.jsonl files
    output_dir:  Directory where split JSON files will be written

Example:
    python3 split_olmocr_results.py batch_0001/results/results/ batch_0001/split/
"""

import json
import sys
from pathlib import Path
from collections import defaultdict


def split_jsonl_by_pdf(results_dir: Path, output_dir: Path):
    """
    Split JSONL files by source PDF.

    Args:
        results_dir: Directory containing output_*.jsonl files
        output_dir: Directory to write split JSON files
    """
    results_dir = Path(results_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect all documents by source PDF
    pdf_documents = defaultdict(list)

    # Process all JSONL files
    jsonl_files = list(results_dir.glob("output_*.jsonl"))

    if not jsonl_files:
        print(f"ERROR: No output_*.jsonl files found in {results_dir}")
        sys.exit(1)

    print(f"Found {len(jsonl_files)} JSONL files to process")

    total_docs = 0
    for jsonl_file in sorted(jsonl_files):
        print(f"Processing: {jsonl_file.name}")

        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    doc = json.loads(line)

                    # Extract source PDF path from metadata
                    metadata = doc.get('metadata', {})
                    source_file = metadata.get('Source-File')

                    if not source_file:
                        print(f"  WARNING: No Source-File in document {line_num}, skipping")
                        continue

                    # Get PDF basename without extension
                    pdf_name = Path(source_file).stem

                    # Add document to this PDF's collection
                    pdf_documents[pdf_name].append(doc)
                    total_docs += 1

                except json.JSONDecodeError as e:
                    print(f"  ERROR: Invalid JSON on line {line_num}: {e}")
                    continue

    print(f"\nProcessed {total_docs} documents from {len(pdf_documents)} PDFs")

    # Write one JSON file per PDF
    for pdf_name, documents in sorted(pdf_documents.items()):
        output_file = output_dir / f"{pdf_name}.json"

        # Create consolidated structure
        output_data = {
            "pdf_name": pdf_name,
            "total_pages": len(documents),
            "metadata": documents[0]['metadata'] if documents else {},
            "pages": documents
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(f"  {pdf_name}: {len(documents)} pages → {output_file.name}")

    print(f"\n✅ Successfully split {total_docs} documents into {len(pdf_documents)} JSON files")
    print(f"   Output directory: {output_dir}")


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)

    results_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])

    if not results_dir.exists():
        print(f"ERROR: Results directory not found: {results_dir}")
        sys.exit(1)

    split_jsonl_by_pdf(results_dir, output_dir)


if __name__ == '__main__':
    main()
