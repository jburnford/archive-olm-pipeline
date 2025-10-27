#!/usr/bin/env python3
"""
Evaluate OLMoCR results against gold standard XML transcriptions.

Compares 100 PDF pages processed by OLMoCR against PAGE XML ground truth
to calculate Character Error Rate (CER) and Word Error Rate (WER).
"""

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Tuple
import re


def parse_page_xml_to_text(xml_path: Path) -> str:
    """
    Extract plain text from PAGE XML format.

    PAGE XML contains text in <TextEquiv><Unicode> tags within TextLine elements.
    We concatenate all text lines with newlines to preserve structure.
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # PAGE XML uses namespace
        ns = {'page': 'http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15'}

        # Find all Unicode elements (contain the transcribed text)
        text_lines = []
        for unicode_elem in root.findall('.//page:Unicode', ns):
            if unicode_elem.text:
                text_lines.append(unicode_elem.text)

        # Join with newlines to preserve line structure
        return '\n'.join(text_lines)

    except Exception as e:
        print(f"Error parsing {xml_path}: {e}")
        return ""


def parse_json_to_text(json_path: Path) -> str:
    """
    Extract plain text from OLMoCR JSON output.

    OLMoCR JSON format has 'text' field in each record.
    """
    try:
        with json_path.open('r', encoding='utf-8') as f:
            data = json.load(f)

        # Handle different possible structures
        if isinstance(data, list):
            # Multiple records - concatenate text fields
            texts = []
            for record in data:
                if isinstance(record, dict) and 'text' in record:
                    texts.append(record['text'])
            return '\n'.join(texts)

        elif isinstance(data, dict):
            # Single record
            if 'text' in data:
                return data['text']
            # Or nested in records
            elif 'records' in data:
                texts = [r['text'] for r in data['records'] if 'text' in r]
                return '\n'.join(texts)

        return ""

    except Exception as e:
        print(f"Error parsing {json_path}: {e}")
        return ""


def normalize_text(text: str) -> str:
    """
    Normalize text for fair comparison.

    - Normalize whitespace (multiple spaces/newlines to single)
    - Strip leading/trailing whitespace
    - Keep punctuation and case as-is for accurate error measurement
    """
    # Replace multiple whitespace with single space
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def calculate_cer(reference: str, hypothesis: str) -> Tuple[float, int, int]:
    """
    Calculate Character Error Rate using Levenshtein distance.

    CER = (substitutions + insertions + deletions) / len(reference)

    Returns:
        (cer, edit_distance, reference_length)
    """
    # Simple Levenshtein distance implementation
    ref_chars = list(reference)
    hyp_chars = list(hypothesis)

    m, n = len(ref_chars), len(hyp_chars)

    # Create distance matrix
    dp = [[0] * (n + 1) for _ in range(m + 1)]

    # Initialize first row and column
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j

    # Fill matrix
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if ref_chars[i-1] == hyp_chars[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = 1 + min(
                    dp[i-1][j],      # deletion
                    dp[i][j-1],      # insertion
                    dp[i-1][j-1]     # substitution
                )

    edit_distance = dp[m][n]
    cer = edit_distance / m if m > 0 else 0.0

    return cer, edit_distance, m


def calculate_wer(reference: str, hypothesis: str) -> Tuple[float, int, int]:
    """
    Calculate Word Error Rate using Levenshtein distance on words.

    WER = (substitutions + insertions + deletions) / len(reference_words)
    """
    ref_words = reference.split()
    hyp_words = hypothesis.split()

    m, n = len(ref_words), len(hyp_words)

    # Create distance matrix
    dp = [[0] * (n + 1) for _ in range(m + 1)]

    # Initialize
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j

    # Fill matrix
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if ref_words[i-1] == hyp_words[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = 1 + min(
                    dp[i-1][j],
                    dp[i][j-1],
                    dp[i-1][j-1]
                )

    edit_distance = dp[m][n]
    wer = edit_distance / m if m > 0 else 0.0

    return wer, edit_distance, m


def match_xml_to_json(xml_path: Path, json_dir: Path) -> Path:
    """
    Find matching JSON file for an XML file.

    XML: "Author - Year - Title_page_N.xml"
    JSON: "Author - Year - Title_page_N.json"
    """
    json_name = xml_path.stem + '.json'
    json_path = json_dir / json_name
    return json_path


def evaluate_single_file(xml_path: Path, json_path: Path) -> Dict:
    """
    Evaluate a single file pair.
    """
    # Parse both files
    gold_text = parse_page_xml_to_text(xml_path)
    ocr_text = parse_json_to_text(json_path)

    # Normalize
    gold_norm = normalize_text(gold_text)
    ocr_norm = normalize_text(ocr_text)

    # Calculate metrics
    cer, cer_edits, cer_ref_len = calculate_cer(gold_norm, ocr_norm)
    wer, wer_edits, wer_ref_len = calculate_wer(gold_norm, ocr_norm)

    return {
        'filename': xml_path.stem,
        'gold_chars': len(gold_norm),
        'ocr_chars': len(ocr_norm),
        'gold_words': len(gold_norm.split()),
        'ocr_words': len(ocr_norm.split()),
        'cer': cer,
        'cer_edits': cer_edits,
        'wer': wer,
        'wer_edits': wer_edits,
        'gold_text_preview': gold_norm[:100],
        'ocr_text_preview': ocr_norm[:100],
    }


def main():
    """
    Evaluate all 100 files and generate report.
    """
    # Paths
    xml_dir = Path('/home/jic823/archive-olm-pipeline/Corpus_for_Transcribing/page')
    json_dir = Path('/home/jic823/projects/def-jic823/pdfs_jacob/results/json')

    # Check if JSON dir exists (on Nibi cluster)
    if not json_dir.exists():
        print(f"JSON directory not found: {json_dir}")
        print("This script should be run on Nibi cluster where JSON files are located.")
        return 1

    # Get all XML files
    xml_files = sorted(xml_dir.glob('*.xml'))
    # Filter out Zone.Identifier and attrs files
    xml_files = [f for f in xml_files if f.suffix == '.xml' and 'Zone.Identifier' not in f.name and 'com.dropbox' not in f.name]

    print("=" * 80)
    print("OLMoCR Evaluation - Jacob's Caribbean Collection")
    print("=" * 80)
    print(f"Gold standard XML files: {len(xml_files)}")
    print(f"XML directory: {xml_dir}")
    print(f"JSON directory: {json_dir}")
    print("-" * 80)

    results = []
    missing_json = []

    for xml_path in xml_files:
        json_path = match_xml_to_json(xml_path, json_dir)

        if not json_path.exists():
            missing_json.append(xml_path.name)
            continue

        result = evaluate_single_file(xml_path, json_path)
        results.append(result)

        # Print progress
        print(f"✓ {result['filename'][:60]:60} | CER: {result['cer']:.3f} | WER: {result['wer']:.3f}")

    if missing_json:
        print("\n⚠ Missing JSON files:")
        for name in missing_json:
            print(f"  - {name}")

    if not results:
        print("\n❌ No matching file pairs found!")
        return 1

    # Calculate aggregate statistics
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)

    total_files = len(results)
    avg_cer = sum(r['cer'] for r in results) / total_files
    avg_wer = sum(r['wer'] for r in results) / total_files

    total_gold_chars = sum(r['gold_chars'] for r in results)
    total_cer_edits = sum(r['cer_edits'] for r in results)
    overall_cer = total_cer_edits / total_gold_chars if total_gold_chars > 0 else 0

    total_gold_words = sum(r['gold_words'] for r in results)
    total_wer_edits = sum(r['wer_edits'] for r in results)
    overall_wer = total_wer_edits / total_gold_words if total_gold_words > 0 else 0

    print(f"Files evaluated: {total_files}")
    print(f"Total gold standard characters: {total_gold_chars:,}")
    print(f"Total gold standard words: {total_gold_words:,}")
    print()
    print(f"Average CER (per file): {avg_cer:.4f} ({avg_cer*100:.2f}%)")
    print(f"Overall CER (corpus):   {overall_cer:.4f} ({overall_cer*100:.2f}%)")
    print()
    print(f"Average WER (per file): {avg_wer:.4f} ({avg_wer*100:.2f}%)")
    print(f"Overall WER (corpus):   {overall_wer:.4f} ({overall_wer*100:.2f}%)")

    # Best and worst files
    results_sorted_cer = sorted(results, key=lambda x: x['cer'])
    results_sorted_wer = sorted(results, key=lambda x: x['wer'])

    print("\n" + "-" * 80)
    print("BEST 5 FILES (by CER):")
    for r in results_sorted_cer[:5]:
        print(f"  {r['filename'][:60]:60} | CER: {r['cer']:.4f}")

    print("\nWORST 5 FILES (by CER):")
    for r in results_sorted_cer[-5:]:
        print(f"  {r['filename'][:60]:60} | CER: {r['cer']:.4f}")

    print("\n" + "-" * 80)
    print("BEST 5 FILES (by WER):")
    for r in results_sorted_wer[:5]:
        print(f"  {r['filename'][:60]:60} | WER: {r['wer']:.4f}")

    print("\nWORST 5 FILES (by WER):")
    for r in results_sorted_wer[-5:]:
        print(f"  {r['filename'][:60]:60} | WER: {r['wer']:.4f}")

    # Save detailed results to JSON
    output_file = Path('jacob_evaluation_results.json')
    with output_file.open('w', encoding='utf-8') as f:
        json.dump({
            'summary': {
                'total_files': total_files,
                'total_gold_chars': total_gold_chars,
                'total_gold_words': total_gold_words,
                'average_cer': avg_cer,
                'overall_cer': overall_cer,
                'average_wer': avg_wer,
                'overall_wer': overall_wer,
            },
            'per_file_results': results,
        }, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 80)
    print(f"✓ Detailed results saved to: {output_file}")
    print("=" * 80)

    return 0


if __name__ == '__main__':
    exit(main())
