#!/usr/bin/env python3
"""
Analyze Chandra's substitution errors - high CER but normal length.

Goal: Understand if these are:
- Spelling modernization (e.g., "colour" → "color")
- Meaning changes (wrong words)
- OCR character substitutions (e.g., "m" → "rn")
"""

import sqlite3
from pathlib import Path
import difflib


def get_chandra_substitution_errors(db_path: Path):
    """
    Get Chandra documents with high errors but normal length.

    Normal length = within 1.5x of Tesseract length
    High errors = CER >= 10%
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            m.short_id,
            m.cer,
            m.wer,
            o_chandra.character_count as chandra_chars,
            o_chandra.word_count as chandra_words,
            o_tess.character_count as tess_chars,
            o_gt.character_count as gt_chars
        FROM evaluation_metrics_strict m
        JOIN ocr_results o_chandra ON m.short_id = o_chandra.short_id
            AND m.system_id = o_chandra.system_id
        JOIN ocr_results o_tess ON m.short_id = o_tess.short_id
            AND o_tess.system_id = 'tesseract_v4_newspapers'
        JOIN ocr_results o_gt ON m.short_id = o_gt.short_id
            AND o_gt.system_id = 'gold_standard'
        WHERE m.system_id = 'chandra'
            AND m.cer >= 0.10
            AND (o_chandra.character_count / o_tess.character_count BETWEEN 0.67 AND 1.5)
        ORDER BY m.cer DESC
    """)

    docs = cursor.fetchall()
    conn.close()

    return docs


def analyze_text_differences(gt_text: str, ocr_text: str, short_id: str):
    """Analyze what types of differences exist between ground truth and OCR."""

    # Normalize whitespace for comparison
    gt_words = gt_text.split()
    ocr_words = ocr_text.split()

    # Find word-level differences
    matcher = difflib.SequenceMatcher(None, gt_words, ocr_words)

    differences = {
        'spelling_changes': [],
        'word_substitutions': [],
        'insertions': [],
        'deletions': [],
        'case_differences': []
    }

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'replace':
            # Word(s) were changed
            gt_chunk = gt_words[i1:i2]
            ocr_chunk = ocr_words[j1:j2]

            # If same length, check if it's spelling/case changes
            if len(gt_chunk) == len(ocr_chunk) == 1:
                gt_word = gt_chunk[0]
                ocr_word = ocr_chunk[0]

                # Case difference only?
                if gt_word.lower() == ocr_word.lower():
                    differences['case_differences'].append((gt_word, ocr_word))
                # Similar length, might be spelling or OCR error
                elif abs(len(gt_word) - len(ocr_word)) <= 2:
                    differences['spelling_changes'].append((gt_word, ocr_word))
                else:
                    differences['word_substitutions'].append((gt_word, ocr_word))
            else:
                # Multiple words changed
                differences['word_substitutions'].append(
                    (' '.join(gt_chunk), ' '.join(ocr_chunk))
                )

        elif tag == 'insert':
            differences['insertions'].append(' '.join(ocr_words[j1:j2]))

        elif tag == 'delete':
            differences['deletions'].append(' '.join(gt_words[i1:i2]))

    return differences


def get_text_sample(db_path: Path, short_id: str):
    """Get ground truth and Chandra text for a document."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get ground truth
    cursor.execute("""
        SELECT text_content
        FROM ocr_results
        WHERE short_id = ? AND system_id = 'gold_standard'
    """, (short_id,))

    gt_result = cursor.fetchone()
    gt_text = gt_result[0] if gt_result else ""

    # Get Chandra output
    cursor.execute("""
        SELECT text_content
        FROM ocr_results
        WHERE short_id = ? AND system_id = 'chandra'
    """, (short_id,))

    chandra_result = cursor.fetchone()
    chandra_text = chandra_result[0] if chandra_result else ""

    conn.close()

    return gt_text, chandra_text


def show_text_comparison(gt_text: str, ocr_text: str, max_length=2000):
    """Show side-by-side comparison of first N characters."""

    gt_sample = gt_text[:max_length]
    ocr_sample = ocr_text[:max_length]

    print("\nGROUND TRUTH (first 2000 chars):")
    print("-" * 100)
    print(gt_sample)
    print()

    print("\nCHANDRA OUTPUT (first 2000 chars):")
    print("-" * 100)
    print(ocr_sample)
    print()


def categorize_errors(differences):
    """Categorize the types of errors."""

    categories = {
        'trivial': 0,      # Case differences only
        'spelling': 0,     # Likely spelling modernization or OCR character errors
        'semantic': 0,     # Word substitutions (meaning changes)
        'structural': 0    # Insertions/deletions
    }

    categories['trivial'] = len(differences['case_differences'])
    categories['spelling'] = len(differences['spelling_changes'])
    categories['semantic'] = len(differences['word_substitutions'])
    categories['structural'] = len(differences['insertions']) + len(differences['deletions'])

    return categories


def main():
    db_path = Path('/home/jic823/archive-olm-pipeline/evaluation/ocr_evaluation_corrected.db')

    print("=" * 100)
    print("CHANDRA SUBSTITUTION ERROR ANALYSIS")
    print("=" * 100)
    print()
    print("Looking for: High CER (≥10%) but normal length (0.67-1.5× Tesseract)")
    print("Goal: Understand if errors are spelling, meaning, or OCR character substitutions")
    print()

    docs = get_chandra_substitution_errors(db_path)

    print(f"Found {len(docs)} Chandra documents with high errors but normal length")
    print()

    if not docs:
        print("No documents found matching criteria.")
        return

    print("=" * 100)
    print("DOCUMENT SUMMARY")
    print("=" * 100)
    print()
    print(f"{'Short ID':<15} {'CER':<8} {'WER':<8} {'Length Ratio':<15} {'Status'}")
    print("-" * 100)

    for short_id, cer, wer, chandra_chars, chandra_words, tess_chars, gt_chars in docs:
        length_ratio = chandra_chars / tess_chars if tess_chars > 0 else 0
        status = "Catastrophic" if cer >= 0.20 else "High Error"
        print(f"{short_id:<15} {cer*100:6.2f}% {wer*100:6.2f}% {length_ratio:>6.2f}x          {status}")

    print()

    # Analyze top 5 documents in detail
    print()
    print("=" * 100)
    print("DETAILED ANALYSIS OF TOP 5 DOCUMENTS")
    print("=" * 100)

    for idx, (short_id, cer, wer, chandra_chars, chandra_words, tess_chars, gt_chars) in enumerate(docs[:5], 1):
        print()
        print("=" * 100)
        print(f"DOCUMENT {idx}: {short_id}")
        print("=" * 100)
        print(f"CER: {cer*100:.2f}%, WER: {wer*100:.2f}%")
        print(f"Length: Chandra={chandra_chars:,} chars, Ground Truth={gt_chars:,} chars ({chandra_chars/gt_chars:.2f}x)")
        print()

        # Get texts
        gt_text, chandra_text = get_text_sample(db_path, short_id)

        # Analyze differences
        differences = analyze_text_differences(gt_text, chandra_text, short_id)
        categories = categorize_errors(differences)

        print("ERROR BREAKDOWN:")
        print(f"  Trivial (case only):     {categories['trivial']}")
        print(f"  Spelling variations:     {categories['spelling']}")
        print(f"  Word substitutions:      {categories['semantic']}")
        print(f"  Insertions/deletions:    {categories['structural']}")
        print()

        # Show example differences
        if differences['spelling_changes']:
            print("EXAMPLE SPELLING CHANGES (first 10):")
            for gt_word, ocr_word in differences['spelling_changes'][:10]:
                print(f"  '{gt_word}' → '{ocr_word}'")
            print()

        if differences['word_substitutions']:
            print("EXAMPLE WORD SUBSTITUTIONS (first 10):")
            for gt_word, ocr_word in differences['word_substitutions'][:10]:
                print(f"  '{gt_word}' → '{ocr_word}'")
            print()

        if differences['case_differences']:
            print("EXAMPLE CASE DIFFERENCES (first 5):")
            for gt_word, ocr_word in differences['case_differences'][:5]:
                print(f"  '{gt_word}' → '{ocr_word}'")
            print()

        # Show text comparison for first document only
        if idx == 1:
            show_text_comparison(gt_text, chandra_text)

    print()
    print("=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print()
    print(f"Total Chandra documents with CER≥10% and normal length: {len(docs)}")
    print()

    catastrophic_count = sum(1 for doc in docs if doc[1] >= 0.20)
    print(f"Catastrophic (CER≥20%): {catastrophic_count}")
    print(f"High but not catastrophic (10%≤CER<20%): {len(docs) - catastrophic_count}")
    print()
    print("These errors were NOT detected by length-based detection because:")
    print("  - Output length is similar to Tesseract (0.67-1.5x ratio)")
    print("  - Suggests character/word substitutions rather than hallucination")
    print()
    print("=" * 100)


if __name__ == '__main__':
    main()
