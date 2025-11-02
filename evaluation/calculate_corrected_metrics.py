#!/usr/bin/env python3
"""
Phase 5: Calculate corrected OCR evaluation metrics.

Calculates BOTH strict and semantic metrics correctly with independent CER/WER.
Includes gold_standard validation (should produce 0% errors).
"""

import sqlite3
from pathlib import Path
from typing import Tuple
import re


def normalize_text(text: str, semantic_only: bool = False) -> str:
    """
    Normalize text for comparison.

    Args:
        text: Input text to normalize
        semantic_only: If True, apply aggressive normalization (lowercase, no punctuation)
                      If False, only normalize whitespace
    """
    if semantic_only:
        # Lowercase
        text = text.lower()
        # Remove punctuation
        text = re.sub(r'[^\w\s]', '', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    else:
        # Basic normalization - whitespace only
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


def calculate_levenshtein_with_ops(reference: list, hypothesis: list) -> Tuple[int, int, int, int]:
    """
    Calculate Levenshtein distance with operation counts.

    Returns:
        (edit_distance, insertions, deletions, substitutions)
    """
    m, n = len(reference), len(hypothesis)

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
            if reference[i-1] == hypothesis[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = 1 + min(
                    dp[i-1][j],      # deletion
                    dp[i][j-1],      # insertion
                    dp[i-1][j-1]     # substitution
                )

    edit_distance = dp[m][n]

    # Backtrack to count operations
    insertions = 0
    deletions = 0
    substitutions = 0

    i, j = m, n
    while i > 0 or j > 0:
        if i == 0:
            insertions += j
            break
        elif j == 0:
            deletions += i
            break
        elif reference[i-1] == hypothesis[j-1]:
            i -= 1
            j -= 1
        else:
            min_cost = min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1])
            if dp[i-1][j-1] == min_cost:
                substitutions += 1
                i -= 1
                j -= 1
            elif dp[i-1][j] == min_cost:
                deletions += 1
                i -= 1
            else:
                insertions += 1
                j -= 1

    return edit_distance, insertions, deletions, substitutions


def calculate_cer(reference: str, hypothesis: str) -> dict:
    """Calculate Character Error Rate using Levenshtein distance."""
    ref_chars = list(reference)
    hyp_chars = list(hypothesis)

    m = len(ref_chars)

    edit_distance, insertions, deletions, substitutions = \
        calculate_levenshtein_with_ops(ref_chars, hyp_chars)

    cer = edit_distance / m if m > 0 else 0.0

    return {
        'cer': cer,
        'char_insertions': insertions,
        'char_deletions': deletions,
        'char_substitutions': substitutions,
        'levenshtein_distance_chars': edit_distance
    }


def calculate_wer(reference: str, hypothesis: str) -> dict:
    """Calculate Word Error Rate using Levenshtein distance on words."""
    ref_words = reference.split()
    hyp_words = hypothesis.split()

    m = len(ref_words)

    edit_distance, insertions, deletions, substitutions = \
        calculate_levenshtein_with_ops(ref_words, hyp_words)

    wer = edit_distance / m if m > 0 else 0.0

    return {
        'wer': wer,
        'word_insertions': insertions,
        'word_deletions': deletions,
        'word_substitutions': substitutions,
        'levenshtein_distance_words': edit_distance
    }


def calculate_metrics_for_pair(ground_truth: str, ocr_text: str, short_id: str, system_id: str):
    """Calculate both strict and semantic metrics for a document pair."""

    # STRICT metrics (whitespace normalization only)
    gt_strict = normalize_text(ground_truth, semantic_only=False)
    ocr_strict = normalize_text(ocr_text, semantic_only=False)

    strict_cer = calculate_cer(gt_strict, ocr_strict)
    strict_wer = calculate_wer(gt_strict, ocr_strict)

    # SEMANTIC metrics (lowercase, no punctuation)
    gt_semantic = normalize_text(ground_truth, semantic_only=True)
    ocr_semantic = normalize_text(ocr_text, semantic_only=True)

    semantic_cer = calculate_cer(gt_semantic, ocr_semantic)
    semantic_wer = calculate_wer(gt_semantic, ocr_semantic)

    return {
        'short_id': short_id,
        'system_id': system_id,
        'strict': {**strict_cer, **strict_wer},
        'semantic': {**semantic_cer, **semantic_wer}
    }


def process_system(db_path: Path, system_id: str):
    """Process all documents for a single OCR system."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all document pairs for this system
    cursor.execute("""
        SELECT
            d.short_id,
            d.ground_truth_path,
            o.text_content
        FROM documents d
        JOIN ocr_results o ON d.short_id = o.short_id
        WHERE o.system_id = ?
    """, (system_id,))

    pairs = cursor.fetchall()

    if not pairs:
        print(f"  ⚠️  No documents found for {system_id}")
        conn.close()
        return 0

    print(f"  Processing {system_id}: {len(pairs)} documents")

    processed = 0
    errors = 0

    for short_id, gt_path, ocr_text in pairs:
        try:
            # Read ground truth
            with open(gt_path, 'r', encoding='utf-8') as f:
                ground_truth = f.read()

            # Calculate metrics
            metrics = calculate_metrics_for_pair(ground_truth, ocr_text, short_id, system_id)

            # Insert STRICT metrics
            cursor.execute("""
                INSERT OR REPLACE INTO evaluation_metrics_strict
                (short_id, system_id, cer, char_insertions, char_deletions,
                 char_substitutions, levenshtein_distance_chars, wer,
                 word_insertions, word_deletions, word_substitutions,
                 levenshtein_distance_words)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                short_id, system_id,
                metrics['strict']['cer'],
                metrics['strict']['char_insertions'],
                metrics['strict']['char_deletions'],
                metrics['strict']['char_substitutions'],
                metrics['strict']['levenshtein_distance_chars'],
                metrics['strict']['wer'],
                metrics['strict']['word_insertions'],
                metrics['strict']['word_deletions'],
                metrics['strict']['word_substitutions'],
                metrics['strict']['levenshtein_distance_words']
            ))

            # Insert SEMANTIC metrics
            cursor.execute("""
                INSERT OR REPLACE INTO evaluation_metrics_semantic
                (short_id, system_id, cer_semantic, char_insertions_semantic,
                 char_deletions_semantic, char_substitutions_semantic,
                 levenshtein_distance_chars_semantic, wer_semantic,
                 word_insertions_semantic, word_deletions_semantic,
                 word_substitutions_semantic, levenshtein_distance_words_semantic)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                short_id, system_id,
                metrics['semantic']['cer'],
                metrics['semantic']['char_insertions'],
                metrics['semantic']['char_deletions'],
                metrics['semantic']['char_substitutions'],
                metrics['semantic']['levenshtein_distance_chars'],
                metrics['semantic']['wer'],
                metrics['semantic']['word_insertions'],
                metrics['semantic']['word_deletions'],
                metrics['semantic']['word_substitutions'],
                metrics['semantic']['levenshtein_distance_words']
            ))

            processed += 1

            if processed % 100 == 0:
                conn.commit()
                print(f"    Progress: {processed}/{len(pairs)}...")

        except Exception as e:
            print(f"    ✗ Error processing {short_id}: {e}")
            errors += 1

    conn.commit()
    conn.close()

    print(f"    ✓ Completed: {processed} documents (errors: {errors})")

    return processed


def validate_gold_standard(db_path: Path):
    """Validate that gold_standard system has 0% errors."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print()
    print("=" * 100)
    print("VALIDATING GOLD STANDARD (SANITY CHECK)")
    print("=" * 100)
    print()

    cursor.execute("""
        SELECT
            AVG(cer) as avg_cer,
            AVG(wer) as avg_wer,
            MAX(cer) as max_cer,
            MAX(wer) as max_wer,
            COUNT(*) as count
        FROM evaluation_metrics_strict
        WHERE system_id = 'gold_standard'
    """)

    strict_stats = cursor.fetchone()

    cursor.execute("""
        SELECT
            AVG(cer_semantic) as avg_cer,
            AVG(wer_semantic) as avg_wer,
            MAX(cer_semantic) as max_cer,
            MAX(wer_semantic) as max_wer
        FROM evaluation_metrics_semantic
        WHERE system_id = 'gold_standard'
    """)

    semantic_stats = cursor.fetchone()

    print("Gold Standard Results:")
    print("-" * 100)
    print(f"  Documents evaluated: {strict_stats[4]}")
    print()
    print("STRICT:")
    print(f"  Average CER: {strict_stats[0]*100:.6f}%")
    print(f"  Average WER: {strict_stats[1]*100:.6f}%")
    print(f"  Max CER: {strict_stats[2]*100:.6f}%")
    print(f"  Max WER: {strict_stats[3]*100:.6f}%")
    print()
    print("SEMANTIC:")
    print(f"  Average CER: {semantic_stats[0]*100:.6f}%")
    print(f"  Average WER: {semantic_stats[1]*100:.6f}%")
    print(f"  Max CER: {semantic_stats[2]*100:.6f}%")
    print(f"  Max WER: {semantic_stats[3]*100:.6f}%")
    print()

    if strict_stats[2] == 0.0 and strict_stats[3] == 0.0:
        print("✅ VALIDATION PASSED: All metrics are 0% as expected!")
        print("   Our metric calculation code is working correctly.")
    else:
        print("❌ VALIDATION FAILED: Gold standard has non-zero errors!")
        print("   This indicates a bug in our metric calculation code.")

    print("=" * 100)

    conn.close()


def main():
    """Main calculation function."""

    db_path = Path('/home/jic823/archive-olm-pipeline/evaluation/ocr_evaluation_corrected.db')

    print("=" * 100)
    print("CALCULATING CORRECTED METRICS FOR ALL SYSTEMS")
    print("=" * 100)
    print()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all systems
    cursor.execute("SELECT system_id, name FROM ocr_systems ORDER BY name")
    systems = cursor.fetchall()

    conn.close()

    print(f"Processing {len(systems)} OCR systems...")
    print()

    for system_id, name in systems:
        process_system(db_path, system_id)

    # Validate gold standard
    validate_gold_standard(db_path)

    print()
    print("=" * 100)
    print("✓ PHASE 5 COMPLETE")
    print("=" * 100)
    print()
    print("Next step: Run generate_final_comparison.py")
    print("=" * 100)


if __name__ == '__main__':
    main()
