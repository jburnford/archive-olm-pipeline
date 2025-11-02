#!/usr/bin/env python3
"""
Test catastrophic failure detection using Tesseract as length baseline.

Strategy: Flag documents where OCR output length deviates significantly
from Tesseract output length (not ground truth).
"""

import sqlite3
from pathlib import Path


def detect_with_tesseract_baseline(db_path: Path, length_threshold=2.0, cer_threshold=0.20):
    """
    Detect catastrophic failures using Tesseract length as baseline.

    Args:
        db_path: Path to database
        length_threshold: Flag if OCR length > threshold × Tesseract length
        cer_threshold: CER threshold for ground-truth catastrophic failure

    Returns:
        Detection statistics for each system
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=" * 100)
    print("TESSERACT BASELINE DETECTION TEST")
    print("=" * 100)
    print()
    print(f"Detection Rule: Flag if (OCR_length / Tesseract_length) > {length_threshold}x OR < {1/length_threshold}x")
    print(f"Ground Truth: CER >= {cer_threshold*100}% is catastrophic failure")
    print()

    # Get all systems except gold_standard and tesseract
    cursor.execute("""
        SELECT system_id, name
        FROM ocr_systems
        WHERE system_id NOT IN ('gold_standard', 'tesseract_v4_newspapers')
        ORDER BY name
    """)

    systems = cursor.fetchall()

    results = {}

    for system_id, system_name in systems:
        # Get all documents for this system with Tesseract baseline
        cursor.execute("""
            SELECT
                m.short_id,
                m.cer,
                o_sys.character_count as sys_chars,
                o_tess.character_count as tess_chars
            FROM evaluation_metrics_strict m
            JOIN ocr_results o_sys ON m.short_id = o_sys.short_id
                AND m.system_id = o_sys.system_id
            JOIN ocr_results o_tess ON m.short_id = o_tess.short_id
                AND o_tess.system_id = 'tesseract_v4_newspapers'
            WHERE m.system_id = ?
        """, (system_id,))

        docs = cursor.fetchall()

        true_positives = 0  # Correctly flagged catastrophic failures
        false_positives = 0  # Flagged as failure but CER < threshold
        true_negatives = 0   # Correctly not flagged
        false_negatives = 0  # Missed catastrophic failures

        flagged_docs = []
        missed_docs = []

        for short_id, cer, sys_chars, tess_chars in docs:
            # Calculate length ratio
            length_ratio = sys_chars / tess_chars if tess_chars > 0 else 0

            # Detection: Flag if length deviates significantly
            is_flagged = (length_ratio > length_threshold or length_ratio < 1/length_threshold)

            # Ground truth: Is this actually a catastrophic failure?
            is_catastrophic = (cer >= cer_threshold)

            if is_flagged and is_catastrophic:
                true_positives += 1
                flagged_docs.append((short_id, cer, length_ratio, "TP"))
            elif is_flagged and not is_catastrophic:
                false_positives += 1
                flagged_docs.append((short_id, cer, length_ratio, "FP"))
            elif not is_flagged and is_catastrophic:
                false_negatives += 1
                missed_docs.append((short_id, cer, length_ratio))
            else:
                true_negatives += 1

        total_catastrophic = true_positives + false_negatives
        total_flagged = true_positives + false_positives

        # Calculate metrics
        precision = true_positives / total_flagged if total_flagged > 0 else 0
        recall = true_positives / total_catastrophic if total_catastrophic > 0 else 0
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

        results[system_id] = {
            'name': system_name,
            'total_docs': len(docs),
            'true_positives': true_positives,
            'false_positives': false_positives,
            'true_negatives': true_negatives,
            'false_negatives': false_negatives,
            'total_catastrophic': total_catastrophic,
            'total_flagged': total_flagged,
            'precision': precision,
            'recall': recall,
            'f1_score': f1_score,
            'flagged_docs': flagged_docs[:5],  # Top 5
            'missed_docs': missed_docs[:5]      # Top 5
        }

    conn.close()
    return results


def print_results(results):
    """Print detection results in a clear table."""

    print("=" * 100)
    print("DETECTION PERFORMANCE BY SYSTEM")
    print("=" * 100)
    print()
    print(f"{'System':<30} {'Total':<7} {'Catast':<7} {'Flagged':<8} {'TP':<5} {'FP':<5} {'FN':<5} {'Precision':<10} {'Recall':<8} {'F1':<8}")
    print("-" * 100)

    for system_id, stats in sorted(results.items(), key=lambda x: x[1]['f1_score'], reverse=True):
        print(f"{stats['name']:<30} "
              f"{stats['total_docs']:<7} "
              f"{stats['total_catastrophic']:<7} "
              f"{stats['total_flagged']:<8} "
              f"{stats['true_positives']:<5} "
              f"{stats['false_positives']:<5} "
              f"{stats['false_negatives']:<5} "
              f"{stats['precision']*100:5.1f}%     "
              f"{stats['recall']*100:5.1f}%    "
              f"{stats['f1_score']:.3f}")

    print()
    print("Legend:")
    print("  Total    = Total documents evaluated")
    print("  Catast   = Ground truth catastrophic failures (CER≥20%)")
    print("  Flagged  = Documents flagged by length-based detector")
    print("  TP       = True Positives (correctly flagged)")
    print("  FP       = False Positives (wrongly flagged)")
    print("  FN       = False Negatives (missed failures)")
    print()

    # Overall statistics
    total_catastrophic = sum(s['total_catastrophic'] for s in results.values())
    total_detected = sum(s['true_positives'] for s in results.values())
    total_flagged = sum(s['total_flagged'] for s in results.values())
    total_false_positives = sum(s['false_positives'] for s in results.values())

    overall_precision = total_detected / total_flagged if total_flagged > 0 else 0
    overall_recall = total_detected / total_catastrophic if total_catastrophic > 0 else 0
    overall_f1 = 2 * (overall_precision * overall_recall) / (overall_precision + overall_recall) if (overall_precision + overall_recall) > 0 else 0

    print("=" * 100)
    print("OVERALL PERFORMANCE (ALL SYSTEMS)")
    print("=" * 100)
    print()
    print(f"Total catastrophic failures (CER≥20%): {total_catastrophic}")
    print(f"Total flagged by detector: {total_flagged}")
    print(f"True positives (correctly detected): {total_detected}")
    print(f"False positives (false alarms): {total_false_positives}")
    print(f"False negatives (missed): {total_catastrophic - total_detected}")
    print()
    print(f"Precision: {overall_precision*100:.1f}% (of flagged docs, how many are real failures)")
    print(f"Recall: {overall_recall*100:.1f}% (of real failures, how many did we catch)")
    print(f"F1 Score: {overall_f1:.3f} (harmonic mean of precision and recall)")
    print()


def show_examples(results, system_id):
    """Show example flagged and missed documents for a system."""

    stats = results.get(system_id)
    if not stats:
        return

    print("=" * 100)
    print(f"EXAMPLES: {stats['name']}")
    print("=" * 100)
    print()

    if stats['flagged_docs']:
        print("Flagged Documents:")
        print("-" * 100)
        for short_id, cer, ratio, status in stats['flagged_docs']:
            status_text = "✓ TRUE POSITIVE" if status == "TP" else "✗ FALSE POSITIVE"
            print(f"  {short_id}: CER={cer*100:6.2f}%, Length ratio={ratio:.2f}x  [{status_text}]")
        print()

    if stats['missed_docs']:
        print("Missed Catastrophic Failures:")
        print("-" * 100)
        for short_id, cer, ratio in stats['missed_docs']:
            print(f"  {short_id}: CER={cer*100:6.2f}%, Length ratio={ratio:.2f}x  [✗ FALSE NEGATIVE]")
        print()


def test_different_thresholds(db_path: Path):
    """Test different length thresholds to find optimal value."""

    print()
    print("=" * 100)
    print("TESTING DIFFERENT LENGTH THRESHOLDS")
    print("=" * 100)
    print()

    thresholds = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]

    print(f"{'Threshold':<12} {'Precision':<12} {'Recall':<12} {'F1 Score':<12} {'Total Flagged':<15}")
    print("-" * 100)

    for threshold in thresholds:
        results = detect_with_tesseract_baseline(db_path, length_threshold=threshold, cer_threshold=0.20)

        total_catastrophic = sum(s['total_catastrophic'] for s in results.values())
        total_detected = sum(s['true_positives'] for s in results.values())
        total_flagged = sum(s['total_flagged'] for s in results.values())

        precision = total_detected / total_flagged if total_flagged > 0 else 0
        recall = total_detected / total_catastrophic if total_catastrophic > 0 else 0
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

        print(f"{threshold:.1f}x         "
              f"{precision*100:6.1f}%       "
              f"{recall*100:6.1f}%       "
              f"{f1_score:.3f}         "
              f"{total_flagged}")

    print()


def main():
    db_path = Path('/home/jic823/archive-olm-pipeline/evaluation/ocr_evaluation_corrected.db')

    # Test with default threshold (2.0x)
    results = detect_with_tesseract_baseline(db_path, length_threshold=2.0, cer_threshold=0.20)
    print_results(results)

    # Show examples for systems with failures
    print()
    show_examples(results, 'chandra')
    show_examples(results, 'deepseek_ocr')

    # Test different thresholds
    test_different_thresholds(db_path)

    print()
    print("=" * 100)
    print("CONCLUSION")
    print("=" * 100)
    print()
    print("Using Tesseract as a length baseline is effective for detecting:")
    print("  ✓ Expansion failures (LLM looping, hallucination)")
    print("  ✓ Extreme collapse failures (empty/truncated output)")
    print()
    print("Limitations:")
    print("  ✗ Misses substitution errors (wrong content, similar length)")
    print("  ✗ Requires running Tesseract on all documents")
    print("  ✗ May flag documents where Tesseract itself has unusual length")
    print()
    print("=" * 100)


if __name__ == '__main__':
    main()
