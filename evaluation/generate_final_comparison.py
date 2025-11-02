#!/usr/bin/env python3
"""
Phase 6: Generate final comprehensive OCR comparison report.

Includes:
- Statistical comparison (mean, median, std dev, CV)
- System rankings
- Failure analysis with Tesseract baseline
- Catastrophic failure detection
"""

import sqlite3
from pathlib import Path
from collections import Counter
import json
from detect_catastrophic_failures import detect_catastrophic_failure


def get_system_statistics(db_path: Path):
    """Get comprehensive statistics for all OCR systems."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get strict metrics
    cursor.execute("""
        SELECT
            s.name,
            COUNT(*) as doc_count,
            AVG(m.cer) as mean_cer,
            AVG(m.wer) as mean_wer,
            MIN(m.cer) as min_cer,
            MIN(m.wer) as min_wer,
            MAX(m.cer) as max_cer,
            MAX(m.wer) as max_wer
        FROM evaluation_metrics_strict m
        JOIN ocr_systems s ON m.system_id = s.system_id
        WHERE s.system_id != 'gold_standard'
        GROUP BY s.system_id, s.name
        ORDER BY mean_cer
    """)

    strict_stats = cursor.fetchall()

    # Calculate median and std dev for each system
    results = []

    for name, doc_count, mean_cer, mean_wer, min_cer, min_wer, max_cer, max_wer in strict_stats:
        # Get system_id
        cursor.execute("SELECT system_id FROM ocr_systems WHERE name = ?", (name,))
        system_id = cursor.fetchone()[0]

        # Get all CER values for median and std dev
        cursor.execute("""
            SELECT cer, wer
            FROM evaluation_metrics_strict
            WHERE system_id = ?
            ORDER BY cer
        """, (system_id,))

        values = cursor.fetchall()
        cer_values = [v[0] for v in values]
        wer_values = [v[1] for v in values]

        # Calculate median
        n = len(cer_values)
        median_cer = cer_values[n // 2] if n % 2 == 1 else (cer_values[n // 2 - 1] + cer_values[n // 2]) / 2
        median_wer = sorted(wer_values)[n // 2] if n % 2 == 1 else (sorted(wer_values)[n // 2 - 1] + sorted(wer_values)[n // 2]) / 2

        # Calculate standard deviation
        variance_cer = sum((x - mean_cer) ** 2 for x in cer_values) / n
        std_cer = variance_cer ** 0.5

        variance_wer = sum((x - mean_wer) ** 2 for x in wer_values) / n
        std_wer = variance_wer ** 0.5

        # Coefficient of variation (CV)
        cv_cer = (std_cer / mean_cer * 100) if mean_cer > 0 else 0
        cv_wer = (std_wer / mean_wer * 100) if mean_wer > 0 else 0

        results.append({
            'system_id': system_id,
            'name': name,
            'doc_count': doc_count,
            'cer': {
                'mean': mean_cer,
                'median': median_cer,
                'std': std_cer,
                'cv': cv_cer,
                'min': min_cer,
                'max': max_cer
            },
            'wer': {
                'mean': mean_wer,
                'median': median_wer,
                'std': std_wer,
                'cv': cv_wer,
                'min': min_wer,
                'max': max_wer
            }
        })

    conn.close()
    return results


def analyze_failures_with_baseline(db_path: Path, threshold_cer=0.20):
    """
    Analyze catastrophic failures using Tesseract as baseline.

    User suggestion: "we would uses Tesseract as a baseline. it should give us a rough length to compare with."
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print()
    print("=" * 100)
    print("CATASTROPHIC FAILURE ANALYSIS (Tesseract Baseline)")
    print("=" * 100)
    print()
    print("Strategy: Use Tesseract output length as baseline for expected document length.")
    print(f"Threshold: CER >= {threshold_cer*100}% considered catastrophic")
    print()

    # Get all systems except gold_standard and tesseract
    cursor.execute("""
        SELECT system_id, name
        FROM ocr_systems
        WHERE system_id NOT IN ('gold_standard', 'tesseract_v4_newspapers')
        ORDER BY name
    """)

    systems = cursor.fetchall()

    all_failures = {}

    for system_id, system_name in systems:
        # Get catastrophic failures for this system
        cursor.execute("""
            SELECT
                m.short_id,
                m.cer,
                m.wer,
                o.character_count as ocr_chars,
                o.word_count as ocr_words
            FROM evaluation_metrics_strict m
            JOIN ocr_results o ON m.short_id = o.short_id AND m.system_id = o.system_id
            WHERE m.system_id = ? AND m.cer >= ?
            ORDER BY m.cer DESC
        """, (system_id, threshold_cer))

        failures = cursor.fetchall()

        if failures:
            print(f"{system_name}: {len(failures)} catastrophic failures")
            print("-" * 100)

            for short_id, cer, wer, ocr_chars, ocr_words in failures[:5]:  # Show top 5
                # Get ground truth stats
                cursor.execute("""
                    SELECT character_count, word_count
                    FROM ocr_results
                    WHERE short_id = ? AND system_id = 'gold_standard'
                """, (short_id,))

                gt_stats = cursor.fetchone()
                gt_chars, gt_words = gt_stats if gt_stats else (0, 0)

                # Get Tesseract stats (baseline)
                cursor.execute("""
                    SELECT character_count, word_count
                    FROM ocr_results
                    WHERE short_id = ? AND system_id = 'tesseract_v4_newspapers'
                """, (short_id,))

                tess_stats = cursor.fetchone()
                tess_chars, tess_words = tess_stats if tess_stats else (0, 0)

                # Calculate length ratios
                char_ratio = ocr_chars / gt_chars if gt_chars > 0 else 0
                tess_ratio = tess_chars / gt_chars if gt_chars > 0 else 0

                # Get OCR text to analyze failure type
                cursor.execute("""
                    SELECT text_content
                    FROM ocr_results
                    WHERE short_id = ? AND system_id = ?
                """, (short_id, system_id))

                result = cursor.fetchone()
                ocr_text = result[0] if result else ""

                is_failure, confidence, failure_type = detect_catastrophic_failure(ocr_text)

                detection_status = f"✓ DETECTED: {failure_type} ({confidence:.0%})" if is_failure else "✗ NOT DETECTED"

                print(f"  {short_id}: CER={cer*100:6.2f}% WER={wer*100:6.2f}%")
                print(f"    Length: GT={gt_chars:,} chars, Tesseract={tess_chars:,} chars ({tess_ratio:.2f}x)")
                print(f"    This system: {ocr_chars:,} chars ({char_ratio:.2f}x vs GT)")
                print(f"    Detection: {detection_status}")
                print()

            if len(failures) > 5:
                print(f"  ... and {len(failures) - 5} more failures")
                print()

            all_failures[system_id] = failures

    conn.close()

    return all_failures


def generate_ranking(db_path: Path):
    """Generate system rankings by different criteria."""

    stats = get_system_statistics(db_path)

    print()
    print("=" * 100)
    print("SYSTEM RANKINGS")
    print("=" * 100)
    print()

    # Rank by mean CER
    print("By Mean CER (Character Error Rate):")
    print("-" * 100)
    ranked_by_mean = sorted(stats, key=lambda x: x['cer']['mean'])

    for i, sys in enumerate(ranked_by_mean, 1):
        print(f"{i:2}. {sys['name']:<30} {sys['cer']['mean']*100:6.2f}% (±{sys['cer']['std']*100:.2f}%)")

    print()

    # Rank by median CER (more robust to outliers)
    print("By Median CER (robust to outliers):")
    print("-" * 100)
    ranked_by_median = sorted(stats, key=lambda x: x['cer']['median'])

    for i, sys in enumerate(ranked_by_median, 1):
        print(f"{i:2}. {sys['name']:<30} {sys['cer']['median']*100:6.2f}%")

    print()

    # Rank by reliability (lowest CV = most consistent)
    print("By Reliability (Coefficient of Variation - lower is better):")
    print("-" * 100)
    ranked_by_reliability = sorted(stats, key=lambda x: x['cer']['cv'])

    for i, sys in enumerate(ranked_by_reliability, 1):
        cv = sys['cer']['cv']
        reliability = "Very Consistent" if cv < 100 else "Consistent" if cv < 200 else "Inconsistent"
        print(f"{i:2}. {sys['name']:<30} CV={cv:6.1f}% ({reliability})")

    print()


def print_detailed_statistics(db_path: Path):
    """Print detailed statistical table."""

    stats = get_system_statistics(db_path)

    print()
    print("=" * 100)
    print("DETAILED STATISTICS (STRICT METRICS)")
    print("=" * 100)
    print()

    # Header
    print(f"{'System':<30} {'Docs':<6} {'Mean CER':<10} {'Median CER':<12} {'Std Dev':<10} {'CV':<10} {'Max CER':<10}")
    print("-" * 100)

    # Sort by mean CER
    for sys in sorted(stats, key=lambda x: x['cer']['mean']):
        print(f"{sys['name']:<30} {sys['doc_count']:<6} "
              f"{sys['cer']['mean']*100:6.2f}%    "
              f"{sys['cer']['median']*100:6.2f}%      "
              f"{sys['cer']['std']*100:6.2f}%    "
              f"{sys['cer']['cv']:6.1f}%    "
              f"{sys['cer']['max']*100:6.2f}%")

    print()

    # WER table
    print(f"{'System':<30} {'Docs':<6} {'Mean WER':<10} {'Median WER':<12} {'Std Dev':<10} {'CV':<10} {'Max WER':<10}")
    print("-" * 100)

    for sys in sorted(stats, key=lambda x: x['wer']['mean']):
        print(f"{sys['name']:<30} {sys['doc_count']:<6} "
              f"{sys['wer']['mean']*100:6.2f}%    "
              f"{sys['wer']['median']*100:6.2f}%      "
              f"{sys['wer']['std']*100:6.2f}%    "
              f"{sys['wer']['cv']:6.1f}%    "
              f"{sys['wer']['max']*100:6.2f}%")

    print()


def main():
    """Generate comprehensive comparison report."""

    db_path = Path('/home/jic823/archive-olm-pipeline/evaluation/ocr_evaluation_corrected.db')

    print("=" * 100)
    print("FINAL OCR SYSTEM COMPARISON REPORT")
    print("=" * 100)
    print()
    print("Dataset: British Library Newspaper Collection (600 documents)")
    print("Metrics: CER (Character Error Rate), WER (Word Error Rate)")
    print("Normalization: Strict (case/punctuation preserved)")
    print()

    # Detailed statistics
    print_detailed_statistics(db_path)

    # Rankings
    generate_ranking(db_path)

    # Failure analysis with Tesseract baseline
    failures = analyze_failures_with_baseline(db_path, threshold_cer=0.20)

    # Summary
    print()
    print("=" * 100)
    print("KEY FINDINGS")
    print("=" * 100)
    print()

    stats = get_system_statistics(db_path)

    best_mean = min(stats, key=lambda x: x['cer']['mean'])
    best_median = min(stats, key=lambda x: x['cer']['median'])
    most_reliable = min(stats, key=lambda x: x['cer']['cv'])

    print(f"✓ Best Mean Performance: {best_mean['name']} ({best_mean['cer']['mean']*100:.2f}% CER)")
    print(f"✓ Best Median Performance: {best_median['name']} ({best_median['cer']['median']*100:.2f}% CER)")
    print(f"✓ Most Reliable (Consistent): {most_reliable['name']} (CV={most_reliable['cer']['cv']:.1f}%)")
    print()

    total_catastrophic = sum(len(f) for f in failures.values())
    print(f"⚠ Total catastrophic failures (CER≥20%): {total_catastrophic}")
    print()

    print("Median vs Mean Gap Analysis:")
    print("-" * 100)
    for sys in sorted(stats, key=lambda x: (x['cer']['mean'] - x['cer']['median']), reverse=True):
        gap = (sys['cer']['mean'] - sys['cer']['median']) * 100
        if gap > 0.5:  # Only show significant gaps
            print(f"  {sys['name']:<30} Gap: {gap:5.2f}% (outliers drag mean up)")

    print()
    print("=" * 100)
    print("✓ ANALYSIS COMPLETE")
    print("=" * 100)
    print()
    print("Visualizations available:")
    print("  - ocr_error_distributions.png (histograms)")
    print("  - ocr_error_boxplots.png (box plots)")
    print()
    print("Failure detection tool:")
    print("  - detect_catastrophic_failures.py (ground-truth-free detection)")
    print()
    print("=" * 100)


if __name__ == '__main__':
    main()
