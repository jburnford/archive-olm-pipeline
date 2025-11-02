#!/usr/bin/env python3
"""
Production-ready catastrophic failure detection for OCR systems.

Works WITHOUT ground truth - uses intrinsic text properties to detect failures.
"""

import zlib
from collections import Counter


def detect_catastrophic_failure(ocr_text: str) -> tuple[bool, float, str]:
    """
    Detects catastrophic OCR failures without ground truth.

    Args:
        ocr_text: The OCR output text to analyze

    Returns:
        (is_failure, confidence, failure_type)

    Example:
        is_fail, conf, ftype = detect_catastrophic_failure(ocr_output)
        if is_fail:
            print(f"FAILURE: {ftype} (confidence: {conf:.0%})")
    """

    if not ocr_text or len(ocr_text) < 50:
        return (True, 0.99, "EMPTY_OR_TOO_SHORT")

    # ============================================================================
    # Signal 1: LINE REPETITION (catches looping LLMs)
    # ============================================================================
    lines = [l.strip() for l in ocr_text.split('\n') if len(l.strip()) > 20]

    if lines:
        line_counts = Counter(lines)
        max_line_rep = max(line_counts.values())

        if max_line_rep > 10:
            return (True, 0.99, "SEVERE_LOOPING")
        elif max_line_rep > 5:
            return (True, 0.95, "LOOPING_REPETITION")

    # ============================================================================
    # Signal 2: PHRASE REPETITION (10-word n-grams)
    # ============================================================================
    words = ocr_text.split()

    if len(words) > 20:
        ngrams = [' '.join(words[i:i+10]) for i in range(len(words)-9)]
        ngram_counts = Counter(ngrams)
        max_ngram_rep = max(ngram_counts.values()) if ngram_counts else 0

        if max_ngram_rep > 5:
            return (True, 0.92, "PHRASE_HALLUCINATION")

    # ============================================================================
    # Signal 3: COMPRESSION RATIO (low ratio = high repetition)
    # ============================================================================
    try:
        compressed = zlib.compress(ocr_text.encode('utf-8', errors='ignore'))
        comp_ratio = len(compressed) / len(ocr_text)

        if comp_ratio < 0.25:
            return (True, 0.90, "EXTREME_REPETITION")
        elif comp_ratio < 0.35:
            return (True, 0.75, "HIGH_REPETITION")
    except Exception:
        pass  # Compression failed, skip this signal

    # ============================================================================
    # Signal 4: LEXICAL DIVERSITY (unique words / total words)
    # ============================================================================
    if words:
        unique_ratio = len(set(words)) / len(words)

        # Typical natural text: 0.4-0.7 unique ratio
        # Repetitive garbage: <0.3 unique ratio
        if unique_ratio < 0.25:
            return (True, 0.85, "VERY_LOW_LEXICAL_DIVERSITY")
        elif unique_ratio < 0.30:
            return (True, 0.70, "LOW_LEXICAL_DIVERSITY")

    # ============================================================================
    # Signal 5: EXTREME LINE LENGTH (parsing/encoding failure)
    # ============================================================================
    if lines:
        max_line_len = max((len(line) for line in lines), default=0)

        if max_line_len > 10000:
            return (True, 0.88, "PARSING_FAILURE_EXTREME")
        elif max_line_len > 5000:
            return (True, 0.65, "PARSING_FAILURE_MODERATE")

    # ============================================================================
    # Signal 6: ABNORMAL CHARACTER DISTRIBUTION
    # ============================================================================
    # Check for single character domination (e.g., 50%+ same char)
    char_counts = Counter(ocr_text.lower())
    total_chars = sum(char_counts.values())

    if total_chars > 0:
        # Exclude spaces
        char_counts.pop(' ', None)
        char_counts.pop('\n', None)
        char_counts.pop('\t', None)

        if char_counts:
            max_char_count = max(char_counts.values())
            max_char_ratio = max_char_count / total_chars

            if max_char_ratio > 0.30:  # One character is >30% of text
                return (True, 0.82, "CHARACTER_DISTRIBUTION_ANOMALY")

    # No failures detected
    return (False, 0.0, None)


def batch_detect_failures(ocr_outputs: list[str]) -> dict:
    """
    Analyze multiple OCR outputs and return statistics.

    Args:
        ocr_outputs: List of OCR text outputs

    Returns:
        dict with statistics and flagged documents
    """
    results = {
        'total': len(ocr_outputs),
        'failures': 0,
        'failure_types': Counter(),
        'flagged_indices': []
    }

    for idx, text in enumerate(ocr_outputs):
        is_fail, conf, ftype = detect_catastrophic_failure(text)

        if is_fail:
            results['failures'] += 1
            results['failure_types'][ftype] += 1
            results['flagged_indices'].append((idx, conf, ftype))

    results['failure_rate'] = results['failures'] / results['total'] if results['total'] > 0 else 0

    return results


if __name__ == '__main__':
    # Test on known failure
    test_looping = "The same sentence. " * 100
    test_normal = "This is a normal document with varied content and reasonable length."
    test_diverse = """
    The quick brown fox jumps over the lazy dog.
    Historical documents contain various information about past events.
    Optical character recognition technology has improved significantly.
    Machine learning models can now handle complex layouts.
    """

    print("=" * 100)
    print("TESTING CATASTROPHIC FAILURE DETECTOR")
    print("=" * 100)
    print()

    tests = [
        ("Looping text", test_looping),
        ("Normal text", test_normal),
        ("Diverse text", test_diverse)
    ]

    for name, text in tests:
        is_fail, conf, ftype = detect_catastrophic_failure(text)

        status = "FAIL" if is_fail else "PASS"
        print(f"{name:<20} {status:<6} ", end="")

        if is_fail:
            print(f"{ftype:<30} (confidence: {conf:.0%})")
        else:
            print()

    print()
    print("=" * 100)
