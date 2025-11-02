# OCR System Evaluation - British Library Newspaper Collection

Comprehensive evaluation of 9 OCR systems on 600 historical newspaper documents from the British Library collection.

## Dataset

- **Documents**: 600 newspaper articles (British Library collection)
- **Ground Truth**: Manual transcriptions
- **Systems Evaluated**: 9 OCR systems including LLM-based and conventional approaches
- **Metrics**: Character Error Rate (CER) and Word Error Rate (WER)
- **Normalization**: Both strict (case/punctuation preserved) and semantic (lowercase, no punctuation)

## Key Findings

### Overall Performance Rankings

**By Mean CER (Character Error Rate):**

| Rank | System | Mean CER | Median CER | Std Dev | CV | Documents |
|------|--------|----------|------------|---------|-------|-----------|
| 1 | Google Gemini 2.5 Pro | 0.99% | 0.60% | ±2.15% | 216.9% | 593 |
| 2 | Mistral Small 3.2 | 1.42% | 0.99% | ±3.23% | 227.2% | 600 |
| 3 | OLMoCR | 2.07% | 0.79% | ±5.80% | 280.5% | 600 |
| 4 | Chandra | 2.11% | 0.66% | ±19.38% | 920.3% | 600 |
| 5 | GALE OCR | 7.20% | 4.90% | ±7.06% | 98.1% | 600 |
| 6 | Tesseract v4 | 8.00% | 6.37% | ±6.23% | 77.9% | 600 |
| 7 | DeepSeek OCR | 14.97% | 1.43% | ±44.16% | 294.9% | 588 |
| 8 | PaddleOCR | 39.78% | 8.14% | ±44.52% | 111.9% | 600 |
| 9 | EffOCR | 58.16% | 58.24% | ±13.01% | 22.4% | 296 |

### Reliability Analysis

**Most Consistent (Lowest Coefficient of Variation):**
1. EffOCR (22.4%) - Very consistent but worst absolute performance
2. Tesseract v4 (77.9%) - Reliable baseline
3. GALE OCR (98.1%) - Consistent conventional OCR

**Most Inconsistent:**
1. Chandra (920.3%) - Excellent typical performance but prone to catastrophic failures
2. DeepSeek (294.9%) - High variability
3. OLMoCR (280.5%) - Moderate variability

### Chandra Performance Profile

**Strengths:**
- **Median CER: 0.66%** (2nd best overall)
- Excellent typical performance on most documents
- Low error rate when working correctly

**Weaknesses:**
- **Mean CER: 2.11%** (4th place - dragged down by outliers)
- **CV: 920.3%** (extremely inconsistent)
- **6 catastrophic failures** (CER ≥ 20%)
- Unpredictable: either excellent or catastrophic

**Failure Modes:**
1. **LLM Looping** (1 case, 466.93% CER): Repeated same dialogue 15 times
2. **Collapse Failures** (1 case, 65.17% CER): Output 0.33× expected length
3. **Character Substitution** (2 cases, 25-40% CER): P→F, A→I, l/i confusion
4. **Format Hallucination** (2 cases): Added markdown headers, LaTeX math notation

## Catastrophic Failure Detection

### Approach 1: Tesseract Baseline (Recommended)

**Strategy:** Run Tesseract on all documents and flag when other OCR output length deviates significantly from Tesseract length.

**Optimal Threshold: 1.5×**

| Metric | Value |
|--------|-------|
| Precision | **100.0%** (zero false positives) |
| Recall | **75.4%** (493/654 catastrophic failures detected) |
| F1 Score | **0.860** |
| Total Flagged | 493 documents |

**Performance by System:**

| System | Catastrophic Failures | Detected | Recall |
|--------|----------------------|----------|--------|
| PaddleOCR | 232 | 216 | 93.1% |
| DeepSeek | 72 | 62 | 86.1% |
| EffOCR | 295 | 87 | 29.5% |
| Chandra | 6 | 2 | 33.3% |
| OLMoCR | 13 | 1 | 7.7% |

**What It Catches:**
- ✅ LLM looping (5.25× length expansion)
- ✅ Hallucination (3-5× length expansion)
- ✅ Empty/truncated output (0.01-0.2× length collapse)
- ✅ Most severe failures (CER > 50%)

**What It Misses:**
- ❌ Substitution errors (wrong content, similar length)
- ❌ Character confusion (P/F, A/I, l/i)
- ❌ Moderate errors with normal length (10-20% CER)

**Advantages:**
- **100% precision** - every flagged document is genuinely a failure
- Works on any document type (500-page books, articles, etc.)
- No ground truth needed for production use
- Simple implementation

**Requirements:**
- Must run Tesseract on all documents
- Small computational overhead (~8% mean CER baseline)

### Approach 2: Intrinsic Detection (No Baseline Required)

**Script:** `detect_catastrophic_failures.py`

**Strategy:** Detect failures using only the OCR output text properties (no ground truth, no baseline).

**Detection Signals:**
1. **Line repetition**: Catches LLM looping (>10 identical lines = severe)
2. **Phrase repetition**: Catches hallucination (>5 repeated 10-word phrases)
3. **Compression ratio**: Low ratio indicates high repetition (zlib compression)
4. **Lexical diversity**: Unique words / total words (< 0.25 = suspicious)
5. **Extreme line length**: Parsing failures (>10,000 chars per line)
6. **Character distribution**: Single character >30% of text

**Performance:**
- Precision: High (catches severe looping and empty output)
- Recall: Lower (misses substitution errors)

**Use Case:** Real-time monitoring when Tesseract baseline not available.

## Chandra Substitution Error Analysis

**Question:** Are high-error, normal-length documents due to spelling modernization?

**Answer:** No. Only 2 documents (out of 600) have high CER with normal length:

**Document 1 (25.94% CER):**
- Character misreads: `Preece`→`Free`, `MERIVALE`→`MERVINE`, `FANK`→`FARR`
- Markdown hallucination: Added `##` headers
- Name substitution: `#.`→`ROOKWOOD.` (completely wrong)

**Document 2 (10.15% CER):**
- LaTeX hallucination: `¼`→`$\frac{1}{4}$` (unnecessary formatting)
- Minor spelling: `especially`→`specially`, `realizations`→`realisations`

**Conclusion:** These are OCR errors and format hallucinations, NOT spelling modernization.

## Recommendations

### For Production Use

1. **Run Tesseract as baseline** on all documents (1.5× threshold)
   - Flags 75% of catastrophic failures with zero false positives
   - Cost: ~8% baseline CER, but worth it for quality control

2. **Use Chandra for primary OCR** despite variability
   - Median performance (0.66% CER) is excellent
   - 99% of documents will have very low error rates
   - Catastrophic failures are rare (1% failure rate)

3. **Automatic reprocessing** for flagged documents
   - If length ratio > 1.5× or < 0.67× vs Tesseract: reprocess with different prompt
   - Consider using Gemini 2.5 Pro for flagged documents (most reliable)

### For Research/Evaluation

1. **Focus on median, not mean** when comparing LLM-based OCR
   - Mean is heavily influenced by rare catastrophic failures
   - Median better represents typical performance

2. **Calculate Coefficient of Variation (CV)** for reliability
   - CV < 100% = Very consistent
   - CV > 200% = Inconsistent (needs quality monitoring)

3. **Analyze failure patterns** across systems
   - Systems fail on different documents (Jaccard similarity = 0.094)
   - Not universally "hard" documents - system-specific weaknesses

## Database Schema

**File:** `ocr_evaluation_corrected.db` (SQLite)

### Tables

**`ocr_systems`**
- System metadata (name, version, description)

**`documents`**
- Ground truth documents with file paths
- Character count, word count, line count

**`ocr_results`**
- OCR output text for each system
- Statistics (character count, word count)

**`evaluation_metrics_strict`**
- Strict metrics (case/punctuation preserved)
- CER, WER, Levenshtein distance
- Operation counts (insertions, deletions, substitutions)

**`evaluation_metrics_semantic`**
- Semantic metrics (lowercase, no punctuation)
- Same structure as strict metrics

**`gold_standard`** (validation)
- Ground truth compared to itself
- Should produce 0.000% CER/WER (validation passed ✅)

## Visualizations

**`ocr_error_distributions.png`**
- Histograms of CER/WER distributions for all systems
- Shows median vs mean gap
- Highlights outliers

**`ocr_error_boxplots.png`**
- Box plots comparing systems side-by-side
- Visualizes quartiles, median, outliers
- Easy comparison across systems

## Scripts

### Evaluation Pipeline

1. **`create_corrected_database.py`** - Initialize database schema
2. **`import_ground_truth.py`** - Load ground truth documents
3. **`import_ocr_systems.py`** - Register OCR systems
4. **`extract_ocr_text.py`** - Extract OCR results from old database and files
5. **`calculate_corrected_metrics.py`** - Calculate CER/WER for all systems
6. **`generate_final_comparison.py`** - Generate comprehensive report

### Analysis Tools

- **`detect_catastrophic_failures.py`** - Ground-truth-free failure detection
- **`test_tesseract_baseline_detection.py`** - Test Tesseract baseline approach
- **`analyze_chandra_substitution_errors.py`** - Investigate substitution errors
- **`plot_wer_distribution.py`** - Create distribution visualizations

### Usage

```bash
# Run complete evaluation pipeline
cd /home/jic823/archive-olm-pipeline/evaluation

# Phase 1: Create database
python3 create_corrected_database.py

# Phase 2: Import ground truth
python3 import_ground_truth.py

# Phase 3: Import OCR systems
python3 import_ocr_systems.py

# Phase 4: Extract OCR text
python3 extract_ocr_text.py

# Phase 5: Calculate metrics
python3 calculate_corrected_metrics.py

# Phase 6: Generate report
python3 generate_final_comparison.py

# Test catastrophic failure detection
python3 detect_catastrophic_failures.py
python3 test_tesseract_baseline_detection.py
```

## Key Metrics Explained

**CER (Character Error Rate)**
```
CER = Levenshtein_Distance_Chars / Total_Characters_in_Reference
```
- Measures character-level accuracy
- Lower is better (0% = perfect)
- Sensitive to small errors

**WER (Word Error Rate)**
```
WER = Levenshtein_Distance_Words / Total_Words_in_Reference
```
- Measures word-level accuracy
- More forgiving than CER for minor character errors
- Better for downstream NLP tasks

**Coefficient of Variation (CV)**
```
CV = (Standard_Deviation / Mean) × 100
```
- Measures relative variability
- Low CV = consistent performance
- High CV = unpredictable (needs monitoring)

**Precision (Failure Detection)**
```
Precision = True_Positives / (True_Positives + False_Positives)
```
- Of flagged documents, how many are real failures?
- 100% precision = zero false alarms

**Recall (Failure Detection)**
```
Recall = True_Positives / (True_Positives + False_Negatives)
```
- Of real failures, how many did we catch?
- 75% recall = caught 3 out of 4 failures

## Validation

**Gold Standard Test (Sanity Check):**
- Ground truth compared to itself
- Expected: 0.000% CER/WER
- **Result: ✅ PASSED** (all metrics 0.000%)
- Confirms metric calculation is correct

## Future Work

1. **Semantic WER Analysis** - Compare semantic vs strict metrics
2. **Document Type Analysis** - Performance on ads vs articles vs tables
3. **Confidence Scores** - LLM-based OCR often provides confidence
4. **Prompt Engineering** - Test different prompts to reduce Chandra failures
5. **Ensemble Approach** - Combine multiple systems with voting
6. **Cost Analysis** - Compare accuracy vs computational cost

## Citation

If you use this evaluation in your research, please cite:

```
OCR System Evaluation - British Library Newspaper Collection
Jacob Burnford, University of Saskatchewan
2025
GitHub: archive-olm-pipeline/evaluation
```

## License

This evaluation framework is open source. The British Library newspaper images and ground truth transcriptions are subject to British Library terms of use.

## Contact

For questions about this evaluation:
- Repository: github.com/jburnford/archive-olm-pipeline
- Email: [contact information]

---

**Last Updated:** November 1, 2025
**Database Version:** ocr_evaluation_corrected.db (23 MB, 600 documents × 10 systems)
**Validation Status:** ✅ Gold standard test passed (0.000% error)
