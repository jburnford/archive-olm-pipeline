# Saskatchewan Corpus Text Analysis

Tools for analyzing the Saskatchewan historical corpus (1808-1946) using TF-IDF and MALLET topic modeling.

## Quick Start

```bash
# 1. Install required Python packages
pip install scikit-learn pandas numpy

# 2. Run TF-IDF analysis
python3 build_tfidf_analysis.py

# 3. Prepare MALLET corpus
python3 build_mallet_corpus.py

# 4. Install and run MALLET (see MALLET instructions below)
```

## Analysis Pipeline

### 1. Corpus Overview (`explore_saskatchewan_corpus.py`)

Provides high-level statistics about the corpus:
- **~87 million words** across 1,265 documents
- **Peak period**: 1870s-1880s (Riel Rebellion era)
- **Major components**:
  - 347 Residential School administrative files
  - 147 newspaper issues (Prince Albert Times)
  - Historical works, government reports, exploration accounts

**Run it:**
```bash
python3 explore_saskatchewan_corpus.py
```

### 2. OCR Quality Check (`spot_check_ocr.py`)

Spot checks OCR quality for key document types:
- Residential school documents: Good quality, ~300K-400K words/doc
- Newspapers: Excellent quality, multi-column layout preserved

**Run it:**
```bash
python3 spot_check_ocr.py
```

### 3. TF-IDF Analysis (`build_tfidf_analysis.py`)

**What it does:**
- Builds document-term matrix with TF-IDF weights
- Identifies distinctive terms by:
  - Decade (1800s-1880s)
  - Document type (residential school, newspaper, government, etc.)
  - Publisher/source
- Finds similar document pairs
- Exports vocabulary and document list

**Output:** `analysis_output/`
- `tfidf_vectorizer.pkl` - Trained TF-IDF model
- `tfidf_matrix.pkl` - Document-term matrix
- `document_metadata.pkl` - Document metadata
- `vocabulary.txt` - Full vocabulary list
- `document_list.txt` - Document index

**Run it:**
```bash
python3 build_tfidf_analysis.py
```

**Key findings examples:**
- **1870s distinctive terms**: rebellion, riel, métis, saskatchewan, north-west
- **1880s distinctive terms**: settlement, railway, homestead, agriculture
- **Residential school docs**: pupil, teacher, attendance, agent, file
- **Newspapers**: advertisement, editor, subscription, local news

### 4. MALLET Topic Modeling (`build_mallet_corpus.py`)

**What it does:**
- Exports corpus in MALLET-compatible format
- Creates stopword list (historical + boilerplate terms)
- Generates shell scripts for running MALLET
- Provides analysis tools for topic results

**Output:** `mallet_corpus/`
- `corpus.txt` - Documents in MALLET format
- `metadata.tsv` - Document metadata
- `stopwords.txt` - Extended stopword list
- `run_mallet_import.sh` - Import script
- `run_mallet_topics.sh` - Topic modeling script
- `analyze_topics.py` - Results analysis script

**Run it:**
```bash
python3 build_mallet_corpus.py
```

## MALLET Topic Modeling Workflow

### Install MALLET

1. Download from: http://mallet.cs.umass.edu/download.php
2. Extract: `tar -xzf mallet-2.0.8.tar.gz`
3. Set environment variable:
   ```bash
   export MALLET_HOME=/path/to/mallet-2.0.8
   export PATH=$PATH:$MALLET_HOME/bin
   ```

### Run Topic Modeling

```bash
cd mallet_corpus/

# 1. Import corpus
./run_mallet_import.sh

# 2. Run topic modeling (20 topics, 1000 iterations)
./run_mallet_topics.sh 20 1000

# 3. Analyze results
python analyze_topics.py topics_20/
```

### Recommended Experiments

Try different topic counts to find optimal granularity:

```bash
# Broad themes (10 topics)
./run_mallet_topics.sh 10 1000

# Medium granularity (20 topics) - RECOMMENDED START
./run_mallet_topics.sh 20 1000

# Fine-grained (30 topics)
./run_mallet_topics.sh 30 1500

# Very detailed (50 topics) - for deeper analysis
./run_mallet_topics.sh 50 2000
```

### Interpreting Topic Results

**Files created:**
- `topic-keys.txt` - Top 20 words per topic (human-readable labels)
- `doc-topics.txt` - Topic proportions for each document
- `word-topic-counts.txt` - Word-topic co-occurrence counts

**Analysis strategy:**
1. **Read topic-keys.txt** - Identify what each topic represents
2. **Label topics** based on top words and historical context
   - Example: Topic with "rebellion, riel, métis, troops" = 1885 Rebellion
3. **Check temporal patterns** - Which topics dominate which decades?
4. **Check document types** - Do newspapers vs admin docs have different topics?
5. **Find representative documents** - High-probability docs for each topic

## Expected Topics (Hypotheses)

Based on metadata analysis, you might find topics like:

**Historical Events:**
- 1885 Riel Rebellion (rebellion, métis, troops, batoche)
- Western exploration (expedition, prairie, resources, territory)

**Institutional:**
- Residential schools (pupil, attendance, teacher, agent, school)
- Government administration (department, official, correspondence)
- Land settlement (homestead, survey, patent, township)

**Economic:**
- Agriculture (farming, wheat, cattle, harvest)
- Railway development (railway, line, station, transcontinental)
- Trade & commerce (trade, company, hudson, store)

**Geographic:**
- Saskatchewan places (prince albert, regina, saskatoon, qu'appelle)
- Regional development (settlement, district, territory)

**Indigenous Relations:**
- Treaties & reserves (treaty, reserve, band, chief)
- Indian Affairs (agent, inspector, department, affairs)

## Advanced Analysis (Future Work)

### Document Clustering
```python
# Load TF-IDF matrix and run k-means
import pickle
from sklearn.cluster import KMeans

with open('analysis_output/tfidf_matrix.pkl', 'rb') as f:
    tfidf_matrix = pickle.load(f)

kmeans = KMeans(n_clusters=10, random_state=42)
clusters = kmeans.fit_predict(tfidf_matrix)

# Analyze cluster compositions
```

### Time Series Analysis
- Track term frequencies over decades
- Identify emerging vs declining themes
- Detect shift points in discourse

### LLM-Enhanced Analysis (Future)
- Use GPT-4/Claude for named entity extraction
- Generate topic labels automatically
- Summarize representative documents per topic
- Extract structured historical events

## Data Characteristics

### Temporal Distribution
- 1800s-1830s: Early exploration narratives (sparse)
- 1840s-1860s: Settlement accounts, governance
- **1870s: Peak activity** (442 docs) - Territorial development
- **1880s: High activity** (371 docs) - Post-rebellion settlement

### Language Diversity
- Primarily English (857 docs)
- French (100 docs) - Including bilingual admin records
- Indigenous languages: Cree, Chipewyan (small sample)

### Document Quality
- OCR quality: Good to excellent
- Average document: ~68K words
- Range: Short pamphlets to 400K-word administrative files
- Multi-column layouts (newspapers) handled well

## Troubleshooting

### scikit-learn not found
```bash
pip install scikit-learn
```

### MALLET import fails
- Check MALLET_HOME is set correctly
- Ensure corpus.txt exists and has content
- Verify Java is installed: `java -version`

### Out of memory (MALLET)
- Reduce corpus size or split into subcorpora
- Increase Java heap: `export JAVA_OPTIONS="-Xmx4g"`

### Topics look nonsensical
- Increase stopwords (edit `stopwords.txt`)
- Try different topic counts (10, 20, 30, 50)
- Increase iterations (2000-5000)
- Check alpha/beta hyperparameters

## Files Summary

| File | Purpose |
|------|---------|
| `explore_saskatchewan_corpus.py` | Corpus overview & statistics |
| `spot_check_ocr.py` | OCR quality assessment |
| `build_tfidf_analysis.py` | TF-IDF analysis & distinctive terms |
| `build_mallet_corpus.py` | MALLET corpus preparation |
| `analysis_output/` | TF-IDF results & matrices |
| `mallet_corpus/` | MALLET input files & scripts |

## Citation

If you use this corpus or analysis tools, please cite:

```
Saskatchewan Historical Corpus (1808-1946)
Internet Archive Collections: Peel's Prairie Provinces, University of Alberta
Residential School Files (RG10), Library and Archives Canada
OCR Processing: OLMoCR (allenai/olmocr)
```

---

**Questions?** Check logs in `archive-olm-pipeline/logs/`
