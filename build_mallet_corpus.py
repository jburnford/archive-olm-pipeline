#!/usr/bin/env python3
"""
MALLET Topic Modeling - Corpus Preparation

Exports Saskatchewan corpus in MALLET input format and provides
scripts to run topic modeling with various configurations.
"""

import sqlite3
import json
import re
from pathlib import Path
from collections import defaultdict

db_path = "/home/jic823/archive-olm-pipeline/archive_tracking.db"
output_dir = Path("/home/jic823/archive-olm-pipeline/mallet_corpus")
output_dir.mkdir(exist_ok=True)

# Same stopwords as TF-IDF analysis
STOPWORDS = {
    # Standard English stopwords
    'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i', 'it',
    'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at', 'this',
    'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she', 'or',
    'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their', 'what',
    'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which', 'go', 'me',
    'when', 'make', 'can', 'like', 'time', 'no', 'just', 'him', 'know',
    'take', 'people', 'into', 'year', 'your', 'good', 'some', 'could',
    'them', 'see', 'other', 'than', 'then', 'now', 'look', 'only', 'come',
    'its', 'over', 'think', 'also', 'back', 'after', 'use', 'two', 'how',
    'our', 'work', 'first', 'well', 'way', 'even', 'new', 'want', 'because',
    'any', 'these', 'give', 'day', 'most', 'us', 'is', 'was', 'are', 'been',
    'has', 'had', 'were', 'said', 'did', 'having', 'may', 'should', 'does',
    # Historical/archival boilerplate (more aggressive for topic modeling)
    'microfilm', 'copy', 'copies', 'original', 'documentation', 'archives',
    'public', 'canada', 'canadian', 'permission', 'reproduce', 'reproduced',
    'obtained', 'research', 'purposes', 'provided', 'recipient', 'questions',
    'copyright', 'arise', 'assumed', 'responsibility', 'regarding', 'must',
    'exacte', 'documents', 'conservés', 'publiques', 'préparée', 'fins',
    'seulement', 'peut', 'être', 'sans', 'tenu', 'responsable', 'toute',
    'infraction', 'droit', 'propriété', 'page', 'pages', 'volume', 'file',
    'part', 'series', 'roll', 'reel', 'indian', 'affairs', 'department',
    # Very common words that add little value for topics
    'made', 'shall', 'being', 'such', 'upon', 'hereby', 'whereas', 'therefore',
}

def extract_text_from_ocr(ocr_json):
    """Extract text from OCR JSON."""
    try:
        pages = json.loads(ocr_json)
        full_text = ' '.join([page.get('text', '') for page in pages if 'text' in page])
        return full_text
    except:
        return ""

def clean_text_for_mallet(text):
    """Clean text for MALLET topic modeling."""
    # Convert to lowercase
    text = text.lower()

    # Remove URLs, emails, numbers
    text = re.sub(r'http\S+|www\.\S+|\S+@\S+|\d+', '', text)

    # Remove special characters
    text = re.sub(r'[^a-z\s\-]', ' ', text)

    # Extract words (4+ letters)
    words = re.findall(r'\b[a-z]+(?:-[a-z]+)*\b', text)
    words = [w for w in words if len(w) >= 4 and w not in STOPWORDS]

    return ' '.join(words)

def categorize_document(row):
    """Categorize document by type."""
    title = (row['title'] or '').lower()
    subject = (row['subject'] or '').lower()

    if 'residential school' in subject or 'school files' in subject:
        return 'residential_school'
    elif 'newspaper' in subject or 'times' in subject or 'review' in subject:
        return 'newspaper'
    elif 'annual report' in title or 'report of' in title:
        return 'annual_report'
    elif 'census' in title or 'population' in title:
        return 'census'
    elif 'gazette' in title or 'ordinance' in title:
        return 'government'
    else:
        return 'other'

print("=" * 80)
print("MALLET CORPUS PREPARATION")
print("=" * 80)

# 1. Load and prepare documents
print("\n1. Loading documents from database...")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

cursor = conn.execute("""
    SELECT
        i.identifier,
        i.title,
        i.subject,
        i.publisher,
        i.year,
        p.filename,
        o.ocr_data
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    JOIN items i ON p.identifier = i.identifier
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND o.status = 'completed'
      AND o.ocr_data IS NOT NULL
""")

# MALLET format: one document per line
# Format: [doc_id] [label] [text]
mallet_file = output_dir / 'corpus.txt'
metadata_file = output_dir / 'metadata.tsv'

doc_count = 0
decade_counts = defaultdict(int)
type_counts = defaultdict(int)

with open(mallet_file, 'w', encoding='utf-8') as mallet_f, \
     open(metadata_file, 'w', encoding='utf-8') as meta_f:

    # Write metadata header
    meta_f.write("doc_id\tfilename\ttitle\tyear\tdecade\tdoc_type\tpublisher\n")

    for row in cursor:
        text = extract_text_from_ocr(row['ocr_data'])
        if not text:
            continue

        cleaned = clean_text_for_mallet(text)
        word_count = len(cleaned.split())

        if word_count < 100:  # Skip very short documents
            continue

        # Create document ID and label
        doc_id = f"doc_{doc_count:05d}"
        decade = (row['year'] // 10) * 10 if row['year'] else 0
        doc_type = categorize_document(row)
        label = f"{decade}_{doc_type}"

        # Write to MALLET file
        mallet_f.write(f"{doc_id} {label} {cleaned}\n")

        # Write metadata
        meta_f.write(f"{doc_id}\t{row['filename']}\t{row['title'][:100]}\t"
                    f"{row['year']}\t{decade}\t{doc_type}\t{row['publisher'] or 'unknown'}\n")

        decade_counts[decade] += 1
        type_counts[doc_type] += 1
        doc_count += 1

conn.close()

print(f"   Processed {doc_count} documents")
print(f"\n   Documents by decade:")
for decade in sorted(decade_counts.keys()):
    print(f"      {decade}s: {decade_counts[decade]}")

print(f"\n   Documents by type:")
for doc_type in sorted(type_counts.keys()):
    print(f"      {doc_type}: {type_counts[doc_type]}")

# 2. Create stopwords file for MALLET
stopwords_file = output_dir / 'stopwords.txt'
with open(stopwords_file, 'w') as f:
    for word in sorted(STOPWORDS):
        f.write(f"{word}\n")

print(f"\n2. Stopwords file created: {stopwords_file}")

# 3. Create MALLET run scripts
print("\n3. Creating MALLET execution scripts...")

# Import script
import_script = output_dir / 'run_mallet_import.sh'
with open(import_script, 'w') as f:
    f.write("""#!/bin/bash
# Import corpus into MALLET format

MALLET_HOME=${MALLET_HOME:-/path/to/mallet}
CORPUS_DIR=$(dirname "$0")

# Check if MALLET is installed
if [ ! -f "$MALLET_HOME/bin/mallet" ]; then
    echo "ERROR: MALLET not found at $MALLET_HOME"
    echo "Please set MALLET_HOME environment variable or edit this script"
    exit 1
fi

echo "Importing corpus into MALLET format..."

$MALLET_HOME/bin/mallet import-file \\
    --input "$CORPUS_DIR/corpus.txt" \\
    --output "$CORPUS_DIR/corpus.mallet" \\
    --keep-sequence \\
    --remove-stopwords \\
    --extra-stopwords "$CORPUS_DIR/stopwords.txt" \\
    --token-regex '[a-z]+' \\
    --preserve-case FALSE

echo "Import complete: corpus.mallet created"
""")
import_script.chmod(0o755)

# Topic modeling script
topic_script = output_dir / 'run_mallet_topics.sh'
with open(topic_script, 'w') as f:
    f.write("""#!/bin/bash
# Run MALLET topic modeling with various configurations

MALLET_HOME=${MALLET_HOME:-/path/to/mallet}
CORPUS_DIR=$(dirname "$0")
NUM_TOPICS=${1:-20}
NUM_ITERATIONS=${2:-1000}

# Check if corpus.mallet exists
if [ ! -f "$CORPUS_DIR/corpus.mallet" ]; then
    echo "ERROR: corpus.mallet not found. Run run_mallet_import.sh first"
    exit 1
fi

echo "Running topic modeling..."
echo "  Topics: $NUM_TOPICS"
echo "  Iterations: $NUM_ITERATIONS"

OUTPUT_DIR="$CORPUS_DIR/topics_${NUM_TOPICS}"
mkdir -p "$OUTPUT_DIR"

$MALLET_HOME/bin/mallet train-topics \\
    --input "$CORPUS_DIR/corpus.mallet" \\
    --num-topics $NUM_TOPICS \\
    --num-iterations $NUM_ITERATIONS \\
    --optimize-interval 10 \\
    --output-state "$OUTPUT_DIR/topic-state.gz" \\
    --output-doc-topics "$OUTPUT_DIR/doc-topics.txt" \\
    --output-topic-keys "$OUTPUT_DIR/topic-keys.txt" \\
    --word-topic-counts-file "$OUTPUT_DIR/word-topic-counts.txt" \\
    --num-top-words 20 \\
    --alpha 5.0 \\
    --beta 0.01

echo ""
echo "Topic modeling complete!"
echo "Results saved to: $OUTPUT_DIR/"
echo ""
echo "Key output files:"
echo "  - topic-keys.txt: Top words for each topic"
echo "  - doc-topics.txt: Topic distribution per document"
echo "  - word-topic-counts.txt: Word-topic associations"
""")
topic_script.chmod(0o755)

# Analysis helper script
analysis_script = output_dir / 'analyze_topics.py'
with open(analysis_script, 'w') as f:
    f.write("""#!/usr/bin/env python3
\"\"\"
Analyze MALLET topic modeling results.
\"\"\"

import sys
from pathlib import Path
import pandas as pd
from collections import defaultdict

if len(sys.argv) < 2:
    print("Usage: python analyze_topics.py <topics_directory>")
    print("Example: python analyze_topics.py topics_20")
    sys.exit(1)

topics_dir = Path(sys.argv[1])

# Load topic keys
print("=" * 80)
print("TOPIC ANALYSIS")
print("=" * 80)

topic_keys_file = topics_dir / 'topic-keys.txt'
if topic_keys_file.exists():
    print(f"\\nTOP TOPICS (from {topic_keys_file}):")
    print("-" * 80)
    with open(topic_keys_file) as f:
        for line in f:
            parts = line.strip().split('\\t')
            if len(parts) >= 3:
                topic_id = parts[0]
                weight = parts[1]
                words = parts[2]
                print(f"\\nTopic {topic_id} (weight: {weight}):")
                print(f"  {words}")

# Load document-topics
doc_topics_file = topics_dir / 'doc-topics.txt'
metadata_file = Path('metadata.tsv')

if doc_topics_file.exists() and metadata_file.exists():
    print(f"\\n\\nDOCUMENT-TOPIC DISTRIBUTIONS:")
    print("-" * 80)

    # Load metadata
    metadata = pd.read_csv(metadata_file, sep='\\t')

    # Parse doc-topics file (format: doc_id doc_name topic_proportions...)
    topic_dist = []
    with open(doc_topics_file) as f:
        for line in f:
            if line.startswith('#'):
                continue
            parts = line.strip().split('\\t')
            doc_id = parts[1]
            topics = [(int(parts[i]), float(parts[i+1]))
                     for i in range(2, len(parts), 2)]
            topic_dist.append({'doc_id': doc_id, 'topics': topics})

    # Analyze by decade
    print("\\nTop topics by decade:")
    decade_topics = defaultdict(lambda: defaultdict(float))

    for doc in topic_dist:
        doc_id = doc['doc_id']
        doc_meta = metadata[metadata['doc_id'] == doc_id]
        if not doc_meta.empty:
            decade = doc_meta.iloc[0]['decade']
            for topic_id, prob in doc['topics']:
                decade_topics[decade][topic_id] += prob

    for decade in sorted(decade_topics.keys()):
        top_topics = sorted(decade_topics[decade].items(),
                          key=lambda x: x[1], reverse=True)[:3]
        print(f"\\n  {decade}s:")
        for topic_id, weight in top_topics:
            print(f"    Topic {topic_id}: {weight:.2f}")

    # Analyze by document type
    print("\\nTop topics by document type:")
    type_topics = defaultdict(lambda: defaultdict(float))

    for doc in topic_dist:
        doc_id = doc['doc_id']
        doc_meta = metadata[metadata['doc_id'] == doc_id]
        if not doc_meta.empty:
            doc_type = doc_meta.iloc[0]['doc_type']
            for topic_id, prob in doc['topics']:
                type_topics[doc_type][topic_id] += prob

    for doc_type in sorted(type_topics.keys()):
        top_topics = sorted(type_topics[doc_type].items(),
                          key=lambda x: x[1], reverse=True)[:3]
        print(f"\\n  {doc_type}:")
        for topic_id, weight in top_topics:
            print(f"    Topic {topic_id}: {weight:.2f}")

print("\\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
""")
analysis_script.chmod(0o755)

print(f"   Created: {import_script}")
print(f"   Created: {topic_script}")
print(f"   Created: {analysis_script}")

# 4. Print instructions
print("\n" + "=" * 80)
print("MALLET CORPUS READY")
print("=" * 80)

print(f"\nCorpus files created in: {output_dir}/")
print(f"  - corpus.txt: {doc_count} documents in MALLET format")
print(f"  - metadata.tsv: Document metadata")
print(f"  - stopwords.txt: Stopword list")

print("\n" + "=" * 80)
print("NEXT STEPS - TOPIC MODELING WITH MALLET")
print("=" * 80)

print("""
1. INSTALL MALLET (if not already installed):
   - Download from: http://mallet.cs.umass.edu/download.php
   - Extract and set MALLET_HOME environment variable:
     export MALLET_HOME=/path/to/mallet-2.0.8

2. IMPORT CORPUS:
   cd {corpus_dir}
   ./run_mallet_import.sh

3. RUN TOPIC MODELING:
   ./run_mallet_topics.sh 20 1000    # 20 topics, 1000 iterations

   Try different configurations:
   ./run_mallet_topics.sh 10 1000    # Fewer topics
   ./run_mallet_topics.sh 30 2000    # More topics, more iterations

4. ANALYZE RESULTS:
   python analyze_topics.py topics_20/

5. RECOMMENDED TOPIC COUNTS TO TRY:
   - 10 topics: Broad themes
   - 20 topics: Medium granularity (recommended start)
   - 30 topics: Fine-grained topics
   - 50 topics: Very detailed (for large corpus)

6. INTERPRETING RESULTS:
   - topic-keys.txt: Shows top words for each topic
   - doc-topics.txt: Shows topic distribution for each document
   - Look for topics that align with:
     * Time periods (1870s rebellion vs 1880s settlement)
     * Document types (newspapers vs administrative records)
     * Themes (education, land, Indigenous relations)
""".format(corpus_dir=output_dir))

print("\n" + "=" * 80)
