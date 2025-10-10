#!/usr/bin/env python3
"""
TF-IDF Analysis for Saskatchewan Corpus.

Creates document-term matrix and identifies distinctive terms by:
- Time period (decade)
- Document type (inferred from metadata)
- Publisher/source
"""

import sqlite3
import json
import re
from collections import defaultdict, Counter
from pathlib import Path
import pickle

# Will ask user to install these if missing
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("WARNING: scikit-learn not installed. Install with:")
    print("  pip install scikit-learn")
    print()

db_path = "/home/jic823/archive-olm-pipeline/archive_tracking.db"
output_dir = Path("/home/jic823/archive-olm-pipeline/analysis_output")
output_dir.mkdir(exist_ok=True)

# Stopwords for historical texts (includes common OCR errors and boilerplate)
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
    # Historical/archival boilerplate
    'microfilm', 'copy', 'original', 'documentation', 'archives', 'public',
    'canada', 'permission', 'reproduce', 'obtained', 'research', 'purposes',
    'provided', 'recipient', 'questions', 'copyright', 'arise', 'assumed',
    'responsibility', 'regarding', 'must', 'exacte', 'documents', 'conservés',
    'publiques', 'préparée', 'fins', 'seulement', 'peut', 'être', 'sans',
    'tenu', 'responsable', 'toute', 'infraction', 'droit', 'propriété',
    # Common OCR errors
    'rn', 'rrn', 'lll', 'iii',
}

def extract_text_from_ocr(ocr_json):
    """Extract and clean text from OCR JSON data."""
    try:
        pages = json.loads(ocr_json)
        full_text = ' '.join([page.get('text', '') for page in pages if 'text' in page])
        return full_text
    except:
        return ""

def clean_text(text):
    """Clean and normalize text for analysis."""
    # Convert to lowercase
    text = text.lower()

    # Remove URLs and emails
    text = re.sub(r'http\S+|www\.\S+|\S+@\S+', '', text)

    # Remove special characters but keep hyphens in words
    text = re.sub(r'[^a-z\s\-]', ' ', text)

    # Extract words (4+ letters, handles hyphenated words)
    words = re.findall(r'\b[a-z]+(?:-[a-z]+)*\b', text)
    words = [w for w in words if len(w) >= 4]

    # Remove stopwords
    words = [w for w in words if w not in STOPWORDS]

    return ' '.join(words)

def categorize_document(row):
    """Categorize document by type from metadata."""
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
    elif 'map' in title or 'atlas' in title:
        return 'map'
    else:
        return 'other'

print("=" * 80)
print("TF-IDF ANALYSIS - Saskatchewan Corpus")
print("=" * 80)

if not SKLEARN_AVAILABLE:
    print("\nERROR: scikit-learn is required. Please install it first.")
    exit(1)

# Load data
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

documents = []
metadata = []

for row in cursor:
    text = extract_text_from_ocr(row['ocr_data'])
    if not text:
        continue

    cleaned = clean_text(text)
    if len(cleaned.split()) < 50:  # Skip very short documents
        continue

    documents.append(cleaned)
    metadata.append({
        'identifier': row['identifier'],
        'title': row['title'],
        'filename': row['filename'],
        'year': row['year'],
        'decade': (row['year'] // 10) * 10 if row['year'] else None,
        'publisher': row['publisher'],
        'doc_type': categorize_document(row),
        'subject': row['subject']
    })

conn.close()

print(f"   Loaded {len(documents)} documents")

# Build TF-IDF matrix
print("\n2. Building TF-IDF matrix...")
vectorizer = TfidfVectorizer(
    max_features=5000,
    min_df=2,  # Term must appear in at least 2 documents
    max_df=0.8,  # Ignore terms in more than 80% of documents
    ngram_range=(1, 2),  # Include unigrams and bigrams
)

tfidf_matrix = vectorizer.fit_transform(documents)
feature_names = vectorizer.get_feature_names_out()

print(f"   Matrix shape: {tfidf_matrix.shape}")
print(f"   Vocabulary size: {len(feature_names)}")

# Save for later use
print("\n3. Saving TF-IDF model...")
with open(output_dir / 'tfidf_vectorizer.pkl', 'wb') as f:
    pickle.dump(vectorizer, f)

with open(output_dir / 'tfidf_matrix.pkl', 'wb') as f:
    pickle.dump(tfidf_matrix, f)

with open(output_dir / 'document_metadata.pkl', 'wb') as f:
    pickle.dump(metadata, f)

print(f"   Saved to {output_dir}/")

# Analyze distinctive terms by category
print("\n4. ANALYZING DISTINCTIVE TERMS")
print("=" * 80)

# Group documents by category
def get_top_tfidf_terms(doc_indices, n=20):
    """Get top TF-IDF terms for a set of documents."""
    # Average TF-IDF scores across documents
    if len(doc_indices) == 0:
        return []

    avg_tfidf = np.asarray(tfidf_matrix[doc_indices].mean(axis=0)).flatten()
    top_indices = avg_tfidf.argsort()[-n:][::-1]

    return [(feature_names[i], avg_tfidf[i]) for i in top_indices]

# By decade
print("\nA. DISTINCTIVE TERMS BY DECADE:")
print("-" * 80)

decade_groups = defaultdict(list)
for idx, meta in enumerate(metadata):
    if meta['decade']:
        decade_groups[meta['decade']].append(idx)

for decade in sorted(decade_groups.keys()):
    doc_indices = decade_groups[decade]
    top_terms = get_top_tfidf_terms(doc_indices, 15)

    print(f"\n{decade}s ({len(doc_indices)} docs):")
    for term, score in top_terms:
        print(f"  {score:.4f}  {term}")

# By document type
print("\n\nB. DISTINCTIVE TERMS BY DOCUMENT TYPE:")
print("-" * 80)

type_groups = defaultdict(list)
for idx, meta in enumerate(metadata):
    type_groups[meta['doc_type']].append(idx)

for doc_type in sorted(type_groups.keys()):
    doc_indices = type_groups[doc_type]
    if len(doc_indices) < 5:  # Skip categories with too few documents
        continue

    top_terms = get_top_tfidf_terms(doc_indices, 15)

    print(f"\n{doc_type.upper()} ({len(doc_indices)} docs):")
    for term, score in top_terms:
        print(f"  {score:.4f}  {term}")

# Document similarity
print("\n\n5. DOCUMENT SIMILARITY ANALYSIS")
print("=" * 80)

print("\nFinding similar document pairs...")
similarity_matrix = cosine_similarity(tfidf_matrix)

# Find top similar pairs (excluding self-similarity)
similar_pairs = []
for i in range(len(documents)):
    for j in range(i+1, min(i+50, len(documents))):  # Only check nearby docs for speed
        sim = similarity_matrix[i, j]
        if sim > 0.3:  # Threshold for similarity
            similar_pairs.append((i, j, sim))

similar_pairs.sort(key=lambda x: x[2], reverse=True)

print(f"\nTop 10 most similar document pairs:")
print("-" * 80)
for i, j, sim in similar_pairs[:10]:
    print(f"\nSimilarity: {sim:.3f}")
    print(f"  Doc 1: {metadata[i]['title'][:60]}... ({metadata[i]['year']})")
    print(f"  Doc 2: {metadata[j]['title'][:60]}... ({metadata[j]['year']})")

# Export term list for MALLET
print("\n\n6. EXPORTING FOR EXTERNAL TOOLS")
print("=" * 80)

# Export vocabulary
vocab_file = output_dir / 'vocabulary.txt'
with open(vocab_file, 'w') as f:
    for term in feature_names:
        f.write(f"{term}\n")
print(f"   Vocabulary exported to {vocab_file}")

# Export document list with metadata
doc_list_file = output_dir / 'document_list.txt'
with open(doc_list_file, 'w') as f:
    f.write("id\tfilename\ttitle\tyear\tdecade\tdoc_type\n")
    for idx, meta in enumerate(metadata):
        f.write(f"{idx}\t{meta['filename']}\t{meta['title'][:50]}\t{meta['year']}\t{meta['decade']}\t{meta['doc_type']}\n")
print(f"   Document list exported to {doc_list_file}")

print("\n" + "=" * 80)
print("TF-IDF ANALYSIS COMPLETE")
print("=" * 80)
print(f"\nOutput files saved to: {output_dir}/")
print("\nNext steps:")
print("  - Review distinctive terms by decade/type")
print("  - Use tfidf_matrix.pkl for clustering")
print("  - Run MALLET topic modeling (see build_mallet_corpus.py)")
