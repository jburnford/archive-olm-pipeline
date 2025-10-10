#!/usr/bin/env python3
"""
Explore Saskatchewan corpus - metadata and content analysis.
"""

import sqlite3
import json
from collections import Counter, defaultdict
import re

db_path = "/home/jic823/archive-olm-pipeline/archive_tracking.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

print("=" * 80)
print("SASKATCHEWAN CORPUS EXPLORATION")
print("=" * 80)

# 1. METADATA ANALYSIS
print("\n1. METADATA OVERVIEW")
print("-" * 80)

# Get basic metadata stats
cursor = conn.execute("""
    SELECT
        i.title,
        i.creator,
        i.publisher,
        i.date,
        i.year,
        i.subject,
        i.language,
        i.collection,
        p.filename
    FROM items i
    JOIN pdf_files p ON i.identifier = p.identifier
    WHERE p.subcollection = 'saskatchewan_1808_1946'
    LIMIT 1000
""")

titles = []
creators = []
publishers = []
years = []
subjects = []
languages = []
collections = []

for row in cursor:
    if row['title']:
        titles.append(row['title'])
    if row['creator']:
        creators.append(row['creator'])
    if row['publisher']:
        publishers.append(row['publisher'])
    if row['year']:
        years.append(row['year'])
    if row['subject']:
        # Split multi-value subjects
        for subj in row['subject'].split(';'):
            subjects.append(subj.strip())
    if row['language']:
        languages.append(row['language'])
    if row['collection']:
        collections.append(row['collection'])

print(f"Metadata records examined: {len(titles)}")
print(f"\nDate Range: {min(years) if years else 'N/A'} - {max(years) if years else 'N/A'}")

# 2. TOP SUBJECTS
print("\n2. TOP SUBJECTS (from metadata)")
print("-" * 80)
subject_counts = Counter(subjects)
for subject, count in subject_counts.most_common(20):
    print(f"  {count:4d}  {subject}")

# 3. TOP PUBLISHERS
print("\n3. TOP PUBLISHERS")
print("-" * 80)
publisher_counts = Counter(publishers)
for pub, count in publisher_counts.most_common(15):
    print(f"  {count:4d}  {pub}")

# 4. TOP CREATORS/AUTHORS
print("\n4. TOP CREATORS/AUTHORS")
print("-" * 80)
creator_counts = Counter(creators)
for creator, count in creator_counts.most_common(15):
    print(f"  {count:4d}  {creator}")

# 5. LANGUAGES
print("\n5. LANGUAGES")
print("-" * 80)
lang_counts = Counter(languages)
for lang, count in lang_counts.items():
    print(f"  {count:4d}  {lang}")

# 6. COLLECTIONS
print("\n6. INTERNET ARCHIVE COLLECTIONS")
print("-" * 80)
collection_counts = Counter(collections)
for coll, count in collection_counts.most_common(15):
    print(f"  {count:4d}  {coll}")

# 7. TEMPORAL DISTRIBUTION
print("\n7. TEMPORAL DISTRIBUTION (by decade)")
print("-" * 80)
decades = defaultdict(int)
for year in years:
    decade = (year // 10) * 10
    decades[decade] += 1

for decade in sorted(decades.keys()):
    bar = "█" * (decades[decade] // 10)
    print(f"  {decade}s: {decades[decade]:4d}  {bar}")

# 8. CORPUS SCALE - WORD COUNTS
print("\n8. CORPUS SCALE - WORD COUNT ANALYSIS")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        COUNT(*) as doc_count,
        SUM(LENGTH(o.ocr_data)) as total_bytes,
        AVG(LENGTH(o.ocr_data)) as avg_bytes,
        MIN(LENGTH(o.ocr_data)) as min_bytes,
        MAX(LENGTH(o.ocr_data)) as max_bytes
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND o.status = 'completed'
      AND o.ocr_data IS NOT NULL
""")

stats_row = cursor.fetchone()
doc_count = stats_row['doc_count']
print(f"  Documents with OCR: {doc_count:,}")
print(f"  Total OCR data: {stats_row['total_bytes'] / 1024 / 1024:.1f} MB")
print(f"  Average per doc: {stats_row['avg_bytes'] / 1024:.1f} KB")
print(f"  Range: {stats_row['min_bytes'] / 1024:.0f} KB - {stats_row['max_bytes'] / 1024 / 1024:.1f} MB")

# Estimate word counts (rough: 1 word ~ 6 chars including spaces)
total_words_est = stats_row['total_bytes'] // 6
avg_words_est = stats_row['avg_bytes'] // 6
print(f"\n  Estimated total words: {total_words_est:,}")
print(f"  Estimated avg words/doc: {avg_words_est:,}")

# Get actual word count from sample
cursor = conn.execute("""
    SELECT o.ocr_data
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND o.status = 'completed'
      AND o.ocr_data IS NOT NULL
    LIMIT 10
""")

actual_word_counts = []
for row in cursor:
    try:
        ocr_data = json.loads(row['ocr_data'])
        total_text = ' '.join([page.get('text', '') for page in ocr_data if 'text' in page])
        words = len(re.findall(r'\b\w+\b', total_text))
        actual_word_counts.append(words)
    except:
        pass

if actual_word_counts:
    avg_actual = sum(actual_word_counts) / len(actual_word_counts)
    print(f"\n  Actual avg words/doc (sample of 10): {avg_actual:,.0f}")
    print(f"  Projected total corpus words: {avg_actual * doc_count:,.0f}")

# Document size distribution
print("\n  Document Size Distribution (OCR data):")
cursor = conn.execute("""
    SELECT
        CASE
            WHEN LENGTH(o.ocr_data) < 100000 THEN '< 100 KB'
            WHEN LENGTH(o.ocr_data) < 500000 THEN '100-500 KB'
            WHEN LENGTH(o.ocr_data) < 1000000 THEN '500 KB - 1 MB'
            WHEN LENGTH(o.ocr_data) < 5000000 THEN '1-5 MB'
            WHEN LENGTH(o.ocr_data) < 10000000 THEN '5-10 MB'
            ELSE '> 10 MB'
        END as size_range,
        COUNT(*) as count
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND o.status = 'completed'
    GROUP BY size_range
    ORDER BY MIN(LENGTH(o.ocr_data))
""")
for row in cursor:
    bar = "█" * (row['count'] // 20)
    print(f"    {row['size_range']:15s}: {row['count']:4d}  {bar}")

# Page count distribution
print("\n  Page Count Distribution:")
cursor = conn.execute("""
    SELECT o.ocr_data
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND o.status = 'completed'
      AND o.ocr_data IS NOT NULL
    LIMIT 100
""")

page_counts = []
for row in cursor:
    try:
        ocr_data = json.loads(row['ocr_data'])
        page_counts.append(len(ocr_data))
    except:
        pass

if page_counts:
    print(f"    Average pages/doc: {sum(page_counts) / len(page_counts):.0f}")
    print(f"    Min pages: {min(page_counts)}")
    print(f"    Max pages: {max(page_counts)}")
    print(f"    Total pages (sample): {sum(page_counts):,}")

# 9. OCR CONTENT SAMPLING
print("\n9. OCR CONTENT SAMPLING")
print("-" * 80)

cursor = conn.execute("""
    SELECT
        p.filename,
        i.title,
        i.year,
        o.ocr_data,
        LENGTH(o.ocr_data) as size
    FROM ocr_processing o
    JOIN pdf_files p ON o.pdf_file_id = p.id
    JOIN items i ON p.identifier = i.identifier
    WHERE p.subcollection = 'saskatchewan_1808_1946'
      AND o.status = 'completed'
      AND o.ocr_data IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 5
""")

print("\nSample documents with OCR content:\n")

word_freq = Counter()
doc_lengths = []

for row in cursor:
    print(f"  Title: {row['title'][:70]}...")
    print(f"  Year: {row['year']}")
    print(f"  File: {row['filename']}")
    print(f"  OCR size: {row['size']:,} bytes")

    # Parse OCR data and extract some text
    try:
        ocr_data = json.loads(row['ocr_data'])

        # Get first page text sample
        if ocr_data and len(ocr_data) > 0:
            first_page = ocr_data[0]
            if 'text' in first_page:
                text_sample = first_page['text'][:200].replace('\n', ' ')
                print(f"  Sample: {text_sample}...")

                # Word frequency analysis
                words = re.findall(r'\b[a-z]{4,}\b', first_page['text'].lower())
                word_freq.update(words)
                doc_lengths.append(len(first_page['text']))
    except:
        print(f"  (Could not parse OCR data)")

    print()

# 10. WORD FREQUENCY FROM SAMPLES
if word_freq:
    print("\n10. TOP WORDS (from sampled OCR - 4+ letters)")
    print("-" * 80)
    # Remove common stopwords
    stopwords = {'that', 'this', 'with', 'from', 'have', 'been', 'were', 'will',
                 'would', 'their', 'there', 'which', 'about', 'other', 'when',
                 'these', 'them', 'being', 'into', 'such', 'some', 'upon'}

    for word, count in word_freq.most_common(40):
        if word not in stopwords:
            print(f"  {count:3d}  {word}")

# 11. DOCUMENT TYPE DETECTION (from titles)
print("\n11. DOCUMENT TYPES (inferred from titles)")
print("-" * 80)

doc_types = defaultdict(int)
for title in titles:
    title_lower = title.lower()
    if any(word in title_lower for word in ['annual report', 'report of', 'report on', 'annual']):
        doc_types['Annual Reports'] += 1
    elif any(word in title_lower for word in ['census', 'population']):
        doc_types['Census/Statistical'] += 1
    elif any(word in title_lower for word in ['gazette', 'ordinance', 'act', 'statute']):
        doc_types['Government Gazettes/Laws'] += 1
    elif any(word in title_lower for word in ['directory', 'guide']):
        doc_types['Directories/Guides'] += 1
    elif any(word in title_lower for word in ['handbook', 'manual']):
        doc_types['Handbooks/Manuals'] += 1
    elif any(word in title_lower for word in ['map', 'atlas']):
        doc_types['Maps/Atlases'] += 1
    elif any(word in title_lower for word in ['newspaper', 'journal', 'magazine']):
        doc_types['Periodicals'] += 1
    elif any(word in title_lower for word in ['history', 'historical']):
        doc_types['Historical Works'] += 1
    else:
        doc_types['Other'] += 1

for dtype, count in sorted(doc_types.items(), key=lambda x: -x[1]):
    print(f"  {count:4d}  {dtype}")

print("\n" + "=" * 80)
print("RECOMMENDATIONS FOR NEXT STEPS")
print("=" * 80)

print("""
Based on this preliminary analysis, here's a suggested workflow:

1. METADATA CATALOGING:
   - Export subject terms → create controlled vocabulary
   - Temporal clustering (by decade + publisher/subject)
   - Geographic focus analysis (Saskatchewan places mentioned)

2. TF-IDF ANALYSIS:
   - Build document-term matrix from OCR text
   - Identify distinctive terms per decade/publisher/subject
   - Find documents similar to seed examples

3. TOPIC MODELING (MALLET):
   - LDA with 10-30 topics to discover themes
   - Compare topics across time periods
   - Label topics based on top terms + sample documents

4. NAMED ENTITY EXTRACTION:
   - Extract place names (Saskatchewan locations)
   - Extract person names (government officials, settlers)
   - Extract organization names (companies, institutions)

5. DOCUMENT CLUSTERING:
   - K-means or hierarchical clustering on TF-IDF vectors
   - Identify document types beyond title keywords
   - Find anomalies/unique documents

Should I create scripts for TF-IDF analysis and MALLET preprocessing?
""")

conn.close()
