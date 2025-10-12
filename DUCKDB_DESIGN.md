# DuckDB Warehouse Design

## Overview

DuckDB serves as the **data warehouse** for querying and analyzing millions of processed documents. It is **NOT** used for pipeline coordination (JSON files handle that).

## Architecture

```
Pipeline Coordination (JSON)          Data Warehouse (DuckDB)
├─ processed_pdfs.json        ─────→  ├─ documents table
├─ download_progress.json             ├─ pages table
└─ batch manifests                    ├─ batches table
                                      └─ processing_log table
```

## Why DuckDB?

1. **Serverless** - No PostgreSQL setup, no administration
2. **Portable** - Copy the .duckdb file to your laptop and query
3. **Fast** - Columnar storage, vectorized execution
4. **SQL** - Full SQL:2003 support with window functions, CTEs, etc.
5. **Scalable** - Handles billions of rows efficiently
6. **Export-friendly** - SQL → Parquet/CSV for ML pipelines

## Schema Design

### Core Tables

#### 1. `documents` - One row per Archive.org item
```sql
CREATE TABLE documents (
    identifier TEXT PRIMARY KEY,
    title TEXT,
    collection TEXT,
    creator TEXT,
    year INTEGER,
    language TEXT,
    subject TEXT[],  -- Array of subjects

    -- Files
    original_filename TEXT,
    pdf_size_bytes BIGINT,

    -- OCR metadata
    ocr_json_path TEXT,
    ocr_markdown_path TEXT,
    total_pages INTEGER,
    total_chars BIGINT,

    -- Processing info
    processed_at TIMESTAMP,
    batch_id INTEGER,
    processing_time_seconds INTEGER,

    -- Storage locations
    processed_dir TEXT,
    archive_url TEXT
);
```

#### 2. `pages` - One row per page (for detailed analysis)
```sql
CREATE TABLE pages (
    identifier TEXT,
    page_number INTEGER,
    text_content TEXT,
    char_count INTEGER,
    word_count INTEGER,
    confidence_avg FLOAT,  -- If available from OLMoCR
    has_tables BOOLEAN,
    has_images BOOLEAN,

    PRIMARY KEY (identifier, page_number)
);
```

#### 3. `batches` - Track processing batches
```sql
CREATE TABLE batches (
    batch_id INTEGER PRIMARY KEY,
    submitted_at TIMESTAMP,
    completed_at TIMESTAMP,
    pdf_count INTEGER,
    total_pages INTEGER,
    chunk_count INTEGER,
    job_ids TEXT[],  -- SLURM job IDs
    status TEXT,  -- submitted, processing, completed, failed

    -- Performance metrics
    avg_pages_per_second FLOAT,
    total_walltime_hours FLOAT
);
```

#### 4. `processing_log` - Audit trail
```sql
CREATE TABLE processing_log (
    log_id INTEGER PRIMARY KEY,
    timestamp TIMESTAMP,
    event_type TEXT,  -- download, ocr_start, ocr_complete, finalize
    identifier TEXT,
    batch_id INTEGER,
    details TEXT,  -- JSON string for flexible logging

    FOREIGN KEY (identifier) REFERENCES documents(identifier)
);
```

## Usage Patterns

### 1. Query Documents

```sql
-- Find all documents from 1890s in Caribbean collection
SELECT identifier, title, year, total_pages
FROM documents
WHERE year BETWEEN 1890 AND 1899
  AND collection LIKE '%caribbean%'
ORDER BY year, title;

-- Statistics by decade
SELECT
    (year / 10) * 10 AS decade,
    COUNT(*) AS document_count,
    SUM(total_pages) AS total_pages,
    AVG(total_pages) AS avg_pages_per_doc
FROM documents
WHERE year IS NOT NULL
GROUP BY decade
ORDER BY decade;
```

### 2. Full-Text Search (with pages table)

```sql
-- Find documents mentioning "sugar plantation"
SELECT DISTINCT d.identifier, d.title, d.year, COUNT(*) AS mention_count
FROM documents d
JOIN pages p ON d.identifier = p.identifier
WHERE p.text_content LIKE '%sugar plantation%'
GROUP BY d.identifier, d.title, d.year
ORDER BY mention_count DESC
LIMIT 50;
```

### 3. Export Subsets for Processing

```sql
-- Export 1890s documents to Parquet for ML pipeline
COPY (
    SELECT d.identifier, d.title, d.year, p.page_number, p.text_content
    FROM documents d
    JOIN pages p ON d.identifier = p.identifier
    WHERE d.year BETWEEN 1890 AND 1899
) TO '1890s_documents.parquet' (FORMAT PARQUET);
```

### 4. Processing Analytics

```sql
-- Batch performance report
SELECT
    batch_id,
    submitted_at,
    pdf_count,
    total_pages,
    chunk_count,
    avg_pages_per_second,
    total_walltime_hours,
    ROUND(total_pages / NULLIF(total_walltime_hours, 0), 2) AS pages_per_hour
FROM batches
ORDER BY submitted_at DESC
LIMIT 10;
```

## Workflow Integration

### Building the Catalog

```bash
# After finalization completes, rebuild catalog
cd ~/projects/def-jic823/archive-olm-pipeline

python3 tools/export_catalog_duckdb.py \
  --base-dir ~/projects/def-jic823/caribbean_pipeline \
  --fast  # Skip page-by-page parsing for speed

# Output: caribbean_pipeline/export/catalog.duckdb
```

### Incremental Updates

For large-scale operations, we can implement incremental updates:

```python
# Pseudocode for incremental catalog update
new_identifiers = get_newly_processed_since(last_update)
for identifier in new_identifiers:
    add_document_to_catalog(identifier)
    add_pages_to_catalog(identifier)
```

### Downloading for Local Analysis

```bash
# Copy catalog to WSL
scp nibi:~/projects/def-jic823/caribbean_pipeline/export/catalog.duckdb \
    ~/local/data/

# Query locally
duckdb ~/local/data/catalog.duckdb

# Or use Python
python3 -c "
import duckdb
con = duckdb.connect('catalog.duckdb')
df = con.execute('SELECT * FROM documents LIMIT 10').df()
print(df)
"
```

## Scaling Considerations

### At 100K Documents
- **Catalog size**: ~500MB-1GB
- **Query time**: Milliseconds
- **Build time**: 5-10 minutes
- **Export time**: Fast (parallel Parquet writes)

### At 1M Documents
- **Catalog size**: ~5-10GB
- **Query time**: Still fast (columnar storage)
- **Build time**: ~1 hour
- **Recommendation**: Implement incremental updates

### At 10M Documents (Future)
- **Catalog size**: ~50-100GB
- **Partitioning**: By year, collection, or batch_id ranges
- **Multiple files**: `catalog_2020.duckdb`, `catalog_2021.duckdb`
- **Attach multiple**: DuckDB can attach multiple databases

## Comparison to Alternatives

| Feature | DuckDB | PostgreSQL | SQLite |
|---------|--------|------------|--------|
| Setup | Copy file | Server setup | Copy file |
| Portability | ✅ Excellent | ❌ Server-bound | ✅ Excellent |
| Analytics | ✅ Optimized | ✓ Good | ❌ Slow |
| Scale | ✅ Billions | ✅ Billions | ⚠️ Millions |
| NFS Safety | ✅ Read-only | ✅ Server-based | ❌ Corruption risk |
| Export | ✅ Parquet/CSV | ✓ COPY command | ✓ .dump |
| Concurrent writes | ⚠️ Single writer | ✅ Many writers | ❌ Locks |

## Implementation Plan

### Phase 1: Enhanced Export (Now)
- ✅ Already have basic `export_catalog_duckdb.py`
- Add `pages` table with per-page data
- Add `batches` table tracking
- Test with 1,776 + 371 documents

### Phase 2: Incremental Updates (After 10K docs)
- Track last catalog update timestamp
- Only process new documents
- Append to existing tables

### Phase 3: Partitioning (At 1M+ docs)
- Split by collection or year
- Multiple DuckDB files
- Union queries across partitions

## Example Queries for Your Research

```sql
-- 1. Coverage report by collection
SELECT
    collection,
    COUNT(*) AS documents,
    SUM(total_pages) AS pages,
    MIN(year) AS earliest,
    MAX(year) AS latest
FROM documents
GROUP BY collection
ORDER BY documents DESC;

-- 2. Processing timeline
SELECT
    DATE_TRUNC('day', processed_at) AS date,
    COUNT(*) AS documents_processed,
    SUM(total_pages) AS pages_processed
FROM documents
WHERE processed_at IS NOT NULL
GROUP BY date
ORDER BY date;

-- 3. Identify gaps in coverage
SELECT year, COUNT(*) AS doc_count
FROM documents
WHERE collection = 'caribbean'
GROUP BY year
ORDER BY year;

-- 4. Export specific subset for NLP
COPY (
    SELECT
        identifier,
        title,
        year,
        ocr_json_path,
        total_pages
    FROM documents
    WHERE
        year BETWEEN 1850 AND 1900
        AND language = 'eng'
        AND total_pages < 500  -- Manageable size
) TO 'nlp_corpus_1850_1900.csv' (HEADER, DELIMITER ',');
```

## Resources

- **DuckDB Docs**: https://duckdb.org/docs/
- **Python Integration**: https://duckdb.org/docs/api/python/overview
- **SQL Reference**: https://duckdb.org/docs/sql/introduction
- **Parquet Export**: https://duckdb.org/docs/data/parquet

## Next Steps

1. Enhance `export_catalog_duckdb.py` with new schema
2. Test with batch_0001 results (200 documents)
3. Create example query notebook
4. Document common analysis patterns
