# Archive OLMoCR Pipeline Documentation

## 🚀 Start Here

### New to OLMoCR Batch Processing?
**READ THIS FIRST:** [OLMOCR_BEST_PRACTICES.md](OLMOCR_BEST_PRACTICES.md)

This guide covers:
- Common pitfalls that cause jobs to silently fail
- Step-by-step workflow for processing PDFs
- Pre-flight checklist to avoid wasted GPU time
- Debugging failed jobs
- Understanding olmocr argument formats

**Created:** November 7, 2025 after debugging session that identified critical issues with argument passing.

---

## Documentation Index

### Essential Guides

| Document | Purpose | When to Read |
|----------|---------|-------------|
| [OLMOCR_BEST_PRACTICES.md](OLMOCR_BEST_PRACTICES.md) | **Production usage guide** | Before submitting any OLMoCR job |
| [PRODUCTION_RUN.md](PRODUCTION_RUN.md) | File-based pipeline architecture | Understanding the full pipeline |

### Reference Docs

| Document | Purpose |
|----------|---------|
| [COMPONENTS.md](COMPONENTS.md) | Pipeline component overview |
| [INTERFACES.md](INTERFACES.md) | API and data structure reference |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development guidelines |

### Historical / Debugging Logs

| Document | Purpose |
|----------|---------|
| [SARAH496_NIBI_ISSUES.md](SARAH496_NIBI_ISSUES.md) | Troubleshooting log from Nov 7, 2025 debugging session |

---

## Quick Links

### Running OLMoCR Jobs

```bash
# Basic workflow
cd ~/projects/def-jic823/archive-olm-pipeline
find /path/to/pdfs -name "*.pdf" -printf "%f\n" > batch_0001/chunks/chunk_0.txt
sbatch --array=0 \
  --export=ALL,PDF_DIR=/path/to/pdfs,BATCH_DIR=/path/to/batch_0001 \
  olmocr/smart_process_pdf_chunks.slurm
```

See [OLMOCR_BEST_PRACTICES.md](OLMOCR_BEST_PRACTICES.md) for complete workflow.

### Monitoring Jobs

```bash
# Check status
squeue -u jic823

# Watch output
tail -f /path/to/output-JOBID.out

# Verify PDF count detected
grep "Found.*total pdf" /path/to/output-JOBID.out
```

### Common Issues

**Job completed in 2 minutes but should have taken 30 minutes?**
→ See [OLMOCR_BEST_PRACTICES.md § Problem: Job Completes in Minutes](OLMOCR_BEST_PRACTICES.md#problem-job-completes-in-minutes-but-only-processes-1-pdf)

**ValueError about pdf paths?**
→ Chunk file has malformed entries. See [Best Practice #1](OLMOCR_BEST_PRACTICES.md#best-practice-1-verify-chunk-file-format)

**olmocr says "Found 1 total pdf" but you expected 32?**
→ Chunk file or argument passing issue. See [Best Practice #4: Monitor Job Progress](OLMOCR_BEST_PRACTICES.md#best-practice-4-monitor-job-progress)

---

## Document History

- **2025-11-07**: Created OLMOCR_BEST_PRACTICES.md after Sarah496 debugging session
- **2025-10-12**: Initial file-based pipeline documentation
- **2025-10-06**: Production run documentation

---

## Contributing

When adding new documentation:
1. Update this README with links
2. Use clear section headings for easy navigation
3. Include code examples for common tasks
4. Add to table of contents above
