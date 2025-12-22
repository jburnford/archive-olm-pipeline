#!/bin/bash
#SBATCH --account=def-jic823
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --job-name=britannica_convert
#SBATCH --output=britannica_convert_%j.out
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=jic823@mail.usask.ca

# Convert NLS Britannica JPGs to PDFs for OLMoCR processing
# Processes editions: EB.4, EB.7, EB.11, EB.12, EB.15, EB.16
# (Excludes EB.1/1771, EB.5/1797, EB.9/1810, EB.10/1815 - already done)

echo "=== Britannica NLS JPG to PDF Conversion ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Start time: $(date)"
echo ""

# Paths
NLS_DIR="/home/jic823/projects/def-jic823/nls-data-encyclopaediaBritannica"
OUTPUT_DIR="/home/jic823/projects/def-jic823/britannica_pipeline/01_downloaded"
SCRIPT_DIR="/home/jic823/projects/def-jic823/archive-olm-pipeline/tools"

# Create pipeline directory structure
mkdir -p "$OUTPUT_DIR"
mkdir -p "/home/jic823/projects/def-jic823/britannica_pipeline/02_processed"
mkdir -p "/home/jic823/projects/def-jic823/britannica_pipeline/99_errors"
mkdir -p "/home/jic823/projects/def-jic823/britannica_pipeline/_manifests"

# Check if img2pdf is available, install if not
if ! python3 -c "import img2pdf" 2>/dev/null; then
    echo "Installing img2pdf..."
    pip install --user img2pdf
fi

# Verify img2pdf is now available
if python3 -c "import img2pdf" 2>/dev/null; then
    echo "img2pdf available - using fast conversion"
else
    echo "WARNING: img2pdf not available, will use ImageMagick (slower)"
fi

echo ""
echo "NLS Directory: $NLS_DIR"
echo "Output Directory: $OUTPUT_DIR"
echo ""

# Run conversion
python3 "$SCRIPT_DIR/britannica_jpg_to_pdf.py" \
    --nls-dir "$NLS_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --editions EB.4 EB.7 EB.11 EB.12 EB.15 EB.16

RESULT=$?

echo ""
echo "=== Conversion Complete ==="
echo "End time: $(date)"
echo "Exit code: $RESULT"

# Show what was created
echo ""
echo "PDFs created:"
ls -lh "$OUTPUT_DIR"/*.pdf 2>/dev/null | wc -l
echo ""
echo "Total size:"
du -sh "$OUTPUT_DIR"

exit $RESULT
