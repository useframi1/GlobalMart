#!/bin/bash
# Daily Batch Processing Job Runner
# This script runs the complete batch processing pipeline

# Change to project directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Configuration
LOG_DIR="$PROJECT_ROOT/logs/batch"
DATE=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/batch_${DATE}.log"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "========================================"
log "GlobalMart Daily Batch Processing"
log "========================================"
log "Started at: $(date)"
log ""

# Activate conda environment
log "Activating conda environment..."
if command -v conda &> /dev/null; then
    source "$(conda info --base)/etc/profile.d/conda.sh"
    conda activate globalmart_env
    log "✓ Conda environment activated"
else
    log "✗ Conda not found, using system Python"
fi

log ""

# Run batch processing
log "Running batch processing..."
log ""

python batch_processing/main.py --mode all 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

log ""
log "========================================"
if [ $EXIT_CODE -eq 0 ]; then
    log "✓ Batch processing completed successfully"
else
    log "✗ Batch processing failed with exit code: $EXIT_CODE"
fi
log "Finished at: $(date)"
log "Log file: $LOG_FILE"
log "========================================"

exit $EXIT_CODE
