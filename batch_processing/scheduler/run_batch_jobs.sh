#!/bin/bash
###############################################################################
# GlobalMart Batch Job Runner
# Executes daily batch processing jobs via cron
#
# Usage: ./run_batch_jobs.sh [options]
# Options:
#   --jobs <jobs>     Specific jobs to run (default: all)
#   --dry-run         Validate without executing
#   --full-reload     Force full reload
###############################################################################

set -e  # Exit on error

# Configuration
PROJECT_DIR="/mnt/c/Users/nadin/OneDrive/Desktop/GlobalMart"
LOG_DIR="/var/log/globalmart"
CONDA_ENV="globalmart_env"
DATE_STAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/batch_${DATE_STAMP}.log"

# Parse arguments
JOBS="all"
DRY_RUN=""
FULL_RELOAD=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --full-reload)
            FULL_RELOAD="--full-reload"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Error handler
error_exit() {
    log "ERROR: $1"
    exit 1
}

# Main execution
log "========================================="
log "GlobalMart Batch Processing Started"
log "========================================="
log "Jobs: $JOBS"
log "Log file: $LOG_FILE"

# Ensure log directory exists
if [ ! -d "$LOG_DIR" ]; then
    sudo mkdir -p "$LOG_DIR"
    sudo chown $USER:$USER "$LOG_DIR"
    log "Created log directory: $LOG_DIR"
fi

# Navigate to project directory
cd "$PROJECT_DIR" || error_exit "Failed to change directory to $PROJECT_DIR"
log "Working directory: $(pwd)"

# Activate conda environment
log "Activating conda environment: $CONDA_ENV"
source ~/miniconda3/etc/profile.d/conda.sh
conda activate "$CONDA_ENV" || error_exit "Failed to activate conda environment: $CONDA_ENV"

# Check Python availability
log "Python version: $(python --version)"
log "PySpark availability: $(python -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null || echo 'NOT FOUND')"

# Check database connectivity (optional health check)
log "Checking database connectivity..."
python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        dbname='globalmart_warehouse',
        user='postgres',
        password='postgres',
        connect_timeout=5
    )
    conn.close()
    print('[INFO] Data warehouse connection: OK')
except Exception as e:
    print(f'[WARNING] Data warehouse connection failed: {e}')

try:
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        dbname='globalmart_realtime',
        user='postgres',
        password='postgres',
        connect_timeout=5
    )
    conn.close()
    print('[INFO] Real-time database connection: OK')
except Exception as e:
    print(f'[WARNING] Real-time database connection failed: {e}')
" | tee -a "$LOG_FILE"

# Run batch processing
log "Starting batch processing..."
log "Command: python batch_processing/main.py --jobs $JOBS $DRY_RUN $FULL_RELOAD"

START_TIME=$(date +%s)

python batch_processing/main.py --jobs $JOBS $DRY_RUN $FULL_RELOAD 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# Log completion
if [ $EXIT_CODE -eq 0 ]; then
    log "========================================="
    log "Batch Processing COMPLETED SUCCESSFULLY"
    log "Duration: ${DURATION}s"
    log "========================================="
else
    log "========================================="
    log "Batch Processing FAILED (Exit code: $EXIT_CODE)"
    log "Duration: ${DURATION}s"
    log "========================================="
fi

# Cleanup old logs (keep last 30 days)
log "Cleaning up old log files (keeping last 30 days)..."
find "$LOG_DIR" -name "batch_*.log" -type f -mtime +30 -delete 2>/dev/null || true

log "Log file saved: $LOG_FILE"

exit $EXIT_CODE
