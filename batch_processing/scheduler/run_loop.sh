#!/bin/bash
# Manual Batch Processing Loop
# Runs batch processing every 5 minutes
# Press Ctrl+C to stop

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BATCH_SCRIPT="$PROJECT_ROOT/batch_processing/main.py"
LOG_FILE="/tmp/globalmart_batch_loop.log"

echo "=========================================="
echo "GlobalMart Batch Processing Loop"
echo "=========================================="
echo "Project root: $PROJECT_ROOT"
echo "Batch script: $BATCH_SCRIPT"
echo "Log file: $LOG_FILE"
echo ""
echo "Running batch processing every 5 minutes"
echo "Press Ctrl+C to stop"
echo "=========================================="
echo ""

# Create/clear log file
> "$LOG_FILE"

# Counter for runs
RUN_COUNT=0

while true; do
    RUN_COUNT=$((RUN_COUNT + 1))
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    echo ""
    echo "=========================================="
    echo "Run #$RUN_COUNT - $TIMESTAMP"
    echo "=========================================="
    echo ""

    # Log to file
    echo "=========================================" >> "$LOG_FILE"
    echo "Run #$RUN_COUNT - $TIMESTAMP" >> "$LOG_FILE"
    echo "=========================================" >> "$LOG_FILE"

    # Run batch processing
    cd "$PROJECT_ROOT"
    python "$BATCH_SCRIPT" 2>&1 | tee -a "$LOG_FILE"

    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo ""
        echo "✓ Batch processing completed successfully"
        echo "✓ Batch processing completed successfully" >> "$LOG_FILE"
    else
        echo ""
        echo "✗ Batch processing failed with exit code $EXIT_CODE"
        echo "✗ Batch processing failed with exit code $EXIT_CODE" >> "$LOG_FILE"
    fi

    NEXT_RUN=$(date -v+5M '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -d '+5 minutes' '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "in 5 minutes")
    echo ""
    echo "Next run at: $NEXT_RUN"
    echo "Waiting 5 minutes..."
    echo ""

    # Wait 5 minutes (300 seconds)
    sleep 60
done
