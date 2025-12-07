#!/bin/bash
# Setup Cron Job for Batch Processing
# This script configures a cron job to run batch processing
# TESTING MODE: Runs every 5 minutes

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BATCH_SCRIPT="$SCRIPT_DIR/run_daily_batch.sh"

echo "=========================================="
echo "GlobalMart Batch Processing Cron Setup"
echo "=========================================="
echo ""

# Check if batch script exists
if [ ! -f "$BATCH_SCRIPT" ]; then
    echo "✗ Error: Batch script not found at $BATCH_SCRIPT"
    exit 1
fi

# Make sure batch script is executable
chmod +x "$BATCH_SCRIPT"

echo "Batch script: $BATCH_SCRIPT"
echo ""

# Create cron job entry (runs every 5 minutes for testing)
# Production: 0 2 * * * (daily at 2 AM UTC)
CRON_ENTRY="*/5 * * * * $BATCH_SCRIPT >> /tmp/globalmart_batch_cron.log 2>&1"

echo "Proposed cron job:"
echo "  $CRON_ENTRY"
echo ""
echo "⚠️  TESTING MODE: This will run batch processing every 5 minutes"
echo "For production, change to: 0 2 * * * (daily at 2 AM UTC)"
echo ""

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "$BATCH_SCRIPT"; then
    echo "⚠ Cron job for batch processing already exists"
    echo ""
    echo "Current cron jobs:"
    crontab -l | grep "$BATCH_SCRIPT"
    echo ""
    read -p "Do you want to replace it? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup cancelled"
        exit 0
    fi

    # Remove existing cron job
    crontab -l | grep -v "$BATCH_SCRIPT" | crontab -
    echo "✓ Removed existing cron job"
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Cron job successfully installed"
    echo ""
    echo "Current cron jobs:"
    crontab -l | grep "$BATCH_SCRIPT"
    echo ""
    echo "To view all cron jobs: crontab -l"
    echo "To remove this cron job: crontab -e (then delete the line)"
    echo ""
    echo "Note: Make sure your system timezone is set correctly!"
else
    echo "✗ Failed to install cron job"
    exit 1
fi

echo "=========================================="
echo "Setup Complete"
echo "=========================================="
