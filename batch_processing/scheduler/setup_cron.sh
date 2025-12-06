#!/bin/bash
###############################################################################
# GlobalMart Cron Setup Script
# Installs cron job for daily batch processing
#
# Usage: ./setup_cron.sh [--uninstall]
###############################################################################

set -e

# Configuration
PROJECT_DIR="/mnt/c/Users/nadin/OneDrive/Desktop/GlobalMart"
SCRIPT_PATH="${PROJECT_DIR}/batch_processing/scheduler/run_batch_jobs.sh"
CRON_SCHEDULE="0 2 * * *"  # Daily at 2 AM UTC
LOG_DIR="/var/log/globalmart"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Logging functions
info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running in WSL
check_wsl() {
    if ! grep -qi microsoft /proc/version; then
        error "This script must be run in WSL (Windows Subsystem for Linux)"
        exit 1
    fi
    info "Running in WSL environment"
}

# Check if script is executable
make_executable() {
    if [ ! -x "$SCRIPT_PATH" ]; then
        info "Making run_batch_jobs.sh executable..."
        chmod +x "$SCRIPT_PATH"
    fi
    info "Script is executable: $SCRIPT_PATH"
}

# Create log directory
setup_log_directory() {
    if [ ! -d "$LOG_DIR" ]; then
        info "Creating log directory: $LOG_DIR"
        sudo mkdir -p "$LOG_DIR"
        sudo chown $USER:$USER "$LOG_DIR"
    else
        info "Log directory exists: $LOG_DIR"
    fi
}

# Check if cron is installed and running
check_cron() {
    if ! command -v cron &> /dev/null; then
        error "Cron is not installed. Installing cron..."
        sudo apt-get update
        sudo apt-get install -y cron
    fi

    # Start cron service if not running
    if ! service cron status &> /dev/null; then
        info "Starting cron service..."
        sudo service cron start
    fi

    info "Cron service is running"
}

# Install cron job
install_cron() {
    info "Installing cron job..."

    # Check if cron job already exists
    if crontab -l 2>/dev/null | grep -q "run_batch_jobs.sh"; then
        warn "Cron job already exists. Removing old entry..."
        crontab -l 2>/dev/null | grep -v "run_batch_jobs.sh" | crontab -
    fi

    # Add new cron job
    (crontab -l 2>/dev/null; echo "# GlobalMart Batch Processing") | crontab -
    (crontab -l 2>/dev/null; echo "$CRON_SCHEDULE $SCRIPT_PATH >> $LOG_DIR/cron.log 2>&1") | crontab -

    info "Cron job installed successfully"
}

# Uninstall cron job
uninstall_cron() {
    info "Uninstalling cron job..."

    if crontab -l 2>/dev/null | grep -q "run_batch_jobs.sh"; then
        crontab -l 2>/dev/null | grep -v "run_batch_jobs.sh" | grep -v "# GlobalMart Batch Processing" | crontab -
        info "Cron job removed successfully"
    else
        warn "No cron job found to remove"
    fi
}

# Display current cron jobs
show_cron() {
    echo ""
    info "Current cron jobs for user $USER:"
    echo "-----------------------------------"
    crontab -l 2>/dev/null || echo "(no cron jobs)"
    echo "-----------------------------------"
    echo ""
}

# Main execution
main() {
    echo ""
    echo "=========================================="
    echo "  GlobalMart Cron Setup"
    echo "=========================================="
    echo ""

    # Check for uninstall flag
    if [ "$1" == "--uninstall" ]; then
        check_wsl
        uninstall_cron
        show_cron
        info "Uninstall complete"
        exit 0
    fi

    # Normal installation
    check_wsl
    check_cron
    make_executable
    setup_log_directory
    install_cron
    show_cron

    echo ""
    echo "=========================================="
    echo "  Installation Summary"
    echo "=========================================="
    info "Schedule: $CRON_SCHEDULE (Daily at 2:00 AM UTC)"
    info "Script: $SCRIPT_PATH"
    info "Logs: $LOG_DIR/"
    echo ""
    info "To verify cron is working:"
    echo "  1. Check cron service: service cron status"
    echo "  2. View cron logs: cat $LOG_DIR/cron.log"
    echo "  3. Test manually: $SCRIPT_PATH --dry-run"
    echo ""
    info "To uninstall: $0 --uninstall"
    echo ""
    echo -e "${GREEN}Setup complete!${NC}"
    echo ""
}

main "$@"
