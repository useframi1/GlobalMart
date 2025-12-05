#!/bin/bash

###############################################################################
# GlobalMart Stop All Services Script
# This script stops all Docker services for the GlobalMart project
###############################################################################

set -e  # Exit on error

echo "=================================="
echo "Stopping GlobalMart Services"
echo "=================================="
echo ""

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo "✗ docker-compose.yml not found!"
    echo "  Please run this script from the project root directory."
    exit 1
fi

# Stop Docker services
echo "Stopping Docker services..."
docker-compose down

echo ""
echo "✓ All services stopped"
echo ""
echo "To remove volumes as well, run: docker-compose down -v"
echo ""
