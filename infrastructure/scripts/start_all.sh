#!/bin/bash

###############################################################################
# GlobalMart Start All Services Script
# This script starts all Docker services for the GlobalMart project
###############################################################################

set -e  # Exit on error

echo "=================================="
echo "Starting GlobalMart Services"
echo "=================================="
echo ""

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo "✗ docker-compose.yml not found!"
    echo "  Please run this script from the project root directory."
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "✗ .env file not found!"
    echo "  Please create .env file from .env.example and configure it."
    exit 1
fi

# Start Docker services
echo "Starting Docker services..."
docker-compose up -d

echo ""
echo "Waiting for services to be ready..."
sleep 10

echo ""
echo "=================================="
echo "Services Status"
echo "=================================="
docker-compose ps

echo ""
echo "=================================="
echo "Service URLs"
echo "=================================="
echo "Grafana:     http://localhost:3000 (admin/admin)"
echo "Prometheus:  http://localhost:9090"
echo "Kafka:       localhost:9092"
echo "MongoDB:     localhost:27017"
echo "PostgreSQL:  localhost:5432"
echo "Redis:       localhost:6379"
echo ""
echo "To check service health: ./infrastructure/scripts/health_check.sh"
echo "To stop services: ./infrastructure/scripts/stop_all.sh"
echo "To view logs: docker-compose logs -f [service-name]"
echo ""
