#!/bin/bash

###############################################################################
# GlobalMart Health Check Script
# This script checks the health of all services
###############################################################################

echo "=================================="
echo "GlobalMart Services Health Check"
echo "=================================="
echo ""

# Function to check if a service is running
check_service() {
    local service_name=$1
    local container_name=$2

    if docker ps | grep -q "$container_name"; then
        echo "✓ $service_name is running"
        return 0
    else
        echo "✗ $service_name is NOT running"
        return 1
    fi
}

# Function to check if a port is accessible
check_port() {
    local service_name=$1
    local host=$2
    local port=$3

    if nc -z "$host" "$port" 2>/dev/null; then
        echo "✓ $service_name is accessible on $host:$port"
        return 0
    else
        echo "✗ $service_name is NOT accessible on $host:$port"
        return 1
    fi
}

# Check Docker services
echo "Checking Docker containers..."
check_service "Zookeeper" "globalmart-zookeeper"
check_service "Kafka" "globalmart-kafka"
check_service "PostgreSQL" "globalmart-postgres"
check_service "MongoDB" "globalmart-mongodb"
check_service "Redis" "globalmart-redis"
check_service "Grafana" "globalmart-grafana"
check_service "Prometheus" "globalmart-prometheus"

echo ""
echo "Checking service ports..."
check_port "Kafka" "localhost" "9092"
check_port "PostgreSQL" "localhost" "5432"
check_port "MongoDB" "localhost" "27017"
check_port "Redis" "localhost" "6379"
check_port "Grafana" "localhost" "3000"
check_port "Prometheus" "localhost" "9090"

echo ""
echo "=================================="
echo "Health Check Complete"
echo "=================================="
echo ""
