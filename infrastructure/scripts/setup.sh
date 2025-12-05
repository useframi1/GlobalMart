#!/bin/bash

###############################################################################
# GlobalMart Initial Setup Script
# This script sets up the initial environment for the GlobalMart project
###############################################################################

set -e  # Exit on error

echo "=================================="
echo "GlobalMart Initial Setup"
echo "=================================="
echo ""

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "✗ .env file not found!"
    echo "  Creating .env from .env.example..."
    cp .env.example .env
    echo "✓ .env file created. Please edit it with your configurations."
    echo ""
fi

# Create necessary directories
echo "Creating data directories..."
mkdir -p data/raw data/processed data/warehouse data/checkpoints
mkdir -p logs/data_generation logs/stream_processing logs/batch_processing logs/api
echo "✓ Data directories created"
echo ""

# Check Docker installation
echo "Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    echo "✗ Docker is not installed. Please install Docker first."
    exit 1
fi
echo "✓ Docker is installed"
echo ""

# Check Docker Compose installation
echo "Checking Docker Compose installation..."
if ! command -v docker-compose &> /dev/null; then
    echo "✗ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi
echo "✓ Docker Compose is installed"
echo ""

# Check Conda installation
echo "Checking Conda installation..."
if ! command -v conda &> /dev/null; then
    echo "✗ Conda is not installed. Please install Anaconda or Miniconda first."
    exit 1
fi
echo "✓ Conda is installed"
echo ""

# Check for globalmart_env environment
echo "Checking for globalmart_env conda environment..."
if conda env list | grep -q "globalmart_env"; then
    echo "✓ globalmart_env conda environment found"
else
    echo "✗ globalmart_env environment not found!"
    echo "  Please create it with: conda create -n globalmart_env python=3.11"
    echo "  Or use: conda env create -f environment.yml"
    exit 1
fi
echo ""

# Install Python dependencies
echo "Do you want to install Python dependencies in globalmart_env? (y/n)"
read -r response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo "Installing Python dependencies in globalmart_env..."
    echo "Activating globalmart_env..."
    eval "$(conda shell.bash hook)"
    conda activate globalmart_env
    pip install -r requirements.txt
    echo "✓ Python dependencies installed in globalmart_env"
    echo ""
fi

echo "=================================="
echo "Setup Complete!"
echo "=================================="
echo ""
echo "Next steps:"
echo "1. Edit .env file with your configuration (passwords, etc.)"
echo "2. Activate conda environment: conda activate globalmart_env"
echo "3. Start Docker services: ./infrastructure/scripts/start_all.sh"
echo "4. Run data generators: python data_generation/main.py"
echo "5. Run stream processing: python stream_processing/main.py"
echo ""
