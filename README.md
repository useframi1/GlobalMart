# GlobalMart: Real-Time E-Commerce Analytics Platform

A comprehensive real-time analytics platform for e-commerce that processes streaming transaction data, performs batch analytics, and provides insights through dashboards and APIs.

## Project Overview

GlobalMart is designed for an e-commerce platform with:
- 10 million active users
- 250,000 products across 100 categories
- Operations in 5 countries
- 100,000 daily transactions
- Real-time monitoring and analytics
- Business intelligence dashboards for management

## Architecture

```
Data Generators → Kafka → Spark Streaming → MongoDB (Real-time)
                                           ↓
                              Daily Spark Batch Jobs
                                           ↓
                              PostgreSQL Warehouse
                                           ↓
                  FastAPI ← Redis Cache → Grafana + Power BI
```

## Technology Stack

### Core Technologies
- **Stream Processing**: Apache Spark Structured Streaming
- **Message Queue**: Apache Kafka
- **Batch Processing**: Apache Spark
- **Databases**:
  - PostgreSQL (data warehouse with star schema)
  - MongoDB (real-time metrics storage)
  - Redis (API caching)
- **API**: FastAPI
- **Visualization**:
  - Grafana (real-time monitoring)
  - Power BI (executive dashboard)
- **Monitoring**: Prometheus
- **Deployment**: Docker Compose

## Project Structure

```
GlobalMart/
├── config/                  # Centralized configuration
├── data/                    # Data storage
├── data_generation/         # Data generators and Kafka producers
├── stream_processing/       # Real-time stream processing jobs
├── batch_processing/        # Batch analytics and ETL
├── api/                     # REST API
├── dashboards/              # Grafana and Power BI dashboards
├── infrastructure/          # Docker, scripts, monitoring
├── tests/                   # Unit and integration tests
└── docs/                    # Documentation
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Anaconda or Miniconda
- Python 3.8+ (via conda environment: globalmart_env)
- 8GB RAM minimum
- 20GB disk space

### Installation

1. **Clone the repository** (assuming current directory is already the repo)
   ```bash
   cd GlobalMart
   ```

2. **Create and activate conda environment**
   ```bash
   # If you haven't created globalmart_env yet:
   # Option 1: Create from environment.yml file (recommended)
   conda env create -f environment.yml

   # Option 2: Create manually
   # conda create -n globalmart_env python=3.11

   # Activate the environment (you already have this)
   conda activate globalmart_env
   ```

3. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   nano .env
   ```

4. **Run setup script**
   ```bash
   chmod +x infrastructure/scripts/setup.sh
   ./infrastructure/scripts/setup.sh
   ```
   This will check for the conda environment and install Python dependencies.

5. **Start Docker services**
   ```bash
   ./infrastructure/scripts/start_all.sh
   ```

6. **Wait for services to be ready** (30-60 seconds)
   ```bash
   ./infrastructure/scripts/health_check.sh
   ```

### Running the System

**Important**: Make sure to activate the conda environment in each terminal:
```bash
conda activate globalmart_env
```

**Terminal 1: Start Data Generation**
```bash
conda activate globalmart_env
python data_generation/main.py
```
This will generate realistic e-commerce transactions and stream them to Kafka at 50 events/second (configurable in .env).

**Terminal 2: Start Stream Processing**
```bash
conda activate globalmart_env
python stream_processing/main.py
```
This will process transactions in real-time and aggregate metrics.

**Terminal 3 (Optional): Monitor Kafka**
```bash
docker exec -it globalmart-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions-topic \
  --from-beginning
```

### Accessing Services

- **Grafana**: http://localhost:3000 (admin/admin)
- **Mongo Express**: http://localhost:8081 (admin/[mongo_password])
- **Prometheus**: http://localhost:9090
- **Kafka**: localhost:9092
- **PostgreSQL**: localhost:5432
- **MongoDB**: localhost:27017
- **Redis**: localhost:6379

## Features

### Component 1: Data Generation (20 points)
- ✅ Realistic user profiles (10K users for MVP, scalable to 10M)
- ✅ Product catalog (1K products for MVP, scalable to 250K)
- ✅ Transaction generation with realistic patterns
- ✅ Kafka producers with error handling and metrics
- ✅ Configurable event generation rate (50 events/sec default)
- ✅ Data validation and quality checks

### Component 2: Stream Processing (30 points)
- ✅ Real-time transaction monitoring
- ✅ Sales aggregation by time window (per minute)
- ✅ Sales aggregation by country
- 🔄 Anomaly detection (planned)
- 🔄 Inventory tracking (planned)
- 🔄 Session analysis (planned)
- 🔄 Alert system (planned)

### Component 3: Batch Processing (25 points)
- ✅ Star schema data warehouse design
- 🔄 Daily batch jobs: RFM analysis (planned)
- 🔄 Product performance analysis (planned)
- 🔄 Sales trend analysis (planned)
- 🔄 ETL pipeline (planned)

### Component 4: Visualization & API (15 points)
- ✅ Grafana setup with data source provisioning
- 🔄 Real-time dashboards (planned)
- 🔄 RESTful API (planned)
- 🔄 Power BI executive dashboard (planned)

### Component 5: Infrastructure (10 points)
- ✅ Docker Compose orchestration
- ✅ All services configured and health-checked
- ✅ Setup and deployment scripts
- ✅ Prometheus monitoring
- ✅ Centralized configuration management

## Configuration

All configuration is managed through the `.env` file. Key configurations:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Data Generation
DATA_GEN_EVENTS_PER_SECOND=50  # Adjust throughput
DATA_GEN_NUM_USERS=10000        # Number of users
DATA_GEN_NUM_PRODUCTS=1000      # Number of products

# Spark
SPARK_MASTER_URL=local[*]
SPARK_DRIVER_MEMORY=2g
```

See `.env.example` for all available options.

## Development Workflow

### Adding New Features
1. Create feature branch
2. Implement feature with tests
3. Update documentation
4. Submit pull request

### Testing
```bash
# Run all tests
pytest tests/

# Run specific test suite
pytest tests/unit/test_generators.py

# Check code coverage
pytest --cov=. tests/
```

### Code Quality
```bash
# Format code
black .

# Lint code
flake8 .
```

## Troubleshooting

### Services won't start
```bash
# Check if ports are already in use
./infrastructure/scripts/health_check.sh

# View service logs
docker-compose logs [service-name]

# Restart all services
./infrastructure/scripts/stop_all.sh
./infrastructure/scripts/start_all.sh
```

### Data generation fails
- Ensure Kafka is running and accessible
- Check Kafka connection in logs
- Verify `.env` configuration

### Stream processing shows no output
- Ensure data generation is running and sending data
- Check that Kafka topic has messages
- Verify Spark can connect to Kafka

### Cannot connect to databases
- Verify Docker services are running
- Check database credentials in `.env`
- Ensure services are on the same Docker network

## Project Status

This is the **MVP (Week 1)** implementation with:
- ✅ Complete infrastructure setup
- ✅ Data generation pipeline
- ✅ Basic stream processing
- ✅ Warehouse schema design
- ✅ Grafana foundation

**Next Steps (Week 2+)**:
- Implement all stream processing jobs
- Build batch processing pipeline
- Create comprehensive dashboards
- Develop REST API
- Add testing and documentation

## Performance

Current MVP Performance:
- **Data Generation**: 50 events/second (configurable)
- **Stream Processing**: ~10 second micro-batches
- **End-to-end latency**: < 30 seconds

Target Production Performance:
- **Data Generation**: 500 events/second
- **Stream Processing**: ~10 second micro-batches
- **End-to-end latency**: < 15 seconds

## Team

- Student 1: Data Generation & Monitoring
- Student 2: Stream & Batch Processing
- Student 3: Infrastructure & Visualization

## Documentation

- [Architecture Documentation](docs/architecture/system_architecture.md) (TBD)
- [Setup Guide](docs/setup/installation.md) (TBD)
- [API Documentation](api/docs/api_documentation.md) (TBD)
- [Grafana Setup](dashboards/grafana/README.md)
- [Troubleshooting Guide](docs/setup/troubleshooting.md) (TBD)

## License

Educational project for CSCE4501 Big Data course.

## Acknowledgments

- Apache Kafka, Spark, and open-source community
- Course instructors and teaching assistants
- "Designing Data-Intensive Applications" by Martin Kleppmann
- "Spark: The Definitive Guide" by Chambers and Zaharia

---

**Status**: Week 1 MVP Complete ✅
**Last Updated**: 2025-12-04
**Version**: 0.1.0
