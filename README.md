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
Data Generators → Kafka → Spark Streaming → PostgreSQL Real-Time DB
(3 types)        (3 topics)  (5 jobs)        (18 tables)
                                           ↓
                              Daily Spark Batch Jobs
                                           ↓
                              PostgreSQL Warehouse
                                    (Star Schema)
                                           ↓
                  FastAPI ← Redis Cache → Grafana + Power BI

All timestamps in UTC throughout the pipeline
```

## Technology Stack

### Core Technologies
- **Stream Processing**: Apache Spark Structured Streaming (UTC timezone)
- **Message Queue**: Apache Kafka (3 topics)
- **Batch Processing**: Apache Spark (planned)
- **Databases**:
  - PostgreSQL Real-Time (18 tables for streaming analytics)
  - PostgreSQL Warehouse (star schema for batch analytics)
  - MongoDB (metrics storage, planned)
  - Redis (API caching, planned)
- **API**: FastAPI (planned)
- **Visualization**:
  - Grafana (real-time monitoring)
  - Power BI (executive dashboard, planned)
- **Monitoring**: Prometheus
- **Deployment**: Docker Compose

### Key Features
- **UTC Timezone Consistency**: All timestamps (event time and processing time) use UTC
- **Natural Primary Keys**: No auto-generated IDs, uses composite keys for better semantics
- **Window-based Aggregations**: 1, 5, and 10-minute time windows
- **Late Data Handling**: Watermarking support (1-10 minutes depending on job)
- **Real-time Anomaly Detection**: High-value transactions, suspicious patterns
- **Inventory Tracking**: Sales velocity, restock alerts, demand forecasting
- **Cart Analytics**: Session tracking, abandonment detection

## Project Structure

```
GlobalMart/
├── config/                  # Centralized configuration
│   ├── settings.py          # Environment-based settings
│   └── constants.py         # Business constants
├── data/                    # Generated data storage
├── data_generation/         # Data generators and Kafka producers
│   ├── generators/          # User, product, transaction, cart, view generators (UTC)
│   ├── kafka_producers/     # Kafka producers for each event type
│   ├── validation/          # Schema validation
│   └── main.py              # Multi-event orchestrator
├── stream_processing/       # Real-time stream processing jobs
│   ├── common/              # Spark session factory (UTC configured)
│   ├── jobs/                # 5 stream processing jobs
│   ├── schema/              # PostgreSQL real-time schema (18 tables)
│   ├── alerts/              # Alert notification system
│   └── main.py              # Stream job orchestrator
├── batch_processing/        # Batch analytics and ETL
│   └── schema/              # Star schema for warehouse
├── api/                     # REST API (planned)
├── dashboards/              # Grafana and Power BI dashboards
│   └── grafana/             # Grafana provisioning
├── infrastructure/          # Docker, scripts, monitoring
│   ├── scripts/             # Setup, start, stop, health check
│   └── monitoring/          # Prometheus configuration
├── tests/                   # Unit and integration tests (planned)
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

   # Activate the environment
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
This will generate realistic e-commerce events and stream them to Kafka:
- 60% product view events (view, search, filter, compare)
- 30% cart events (add, remove, update, checkout, abandonment)
- 10% transaction events (purchases with 2% anomaly rate)
- Default: 50 events/second (configurable in .env)
- **All timestamps in UTC**

**Terminal 2: Start Stream Processing**
```bash
conda activate globalmart_env
python stream_processing/main.py
```
This will run all 5 stream processing jobs:
1. **Sales Aggregator** - Sales metrics per minute and by country
2. **Cart Analyzer** - Cart sessions, abandonment, metrics
3. **Product View Analyzer** - Trending products, category performance, search behavior
4. **Anomaly Detector** - High-value transactions, suspicious patterns
5. **Inventory Tracker** - Sales velocity, restock alerts, demand forecasting

All jobs write to PostgreSQL real-time database with **UTC-aligned timestamps**.

**Terminal 3 (Optional): Monitor Kafka**
```bash
# Monitor transactions
docker exec -it globalmart-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions-topic

# Monitor cart events
docker exec -it globalmart-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cart-events-topic

# Monitor product views
docker exec -it globalmart-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic product-views-topic
```

**Terminal 4 (Optional): Query PostgreSQL**
```bash
docker exec -it globalmart-postgres-realtime psql -U globalmart_user -d globalmart_realtime

# Check sales data
SELECT * FROM sales_per_minute ORDER BY window_start DESC LIMIT 10;

# Check anomalies
SELECT * FROM high_value_anomalies ORDER BY detected_at DESC LIMIT 5;

# Check inventory alerts
SELECT * FROM high_demand_alerts ORDER BY alert_generated_at DESC LIMIT 10;

# Verify timezone consistency
SELECT window_start, window_end, created_at FROM sales_per_minute LIMIT 5;
```

### Accessing Services

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Mongo Express**: http://localhost:8081 (admin/[mongo_password])
- **Kafka**: localhost:9092
- **PostgreSQL Real-Time**: localhost:5433
- **PostgreSQL Warehouse**: localhost:5432
- **MongoDB**: localhost:27017
- **Redis**: localhost:6379

## System Components

### Component 1: Data Generation
Generates realistic e-commerce events with configurable throughput:
- **User Profiles**: 10K users (MVP), scalable to 10M with demographics and behavior patterns
- **Product Catalog**: 1K products (MVP), scalable to 250K across 100 categories
- **Event Types**:
  - Transaction events (10%): Purchases with realistic pricing and 2% anomaly rate
  - Cart events (30%): Add, remove, update, checkout, abandonment
  - Product view events (60%): View, search, filter, compare
- **Kafka Integration**: 3 producers with error handling and delivery confirmation
- **Data Quality**: Schema validation, realistic distributions, UTC timestamps
- **Orchestration**: Multi-event coordinator with 60/30/10 distribution ratio

### Component 2: Stream Processing
Real-time analytics powered by Apache Spark Structured Streaming:
- **5 Stream Processing Jobs**:
  1. **Sales Aggregator**: Revenue and transaction metrics (1-minute windows)
  2. **Cart Analyzer**: Session tracking, abandonment detection (5-minute windows)
  3. **Product View Analyzer**: Trending products, search behavior (5-10 minute windows)
  4. **Anomaly Detector**: High-value transactions, suspicious patterns (real-time + 1-minute aggregations)
  5. **Inventory Tracker**: Sales velocity, restock alerts (5-10 minute windows)
- **Data Sink**: PostgreSQL real-time database with 18 tables
- **Key Features**:
  - UTC timezone consistency throughout pipeline
  - Watermarking for late data handling (1-10 minutes)
  - Natural composite primary keys (no auto-generated IDs)
  - Append-only architecture for auditability
  - Alert notification system with severity levels

### Component 3: Batch Processing
Daily analytics and data warehouse ETL:
- **Star Schema Warehouse**: Optimized for business intelligence queries
- **Batch Jobs**:
  - RFM (Recency, Frequency, Monetary) customer segmentation
  - Product performance analysis
  - Sales trend analysis
  - Historical aggregations
- **ETL Pipeline**: Nightly processing from real-time to warehouse at 2 AM UTC

### Component 4: Visualization & API
Business intelligence and data access layers:
- **Grafana Dashboards**:
  - System Health: Infrastructure monitoring (Docker, Kafka, databases)
  - Data Ingestion: Kafka metrics and topic monitoring
  - Stream Processing: Spark job performance and throughput
  - Real-Time Business Metrics: Revenue, conversion, anomalies
- **FastAPI**: RESTful API with Redis caching
- **Power BI**: Executive dashboard for management insights

### Component 5: Infrastructure
Containerized deployment and monitoring:
- **Docker Compose**: Multi-service orchestration
- **Kafka Cluster**: 3-topic message broker (transactions, cart-events, product-views)
- **Databases**:
  - PostgreSQL Real-Time (port 5433): 18 streaming tables
  - PostgreSQL Warehouse (port 5432): Star schema
  - MongoDB (port 27017): Metrics storage
  - Redis (port 6379): API caching
- **Monitoring**: Prometheus metrics collection
- **Scripts**: Automated setup, health checks, start/stop

## Real-Time Database Tables

The PostgreSQL real-time database contains **18 tables** organized into **5 functional groups**:

### 1. Sales Aggregator (2 tables)
- `sales_per_minute` - Overall sales metrics (1-minute windows)
- `sales_by_country` - Geographic sales breakdown (1-minute windows)

### 2. Cart Analyzer (3 tables)
- `cart_sessions` - Active shopping sessions (5-minute windows)
- `abandoned_carts` - Abandoned cart tracking
- `cart_metrics` - Cart behavior by country (5-minute windows)

### 3. Product View Analyzer (4 tables)
- `trending_products` - High-engagement products (10-minute windows)
- `category_performance` - Category metrics by country (5-minute windows)
- `search_behavior` - Search query analysis (5-minute windows)
- `browsing_sessions` - User browsing patterns (5-minute windows)

### 4. Anomaly Detector (4 tables)
- `high_value_anomalies` - Transactions > $5,000
- `high_quantity_anomalies` - Single products ≥ 50 units
- `suspicious_patterns` - Users with 3+ transactions or > $10K in 5 minutes
- `anomaly_stats` - Aggregate anomaly counts (1-minute windows)

### 5. Inventory Tracker (5 tables)
- `product_sales_velocity` - Global sales rate (5-minute windows)
- `inventory_by_country` - Regional sales tracking (5-minute windows)
- `high_demand_alerts` - Products selling ≥ 20 units in 10 minutes
- `restock_alerts` - Products selling ≥ 1 unit/min by country (5-minute windows)
- `inventory_metrics` - Overall consumption by country (1-minute windows)

**Key Schema Features:**
- Composite natural primary keys (e.g., `PRIMARY KEY(window_start, window_end, product_id)`)
- Window time columns (`window_start`, `window_end`) - event time in UTC
- Processing time columns (`created_at`, `detected_at`) - when Spark processed the data
- All timestamps in UTC for consistency
- Append-only architecture (no updates or deletes)
- Indexes on time columns for efficient queries

## Configuration

All configuration is managed through the `.env` file. Key configurations:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Data Generation
DATA_GEN_EVENTS_PER_SECOND=50  # Adjust throughput (max 500)
DATA_GEN_NUM_USERS=10000        # Number of users (scalable to 10M)
DATA_GEN_NUM_PRODUCTS=1000      # Number of products (scalable to 250K)

# Spark
SPARK_MASTER_URL=local[*]
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g

# PostgreSQL Real-Time
POSTGRES_REALTIME_HOST=localhost
POSTGRES_REALTIME_PORT=5433
POSTGRES_REALTIME_DB=globalmart_realtime

# PostgreSQL Warehouse
POSTGRES_WAREHOUSE_HOST=localhost
POSTGRES_WAREHOUSE_PORT=5432
POSTGRES_WAREHOUSE_DB=globalmart_warehouse
```

See `.env.example` for all available options.

## Performance

System Performance Characteristics:
- **Data Generation**: Configurable 50-500 events/second throughput
- **Event Distribution**: 60% product views, 30% cart events, 10% transactions
- **Stream Processing**: 5 concurrent jobs with 1, 5, and 10-minute time windows
- **Watermark Delays**: 1-10 minutes for late data handling (varies by job)
- **End-to-end Latency**: ~30 seconds from event generation to PostgreSQL storage
- **Memory Footprint**: ~2GB (Spark driver + executor combined)
- **Producer Reliability**: >99% message delivery success rate
- **Timezone**: UTC consistency across entire data pipeline

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
- Ensure conda environment is activated

### Stream processing shows no output
- Ensure data generation is running and sending data
- Check that Kafka topic has messages
- Verify Spark can connect to Kafka and PostgreSQL
- Check PostgreSQL real-time database is running

### Timezone issues
- All timestamps should be in UTC
- Check `spark.sql.session.timeZone` is set to "UTC"
- Verify generators use `datetime.utcnow()` not `datetime.now()`
- Compare `window_start/end` with `created_at` - should be close in time

### Cannot connect to databases
- Verify Docker services are running
- Check database credentials in `.env`
- Ensure services are on the same Docker network
- Check port mappings (5432 vs 5433 for PostgreSQL instances)

For detailed project status and progress tracking, see [PROJECT_STATUS.md](PROJECT_STATUS.md).

## Development Workflow

### Adding New Features
1. Create feature branch
2. Implement feature with tests
3. Update documentation
4. Submit pull request

### Testing
```bash
# Run all tests (when implemented)
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

## Team

- Student 1: Data Generation & Monitoring
- Student 2: Stream & Batch Processing
- Student 3: Infrastructure & Visualization

## Documentation

- [Project Status](PROJECT_STATUS.md) - Detailed progress and task tracking

## License

Educational project for CSCE4501 Big Data course.

## Acknowledgments

- Apache Kafka, Spark, and open-source community
- Course instructors and teaching assistants
- "Designing Data-Intensive Applications" by Martin Kleppmann
- "Spark: The Definitive Guide" by Chambers and Zaharia
