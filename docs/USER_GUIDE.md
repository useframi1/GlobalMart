# GlobalMart User Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [System Components](#system-components)
4. [Running the Platform](#running-the-platform)
5. [Using Grafana Dashboards](#using-grafana-dashboards)
6. [Querying the Data Warehouse](#querying-the-data-warehouse)
7. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
8. [Advanced Usage](#advanced-usage)
9. [FAQ](#faq)

## Introduction

Welcome to GlobalMart, an enterprise-grade real-time analytics platform for e-commerce operations. This guide will help you understand, deploy, and use the platform effectively.

### What is GlobalMart?

GlobalMart provides:
- **Real-time event processing** for immediate insights
- **Batch analytics** for comprehensive business intelligence
- **Interactive dashboards** for data visualization
- **Data warehouse** for historical analysis

### Who Should Use This Guide?

- **Data Engineers**: Setting up and maintaining the platform
- **Data Analysts**: Querying the warehouse and creating reports
- **Business Users**: Using dashboards for insights
- **DevOps**: Deploying and monitoring the system

## Getting Started

### System Requirements

**Minimum:**
- 8GB RAM
- 4 CPU cores
- 20GB disk space
- Ubuntu 20.04+ or macOS 11+

**Recommended:**
- 16GB RAM
- 8 CPU cores
- 50GB SSD
- Ubuntu 22.04 or macOS 13+

### Prerequisites

1. **Docker & Docker Compose**
   ```bash
   # Check installation
   docker --version
   docker-compose --version
   ```

2. **Python 3.11+**
   ```bash
   python --version
   ```

3. **Apache Spark 3.4.0**
   - Spark binaries in PATH
   - SPARK_HOME environment variable set

### Installation Steps

#### Step 1: Clone Repository

```bash
git clone <repository-url>
cd GlobalMart
```

#### Step 2: Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit configuration
nano .env
```

**Key Configuration Parameters:**

```bash
# Data Generation
DATA_GEN_EVENTS_PER_SECOND=50  # Event throughput (10-500)
DATA_GEN_NUM_USERS=10000        # Number of users
DATA_GEN_NUM_PRODUCTS=1000      # Number of products

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# PostgreSQL Realtime
POSTGRES_REALTIME_HOST=localhost
POSTGRES_REALTIME_PORT=5433
POSTGRES_REALTIME_USER=globalmart_user
POSTGRES_REALTIME_PASSWORD=realtime_password

# PostgreSQL Warehouse
POSTGRES_WAREHOUSE_HOST=localhost
POSTGRES_WAREHOUSE_PORT=5432
POSTGRES_WAREHOUSE_USER=globalmart_user
POSTGRES_WAREHOUSE_PASSWORD=warehouse_password

# Spark
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

#### Step 3: Install Python Dependencies

```bash
pip install -r requirements.txt
```

#### Step 4: Start Infrastructure

```bash
# Start all Docker services
docker-compose up -d

# Verify services are running
docker-compose ps
```

Expected output:
```
NAME                        STATUS      PORTS
globalmart-kafka            Up          0.0.0.0:9092->9092/tcp
globalmart-zookeeper        Up          2181/tcp
globalmart-postgres-rt      Up          0.0.0.0:5433->5432/tcp
globalmart-postgres-wh      Up          0.0.0.0:5432->5432/tcp
globalmart-redis            Up          0.0.0.0:6379->6379/tcp
globalmart-grafana          Up          0.0.0.0:3000->3000/tcp
globalmart-prometheus       Up          0.0.0.0:9090->9090/tcp
```

#### Step 5: Initialize Databases

```bash
# Initialize realtime database
psql -h localhost -p 5433 -U globalmart_user -d globalmart_realtime \
     -f stream_processing/schema/init_realtime.sql

# Initialize warehouse database
psql -h localhost -p 5432 -U globalmart_user -d globalmart_warehouse \
     -f batch_processing/schema/init_warehouse.sql
```

When prompted for password, use the values from your `.env` file.

### Verifying Installation

```bash
# Check Kafka topics
docker exec -it globalmart-kafka kafka-topics \
    --bootstrap-server localhost:9092 --list

# Check database connections
psql -h localhost -p 5433 -U globalmart_user -d globalmart_realtime -c "\dt"
psql -h localhost -p 5432 -U globalmart_user -d globalmart_warehouse -c "\dt"

# Check Grafana
curl http://localhost:3000/api/health
```

## System Components

### 1. Data Generation

**Purpose:** Generate realistic e-commerce events

**How to Run:**
```bash
python data_generation/main.py
```

**What It Does:**
- Generates transactions, product views, and cart events
- Sends events to Kafka topics
- Default rate: 50 events/second
- Distribution: 60% views, 30% cart, 10% transactions

**Monitoring Output:**
```
=== GlobalMart Data Generation Started ===
[Transaction] Generated transaction T123 for user U456 - $129.99
[Product View] User U789 viewed product P012
[Cart Event] User U456 added 2x P345 to cart
```

**Adjusting Throughput:**
Edit `.env` file:
```bash
DATA_GEN_EVENTS_PER_SECOND=100  # Increase to 100 events/second
```

### 2. Stream Processing

**Purpose:** Real-time event processing and storage

**How to Run:**
```bash
python stream_processing/main.py
```

**What It Does:**
- Reads events from Kafka topics
- Saves events to PostgreSQL realtime database
- Provides sub-second latency

**Jobs Running:**
- Transaction Saver
- Product View Saver
- Cart Event Saver

**Monitoring Output:**
```
Starting stream processing jobs...
[TransactionSaver] Batch processed: 245 records
[ProductViewSaver] Batch processed: 412 records
[CartEventSaver] Batch processed: 183 records
```

### 3. Batch Processing

**Purpose:** ETL and analytics on warehouse data

**Manual Execution:**
```bash
python batch_processing/main.py
```

**Scheduled Execution:**
```bash
# Set up cron job (runs every 5 minutes for testing)
./batch_processing/scheduler/setup_cron.sh

# View cron status
crontab -l

# Manual loop (alternative to cron)
./batch_processing/scheduler/run_loop.sh
```

**What It Does:**

**ETL Phase:**
1. Extract events from realtime database
2. Transform and enrich with dimensions
3. Load into warehouse fact tables
4. Apply SCD Type 2 for dimension changes

**Analytics Phase:**
1. Calculate product performance metrics
2. Segment customers
3. Perform RFM analysis
4. Analyze sales trends

**Monitoring Output:**
```
=== Batch Processing Started ===
[ETL] Processing fact_sales...
  Extracted: 15,234 transactions
  Transformed: 15,234 records
  Loaded: 12,456 new records (2,778 duplicates skipped)

[Analytics] Running product_performance_job...
  Analyzed 1,000 products
  Updated product_performance table

[Complete] Batch processing finished in 4m 32s
```

## Running the Platform

### Complete Startup Sequence

**Terminal 1: Infrastructure**
```bash
# Start Docker services
docker-compose up -d

# Wait 60 seconds for services to stabilize
```

**Terminal 2: Data Generation**
```bash
# Start event generators
python data_generation/main.py

# Leave running to continuously generate events
```

**Terminal 3: Stream Processing**
```bash
# Start stream processing
python stream_processing/main.py

# Leave running to process events in real-time
```

**Terminal 4: Batch Processing** (Optional - if not using cron)
```bash
# Start batch loop
./batch_processing/scheduler/run_loop.sh

# Or set up cron job
./batch_processing/scheduler/setup_cron.sh
```

### Graceful Shutdown

```bash
# Stop batch processing
# Ctrl+C in Terminal 4

# Stop stream processing
# Ctrl+C in Terminal 3

# Stop data generation
# Ctrl+C in Terminal 2

# Stop infrastructure
docker-compose down
```

## Using Grafana Dashboards

### Accessing Grafana

1. Open browser to http://localhost:3000
2. Login credentials: `admin` / `admin`
3. (Optional) Change password when prompted

### Available Dashboards

#### 1. Executive Dashboard

**Access:** Dashboards → Executive Dashboard - E-Commerce Analytics

**Panels:**

**Key Performance Indicators:**
- Total Revenue (Latest Analysis)
- Total Orders (Latest Analysis)
- Average Order Value (Latest Analysis)
- Total Customers
- Total Units Sold (Latest Analysis)
- Average Conversion Rate (Latest)

**Revenue & Sales Trends:**
- Revenue Over Time (transaction-level)
- Revenue by Country (pie chart)
- Orders Over Time (transaction count)
- Top Products by Revenue (table)

**Customer Analytics:**
- Customer Segments Distribution (pie chart)
- RFM Segments (donut chart)
- Top Customers by Lifetime Value (table)
- Revenue by Customer Segment (time series)
- Recent Active Customers - RFM (table)

**Product Performance & Category Insights:**
- Sales by Category (bar chart)
- Revenue by Category (pie chart)
- Most Viewed Products (table)
- Best Converting Products (table)

**Sales Trends & Performance:**
- Sales Trends Over Time (table with growth rates)

**Features:**
- Auto-refresh: 30 seconds (configurable)
- Time Range: Last 24 hours (adjustable)
- Timezone: UTC
- Export: PNG, PDF, JSON

**Using the Dashboard:**

1. **Adjust Time Range:**
   - Click time picker (top right)
   - Select: Last 5m, 15m, 1h, 6h, 24h, 7d, 30d
   - Or set custom range

2. **Refresh Data:**
   - Click refresh icon (top right)
   - Or wait for auto-refresh

3. **Drill Down:**
   - Click on pie chart segments
   - Hover over graphs for details
   - Click table rows for more info

4. **Filter Data:**
   - Use dashboard variables (if configured)
   - Click legend items to toggle series

#### 2. Real-Time Monitoring Dashboard

**Access:** Dashboards → Real-Time Business Metrics

**Panels:**
- Sales per Minute
- Transactions by Country
- Top Trending Products
- Cart Abandonment Rate
- High-Value Anomalies
- Inventory Alerts

### Creating Custom Dashboards

1. **Click "+" → Create Dashboard**

2. **Add Panel**
   - Click "Add visualization"
   - Select datasource: `postgres-warehouse`

3. **Write Query**
   ```sql
   SELECT
       DATE_TRUNC('hour', transaction_timestamp) as time,
       SUM(total_amount) as revenue
   FROM fact_sales
   WHERE $__timeFilter(transaction_timestamp)
   GROUP BY time
   ORDER BY time
   ```

4. **Configure Visualization**
   - Select panel type: Time series, Table, Pie chart, etc.
   - Adjust axes, legend, colors
   - Set thresholds and alerts

5. **Save Dashboard**
   - Click "Save" icon
   - Enter dashboard name
   - Optionally add to folder

### Exporting Dashboards

**Export as JSON:**
1. Open dashboard
2. Click settings icon (gear)
3. Select "JSON Model"
4. Copy or save JSON

**Export as Image/PDF:**
1. Click share icon
2. Select "Direct link rendered image"
3. Adjust time range and dimensions
4. Click "Render"

## Querying the Data Warehouse

### Connecting to Warehouse

**Using psql:**
```bash
psql -h localhost -p 5432 -U globalmart_user -d globalmart_warehouse
```

**Using Python:**
```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="globalmart_warehouse",
    user="globalmart_user",
    password="warehouse_password"
)
```

### Common Queries

#### 1. Top 10 Products by Revenue

```sql
SELECT
    pp.product_id,
    pp.category,
    pp.total_sales,
    pp.total_revenue,
    pp.conversion_rate
FROM product_performance pp
WHERE pp.analysis_date = (SELECT MAX(analysis_date) FROM product_performance)
ORDER BY pp.total_revenue DESC
LIMIT 10;
```

#### 2. Customer Segmentation Summary

```sql
SELECT
    segment_name,
    customer_count,
    total_revenue,
    avg_order_value,
    ROUND(100.0 * customer_count / SUM(customer_count) OVER (), 2) as pct_customers
FROM customer_segments
ORDER BY total_revenue DESC;
```

#### 3. Daily Sales Trend

```sql
SELECT
    d.date,
    COUNT(DISTINCT fs.transaction_id) as num_orders,
    SUM(fs.total_amount) as revenue,
    AVG(fs.total_amount) as avg_order_value
FROM fact_sales fs
JOIN dim_date d ON fs.date_id = d.date_id
GROUP BY d.date
ORDER BY d.date DESC
LIMIT 30;
```

#### 4. Product Views vs Sales (Conversion)

```sql
SELECT
    p.category,
    COUNT(DISTINCT fpv.event_id) as total_views,
    COUNT(DISTINCT fs.transaction_id) as total_sales,
    ROUND(100.0 * COUNT(DISTINCT fs.transaction_id) /
          NULLIF(COUNT(DISTINCT fpv.event_id), 0), 2) as conversion_rate
FROM dim_products p
LEFT JOIN fact_product_views fpv ON p.product_id_pk = fpv.product_id_pk AND p.is_current = TRUE
LEFT JOIN fact_sales fs ON p.product_id_pk = fs.product_id_pk
GROUP BY p.category
ORDER BY conversion_rate DESC;
```

#### 5. Customer Lifetime Value (Top 20)

```sql
SELECT
    dc.user_id,
    dc.customer_segment,
    dc.total_transactions,
    dc.total_spent,
    dc.avg_order_value,
    dc.first_transaction_date,
    dc.last_transaction_date
FROM dim_customers dc
WHERE dc.is_current = TRUE
ORDER BY dc.total_spent DESC
LIMIT 20;
```

#### 6. Sales by Country

```sql
SELECT
    g.country,
    COUNT(DISTINCT fs.transaction_id) as num_orders,
    SUM(fs.total_amount) as revenue,
    AVG(fs.total_amount) as avg_order_value
FROM fact_sales fs
JOIN dim_geography g ON fs.geography_id = g.geography_id AND g.is_current = TRUE
GROUP BY g.country
ORDER BY revenue DESC;
```

#### 7. RFM Analysis - Champions

```sql
SELECT
    user_id,
    recency_days,
    frequency_count,
    monetary_value,
    rfm_segment
FROM rfm_analysis
WHERE analysis_date = (SELECT MAX(analysis_date) FROM rfm_analysis)
  AND rfm_segment = 'Champions'
ORDER BY monetary_value DESC
LIMIT 50;
```

### Best Practices for Querying

1. **Use Latest Analysis Date:**
   ```sql
   WHERE analysis_date = (SELECT MAX(analysis_date) FROM table_name)
   ```

2. **Filter by is_current for Dimensions:**
   ```sql
   WHERE is_current = TRUE
   ```

3. **Use Indexes:**
   - Dimensions: indexed on natural keys and `is_current`
   - Facts: indexed on foreign keys and timestamps

4. **Limit Result Sets:**
   ```sql
   LIMIT 1000  -- Always use LIMIT for exploratory queries
   ```

5. **Use Aggregate Functions:**
   ```sql
   SELECT SUM(), AVG(), COUNT() -- Better than fetching raw data
   ```

## Monitoring & Troubleshooting

### System Health Checks

#### Check All Services

```bash
docker-compose ps
```

Expected: All services "Up"

#### Check Kafka Topics

```bash
docker exec -it globalmart-kafka kafka-topics \
    --bootstrap-server localhost:9092 --list
```

Expected output:
```
cart-events-topic
product-views-topic
transactions-topic
```

#### Check Database Tables

**Realtime Database:**
```bash
psql -h localhost -p 5433 -U globalmart_user -d globalmart_realtime -c "\dt"
```

**Warehouse:**
```bash
psql -h localhost -p 5432 -U globalmart_user -d globalmart_warehouse -c "\dt"
```

#### Check Data Pipeline

```bash
python batch_processing/check_data_quality.py
```

### Common Issues

#### Issue: Services Won't Start

**Symptoms:**
- Docker containers exit immediately
- Port already in use errors

**Solution:**
```bash
# Check for port conflicts
lsof -i :5432
lsof -i :5433
lsof -i :9092

# Stop conflicting services
sudo service postgresql stop

# Restart Docker services
docker-compose down
docker-compose up -d
```

#### Issue: Time Series Graphs Not Updating

**Symptoms:**
- Grafana graphs show old data
- Graphs don't show recent time periods

**Solution:**
```bash
# 1. Verify data generator is running
ps aux | grep "data_generation/main.py"

# 2. Check stream processing is running
ps aux | grep "stream_processing/main.py"

# 3. Verify batch processing ran recently
tail -f batch_processing/scheduler/batch_processing.log

# 4. Check timezone settings
psql -h localhost -p 5432 -U globalmart_user -d globalmart_warehouse -c \
    "SELECT MAX(transaction_timestamp), NOW() FROM fact_sales;"

# 5. Reload Grafana dashboard
# Click refresh icon in Grafana
```

#### Issue: High Memory Usage

**Symptoms:**
- System slowdown
- Out of memory errors

**Solution:**
```bash
# Check Spark memory usage
ps aux | grep spark

# Adjust Spark memory in .env
nano .env
# Set SPARK_DRIVER_MEMORY=1g
# Set SPARK_EXECUTOR_MEMORY=1g

# Restart stream processing
```

#### Issue: Slow Queries

**Symptoms:**
- Grafana dashboards load slowly
- Database queries timeout

**Solution:**
```sql
-- Check query execution plan
EXPLAIN ANALYZE
SELECT * FROM fact_sales WHERE transaction_timestamp > NOW() - INTERVAL '24 hours';

-- Verify indexes exist
\di

-- Create missing indexes
CREATE INDEX idx_fact_sales_timestamp ON fact_sales(transaction_timestamp);
```

#### Issue: Duplicate Data

**Symptoms:**
- Record counts higher than expected
- Duplicate event_ids

**Solution:**
```bash
# Run data quality validation
python batch_processing/validate_fact_tables.py

# Check for duplicates
psql -h localhost -p 5432 -U globalmart_user -d globalmart_warehouse -c \
    "SELECT event_id, COUNT(*) FROM fact_sales GROUP BY event_id HAVING COUNT(*) > 1;"

# Reset if needed (CAUTION: Deletes all warehouse data)
python batch_processing/reset_warehouse.py
```

### Logs

**View Docker Logs:**
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f globalmart-kafka
docker-compose logs -f globalmart-postgres-rt
docker-compose logs -f globalmart-postgres-wh
```

**View Application Logs:**
```bash
# Stream processing
tail -f stream_processing/logs/stream_processing.log

# Batch processing
tail -f batch_processing/scheduler/batch_processing.log
```

## Advanced Usage

### Scaling Kafka

```bash
# Increase partitions
docker exec -it globalmart-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --topic transactions-topic \
    --alter --partitions 6
```

### Custom Analytics Job

Create `batch_processing/jobs/custom_job.py`:

```python
from batch_processing.jobs.base_job import BaseBatchJob
from pyspark.sql.functions import col, sum as spark_sum, avg

class CustomAnalyticsJob(BaseBatchJob):
    def __init__(self):
        super().__init__("Custom_Analytics")

    def execute(self):
        # Read from warehouse
        fact_sales = self.read_from_warehouse("fact_sales")
        dim_products = self.read_from_warehouse("dim_products")

        # Join and aggregate
        result = fact_sales.join(
            dim_products,
            fact_sales.product_id_pk == dim_products.product_id_pk
        ).groupBy("category").agg(
            spark_sum("total_amount").alias("revenue"),
            avg("quantity").alias("avg_quantity")
        )

        # Write result
        self.write_to_warehouse(result, "custom_analytics", mode="overwrite")

if __name__ == "__main__":
    job = CustomAnalyticsJob()
    job.run()
```

### Database Backup

```bash
# Backup warehouse
pg_dump -h localhost -p 5432 -U globalmart_user globalmart_warehouse \
    > backup_$(date +%Y%m%d).sql

# Restore
psql -h localhost -p 5432 -U globalmart_user -d globalmart_warehouse \
    < backup_20251207.sql
```

### Performance Tuning

**Increase Spark Parallelism:**
```python
# In stream_processing or batch_processing
spark = SparkSession.builder \
    .config("spark.default.parallelism", "8") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
```

**PostgreSQL Tuning:**
```sql
-- Increase shared_buffers (edit postgresql.conf)
shared_buffers = 2GB
effective_cache_size = 6GB
```

## FAQ

**Q: How do I change the event generation rate?**
A: Edit `.env` file and change `DATA_GEN_EVENTS_PER_SECOND` value, then restart data generation.

**Q: Can I run this in production?**
A: This is designed for production use. For production deployment:
- Use managed Kafka (e.g., Confluent Cloud)
- Use managed PostgreSQL (e.g., AWS RDS)
- Set up Spark cluster (e.g., Databricks)
- Enable SSL/TLS for all services
- Configure authentication and authorization

**Q: How do I add a new product category?**
A: Products are generated randomly. To add specific categories, edit `data_generation/generators/product_generator.py`.

**Q: Why are there NULL foreign keys in fact tables?**
A: This is expected:
- `fact_product_views`: Non-product events (search, filter) have NULL product_id_pk
- `fact_cart_events`: Checkout events have NULL product_id_pk (cart-level, not product-level)
- `fact_sales`: Missing customer_id when user not yet in dimension

**Q: How often should batch processing run?**
A: Default is every 5 minutes for testing. In production, typically run daily at off-peak hours (e.g., 2 AM UTC).

**Q: Can I export dashboards to PDF?**
A: Yes, Grafana supports PDF export. Click share icon → Direct link rendered image → Render.

**Q: How do I add a new dimension?**
A: 1) Create table in `batch_processing/schema/init_warehouse.sql`, 2) Create ETL job in `batch_processing/etl/`, 3) Update fact table ETL to join new dimension.

---

**Document Version:** 1.0
**Last Updated:** December 2025
**Support:** For issues, consult logs and troubleshooting section above
