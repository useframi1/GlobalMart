# GlobalMart Technical Architecture

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Lambda Architecture](#lambda-architecture)
3. [Data Pipeline](#data-pipeline)
4. [Database Design](#database-design)
5. [Stream Processing](#stream-processing)
6. [Batch Processing](#batch-processing)
7. [Data Warehouse](#data-warehouse)
8. [Technology Stack](#technology-stack)
9. [Scalability & Performance](#scalability--performance)
10. [Security & Compliance](#security--compliance)

## Architecture Overview

GlobalMart implements a **Lambda Architecture** to provide both real-time and batch analytics for e-commerce operations. The system processes millions of events daily, maintaining low latency for real-time insights while ensuring data accuracy and completeness through batch processing.

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA GENERATION LAYER                      │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │ Transaction │  │ Product View │  │   Cart Event         │   │
│  │  Generator  │  │  Generator   │  │   Generator          │   │
│  └──────┬──────┘  └──────┬───────┘  └──────┬───────────────┘   │
└─────────┼─────────────────┼──────────────────┼───────────────────┘
          │                 │                  │
          ▼                 ▼                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                      MESSAGE BROKER LAYER                         │
│                        Apache Kafka                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │transactions- │  │product-views-│  │   cart-events-       │  │
│  │   topic      │  │   topic      │  │     topic            │  │
│  │ (3 partitions)│ │ (3 partitions)│ │  (3 partitions)      │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────────────┘  │
└─────────┼──────────────────┼──────────────────┼──────────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                 SPEED LAYER (Real-Time Processing)                │
│                   Apache Spark Structured Streaming               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Transaction  │  │Product View  │  │   Cart Event         │  │
│  │    Saver     │  │    Saver     │  │     Saver            │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────────────┘  │
└─────────┼──────────────────┼──────────────────┼──────────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                     REALTIME DATABASE                             │
│                   PostgreSQL (port 5433)                          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ transactions | product_view_events | cart_events         │   │
│  └──────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│              BATCH LAYER (Batch Processing - Spark)               │
│                                                                    │
│  ETL JOBS:                      ANALYTICS JOBS:                   │
│  ┌────────────────────┐         ┌──────────────────────────┐    │
│  │ • Fact Sales ETL   │         │ • Product Performance    │    │
│  │ • Fact Views ETL   │         │ • Customer Segments      │    │
│  │ • Fact Cart ETL    │         │ • RFM Analysis           │    │
│  │ • Dim Customers    │         │ • Sales Trends           │    │
│  │ • Dim Products     │         └──────────────────────────┘    │
│  │ • Dim Geography    │                                          │
│  │ • Dim Date         │                                          │
│  └────────────────────┘                                          │
│                                                                    │
│  Scheduler: Cron (every 5 minutes for testing)                   │
└──────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE (Star Schema)                    │
│                   PostgreSQL (port 5432)                          │
│                                                                    │
│  FACT TABLES:              DIMENSION TABLES (SCD Type 2):        │
│  ┌──────────────────┐     ┌──────────────────────────────┐      │
│  │ fact_sales       │     │ dim_customers                │      │
│  │ fact_product_    │     │ dim_products                 │      │
│  │      views       │     │ dim_geography                │      │
│  │ fact_cart_events │     │ dim_date                     │      │
│  └──────────────────┘     └──────────────────────────────┘      │
│                                                                    │
│  ANALYTICS TABLES:                                                │
│  ┌──────────────────────────────────────────────────────┐        │
│  │ product_performance | customer_segments             │        │
│  │ rfm_analysis        | sales_trends                  │        │
│  └──────────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                     SERVING LAYER                                 │
│                                                                    │
│  ┌────────────────┐    ┌────────────────┐    ┌──────────────┐   │
│  │    Grafana     │    │    Power BI    │    │   FastAPI    │   │
│  │  (Dashboards)  │    │  (Executive)   │    │   (API)      │   │
│  └────────────────┘    └────────────────┘    └──────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

## Lambda Architecture

GlobalMart implements the Lambda Architecture pattern, providing:

### Speed Layer (Real-Time)
- **Purpose**: Low-latency event processing for immediate insights
- **Technology**: Apache Spark Structured Streaming
- **Jobs**: Event savers that persist Kafka events to PostgreSQL
- **Latency**: Sub-second from event to database
- **Use Cases**: Real-time monitoring, operational dashboards

### Batch Layer (Batch Processing)
- **Purpose**: Comprehensive, accurate analytics on complete datasets
- **Technology**: Apache Spark batch processing
- **Jobs**: ETL pipelines and analytics jobs
- **Schedule**: Runs every 5 minutes (configurable, typically daily in production)
- **Use Cases**: Historical analysis, data quality, complex aggregations

### Serving Layer
- **Purpose**: Provide unified view of both real-time and batch data
- **Technology**: Grafana, Power BI, FastAPI
- **Data Source**: PostgreSQL warehouse (star schema)
- **Features**: Dashboard visualization, API access, self-service analytics

## Data Pipeline

### 1. Data Generation

**Components:**
- Transaction Generator
- Product View Generator
- Cart Event Generator

**Output:**
- 50 events/second (configurable)
- Distribution: 60% product views, 30% cart events, 10% transactions
- Format: JSON messages
- Timezone: UTC

**Event Schema Examples:**

**Transaction Event:**
```json
{
  "transaction_id": "uuid",
  "user_id": "user_123",
  "products": [{"product_id": "prod_456", "quantity": 2, "price": 29.99}],
  "total_amount": 59.98,
  "country": "USA",
  "transaction_timestamp": "2025-12-07T04:15:30.123Z"
}
```

**Product View Event:**
```json
{
  "event_id": "uuid",
  "event_type": "view",
  "user_id": "user_123",
  "product_id": "prod_456",
  "session_id": "session_789",
  "timestamp": "2025-12-07T04:15:30.123Z",
  "country": "USA"
}
```

### 2. Message Broker (Apache Kafka)

**Topics:**
- `transactions-topic` (3 partitions)
- `product-views-topic` (3 partitions)
- `cart-events-topic` (3 partitions)

**Configuration:**
- Replication Factor: 1 (single broker)
- Retention: 7 days
- Partitioning: Round-robin by default

### 3. Stream Processing

**Technology:** Apache Spark 3.4.0 Structured Streaming

**Jobs:**

1. **Transaction Saver**
   - Reads from: `transactions-topic`
   - Writes to: `transactions` table
   - Processing: Kafka → Parse JSON → PostgreSQL

2. **Product View Saver**
   - Reads from: `product-views-topic`
   - Writes to: `product_view_events` table
   - Processing: Kafka → Parse JSON → PostgreSQL

3. **Cart Event Saver**
   - Reads from: `cart-events-topic`
   - Writes to: `cart_events` table
   - Processing: Kafka → Parse JSON → PostgreSQL

**Features:**
- Exactly-once processing semantics
- Checkpoint-based fault tolerance
- Automatic schema evolution
- UTC timezone enforcement

### 4. Batch Processing

**ETL Jobs:**

1. **Dimension ETL (SCD Type 2)**
   - `dim_customers_etl.py`: Customer dimension with segmentation
   - `dim_products_etl.py`: Product catalog with categories
   - `dim_geography_etl.py`: Geographic dimensions
   - `dim_date_etl.py`: Date dimension

2. **Fact ETL (Incremental)**
   - `fact_sales_etl.py`: Transaction-level sales
   - `fact_product_views_etl.py`: Product view events
   - `fact_cart_events_etl.py`: Cart interaction events

**Analytics Jobs:**

1. **Product Performance** (`product_performance_job.py`)
   - Calculates: Sales, revenue, views, conversion rate per product
   - Granularity: Daily snapshots
   - Output: `product_performance` table

2. **Customer Segments** (`customer_segments_job.py`)
   - Segments: High Value, Medium Value, Low Value, New
   - Criteria: Total spend thresholds
   - Output: `customer_segments` table

3. **RFM Analysis** (`rfm_analysis_job.py`)
   - Scores: Recency (1-5), Frequency (1-5), Monetary (1-5)
   - Segments: Champions, Loyal, At Risk, etc.
   - Output: `rfm_analysis` table

4. **Sales Trends** (`sales_trends_job.py`)
   - Periods: Weekly, Monthly, Quarterly
   - Metrics: Sales, revenue, growth rate
   - Output: `sales_trends` table

**Scheduling:**
- Cron-based execution
- Default: Every 5 minutes (testing)
- Production: Daily at 2 AM UTC

## Database Design

### Realtime Database (PostgreSQL - Port 5433)

**Purpose:** Store raw events from Kafka for batch processing

**Tables:**
- `transactions` - Sales transactions
- `product_view_events` - Product view events
- `cart_events` - Shopping cart events

**Schema Example (`transactions`):**
```sql
CREATE TABLE transactions (
    transaction_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    country VARCHAR(100),
    products JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transactions_timestamp ON transactions(transaction_timestamp);
CREATE INDEX idx_transactions_user ON transactions(user_id);
```

### Data Warehouse (PostgreSQL - Port 5432)

**Schema Type:** Star Schema

#### Fact Tables

**1. fact_sales**
```sql
CREATE TABLE fact_sales (
    event_id VARCHAR(100) PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL,
    date_id INTEGER,
    customer_id INTEGER,
    product_id_pk INTEGER,
    geography_id INTEGER,
    transaction_timestamp TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id_pk) REFERENCES dim_products(product_id_pk),
    FOREIGN KEY (geography_id) REFERENCES dim_geography(geography_id)
);
```

**2. fact_product_views**
```sql
CREATE TABLE fact_product_views (
    event_id VARCHAR(100) PRIMARY KEY,
    date_id INTEGER,
    customer_id INTEGER,
    product_id_pk INTEGER,
    geography_id INTEGER,
    event_timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50),
    session_id VARCHAR(100),
    view_duration INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id_pk) REFERENCES dim_products(product_id_pk),
    FOREIGN KEY (geography_id) REFERENCES dim_geography(geography_id)
);
```

**3. fact_cart_events**
```sql
CREATE TABLE fact_cart_events (
    event_id VARCHAR(100) PRIMARY KEY,
    date_id INTEGER,
    customer_id INTEGER,
    product_id_pk INTEGER,
    geography_id INTEGER,
    event_timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50),
    session_id VARCHAR(100),
    quantity INTEGER,
    total_amount DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id_pk) REFERENCES dim_products(product_id_pk),
    FOREIGN KEY (geography_id) REFERENCES dim_geography(geography_id)
);
```

#### Dimension Tables (SCD Type 2)

**1. dim_customers**
```sql
CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    first_transaction_date DATE,
    last_transaction_date DATE,
    total_transactions BIGINT DEFAULT 0,
    total_spent DOUBLE PRECISION DEFAULT 0,
    avg_order_value DOUBLE PRECISION DEFAULT 0,
    customer_segment VARCHAR(50),
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customers_user_id ON dim_customers(user_id);
CREATE INDEX idx_dim_customers_is_current ON dim_customers(is_current);
```

**2. dim_products**
```sql
CREATE TABLE dim_products (
    product_id_pk SERIAL PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL,
    category VARCHAR(100),
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_products_product_id ON dim_products(product_id);
CREATE INDEX idx_dim_products_is_current ON dim_products(is_current);
```

**3. dim_geography**
```sql
CREATE TABLE dim_geography (
    geography_id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL UNIQUE,
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**4. dim_date**
```sql
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
```

#### Analytics Tables

**1. product_performance**
```sql
CREATE TABLE product_performance (
    performance_id SERIAL PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL,
    category VARCHAR(100),
    analysis_date DATE NOT NULL,
    total_sales BIGINT NOT NULL DEFAULT 0,
    total_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_quantity BIGINT NOT NULL DEFAULT 0,
    total_views BIGINT NOT NULL DEFAULT 0,
    conversion_rate DOUBLE PRECISION,
    avg_price DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**2. customer_segments**
```sql
CREATE TABLE customer_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(50) NOT NULL UNIQUE,
    customer_count BIGINT DEFAULT 0,
    total_revenue DOUBLE PRECISION DEFAULT 0,
    avg_order_value DOUBLE PRECISION DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**3. rfm_analysis**
```sql
CREATE TABLE rfm_analysis (
    rfm_id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    analysis_date DATE NOT NULL,
    recency_days INTEGER NOT NULL,
    frequency_count BIGINT NOT NULL,
    monetary_value DOUBLE PRECISION NOT NULL,
    recency_score INTEGER NOT NULL CHECK (recency_score BETWEEN 1 AND 5),
    frequency_score INTEGER NOT NULL CHECK (frequency_score BETWEEN 1 AND 5),
    monetary_score INTEGER NOT NULL CHECK (monetary_score BETWEEN 1 AND 5),
    rfm_segment VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**4. sales_trends**
```sql
CREATE TABLE sales_trends (
    trend_id SERIAL PRIMARY KEY,
    period_type VARCHAR(20) NOT NULL,
    period_value VARCHAR(50) NOT NULL,
    category VARCHAR(100),
    total_sales BIGINT NOT NULL DEFAULT 0,
    total_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_order_value DOUBLE PRECISION,
    total_quantity BIGINT NOT NULL DEFAULT 0,
    growth_rate DOUBLE PRECISION,
    analysis_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Stream Processing

### Technology Stack
- Apache Spark 3.4.0
- Structured Streaming API
- Kafka Source Connector
- PostgreSQL Sink (JDBC)

### Configuration
```python
spark = SparkSession.builder \
    .appName("GlobalMart Stream Processing") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
```

### Processing Pattern

```python
# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse and transform
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write to PostgreSQL
query = parsed_df.writeStream \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "transactions") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()
```

## Batch Processing

### ETL Pipeline Architecture

**Pattern:** Extract-Transform-Load

1. **Extract**: Read from realtime PostgreSQL
2. **Transform**:
   - Cleanse and validate
   - Enrich with dimensions
   - Apply business logic
   - Handle SCD Type 2
3. **Load**: Write to warehouse

### SCD Type 2 Implementation

**Algorithm:**
```python
def apply_scd_type_2(new_data, existing_dim):
    # Identify changed records
    changed = new_data.join(
        existing_dim.filter("is_current = true"),
        on=key_columns,
        how="left"
    ).where(hash(new_cols) != hash(existing_cols))

    # Expire old records
    expired = changed.withColumn("end_date", current_date()) \
                    .withColumn("is_current", False)

    # Insert new records
    new_records = changed.withColumn("effective_date", current_date()) \
                        .withColumn("end_date", None) \
                        .withColumn("is_current", True)

    return expired.union(new_records)
```

### Deduplication Strategy

**Event ID-based:**
```python
def deduplicate(new_data, existing_fact):
    existing_ids = existing_fact.select("event_id").distinct()
    deduplicated = new_data.join(
        existing_ids,
        on="event_id",
        how="left_anti"
    )
    return deduplicated
```

## Technology Stack

### Core Technologies

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Stream Processing | Apache Spark | 3.4.0 | Real-time event processing |
| Batch Processing | Apache Spark | 3.4.0 | ETL and analytics |
| Message Broker | Apache Kafka | 3.x | Event streaming |
| Realtime DB | PostgreSQL | 15 | Raw event storage |
| Warehouse DB | PostgreSQL | 15 | Star schema analytics |
| Caching | Redis | 7.x | API response caching |
| API | FastAPI | 0.100+ | REST API |
| Visualization | Grafana | 10.x | Dashboards |
| Monitoring | Prometheus | 2.x | Metrics collection |
| Orchestration | Docker Compose | 2.x | Service deployment |

### Language & Frameworks
- **Python 3.11**: Primary language
- **PySpark**: Spark Python API
- **psycopg2**: PostgreSQL driver
- **kafka-python**: Kafka client

## Scalability & Performance

### Horizontal Scaling

**Kafka:**
- Increase partition count for higher throughput
- Add broker nodes for distributed load

**Spark:**
- Increase executor count
- Adjust parallelism (spark.default.parallelism)

**PostgreSQL:**
- Read replicas for query scaling
- Partitioning for large fact tables

### Performance Optimization

**1. Batch Size Tuning**
```python
.option("maxOffsetsPerTrigger", "10000")  # Kafka read batch
.option("batchsize", "1000")              # JDBC write batch
```

**2. Indexing Strategy**
- B-tree indexes on foreign keys
- B-tree indexes on timestamp columns
- Composite indexes for common query patterns

**3. Query Optimization**
- Materialized views for complex aggregations
- Query result caching in Redis
- Connection pooling

## Security & Compliance

### Data Security
- PostgreSQL authentication with strong passwords
- Kafka SASL/SSL (production)
- Network isolation via Docker networks

### Data Privacy
- PII handling in customer dimensions
- Data retention policies (7 days for Kafka)
- Audit logging for data access

### Compliance
- UTC timestamp consistency for audit trails
- Immutable fact tables (append-only)
- Data lineage tracking

---

**Document Version:** 1.0
**Last Updated:** December 2025
**Maintained By:** GlobalMart Engineering Team
