# GlobalMart Batch Processing

Batch processing and data warehouse layer for GlobalMart e-commerce analytics platform.

## Overview

The batch processing system aggregates real-time streaming data into a data warehouse for historical analytics and business intelligence. It implements:

- **Dimension Loading** with SCD Type 2 (Slowly Changing Dimensions)
- **Fact Loading** with incremental timestamp-based processing
- **Analytics Jobs** for customer segmentation, product performance, and sales trends
- **Automated Scheduling** via cron jobs

## Architecture

```
PostgreSQL Real-Time DB (port 5433, globalmart_realtime)
├─ 18 tables with streaming aggregations
├─ Updated continuously by Spark Streaming
└─ 1-10 minute windowed data
        ↓
  Batch ETL Pipeline (PySpark)
  ├─ Dimension Loading (SCD Type 2)
  ├─ Fact Loading (Incremental by timestamp)
  └─ Analytics Jobs (RFM, Product Performance, etc.)
        ↓
PostgreSQL Data Warehouse (port 5432, globalmart_warehouse)
├─ 4 Dimension Tables (with history tracking)
├─ 3 Fact Tables (transaction-level detail)
└─ 4 Analytics Tables (RFM, performance, trends, segments)
```

## Prerequisites

- Python 3.9+
- Apache Spark 3.5.0
- PostgreSQL 13+
- WSL (for cron scheduling on Windows)
- Conda environment: `globalmart_env`

## Setup

### 1. Apply Schema Migrations

```bash
# Apply SCD Type 2 columns to warehouse
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -f batch_processing/schema/add_scd_columns.sql

# Create job tracking table
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -f batch_processing/schema/create_tracking_table.sql
```

### 2. Install Dependencies

All dependencies are already in the `globalmart_env` conda environment from Phases 1 & 2.

### 3. Configure Environment (Optional)

Set environment variables for custom configuration:

```bash
# Database configuration
export WAREHOUSE_DB_HOST=localhost
export WAREHOUSE_DB_PORT=5432
export WAREHOUSE_DB_NAME=globalmart_warehouse
export WAREHOUSE_DB_USER=postgres
export WAREHOUSE_DB_PASSWORD=postgres

export REALTIME_DB_HOST=localhost
export REALTIME_DB_PORT=5433
export REALTIME_DB_NAME=globalmart_realtime

# Spark configuration
export SPARK_MASTER=local[*]
export SPARK_MEMORY=4g
export SPARK_CORES=4

# Logging
export BATCH_LOG_LEVEL=INFO
```

## Usage

### Manual Execution

#### Run All Jobs (Recommended)

```bash
python batch_processing/main.py --jobs all
```

#### Run Specific Job Groups

```bash
# Dimensions only
python batch_processing/main.py --jobs dimensions

# Facts only (requires dimensions)
python batch_processing/main.py --jobs facts

# Analytics only (requires facts)
python batch_processing/main.py --jobs analytics
```

#### Run Specific Jobs

```bash
# RFM Analysis only
python batch_processing/main.py --jobs rfm_analysis

# Product performance and sales trends
python batch_processing/main.py --jobs product_performance sales_trends
```

#### Dry Run (Validate without executing)

```bash
python batch_processing/main.py --jobs all --dry-run
```

#### Full Reload (Ignore incremental timestamps)

```bash
python batch_processing/main.py --jobs all --full-reload
```

### Automated Scheduling (Cron)

#### Install Cron Job (Daily at 2 AM UTC)

```bash
cd /mnt/c/Users/nadin/OneDrive/Desktop/GlobalMart
chmod +x batch_processing/scheduler/setup_cron.sh
./batch_processing/scheduler/setup_cron.sh
```

#### Verify Cron Installation

```bash
# Check cron service
service cron status

# View installed cron jobs
crontab -l

# View cron logs
cat /var/log/globalmart/cron.log
```

#### Manual Test Run

```bash
# Test the cron script manually
./batch_processing/scheduler/run_batch_jobs.sh --dry-run

# Full execution
./batch_processing/scheduler/run_batch_jobs.sh
```

#### Uninstall Cron Job

```bash
./batch_processing/scheduler/setup_cron.sh --uninstall
```

## Job Descriptions

### Dimension ETL Jobs

| Job | Description | SCD Type | Source |
|-----|-------------|----------|--------|
| `dim_date` | Date dimension with calendar attributes | Static | Generated |
| `dim_geography` | Geography reference data | Static | Static |
| `dim_customers` | Customer profiles | SCD Type 2 | sales_by_country |
| `dim_products` | Product catalog | SCD Type 2 | trending_products |

### Fact ETL Jobs

| Job | Description | Loading | Source |
|-----|-------------|---------|--------|
| `fact_sales` | Transaction-level sales | Incremental | sales_per_minute |
| `fact_cart_events` | Cart interactions | Incremental | cart_sessions |
| `fact_product_views` | Product browsing | Incremental | trending_products |

### Analytics Jobs

| Job | Description | Output Table | Dependencies |
|-----|-------------|--------------|--------------|
| `rfm_analysis` | Customer segmentation (Recency, Frequency, Monetary) | rfm_analysis | fact_sales |
| `product_performance` | Product metrics and rankings | product_performance | fact_sales, fact_product_views |
| `sales_trends` | Temporal and geographic sales trends | sales_trends | fact_sales, dim_date |
| `customer_segments` | Segment characteristics and distributions | customer_segments | rfm_analysis |

## Job Dependencies

The orchestrator automatically handles dependencies:

```
Dimensions (parallel) → Facts (parallel) → Analytics (sequential)
```

Detailed dependency graph:

```
dim_date ──┬──> fact_sales ──┬──> rfm_analysis ──> customer_segments
           │                 │
dim_geography ┘             └──> product_performance
                                  │
dim_customers ──> fact_cart_events │
                                  │
dim_products ──> fact_product_views ┘
                  │
                  └──> sales_trends
```

## Data Quality

The system includes automated data quality checks:

- **Null Value Detection**: Identifies missing required fields
- **Duplicate Detection**: Finds duplicate keys
- **Referential Integrity**: Validates foreign key relationships
- **Value Range Validation**: Checks numeric bounds
- **Data Freshness**: Detects stale data

## Monitoring & Logging

### Log Files

- **Main logs**: `logs/batch/<job_name>.log`
- **Daily logs**: `logs/batch/<job_name>_YYYYMMDD.log`
- **Cron logs**: `/var/log/globalmart/batch_YYYYMMDD_HHMMSS.log`

### Job Tracking

View job execution history:

```sql
SELECT * FROM batch_job_tracker
ORDER BY last_run_timestamp DESC;
```

Check job status:

```sql
SELECT
    job_name,
    last_run_status,
    last_run_timestamp,
    rows_processed,
    execution_duration_sec
FROM batch_job_tracker
WHERE last_run_status = 'FAILED';
```

### Execution Reports

Each run generates a summary report with:
- Jobs executed
- Rows processed/inserted/updated
- Execution duration
- Errors encountered
- Overall status

## Troubleshooting

### Common Issues

#### 1. Spark Memory Errors

```bash
# Increase Spark memory allocation
export SPARK_MEMORY=8g
python batch_processing/main.py --jobs all
```

#### 2. Database Connection Timeouts

```bash
# Check PostgreSQL is running
sudo service postgresql status

# Test connections
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse
psql -h localhost -p 5433 -U postgres -d globalmart_realtime
```

#### 3. Cron Jobs Not Running

```bash
# Check cron service
sudo service cron status
sudo service cron start

# Check cron logs
tail -f /var/log/globalmart/cron.log
```

#### 4. Missing Dependencies

```bash
# Verify environment
conda activate globalmart_env
python -c "import pyspark; print(pyspark.__version__)"
python -c "import psycopg2; print('psycopg2 OK')"
```

### Debug Mode

Run with verbose logging:

```bash
export BATCH_LOG_LEVEL=DEBUG
python batch_processing/main.py --jobs all
```

## Performance Tuning

### Spark Configuration

Adjust based on your system resources:

```python
# In config.py
SPARK_MEMORY = "8g"  # Increase for large datasets
SPARK_CORES = "8"    # Match CPU cores
```

### Incremental Loading

The system uses timestamps to only process new data:

```sql
-- Check last load timestamp for a job
SELECT job_name, last_run_timestamp
FROM batch_job_tracker
WHERE job_name = 'fact_sales';
```

Force full reload when needed:

```bash
python batch_processing/main.py --jobs all --full-reload
```

## SCD Type 2 Implementation

Customer and product dimensions track historical changes:

```sql
-- View customer history
SELECT
    customer_sk,
    user_id,
    customer_segment,
    effective_from,
    effective_to,
    is_current
FROM dim_customers
WHERE user_id = 'USER_1234'
ORDER BY effective_from;
```

Example:
```
customer_sk | user_id   | customer_segment  | effective_from | effective_to | is_current
1           | USER_1234 | New Customer      | 2024-01-01     | 2024-01-30   | FALSE
2           | USER_1234 | Loyal Customers   | 2024-01-30     | 9999-12-31   | TRUE
```

## RFM Analysis Details

Customer segmentation using Recency, Frequency, Monetary metrics:

### Scoring (1-5 scale using quintiles)

- **Recency**: Days since last transaction (5 = recent, 1 = old)
- **Frequency**: Number of transactions (5 = many, 1 = few)
- **Monetary**: Total spending (5 = high, 1 = low)

### Segments

| RFM Score | Segment | Description |
|-----------|---------|-------------|
| 544-555 | Champions | Best customers, highest value |
| 434-543 | Loyal Customers | Frequent buyers, good value |
| 334-433 | Potential Loyalists | Recent customers, growing |
| 244-333 | At Risk | Declining engagement |
| 144-243 | Need Attention | Low activity, require nurturing |
| 111-143 | Lost Customers | Inactive, churn risk |

View RFM results:

```sql
SELECT
    rfm_segment,
    COUNT(*) as customer_count,
    AVG(monetary_value) as avg_spending,
    AVG(recency_days) as avg_recency
FROM rfm_analysis
GROUP BY rfm_segment
ORDER BY customer_count DESC;
```

## Testing

### Test Pipeline End-to-End

1. **Ensure streaming is running** (Phase 2):
```bash
# Start data generation
python data_generation/kafka_producer.py

# Start stream processing
python stream_processing/main.py
```

2. **Run batch processing**:
```bash
# Dry run first
python batch_processing/main.py --jobs all --dry-run

# Full execution
python batch_processing/main.py --jobs all
```

3. **Validate results**:
```sql
-- Check dimension counts
SELECT 'dim_date' as table_name, COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_geography', COUNT(*) FROM dim_geography
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM dim_customers WHERE is_current = TRUE
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products WHERE is_current = TRUE;

-- Check fact counts
SELECT 'fact_sales' as table_name, COUNT(*) FROM fact_sales
UNION ALL
SELECT 'fact_cart_events', COUNT(*) FROM fact_cart_events
UNION ALL
SELECT 'fact_product_views', COUNT(*) FROM fact_product_views;

-- Check analytics
SELECT 'rfm_analysis' as table_name, COUNT(*) FROM rfm_analysis
UNION ALL
SELECT 'product_performance', COUNT(*) FROM product_performance
UNION ALL
SELECT 'sales_trends', COUNT(*) FROM sales_trends
UNION ALL
SELECT 'customer_segments', COUNT(*) FROM customer_segments;
```

## Best Practices

1. **Run dimensions before facts**: Always load dimensions first
2. **Monitor job tracker**: Check `batch_job_tracker` for failures
3. **Review logs regularly**: Check for warnings and errors
4. **Use dry-run**: Validate before production execution
5. **Schedule during off-peak**: Cron runs at 2 AM UTC to minimize impact
6. **Clean old logs**: Automatic cleanup keeps last 30 days
7. **Test incrementally**: Run individual jobs before `--jobs all`

## Contributing

When adding new jobs:

1. Create job class extending `BaseETL` or `BaseAnalyticsJob`
2. Implement `extract()`, `transform()` / `analyze()`, and `load()` methods
3. Add job to `config.JOB_DEPENDENCIES` and `JOB_GROUPS`
4. Add job instantiation to `BatchOrchestrator.get_job_instance()`
5. Test with `--dry-run` first

## Support

- **Logs**: Check `logs/batch/` for detailed execution logs
- **Status**: Query `batch_job_tracker` table
- **Issues**: Review error messages and stack traces

---

**Status**: Production Ready ✅
**Last Updated**: 2024-12-06
**Version**: 1.0.0
