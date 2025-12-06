# Batch Processing Testing Guide

Comprehensive testing guide for GlobalMart Batch Processing system.

## Prerequisites

Before testing, ensure:

1. ✅ **PostgreSQL databases running**:
   - Real-time DB: `localhost:5433/globalmart_realtime`
   - Warehouse DB: `localhost:5432/globalmart_warehouse`

2. ✅ **Schema migrations applied**:
   ```bash
   psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -f batch_processing/schema/add_scd_columns.sql
   psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -f batch_processing/schema/create_tracking_table.sql
   ```

3. ✅ **Streaming pipeline running** (Phase 2):
   ```bash
   # Terminal 1: Data generation
   conda activate globalmart_env
   python data_generation/kafka_producer.py

   # Terminal 2: Stream processing
   python stream_processing/main.py
   ```

4. ✅ **Conda environment activated**:
   ```bash
   conda activate globalmart_env
   ```

## Test Plan

### Level 1: Component Testing (Individual Jobs)

#### Test 1.1: Dimension ETL - Date

```bash
# Test date dimension
python batch_processing/main.py --jobs dim_date

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT COUNT(*) as total_dates,
       MIN(full_date) as start_date,
       MAX(full_date) as end_date
FROM dim_date;
"

# Expected: ~1095 dates (2024-01-01 to 2026-12-31)
```

#### Test 1.2: Dimension ETL - Geography

```bash
# Test geography dimension
python batch_processing/main.py --jobs dim_geography

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT country, region, continent FROM dim_geography;
"

# Expected: 5 countries (USA, UK, Germany, France, Japan)
```

#### Test 1.3: Dimension ETL - Customers (SCD Type 2)

```bash
# Test customer dimension
python batch_processing/main.py --jobs dim_customers

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT COUNT(*) as total_records,
       COUNT(DISTINCT user_id) as unique_customers,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records
FROM dim_customers;
"

# Check SCD columns
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT user_id, customer_segment, effective_from, effective_to, is_current
FROM dim_customers
LIMIT 5;
"
```

#### Test 1.4: Dimension ETL - Products (SCD Type 2)

```bash
# Test product dimension
python batch_processing/main.py --jobs dim_products

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT COUNT(*) as total_records,
       COUNT(DISTINCT product_id) as unique_products,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records
FROM dim_products;
"

# Check categories
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT category, COUNT(*) FROM dim_products WHERE is_current = TRUE GROUP BY category;
"
```

#### Test 1.5: Fact ETL - Sales

```bash
# Test sales fact
python batch_processing/main.py --jobs fact_sales

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT COUNT(*) as total_sales,
       SUM(total_revenue) as total_revenue,
       AVG(total_revenue) as avg_revenue,
       MIN(transaction_timestamp) as earliest,
       MAX(transaction_timestamp) as latest
FROM fact_sales;
"

# Check dimension keys populated
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN customer_sk IS NOT NULL THEN 1 ELSE 0 END) as with_customer_sk,
    SUM(CASE WHEN product_sk IS NOT NULL THEN 1 ELSE 0 END) as with_product_sk,
    SUM(CASE WHEN date_key IS NOT NULL THEN 1 ELSE 0 END) as with_date_key
FROM fact_sales;
"
```

#### Test 1.6: Fact ETL - Cart Events

```bash
# Test cart events fact
python batch_processing/main.py --jobs fact_cart_events

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT COUNT(*) as total_events,
       SUM(CASE WHEN is_abandoned THEN 1 ELSE 0 END) as abandoned_carts,
       SUM(total_value) as total_cart_value
FROM fact_cart_events;
"
```

#### Test 1.7: Fact ETL - Product Views

```bash
# Test product views fact
python batch_processing/main.py --jobs fact_product_views

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT COUNT(*) as total_views,
       SUM(view_count) as total_view_count,
       AVG(click_through_rate) as avg_ctr
FROM fact_product_views;
"
```

#### Test 1.8: Analytics - RFM Analysis (PRIORITY 1)

```bash
# Test RFM analysis
python batch_processing/main.py --jobs rfm_analysis

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT rfm_segment, COUNT(*) as customer_count
FROM rfm_analysis
GROUP BY rfm_segment
ORDER BY customer_count DESC;
"

# Check score distribution
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT
    MIN(recency_score) as min_r,
    MAX(recency_score) as max_r,
    MIN(frequency_score) as min_f,
    MAX(frequency_score) as max_f,
    MIN(monetary_score) as min_m,
    MAX(monetary_score) as max_m
FROM rfm_analysis;
"
# Expected: All scores should be 1-5
```

#### Test 1.9: Analytics - Product Performance

```bash
# Test product performance
python batch_processing/main.py --jobs product_performance

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT performance_tier, COUNT(*)
FROM product_performance
GROUP BY performance_tier
ORDER BY COUNT(*) DESC;
"

# Top products
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT product_id, category, total_revenue, overall_rank
FROM product_performance
ORDER BY overall_rank
LIMIT 10;
"
```

#### Test 1.10: Analytics - Sales Trends

```bash
# Test sales trends
python batch_processing/main.py --jobs sales_trends

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT period_type, COUNT(*)
FROM sales_trends
GROUP BY period_type;
"

# Check growth rates
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT period_type, period_value, country, total_revenue, growth_rate
FROM sales_trends
WHERE period_type = 'monthly'
ORDER BY period_value DESC
LIMIT 10;
"
```

#### Test 1.11: Analytics - Customer Segments

```bash
# Test customer segments
python batch_processing/main.py --jobs customer_segments

# Validate results
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT segment_type, segment_name, customer_count, total_revenue
FROM customer_segments
ORDER BY total_revenue DESC;
"
```

### Level 2: Integration Testing (Job Groups)

#### Test 2.1: All Dimensions

```bash
# Run all dimension jobs
python batch_processing/main.py --jobs dimensions

# Validate all dimensions populated
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM dim_date
UNION ALL
SELECT 'dim_geography', COUNT(*) FROM dim_geography
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM dim_customers WHERE is_current = TRUE
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products WHERE is_current = TRUE;
"
```

#### Test 2.2: All Facts

```bash
# Run all fact jobs (requires dimensions)
python batch_processing/main.py --jobs facts

# Validate all facts populated
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT 'fact_sales' as table_name, COUNT(*) as row_count FROM fact_sales
UNION ALL
SELECT 'fact_cart_events', COUNT(*) FROM fact_cart_events
UNION ALL
SELECT 'fact_product_views', COUNT(*) FROM fact_product_views;
"
```

#### Test 2.3: All Analytics

```bash
# Run all analytics jobs (requires facts)
python batch_processing/main.py --jobs analytics

# Validate all analytics populated
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT 'rfm_analysis' as table_name, COUNT(*) as row_count FROM rfm_analysis
UNION ALL
SELECT 'product_performance', COUNT(*) FROM product_performance
UNION ALL
SELECT 'sales_trends', COUNT(*) FROM sales_trends
UNION ALL
SELECT 'customer_segments', COUNT(*) FROM customer_segments;
"
```

### Level 3: End-to-End Testing

#### Test 3.1: Full Pipeline (All Jobs)

```bash
# Run complete pipeline
python batch_processing/main.py --jobs all

# Check job tracker for success
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT job_name, last_run_status, rows_processed, execution_duration_sec
FROM batch_job_tracker
ORDER BY last_run_timestamp DESC;
"

# All jobs should show 'SUCCESS'
```

#### Test 3.2: Incremental Loading

```bash
# First run
python batch_processing/main.py --jobs fact_sales

# Wait for new streaming data (or manually insert test data)
sleep 60

# Second run (should only process new data)
python batch_processing/main.py --jobs fact_sales

# Check job tracker timestamps
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT job_name, last_run_timestamp, rows_processed
FROM batch_job_tracker
WHERE job_name = 'fact_sales';
"
```

#### Test 3.3: SCD Type 2 Change Detection

```bash
# Initial load
python batch_processing/main.py --jobs dim_customers

# Simulate customer behavior change (run RFM to update segments)
python batch_processing/main.py --jobs rfm_analysis

# Reload customer dimension (should detect changes)
python batch_processing/main.py --jobs dim_customers

# Check for historical records
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT user_id, customer_segment, effective_from, effective_to, is_current
FROM dim_customers
WHERE user_id IN (
    SELECT user_id FROM dim_customers GROUP BY user_id HAVING COUNT(*) > 1
)
ORDER BY user_id, effective_from;
"

# Should see multiple versions with different effective dates
```

### Level 4: Error Handling & Edge Cases

#### Test 4.1: Dry Run

```bash
# Validate without executing
python batch_processing/main.py --jobs all --dry-run

# Should complete quickly without modifying database
```

#### Test 4.2: Empty Source Data

```bash
# Stop streaming pipeline
# Run batch processing (should handle gracefully)
python batch_processing/main.py --jobs fact_sales

# Check logs for warning messages about empty data
tail -f logs/batch/fact_sales.log
```

#### Test 4.3: Full Reload

```bash
# Force full reload (ignore incremental)
python batch_processing/main.py --jobs fact_sales --full-reload

# Should process all historical data
```

#### Test 4.4: Dependency Validation

```bash
# Try running analytics without facts (should fail with clear error)
# First truncate facts
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "TRUNCATE TABLE fact_sales;"

# Run RFM (depends on fact_sales)
python batch_processing/main.py --jobs rfm_analysis

# Should handle gracefully with empty results
```

### Level 5: Performance Testing

#### Test 5.1: Execution Time Benchmarks

```bash
# Measure baseline performance
time python batch_processing/main.py --jobs all

# Check individual job times
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -c "
SELECT job_name, execution_duration_sec
FROM batch_job_tracker
ORDER BY execution_duration_sec DESC;
"
```

#### Test 5.2: Large Dataset Handling

```bash
# Run with more Spark memory for larger datasets
export SPARK_MEMORY=8g
python batch_processing/main.py --jobs all

# Monitor Spark UI at http://localhost:4040
```

### Level 6: Cron & Scheduling

#### Test 6.1: Manual Cron Script

```bash
# Test cron script manually
chmod +x batch_processing/scheduler/run_batch_jobs.sh
./batch_processing/scheduler/run_batch_jobs.sh --dry-run

# Full test run
./batch_processing/scheduler/run_batch_jobs.sh

# Check logs
tail -f /var/log/globalmart/batch_*.log
```

#### Test 6.2: Cron Installation

```bash
# Install cron job
chmod +x batch_processing/scheduler/setup_cron.sh
./batch_processing/scheduler/setup_cron.sh

# Verify installation
crontab -l | grep run_batch_jobs

# Check cron service
service cron status
```

## Validation Queries

### Data Quality Checks

```sql
-- 1. Check for null dimension keys in facts
SELECT
    'fact_sales' as table_name,
    COUNT(*) as total,
    SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) as null_customer_sk,
    SUM(CASE WHEN product_sk IS NULL THEN 1 ELSE 0 END) as null_product_sk
FROM fact_sales;

-- 2. Check for orphaned records (fact records without matching dimension)
SELECT COUNT(*)
FROM fact_sales fs
LEFT JOIN dim_customers dc ON fs.customer_sk = dc.customer_sk
WHERE dc.customer_sk IS NULL;

-- 3. Check SCD Type 2 integrity (no overlapping effective dates)
SELECT user_id, COUNT(*) as versions
FROM dim_customers
WHERE is_current = TRUE
GROUP BY user_id
HAVING COUNT(*) > 1;
-- Should return 0 rows

-- 4. Check for duplicate business keys in current dimensions
SELECT product_id, COUNT(*)
FROM dim_products
WHERE is_current = TRUE
GROUP BY product_id
HAVING COUNT(*) > 1;
-- Should return 0 rows

-- 5. Check RFM score distribution (should be 1-5)
SELECT
    MIN(recency_score) as min_r,
    MAX(recency_score) as max_r,
    MIN(frequency_score) as min_f,
    MAX(frequency_score) as max_f,
    MIN(monetary_score) as min_m,
    MAX(monetary_score) as max_m
FROM rfm_analysis;
-- All min should be >=1, all max should be <=5
```

## Expected Results

### Dimension Counts

- `dim_date`: ~1,095 rows (3 years)
- `dim_geography`: 5 rows (5 countries)
- `dim_customers`: Varies (depends on unique users in streaming data)
- `dim_products`: Varies (depends on unique products)

### Fact Counts

Depends on streaming data volume:
- `fact_sales`: Hundreds to thousands per run
- `fact_cart_events`: Similar to sales
- `fact_product_views`: Higher than sales (browsing > purchases)

### Analytics Counts

- `rfm_analysis`: One record per unique customer
- `product_performance`: One record per unique product
- `sales_trends`: Multiple records per time period/country/category
- `customer_segments`: One record per segment (6 RFM + 5 geographic = 11 total)

## Troubleshooting Test Failures

### Issue: "No data to load"

**Cause**: Streaming pipeline not generating data
**Fix**: Start data generation and stream processing (Phase 2)

### Issue: "Null dimension keys in facts"

**Cause**: Dimensions not loaded before facts
**Fix**: Run `--jobs dimensions` before `--jobs facts`

### Issue: "Spark out of memory"

**Cause**: Insufficient memory allocation
**Fix**: Increase `SPARK_MEMORY` environment variable

### Issue: "Database connection timeout"

**Cause**: PostgreSQL not running or wrong port
**Fix**: Check PostgreSQL service and connection settings

## Success Criteria

Phase 3 is complete when:

- ✅ All 11 jobs execute successfully
- ✅ All dimension tables populated with valid data
- ✅ All fact tables contain transactions with proper dimension keys
- ✅ All analytics tables show meaningful results
- ✅ SCD Type 2 correctly tracks customer/product history
- ✅ Incremental loading only processes new data
- ✅ Cron job installed and scheduled correctly
- ✅ Job tracker shows all SUCCESS statuses
- ✅ Data quality checks pass
- ✅ No errors in logs

---

**Test Status**: Ready for Execution
**Last Updated**: 2024-12-06
