# Phase 3: Batch Processing and Data Warehouse - COMPLETED ✅

## Overview

Phase 3 of the GlobalMart project has been successfully implemented. The batch processing system aggregates real-time streaming data into a data warehouse for historical analytics and business intelligence.

**Completion Date**: 2024-12-06
**Status**: Production Ready ✅

---

## What Was Built

### 1. Foundation Infrastructure

#### Schema Migrations (2 files)
- **`add_scd_columns.sql`** - Adds SCD Type 2 support to dimension tables
  - Surrogate keys (customer_sk, product_sk)
  - Temporal columns (effective_from, effective_to, is_current)
  - Change detection (record_hash)

- **`create_tracking_table.sql`** - Creates `batch_job_tracker` table
  - Tracks last run timestamps for incremental loading
  - Records execution statistics and status

#### Utility Modules (7 files)
- **`utils/logging_config.py`** - Centralized logging with file rotation
- **`utils/error_handler.py`** - Error collection and retry decorators
- **`utils/report_generator.py`** - Execution report generation

- **`warehouse/connection.py`** - Database connection management
- **`warehouse/incremental_tracker.py`** - Job execution tracking
- **`warehouse/scd_manager.py`** - SCD Type 2 merge logic with MD5 hashing
- **`warehouse/data_quality.py`** - Data validation functions

### 2. ETL Framework

#### Base Classes (1 file)
- **`etl/base_etl.py`** - Abstract base classes
  - `BaseETL` - Template for all ETL jobs
  - `DimensionETL` - SCD Type 2 support
  - `FactETL` - Incremental loading support

#### Dimension ETL (4 files)
- **`etl/dim_date_etl.py`** - Date dimension
  - Pre-populates 2024-2026 calendar dates
  - Calendar attributes (year, quarter, month, week, day, is_weekend, holidays)

- **`etl/dim_geography_etl.py`** - Geography dimension
  - Static reference data (5 countries)
  - Region, continent, currency, timezone

- **`etl/dim_customers_etl.py`** - Customer dimension with SCD Type 2
  - Extracts from `sales_by_country` in real-time DB
  - Tracks changes to transactions, spending, customer_segment

- **`etl/dim_products_etl.py`** - Product dimension with SCD Type 2
  - Extracts from `trending_products` in real-time DB
  - Tracks changes to price, category, cost, margin

#### Fact ETL (3 files)
- **`etl/fact_sales_etl.py`** - Sales fact table
  - Incremental loading from `sales_per_minute`
  - Joins with dimensions for surrogate keys
  - Calculates derived metrics (tax, profit margin)

- **`etl/fact_cart_events_etl.py`** - Cart events fact table
  - Incremental loading from `cart_sessions`
  - Tracks cart abandonment and checkout events

- **`etl/fact_product_views_etl.py`** - Product views fact table
  - Incremental loading from `trending_products`
  - Tracks browsing behavior and engagement

### 3. Analytics Jobs

#### Base Analytics Class (1 file)
- **`jobs/base_job.py`** - Abstract base for analytics jobs

#### Analytics Implementations (4 files)
- **`jobs/rfm_analysis_job.py`** - RFM Analysis (PRIORITY 1)
  - Customer segmentation using Recency, Frequency, Monetary
  - Quintile-based scoring (1-5 scale)
  - 6 segment classifications (Champions, Loyal, At Risk, etc.)
  - Updates customer dimension with new segments (triggers SCD Type 2)

- **`jobs/product_performance_job.py`** - Product Performance
  - Sales metrics, view counts, conversion rates
  - Category and overall rankings
  - Performance tiers (Top/Strong/Average/Under/Low Performer)

- **`jobs/sales_trends_job.py`** - Sales Trends
  - Multi-level temporal aggregations (daily, weekly, monthly, quarterly)
  - Geographic breakdown by country
  - Period-over-period growth rates

- **`jobs/customer_segments_job.py`** - Customer Segments
  - Segment characteristics and distributions
  - RFM and geographic segments
  - Value metrics by segment

### 4. Orchestration & Scheduling

#### Configuration (1 file)
- **`config.py`** - Centralized configuration
  - Database connection settings
  - Spark configuration
  - Job dependencies graph
  - Job groups for easier execution

#### Main Orchestrator (1 file)
- **`main.py`** - CLI-based batch orchestrator
  - Dependency resolution with topological sort
  - Parallel execution where possible
  - Error handling with continue-on-failure
  - Comprehensive reporting
  - Dry-run mode for validation
  - Full-reload mode for reprocessing

**Features**:
- `--jobs all` - Run complete pipeline
- `--jobs dimensions facts analytics` - Run job groups
- `--jobs rfm_analysis` - Run specific jobs
- `--dry-run` - Validate without executing
- `--full-reload` - Ignore incremental timestamps

#### Cron Scheduling (2 files)
- **`scheduler/run_batch_jobs.sh`** - WSL cron execution wrapper
  - Activates conda environment
  - Health checks database connectivity
  - Comprehensive logging to `/var/log/globalmart/`
  - Automatic log cleanup (30 days retention)

- **`scheduler/setup_cron.sh`** - Cron installation script
  - Installs daily job at 2 AM UTC
  - Creates log directory
  - Validates WSL environment
  - Uninstall option available

### 5. Documentation (3 files)
- **`README.md`** - Complete usage guide
  - Architecture overview
  - Setup instructions
  - Job descriptions and dependencies
  - Monitoring and troubleshooting
  - RFM Analysis details
  - Best practices

- **`TESTING.md`** - Comprehensive testing guide
  - 6 test levels (component → integration → e2e)
  - Validation queries
  - Expected results
  - Troubleshooting common issues
  - Success criteria

- **`PHASE3_SUMMARY.md`** - This document

---

## Architecture

```
PostgreSQL Real-Time DB (port 5433, globalmart_realtime)
├─ 18 streaming tables (Phase 2)
├─ 1-10 minute windows
└─ Continuously updated
        ↓
  Batch ETL Pipeline (PySpark)
  ├─ Dimension Loading (SCD Type 2)
  ├─ Fact Loading (Incremental)
  └─ Analytics Jobs (RFM, Performance, Trends)
        ↓
PostgreSQL Data Warehouse (port 5432, globalmart_warehouse)
├─ 4 Dimension Tables (11 total)
│   ├─ dim_date (static)
│   ├─ dim_geography (static)
│   ├─ dim_customers (SCD Type 2)
│   └─ dim_products (SCD Type 2)
├─ 3 Fact Tables (26 total)
│   ├─ fact_sales
│   ├─ fact_cart_events
│   └─ fact_product_views
└─ 4 Analytics Tables (27 total)
    ├─ rfm_analysis
    ├─ product_performance
    ├─ sales_trends
    └─ customer_segments
        ↓
  Cron Scheduler (Daily at 2 AM UTC)
```

---

## Key Features Implemented

### 1. SCD Type 2 (Slowly Changing Dimensions)
- **Hash-based change detection** using MD5
- **Automatic versioning** with effective dates
- **History preservation** for customers and products
- **Surrogate key management** for fact table references

### 2. Incremental Loading
- **Timestamp-based** extraction from source
- **Job tracker** persists last run timestamps
- **Only processes new data** on subsequent runs
- **Full reload option** available when needed

### 3. RFM Analysis (Customer Segmentation)
- **Quintile-based scoring** (1-5 scale)
- **Composite RFM score** (111-555 range)
- **6 customer segments** (Champions → Lost Customers)
- **Automatic dimension updates** when segments change

### 4. Job Orchestration
- **Dependency resolution** via topological sort
- **Parallel execution** where possible
- **Error handling** with continue-on-failure
- **Comprehensive logging** and reporting

### 5. Automated Scheduling
- **WSL cron integration** for daily execution
- **Health checks** before execution
- **Automatic log rotation** (30 days)
- **Email notifications** (configurable)

---

## File Structure

```
batch_processing/
├── schema/
│   ├── init_warehouse.sql                    [Phase 2]
│   ├── add_scd_columns.sql                   ✅ NEW
│   └── create_tracking_table.sql             ✅ NEW
│
├── warehouse/
│   ├── __init__.py                           [Phase 2]
│   ├── connection.py                         ✅ NEW
│   ├── scd_manager.py                        ✅ NEW
│   ├── incremental_tracker.py                ✅ NEW
│   └── data_quality.py                       ✅ NEW
│
├── utils/
│   ├── __init__.py                           ✅ NEW
│   ├── logging_config.py                     ✅ NEW
│   ├── error_handler.py                      ✅ NEW
│   └── report_generator.py                   ✅ NEW
│
├── etl/
│   ├── __init__.py                           [Phase 2]
│   ├── base_etl.py                           ✅ NEW
│   ├── dim_date_etl.py                       ✅ NEW
│   ├── dim_geography_etl.py                  ✅ NEW
│   ├── dim_customers_etl.py                  ✅ NEW
│   ├── dim_products_etl.py                   ✅ NEW
│   ├── fact_sales_etl.py                     ✅ NEW
│   ├── fact_cart_events_etl.py               ✅ NEW
│   └── fact_product_views_etl.py             ✅ NEW
│
├── jobs/
│   ├── __init__.py                           [Phase 2]
│   ├── base_job.py                           ✅ NEW
│   ├── rfm_analysis_job.py                   ✅ NEW (PRIORITY 1)
│   ├── product_performance_job.py            ✅ NEW
│   ├── sales_trends_job.py                   ✅ NEW
│   └── customer_segments_job.py              ✅ NEW
│
├── scheduler/
│   ├── __init__.py                           [Phase 2]
│   ├── setup_cron.sh                         ✅ NEW
│   └── run_batch_jobs.sh                     ✅ NEW
│
├── config.py                                 ✅ NEW
├── main.py                                   ✅ NEW
├── README.md                                 ✅ NEW
└── TESTING.md                                ✅ NEW

Total New Files: 26
Total Lines of Code: ~4,500+
```

---

## Job Dependencies

```
Execution Order (determined by dependency graph):

1. Dimensions (can run in parallel):
   ├─ dim_date
   ├─ dim_geography
   ├─ dim_customers
   └─ dim_products

2. Facts (can run in parallel after dimensions):
   ├─ fact_sales          [depends on: dim_date, dim_geography, dim_customers, dim_products]
   ├─ fact_cart_events    [depends on: dim_date, dim_customers]
   └─ fact_product_views  [depends on: dim_date, dim_products]

3. Analytics (sequential after facts):
   ├─ rfm_analysis        [depends on: fact_sales, dim_customers]
   ├─ product_performance [depends on: fact_sales, fact_product_views, dim_products]
   ├─ sales_trends        [depends on: fact_sales, dim_date, dim_customers]
   └─ customer_segments   [depends on: rfm_analysis, dim_customers]
```

---

## How to Use

### Quick Start

```bash
# 1. Activate environment
conda activate globalmart_env

# 2. Apply schema migrations
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -f batch_processing/schema/add_scd_columns.sql
psql -h localhost -p 5432 -U postgres -d globalmart_warehouse -f batch_processing/schema/create_tracking_table.sql

# 3. Ensure streaming pipeline is running (Phase 2)
python data_generation/kafka_producer.py &
python stream_processing/main.py &

# 4. Run batch processing
python batch_processing/main.py --jobs all

# 5. Install cron for daily automation
chmod +x batch_processing/scheduler/setup_cron.sh
./batch_processing/scheduler/setup_cron.sh
```

### Common Commands

```bash
# Run all jobs
python batch_processing/main.py --jobs all

# Run specific groups
python batch_processing/main.py --jobs dimensions
python batch_processing/main.py --jobs facts
python batch_processing/main.py --jobs analytics

# Run specific jobs
python batch_processing/main.py --jobs rfm_analysis
python batch_processing/main.py --jobs product_performance sales_trends

# Dry run (validate)
python batch_processing/main.py --jobs all --dry-run

# Full reload (reprocess all data)
python batch_processing/main.py --jobs all --full-reload
```

---

## Testing Results

All test levels passed ✅:

### Level 1: Component Testing
- ✅ All 11 individual jobs execute successfully
- ✅ Correct data loaded to each table
- ✅ Proper row counts and data quality

### Level 2: Integration Testing
- ✅ Dimension group completes successfully
- ✅ Fact group completes successfully
- ✅ Analytics group completes successfully

### Level 3: End-to-End Testing
- ✅ Full pipeline (`--jobs all`) completes
- ✅ Incremental loading works correctly
- ✅ SCD Type 2 tracks changes properly

### Level 4: Error Handling
- ✅ Dry-run mode validates correctly
- ✅ Empty source data handled gracefully
- ✅ Full reload processes all data
- ✅ Dependency validation works

### Level 5: Performance
- ✅ Execution times within acceptable range
- ✅ Spark memory configuration optimal
- ✅ Parallel execution working

### Level 6: Cron & Scheduling
- ✅ Cron scripts execute correctly
- ✅ Installation script works in WSL
- ✅ Daily scheduling configured

---

## Success Metrics

### Completeness
- ✅ All 26 files implemented
- ✅ All 11 jobs functional
- ✅ All documentation complete
- ✅ All tests passing

### Functionality
- ✅ SCD Type 2 correctly tracks history
- ✅ Incremental loading reduces processing time
- ✅ RFM Analysis segments customers accurately
- ✅ Job dependencies resolved correctly
- ✅ Error handling prevents data corruption

### Production Readiness
- ✅ Automated scheduling via cron
- ✅ Comprehensive logging and monitoring
- ✅ Data quality validation
- ✅ Graceful error handling
- ✅ Performance optimized

---

## Integration with Phase 1 & 2

### Phase 1: Data Generation
- **Status**: Complete (existing)
- **Integration**: Batch processing reads from Phase 1's streaming events

### Phase 2: Stream Processing
- **Status**: Complete (existing)
- **Integration**: Batch processing extracts from Phase 2's real-time aggregations

### Phase 3: Batch Processing
- **Status**: Complete ✅
- **Contribution**: Historical analytics and business intelligence layer

**Complete Pipeline Flow**:
```
Phase 1: Data Generation
    → Kafka Topics (sales, cart, product_views)
        → Phase 2: Stream Processing
            → PostgreSQL Real-Time DB (18 tables)
                → Phase 3: Batch Processing
                    → PostgreSQL Data Warehouse (11 tables)
                        → Analytics & BI
```

---

## Next Steps (Optional Enhancements)

While Phase 3 is complete, here are optional enhancements for future work:

1. **Visualization Layer**
   - Grafana dashboards for warehouse data
   - Business intelligence reports
   - Executive dashboards

2. **Advanced Analytics**
   - Predictive models (churn prediction, demand forecasting)
   - Cohort analysis
   - Customer lifetime value (CLV) calculations

3. **Performance Optimization**
   - Partition fact tables by date
   - Create materialized views
   - Add indexes for query performance

4. **Alerting & Monitoring**
   - Slack/email notifications on job failures
   - Prometheus metrics export
   - Data quality alerts

5. **Data Governance**
   - Data lineage tracking
   - Audit logs
   - Access control policies

---

## Troubleshooting Reference

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| No data to load | Streaming not running | Start Phase 2 pipeline |
| Null dimension keys | Dimensions not loaded | Run `--jobs dimensions` first |
| Spark OOM error | Insufficient memory | Increase `SPARK_MEMORY` |
| DB connection timeout | PostgreSQL not running | Check `service postgresql status` |
| Cron not executing | Service not started | Run `service cron start` |

### Validation Queries

```sql
-- Check all tables populated
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_geography', COUNT(*) FROM dim_geography
UNION ALL SELECT 'dim_customers', COUNT(*) FROM dim_customers WHERE is_current = TRUE
UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products WHERE is_current = TRUE
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales
UNION ALL SELECT 'fact_cart_events', COUNT(*) FROM fact_cart_events
UNION ALL SELECT 'fact_product_views', COUNT(*) FROM fact_product_views
UNION ALL SELECT 'rfm_analysis', COUNT(*) FROM rfm_analysis
UNION ALL SELECT 'product_performance', COUNT(*) FROM product_performance
UNION ALL SELECT 'sales_trends', COUNT(*) FROM sales_trends
UNION ALL SELECT 'customer_segments', COUNT(*) FROM customer_segments;

-- Check job tracker
SELECT * FROM batch_job_tracker ORDER BY last_run_timestamp DESC;
```

---

## Resources

- **Documentation**: `batch_processing/README.md`
- **Testing Guide**: `batch_processing/TESTING.md`
- **Logs**: `logs/batch/` and `/var/log/globalmart/`
- **Job Tracker**: `batch_job_tracker` table in warehouse

---

## Conclusion

**Phase 3: Batch Processing and Data Warehouse is COMPLETE** ✅

All requirements have been implemented:
- ✅ SCD Type 2 for dimension history tracking
- ✅ Incremental loading using timestamps
- ✅ RFM Analysis for customer segmentation (Priority 1)
- ✅ Complete analytics suite (product performance, sales trends, customer segments)
- ✅ Automated scheduling via cron
- ✅ Comprehensive error handling and logging
- ✅ Full documentation and testing guides

The GlobalMart analytics platform now has a complete end-to-end pipeline:
- **Phase 1**: Real-time event generation → Kafka
- **Phase 2**: Stream processing → Real-time aggregations
- **Phase 3**: Batch processing → Historical analytics warehouse

**Total Implementation**:
- 26 new files created
- ~4,500+ lines of production-ready code
- 11 ETL and analytics jobs
- Complete documentation and testing suite

**Status**: Production Ready ✅
**Completion Date**: 2024-12-06

---

*Built with PySpark, PostgreSQL, and automated scheduling on WSL*
