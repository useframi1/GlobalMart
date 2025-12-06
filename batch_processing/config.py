"""
Batch Processing Configuration
Centralized configuration for all batch jobs
"""
import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Logging configuration
LOG_DIR = PROJECT_ROOT / "logs" / "batch"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_LEVEL = os.getenv("BATCH_LOG_LEVEL", "INFO")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "30"))

# Spark configuration
SPARK_APP_NAME = "GlobalMart-BatchProcessing"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_MEMORY = os.getenv("SPARK_MEMORY", "4g")
SPARK_CORES = os.getenv("SPARK_CORES", "4")

# Database configuration (from environment or defaults)
# Real-time database (source)
REALTIME_DB_HOST = os.getenv("REALTIME_DB_HOST", "localhost")
REALTIME_DB_PORT = int(os.getenv("REALTIME_DB_PORT", "5433"))
REALTIME_DB_NAME = os.getenv("REALTIME_DB_NAME", "globalmart_realtime")
REALTIME_DB_USER = os.getenv("REALTIME_DB_USER", "postgres")
REALTIME_DB_PASSWORD = os.getenv("REALTIME_DB_PASSWORD", "postgres")

# Data warehouse database (target)
WAREHOUSE_DB_HOST = os.getenv("WAREHOUSE_DB_HOST", "localhost")
WAREHOUSE_DB_PORT = int(os.getenv("WAREHOUSE_DB_PORT", "5432"))
WAREHOUSE_DB_NAME = os.getenv("WAREHOUSE_DB_NAME", "globalmart_warehouse")
WAREHOUSE_DB_USER = os.getenv("WAREHOUSE_DB_USER", "postgres")
WAREHOUSE_DB_PASSWORD = os.getenv("WAREHOUSE_DB_PASSWORD", "postgres")

# Job execution configuration
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", "3600"))

# Job dependencies (defines execution order)
JOB_DEPENDENCIES = {
    # Dimension ETL jobs (no dependencies, can run in parallel)
    "dim_date": [],
    "dim_geography": [],
    "dim_customers": [],
    "dim_products": [],

    # Fact ETL jobs (depend on dimensions)
    "fact_sales": ["dim_date", "dim_geography", "dim_customers", "dim_products"],
    "fact_cart_events": ["dim_date", "dim_customers"],
    "fact_product_views": ["dim_date", "dim_products"],

    # Analytics jobs (depend on facts and dimensions)
    "rfm_analysis": ["fact_sales", "dim_customers"],
    "product_performance": ["fact_sales", "fact_product_views", "dim_products"],
    "sales_trends": ["fact_sales", "dim_date", "dim_customers"],
    "customer_segments": ["rfm_analysis", "dim_customers"],
}

# Job groups for easier execution
JOB_GROUPS = {
    "dimensions": ["dim_date", "dim_geography", "dim_customers", "dim_products"],
    "facts": ["fact_sales", "fact_cart_events", "fact_product_views"],
    "analytics": ["rfm_analysis", "product_performance", "sales_trends", "customer_segments"],
    "all": [
        # Dimensions first
        "dim_date", "dim_geography", "dim_customers", "dim_products",
        # Then facts
        "fact_sales", "fact_cart_events", "fact_product_views",
        # Finally analytics
        "rfm_analysis", "product_performance", "sales_trends", "customer_segments"
    ]
}

# Cron schedule (for documentation)
CRON_SCHEDULE = "0 2 * * *"  # Daily at 2 AM UTC
CRON_SCHEDULE_DESCRIPTION = "Daily at 2:00 AM UTC"
