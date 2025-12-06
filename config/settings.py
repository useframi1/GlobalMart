"""
Centralized configuration management for GlobalMart.
Loads configuration from environment variables using .env file.
"""
import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent


class KafkaConfig:
    """Kafka configuration settings"""

    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.zookeeper_connect = os.getenv("KAFKA_ZOOKEEPER_CONNECT", "localhost:2181")
        self.topic_transactions = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions-topic")
        self.topic_cart_events = os.getenv("KAFKA_TOPIC_CART_EVENTS", "cart-events-topic")
        self.topic_product_views = os.getenv("KAFKA_TOPIC_PRODUCT_VIEWS", "product-views-topic")
        self.topic_alerts = os.getenv("KAFKA_TOPIC_ALERTS", "alerts-topic")
        self.num_partitions = int(os.getenv("KAFKA_NUM_PARTITIONS", "3"))
        self.replication_factor = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
        self.auto_create_topics = os.getenv("KAFKA_AUTO_CREATE_TOPICS", "true").lower() == "true"


class SparkConfig:
    """Spark configuration settings"""

    def __init__(self):
        self.master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
        self.app_name = os.getenv("SPARK_APP_NAME", "GlobalMart")
        self.driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "2g")
        self.executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
        self.executor_cores = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))
        self.log_level = os.getenv("SPARK_LOG_LEVEL", "WARN")
        self.streaming_batch_interval = int(os.getenv("SPARK_STREAMING_BATCH_INTERVAL", "10"))
        self.checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "./data/checkpoints")


class PostgresWarehouseConfig:
    """PostgreSQL Data Warehouse configuration settings (for batch processing)"""

    def __init__(self):
        self.host = os.getenv("POSTGRES_WAREHOUSE_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_WAREHOUSE_PORT", "5432"))
        self.database = os.getenv("POSTGRES_WAREHOUSE_DB", "globalmart_warehouse")
        self.user = os.getenv("POSTGRES_WAREHOUSE_USER", "globalmart_user")
        self.password = os.getenv("POSTGRES_WAREHOUSE_PASSWORD", "")

    @property
    def connection_string(self) -> str:
        """Returns PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_url(self) -> str:
        """Returns JDBC URL for Spark"""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


class PostgresRealtimeConfig:
    """PostgreSQL Real-Time Analytics configuration settings (for stream processing)"""

    def __init__(self):
        self.host = os.getenv("POSTGRES_REALTIME_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_REALTIME_PORT", "5432"))
        self.database = os.getenv("POSTGRES_REALTIME_DB", "globalmart_realtime")
        self.user = os.getenv("POSTGRES_REALTIME_USER", "globalmart_user")
        self.password = os.getenv("POSTGRES_REALTIME_PASSWORD", "")

    @property
    def connection_string(self) -> str:
        """Returns PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_url(self) -> str:
        """Returns JDBC URL for Spark"""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


class MongoConfig:
    """MongoDB configuration settings"""

    def __init__(self):
        self.host = os.getenv("MONGO_HOST", "localhost")
        self.port = int(os.getenv("MONGO_PORT", "27017"))
        self.database = os.getenv("MONGO_DB", "globalmart_realtime")
        self.user = os.getenv("MONGO_USER", "globalmart_user")
        self.password = os.getenv("MONGO_PASSWORD", "")
        self.auth_source = os.getenv("MONGO_AUTH_SOURCE", "admin")

    @property
    def connection_string(self) -> str:
        """Returns MongoDB connection string"""
        if self.user and self.password:
            return f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?authSource={self.auth_source}"
        return f"mongodb://{self.host}:{self.port}/{self.database}"


class RedisConfig:
    """Redis configuration settings"""

    def __init__(self):
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", "6379"))
        self.password = os.getenv("REDIS_PASSWORD", "")
        self.db = int(os.getenv("REDIS_DB", "0"))
        self.ttl = int(os.getenv("REDIS_TTL", "300"))


class APIConfig:
    """API configuration settings"""

    def __init__(self):
        self.host = os.getenv("API_HOST", "0.0.0.0")
        self.port = int(os.getenv("API_PORT", "8000"))
        self.workers = int(os.getenv("API_WORKERS", "4"))
        self.debug = os.getenv("API_DEBUG", "false").lower() == "true"
        self.secret_key = os.getenv("API_SECRET_KEY", "default-secret-key-change-me")
        self.allowed_origins = os.getenv("API_ALLOWED_ORIGINS", "").split(",")
        self.key_header = os.getenv("API_KEY_HEADER", "X-API-Key")
        self.api_keys = os.getenv("API_KEYS", "").split(",")


class DataGenerationConfig:
    """Data generation configuration settings"""

    def __init__(self):
        self.events_per_second = int(os.getenv("DATA_GEN_EVENTS_PER_SECOND", "50"))
        self.num_users = int(os.getenv("DATA_GEN_NUM_USERS", "10000"))
        self.num_products = int(os.getenv("DATA_GEN_NUM_PRODUCTS", "1000"))
        self.num_categories = int(os.getenv("DATA_GEN_NUM_CATEGORIES", "100"))
        self.num_countries = int(os.getenv("DATA_GEN_NUM_COUNTRIES", "5"))
        self.enable_validation = os.getenv("DATA_GEN_ENABLE_VALIDATION", "true").lower() == "true"


class MonitoringConfig:
    """Monitoring configuration settings"""

    def __init__(self):
        self.prometheus_port = int(os.getenv("PROMETHEUS_PORT", "9090"))
        self.prometheus_scrape_interval = os.getenv("PROMETHEUS_SCRAPE_INTERVAL", "15s")
        self.grafana_port = int(os.getenv("GRAFANA_PORT", "3000"))
        self.grafana_admin_user = os.getenv("GRAFANA_ADMIN_USER", "admin")
        self.grafana_admin_password = os.getenv("GRAFANA_ADMIN_PASSWORD", "admin")


class AlertConfig:
    """Alerting configuration settings"""

    def __init__(self):
        self.email_enabled = os.getenv("ALERT_EMAIL_ENABLED", "false").lower() == "true"
        self.email_smtp_host = os.getenv("ALERT_EMAIL_SMTP_HOST", "smtp.gmail.com")
        self.email_smtp_port = int(os.getenv("ALERT_EMAIL_SMTP_PORT", "587"))
        self.email_from = os.getenv("ALERT_EMAIL_FROM", "")
        self.email_to = os.getenv("ALERT_EMAIL_TO", "")
        self.email_password = os.getenv("ALERT_EMAIL_PASSWORD", "")
        self.slack_enabled = os.getenv("ALERT_SLACK_ENABLED", "false").lower() == "true"
        self.slack_webhook_url = os.getenv("ALERT_SLACK_WEBHOOK_URL", "")


class LoggingConfig:
    """Logging configuration settings"""

    def __init__(self):
        self.level = os.getenv("LOG_LEVEL", "INFO")
        self.format = os.getenv("LOG_FORMAT", "json")
        self.dir = os.getenv("LOG_DIR", "./logs")


class BusinessRulesConfig:
    """Business rules configuration"""

    def __init__(self):
        self.low_stock_threshold = int(os.getenv("INVENTORY_LOW_STOCK_THRESHOLD", "10"))
        self.anomaly_z_score_threshold = float(os.getenv("ANOMALY_DETECTION_Z_SCORE_THRESHOLD", "3.0"))
        self.session_timeout_minutes = int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))
        self.cart_abandonment_timeout_minutes = int(os.getenv("CART_ABANDONMENT_TIMEOUT_MINUTES", "60"))


class BatchProcessingConfig:
    """Batch processing configuration settings"""

    def __init__(self):
        # Logging configuration
        self.log_dir = PROJECT_ROOT / "logs" / "batch"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_level = os.getenv("BATCH_LOG_LEVEL", "INFO")
        self.log_retention_days = int(os.getenv("BATCH_LOG_RETENTION_DAYS", "30"))
        
        # Spark configuration for batch processing
        self.spark_app_name = os.getenv("BATCH_SPARK_APP_NAME", "GlobalMart-BatchProcessing")
        self.spark_master = os.getenv("BATCH_SPARK_MASTER", "local[*]")
        self.spark_memory = os.getenv("BATCH_SPARK_MEMORY", "4g")
        self.spark_cores = os.getenv("BATCH_SPARK_CORES", "4")
        
        # Job execution configuration
        self.max_retries = int(os.getenv("BATCH_MAX_RETRIES", "3"))
        self.retry_delay_seconds = int(os.getenv("BATCH_RETRY_DELAY_SECONDS", "60"))
        self.job_timeout_seconds = int(os.getenv("BATCH_JOB_TIMEOUT_SECONDS", "3600"))
        
        # Scheduling
        self.cron_schedule = os.getenv("BATCH_CRON_SCHEDULE", "0 2 * * *")
        self.cron_schedule_description = os.getenv("BATCH_CRON_SCHEDULE_DESCRIPTION", "Daily at 2:00 AM UTC")
        
        # Job dependencies (defines execution order)
        self.job_dependencies = {
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
        self.job_groups = {
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
        
        # Convenience attributes (uppercase for backward compatibility)
        self.PROJECT_ROOT = PROJECT_ROOT
        self.LOG_DIR = self.log_dir
        self.LOG_LEVEL = self.log_level
        self.LOG_RETENTION_DAYS = self.log_retention_days
        self.SPARK_APP_NAME = self.spark_app_name
        self.SPARK_MASTER = self.spark_master
        self.SPARK_MEMORY = self.spark_memory
        self.SPARK_CORES = self.spark_cores
        self.MAX_RETRIES = self.max_retries
        self.RETRY_DELAY_SECONDS = self.retry_delay_seconds
        self.JOB_TIMEOUT_SECONDS = self.job_timeout_seconds
        self.CRON_SCHEDULE = self.cron_schedule
        self.CRON_SCHEDULE_DESCRIPTION = self.cron_schedule_description
        self.JOB_DEPENDENCIES = self.job_dependencies
        self.JOB_GROUPS = self.job_groups


class Settings:
    """Main settings class that aggregates all configuration"""

    def __init__(self):
        self.kafka = KafkaConfig()
        self.spark = SparkConfig()
        self.postgres_warehouse = PostgresWarehouseConfig()
        self.postgres_realtime = PostgresRealtimeConfig()
        self.postgres = self.postgres_warehouse  # Backward compatibility
        self.mongo = MongoConfig()
        self.redis = RedisConfig()
        self.api = APIConfig()
        self.data_generation = DataGenerationConfig()
        self.monitoring = MonitoringConfig()
        self.alert = AlertConfig()
        self.logging = LoggingConfig()
        self.business_rules = BusinessRulesConfig()
        self.batch = BatchProcessingConfig()


# Global settings instance
settings = Settings()