"""
Base ETL Classes
Abstract base classes for all ETL operations
"""
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.warehouse.connection import db_connection
from batch_processing.warehouse.incremental_tracker import tracker
from batch_processing.utils.logging_config import get_logger


class BaseETL(ABC):
    """Abstract base class for ETL operations"""

    def __init__(self, spark: SparkSession, job_name: str):
        self.spark = spark
        self.job_name = job_name
        self.logger = get_logger(job_name)
        self.connection = db_connection
        self.tracker = tracker

        # JDBC configurations
        self.realtime_jdbc_url = self.connection.get_realtime_jdbc_url()
        self.warehouse_jdbc_url = self.connection.get_warehouse_jdbc_url()
        self.realtime_jdbc_props = self.connection.get_realtime_jdbc_properties()
        self.warehouse_jdbc_props = self.connection.get_warehouse_jdbc_properties()

        self.start_time = None
        self.stats = {
            "rows_processed": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "execution_duration_sec": 0
        }

    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract data from source

        Returns:
            DataFrame: Extracted data
        """
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform extracted data

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: Transformed data
        """
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """
        Load transformed data to warehouse

        Args:
            df: Transformed DataFrame
        """
        pass

    def run(self) -> dict:
        """
        Execute ETL pipeline with error handling and tracking

        Returns:
            dict: Execution statistics
        """
        self.start_time = datetime.utcnow()

        try:
            self.logger.info(f"Starting ETL job: {self.job_name}")
            self.tracker.start_job(self.job_name)

            # ETL Steps
            self.logger.info("Step 1: Extract")
            extracted_df = self.extract()

            self.logger.info("Step 2: Transform")
            transformed_df = self.transform(extracted_df)

            self.logger.info("Step 3: Load")
            self.load(transformed_df)

            # Calculate duration
            end_time = datetime.utcnow()
            self.stats["execution_duration_sec"] = int((end_time - self.start_time).total_seconds())

            # Mark as complete
            self.tracker.complete_job(self.job_name, self.stats)

            self.logger.info(f"ETL job completed: {self.job_name}")
            self.logger.info(f"Statistics: {self.stats}")

            return self.stats

        except Exception as e:
            self.logger.error(f"ETL job failed: {self.job_name}")
            self.logger.error(f"Error: {str(e)}", exc_info=True)
            self.tracker.fail_job(self.job_name, str(e))
            raise


class DimensionETL(BaseETL):
    """Base class for dimension table ETL with SCD Type 2 support"""

    def __init__(self, spark: SparkSession, job_name: str, use_scd2: bool = False):
        super().__init__(spark, job_name)
        self.use_scd2 = use_scd2


class FactETL(BaseETL):
    """Base class for fact table ETL with incremental loading"""

    def __init__(self, spark: SparkSession, job_name: str):
        super().__init__(spark, job_name)

    def get_last_load_timestamp(self) -> datetime:
        """Get last successful load timestamp for incremental processing"""
        return self.tracker.get_last_run_timestamp(self.job_name)
