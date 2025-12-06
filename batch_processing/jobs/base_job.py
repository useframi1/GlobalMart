"""
Base Analytics Job Class
Abstract base class for all analytics jobs
"""
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.warehouse.connection import db_connection
from batch_processing.utils.logging_config import get_logger


class BaseAnalyticsJob(ABC):
    """Abstract base class for analytics jobs"""

    def __init__(self, spark: SparkSession, job_name: str):
        self.spark = spark
        self.job_name = job_name
        self.logger = get_logger(job_name)
        self.connection = db_connection

        # JDBC configurations
        self.warehouse_jdbc_url = self.connection.get_warehouse_jdbc_url()
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
        Extract data from warehouse for analysis

        Returns:
            DataFrame: Source data for analysis
        """
        pass

    @abstractmethod
    def analyze(self, df: DataFrame) -> DataFrame:
        """
        Perform analytics calculations

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: Analysis results
        """
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """
        Load analysis results to warehouse

        Args:
            df: Analysis results DataFrame
        """
        pass

    def run(self) -> dict:
        """
        Execute analytics job with error handling

        Returns:
            dict: Execution statistics
        """
        self.start_time = datetime.utcnow()

        try:
            self.logger.info(f"Starting analytics job: {self.job_name}")

            # Analytics Steps
            self.logger.info("Step 1: Extract data from warehouse")
            source_df = self.extract()

            self.logger.info("Step 2: Perform analysis")
            results_df = self.analyze(source_df)

            self.logger.info("Step 3: Load results")
            self.load(results_df)

            # Calculate duration
            end_time = datetime.utcnow()
            self.stats["execution_duration_sec"] = int((end_time - self.start_time).total_seconds())

            self.logger.info(f"Analytics job completed: {self.job_name}")
            self.logger.info(f"Statistics: {self.stats}")

            return self.stats

        except Exception as e:
            self.logger.error(f"Analytics job failed: {self.job_name}")
            self.logger.error(f"Error: {str(e)}", exc_info=True)
            raise
