"""
Base Batch Job Class
Provides common functionality for all batch analytics jobs
"""
import sys
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from batch_processing.warehouse.spark_batch import create_batch_spark_session


class BaseBatchJob(ABC):
    """Base class for all batch analytics jobs"""

    def __init__(self, job_name: str):
        """
        Initialize base batch job

        Args:
            job_name: Name of the batch job
        """
        self.job_name = job_name
        self.spark = create_batch_spark_session(f"GlobalMart-Batch-{job_name}")
        self.start_time = None
        self.end_time = None

    @abstractmethod
    def execute(self) -> None:
        """Execute the batch analytics job"""
        pass

    def read_from_warehouse(self, table_name: str, query: str = None) -> DataFrame:
        """
        Read data from warehouse PostgreSQL

        Args:
            table_name: Table name to read from
            query: Optional SQL query (overrides table_name)

        Returns:
            DataFrame: Data from warehouse
        """
        jdbc_options = {
            "url": settings.postgres_warehouse.jdbc_url,
            "user": settings.postgres_warehouse.user,
            "password": settings.postgres_warehouse.password,
            "driver": "org.postgresql.Driver"
        }

        if query:
            jdbc_options["query"] = query
        else:
            jdbc_options["dbtable"] = table_name

        df = self.spark.read.format("jdbc").options(**jdbc_options).load()

        return df

    def write_to_warehouse(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite"
    ) -> None:
        """
        Write DataFrame to warehouse PostgreSQL

        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode ('append', 'overwrite', 'ignore')
        """
        jdbc_url = settings.postgres_warehouse.jdbc_url
        properties = {
            "user": settings.postgres_warehouse.user,
            "password": settings.postgres_warehouse.password,
            "driver": "org.postgresql.Driver"
        }

        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=properties
        )

    def run(self) -> None:
        """Execute the complete batch job"""
        print("=" * 60)
        print(f"Batch Job: {self.job_name}")
        print("=" * 60)
        print(f"Started at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print()

        self.start_time = datetime.utcnow()

        try:
            # Execute job logic
            self.execute()

            self.end_time = datetime.utcnow()
            duration = (self.end_time - self.start_time).total_seconds()

            print()
            print("=" * 60)
            print(f"Batch Job Completed: {self.job_name}")
            print(f"Duration: {duration:.2f} seconds")
            print(f"Completed at: {self.end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print("=" * 60)
            print()

        except Exception as e:
            print(f"\n✗ Batch Job Failed: {self.job_name}")
            print(f"Error: {str(e)}")
            raise e

        finally:
            self.spark.stop()


if __name__ == "__main__":
    print("Base Batch Job class - Use this as a parent for specific analytics jobs")
