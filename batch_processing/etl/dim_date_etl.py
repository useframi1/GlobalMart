"""
Dimension Date ETL
Populates dim_date table with date dimension data
"""
import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, dayofweek, dayofmonth, dayofyear, weekofyear,
    month, quarter, year, date_format, when, lit
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import BaseETL


class DimDateETL(BaseETL):
    """ETL job for populating dim_date dimension table"""

    def __init__(self, start_date: str = "2024-01-01", end_date: str = "2026-12-31"):
        """
        Initialize DimDateETL

        Args:
            start_date: Start date for dimension (YYYY-MM-DD)
            end_date: End date for dimension (YYYY-MM-DD)
        """
        super().__init__("DimDate")
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    def extract(self) -> DataFrame:
        """
        Generate date range data

        Returns:
            DataFrame: Date range data
        """
        # Generate date range as list of tuples
        date_list = []
        current_date = self.start_date

        while current_date <= self.end_date:
            date_list.append((current_date,))
            current_date += timedelta(days=1)

        # Create DataFrame using createDataFrame (works with Spark 3.4.0)
        df = self.spark.createDataFrame(date_list, ["date"])

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform date data into dimension attributes

        Args:
            df: Input DataFrame with date column

        Returns:
            DataFrame: Transformed date dimension
        """
        # Add date dimension attributes
        transformed_df = df.select(
            col("date"),
            year(col("date")).alias("year"),
            quarter(col("date")).alias("quarter"),
            month(col("date")).alias("month"),
            weekofyear(col("date")).alias("week"),
            dayofmonth(col("date")).alias("day"),
            dayofweek(col("date")).alias("day_of_week"),
            date_format(col("date"), "EEEE").alias("day_name"),
            date_format(col("date"), "MMMM").alias("month_name"),
            # Weekend check (1=Sunday, 7=Saturday in Spark)
            when(dayofweek(col("date")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
            lit(False).alias("is_holiday")  # Can be updated later with actual holidays
        )

        return transformed_df

    def load(self, df: DataFrame) -> None:
        """
        Load date dimension to warehouse

        Args:
            df: Transformed DataFrame
        """
        # Write to warehouse (overwrite to ensure latest data)
        self.write_to_warehouse(df, "dim_date", mode="overwrite")
        print(f"  Loaded {df.count()} date records to dim_date")


if __name__ == "__main__":
    # Run DimDate ETL
    etl = DimDateETL(start_date="2024-01-01", end_date="2026-12-31")
    etl.run()
