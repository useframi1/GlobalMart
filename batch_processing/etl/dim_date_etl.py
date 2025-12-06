"""
Date Dimension ETL
Pre-populates date dimension with calendar attributes
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, year, quarter, month, dayofmonth, dayofweek, weekofyear,
    date_format, when, lit, to_date
)
from datetime import datetime, timedelta
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import DimensionETL


class DateDimensionETL(DimensionETL):
    """ETL for date dimension - pre-populate with date range"""

    def __init__(self, spark, start_date: str = "2024-01-01", end_date: str = "2026-12-31"):
        super().__init__(spark, "dim_date", use_scd2=False)
        self.start_date = start_date
        self.end_date = end_date

    def extract(self) -> DataFrame:
        """Generate date range"""
        self.logger.info(f"Generating dates from {self.start_date} to {self.end_date}")

        # Generate date range using Python
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")

        date_list = []
        current_date = start
        while current_date <= end:
            date_list.append((current_date.strftime("%Y-%m-%d"),))
            current_date += timedelta(days=1)

        # Create DataFrame
        df = self.spark.createDataFrame(date_list, ["date"])
        df = df.withColumn("date", to_date(col("date")))

        self.stats["rows_processed"] = df.count()
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Add calendar attributes"""
        self.logger.info("Transforming date dimension")

        # US holidays (simplified - add more as needed)
        holidays = [
            "2024-01-01", "2024-07-04", "2024-12-25",
            "2025-01-01", "2025-07-04", "2025-12-25",
            "2026-01-01", "2026-07-04", "2026-12-25"
        ]

        transformed = (df
            .withColumn("year", year(col("date")))
            .withColumn("quarter", quarter(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("week", weekofyear(col("date")))  # Use weekofyear function instead of date_format
            .withColumn("day", dayofmonth(col("date")))
            .withColumn("day_of_week", dayofweek(col("date")))
            .withColumn("day_name", date_format(col("date"), "EEEE"))
            .withColumn("month_name", date_format(col("date"), "MMMM"))
            .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True).otherwise(False))
            .withColumn("is_holiday", when(col("date").cast("string").isin(holidays), True).otherwise(False))
        )

        return transformed

    def load(self, df: DataFrame) -> None:
        """Load to warehouse (upsert logic)"""
        self.logger.info("Loading date dimension")

        # Check if table has data
        try:
            existing_df = (self.spark.read
                          .format("jdbc")
                          .option("url", self.warehouse_jdbc_url)
                          .option("dbtable", "dim_date")
                          .option("user", self.warehouse_jdbc_props["user"])
                          .option("password", self.warehouse_jdbc_props["password"])
                          .option("driver", self.warehouse_jdbc_props["driver"])
                          .load())

            # Get existing dates
            existing_dates = [row.date for row in existing_df.select("date").collect()]

            # Filter only new dates
            new_dates_df = df.filter(~col("date").isin(existing_dates))

            insert_count = new_dates_df.count()

            if insert_count > 0:
                (new_dates_df.write
                 .format("jdbc")
                 .option("url", self.warehouse_jdbc_url)
                 .option("dbtable", "dim_date")
                 .option("user", self.warehouse_jdbc_props["user"])
                 .option("password", self.warehouse_jdbc_props["password"])
                 .option("driver", self.warehouse_jdbc_props["driver"])
                 .mode("append")
                 .save())

                self.stats["rows_inserted"] = insert_count
                self.logger.info(f"Inserted {insert_count} new dates")
            else:
                self.logger.info("No new dates to insert")

        except Exception:
            # Table is empty - load all
            (df.write
             .format("jdbc")
             .option("url", self.warehouse_jdbc_url)
             .option("dbtable", "dim_date")
             .option("user", self.warehouse_jdbc_props["user"])
             .option("password", self.warehouse_jdbc_props["password"])
             .option("driver", self.warehouse_jdbc_props["driver"])
             .mode("append")
             .save())

            self.stats["rows_inserted"] = df.count()
            self.logger.info(f"Inserted {self.stats['rows_inserted']} dates")
