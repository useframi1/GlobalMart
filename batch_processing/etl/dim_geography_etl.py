"""
Geography Dimension ETL
Static dimension with country reference data
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.constants import COUNTRIES
from batch_processing.etl.base_etl import DimensionETL


class GeographyDimensionETL(DimensionETL):
    """ETL for geography dimension - static reference data"""

    def __init__(self, spark):
        super().__init__(spark, "dim_geography", use_scd2=False)

    def extract(self) -> DataFrame:
        """Create geography data from constants"""
        self.logger.info("Extracting geography data from constants")

        # Geography mapping
        geography_data = [
            ("USA", "North America", "Americas", "USD", "America/New_York"),
            ("UK", "Western Europe", "Europe", "GBP", "Europe/London"),
            ("Germany", "Western Europe", "Europe", "EUR", "Europe/Berlin"),
            ("France", "Western Europe", "Europe", "EUR", "Europe/Paris"),
            ("Japan", "East Asia", "Asia", "JPY", "Asia/Tokyo")
        ]

        df = self.spark.createDataFrame(
            geography_data,
            ["country", "region", "continent", "currency", "timezone"]
        )

        self.stats["rows_processed"] = df.count()
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """No transformation needed for static data"""
        self.logger.info("Transforming geography dimension")
        return df

    def load(self, df: DataFrame) -> None:
        """Load to warehouse (insert if not exists)"""
        self.logger.info("Loading geography dimension")

        try:
            # Check existing countries
            existing_df = (self.spark.read
                          .format("jdbc")
                          .option("url", self.warehouse_jdbc_url)
                          .option("dbtable", "dim_geography")
                          .option("user", self.warehouse_jdbc_props["user"])
                          .option("password", self.warehouse_jdbc_props["password"])
                          .option("driver", self.warehouse_jdbc_props["driver"])
                          .load())

            existing_countries = [row.country for row in existing_df.select("country").collect()]

            # Insert only new countries
            new_countries_df = df.filter(~df.country.isin(existing_countries))

            insert_count = new_countries_df.count()

            if insert_count > 0:
                (new_countries_df.write
                 .format("jdbc")
                 .option("url", self.warehouse_jdbc_url)
                 .option("dbtable", "dim_geography")
                 .option("user", self.warehouse_jdbc_props["user"])
                 .option("password", self.warehouse_jdbc_props["password"])
                 .option("driver", self.warehouse_jdbc_props["driver"])
                 .mode("append")
                 .save())

                self.stats["rows_inserted"] = insert_count
                self.logger.info(f"Inserted {insert_count} new geographies")
            else:
                self.logger.info("No new geographies to insert")

        except Exception:
            # Table is empty
            (df.write
             .format("jdbc")
             .option("url", self.warehouse_jdbc_url)
             .option("dbtable", "dim_geography")
             .option("user", self.warehouse_jdbc_props["user"])
             .option("password", self.warehouse_jdbc_props["password"])
             .option("driver", self.warehouse_jdbc_props["driver"])
             .mode("append")
             .save())

            self.stats["rows_inserted"] = df.count()
            self.logger.info(f"Inserted {self.stats['rows_inserted']} geographies")
