"""
Dimension Geography ETL
Populates dim_geography table with country/region data
"""
import sys
import os
from pyspark.sql import DataFrame

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import BaseETL


class DimGeographyETL(BaseETL):
    """ETL job for populating dim_geography dimension table"""

    def __init__(self):
        super().__init__("DimGeography")

    def extract(self) -> DataFrame:
        """
        Extract geography data from constants

        Returns:
            DataFrame: Geography data
        """
        # Geography mapping (country, region, continent, currency, timezone)
        geography_data = [
            ('USA', 'North America', 'Americas', 'USD', 'America/New_York'),
            ('UK', 'Western Europe', 'Europe', 'GBP', 'Europe/London'),
            ('Germany', 'Western Europe', 'Europe', 'EUR', 'Europe/Berlin'),
            ('France', 'Western Europe', 'Europe', 'EUR', 'Europe/Paris'),
            ('Japan', 'East Asia', 'Asia', 'JPY', 'Asia/Tokyo')
        ]

        # Create DataFrame using createDataFrame (works with Spark 3.4.0)
        df = self.spark.createDataFrame(
            geography_data,
            ["country", "region", "continent", "currency", "timezone"]
        )

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform geography data with SCD Type 2 columns

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: Transformed DataFrame with SCD Type 2 columns
        """
        from pyspark.sql.functions import current_timestamp, lit

        # Add SCD Type 2 columns
        transformed_df = df.select(
            "*",
            current_timestamp().alias("effective_start_date"),
            lit("9999-12-31 23:59:59").cast("timestamp").alias("effective_end_date"),
            lit(True).alias("is_current")
        )

        return transformed_df

    def load(self, df: DataFrame) -> None:
        """
        Load geography dimension to warehouse using SCD Type 2

        Args:
            df: Transformed DataFrame
        """
        self.write_to_warehouse(
            df,
            "dim_geography",
            mode="append",
            scd_type2=True,
            natural_key="country"
        )
        print(f"  Loaded {df.count()} geography records to dim_geography")


if __name__ == "__main__":
    # Run DimGeography ETL
    etl = DimGeographyETL()
    etl.run()
