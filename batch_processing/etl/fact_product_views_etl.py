"""
Fact Product Views ETL
Populates fact_product_views table with product view events
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import BaseETL


class FactProductViewsETL(BaseETL):
    """ETL job for populating fact_product_views fact table"""

    def __init__(self):
        super().__init__("FactProductViews")

    def extract(self) -> DataFrame:
        """
        Extract product view events from real-time database

        Returns:
            DataFrame: Raw product view events
        """
        # Read from product_view_events table in realtime DB
        df = self.read_from_postgres(table_name="product_view_events", database="realtime")

        print(f"  Extracted {df.count()} product view events")

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform product view events to fact table format

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: Transformed fact records with dimension keys
        """
        # Read dimension tables for lookups - filter for current records only
        dim_customers = self.read_from_warehouse(table_name="dim_customers").filter(col("is_current") == True)
        dim_products = self.read_from_warehouse(table_name="dim_products").filter(col("is_current") == True)
        dim_geography = self.read_from_warehouse(table_name="dim_geography").filter(col("is_current") == True)
        dim_date = self.read_from_warehouse(table_name="dim_date")

        # Join with dimension tables to get surrogate keys
        transformed_df = df.alias("v") \
            .join(dim_customers.select("customer_id", "user_id").alias("c"),
                  col("v.user_id") == col("c.user_id"), "left") \
            .join(dim_products.select("product_id_pk", "product_id").alias("p"),
                  col("v.product_id") == col("p.product_id"), "left") \
            .join(dim_geography.select("geography_id", "country").alias("g"),
                  col("v.country") == col("g.country"), "left") \
            .join(dim_date.select("date_id", "date").alias("d"),
                  col("v.event_timestamp").cast("date") == col("d.date"), "left") \
            .select(
                col("v.event_id"),
                col("d.date_id"),
                col("c.customer_id"),
                col("p.product_id_pk"),
                col("g.geography_id"),
                col("v.session_id"),
                col("v.event_type"),
                col("v.event_timestamp"),
                col("v.view_duration"),
                col("v.search_query")
            )

        return transformed_df

    def load(self, df: DataFrame) -> None:
        """
        Load fact product views data to warehouse with deduplication

        Args:
            df: Transformed DataFrame
        """
        if df.count() == 0:
            print("  No product view events to load for this period")
            return

        try:
            # Read existing product views to check for duplicates
            existing_views = self.read_from_warehouse("fact_product_views")
            existing_event_ids = existing_views.select("event_id").distinct()

            # Filter out records that already exist (based on event_id)
            new_views = df.join(
                existing_event_ids,
                df.event_id == existing_event_ids.event_id,
                "left_anti"
            )

            new_count = new_views.count()
            duplicate_count = df.count() - new_count

            if duplicate_count > 0:
                print(f"  Skipping {duplicate_count} duplicate records")

            if new_count > 0:
                self.write_to_warehouse(new_views, "fact_product_views", mode="append")
                print(f"  Loaded {new_count} new product view events to fact_product_views")
            else:
                print("  No new product view events to load (all records already exist)")

        except Exception as e:
            # If table doesn't exist yet or other error, load all records
            print(f"  Note: {str(e)}")
            print(f"  Loading all {df.count()} records to fact_product_views")
            self.write_to_warehouse(df, "fact_product_views", mode="append")


if __name__ == "__main__":
    # Run FactProductViews ETL
    etl = FactProductViewsETL()
    etl.run()
