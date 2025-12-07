"""
Fact Cart Events ETL
Populates fact_cart_events table with cart events
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import BaseETL


class FactCartEventsETL(BaseETL):
    """ETL job for populating fact_cart_events fact table"""

    def __init__(self):
        super().__init__("FactCartEvents")

    def extract(self) -> DataFrame:
        """
        Extract cart events from real-time database

        Returns:
            DataFrame: Raw cart events
        """
        # Read from cart_events table in realtime DB
        df = self.read_from_postgres(table_name="cart_events", database="realtime")

        print(f"  Extracted {df.count()} cart events")

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform cart events to fact table format

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
        transformed_df = df.alias("c") \
            .join(dim_customers.select("customer_id", "user_id").alias("cu"),
                  col("c.user_id") == col("cu.user_id"), "left") \
            .join(dim_products.select("product_id_pk", "product_id").alias("p"),
                  col("c.product_id") == col("p.product_id"), "left") \
            .join(dim_geography.select("geography_id", "country").alias("g"),
                  col("c.country") == col("g.country"), "left") \
            .join(dim_date.select("date_id", "date").alias("d"),
                  col("c.event_timestamp").cast("date") == col("d.date"), "left") \
            .select(
                col("c.event_id"),
                col("d.date_id"),
                col("cu.customer_id"),
                col("p.product_id_pk"),
                col("g.geography_id"),
                col("c.session_id"),
                col("c.event_type"),
                col("c.event_timestamp"),
                col("c.quantity"),
                col("c.cart_value")
            )

        return transformed_df

    def load(self, df: DataFrame) -> None:
        """
        Load fact cart events data to warehouse with deduplication

        Args:
            df: Transformed DataFrame
        """
        if df.count() == 0:
            print("  No cart events to load for this period")
            return

        try:
            # Read existing cart events to check for duplicates
            existing_events = self.read_from_warehouse("fact_cart_events")
            existing_event_ids = existing_events.select("event_id").distinct()

            # Filter out records that already exist (based on event_id)
            new_events = df.join(
                existing_event_ids,
                df.event_id == existing_event_ids.event_id,
                "left_anti"
            )

            new_count = new_events.count()
            duplicate_count = df.count() - new_count

            if duplicate_count > 0:
                print(f"  Skipping {duplicate_count} duplicate records")

            if new_count > 0:
                self.write_to_warehouse(new_events, "fact_cart_events", mode="append")
                print(f"  Loaded {new_count} new cart events to fact_cart_events")
            else:
                print("  No new cart events to load (all records already exist)")

        except Exception as e:
            # If table doesn't exist yet or other error, load all records
            print(f"  Note: {str(e)}")
            print(f"  Loading all {df.count()} records to fact_cart_events")
            self.write_to_warehouse(df, "fact_cart_events", mode="append")


if __name__ == "__main__":
    # Run FactCartEvents ETL
    etl = FactCartEventsETL()
    etl.run()
