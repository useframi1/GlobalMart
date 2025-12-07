"""
Fact Sales ETL
Populates fact_sales table with transaction data from real-time database
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, expr, lit, current_timestamp, to_date, monotonically_increasing_id
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import BaseETL


class FactSalesETL(BaseETL):
    """ETL job for populating fact_sales table from real-time transactions"""

    def __init__(self, batch_date: str = None):
        """
        Initialize FactSalesETL

        Args:
            batch_date: Date to process (YYYY-MM-DD), defaults to yesterday
        """
        super().__init__("FactSales")
        self.batch_date = batch_date

    def extract(self) -> DataFrame:
        """
        Extract transaction line items from real-time transactions table

        Returns:
            DataFrame: Transaction line items from real-time database
        """
        # Read transaction line items from real-time database
        df = self.read_from_postgres(table_name="transactions", database="realtime")

        # Filter for recent data (last 7 days if batch_date not specified)
        from pyspark.sql.functions import current_date, date_sub
        if self.batch_date is None:
            df = df.filter(col("transaction_timestamp") >= date_sub(current_date(), 7))
        else:
            # Filter for specific batch date
            df = df.filter(col("transaction_timestamp").cast("date") == self.batch_date)

        df = df.orderBy(col("transaction_timestamp").desc())

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform transaction line items to fact table format
        Maps to dimension tables using lookup joins

        Args:
            df: Input DataFrame with transaction line items

        Returns:
            DataFrame: Transformed fact sales data ready for warehouse
        """
        # Read dimension tables for lookups
        # For SCD Type 2 dimensions, filter for current records only
        dim_customers = self.read_from_warehouse(table_name="dim_customers").filter(col("is_current") == True)
        dim_products = self.read_from_warehouse(table_name="dim_products").filter(col("is_current") == True)
        dim_geography = self.read_from_warehouse(table_name="dim_geography").filter(col("is_current") == True)
        dim_date = self.read_from_warehouse(table_name="dim_date")

        # Join with dimension tables to get surrogate keys
        # Left join to handle cases where dimension data might be missing
        transformed_df = df.alias("t") \
            .join(
                dim_customers.select("customer_id", "user_id").alias("c"),
                col("t.user_id") == col("c.user_id"),
                "left"
            ) \
            .join(
                dim_products.select("product_id_pk", "product_id").alias("p"),
                col("t.product_id") == col("p.product_id"),
                "left"
            ) \
            .join(
                dim_geography.select("geography_id", "country").alias("g"),
                col("t.country") == col("g.country"),
                "left"
            ) \
            .join(
                dim_date.select("date_id", "date").alias("d"),
                col("t.transaction_timestamp").cast("date") == col("d.date"),
                "left"
            ) \
            .select(
                col("t.line_item_id").alias("event_id"),
                col("t.transaction_id"),
                col("d.date_id"),
                col("c.customer_id"),
                col("p.product_id_pk"),
                col("g.geography_id"),
                col("t.transaction_timestamp"),
                col("t.quantity"),
                col("t.unit_price"),
                col("t.total_amount"),
                col("t.discount_amount"),
                col("t.payment_method")
            )

        return transformed_df

    def load(self, df: DataFrame) -> None:
        """
        Load fact sales data to warehouse with deduplication

        Args:
            df: Transformed DataFrame
        """
        if df.count() == 0:
            print("  No sales data to load for this period")
            return

        # Read existing fact_sales to check for duplicates
        try:
            existing_sales = self.read_from_warehouse("fact_sales")
            existing_event_ids = existing_sales.select("event_id").distinct()

            # Filter out records that already exist (based on event_id which is line_item_id)
            new_sales = df.join(
                existing_event_ids,
                df.event_id == existing_event_ids.event_id,
                "left_anti"
            )

            new_count = new_sales.count()
            duplicate_count = df.count() - new_count

            if duplicate_count > 0:
                print(f"  Skipping {duplicate_count} duplicate records")

            if new_count > 0:
                self.write_to_warehouse(new_sales, "fact_sales", mode="append")
                print(f"  Loaded {new_count} new sales records to fact_sales")
            else:
                print("  No new sales records to load (all records already exist)")

        except Exception as e:
            # If fact_sales doesn't exist or is empty, load all records
            print(f"  Note: {str(e)}")
            print(f"  Loading all {df.count()} records to fact_sales")
            self.write_to_warehouse(df, "fact_sales", mode="append")
            print(f"  Loaded {df.count()} sales records to fact_sales")


if __name__ == "__main__":
    # Run FactSales ETL
    etl = FactSalesETL()
    etl.run()
