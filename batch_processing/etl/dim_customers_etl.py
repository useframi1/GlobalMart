"""
Dimension Customers ETL
Populates dim_customers table with customer data from transactions
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, min as spark_min, max as spark_max, count, sum as spark_sum, avg,
    when, lit, current_timestamp, monotonically_increasing_id, expr
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import BaseETL


class DimCustomersETL(BaseETL):
    """ETL job for populating dim_customers dimension table"""

    def __init__(self):
        super().__init__("DimCustomers")

    def extract(self) -> DataFrame:
        """
        Extract customer data from real-time transactions table with aggregated statistics

        Returns:
            DataFrame: Customer data with transaction statistics
        """
        # Extract customer transaction statistics
        try:
            transactions = self.read_from_postgres(table_name="transactions", database="realtime")

            # Calculate customer statistics from transactions
            df = transactions.groupBy("user_id").agg(
                spark_min("transaction_timestamp").cast("date").alias("first_transaction_date"),
                spark_max("transaction_timestamp").cast("date").alias("last_transaction_date"),
                count("transaction_id").alias("total_transactions"),
                spark_sum("total_amount").alias("total_spent")
            )

            print(f"  Extracted {df.count()} distinct customers from transactions")

        except Exception as e:
            print(f"  Warning: Could not read from transactions table: {str(e)}")
            print("  Creating default customer records...")
            # Create default customer DataFrame as fallback
            data = [(f"user_{i}",) for i in range(1, 101)]
            df = self.spark.createDataFrame(data, ["user_id"])
            df = df.select(
                col("user_id"),
                lit(None).cast("date").alias("first_transaction_date"),
                lit(None).cast("date").alias("last_transaction_date"),
                lit(0).cast("long").alias("total_transactions"),
                lit(0.0).alias("total_spent")
            )

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform customer data to dimension format with SCD Type 2

        Args:
            df: Input DataFrame with customer statistics

        Returns:
            DataFrame: Transformed customer dimension with SCD Type 2 columns
        """
        from pyspark.sql.functions import current_timestamp

        # Calculate derived metrics and add dimension attributes
        transformed_df = df.select(
            col("user_id"),
            col("first_transaction_date"),
            col("last_transaction_date"),
            col("total_transactions"),
            col("total_spent"),
            # Calculate avg_order_value
            when(col("total_transactions") > 0, col("total_spent") / col("total_transactions"))
            .otherwise(0.0).alias("avg_order_value"),
            # Simple segmentation based on spend
            when(col("total_spent") >= 1000, "High Value")
            .when(col("total_spent") >= 500, "Medium Value")
            .when(col("total_spent") >= 100, "Low Value")
            .otherwise("New").alias("customer_segment"),
            # RFM score will be populated by RFM analysis job
            lit(None).cast("int").alias("rfm_score"),
            # Mark customers as active if they have transactions
            when(col("total_transactions") > 0, True).otherwise(False).alias("is_active"),
            # SCD Type 2 columns
            current_timestamp().alias("effective_start_date"),
            lit("9999-12-31 23:59:59").cast("timestamp").alias("effective_end_date"),
            lit(True).alias("is_current")
        )

        return transformed_df

    def load(self, df: DataFrame) -> None:
        """
        Load customer dimension to warehouse using SCD Type 2

        Args:
            df: Transformed DataFrame
        """
        self.write_to_warehouse(
            df,
            "dim_customers",
            mode="append",
            scd_type2=True,
            natural_key="user_id"
        )
        print(f"  Loaded {df.count()} customer records to dim_customers")


if __name__ == "__main__":
    # Run DimCustomers ETL
    etl = DimCustomersETL()
    etl.run()
