"""
Customer Dimension ETL with SCD Type 2
Tracks historical changes to customer attributes
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, coalesce, max as spark_max, min as spark_min, count, sum as spark_sum, avg
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import DimensionETL
from batch_processing.warehouse.scd_manager import SCDType2Manager


class CustomerDimensionETL(DimensionETL):
    """ETL for customer dimension with SCD Type 2 support"""

    def __init__(self, spark):
        super().__init__(spark, "dim_customers", use_scd2=True)
        self.scd_manager = SCDType2Manager(spark)

    def extract(self) -> DataFrame:
        """
        Extract customer data from real-time database
        Aggregates transaction history to create customer profiles
        """
        self.logger.info("Extracting customer data from real-time database")

        # Read sales data from real-time DB to get customer transaction summary
        # Using sales_per_minute and sales_by_country tables
        try:
            # Extract customer data from cart sessions which has user_id
            # In production, this would combine cart and sales data
            query = """
                (SELECT
                    user_id,
                    MIN(window_start) as first_transaction_date,
                    MAX(last_activity) as last_transaction_date,
                    COUNT(DISTINCT session_id) as total_transactions,
                    COALESCE(SUM(max_cart_value), 0) as total_spent,
                    COALESCE(AVG(max_cart_value), 0) as avg_order_value
                FROM cart_sessions
                WHERE checkout_count > 0
                GROUP BY user_id) as customer_data
            """

            df = (self.spark.read
                  .format("jdbc")
                  .option("url", self.realtime_jdbc_url)
                  .option("dbtable", query)
                  .option("user", self.realtime_jdbc_props["user"])
                  .option("password", self.realtime_jdbc_props["password"])
                  .option("driver", self.realtime_jdbc_props["driver"])
                  .load())

            self.stats["rows_processed"] = df.count()
            self.logger.info(f"Extracted {self.stats['rows_processed']} customer records")

            return df

        except Exception as e:
            self.logger.warning(f"Could not extract from cart sessions: {e}")
            self.logger.info("Creating empty customer dimension")
            # Return empty DataFrame with correct schema
            return self.spark.createDataFrame([], """
                user_id STRING,
                first_transaction_date TIMESTAMP,
                last_transaction_date TIMESTAMP,
                total_transactions BIGINT,
                total_spent DOUBLE,
                avg_order_value DOUBLE
            """)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform customer data
        Add customer segmentation and activity flags
        """
        self.logger.info("Transforming customer dimension")

        if df.count() == 0:
            self.logger.info("No customers to transform")
            return df

        # Add derived attributes
        transformed = (df
            .withColumn("customer_segment", lit("Active"))  # Will be updated by RFM analysis
            .withColumn("rfm_score", lit(None).cast("int"))  # Will be updated by RFM analysis
            .withColumn("is_active", lit(True))
            .withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
        )

        return transformed

    def load(self, df: DataFrame) -> None:
        """
        Load customer dimension with SCD Type 2 logic
        """
        self.logger.info("Loading customer dimension with SCD Type 2")

        if df.count() == 0:
            self.logger.info("No customers to load")
            return

        # Columns to track for changes (excluding keys and metadata)
        tracked_columns = [
            "total_transactions", "total_spent", "avg_order_value",
            "customer_segment", "rfm_score", "is_active"
        ]

        # Use SCD Type 2 manager to handle merge
        stats = self.scd_manager.merge_scd_type2(
            source_df=df,
            target_table="dim_customers",
            jdbc_url=self.warehouse_jdbc_url,
            jdbc_properties=self.warehouse_jdbc_props,
            business_key="user_id",
            tracked_columns=tracked_columns,
            surrogate_key="customer_sk"
        )

        # Update statistics
        self.stats["rows_inserted"] = stats["inserts"]
        self.stats["rows_updated"] = stats["updates"]

        self.logger.info(f"Customer dimension loaded: {stats}")
