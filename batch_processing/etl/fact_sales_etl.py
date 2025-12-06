"""
Sales Fact ETL with Incremental Loading
Loads transaction-level sales data from real-time database
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, when
from datetime import datetime, timedelta
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import FactETL


class SalesFactETL(FactETL):
    """ETL for sales fact table with incremental loading"""

    def __init__(self, spark):
        super().__init__(spark, "fact_sales")

    def extract(self) -> DataFrame:
        """
        Extract sales data from real-time database
        Uses incremental loading based on last run timestamp
        """
        self.logger.info("Extracting sales data from real-time database")

        # Get last load timestamp for incremental processing
        last_load = self.get_last_load_timestamp()

        if last_load is None:
            # First run - load all historical data
            self.logger.info("First run - loading all historical sales data")
            where_clause = "1=1"
        else:
            # Incremental load - only new data
            self.logger.info(f"Incremental load - data since {last_load}")
            where_clause = f"window_start > '{last_load}'"

        try:
            # Extract from product_sales_velocity table
            # This contains product-level sales data aggregated by window
            query = f"""
                (SELECT
                    window_start as transaction_timestamp,
                    product_id,
                    units_sold as quantity,
                    total_revenue,
                    avg_price as product_price,
                    created_at
                FROM product_sales_velocity
                WHERE {where_clause}) as sales_data
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
            self.logger.info(f"Extracted {self.stats['rows_processed']} sales records")

            return df

        except Exception as e:
            self.logger.error(f"Error extracting sales data: {e}")
            # Return empty DataFrame with correct schema
            return self.spark.createDataFrame([], """
                transaction_timestamp TIMESTAMP,
                product_id STRING,
                product_name STRING,
                category STRING,
                product_price DOUBLE,
                quantity BIGINT,
                total_revenue DOUBLE,
                payment_method STRING,
                created_at TIMESTAMP
            """)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform sales data
        Add surrogate key lookups and derived metrics
        """
        self.logger.info("Transforming sales fact data")

        if df.count() == 0:
            self.logger.info("No sales data to transform")
            return df

        # Read dimension tables to get surrogate keys
        try:
            # Get product surrogate keys (current records only)
            dim_products = (self.spark.read
                          .format("jdbc")
                          .option("url", self.warehouse_jdbc_url)
                          .option("dbtable", "(SELECT product_sk, product_id FROM dim_products WHERE is_current = TRUE) as dim_prod")
                          .option("user", self.warehouse_jdbc_props["user"])
                          .option("password", self.warehouse_jdbc_props["password"])
                          .option("driver", self.warehouse_jdbc_props["driver"])
                          .load())

            # Get date keys
            dim_date = (self.spark.read
                       .format("jdbc")
                       .option("url", self.warehouse_jdbc_url)
                       .option("dbtable", "(SELECT date_id, date FROM dim_date) as dim_dt")
                       .option("user", self.warehouse_jdbc_props["user"])
                       .option("password", self.warehouse_jdbc_props["password"])
                       .option("driver", self.warehouse_jdbc_props["driver"])
                       .load())

        except Exception as e:
            self.logger.error(f"Error reading dimension tables: {e}")
            self.logger.warning("Proceeding without dimension lookups - will use business keys only")
            dim_products = None
            dim_date = None

        # Join with dimensions to get surrogate keys
        transformed = df

        if dim_products is not None:
            transformed = transformed.join(
                dim_products,
                transformed.product_id == dim_products.product_id,
                "left"
            ).drop(dim_products.product_id)

        if dim_date is not None:
            # Join on date (cast transaction timestamp to date)
            transformed = transformed.join(
                dim_date,
                col("transaction_timestamp").cast("date") == col("date"),
                "left"
            ).drop("date")

        # Add derived metrics
        transformed = (transformed
            .withColumn("discount_amount", lit(0.0))  # Would come from promotions
            .withColumn("tax_amount", col("total_revenue") * 0.08)  # Simplified 8% tax
            .withColumn("net_revenue", col("total_revenue") - col("discount_amount"))
            .withColumn("profit_margin",
                when(col("product_price") > 0,
                     (col("total_revenue") * 0.4) / col("product_price"))
                .otherwise(0.0))
            .withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
        )

        return transformed

    def load(self, df: DataFrame) -> None:
        """
        Load sales facts to warehouse
        Appends new records (facts are immutable)
        """
        self.logger.info("Loading sales fact data to warehouse")

        if df.count() == 0:
            self.logger.info("No sales data to load")
            return

        # Select only columns that exist in target table
        # Exclude created_at from source to avoid conflict
        columns_to_insert = [
            "transaction_timestamp", "user_id", "product_id",
            "customer_sk", "product_sk", "date_key",
            "product_name", "category", "product_price",
            "quantity", "total_revenue", "discount_amount",
            "tax_amount", "net_revenue", "payment_method",
            "profit_margin"
        ]

        # Filter to only existing columns
        available_columns = [c for c in columns_to_insert if c in df.columns]
        df_to_load = df.select(available_columns)

        try:
            # Append to fact table (facts are insert-only, no updates)
            (df_to_load.write
             .format("jdbc")
             .option("url", self.warehouse_jdbc_url)
             .option("dbtable", "fact_sales")
             .option("user", self.warehouse_jdbc_props["user"])
             .option("password", self.warehouse_jdbc_props["password"])
             .option("driver", self.warehouse_jdbc_props["driver"])
             .mode("append")
             .save())

            self.stats["rows_inserted"] = df_to_load.count()
            self.stats["rows_updated"] = 0  # Facts are immutable

            self.logger.info(f"Loaded {self.stats['rows_inserted']} sales records to warehouse")

        except Exception as e:
            self.logger.error(f"Error loading sales data: {e}")
            raise
