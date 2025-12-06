"""
Product Views Fact ETL with Incremental Loading
Loads product browsing/view events from real-time database
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import FactETL


class ProductViewsFactETL(FactETL):
    """ETL for product views fact table with incremental loading"""

    def __init__(self, spark):
        super().__init__(spark, "fact_product_views")

    def extract(self) -> DataFrame:
        """
        Extract product view data from real-time database
        Uses incremental loading based on last run timestamp
        """
        self.logger.info("Extracting product views from real-time database")

        # Get last load timestamp for incremental processing
        last_load = self.get_last_load_timestamp()

        if last_load is None:
            # First run - load all historical data
            self.logger.info("First run - loading all historical product views")
            where_clause = "1=1"
        else:
            # Incremental load - only new data
            self.logger.info(f"Incremental load - data since {last_load}")
            where_clause = f"window_start > '{last_load}'"

        try:
            # Extract from trending_products table
            # Contains aggregated product view metrics
            query = f"""
                (SELECT
                    window_start as view_timestamp,
                    product_id,
                    product_name,
                    category,
                    product_price,
                    view_count,
                    click_through_rate,
                    created_at
                FROM trending_products
                WHERE {where_clause}) as view_data
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
            self.logger.info(f"Extracted {self.stats['rows_processed']} product view records")

            return df

        except Exception as e:
            self.logger.error(f"Error extracting product views: {e}")
            # Return empty DataFrame with correct schema
            return self.spark.createDataFrame([], """
                view_timestamp TIMESTAMP,
                product_id STRING,
                product_name STRING,
                category STRING,
                product_price DOUBLE,
                view_count BIGINT,
                click_through_rate DOUBLE,
                created_at TIMESTAMP
            """)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform product view data
        Add surrogate key lookups and derived metrics
        """
        self.logger.info("Transforming product views fact data")

        if df.count() == 0:
            self.logger.info("No product views to transform")
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
                       .option("dbtable", "(SELECT date_key, full_date FROM dim_date) as dim_dt")
                       .option("user", self.warehouse_jdbc_props["user"])
                       .option("password", self.warehouse_jdbc_props["password"])
                       .option("driver", self.warehouse_jdbc_props["driver"])
                       .load())

        except Exception as e:
            self.logger.error(f"Error reading dimension tables: {e}")
            self.logger.warning("Proceeding without dimension lookups")
            dim_products = None
            dim_date = None

        # Join with dimensions to get surrogate keys
        transformed = df

        if dim_products is not None:
            transformed = transformed.join(
                dim_products,
                df.product_id == dim_products.product_id,
                "left"
            ).drop(dim_products.product_id)

        if dim_date is not None:
            # Join on date
            transformed = transformed.join(
                dim_date,
                col("view_timestamp").cast("date") == col("full_date"),
                "left"
            ).drop("full_date")

        # Add derived metrics
        transformed = (transformed
            .withColumn("view_source", lit("web"))  # Would come from event data
            .withColumn("device_type", lit("desktop"))  # Would come from event data
            .withColumn("conversion_flag",
                when(col("click_through_rate") > 0.05, lit(True))
                .otherwise(lit(False)))
            .withColumn("engagement_score",
                col("view_count") * col("click_through_rate"))
            .withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
        )

        return transformed

    def load(self, df: DataFrame) -> None:
        """
        Load product views to warehouse
        Appends new records (facts are immutable)
        """
        self.logger.info("Loading product views fact data to warehouse")

        if df.count() == 0:
            self.logger.info("No product views to load")
            return

        # Select only columns that exist in target table
        columns_to_insert = [
            "view_timestamp", "product_id", "product_sk", "date_key",
            "product_name", "category", "product_price",
            "view_count", "click_through_rate", "conversion_flag",
            "engagement_score", "view_source", "device_type"
        ]

        # Filter to only existing columns
        available_columns = [c for c in columns_to_insert if c in df.columns]
        df_to_load = df.select(available_columns)

        try:
            # Append to fact table
            (df_to_load.write
             .format("jdbc")
             .option("url", self.warehouse_jdbc_url)
             .option("dbtable", "fact_product_views")
             .option("user", self.warehouse_jdbc_props["user"])
             .option("password", self.warehouse_jdbc_props["password"])
             .option("driver", self.warehouse_jdbc_props["driver"])
             .mode("append")
             .save())

            self.stats["rows_inserted"] = df_to_load.count()
            self.stats["rows_updated"] = 0  # Facts are immutable

            self.logger.info(f"Loaded {self.stats['rows_inserted']} product view records to warehouse")

        except Exception as e:
            self.logger.error(f"Error loading product views: {e}")
            raise
