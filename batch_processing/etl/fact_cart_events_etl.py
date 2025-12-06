"""
Cart Events Fact ETL with Incremental Loading
Loads cart interaction events from real-time database
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import FactETL


class CartEventsFactETL(FactETL):
    """ETL for cart events fact table with incremental loading"""

    def __init__(self, spark):
        super().__init__(spark, "fact_cart_events")

    def extract(self) -> DataFrame:
        """
        Extract cart event data from real-time database
        Uses incremental loading based on last run timestamp
        """
        self.logger.info("Extracting cart events from real-time database")

        # Get last load timestamp for incremental processing
        last_load = self.get_last_load_timestamp()

        if last_load is None:
            # First run - load all historical data
            self.logger.info("First run - loading all historical cart events")
            where_clause = "1=1"
        else:
            # Incremental load - only new data
            self.logger.info(f"Incremental load - data since {last_load}")
            where_clause = f"window_start > '{last_load}'"

        try:
            # Extract from cart_sessions table
            # Contains aggregated cart metrics per session window
            query = f"""
                (SELECT
                    window_start as event_timestamp,
                    session_id,
                    user_id,
                    country,
                    add_count + remove_count + update_count + checkout_count as total_events,
                    max_cart_value,
                    checkout_count > 0 as is_checkout,
                    last_activity,
                    created_at
                FROM cart_sessions
                WHERE {where_clause}) as cart_data
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
            self.logger.info(f"Extracted {self.stats['rows_processed']} cart event records")

            return df

        except Exception as e:
            self.logger.error(f"Error extracting cart events: {e}")
            # Return empty DataFrame with correct schema
            return self.spark.createDataFrame([], """
                event_timestamp TIMESTAMP,
                session_id STRING,
                user_id STRING,
                country STRING,
                total_events BIGINT,
                max_cart_value DOUBLE,
                is_checkout BOOLEAN,
                last_activity TIMESTAMP,
                total_value DOUBLE,
                is_abandoned BOOLEAN,
                session_duration_minutes INTEGER,
                created_at TIMESTAMP
            """)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform cart event data
        Add surrogate key lookups and derived metrics
        """
        self.logger.info("Transforming cart events fact data")

        if df.count() == 0:
            self.logger.info("No cart events to transform")
            return df

        # Read dimension tables to get surrogate keys
        try:
            # Get customer surrogate keys (current records only)
            dim_customers = (self.spark.read
                           .format("jdbc")
                           .option("url", self.warehouse_jdbc_url)
                           .option("dbtable", "(SELECT customer_sk, user_id FROM dim_customers WHERE is_current = TRUE) as dim_cust")
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
            self.logger.warning("Proceeding without dimension lookups")
            dim_customers = None
            dim_date = None

        # Join with dimensions to get surrogate keys
        transformed = df

        if dim_customers is not None:
            transformed = transformed.join(
                dim_customers,
                transformed.user_id == dim_customers.user_id,
                "left"
            ).drop(dim_customers.user_id)

        if dim_date is not None:
            # Join on date
            transformed = transformed.join(
                dim_date,
                col("event_timestamp").cast("date") == col("date"),
                "left"
            ).drop("date")

        # Add derived metrics
        transformed = (transformed
            .withColumn("event_type",
                when(col("is_checkout") == True, lit("cart_checkout"))
                .otherwise(lit("cart_event")))
            .withColumn("cart_value", col("max_cart_value"))
            .withColumn("session_duration_sec",
                when(col("last_activity").isNotNull(), lit(0))
                .otherwise(lit(0)))
            .withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
        )

        return transformed

    def load(self, df: DataFrame) -> None:
        """
        Load cart events to warehouse
        Appends new records (facts are immutable)
        """
        self.logger.info("Loading cart events fact data to warehouse")

        if df.count() == 0:
            self.logger.info("No cart events to load")
            return

        # Select only columns that exist in target table
        columns_to_insert = [
            "event_timestamp", "session_id", "user_id",
            "customer_sk", "date_key",
            "event_type", "total_items", "total_value",
            "avg_item_value", "is_abandoned", "abandonment_value",
            "session_duration_minutes"
        ]

        # Filter to only existing columns
        available_columns = [c for c in columns_to_insert if c in df.columns]
        df_to_load = df.select(available_columns)

        try:
            # Append to fact table
            (df_to_load.write
             .format("jdbc")
             .option("url", self.warehouse_jdbc_url)
             .option("dbtable", "fact_cart_events")
             .option("user", self.warehouse_jdbc_props["user"])
             .option("password", self.warehouse_jdbc_props["password"])
             .option("driver", self.warehouse_jdbc_props["driver"])
             .mode("append")
             .save())

            self.stats["rows_inserted"] = df_to_load.count()
            self.stats["rows_updated"] = 0  # Facts are immutable

            self.logger.info(f"Loaded {self.stats['rows_inserted']} cart event records to warehouse")

        except Exception as e:
            self.logger.error(f"Error loading cart events: {e}")
            raise
