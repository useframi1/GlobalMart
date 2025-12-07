"""
Product View Saver Stream Processing Job
Saves raw product view events to PostgreSQL for batch processing
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from stream_processing.common.spark_session import create_streaming_session


class ProductViewSaver:
    """Saves raw product view events from Kafka to PostgreSQL for batch ETL"""

    def __init__(self):
        self.spark = create_streaming_session("GlobalMart-ProductViewSaver")
        self.product_view_schema = self._get_product_view_schema()

    def _get_product_view_schema(self) -> StructType:
        """Define the schema for product view event messages"""
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            # View event fields
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("view_duration", IntegerType(), True),
            StructField("from_search", StringType(), True),
            StructField("previously_viewed_count", IntegerType(), True),
            # Search event fields
            StructField("search_query", StringType(), True),
            StructField("results_count", IntegerType(), True),
            # Filter event fields
            StructField("filter_category", StringType(), True),
            StructField("min_price", DoubleType(), True),
            StructField("max_price", DoubleType(), True),
            StructField("filtered_results_count", IntegerType(), True),
            # Compare event fields
            StructField("compared_product_ids", ArrayType(StringType()), True),
            StructField("comparison_count", IntegerType(), True),
            # Common fields
            StructField("country", StringType(), True)
        ])

    def read_from_kafka(self) -> DataFrame:
        """Read product view events stream from Kafka"""
        print(f"Reading from Kafka topic: {settings.kafka.topic_product_views}")

        df = (self.spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
              .option("subscribe", settings.kafka.topic_product_views)
              .option("startingOffsets", "latest")
              .option("failOnDataLoss", "false")
              .load())

        # Parse JSON messages
        product_views_df = (df
                           .selectExpr("CAST(value AS STRING) as json_value")
                           .select(from_json(col("json_value"), self.product_view_schema).alias("data"))
                           .select("data.*")
                           .withColumn("timestamp", to_timestamp(col("timestamp"))))

        print("✓ Connected to Kafka stream")
        return product_views_df

    def transform_to_events(self, product_views_df: DataFrame) -> DataFrame:
        """
        Transform product view events to standard format for warehouse
        Selects relevant fields for batch ETL
        """
        events = product_views_df.select(
            col("event_id"),
            col("event_type"),
            col("user_id"),
            col("session_id"),
            col("product_id"),
            col("timestamp").alias("event_timestamp"),
            col("view_duration"),
            col("search_query"),
            col("country"),
            current_timestamp().alias("created_at")
        )

        return events

    def write_to_postgres(self, df: DataFrame, checkpoint_location: str):
        """Write product view events to PostgreSQL"""
        postgres_config = settings.postgres_realtime

        def write_batch(batch_df, batch_id):
            """Write a micro-batch to PostgreSQL"""
            if batch_df.count() > 0:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", postgres_config.jdbc_url) \
                    .option("dbtable", "product_view_events") \
                    .option("user", postgres_config.user) \
                    .option("password", postgres_config.password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                print(f"✓ Batch {batch_id}: Saved {batch_df.count()} product view events to PostgreSQL")

        query = (df.writeStream
                 .foreachBatch(write_batch)
                 .outputMode("append")
                 .option("checkpointLocation", checkpoint_location)
                 .trigger(processingTime="10 seconds")
                 .start())

        return query

    def run(self, output_mode: str = "postgres"):
        """
        Run the product view saver streaming job

        Args:
            output_mode: 'console' for testing, 'postgres' for production
        """
        print("=" * 60)
        print("GlobalMart Product View Saver - Stream Processing")
        print("=" * 60)
        print()

        # Read from Kafka
        product_views = self.read_from_kafka()

        # Transform to event format
        print("Transforming product view events...")
        events = self.transform_to_events(product_views)
        print("✓ Transformation configured")
        print()

        # Write based on output mode
        if output_mode == "console":
            print("Starting console output (testing mode)...")
            query = (events.writeStream
                     .outputMode("append")
                     .format("console")
                     .option("truncate", "false")
                     .trigger(processingTime="10 seconds")
                     .start())
        else:
            print("Starting PostgreSQL sink...")
            checkpoint = "/tmp/globalmart/checkpoints/product_view_saver"
            query = self.write_to_postgres(events, checkpoint)
            print(f"✓ Product view saver started with checkpoint: {checkpoint}")

        print()
        print("=" * 60)
        print("Streaming product view events")
        print(f"Output: {output_mode}")
        print("Press Ctrl+C to stop")
        print("=" * 60)
        print()

        # Wait for termination
        query.awaitTermination()


if __name__ == "__main__":
    saver = ProductViewSaver()
    saver.run()
