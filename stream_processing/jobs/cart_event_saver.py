"""
Cart Event Saver Stream Processing Job
Saves raw cart events to PostgreSQL for batch processing
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from stream_processing.common.spark_session import create_streaming_session


class CartEventSaver:
    """Saves raw cart events from Kafka to PostgreSQL for batch ETL"""

    def __init__(self):
        self.spark = create_streaming_session("GlobalMart-CartEventSaver")
        self.cart_event_schema = self._get_cart_event_schema()

    def _get_cart_event_schema(self) -> StructType:
        """Define the schema for cart event messages"""
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("old_quantity", IntegerType(), True),
            StructField("new_quantity", IntegerType(), True),
            StructField("cart_size", IntegerType(), True),
            StructField("cart_value", DoubleType(), True),
            StructField("country", StringType(), True)
        ])

    def read_from_kafka(self) -> DataFrame:
        """Read cart events stream from Kafka"""
        print(f"Reading from Kafka topic: {settings.kafka.topic_cart_events}")

        df = (self.spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
              .option("subscribe", settings.kafka.topic_cart_events)
              .option("startingOffsets", "latest")
              .option("failOnDataLoss", "false")
              .load())

        # Parse JSON messages
        cart_events_df = (df
                         .selectExpr("CAST(value AS STRING) as json_value")
                         .select(from_json(col("json_value"), self.cart_event_schema).alias("data"))
                         .select("data.*")
                         .withColumn("timestamp", to_timestamp(col("timestamp"))))

        print("✓ Connected to Kafka stream")
        return cart_events_df

    def transform_to_events(self, cart_events_df: DataFrame) -> DataFrame:
        """
        Transform cart events to standard format for warehouse
        Selects relevant fields for batch ETL
        """
        events = cart_events_df.select(
            col("event_id"),
            col("event_type"),
            col("user_id"),
            col("session_id"),
            col("product_id"),
            col("timestamp").alias("event_timestamp"),
            col("quantity"),
            col("cart_value"),
            col("country"),
            current_timestamp().alias("created_at")
        )

        return events

    def write_to_postgres(self, df: DataFrame, checkpoint_location: str):
        """Write cart events to PostgreSQL"""
        postgres_config = settings.postgres_realtime

        def write_batch(batch_df, batch_id):
            """Write a micro-batch to PostgreSQL"""
            if batch_df.count() > 0:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", postgres_config.jdbc_url) \
                    .option("dbtable", "cart_events") \
                    .option("user", postgres_config.user) \
                    .option("password", postgres_config.password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                print(f"✓ Batch {batch_id}: Saved {batch_df.count()} cart events to PostgreSQL")

        query = (df.writeStream
                 .foreachBatch(write_batch)
                 .outputMode("append")
                 .option("checkpointLocation", checkpoint_location)
                 .trigger(processingTime="10 seconds")
                 .start())

        return query

    def run(self, output_mode: str = "postgres"):
        """
        Run the cart event saver streaming job

        Args:
            output_mode: 'console' for testing, 'postgres' for production
        """
        print("=" * 60)
        print("GlobalMart Cart Event Saver - Stream Processing")
        print("=" * 60)
        print()

        # Read from Kafka
        cart_events = self.read_from_kafka()

        # Transform to event format
        print("Transforming cart events...")
        events = self.transform_to_events(cart_events)
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
            checkpoint = "/tmp/globalmart/checkpoints/cart_event_saver"
            query = self.write_to_postgres(events, checkpoint)
            print(f"✓ Cart event saver started with checkpoint: {checkpoint}")

        print()
        print("=" * 60)
        print("Streaming cart events")
        print(f"Output: {output_mode}")
        print("Press Ctrl+C to stop")
        print("=" * 60)
        print()

        # Wait for termination
        query.awaitTermination()


if __name__ == "__main__":
    saver = CartEventSaver()
    saver.run()
