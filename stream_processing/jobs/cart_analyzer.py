"""
Cart Events Analyzer Stream Processing Job
Analyzes cart events for session tracking and abandonment detection
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum, avg,
    current_timestamp, to_timestamp, when, lit, expr, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, ArrayType
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from stream_processing.common.spark_session import create_streaming_session


class CartAnalyzer:
    """Real-time cart events analysis for session tracking and abandonment detection"""

    def __init__(self):
        self.spark = create_streaming_session("GlobalMart-CartAnalyzer")
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
            StructField("old_quantity", IntegerType(), True),  # Only for update_quantity events
            StructField("new_quantity", IntegerType(), True),  # Only for update_quantity events
            StructField("cart_size", IntegerType(), True),
            StructField("cart_value", DoubleType(), True),  # FIXED: was cart_total
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

    def analyze_cart_sessions(self, cart_events_df: DataFrame) -> DataFrame:
        """Analyze cart sessions - track active carts and their behavior"""
        session_metrics = (cart_events_df
                          .withWatermark("timestamp", "5 minutes")
                          .groupBy(
                              window(col("timestamp"), "5 minutes"),
                              col("user_id"),
                              col("session_id"),
                              col("country")
                          )
                          .agg(
                              count(when(col("event_type") == "add_to_cart", 1)).alias("add_count"),
                              count(when(col("event_type") == "remove_from_cart", 1)).alias("remove_count"),
                              count(when(col("event_type") == "update_quantity", 1)).alias("update_count"),
                              count(when(col("event_type") == "checkout", 1)).alias("checkout_count"),
                              spark_max("cart_value").alias("max_cart_value"),  # FIXED: was cart_total
                              spark_max(col("timestamp")).alias("last_activity"),
                              count("*").alias("total_events")
                          )
                          .select(
                              col("window.start").alias("window_start"),
                              col("window.end").alias("window_end"),
                              col("user_id"),
                              col("session_id"),
                              col("country"),
                              col("add_count"),
                              col("remove_count"),
                              col("update_count"),
                              col("checkout_count"),
                              col("max_cart_value"),
                              col("last_activity"),
                              col("total_events"),
                              # Session completed if checkout occurred
                              (col("checkout_count") > 0).alias("session_completed"),
                              current_timestamp().alias("created_at")
                          ))

        return session_metrics

    def detect_cart_abandonment(self, cart_events_df: DataFrame) -> DataFrame:
        """Detect abandoned carts - carts with no checkout after activity"""
        # Group by session and check for abandonment
        abandonment = (cart_events_df
                      .withWatermark("timestamp", "30 minutes")
                      .groupBy(
                          window(col("timestamp"), "30 minutes", "10 minutes"),
                          col("user_id"),
                          col("session_id"),
                          col("country")
                      )
                      .agg(
                          count(when(col("event_type") == "add_to_cart", 1)).alias("items_added"),
                          count(when(col("event_type") == "checkout", 1)).alias("checkouts"),
                          spark_max("cart_value").alias("abandoned_cart_value"),  # FIXED: was cart_total
                          spark_max(col("timestamp")).alias("last_activity"),
                          count("*").alias("total_events")
                      )
                      .select(
                          col("window.start").alias("window_start"),
                          col("window.end").alias("window_end"),
                          col("user_id"),
                          col("session_id"),
                          col("country"),
                          col("items_added"),
                          col("abandoned_cart_value"),
                          col("last_activity"),
                          col("total_events")
                      )
                      # Only abandoned carts: items added but no checkout
                      .filter((col("items_added") > 0) & (col("checkouts") == 0))
                      .withColumn("abandonment_detected_at", current_timestamp())
                      .withColumn("created_at", current_timestamp()))

        return abandonment

    def aggregate_cart_metrics(self, cart_events_df: DataFrame) -> DataFrame:
        """Aggregate cart metrics by time window"""
        metrics = (cart_events_df
                  .withWatermark("timestamp", "1 minute")
                  .groupBy(
                      window(col("timestamp"), "1 minute"),
                      col("country")
                  )
                  .agg(
                      count("*").alias("total_cart_events"),
                      count(when(col("event_type") == "add_to_cart", 1)).alias("adds"),
                      count(when(col("event_type") == "remove_from_cart", 1)).alias("removes"),
                      count(when(col("event_type") == "update_quantity", 1)).alias("updates"),
                      count(when(col("event_type") == "checkout", 1)).alias("checkouts"),
                      avg("cart_value").alias("avg_cart_value"),  # FIXED: was cart_total
                      spark_max("cart_value").alias("max_cart_value")  # FIXED: was cart_total
                  )
                  .select(
                      col("window.start").alias("window_start"),
                      col("window.end").alias("window_end"),
                      col("country"),
                      col("total_cart_events"),
                      col("adds"),
                      col("removes"),
                      col("updates"),
                      col("checkouts"),
                      col("avg_cart_value"),
                      col("max_cart_value"),
                      # Calculate abandonment rate
                      when(col("adds") > 0,
                           (col("adds") - col("checkouts")) / col("adds") * 100)
                      .otherwise(0).alias("abandonment_rate_pct"),
                      current_timestamp().alias("created_at")
                  ))

        return metrics

    def write_to_postgres(self, df: DataFrame, table_name: str, checkpoint_location: str):
        """Write stream to PostgreSQL Real-Time database using foreachBatch"""
        print(f"Writing to PostgreSQL real-time table: {table_name}")

        jdbc_url = settings.postgres_realtime.jdbc_url
        db_user = settings.postgres_realtime.user
        db_password = settings.postgres_realtime.password

        def process_batch(batch_df, batch_id):
            """Process each micro-batch and write to PostgreSQL"""
            if batch_df.count() > 0:
                # Write to PostgreSQL
                (batch_df.write
                 .format("jdbc")
                 .option("url", jdbc_url)
                 .option("dbtable", table_name)
                 .option("user", db_user)
                 .option("password", db_password)
                 .option("driver", "org.postgresql.Driver")
                 .mode("append")
                 .save())

                print(f"[Batch {batch_id}] Wrote {batch_df.count()} records to {table_name}")

        query = (df
                .writeStream
                .foreachBatch(process_batch)
                .option("checkpointLocation", checkpoint_location)
                .start())

        return query

    def write_to_console(self, df: DataFrame, query_name: str):
        """Write stream to console for testing"""
        print(f"Writing stream {query_name} to console...")

        query = (df
                .writeStream
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .queryName(query_name)
                .start())

        return query

    def run(self, output_mode: str = "console"):
        """
        Run the cart analyzer

        Args:
            output_mode: 'console' for testing, 'postgres' for production
        """
        print("=" * 60)
        print("Cart Events Analyzer Stream Processing")
        print("=" * 60)
        print()

        # Read cart events from Kafka
        cart_events_df = self.read_from_kafka()

        # Run analyses
        session_metrics = self.analyze_cart_sessions(cart_events_df)
        abandoned_carts = self.detect_cart_abandonment(cart_events_df)
        cart_metrics = self.aggregate_cart_metrics(cart_events_df)

        # Write streams
        queries = []

        if output_mode == "console":
            query1 = self.write_to_console(session_metrics, "cart_sessions")
            query2 = self.write_to_console(abandoned_carts, "abandoned_carts")
            query3 = self.write_to_console(cart_metrics, "cart_metrics")
            queries = [query1, query2, query3]

        elif output_mode == "postgres":
            query1 = self.write_to_postgres(
                session_metrics,
                "cart_sessions",
                f"{settings.spark.checkpoint_dir}/cart_sessions"
            )
            query2 = self.write_to_postgres(
                abandoned_carts,
                "abandoned_carts",
                f"{settings.spark.checkpoint_dir}/abandoned_carts"
            )
            query3 = self.write_to_postgres(
                cart_metrics,
                "cart_metrics",
                f"{settings.spark.checkpoint_dir}/cart_metrics"
            )
            queries = [query1, query2, query3]

        print()
        print("✓ Cart analyzer started")
        print("  Analyzing: Session tracking, abandonment detection, metrics")
        print("  Press Ctrl+C to stop")
        print()

        # Wait for termination
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            print("\nStopping streams...")
            for query in queries:
                query.stop()
            print("✓ Streams stopped")

        self.spark.stop()


if __name__ == "__main__":
    analyzer = CartAnalyzer()
    analyzer.run(output_mode="console")
