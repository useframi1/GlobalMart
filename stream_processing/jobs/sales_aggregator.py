"""
Sales Aggregator Stream Processing Job
Aggregates transaction data in real-time and stores in MongoDB
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum, avg,
    current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    ArrayType, IntegerType, TimestampType
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from stream_processing.common.spark_session import create_streaming_session


class SalesAggregator:
    """Real-time sales aggregation from Kafka transactions"""

    def __init__(self):
        self.spark = create_streaming_session("GlobalMart-SalesAggregator")
        self.transaction_schema = self._get_transaction_schema()

    def _get_transaction_schema(self) -> StructType:
        """Define the schema for transaction messages"""
        product_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True)
        ])

        return StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("products", ArrayType(product_schema), True),
            StructField("total_amount", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("country", StringType(), True)
        ])

    def read_from_kafka(self) -> DataFrame:
        """Read transaction stream from Kafka"""
        print(f"Reading from Kafka topic: {settings.kafka.topic_transactions}")

        df = (self.spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
              .option("subscribe", settings.kafka.topic_transactions)
              .option("startingOffsets", "latest")
              .option("failOnDataLoss", "false")
              .load())

        # Parse JSON messages
        transactions_df = (df
                          .selectExpr("CAST(value AS STRING) as json_value")
                          .select(from_json(col("json_value"), self.transaction_schema).alias("data"))
                          .select("data.*")
                          .withColumn("timestamp", to_timestamp(col("timestamp"))))

        print("✓ Connected to Kafka stream")
        return transactions_df

    def aggregate_per_minute(self, transactions_df: DataFrame) -> DataFrame:
        """Aggregate transactions per minute"""
        aggregated = (transactions_df
                     .withWatermark("timestamp", "1 minute")
                     .groupBy(window(col("timestamp"), "1 minute"))
                     .agg(
                         count("transaction_id").alias("transaction_count"),
                         spark_sum("total_amount").alias("total_revenue"),
                         avg("total_amount").alias("avg_transaction_value")
                     )
                     .select(
                         col("window.start").alias("window_start"),
                         col("window.end").alias("window_end"),
                         col("transaction_count"),
                         col("total_revenue"),
                         col("avg_transaction_value")
                     ))

        return aggregated

    def aggregate_by_country(self, transactions_df: DataFrame) -> DataFrame:
        """Aggregate transactions by country"""
        aggregated = (transactions_df
                     .withWatermark("timestamp", "1 minute")
                     .groupBy(
                         window(col("timestamp"), "1 minute"),
                         col("country")
                     )
                     .agg(
                         count("transaction_id").alias("transaction_count"),
                         spark_sum("total_amount").alias("total_revenue")
                     )
                     .select(
                         col("window.start").alias("window_start"),
                         col("window.end").alias("window_end"),
                         col("country"),
                         col("transaction_count"),
                         col("total_revenue")
                     ))

        return aggregated

    def write_to_mongodb(self, df: DataFrame, collection_name: str, checkpoint_location: str):
        """Write stream to MongoDB using foreachBatch"""
        print(f"Writing to MongoDB collection: {collection_name}")

        mongo_uri = settings.mongo.connection_string
        mongo_database = settings.mongo.database

        def process_batch(batch_df, batch_id):
            """Process each micro-batch and write to MongoDB"""
            if batch_df.count() > 0:
                # Write to MongoDB
                (batch_df.write
                 .format("mongodb")
                 .mode("append")
                 .option("spark.mongodb.connection.uri", mongo_uri)
                 .option("spark.mongodb.database", mongo_database)
                 .option("spark.mongodb.collection", collection_name)
                 .save())

                print(f"[Batch {batch_id}] Wrote {batch_df.count()} records to {collection_name}")

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
        Run the sales aggregator

        Args:
            output_mode: 'console' for testing, 'mongodb' for production
        """
        print("=" * 60)
        print("Sales Aggregator Stream Processing")
        print("=" * 60)
        print()

        # Read transactions from Kafka
        transactions_df = self.read_from_kafka()

        # Aggregate per minute
        per_minute = self.aggregate_per_minute(transactions_df)

        # Aggregate by country
        by_country = self.aggregate_by_country(transactions_df)

        # Write streams
        queries = []

        if output_mode == "console":
            query1 = self.write_to_console(per_minute, "per_minute_aggregates")
            query2 = self.write_to_console(by_country, "country_aggregates")
            queries = [query1, query2]

        elif output_mode == "mongodb":
            query1 = self.write_to_mongodb(
                per_minute,
                "sales_per_minute",
                f"{settings.spark.checkpoint_dir}/sales_per_minute"
            )
            query2 = self.write_to_mongodb(
                by_country,
                "sales_by_country",
                f"{settings.spark.checkpoint_dir}/sales_by_country"
            )
            queries = [query1, query2]

        print()
        print("✓ Stream processing started")
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
    aggregator = SalesAggregator()

    # Use console output for testing, change to 'mongodb' for production
    # Note: MongoDB output requires MongoDB Spark Connector
    aggregator.run(output_mode="console")
