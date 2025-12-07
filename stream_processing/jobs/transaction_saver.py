"""
Transaction Saver Stream Processing Job
Saves raw transaction details to PostgreSQL for batch processing
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json, col, explode, to_timestamp, current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    ArrayType, IntegerType
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from stream_processing.common.spark_session import create_streaming_session


class TransactionSaver:
    """Saves raw transactions from Kafka to PostgreSQL for batch ETL"""

    def __init__(self):
        self.spark = create_streaming_session("GlobalMart-TransactionSaver")
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

    def transform_to_line_items(self, transactions_df: DataFrame) -> DataFrame:
        """
        Transform transactions to individual line items
        One row per product in each transaction
        """
        # Explode products array to create one row per product
        line_items = (transactions_df
                     .select(
                         col("transaction_id"),
                         col("user_id"),
                         col("timestamp").alias("transaction_timestamp"),
                         col("payment_method"),
                         col("country"),
                         explode(col("products")).alias("product")
                     )
                     .select(
                         col("transaction_id"),
                         # Create unique line_item_id for each product in transaction
                         expr("uuid()").alias("line_item_id"),
                         col("user_id"),
                         col("product.product_id"),
                         col("product.quantity"),
                         col("product.price").alias("unit_price"),
                         (col("product.quantity") * col("product.price")).alias("total_amount"),
                         expr("0.0").alias("discount_amount"),
                         col("payment_method"),
                         col("country"),
                         col("transaction_timestamp"),
                         current_timestamp().alias("created_at")
                     ))

        return line_items

    def write_to_postgres(self, df: DataFrame, checkpoint_location: str):
        """Write transaction line items to PostgreSQL"""
        postgres_config = settings.postgres_realtime

        def write_batch(batch_df, batch_id):
            """Write a micro-batch to PostgreSQL"""
            if batch_df.count() > 0:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", postgres_config.jdbc_url) \
                    .option("dbtable", "transactions") \
                    .option("user", postgres_config.user) \
                    .option("password", postgres_config.password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                print(f"✓ Batch {batch_id}: Saved {batch_df.count()} transaction line items to PostgreSQL")

        query = (df.writeStream
                 .foreachBatch(write_batch)
                 .outputMode("append")
                 .option("checkpointLocation", checkpoint_location)
                 .trigger(processingTime="10 seconds")
                 .start())

        return query

    def run(self, output_mode: str = "postgres"):
        """
        Run the transaction saver streaming job

        Args:
            output_mode: 'console' for testing, 'postgres' for production
        """
        print("=" * 60)
        print("GlobalMart Transaction Saver - Stream Processing")
        print("=" * 60)
        print()

        # Read from Kafka
        transactions = self.read_from_kafka()

        # Transform to line items
        print("Transforming transactions to line items...")
        line_items = self.transform_to_line_items(transactions)
        print("✓ Transformation configured")
        print()

        # Write based on output mode
        if output_mode == "console":
            print("Starting console output (testing mode)...")
            query = (line_items.writeStream
                     .outputMode("append")
                     .format("console")
                     .option("truncate", "false")
                     .trigger(processingTime="10 seconds")
                     .start())
        else:
            print("Starting PostgreSQL sink...")
            checkpoint = "/tmp/globalmart/checkpoints/transaction_saver"
            query = self.write_to_postgres(line_items, checkpoint)
            print(f"✓ Transaction saver started with checkpoint: {checkpoint}")

        print()
        print("=" * 60)
        print("Streaming transaction line items")
        print(f"Output: {output_mode}")
        print("Press Ctrl+C to stop")
        print("=" * 60)
        print()

        # Wait for termination
        query.awaitTermination()


if __name__ == "__main__":
    saver = TransactionSaver()
    saver.run()
