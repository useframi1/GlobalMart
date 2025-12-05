"""
Inventory Tracker Stream Processing Job
Tracks real-time inventory levels and generates low-stock alerts
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum, avg,
    current_timestamp, to_timestamp, when, lit, expr, explode,
    max as spark_max, lag, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    ArrayType, IntegerType, TimestampType
)
from pyspark.sql.window import Window

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from stream_processing.common.spark_session import create_streaming_session


class InventoryTracker:
    """Real-time inventory tracking and low-stock alerting"""

    def __init__(self):
        self.spark = create_streaming_session("GlobalMart-InventoryTracker")
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

    def track_product_sales(self, transactions_df: DataFrame) -> DataFrame:
        """Track product sales velocity in real-time"""
        # Explode products to get individual product sales
        exploded_df = (transactions_df
                      .withWatermark("timestamp", "5 minutes")
                      .select(
                          col("transaction_id"),
                          col("timestamp"),
                          col("country"),
                          explode(col("products")).alias("product")
                      ))

        # Aggregate sales by product
        product_sales = (exploded_df
                        .groupBy(
                            window(col("timestamp"), "5 minutes"),
                            col("product.product_id").alias("product_id")
                        )
                        .agg(
                            spark_sum("product.quantity").alias("units_sold"),
                            count("transaction_id").alias("transaction_count"),
                            avg("product.price").alias("avg_price"),
                            spark_max("product.price").alias("current_price"),
                            spark_sum(expr("product.quantity * product.price")).alias("revenue")
                        )
                        .select(
                            col("window.start").alias("window_start"),
                            col("window.end").alias("window_end"),
                            col("product_id"),
                            col("units_sold"),
                            col("transaction_count"),
                            col("avg_price"),
                            col("current_price"),
                            col("revenue"),
                            # Calculate units per minute
                            (col("units_sold") / 5.0).alias("units_per_minute")
                        ))

        return product_sales

    def track_inventory_by_country(self, transactions_df: DataFrame) -> DataFrame:
        """Track inventory consumption by country"""
        exploded_df = (transactions_df
                      .withWatermark("timestamp", "5 minutes")
                      .select(
                          col("timestamp"),
                          col("country"),
                          explode(col("products")).alias("product")
                      ))

        inventory_by_country = (exploded_df
                               .groupBy(
                                   window(col("timestamp"), "5 minutes"),
                                   col("country"),
                                   col("product.product_id").alias("product_id")
                               )
                               .agg(
                                   spark_sum("product.quantity").alias("units_sold"),
                                   count("*").alias("transaction_count"),
                                   avg("product.price").alias("avg_price")
                               )
                               .select(
                                   col("window.start").alias("window_start"),
                                   col("window.end").alias("window_end"),
                                   col("country"),
                                   col("product_id"),
                                   col("units_sold"),
                                   col("transaction_count"),
                                   col("avg_price")
                               ))

        return inventory_by_country

    def detect_high_demand_products(self, transactions_df: DataFrame) -> DataFrame:
        """Detect products with unusually high demand"""
        exploded_df = (transactions_df
                      .withWatermark("timestamp", "10 minutes")
                      .select(
                          col("timestamp"),
                          explode(col("products")).alias("product")
                      ))

        high_demand = (exploded_df
                      .groupBy(
                          window(col("timestamp"), "10 minutes"),
                          col("product.product_id").alias("product_id")
                      )
                      .agg(
                          spark_sum("product.quantity").alias("total_quantity_sold"),
                          count("*").alias("sale_count"),
                          avg("product.price").alias("product_price")
                      )
                      .select(
                          col("window.start").alias("window_start"),
                          col("window.end").alias("window_end"),
                          col("product_id"),
                          col("total_quantity_sold"),
                          col("sale_count"),
                          col("product_price"),
                          # Sales velocity
                          (col("total_quantity_sold") / 10.0).alias("units_per_minute")
                      )
                      # High demand threshold: 20+ units in 10 minutes
                      .filter(col("total_quantity_sold") >= 20)
                      .withColumn("alert_type", lit("high_demand"))
                      .withColumn("alert_message",
                                 expr("concat('High demand detected: ', total_quantity_sold, ' units sold in 10 minutes')"))
                      .withColumn("priority",
                                 when(col("total_quantity_sold") >= 50, "critical")
                                 .when(col("total_quantity_sold") >= 30, "high")
                                 .otherwise("medium"))
                      .withColumn("detected_at", current_timestamp()))

        return high_demand

    def generate_restock_alerts(self, product_sales: DataFrame) -> DataFrame:
        """Generate restock alerts based on sales velocity"""
        # This is a simplified alert system
        # In production, you'd compare against actual inventory levels
        restock_alerts = (product_sales
                         # High sales velocity suggests potential stock issues
                         .filter(col("units_per_minute") >= 1.0)  # 1+ unit per minute
                         .withColumn("alert_type", lit("potential_low_stock"))
                         .withColumn("alert_message",
                                    expr("concat('High sales velocity: ', round(units_per_minute, 2), ' units/min. Check inventory levels.')"))
                         .withColumn("priority",
                                    when(col("units_per_minute") >= 3.0, "high")
                                    .when(col("units_per_minute") >= 2.0, "medium")
                                    .otherwise("low"))
                         .withColumn("detected_at", current_timestamp())
                         .select(
                             col("window_start"),
                             col("window_end"),
                             col("product_id"),
                             col("units_sold"),
                             col("units_per_minute"),
                             col("current_price"),
                             col("revenue"),
                             col("alert_type"),
                             col("alert_message"),
                             col("priority"),
                             col("detected_at")
                         ))

        return restock_alerts

    def aggregate_inventory_metrics(self, transactions_df: DataFrame) -> DataFrame:
        """Aggregate overall inventory consumption metrics"""
        exploded_df = (transactions_df
                      .withWatermark("timestamp", "1 minute")
                      .select(
                          col("timestamp"),
                          explode(col("products")).alias("product")
                      ))

        metrics = (exploded_df
                  .groupBy(window(col("timestamp"), "1 minute"))
                  .agg(
                      spark_sum("product.quantity").alias("total_units_sold"),
                      count("*").alias("total_product_sales"),
                      count(when(col("product.quantity") >= 10, 1)).alias("bulk_purchases"),
                      avg("product.quantity").alias("avg_quantity_per_sale"),
                      spark_sum(expr("product.quantity * product.price")).alias("total_product_revenue")
                  )
                  .select(
                      col("window.start").alias("window_start"),
                      col("window.end").alias("window_end"),
                      col("total_units_sold"),
                      col("total_product_sales"),
                      col("bulk_purchases"),
                      col("avg_quantity_per_sale"),
                      col("total_product_revenue")
                  ))

        return metrics

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

                # Log high-priority alerts
                if "priority" in batch_df.columns:
                    high_priority = batch_df.filter(col("priority") == "high")
                    if high_priority.count() > 0:
                        print(f"  ⚠️  {high_priority.count()} high-priority inventory alerts!")

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
        Run the inventory tracker

        Args:
            output_mode: 'console' for testing, 'mongodb' for production
        """
        print("=" * 60)
        print("Inventory Tracker Stream Processing")
        print("=" * 60)
        print()

        # Read transactions from Kafka
        transactions_df = self.read_from_kafka()

        # Track inventory
        product_sales = self.track_product_sales(transactions_df)
        inventory_by_country = self.track_inventory_by_country(transactions_df)
        high_demand = self.detect_high_demand_products(transactions_df)
        restock_alerts = self.generate_restock_alerts(product_sales)
        inventory_metrics = self.aggregate_inventory_metrics(transactions_df)

        # Write streams
        queries = []

        if output_mode == "console":
            query1 = self.write_to_console(product_sales, "product_sales")
            query2 = self.write_to_console(high_demand, "high_demand_alerts")
            query3 = self.write_to_console(restock_alerts, "restock_alerts")
            queries = [query1, query2, query3]

        elif output_mode == "mongodb":
            query1 = self.write_to_mongodb(
                product_sales,
                "product_sales_velocity",
                f"{settings.spark.checkpoint_dir}/product_sales_velocity"
            )
            query2 = self.write_to_mongodb(
                inventory_by_country,
                "inventory_by_country",
                f"{settings.spark.checkpoint_dir}/inventory_by_country"
            )
            query3 = self.write_to_mongodb(
                high_demand,
                "high_demand_alerts",
                f"{settings.spark.checkpoint_dir}/high_demand_alerts"
            )
            query4 = self.write_to_mongodb(
                restock_alerts,
                "restock_alerts",
                f"{settings.spark.checkpoint_dir}/restock_alerts"
            )
            query5 = self.write_to_mongodb(
                inventory_metrics,
                "inventory_metrics",
                f"{settings.spark.checkpoint_dir}/inventory_metrics"
            )
            queries = [query1, query2, query3, query4, query5]

        print()
        print("✓ Inventory tracker started")
        print("  Tracking: Product sales, demand patterns, restock needs")
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
    tracker = InventoryTracker()
    tracker.run(output_mode="console")
