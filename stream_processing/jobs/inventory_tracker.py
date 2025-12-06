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
    max as spark_max, lag, struct, approx_count_distinct
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
                            spark_sum(expr("product.quantity * product.price")).alias("total_revenue"),
                            avg("product.price").alias("avg_price")
                        )
                        .select(
                            col("window.start").alias("window_start"),
                            col("window.end").alias("window_end"),
                            col("product_id"),
                            col("units_sold"),
                            col("total_revenue"),
                            col("avg_price"),
                            current_timestamp().alias("created_at")
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
                                   spark_sum(expr("product.quantity * product.price")).alias("total_revenue")
                               )
                               .select(
                                   col("window.start").alias("window_start"),
                                   col("window.end").alias("window_end"),
                                   col("product_id"),
                                   col("country"),
                                   col("units_sold"),
                                   col("total_revenue"),
                                   current_timestamp().alias("created_at")
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
                          spark_sum("product.quantity").alias("units_sold"),
                          spark_sum(expr("product.quantity * product.price")).alias("total_revenue")
                      )
                      .select(
                          col("window.start").alias("window_start"),
                          col("window.end").alias("window_end"),
                          col("product_id"),
                          col("units_sold"),
                          col("total_revenue")
                      )
                      # High demand threshold: 20+ units in 10 minutes
                      .filter(col("units_sold") >= 20)
                      .withColumn("demand_level",
                                 when(col("units_sold") >= 50, "critical")
                                 .when(col("units_sold") >= 30, "high")
                                 .otherwise("medium"))
                      .withColumn("alert_generated_at", current_timestamp())
                      .withColumn("created_at", current_timestamp())
                      .select(
                          col("window_start"),
                          col("window_end"),
                          col("product_id"),
                          col("units_sold"),
                          col("total_revenue"),
                          col("demand_level"),
                          col("alert_generated_at"),
                          col("created_at")
                      ))

        return high_demand

    def generate_restock_alerts(self, transactions_df: DataFrame) -> DataFrame:
        """Generate restock alerts based on sales velocity by country"""
        # Track sales by product and country
        exploded_df = (transactions_df
                      .withWatermark("timestamp", "5 minutes")
                      .select(
                          col("timestamp"),
                          col("country"),
                          explode(col("products")).alias("product")
                      ))

        restock_alerts = (exploded_df
                         .groupBy(
                             window(col("timestamp"), "5 minutes"),
                             col("country"),
                             col("product.product_id").alias("product_id")
                         )
                         .agg(
                             spark_sum("product.quantity").alias("units_sold")
                         )
                         .select(
                             col("window.start").alias("window_start"),
                             col("window.end").alias("window_end"),
                             col("product_id"),
                             col("country"),
                             col("units_sold"),
                             # Calculate sales rate per minute
                             (col("units_sold") / 5.0).alias("sales_rate_per_min")
                         )
                         # High sales velocity suggests potential stock issues
                         .filter(col("sales_rate_per_min") >= 1.0)  # 1+ unit per minute
                         .withColumn("urgency",
                                    when(col("sales_rate_per_min") >= 3.0, "high")
                                    .when(col("sales_rate_per_min") >= 2.0, "medium")
                                    .otherwise("low"))
                         .withColumn("alert_generated_at", current_timestamp())
                         .withColumn("created_at", current_timestamp())
                         .select(
                             col("window_start"),
                             col("window_end"),
                             col("product_id"),
                             col("country"),
                             col("units_sold"),
                             col("sales_rate_per_min"),
                             col("urgency"),
                             col("alert_generated_at"),
                             col("created_at")
                         ))

        return restock_alerts

    def aggregate_inventory_metrics(self, transactions_df: DataFrame) -> DataFrame:
        """Aggregate overall inventory consumption metrics by country"""
        exploded_df = (transactions_df
                      .withWatermark("timestamp", "1 minute")
                      .select(
                          col("timestamp"),
                          col("country"),
                          explode(col("products")).alias("product")
                      ))

        metrics = (exploded_df
                  .groupBy(
                      window(col("timestamp"), "1 minute"),
                      col("country")
                  )
                  .agg(
                      spark_sum("product.quantity").alias("total_units_sold"),
                      approx_count_distinct("product.product_id").alias("unique_products_sold"),
                      spark_sum(expr("product.quantity * product.price")).alias("total_revenue"),
                      count(when(col("product.quantity") >= 10, 1)).alias("fast_moving_products")
                  )
                  .select(
                      col("window.start").alias("window_start"),
                      col("window.end").alias("window_end"),
                      col("country"),
                      col("total_units_sold"),
                      col("unique_products_sold"),
                      col("total_revenue"),
                      col("fast_moving_products"),
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
            output_mode: 'console' for testing, 'postgres' for production
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
        restock_alerts = self.generate_restock_alerts(transactions_df)
        inventory_metrics = self.aggregate_inventory_metrics(transactions_df)

        # Write streams
        queries = []

        if output_mode == "console":
            query1 = self.write_to_console(product_sales, "product_sales")
            query2 = self.write_to_console(high_demand, "high_demand_alerts")
            query3 = self.write_to_console(restock_alerts, "restock_alerts")
            queries = [query1, query2, query3]

        elif output_mode == "postgres":
            query1 = self.write_to_postgres(
                product_sales,
                "product_sales_velocity",
                f"{settings.spark.checkpoint_dir}/product_sales_velocity"
            )
            query2 = self.write_to_postgres(
                inventory_by_country,
                "inventory_by_country",
                f"{settings.spark.checkpoint_dir}/inventory_by_country"
            )
            query3 = self.write_to_postgres(
                high_demand,
                "high_demand_alerts",
                f"{settings.spark.checkpoint_dir}/high_demand_alerts"
            )
            query4 = self.write_to_postgres(
                restock_alerts,
                "restock_alerts",
                f"{settings.spark.checkpoint_dir}/restock_alerts"
            )
            query5 = self.write_to_postgres(
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
