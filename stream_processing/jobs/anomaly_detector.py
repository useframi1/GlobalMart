"""
Anomaly Detector Stream Processing Job
Detects anomalous transactions in real-time
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum, avg, stddev,
    current_timestamp, to_timestamp, when, lit, expr, explode,
    max as spark_max, min as spark_min
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    ArrayType, IntegerType, TimestampType
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from config.constants import ANOMALY_VALUE_RANGE, ANOMALY_QUANTITY_RANGE
from stream_processing.common.spark_session import create_streaming_session


class AnomalyDetector:
    """Real-time transaction anomaly detection"""

    def __init__(self):
        self.spark = create_streaming_session("GlobalMart-AnomalyDetector")
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

    def detect_high_value_anomalies(self, transactions_df: DataFrame) -> DataFrame:
        """Detect transactions with unusually high values"""
        # Simplified: Use static threshold without complex windowed statistics
        # High-value anomaly threshold from constants
        high_value_threshold = ANOMALY_VALUE_RANGE[0]  # $3000+

        anomalies = (transactions_df
                    .withWatermark("timestamp", "1 minute")
                    .filter(col("total_amount") >= high_value_threshold)
                    .withColumn("anomaly_type", lit("high_value"))
                    .withColumn("anomaly_reason",
                               expr(f"concat('Transaction amount $', round(total_amount, 2), ' exceeds threshold $', {high_value_threshold})"))
                    .withColumn("severity",
                               when(col("total_amount") >= ANOMALY_VALUE_RANGE[1], "critical")
                               .when(col("total_amount") >= (ANOMALY_VALUE_RANGE[0] + ANOMALY_VALUE_RANGE[1])/2, "high")
                               .otherwise("medium"))
                    .withColumn("detected_at", current_timestamp())
                    .withColumn("created_at", current_timestamp())
                    .select(
                        col("transaction_id"),
                        col("user_id"),
                        col("timestamp"),
                        col("total_amount"),
                        col("payment_method"),
                        col("country"),
                        col("anomaly_type"),
                        col("anomaly_reason"),
                        col("severity"),
                        col("detected_at"),
                        col("created_at")
                    ))

        return anomalies

    def detect_high_quantity_anomalies(self, transactions_df: DataFrame) -> DataFrame:
        """Detect transactions with unusually high product quantities"""
        # Explode products array to check individual quantities
        high_quantity_threshold = ANOMALY_QUANTITY_RANGE[0]  # 50+ items

        exploded_df = (transactions_df
                      .withWatermark("timestamp", "1 minute")
                      .select(
                          col("transaction_id"),
                          col("user_id"),
                          col("timestamp"),
                          col("total_amount"),
                          col("payment_method"),
                          col("country"),
                          explode(col("products")).alias("product")
                      ))

        anomalies = (exploded_df
                    .filter(col("product.quantity") >= high_quantity_threshold)
                    .withColumn("anomaly_type", lit("high_quantity"))
                    .withColumn("anomaly_reason",
                               expr(f"concat('Product quantity ', product.quantity, ' exceeds threshold ', {high_quantity_threshold})"))
                    .withColumn("severity",
                               when(col("product.quantity") >= ANOMALY_QUANTITY_RANGE[1], "critical")
                               .when(col("product.quantity") >= (ANOMALY_QUANTITY_RANGE[0] + ANOMALY_QUANTITY_RANGE[1])/2, "high")
                               .otherwise("medium"))
                    .withColumn("detected_at", current_timestamp())
                    .withColumn("created_at", current_timestamp())
                    .withColumn("product_id", col("product.product_id"))
                    .withColumn("quantity", col("product.quantity"))
                    .select(
                        col("transaction_id"),
                        col("user_id"),
                        col("timestamp"),
                        col("total_amount"),
                        col("payment_method"),
                        col("country"),
                        col("product_id"),
                        col("quantity"),
                        col("anomaly_type"),
                        col("anomaly_reason"),
                        col("severity"),
                        col("detected_at"),
                        col("created_at")
                    ))

        return anomalies

    def detect_suspicious_patterns(self, transactions_df: DataFrame) -> DataFrame:
        """Detect suspicious transaction patterns (e.g., rapid repeated transactions)"""
        # Detect users with multiple high-value transactions in short time (1 minute)
        suspicious = (transactions_df
                     .withWatermark("timestamp", "1 minute")
                     .filter(col("total_amount") >= 1000)  # Only high-value transactions
                     .groupBy(
                         window(col("timestamp"), "1 minute"),
                         col("user_id"),
                         col("country")
                     )
                     .agg(
                         count("transaction_id").alias("transaction_count"),
                         spark_sum("total_amount").alias("total_spent"),
                         avg("total_amount").alias("avg_transaction"),
                         spark_max("total_amount").alias("max_transaction"),
                         spark_min(col("timestamp")).alias("first_transaction"),
                         spark_max(col("timestamp")).alias("last_transaction")
                     )
                     .select(
                         col("window.start").alias("window_start"),
                         col("window.end").alias("window_end"),
                         col("user_id"),
                         col("country"),
                         col("transaction_count"),
                         col("total_spent"),
                         col("avg_transaction"),
                         col("max_transaction"),
                         col("first_transaction"),
                         col("last_transaction")
                     )
                     # Suspicious if 2+ high-value transactions in 1 minute (very rapid)
                     .filter(col("transaction_count") >= 2)
                     .withColumn("anomaly_type", lit("suspicious_pattern"))
                     .withColumn("anomaly_reason",
                                expr("concat(transaction_count, ' high-value transactions totaling $', round(total_spent, 2), ' in 1 minute')"))
                     .withColumn("severity", lit("high"))
                     .withColumn("detected_at", current_timestamp())
                     .withColumn("created_at", current_timestamp()))

        return suspicious

    def aggregate_anomaly_stats(self, high_value_anomalies: DataFrame, high_quantity_anomalies: DataFrame) -> DataFrame:
        """Aggregate anomaly statistics"""
        # Union both anomaly types for aggregation
        all_anomalies = (high_value_anomalies
                        .select(
                            col("timestamp"),
                            col("country"),
                            col("anomaly_type"),
                            col("severity")
                        )
                        .union(
                            high_quantity_anomalies
                            .select(
                                col("timestamp"),
                                col("country"),
                                col("anomaly_type"),
                                col("severity")
                            )
                        ))

        stats = (all_anomalies
                .withWatermark("timestamp", "1 minute")
                .groupBy(
                    window(col("timestamp"), "1 minute"),
                    col("country")
                )
                .agg(
                    count("*").alias("total_anomalies"),
                    count(when(col("anomaly_type") == "high_value", 1)).alias("high_value_count"),
                    count(when(col("anomaly_type") == "high_quantity", 1)).alias("high_quantity_count"),
                    count(when(col("severity") == "critical", 1)).alias("critical_count"),
                    count(when(col("severity") == "high", 1)).alias("high_severity_count"),
                    count(when(col("severity") == "medium", 1)).alias("medium_severity_count")
                )
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("country"),
                    col("total_anomalies"),
                    col("high_value_count"),
                    col("high_quantity_count"),
                    col("critical_count"),
                    col("high_severity_count"),
                    col("medium_severity_count"),
                    current_timestamp().alias("created_at")
                ))

        return stats

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

                # Publish alerts to Kafka for critical anomalies (only if severity column exists)
                if "severity" in batch_df.columns:
                    critical = batch_df.filter(col("severity") == "critical")
                    critical_count = critical.count()
                    if critical_count > 0:
                        print(f"  ⚠️  CRITICAL: {critical_count} critical anomalies detected!")

                        # Generate alerts DataFrame
                        alerts_df = critical.select(
                            expr("uuid()").alias("alert_id"),
                            lit("anomaly").alias("alert_type"),
                            col("severity"),
                            expr("concat('Critical anomaly detected: ', anomaly_reason)").alias("title"),
                            col("anomaly_reason").alias("message"),
                            lit("AnomalyDetector").alias("source"),
                            current_timestamp().alias("created_at"),
                            lit(False).alias("resolved"),
                            # Metadata as struct
                            expr("struct(transaction_id, user_id, total_amount, country, anomaly_type) as metadata")
                        )

                        # Convert to JSON and publish to Kafka alerts topic
                        from pyspark.sql.functions import to_json, struct
                        alerts_json = alerts_df.select(
                            col("alert_id").cast("string").alias("key"),  # Use alert_id as Kafka key
                            to_json(struct("*")).alias("value")  # Serialize entire record as JSON
                        )

                        # Write alerts to Kafka
                        (alerts_json.write
                         .format("kafka")
                         .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
                         .option("topic", settings.kafka.topic_alerts)
                         .save())

                        print(f"  📢 Published {critical_count} alerts to Kafka topic: {settings.kafka.topic_alerts}")

                elif "critical_count" in batch_df.columns:
                    # For stats dataframes, check the critical_count
                    critical_stats = batch_df.filter(col("critical_count") > 0)
                    if critical_stats.count() > 0:
                        total_critical = critical_stats.agg({"critical_count": "sum"}).collect()[0][0]
                        if total_critical > 0:
                            print(f"  ⚠️  CRITICAL: {total_critical} critical anomalies in this window!")

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
                .outputMode("append")
                .format("console")
                .option("truncate", "false")
                .queryName(query_name)
                .start())

        return query

    def run(self, output_mode: str = "console"):
        """
        Run the anomaly detector

        Args:
            output_mode: 'console' for testing, 'postgres' for production
        """
        print("=" * 60)
        print("Transaction Anomaly Detector Stream Processing")
        print("=" * 60)
        print()

        # Read transactions from Kafka
        transactions_df = self.read_from_kafka()

        # Detect different types of anomalies
        high_value_anomalies = self.detect_high_value_anomalies(transactions_df)
        high_quantity_anomalies = self.detect_high_quantity_anomalies(transactions_df)
        suspicious_patterns = self.detect_suspicious_patterns(transactions_df)
        anomaly_stats = self.aggregate_anomaly_stats(high_value_anomalies, high_quantity_anomalies)

        # Write streams
        queries = []

        if output_mode == "console":
            query1 = self.write_to_console(high_value_anomalies, "high_value_anomalies")
            query2 = self.write_to_console(high_quantity_anomalies, "high_quantity_anomalies")
            query3 = self.write_to_console(suspicious_patterns, "suspicious_patterns")
            queries = [query1, query2, query3]

        elif output_mode == "postgres":
            query1 = self.write_to_postgres(
                high_value_anomalies,
                "high_value_anomalies",
                f"{settings.spark.checkpoint_dir}/high_value_anomalies"
            )
            query2 = self.write_to_postgres(
                high_quantity_anomalies,
                "high_quantity_anomalies",
                f"{settings.spark.checkpoint_dir}/high_quantity_anomalies"
            )
            query3 = self.write_to_postgres(
                suspicious_patterns,
                "suspicious_patterns",
                f"{settings.spark.checkpoint_dir}/suspicious_patterns"
            )
            query4 = self.write_to_postgres(
                anomaly_stats,
                "anomaly_stats",
                f"{settings.spark.checkpoint_dir}/anomaly_stats"
            )
            queries = [query1, query2, query3, query4]

        print()
        print("✓ Anomaly detector started")
        print("  Detecting: High-value, high-quantity, suspicious patterns")
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
    detector = AnomalyDetector()
    detector.run(output_mode="console")
