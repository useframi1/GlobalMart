"""
Product View Analyzer Stream Processing Job
Analyzes product view events for trending products and user behavior
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum, avg,
    current_timestamp, to_timestamp, when, lit, expr,
    max as spark_max, min as spark_min, approx_count_distinct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, ArrayType
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from stream_processing.common.spark_session import create_streaming_session


class ProductViewAnalyzer:
    """Real-time product view analysis for trending products and user behavior"""

    def __init__(self):
        self.spark = create_streaming_session("GlobalMart-ProductViewAnalyzer")
        self.product_view_schema = self._get_product_view_schema()

    def _get_product_view_schema(self) -> StructType:
        """Define the schema for product view event messages"""
        # Schema matches actual generator output - fields are FLAT, not nested
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

    def analyze_trending_products(self, product_views_df: DataFrame) -> DataFrame:
        """Identify trending products based on view counts"""
        trending = (product_views_df
                   .filter(col("event_type") == "view")
                   .withWatermark("timestamp", "5 minutes")
                   .groupBy(
                       window(col("timestamp"), "5 minutes"),
                       col("product_id"),  # FIXED: removed .product
                       col("product_name"),  # FIXED: removed .product
                       col("category")  # FIXED: removed .product
                   )
                   .agg(
                       count("*").alias("view_count"),
                       approx_count_distinct("user_id").alias("unique_viewers"),
                       avg("view_duration").alias("avg_view_duration"),
                       spark_max("price").alias("product_price")  # FIXED: removed .product
                   )
                   .select(
                       col("window.start").alias("window_start"),
                       col("window.end").alias("window_end"),
                       col("product_id"),
                       col("product_name"),
                       col("category"),
                       col("view_count"),
                       col("unique_viewers"),
                       col("avg_view_duration"),
                       col("product_price"),
                       current_timestamp().alias("analyzed_at")
                   )
                   # Only products with significant activity
                   .filter(col("view_count") >= 5))

        return trending

    def analyze_category_performance(self, product_views_df: DataFrame) -> DataFrame:
        """Analyze category-level metrics"""
        category_metrics = (product_views_df
                           .filter(col("event_type") == "view")
                           .withWatermark("timestamp", "1 minute")
                           .groupBy(
                               window(col("timestamp"), "1 minute"),
                               col("category"),  # FIXED: removed .product
                               col("country")
                           )
                           .agg(
                               count("*").alias("total_views"),
                               approx_count_distinct("user_id").alias("unique_users"),
                               approx_count_distinct("product_id").alias("unique_products"),  # FIXED: removed .product
                               avg("view_duration").alias("avg_view_duration"),
                               avg("price").alias("avg_product_price")  # FIXED: removed .product
                           )
                           .select(
                               col("window.start").alias("window_start"),
                               col("window.end").alias("window_end"),
                               col("category"),
                               col("country"),
                               col("total_views"),
                               col("unique_users"),
                               col("unique_products"),
                               col("avg_view_duration"),
                               col("avg_product_price")
                           ))

        return category_metrics

    def analyze_search_behavior(self, product_views_df: DataFrame) -> DataFrame:
        """Analyze search queries and patterns"""
        search_metrics = (product_views_df
                         .filter(col("event_type") == "search")
                         .withWatermark("timestamp", "5 minutes")
                         .groupBy(
                             window(col("timestamp"), "5 minutes"),
                             col("search_query"),
                             col("country")
                         )
                         .agg(
                             count("*").alias("search_count"),
                             approx_count_distinct("user_id").alias("unique_searchers"),
                             avg("results_count").alias("avg_results")  # ADDED: useful metric
                         )
                         .select(
                             col("window.start").alias("window_start"),
                             col("window.end").alias("window_end"),
                             col("search_query"),
                             col("country"),
                             col("search_count"),
                             col("unique_searchers"),
                             col("avg_results")  # ADDED
                         )
                         # Only popular searches
                         .filter(col("search_count") >= 3))

        return search_metrics

    def analyze_user_sessions(self, product_views_df: DataFrame) -> DataFrame:
        """Analyze user browsing sessions"""
        session_metrics = (product_views_df
                          .withWatermark("timestamp", "10 minutes")
                          .groupBy(
                              window(col("timestamp"), "10 minutes"),
                              col("user_id"),
                              col("session_id"),
                              col("country")
                          )
                          .agg(
                              count(when(col("event_type") == "view", 1)).alias("views"),
                              count(when(col("event_type") == "search", 1)).alias("searches"),
                              count(when(col("event_type") == "filter", 1)).alias("filters"),
                              count(when(col("event_type") == "compare", 1)).alias("compares"),
                              approx_count_distinct("product_id").alias("unique_products_viewed"),  # FIXED: removed .product
                              avg("view_duration").alias("avg_view_duration"),
                              spark_max(col("timestamp")).alias("last_activity"),
                              spark_min(col("timestamp")).alias("first_activity"),
                              count("*").alias("total_events")
                          )
                          .select(
                              col("window.start").alias("window_start"),
                              col("window.end").alias("window_end"),
                              col("user_id"),
                              col("session_id"),
                              col("country"),
                              col("views"),
                              col("searches"),
                              col("filters"),
                              col("compares"),
                              col("unique_products_viewed"),
                              col("avg_view_duration"),
                              col("first_activity"),
                              col("last_activity"),
                              col("total_events"),
                              # Calculate session duration in seconds
                              (col("last_activity").cast("long") - col("first_activity").cast("long")).alias("session_duration_sec")
                          ))

        return session_metrics

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
        Run the product view analyzer

        Args:
            output_mode: 'console' for testing, 'mongodb' for production
        """
        print("=" * 60)
        print("Product View Analyzer Stream Processing")
        print("=" * 60)
        print()

        # Read product view events from Kafka
        product_views_df = self.read_from_kafka()

        # Run analyses
        trending_products = self.analyze_trending_products(product_views_df)
        category_performance = self.analyze_category_performance(product_views_df)
        search_behavior = self.analyze_search_behavior(product_views_df)
        user_sessions = self.analyze_user_sessions(product_views_df)

        # Write streams
        queries = []

        if output_mode == "console":
            query1 = self.write_to_console(trending_products, "trending_products")
            query2 = self.write_to_console(category_performance, "category_performance")
            query3 = self.write_to_console(search_behavior, "search_behavior")
            query4 = self.write_to_console(user_sessions, "user_sessions")
            queries = [query1, query2, query3, query4]

        elif output_mode == "mongodb":
            query1 = self.write_to_mongodb(
                trending_products,
                "trending_products",
                f"{settings.spark.checkpoint_dir}/trending_products"
            )
            query2 = self.write_to_mongodb(
                category_performance,
                "category_performance",
                f"{settings.spark.checkpoint_dir}/category_performance"
            )
            query3 = self.write_to_mongodb(
                search_behavior,
                "search_behavior",
                f"{settings.spark.checkpoint_dir}/search_behavior"
            )
            query4 = self.write_to_mongodb(
                user_sessions,
                "browsing_sessions",
                f"{settings.spark.checkpoint_dir}/browsing_sessions"
            )
            queries = [query1, query2, query3, query4]

        print()
        print("✓ Product view analyzer started")
        print("  Analyzing: Trending products, categories, searches, sessions")
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
    analyzer = ProductViewAnalyzer()
    analyzer.run(output_mode="console")
