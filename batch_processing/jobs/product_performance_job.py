"""
Product Performance Analysis Job
Calculates product metrics, rankings, and performance indicators
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    rank, dense_rank, current_date, current_timestamp, when, lit
)
from pyspark.sql.window import Window
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.jobs.base_job import BaseAnalyticsJob


class ProductPerformanceJob(BaseAnalyticsJob):
    """
    Product Performance Analysis Job

    Analyzes product performance based on:
    - Sales metrics (revenue, quantity, transactions)
    - View-to-purchase conversion rates
    - Category rankings
    - Performance trends
    """

    def __init__(self, spark):
        super().__init__(spark, "product_performance")

    def extract(self) -> DataFrame:
        """
        Extract product data from warehouse
        Join fact_sales with dim_products to get business keys
        """
        self.logger.info("Extracting product data from data warehouse")

        try:
            # Read fact_sales
            sales_df = (self.spark.read
                       .format("jdbc")
                       .option("url", self.warehouse_jdbc_url)
                       .option("dbtable", "fact_sales")
                       .option("user", self.warehouse_jdbc_props["user"])
                       .option("password", self.warehouse_jdbc_props["password"])
                       .option("driver", self.warehouse_jdbc_props["driver"])
                       .load())

            # Read dim_products to get product_id and category
            dim_products = (self.spark.read
                           .format("jdbc")
                           .option("url", self.warehouse_jdbc_url)
                           .option("dbtable", "(SELECT product_id_pk, product_id, category, product_name FROM dim_products) as dim_prod")
                           .option("user", self.warehouse_jdbc_props["user"])
                           .option("password", self.warehouse_jdbc_props["password"])
                           .option("driver", self.warehouse_jdbc_props["driver"])
                           .load())

            # Join to get business keys and select columns explicitly
            sales_with_products = (sales_df
                .join(dim_products, "product_id_pk", "left")
                .select(
                    sales_df["*"],
                    dim_products["product_id"],
                    dim_products["category"],
                    dim_products["product_name"]
                ))

            # Read fact_product_views
            views_df = (self.spark.read
                       .format("jdbc")
                       .option("url", self.warehouse_jdbc_url)
                       .option("dbtable", "fact_product_views")
                       .option("user", self.warehouse_jdbc_props["user"])
                       .option("password", self.warehouse_jdbc_props["password"])
                       .option("driver", self.warehouse_jdbc_props["driver"])
                       .load())

            sales_count = sales_with_products.count()
            views_count = views_df.count()

            self.logger.info(f"Extracted {sales_count} sales and {views_count} view records")

            return {"sales": sales_with_products, "views": views_df}

        except Exception as e:
            self.logger.error(f"Error extracting product data: {e}")
            raise

    def analyze(self, data: dict) -> DataFrame:
        """
        Perform product performance analysis

        Metrics:
        - Total sales, revenue, quantity
        - View counts and conversion rates
        - Category rankings
        - Performance indicators
        """
        self.logger.info("Performing product performance analysis")

        sales_df = data["sales"]
        views_df = data["views"]

        if sales_df.count() == 0:
            self.logger.warning("No sales data available for product analysis")
            return self.spark.createDataFrame([], """
                product_id STRING,
                category STRING,
                total_sales BIGINT,
                total_revenue DOUBLE,
                avg_price DOUBLE,
                total_quantity BIGINT,
                total_views BIGINT,
                conversion_rate DOUBLE,
                category_rank INT,
                overall_rank INT,
                performance_score DOUBLE,
                performance_tier STRING,
                analysis_date DATE
            """)

        # Step 1: Aggregate sales metrics by product
        self.logger.info("Step 1: Aggregating sales metrics")

        # Filter out NULLs before aggregation
        sales_clean = sales_df.filter(col("product_id").isNotNull() & col("category").isNotNull())

        sales_metrics = sales_clean.groupBy("product_id", "category").agg(
            count("*").alias("total_sales"),
            spark_sum("total_amount").alias("total_revenue"),
            avg("unit_price").alias("avg_price"),
            spark_sum("quantity").alias("total_quantity")
        )

        # Step 2: Aggregate view metrics by product
        self.logger.info("Step 2: Aggregating view metrics")

        if views_df.count() > 0:
            # Join views with dim_products to get product_id
            dim_products = (self.spark.read
                           .format("jdbc")
                           .option("url", self.warehouse_jdbc_url)
                           .option("dbtable", "(SELECT product_id_pk, product_id FROM dim_products) as dim_prod")
                           .option("user", self.warehouse_jdbc_props["user"])
                           .option("password", self.warehouse_jdbc_props["password"])
                           .option("driver", self.warehouse_jdbc_props["driver"])
                           .load())

            views_with_product = views_df.join(dim_products, "product_id_pk", "left")
            
            view_metrics = views_with_product.groupBy("product_id").agg(
                count("*").alias("total_views")
            )

            # Join sales and views
            product_metrics = sales_metrics.join(
                view_metrics,
                "product_id",
                "left"
            ).fillna(0, ["total_views"])
        else:
            product_metrics = sales_metrics.withColumn("total_views", lit(0))

        # Step 3: Calculate conversion rates
        self.logger.info("Step 3: Calculating conversion rates")

        product_metrics = product_metrics.withColumn(
            "conversion_rate",
            when(col("total_views") > 0, col("total_sales") / col("total_views") * 100)
            .otherwise(0.0)
        )

        # Step 4: Calculate rankings
        self.logger.info("Step 4: Calculating product rankings")

        # Overall ranking by revenue
        overall_window = Window.orderBy(col("total_revenue").desc())

        # Category ranking by revenue
        category_window = Window.partitionBy("category").orderBy(col("total_revenue").desc())

        product_ranked = product_metrics.withColumn(
            "overall_rank", dense_rank().over(overall_window)
        ).withColumn(
            "category_rank", dense_rank().over(category_window)
        )

        # Step 5: Calculate performance score and tier
        self.logger.info("Step 5: Calculating performance scores")

        # Performance score: weighted combination of metrics (normalized to 0-100)
        # 40% revenue, 30% conversion rate, 20% quantity, 10% views
        max_revenue = product_ranked.agg(spark_max("total_revenue")).collect()[0][0] or 1
        max_conversion = product_ranked.agg(spark_max("conversion_rate")).collect()[0][0] or 1
        max_quantity = product_ranked.agg(spark_max("total_quantity")).collect()[0][0] or 1
        max_views = product_ranked.agg(spark_max("total_views")).collect()[0][0] or 1

        product_scored = product_ranked.withColumn(
            "performance_score",
            (
                (col("total_revenue") / max_revenue * 40) +
                (col("conversion_rate") / max_conversion * 30) +
                (col("total_quantity") / max_quantity * 20) +
                (col("total_views") / max_views * 10)
            )
        ).withColumn(
            "performance_tier",
            when(col("performance_score") >= 80, "Top Performer")
            .when(col("performance_score") >= 60, "Strong Performer")
            .when(col("performance_score") >= 40, "Average Performer")
            .when(col("performance_score") >= 20, "Underperformer")
            .otherwise("Low Performer")
        ).withColumn(
            "analysis_date", current_date()
        )

        # Log performance tier distribution
        tier_counts = product_scored.groupBy("performance_tier").count().collect()
        self.logger.info("Product Performance Tier Distribution:")
        for row in tier_counts:
            self.logger.info(f"  {row['performance_tier']}: {row['count']} products")

        return product_scored

    def load(self, df: DataFrame) -> None:
        """
        Load product performance results to warehouse
        Truncates and reloads the table (full refresh)
        """
        self.logger.info("Loading product performance results to warehouse")

        if df.count() == 0:
            self.logger.warning("No product performance results to load")
            return

        # Select columns for target table
        columns_to_insert = [
            "product_id", "category",
            "total_sales", "total_revenue", "avg_price", "total_quantity",
            "total_views", "conversion_rate",
            "category_rank", "overall_rank",
            "performance_score", "performance_tier",
            "analysis_date"
        ]

        df_to_load = df.select(columns_to_insert)

        try:
            # Truncate existing data
            self.logger.info("Truncating existing product performance data")
            with self.connection.get_warehouse_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("TRUNCATE TABLE product_performance")
                    conn.commit()

            # Load new analysis results
            (df_to_load.write
             .format("jdbc")
             .option("url", self.warehouse_jdbc_url)
             .option("dbtable", "product_performance")
             .option("user", self.warehouse_jdbc_props["user"])
             .option("password", self.warehouse_jdbc_props["password"])
             .option("driver", self.warehouse_jdbc_props["driver"])
             .mode("append")
             .save())

            self.stats["rows_inserted"] = df_to_load.count()
            self.logger.info(f"Loaded {self.stats['rows_inserted']} product performance records")

        except Exception as e:
            self.logger.error(f"Error loading product performance results: {e}")
            raise