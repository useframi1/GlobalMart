"""
Product Performance Analysis Batch Job
Analyzes product sales performance and conversion metrics
"""
import sys
import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, dense_rank, when, lit, to_date
)
from pyspark.sql.window import Window

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.jobs.base_job import BaseBatchJob


class ProductPerformanceJob(BaseBatchJob):
    """Product Performance Analysis job"""

    def __init__(self, analysis_date: str = None):
        """
        Initialize Product Performance Analysis job

        Args:
            analysis_date: Date of analysis (YYYY-MM-DD), defaults to today
        """
        super().__init__("Product_Performance")
        self.analysis_date = analysis_date or datetime.utcnow().strftime("%Y-%m-%d")

    def execute(self) -> None:
        """Execute product performance analysis"""
        print("Executing Product Performance Analysis...")
        print(f"Analysis date: {self.analysis_date}")
        print()

        # Read product dimension (filter for current records only)
        print("Reading product dimension...")
        dim_products = self.read_from_warehouse("dim_products").filter(col("is_current") == True)
        print(f"✓ Loaded {dim_products.count()} current products")
        print()

        # Read sales fact data
        print("Reading sales data...")

        try:
            fact_sales = self.read_from_warehouse(table_name="fact_sales")
            # Filter in Spark
            fact_sales = fact_sales.filter(
                (col("transaction_timestamp").isNotNull()) &
                (col("product_id_pk").isNotNull())
            )

            if fact_sales.count() == 0:
                print("No sales data found")
                print("Skipping product performance analysis")
                return

            print(f"✓ Loaded {fact_sales.count()} sales records")
            print()

            # Calculate product performance metrics
            print("Calculating product metrics...")

            # Join with product dimension
            sales_with_products = fact_sales.join(
                dim_products,
                fact_sales.product_id_pk == dim_products.product_id_pk,
                "inner"
            )

            # Aggregate by product
            product_metrics = sales_with_products.groupBy(
                "product_id", "category"
            ).agg(
                count("*").alias("total_sales"),
                spark_sum("total_amount").alias("total_revenue"),
                spark_sum("quantity").alias("total_quantity"),
                avg("unit_price").alias("avg_price")
            ).withColumn(
                "analysis_date",
                lit(self.analysis_date).cast("date")
            )

            # Try to get product view counts from fact_product_views table
            try:
                fact_product_views = self.read_from_warehouse(table_name="fact_product_views")

                # Aggregate views by product_id
                product_views = fact_product_views.alias("v") \
                    .join(dim_products.select("product_id_pk", "product_id").alias("p"),
                          col("v.product_id_pk") == col("p.product_id_pk"), "inner") \
                    .groupBy("p.product_id").agg(
                        count("*").alias("total_views")
                    )

                # Left join to add view counts
                product_metrics = product_metrics.alias("m").join(
                    product_views.alias("v"),
                    col("m.product_id") == col("v.product_id"),
                    "left"
                ).select(
                    col("m.*"),
                    when(col("v.total_views").isNotNull(), col("v.total_views"))
                    .otherwise(0).alias("total_views")
                )

                # Calculate conversion rate (sales / views) as percentage
                product_metrics = product_metrics.withColumn(
                    "conversion_rate",
                    when(col("total_views") > 0, (col("total_sales") / col("total_views")) * 100.0)
                    .otherwise(None).cast("double")
                )

                print(f"  ✓ Enriched with product view data from warehouse")

            except Exception as e:
                print(f"  Note: Could not fetch product views from warehouse: {str(e)}")
                print(f"  Using default values for total_views and conversion_rate")
                product_metrics = product_metrics.withColumn(
                    "total_views",
                    lit(0)
                ).withColumn(
                    "conversion_rate",
                    lit(None).cast("double")
                )

            # Calculate category ranks
            category_window = Window.partitionBy("category").orderBy(col("total_revenue").desc())
            overall_window = Window.orderBy(col("total_revenue").desc())

            product_ranked = product_metrics.withColumn(
                "category_rank",
                dense_rank().over(category_window)
            ).withColumn(
                "overall_rank",
                dense_rank().over(overall_window)
            )

            # Calculate performance score and tier
            product_final = product_ranked.withColumn(
                "performance_score",
                (col("total_revenue") / 1000.0 + col("total_sales") / 10.0)
            ).withColumn(
                "performance_tier",
                when(col("overall_rank") <= 10, "Top Seller")
                .when(col("overall_rank") <= 50, "High Performer")
                .when(col("overall_rank") <= 200, "Moderate")
                .otherwise("Low Performer")
            )

            # Select final columns
            performance_final = product_final.select(
                "product_id",
                "category",
                "analysis_date",
                "total_sales",
                "total_revenue",
                "total_quantity",
                "total_views",
                "conversion_rate",
                "avg_price",
                "category_rank",
                "overall_rank",
                "performance_score",
                "performance_tier"
            )

            print(f"✓ Analyzed {performance_final.count()} products")
            print()

            # Show performance tier distribution
            print("Performance Tier Distribution:")
            performance_final.groupBy("performance_tier").count().orderBy(col("count").desc()).show()

            # Load to warehouse
            # Delete existing records for this analysis_date to ensure idempotency
            print("Loading product performance to warehouse...")

            # First, delete existing records for this analysis date
            import psycopg2
            from config.settings import settings

            conn = psycopg2.connect(
                host=settings.postgres_warehouse.host,
                port=settings.postgres_warehouse.port,
                database=settings.postgres_warehouse.database,
                user=settings.postgres_warehouse.user,
                password=settings.postgres_warehouse.password
            )

            try:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM product_performance WHERE analysis_date = %s",
                    (self.analysis_date,)
                )
                deleted_count = cursor.rowcount
                conn.commit()
                if deleted_count > 0:
                    print(f"  Deleted {deleted_count} existing records for {self.analysis_date}")
                cursor.close()
            finally:
                conn.close()

            # Now insert new records
            self.write_to_warehouse(performance_final, "product_performance", mode="append")
            print("✓ Loaded product performance results")

        except Exception as e:
            print(f"Error during product performance analysis: {str(e)}")
            raise e


if __name__ == "__main__":
    # Run Product Performance Analysis
    job = ProductPerformanceJob()
    job.run()
