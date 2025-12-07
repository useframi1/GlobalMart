"""
Sales Trends Analysis Batch Job
Analyzes sales trends by time period and category
"""
import sys
import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, year, month, quarter, weekofyear,
    date_format, lit, lag, when
)
from pyspark.sql.window import Window

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.jobs.base_job import BaseBatchJob


class SalesTrendsJob(BaseBatchJob):
    """Sales Trends Analysis job"""

    def __init__(self, analysis_date: str = None):
        """
        Initialize Sales Trends Analysis job

        Args:
            analysis_date: Date of analysis (YYYY-MM-DD), defaults to today
        """
        super().__init__("Sales_Trends")
        self.analysis_date = analysis_date or datetime.utcnow().strftime("%Y-%m-%d")

    def calculate_trends(
        self,
        df: DataFrame,
        period_type: str,
        period_col: str
    ) -> DataFrame:
        """
        Calculate sales trends for a time period

        Args:
            df: Input sales DataFrame
            period_type: Type of period (daily, weekly, monthly, quarterly, yearly)
            period_col: Column containing the period value

        Returns:
            DataFrame: Trend analysis results
        """
        # Aggregate by period and category
        trend_data = df.groupBy(period_col, "category").agg(
            count("*").alias("total_sales"),
            spark_sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            spark_sum("quantity").alias("total_quantity")
        ).withColumn(
            "period_type",
            lit(period_type)
        ).withColumnRenamed(
            period_col,
            "period_value"
        ).withColumn(
            "analysis_date",
            lit(self.analysis_date).cast("date")
        )

        # Calculate growth rate (vs previous period)
        window_spec = Window.partitionBy("category").orderBy("period_value")
        trend_with_growth = trend_data.withColumn(
            "prev_revenue",
            lag("total_revenue", 1).over(window_spec)
        ).withColumn(
            "growth_rate",
            when(col("prev_revenue").isNotNull(),
                 ((col("total_revenue") - col("prev_revenue")) / col("prev_revenue")) * 100
            ).otherwise(None)
        ).drop("prev_revenue")

        # Cast period_value to string for consistency
        trend_final = trend_with_growth.withColumn(
            "period_value",
            col("period_value").cast("string")
        )

        return trend_final

    def execute(self) -> None:
        """Execute sales trends analysis"""
        print("Executing Sales Trends Analysis...")
        print(f"Analysis date: {self.analysis_date}")
        print()

        # Read sales data with product info
        print("Reading sales data...")

        try:
            # Read both tables
            fact_sales = self.read_from_warehouse(table_name="fact_sales")
            # Filter for current products only (SCD Type 2)
            dim_products = self.read_from_warehouse(table_name="dim_products").filter(col("is_current") == True)

            # Join and filter in Spark
            sales_df = fact_sales.join(
                dim_products,
                fact_sales.product_id_pk == dim_products.product_id_pk,
                "left"
            ).select(
                fact_sales.transaction_timestamp,
                fact_sales.total_amount,
                fact_sales.quantity,
                fact_sales.product_id_pk,
                dim_products.category
            ).filter(col("transaction_timestamp").isNotNull())

            if sales_df.count() == 0:
                print("No sales data found")
                print("Skipping sales trends analysis")
                return

            print(f"✓ Loaded {sales_df.count()} sales records")
            print()

            # Add time period columns
            sales_with_periods = sales_df.select(
                "*",
                year("transaction_timestamp").alias("year_period"),
                quarter("transaction_timestamp").alias("quarter_period"),
                date_format("transaction_timestamp", "yyyy-MM").alias("month_period"),
                weekofyear("transaction_timestamp").alias("week_period"),
                date_format("transaction_timestamp", "yyyy-MM-dd").alias("day_period")
            )

            # Calculate trends for different periods
            all_trends = []

            print("Calculating monthly trends...")
            monthly_trends = self.calculate_trends(sales_with_periods, "monthly", "month_period")
            all_trends.append(monthly_trends)
            print(f"✓ Calculated {monthly_trends.count()} monthly trend records")

            print("Calculating weekly trends...")
            weekly_trends = self.calculate_trends(sales_with_periods, "weekly", "week_period")
            all_trends.append(weekly_trends)
            print(f"✓ Calculated {weekly_trends.count()} weekly trend records")

            print("Calculating quarterly trends...")
            quarterly_trends = self.calculate_trends(sales_with_periods, "quarterly", "quarter_period")
            all_trends.append(quarterly_trends)
            print(f"✓ Calculated {quarterly_trends.count()} quarterly trend records")

            # Combine all trends
            combined_trends = all_trends[0]
            for trend_df in all_trends[1:]:
                combined_trends = combined_trends.union(trend_df)

            print(f"\n✓ Total trend records: {combined_trends.count()}")
            print()

            # Select final columns
            trends_final = combined_trends.select(
                "period_type",
                "period_value",
                "category",
                "total_sales",
                "total_revenue",
                "avg_order_value",
                "total_quantity",
                "growth_rate",
                "analysis_date"
            )

            # Show sample trends
            print("Sample Trends (Top 10 by revenue):")
            trends_final.orderBy(col("total_revenue").desc()).show(10, truncate=False)

            # Load to warehouse
            # Delete existing records for this analysis_date to ensure idempotency
            print("Loading sales trends to warehouse...")

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
                    "DELETE FROM sales_trends WHERE analysis_date = %s",
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
            self.write_to_warehouse(trends_final, "sales_trends", mode="append")
            print("✓ Loaded sales trends results")

        except Exception as e:
            print(f"Error during sales trends analysis: {str(e)}")
            raise e


if __name__ == "__main__":
    # Run Sales Trends Analysis
    job = SalesTrendsJob()
    job.run()
