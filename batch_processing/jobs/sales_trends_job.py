"""
Sales Trends Analysis Job
Analyzes sales patterns across time and geography
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    current_date, current_timestamp, lag, when, lit
)
from pyspark.sql.window import Window
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.jobs.base_job import BaseAnalyticsJob


class SalesTrendsJob(BaseAnalyticsJob):
    """
    Sales Trends Analysis Job

    Analyzes sales trends by:
    - Time dimensions (day, week, month, quarter)
    - Geographic dimensions (country, region)
    - Category performance over time
    - Growth rates and comparisons
    """

    def __init__(self, spark):
        super().__init__(spark, "sales_trends")

    def extract(self) -> DataFrame:
        """
        Extract sales data with time and geography dimensions
        """
        self.logger.info("Extracting sales data from data warehouse")

        try:
            # Join sales fact with date and geography dimensions
            query = """
                (SELECT
                    fs.transaction_timestamp,
                    fs.product_id,
                    fs.category,
                    fs.total_revenue,
                    fs.quantity,
                    dd.full_date,
                    dd.year,
                    dd.quarter,
                    dd.month,
                    dd.month_name,
                    dd.week,
                    dd.day_of_week,
                    dd.day_name,
                    dd.is_weekend,
                    COALESCE(dc.country, 'Unknown') as country
                FROM fact_sales fs
                LEFT JOIN dim_date dd ON fs.date_key = dd.date_key
                LEFT JOIN dim_customers dc ON fs.customer_sk = dc.customer_sk
                WHERE dd.full_date IS NOT NULL) as sales_trends_data
            """

            df = (self.spark.read
                  .format("jdbc")
                  .option("url", self.warehouse_jdbc_url)
                  .option("dbtable", query)
                  .option("user", self.warehouse_jdbc_props["user"])
                  .option("password", self.warehouse_jdbc_props["password"])
                  .option("driver", self.warehouse_jdbc_props["driver"])
                  .load())

            self.stats["rows_processed"] = df.count()
            self.logger.info(f"Extracted {self.stats['rows_processed']} sales records with dimensions")

            return df

        except Exception as e:
            self.logger.error(f"Error extracting sales trends data: {e}")
            # Return empty DataFrame
            return self.spark.createDataFrame([], """
                full_date DATE,
                year INT,
                quarter INT,
                month INT,
                week INT,
                country STRING,
                category STRING,
                total_revenue DOUBLE,
                quantity BIGINT
            """)

    def analyze(self, df: DataFrame) -> DataFrame:
        """
        Perform sales trends analysis

        Creates multiple aggregation levels:
        - Daily trends by country and category
        - Weekly/Monthly/Quarterly aggregations
        - Growth rate calculations
        """
        self.logger.info("Performing sales trends analysis")

        if df.count() == 0:
            self.logger.warning("No sales data available for trends analysis")
            return self.spark.createDataFrame([], """
                period_type STRING,
                period_value STRING,
                country STRING,
                category STRING,
                total_sales BIGINT,
                total_revenue DOUBLE,
                avg_order_value DOUBLE,
                total_quantity BIGINT,
                growth_rate DOUBLE,
                analysis_date DATE
            """)

        # Step 1: Daily trends
        self.logger.info("Step 1: Calculating daily trends")

        daily_trends = df.groupBy(
            "full_date", "year", "quarter", "month", "week",
            "country", "category"
        ).agg(
            count("*").alias("total_sales"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("total_revenue").alias("avg_order_value"),
            spark_sum("quantity").alias("total_quantity")
        ).withColumn("period_type", lit("daily"))\
         .withColumn("period_value", col("full_date").cast("string"))

        # Step 2: Weekly trends
        self.logger.info("Step 2: Calculating weekly trends")

        weekly_trends = df.groupBy(
            "year", "week", "country", "category"
        ).agg(
            count("*").alias("total_sales"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("total_revenue").alias("avg_order_value"),
            spark_sum("quantity").alias("total_quantity")
        ).withColumn("period_type", lit("weekly"))\
         .withColumn("period_value",
                    when(col("week") < 10,
                         col("year").cast("string") + "-W0" + col("week").cast("string"))
                    .otherwise(col("year").cast("string") + "-W" + col("week").cast("string")))\
         .withColumn("quarter", lit(None).cast("int"))\
         .withColumn("month", lit(None).cast("int"))\
         .withColumn("full_date", lit(None).cast("date"))

        # Step 3: Monthly trends
        self.logger.info("Step 3: Calculating monthly trends")

        monthly_trends = df.groupBy(
            "year", "quarter", "month", "country", "category"
        ).agg(
            count("*").alias("total_sales"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("total_revenue").alias("avg_order_value"),
            spark_sum("quantity").alias("total_quantity")
        ).withColumn("period_type", lit("monthly"))\
         .withColumn("period_value",
                    when(col("month") < 10,
                         col("year").cast("string") + "-0" + col("month").cast("string"))
                    .otherwise(col("year").cast("string") + "-" + col("month").cast("string")))\
         .withColumn("week", lit(None).cast("int"))\
         .withColumn("full_date", lit(None).cast("date"))

        # Step 4: Quarterly trends
        self.logger.info("Step 4: Calculating quarterly trends")

        quarterly_trends = df.groupBy(
            "year", "quarter", "country", "category"
        ).agg(
            count("*").alias("total_sales"),
            spark_sum("total_revenue").alias("total_revenue"),
            avg("total_revenue").alias("avg_order_value"),
            spark_sum("quantity").alias("total_quantity")
        ).withColumn("period_type", lit("quarterly"))\
         .withColumn("period_value", col("year").cast("string") + "-Q" + col("quarter").cast("string"))\
         .withColumn("month", lit(None).cast("int"))\
         .withColumn("week", lit(None).cast("int"))\
         .withColumn("full_date", lit(None).cast("date"))

        # Step 5: Union all trends
        self.logger.info("Step 5: Combining all trend levels")

        all_trends = daily_trends.unionByName(weekly_trends, allowMissingColumns=True)\
                                 .unionByName(monthly_trends, allowMissingColumns=True)\
                                 .unionByName(quarterly_trends, allowMissingColumns=True)

        # Step 6: Calculate growth rates (period-over-period)
        self.logger.info("Step 6: Calculating growth rates")

        # Window for lag calculation (by period type, country, category)
        window_spec = Window.partitionBy("period_type", "country", "category")\
                            .orderBy("period_value")

        trends_with_growth = all_trends.withColumn(
            "prev_revenue", lag("total_revenue", 1).over(window_spec)
        ).withColumn(
            "growth_rate",
            when(col("prev_revenue").isNotNull() & (col("prev_revenue") > 0),
                 ((col("total_revenue") - col("prev_revenue")) / col("prev_revenue") * 100))
            .otherwise(0.0)
        ).drop("prev_revenue")\
         .withColumn("analysis_date", current_date())

        # Select final columns
        final_trends = trends_with_growth.select(
            "period_type", "period_value",
            "country", "category",
            "total_sales", "total_revenue",
            "avg_order_value", "total_quantity",
            "growth_rate", "analysis_date"
        )

        # Log some statistics
        trend_counts = final_trends.groupBy("period_type").count().collect()
        self.logger.info("Sales Trends by Period Type:")
        for row in trend_counts:
            self.logger.info(f"  {row['period_type']}: {row['count']} records")

        return final_trends

    def load(self, df: DataFrame) -> None:
        """
        Load sales trends results to warehouse
        Truncates and reloads the table (full refresh)
        """
        self.logger.info("Loading sales trends results to warehouse")

        if df.count() == 0:
            self.logger.warning("No sales trends results to load")
            return

        try:
            # Truncate existing data
            self.logger.info("Truncating existing sales trends data")
            with self.connection.get_warehouse_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("TRUNCATE TABLE sales_trends")
                    conn.commit()

            # Load new analysis results
            (df.write
             .format("jdbc")
             .option("url", self.warehouse_jdbc_url)
             .option("dbtable", "sales_trends")
             .option("user", self.warehouse_jdbc_props["user"])
             .option("password", self.warehouse_jdbc_props["password"])
             .option("driver", self.warehouse_jdbc_props["driver"])
             .mode("append")
             .save())

            self.stats["rows_inserted"] = df.count()
            self.logger.info(f"Loaded {self.stats['rows_inserted']} sales trends records")

        except Exception as e:
            self.logger.error(f"Error loading sales trends results: {e}")
            raise
