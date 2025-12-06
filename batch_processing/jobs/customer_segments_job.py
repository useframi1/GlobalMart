"""
Customer Segments Analysis Job
Analyzes customer segment distributions and characteristics
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    current_date, current_timestamp, lit
)
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.jobs.base_job import BaseAnalyticsJob


class CustomerSegmentsJob(BaseAnalyticsJob):
    """
    Customer Segments Analysis Job

    Analyzes customer segment characteristics:
    - Segment sizes and distributions
    - Segment value metrics
    - Geographic distribution by segment
    - Segment performance indicators

    Depends on RFM Analysis job completing first
    """

    def __init__(self, spark):
        super().__init__(spark, "customer_segments")

    def extract(self) -> DataFrame:
        """
        Extract customer and RFM data from warehouse
        """
        self.logger.info("Extracting customer segment data from data warehouse")

        try:
            # Join customer dimension with RFM analysis
            query = """
                (SELECT
                    dc.user_id,
                    dc.first_transaction_date,
                    dc.last_transaction_date,
                    dc.total_transactions,
                    dc.total_spent,
                    dc.avg_order_value,
                    dc.customer_segment,
                    rfm.recency_days,
                    rfm.frequency_count,
                    rfm.monetary_value,
                    rfm.rfm_score,
                    rfm.rfm_segment
                FROM dim_customers dc
                LEFT JOIN rfm_analysis rfm ON dc.user_id = rfm.user_id
                WHERE dc.is_current = TRUE) as segment_data
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
            self.logger.info(f"Extracted {self.stats['rows_processed']} customer records with segments")

            return df

        except Exception as e:
            self.logger.error(f"Error extracting customer segment data: {e}")
            # Return empty DataFrame
            return self.spark.createDataFrame([], """
                user_id STRING,
                country STRING,
                customer_segment STRING,
                rfm_segment STRING,
                total_spent DOUBLE,
                total_transactions BIGINT
            """)

    def analyze(self, df: DataFrame) -> DataFrame:
        """
        Perform customer segment analysis

        Calculates:
        - Segment size and customer count
        - Segment value metrics (total/avg revenue, transactions)
        - Geographic distribution
        - Segment characteristics
        """
        self.logger.info("Performing customer segment analysis")

        if df.count() == 0:
            self.logger.warning("No customer data available for segment analysis")
            return self.spark.createDataFrame([], """
                segment_name STRING,
                segment_type STRING,
                customer_count BIGINT,
                total_revenue DOUBLE,
                avg_revenue_per_customer DOUBLE,
                total_transactions BIGINT,
                avg_transactions_per_customer DOUBLE,
                avg_order_value DOUBLE,
                avg_recency_days DOUBLE,
                primary_country STRING,
                analysis_date DATE
            """)

        # Analyze RFM segments
        self.logger.info("Analyzing RFM segments")

        rfm_segments = df.filter(col("rfm_segment").isNotNull()).groupBy("rfm_segment").agg(
            count("user_id").alias("customer_count"),
            spark_sum("total_spent").alias("total_revenue"),
            avg("total_spent").alias("avg_revenue_per_customer"),
            spark_sum("total_transactions").alias("total_transactions"),
            avg("total_transactions").alias("avg_transactions_per_customer"),
            avg("avg_order_value").alias("avg_order_value"),
            avg("recency_days").alias("avg_recency_days")
        ).withColumn("segment_type", lit("RFM"))\
         .withColumnRenamed("rfm_segment", "segment_name")

        # Get primary country for each segment (most common country in that segment)
        country_by_segment = df.filter(col("rfm_segment").isNotNull()).groupBy("rfm_segment", "country")\
                               .agg(count("user_id").alias("country_count"))

        window_spec = Window.partitionBy("rfm_segment").orderBy(col("country_count").desc())
        primary_countries = country_by_segment.withColumn("rank", rank().over(window_spec))\
                                             .filter(col("rank") == 1)\
                                             .select(col("rfm_segment").alias("segment_name"),
                                                    col("country").alias("primary_country"))

        # Join primary countries
        rfm_segments = rfm_segments.join(primary_countries, "segment_name", "left")

        # Also analyze by geographic segments (country)
        self.logger.info("Analyzing geographic segments")

        from pyspark.sql.window import Window
        from pyspark.sql.functions import rank

        geo_segments = df.groupBy("country").agg(
            count("user_id").alias("customer_count"),
            spark_sum("total_spent").alias("total_revenue"),
            avg("total_spent").alias("avg_revenue_per_customer"),
            spark_sum("total_transactions").alias("total_transactions"),
            avg("total_transactions").alias("avg_transactions_per_customer"),
            avg("avg_order_value").alias("avg_order_value"),
            avg("recency_days").alias("avg_recency_days")
        ).withColumn("segment_type", lit("Geographic"))\
         .withColumnRenamed("country", "segment_name")\
         .withColumn("primary_country", col("segment_name"))

        # Combine both segment types
        all_segments = rfm_segments.unionByName(geo_segments, allowMissingColumns=True)\
                                   .withColumn("analysis_date", current_date())

        # Select final columns
        final_segments = all_segments.select(
            "segment_name", "segment_type",
            "customer_count", "total_revenue",
            "avg_revenue_per_customer", "total_transactions",
            "avg_transactions_per_customer", "avg_order_value",
            "avg_recency_days", "primary_country",
            "analysis_date"
        )

        # Log segment statistics
        segment_counts = final_segments.groupBy("segment_type").agg(
            count("segment_name").alias("num_segments"),
            spark_sum("customer_count").alias("total_customers")
        ).collect()

        self.logger.info("Customer Segment Analysis Summary:")
        for row in segment_counts:
            self.logger.info(f"  {row['segment_type']}: {row['num_segments']} segments, {row['total_customers']} customers")

        return final_segments

    def load(self, df: DataFrame) -> None:
        """
        Load customer segment analysis results to warehouse
        Truncates and reloads the table (full refresh)
        """
        self.logger.info("Loading customer segments results to warehouse")

        if df.count() == 0:
            self.logger.warning("No customer segment results to load")
            return

        try:
            # Truncate existing data
            self.logger.info("Truncating existing customer segments data")
            with self.connection.get_warehouse_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("TRUNCATE TABLE customer_segments")
                    conn.commit()

            # Load new analysis results
            (df.write
             .format("jdbc")
             .option("url", self.warehouse_jdbc_url)
             .option("dbtable", "customer_segments")
             .option("user", self.warehouse_jdbc_props["user"])
             .option("password", self.warehouse_jdbc_props["password"])
             .option("driver", self.warehouse_jdbc_props["driver"])
             .mode("append")
             .save())

            self.stats["rows_inserted"] = df.count()
            self.logger.info(f"Loaded {self.stats['rows_inserted']} customer segment records")

        except Exception as e:
            self.logger.error(f"Error loading customer segments results: {e}")
            raise
