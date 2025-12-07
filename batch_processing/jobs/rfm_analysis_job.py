"""
RFM (Recency, Frequency, Monetary) Analysis Batch Job
Segments customers based on transaction behavior
"""
import sys
import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, max as spark_max, count, sum as spark_sum,
    datediff, lit, current_date, when, expr
)
from pyspark.sql.window import Window

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.jobs.base_job import BaseBatchJob


class RFMAnalysisJob(BaseBatchJob):
    """RFM Analysis job for customer segmentation"""

    def __init__(self, analysis_date: str = None):
        """
        Initialize RFM Analysis job

        Args:
            analysis_date: Date of analysis (YYYY-MM-DD), defaults to today
        """
        super().__init__("RFM_Analysis")
        self.analysis_date = analysis_date or datetime.utcnow().strftime("%Y-%m-%d")

    def calculate_rfm_scores(self, df: DataFrame) -> DataFrame:
        """
        Calculate RFM scores (1-5) for each metric

        Args:
            df: DataFrame with recency, frequency, monetary values

        Returns:
            DataFrame: RFM data with scores
        """
        # Calculate quintiles for scoring (1-5)
        # Lower recency is better (scored in reverse)
        # Higher frequency is better
        # Higher monetary is better

        rfm_with_scores = df.select(
            col("user_id"),
            col("analysis_date"),
            col("recency_days"),
            col("frequency_count"),
            col("monetary_value"),
            # Recency score (reverse: lower days = higher score)
            when(col("recency_days") <= 30, 5)
            .when(col("recency_days") <= 60, 4)
            .when(col("recency_days") <= 90, 3)
            .when(col("recency_days") <= 180, 2)
            .otherwise(1).alias("recency_score"),
            # Frequency score
            when(col("frequency_count") >= 10, 5)
            .when(col("frequency_count") >= 7, 4)
            .when(col("frequency_count") >= 4, 3)
            .when(col("frequency_count") >= 2, 2)
            .otherwise(1).alias("frequency_score"),
            # Monetary score
            when(col("monetary_value") >= 1000, 5)
            .when(col("monetary_value") >= 500, 4)
            .when(col("monetary_value") >= 250, 3)
            .when(col("monetary_value") >= 100, 2)
            .otherwise(1).alias("monetary_score")
        )

        # Calculate combined RFM score
        rfm_final = rfm_with_scores.withColumn(
            "rfm_score",
            col("recency_score") * 100 + col("frequency_score") * 10 + col("monetary_score")
        )

        # Assign RFM segments
        rfm_final = rfm_final.withColumn(
            "rfm_segment",
            when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
            .when((col("recency_score") >= 3) & (col("frequency_score") >= 3), "Loyal Customers")
            .when((col("recency_score") >= 4) & (col("frequency_score") <= 2), "Promising")
            .when((col("recency_score") >= 3) & (col("monetary_score") >= 4), "Big Spenders")
            .when((col("recency_score") <= 2) & (col("frequency_score") >= 3), "At Risk")
            .when((col("recency_score") <= 2) & (col("frequency_score") <= 2), "Lost")
            .otherwise("Regular")
        )

        return rfm_final

    def execute(self) -> None:
        """Execute RFM analysis"""
        print("Executing RFM Analysis...")
        print(f"Analysis date: {self.analysis_date}")
        print()

        # Read fact_sales data
        print("Reading fact_sales data...")

        try:
            fact_sales = self.read_from_warehouse(table_name="fact_sales")
            # Filter in Spark
            fact_sales = fact_sales.filter(
                (col("transaction_timestamp").isNotNull()) &
                (col("customer_id").isNotNull())
            )

            if fact_sales.count() == 0:
                print("No sales data found in warehouse")
                print("Skipping RFM analysis")
                return

            print(f"✓ Loaded {fact_sales.count()} sales records")
            print()

            # Calculate RFM metrics
            print("Calculating RFM metrics...")

            # Group by customer and calculate RFM
            rfm_metrics = fact_sales.groupBy("customer_id").agg(
                datediff(lit(self.analysis_date), spark_max("transaction_timestamp")).alias("recency_days"),
                count("transaction_id").alias("frequency_count"),
                spark_sum("total_amount").alias("monetary_value")
            ).withColumn(
                "user_id",
                expr("CONCAT('user_', customer_id)")
            ).withColumn(
                "analysis_date",
                lit(self.analysis_date).cast("date")
            )

            print(f"✓ Calculated RFM for {rfm_metrics.count()} customers")
            print()

            # Calculate RFM scores
            print("Assigning RFM scores and segments...")
            rfm_scored = self.calculate_rfm_scores(rfm_metrics)

            # Select final columns
            rfm_final = rfm_scored.select(
                "user_id",
                "analysis_date",
                "recency_days",
                "frequency_count",
                "monetary_value",
                "recency_score",
                "frequency_score",
                "monetary_score",
                "rfm_score",
                "rfm_segment"
            )

            print(f"✓ Scored {rfm_final.count()} customers")
            print()

            # Show segment distribution
            print("RFM Segment Distribution:")
            rfm_final.groupBy("rfm_segment").count().orderBy(col("count").desc()).show()

            # Load to warehouse
            # Delete existing records for this analysis_date to ensure idempotency
            print("Loading RFM analysis to warehouse...")

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
                    "DELETE FROM rfm_analysis WHERE analysis_date = %s",
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
            self.write_to_warehouse(rfm_final, "rfm_analysis", mode="append")
            print(f"✓ Loaded RFM analysis results")

        except Exception as e:
            print(f"Error during RFM analysis: {str(e)}")
            print("This may be normal if no sales data exists yet")
            raise e


if __name__ == "__main__":
    # Run RFM Analysis
    job = RFMAnalysisJob()
    job.run()
