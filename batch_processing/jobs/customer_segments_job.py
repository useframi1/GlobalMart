"""
Customer Segmentation Batch Job
Aggregates RFM analysis results into segment-level statistics
"""
import sys
import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg,
    min as spark_min, max as spark_max,
    current_timestamp, lit
)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.jobs.base_job import BaseBatchJob


class CustomerSegmentsJob(BaseBatchJob):
    """Customer Segmentation job for aggregating RFM segments"""

    # Segment definitions based on RFM analysis logic
    SEGMENT_DEFINITIONS = {
        "Champions": {
            "description": "Best customers with high recency, frequency, and monetary value",
            "min_rfm": 444,
            "max_rfm": 555
        },
        "Loyal Customers": {
            "description": "Consistent customers with good recency and frequency",
            "min_rfm": 333,
            "max_rfm": 555
        },
        "Big Spenders": {
            "description": "Customers with high monetary value",
            "min_rfm": 300,
            "max_rfm": 555
        },
        "Promising": {
            "description": "Recent customers with low frequency",
            "min_rfm": 411,
            "max_rfm": 455
        },
        "At Risk": {
            "description": "Customers who haven't purchased recently but had good frequency",
            "min_rfm": 100,
            "max_rfm": 299
        },
        "Lost": {
            "description": "Inactive customers with low recency and frequency",
            "min_rfm": 100,
            "max_rfm": 222
        },
        "Regular": {
            "description": "Average customers who don't fit other segments",
            "min_rfm": 100,
            "max_rfm": 555
        }
    }

    def __init__(self, analysis_date: str = None):
        """
        Initialize Customer Segmentation job

        Args:
            analysis_date: Date of analysis (YYYY-MM-DD), defaults to today
        """
        super().__init__("Customer_Segmentation")
        self.analysis_date = analysis_date or datetime.utcnow().strftime("%Y-%m-%d")

    def execute(self) -> None:
        """Execute customer segmentation analysis"""
        print("Executing Customer Segmentation Analysis...")
        print(f"Analysis date: {self.analysis_date}")
        print()

        try:
            # Read RFM analysis data for the analysis date
            print("Reading RFM analysis data...")
            rfm_data = self.read_from_warehouse(table_name="rfm_analysis")
            rfm_data = rfm_data.filter(col("analysis_date") == lit(self.analysis_date))

            rfm_count = rfm_data.count()
            if rfm_count == 0:
                print(f"No RFM analysis data found for {self.analysis_date}")
                print("Skipping customer segmentation")
                return

            print(f"✓ Loaded {rfm_count} RFM records")
            print()

            # Aggregate by segment
            print("Aggregating customer segments...")
            # RFM data already has frequency_count (number of transactions per customer)
            # and monetary_value (total spent per customer)
            segment_stats = rfm_data.groupBy("rfm_segment").agg(
                count("user_id").alias("customer_count"),
                spark_sum("monetary_value").alias("total_revenue"),
                avg("monetary_value").alias("avg_customer_value"),
                spark_sum("frequency_count").alias("total_transactions"),
                spark_min("rfm_score").alias("min_rfm_score"),
                spark_max("rfm_score").alias("max_rfm_score")
            )

            # Calculate avg_order_value (total_revenue / total transactions)
            segment_final = segment_stats.select(
                col("rfm_segment").alias("segment_name"),
                col("customer_count"),
                col("total_revenue"),
                col("avg_customer_value"),
                col("min_rfm_score"),
                col("max_rfm_score"),
                (col("total_revenue") / col("total_transactions")).alias("avg_order_value")
            )

            # Add segment descriptions using a single cascading when statement
            from pyspark.sql.functions import when

            # Build cascading when statement
            desc_expr = None
            for segment_name, segment_info in self.SEGMENT_DEFINITIONS.items():
                if desc_expr is None:
                    desc_expr = when(col("segment_name") == segment_name, lit(segment_info["description"]))
                else:
                    desc_expr = desc_expr.when(col("segment_name") == segment_name, lit(segment_info["description"]))

            # Add otherwise clause with a default value
            desc_expr = desc_expr.otherwise(lit("Unknown segment"))

            segment_with_desc = segment_final.withColumn("segment_description", desc_expr)

            # Add last_updated timestamp
            segment_with_desc = segment_with_desc.withColumn(
                "last_updated",
                current_timestamp()
            )

            # Select final columns in correct order
            segment_result = segment_with_desc.select(
                "segment_name",
                "segment_description",
                "min_rfm_score",
                "max_rfm_score",
                "customer_count",
                "total_revenue",
                "avg_order_value",
                "last_updated"
            )

            segment_count = segment_result.count()
            print(f"✓ Aggregated {segment_count} customer segments")
            print()

            # Show segment distribution
            print("Customer Segment Statistics:")
            segment_result.select(
                "segment_name",
                "customer_count",
                "total_revenue",
                "avg_order_value"
            ).orderBy(col("customer_count").desc()).show(truncate=False)

            # Load to warehouse using delete-before-insert pattern
            print("Loading customer segments to warehouse...")

            # Delete all existing records (segments are re-calculated each time)
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
                cursor.execute("DELETE FROM customer_segments")
                deleted_count = cursor.rowcount
                conn.commit()
                if deleted_count > 0:
                    print(f"  Deleted {deleted_count} existing segment records")
                cursor.close()
            finally:
                conn.close()

            # Insert new segment data
            self.write_to_warehouse(segment_result, "customer_segments", mode="append")
            print(f"✓ Loaded {segment_count} customer segments")

        except Exception as e:
            print(f"Error during customer segmentation: {str(e)}")
            print("This may be normal if no RFM analysis data exists yet")
            raise e


if __name__ == "__main__":
    # Run Customer Segmentation
    job = CustomerSegmentsJob()
    job.run()
