"""
RFM Analysis Job
Segments customers based on Recency, Frequency, and Monetary value
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, max as spark_max, min as spark_min,
    datediff, current_date, ntile, expr, when, lit, current_timestamp
)
from pyspark.sql.window import Window
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.jobs.base_job import BaseAnalyticsJob


class RFMAnalysisJob(BaseAnalyticsJob):
    """
    RFM (Recency, Frequency, Monetary) Analysis Job

    Segments customers based on:
    - Recency: Days since last transaction
    - Frequency: Total number of transactions
    - Monetary: Total amount spent

    Uses quintile-based scoring (1-5 scale) to create composite RFM score
    and segment classifications.
    """

    def __init__(self, spark):
        super().__init__(spark, "rfm_analysis")

    def extract(self) -> DataFrame:
        """
        Extract sales data from warehouse for RFM analysis
        """
        self.logger.info("Extracting sales data from data warehouse")

        try:
            # Read sales fact table
            sales_df = (self.spark.read
                       .format("jdbc")
                       .option("url", self.warehouse_jdbc_url)
                       .option("dbtable", "fact_sales")
                       .option("user", self.warehouse_jdbc_props["user"])
                       .option("password", self.warehouse_jdbc_props["password"])
                       .option("driver", self.warehouse_jdbc_props["driver"])
                       .load())

            self.stats["rows_processed"] = sales_df.count()
            self.logger.info(f"Extracted {self.stats['rows_processed']} sales records for RFM analysis")

            return sales_df

        except Exception as e:
            self.logger.error(f"Error extracting sales data: {e}")
            # Return empty DataFrame with minimal schema
            return self.spark.createDataFrame([], """
                user_id STRING,
                transaction_timestamp TIMESTAMP,
                total_revenue DOUBLE
            """)

    def analyze(self, df: DataFrame) -> DataFrame:
        """
        Perform RFM analysis on customer transaction data

        Algorithm:
        1. Calculate RFM metrics per customer
           - Recency: Days since last transaction
           - Frequency: Count of transactions
           - Monetary: Sum of revenue
        2. Score using quintiles (1-5 scale)
        3. Create composite RFM score (111-555)
        4. Classify into segments (Champions, Loyal, etc.)
        """
        self.logger.info("Performing RFM analysis")

        if df.count() == 0:
            self.logger.warning("No sales data available for RFM analysis")
            return self.spark.createDataFrame([], """
                user_id STRING,
                analysis_date DATE,
                recency_days INT,
                frequency_count BIGINT,
                monetary_value DOUBLE,
                recency_score INT,
                frequency_score INT,
                monetary_score INT,
                rfm_score INT,
                rfm_segment STRING
            """)

        # Step 1: Calculate RFM metrics per customer
        self.logger.info("Step 1: Calculating RFM metrics")

        rfm_metrics = df.groupBy("user_id").agg(
            # Recency: Days since last transaction
            datediff(current_date(), spark_max("transaction_timestamp")).alias("recency_days"),

            # Frequency: Total number of transactions
            count("transaction_timestamp").alias("frequency_count"),

            # Monetary: Total revenue
            spark_sum("total_revenue").alias("monetary_value")
        )

        # Step 2: Calculate quintile-based scores (1-5)
        self.logger.info("Step 2: Calculating RFM scores using quintiles")

        # Define windows for ntile calculation
        recency_window = Window.orderBy(col("recency_days").desc())  # Recent = 5, Old = 1
        frequency_window = Window.orderBy(col("frequency_count").asc())  # Many = 5, Few = 1
        monetary_window = Window.orderBy(col("monetary_value").asc())  # High = 5, Low = 1

        rfm_scored = rfm_metrics.withColumn(
            "recency_score",
            ntile(5).over(recency_window)
        ).withColumn(
            "frequency_score",
            ntile(5).over(frequency_window)
        ).withColumn(
            "monetary_score",
            ntile(5).over(monetary_window)
        )

        # Step 3: Create composite RFM score
        self.logger.info("Step 3: Creating composite RFM scores")

        rfm_scored = rfm_scored.withColumn(
            "rfm_score",
            (col("recency_score") * 100) + (col("frequency_score") * 10) + col("monetary_score")
        )

        # Step 4: Segment classification
        self.logger.info("Step 4: Classifying customers into segments")

        rfm_segmented = rfm_scored.withColumn(
            "rfm_segment",
            when(col("rfm_score") >= 544, "Champions")  # 555, 554, 545, 544
            .when(col("rfm_score") >= 434, "Loyal Customers")  # 543-434
            .when(col("rfm_score") >= 334, "Potential Loyalists")  # 433-334
            .when(col("rfm_score") >= 244, "At Risk")  # 333-244
            .when(col("rfm_score") >= 144, "Need Attention")  # 243-144
            .otherwise("Lost Customers")  # 143-111
        ).withColumn(
            "analysis_date", current_date()
        )

        # Log segment distribution
        segment_counts = rfm_segmented.groupBy("rfm_segment").count().collect()
        self.logger.info("RFM Segment Distribution:")
        for row in segment_counts:
            self.logger.info(f"  {row['rfm_segment']}: {row['count']} customers")

        return rfm_segmented

    def load(self, df: DataFrame) -> None:
        """
        Load RFM analysis results to warehouse
        Truncates and reloads the table (full refresh)
        """
        self.logger.info("Loading RFM analysis results to warehouse")

        if df.count() == 0:
            self.logger.warning("No RFM results to load")
            return

        # Select columns for target table
        columns_to_insert = [
            "user_id", "analysis_date",
            "recency_days", "frequency_count", "monetary_value",
            "recency_score", "frequency_score", "monetary_score",
            "rfm_score", "rfm_segment"
        ]

        df_to_load = df.select(columns_to_insert)

        try:
            # Truncate existing data (full refresh approach)
            self.logger.info("Truncating existing RFM analysis data")
            with self.connection.get_warehouse_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("TRUNCATE TABLE rfm_analysis")
                    conn.commit()

            # Load new analysis results
            (df_to_load.write
             .format("jdbc")
             .option("url", self.warehouse_jdbc_url)
             .option("dbtable", "rfm_analysis")
             .option("user", self.warehouse_jdbc_props["user"])
             .option("password", self.warehouse_jdbc_props["password"])
             .option("driver", self.warehouse_jdbc_props["driver"])
             .mode("append")
             .save())

            self.stats["rows_inserted"] = df_to_load.count()
            self.logger.info(f"Loaded {self.stats['rows_inserted']} RFM analysis records")

            # Update customer dimension with RFM scores
            self.update_customer_dimension(df)

        except Exception as e:
            self.logger.error(f"Error loading RFM analysis results: {e}")
            raise

    def update_customer_dimension(self, rfm_df: DataFrame) -> None:
        """
        Update customer dimension with RFM segment and score
        This triggers SCD Type 2 changes for customers whose segment changed
        """
        self.logger.info("Updating customer dimension with RFM scores")

        try:
            # Get current customer dimension
            dim_customers = (self.spark.read
                           .format("jdbc")
                           .option("url", self.warehouse_jdbc_url)
                           .option("dbtable", "(SELECT user_id, customer_segment, rfm_score FROM dim_customers WHERE is_current = TRUE) as dim_cust")
                           .option("user", self.warehouse_jdbc_props["user"])
                           .option("password", self.warehouse_jdbc_props["password"])
                           .option("driver", self.warehouse_jdbc_props["driver"])
                           .load())

            # Join with RFM results to find changes
            updates = rfm_df.alias("rfm").join(
                dim_customers.alias("dim"),
                col("rfm.user_id") == col("dim.user_id"),
                "inner"
            ).filter(
                (col("rfm.rfm_segment") != col("dim.customer_segment")) |
                (col("rfm.rfm_score") != col("dim.rfm_score"))
            ).select(
                col("rfm.user_id"),
                col("rfm.rfm_segment"),
                col("rfm.rfm_score").cast("int")
            )

            update_count = updates.count()

            if update_count > 0:
                self.logger.info(f"Found {update_count} customers with changed RFM segments")

                # Collect updates to apply via SQL
                updates_list = updates.collect()

                with self.connection.get_warehouse_connection() as conn:
                    with conn.cursor() as cursor:
                        for row in updates_list:
                            # Expire current record
                            cursor.execute("""
                                UPDATE dim_customers
                                SET effective_to = CURRENT_TIMESTAMP,
                                    is_current = FALSE,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE user_id = %s AND is_current = TRUE
                            """, (row["user_id"],))

                            # Insert new version with updated RFM data
                            cursor.execute("""
                                INSERT INTO dim_customers (
                                    user_id, country, first_transaction_date, last_transaction_date,
                                    total_transactions, total_spent, avg_order_value,
                                    customer_segment, rfm_score, is_active,
                                    effective_from, effective_to, is_current,
                                    created_at, updated_at, record_hash
                                )
                                SELECT
                                    user_id, country, first_transaction_date, last_transaction_date,
                                    total_transactions, total_spent, avg_order_value,
                                    %s as customer_segment,
                                    %s as rfm_score,
                                    is_active,
                                    CURRENT_TIMESTAMP as effective_from,
                                    '9999-12-31 23:59:59'::TIMESTAMP as effective_to,
                                    TRUE as is_current,
                                    created_at,
                                    CURRENT_TIMESTAMP as updated_at,
                                    MD5(CONCAT_WS('|',
                                        COALESCE(total_transactions::TEXT, ''),
                                        COALESCE(total_spent::TEXT, ''),
                                        COALESCE(avg_order_value::TEXT, ''),
                                        COALESCE(%s, ''),
                                        COALESCE(%s::TEXT, ''),
                                        COALESCE(is_active::TEXT, '')
                                    )) as record_hash
                                FROM dim_customers
                                WHERE user_id = %s
                                  AND effective_to = CURRENT_TIMESTAMP
                                  AND is_current = FALSE
                            """, (
                                row["rfm_segment"],
                                row["rfm_score"],
                                row["rfm_segment"],
                                row["rfm_score"],
                                row["user_id"]
                            ))

                        conn.commit()

                self.logger.info(f"Updated {update_count} customer dimension records with new RFM segments")
            else:
                self.logger.info("No customer segment changes detected")

        except Exception as e:
            self.logger.warning(f"Could not update customer dimension: {e}")
            self.logger.info("RFM analysis completed successfully despite dimension update issue")
