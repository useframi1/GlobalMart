"""
SCD Type 2 Manager
Implements Slowly Changing Dimension Type 2 logic for tracking historical changes
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, md5, concat_ws,
    when, coalesce
)
from datetime import datetime
from typing import List
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class SCDType2Manager:
    """Manages SCD Type 2 operations for dimension tables"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def calculate_hash(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Calculate MD5 hash for change detection

        Args:
            df: Input DataFrame
            columns: List of column names to include in hash

        Returns:
            DataFrame with 'record_hash' column added
        """
        # Create concatenated string of all tracked columns
        concat_expr = concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in columns])
        return df.withColumn("record_hash", md5(concat_expr))

    def merge_scd_type2(
        self,
        source_df: DataFrame,
        target_table: str,
        jdbc_url: str,
        jdbc_properties: dict,
        business_key: str,
        tracked_columns: List[str],
        surrogate_key: str = None
    ) -> dict:
        """
        Perform SCD Type 2 merge operation

        Process:
        1. Read existing dimension table
        2. Identify new records (inserts)
        3. Identify changed records (updates - expire old, insert new)
        4. Identify unchanged records (no action)

        Args:
            source_df: Source DataFrame with new/updated data
            target_table: Target dimension table name
            jdbc_url: JDBC URL for warehouse database
            jdbc_properties: JDBC connection properties
            business_key: Natural key column (e.g., 'user_id', 'product_id')
            tracked_columns: Columns to track for changes
            surrogate_key: Surrogate key column name (e.g., 'customer_sk')

        Returns:
            dict: Statistics (inserts, updates, unchanged)
        """
        from batch_processing.warehouse.connection import db_connection

        # Calculate hash for source data
        source_with_hash = self.calculate_hash(source_df, tracked_columns)

        # Read current dimension table (only current records)
        try:
            target_df = (self.spark.read
                        .format("jdbc")
                        .option("url", jdbc_url)
                        .option("dbtable", target_table)
                        .option("user", jdbc_properties["user"])
                        .option("password", jdbc_properties["password"])
                        .option("driver", jdbc_properties["driver"])
                        .load()
                        .filter(col("is_current") == True))
        except Exception:
            # Table is empty or doesn't exist
            target_df = None

        if target_df is None or target_df.count() == 0:
            # First load - all records are new
            records_to_insert = (source_with_hash
                                .withColumn("effective_from", current_timestamp())
                                .withColumn("effective_to", lit("9999-12-31 23:59:59").cast("timestamp"))
                                .withColumn("is_current", lit(True))
                                .withColumn("created_at", current_timestamp())
                                .withColumn("updated_at", current_timestamp()))

            # Write to database
            (records_to_insert.write
             .format("jdbc")
             .option("url", jdbc_url)
             .option("dbtable", target_table)
             .option("user", jdbc_properties["user"])
             .option("password", jdbc_properties["password"])
             .option("driver", jdbc_properties["driver"])
             .mode("append")
             .save())

            return {
                "inserts": records_to_insert.count(),
                "updates": 0,
                "unchanged": 0
            }

        # Join source and target on business key
        comparison = source_with_hash.alias("src").join(
            target_df.alias("tgt"),
            col("src." + business_key) == col("tgt." + business_key),
            "full_outer"
        )

        # Identify new records (in source but not in target)
        new_records = (comparison
                      .filter(col(f"tgt.{business_key}").isNull())
                      .select("src.*")
                      .withColumn("effective_from", current_timestamp())
                      .withColumn("effective_to", lit("9999-12-31 23:59:59").cast("timestamp"))
                      .withColumn("is_current", lit(True))
                      .withColumn("created_at", current_timestamp())
                      .withColumn("updated_at", current_timestamp()))

        # Identify changed records (hash is different)
        changed_records = (comparison
                          .filter(
                              col(f"src.{business_key}").isNotNull() &
                              col(f"tgt.{business_key}").isNotNull() &
                              (col("src.record_hash") != col("tgt.record_hash"))
                          )
                          .select("src.*")
                          .withColumn("effective_from", current_timestamp())
                          .withColumn("effective_to", lit("9999-12-31 23:59:59").cast("timestamp"))
                          .withColumn("is_current", lit(True))
                          .withColumn("created_at", current_timestamp())
                          .withColumn("updated_at", current_timestamp()))

        # Count statistics
        inserts_count = new_records.count()
        updates_count = changed_records.count()

        # Insert new records
        if inserts_count > 0:
            (new_records.write
             .format("jdbc")
             .option("url", jdbc_url)
             .option("dbtable", target_table)
             .option("user", jdbc_properties["user"])
             .option("password", jdbc_properties["password"])
             .option("driver", jdbc_properties["driver"])
             .mode("append")
             .save())

        # Handle updates: Expire old records and insert new versions
        if updates_count > 0:
            # Get list of business keys that changed
            changed_keys = [row[business_key] for row in changed_records.select(business_key).collect()]

            # Expire old records
            with db_connection.get_warehouse_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        UPDATE {target_table}
                        SET
                            effective_to = CURRENT_TIMESTAMP,
                            is_current = FALSE,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE
                            {business_key} = ANY(%s)
                            AND is_current = TRUE
                    """, (changed_keys,))
                    conn.commit()

            # Insert new versions
            (changed_records.write
             .format("jdbc")
             .option("url", jdbc_url)
             .option("dbtable", target_table)
             .option("user", jdbc_properties["user"])
             .option("password", jdbc_properties["password"])
             .option("driver", jdbc_properties["driver"])
             .mode("append")
             .save())

        return {
            "inserts": inserts_count,
            "updates": updates_count,
            "unchanged": target_df.count() - updates_count if target_df else 0
        }
