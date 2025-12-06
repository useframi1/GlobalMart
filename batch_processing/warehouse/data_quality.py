"""
Data Quality Validation
Checks data quality metrics for warehouse loads
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, isnan, isnull
from typing import Dict, List
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class DataQualityValidator:
    """Validates data quality for warehouse loads"""

    def __init__(self):
        self.validation_results = []

    def check_null_values(self, df: DataFrame, required_columns: List[str]) -> Dict:
        """
        Check for null values in required columns

        Args:
            df: DataFrame to validate
            required_columns: List of columns that should not have nulls

        Returns:
            dict: Validation results
        """
        null_counts = {}
        total_rows = df.count()

        for column in required_columns:
            null_count = df.filter(col(column).isNull()).count()
            null_counts[column] = {
                "null_count": null_count,
                "null_percentage": (null_count / total_rows * 100) if total_rows > 0 else 0
            }

        has_issues = any(v["null_count"] > 0 for v in null_counts.values())

        return {
            "check_name": "null_values",
            "passed": not has_issues,
            "details": null_counts,
            "total_rows": total_rows
        }

    def check_duplicates(self, df: DataFrame, key_columns: List[str]) -> Dict:
        """
        Check for duplicate records based on key columns

        Args:
            df: DataFrame to validate
            key_columns: Columns that should be unique together

        Returns:
            dict: Validation results
        """
        total_rows = df.count()
        distinct_rows = df.select(key_columns).distinct().count()
        duplicate_count = total_rows - distinct_rows

        return {
            "check_name": "duplicates",
            "passed": duplicate_count == 0,
            "details": {
                "total_rows": total_rows,
                "distinct_rows": distinct_rows,
                "duplicate_count": duplicate_count
            }
        }

    def check_referential_integrity(
        self,
        fact_df: DataFrame,
        dim_df: DataFrame,
        fact_key: str,
        dim_key: str
    ) -> Dict:
        """
        Check referential integrity between fact and dimension

        Args:
            fact_df: Fact table DataFrame
            dim_df: Dimension table DataFrame
            fact_key: Foreign key column in fact table
            dim_key: Primary key column in dimension table

        Returns:
            dict: Validation results
        """
        # Left anti join to find orphaned records
        orphaned = fact_df.join(
            dim_df,
            fact_df[fact_key] == dim_df[dim_key],
            "left_anti"
        )

        orphaned_count = orphaned.count()
        total_fact_rows = fact_df.count()

        return {
            "check_name": "referential_integrity",
            "passed": orphaned_count == 0,
            "details": {
                "fact_key": fact_key,
                "dim_key": dim_key,
                "orphaned_records": orphaned_count,
                "total_fact_rows": total_fact_rows,
                "integrity_percentage": ((total_fact_rows - orphaned_count) / total_fact_rows * 100) if total_fact_rows > 0 else 0
            }
        }

    def check_value_ranges(
        self,
        df: DataFrame,
        column: str,
        min_value: float = None,
        max_value: float = None
    ) -> Dict:
        """
        Check if numeric values are within expected range

        Args:
            df: DataFrame to validate
            column: Column name to check
            min_value: Minimum acceptable value (optional)
            max_value: Maximum acceptable value (optional)

        Returns:
            dict: Validation results
        """
        total_rows = df.count()

        # Build filter for out-of-range values
        if min_value is not None and max_value is not None:
            out_of_range_df = df.filter((col(column) < min_value) | (col(column) > max_value))
        elif min_value is not None:
            out_of_range_df = df.filter(col(column) < min_value)
        elif max_value is not None:
            out_of_range_df = df.filter(col(column) > max_value)
        else:
            out_of_range_df = df.filter(lit(False))  # No range specified

        out_of_range_count = out_of_range_df.count()

        return {
            "check_name": "value_range",
            "passed": out_of_range_count == 0,
            "details": {
                "column": column,
                "min_value": min_value,
                "max_value": max_value,
                "out_of_range_count": out_of_range_count,
                "total_rows": total_rows
            }
        }

    def check_data_freshness(
        self,
        df: DataFrame,
        timestamp_column: str,
        max_age_hours: int = 48
    ) -> Dict:
        """
        Check if data is fresh (not too old)

        Args:
            df: DataFrame to validate
            timestamp_column: Column containing timestamp
            max_age_hours: Maximum acceptable age in hours

        Returns:
            dict: Validation results
        """
        from pyspark.sql.functions import current_timestamp, hour, unix_timestamp

        # Calculate age in hours
        df_with_age = df.withColumn(
            "age_hours",
            (unix_timestamp(current_timestamp()) - unix_timestamp(col(timestamp_column))) / 3600
        )

        stale_count = df_with_age.filter(col("age_hours") > max_age_hours).count()
        total_rows = df.count()

        return {
            "check_name": "data_freshness",
            "passed": stale_count == 0,
            "details": {
                "timestamp_column": timestamp_column,
                "max_age_hours": max_age_hours,
                "stale_records": stale_count,
                "total_rows": total_rows
            }
        }

    def run_all_validations(self, validations: List[Dict]) -> Dict:
        """
        Run multiple validations and aggregate results

        Args:
            validations: List of validation result dictionaries

        Returns:
            dict: Aggregated validation report
        """
        total_checks = len(validations)
        passed_checks = sum(1 for v in validations if v.get("passed", False))
        failed_checks = total_checks - passed_checks

        return {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "success_rate": (passed_checks / total_checks * 100) if total_checks > 0 else 0,
            "validations": validations,
            "overall_status": "PASSED" if failed_checks == 0 else "FAILED"
        }


# Singleton instance
validator = DataQualityValidator()
