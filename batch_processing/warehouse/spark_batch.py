"""
Spark Batch Session Factory
Provides configured Spark sessions for batch processing
"""
import sys
import os
from pyspark.sql import SparkSession

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings


def create_batch_spark_session(app_name: str = "GlobalMart-Batch") -> SparkSession:
    """
    Create and configure a Spark session for batch processing

    Args:
        app_name: Application name for Spark session

    Returns:
        SparkSession: Configured Spark session for batch processing
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(settings.spark.master_url)
        .config("spark.driver.memory", settings.spark.driver_memory)
        .config("spark.executor.memory", settings.spark.executor_memory)
        .config("spark.executor.cores", settings.spark.executor_cores)
        .config("spark.sql.shuffle.partitions", "12")
        # Timezone configuration - force UTC for consistency
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        # Fix for Spark 3.3.0 schema inference issues
        .config("spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled", "false")
        # JDBC driver for PostgreSQL
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.6.0"
        )
        .getOrCreate()
    )

    # Set log level
    spark.sparkContext.setLogLevel(settings.spark.log_level)

    print(f"✓ Batch Spark session created: {app_name}")
    print(f"  Master: {settings.spark.master_url}")
    print(f"  Driver memory: {settings.spark.driver_memory}")
    print(f"  Timezone: UTC")

    return spark


if __name__ == "__main__":
    # Test Spark session creation
    print("Testing Batch Spark Session Creation...")
    print()

    spark = create_batch_spark_session("Test-Batch-Session")

    # Display Spark version
    print(f"\nSpark version: {spark.version}")

    # Test simple operation
    data = [(1, "test"), (2, "batch")]
    df = spark.createDataFrame(data, ["id", "value"])

    print("\nTest DataFrame:")
    df.show()

    print("\n✓ Batch Spark session test complete")

    spark.stop()
