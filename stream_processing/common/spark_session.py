"""
Spark Session Factory for GlobalMart
Provides configured Spark sessions for streaming and batch processing
"""
import sys
import os
from pyspark.sql import SparkSession

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings


def create_spark_session(app_name: str = None) -> SparkSession:
    """
    Create and configure a Spark session

    Args:
        app_name: Optional custom app name (defaults to settings)

    Returns:
        SparkSession: Configured Spark session
    """
    if app_name is None:
        app_name = settings.spark.app_name

    # Build Spark session
    spark = (SparkSession.builder
             .appName(app_name)
             .master(settings.spark.master_url)
             .config("spark.driver.memory", settings.spark.driver_memory)
             .config("spark.executor.memory", settings.spark.executor_memory)
             .config("spark.executor.cores", settings.spark.executor_cores)
             .config("spark.sql.shuffle.partitions", "12")
             .config("spark.streaming.stopGracefullyOnShutdown", "true")
             # Kafka packages
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                     "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
                     "org.postgresql:postgresql:42.6.0")
             .getOrCreate())

    # Set log level
    spark.sparkContext.setLogLevel(settings.spark.log_level)

    print(f"✓ Spark session created: {app_name}")
    print(f"  Master: {settings.spark.master_url}")
    print(f"  Driver memory: {settings.spark.driver_memory}")
    print(f"  Executor memory: {settings.spark.executor_memory}")

    return spark


def create_streaming_session(app_name: str = None) -> SparkSession:
    """
    Create a Spark session optimized for streaming

    Args:
        app_name: Optional custom app name

    Returns:
        SparkSession: Configured Spark session for streaming
    """
    spark = create_spark_session(app_name or "GlobalMart-Streaming")

    # Additional streaming configurations
    spark.conf.set("spark.sql.streaming.checkpointLocation", settings.spark.checkpoint_dir)

    return spark


if __name__ == "__main__":
    # Test Spark session creation
    print("Testing Spark Session Creation...")
    print()

    spark = create_spark_session("Test-Session")

    # Display Spark version
    print(f"\nSpark version: {spark.version}")

    # Test simple operation
    data = [(1, "test"), (2, "data")]
    df = spark.createDataFrame(data, ["id", "value"])

    print("\nTest DataFrame:")
    df.show()

    print("\n✓ Spark session test complete")

    spark.stop()
