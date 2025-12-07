"""
Base ETL Class
Provides common functionality for all ETL jobs
"""
import sys
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from batch_processing.warehouse.spark_batch import create_batch_spark_session


class BaseETL(ABC):
    """Base class for all ETL jobs"""

    def __init__(self, job_name: str):
        """
        Initialize base ETL job

        Args:
            job_name: Name of the ETL job
        """
        self.job_name = job_name
        self.spark = create_batch_spark_session(f"GlobalMart-ETL-{job_name}")
        self.start_time = None
        self.end_time = None

    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract data from source

        Returns:
            DataFrame: Extracted data
        """
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform extracted data

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: Transformed data
        """
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """
        Load transformed data to warehouse

        Args:
            df: DataFrame to load
        """
        pass

    def read_from_postgres(
        self,
        table_name: str,
        database: str = "realtime",
        query: str = None
    ) -> DataFrame:
        """
        Read data from PostgreSQL database

        Args:
            table_name: Table name to read from
            database: 'realtime' or 'warehouse'
            query: Optional SQL query (overrides table_name)

        Returns:
            DataFrame: Data from PostgreSQL
        """
        config = (
            settings.postgres_realtime
            if database == "realtime"
            else settings.postgres_warehouse
        )

        jdbc_options = {
            "url": config.jdbc_url,
            "user": config.user,
            "password": config.password,
            "driver": "org.postgresql.Driver"
        }

        if query:
            jdbc_options["query"] = query
        else:
            jdbc_options["dbtable"] = table_name

        df = self.spark.read.format("jdbc").options(**jdbc_options).load()

        return df

    def read_from_warehouse(self, table_name: str) -> DataFrame:
        """
        Read data from warehouse PostgreSQL (convenience method)

        Args:
            table_name: Table name to read from warehouse

        Returns:
            DataFrame: Data from warehouse
        """
        return self.read_from_postgres(table_name, database="warehouse")

    def write_to_warehouse(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        scd_type2: bool = False,
        natural_key: str = None
    ) -> None:
        """
        Write DataFrame to warehouse PostgreSQL

        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode ('append', 'overwrite', 'ignore')
            scd_type2: If True, use SCD Type 2 merge logic
            natural_key: Natural key column for SCD Type 2 (e.g., 'user_id', 'product_id', 'country')
        """
        jdbc_url = settings.postgres_warehouse.jdbc_url
        properties = {
            "user": settings.postgres_warehouse.user,
            "password": settings.postgres_warehouse.password,
            "driver": "org.postgresql.Driver"
        }

        # For static dimension tables (dim_date), skip if data already exists
        # This makes the ETL idempotent and prevents FK constraint violations
        if mode == "overwrite" and table_name == "dim_date":
            print(f"  Checking if {table_name} already has data...")
            existing_count = self.read_from_warehouse(table_name).count()
            if existing_count > 0:
                print(f"  {table_name} already has {existing_count} records, skipping load")
                return
            else:
                mode = "append"

        # For dimension tables with SCD Type 2, use merge logic
        if scd_type2 and natural_key:
            self._write_scd_type2(df, table_name, natural_key, jdbc_url, properties)
        else:
            # For fact tables and non-SCD dimensions, use standard write
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode=mode,
                properties=properties
            )

    def _write_scd_type2(
        self,
        df: DataFrame,
        table_name: str,
        natural_key: str,
        jdbc_url: str,
        properties: dict
    ) -> None:
        """
        Write dimension data using SCD Type 2 logic

        Args:
            df: DataFrame to write
            table_name: Target table name
            natural_key: Natural key column name
            jdbc_url: JDBC URL for warehouse
            properties: JDBC connection properties
        """
        from pyspark.sql.functions import col, current_timestamp, lit

        # Read existing current records from warehouse
        try:
            existing_df = self.read_from_warehouse(table_name).filter(col("is_current") == True)
            existing_count = existing_df.count()
        except Exception:
            # Table might be empty or not exist yet
            existing_count = 0
            existing_df = None

        if existing_count == 0:
            # No existing records - insert all as new current records
            print(f"  No existing records in {table_name}, inserting {df.count()} new records")
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="append",
                properties=properties
            )
            return

        # Compare incoming records with existing records
        # Get columns to compare (exclude surrogate key, timestamps, SCD columns)
        exclude_cols = {
            "customer_id", "product_id_pk", "geography_id",  # Surrogate keys
            "effective_start_date", "effective_end_date", "is_current",  # SCD columns
            "created_at", "updated_at"  # Timestamps
        }
        compare_cols = [c for c in df.columns if c not in exclude_cols]

        # Join incoming with existing on natural key to find changes
        incoming_alias = df.alias("incoming")
        existing_alias = existing_df.alias("existing")

        # Find records that exist in incoming but not in existing (new records)
        new_records = incoming_alias.join(
            existing_alias,
            col(f"incoming.{natural_key}") == col(f"existing.{natural_key}"),
            "left_anti"
        )
        new_count = new_records.count()

        # Find records that exist in both (potential updates)
        matched_records = incoming_alias.join(
            existing_alias,
            col(f"incoming.{natural_key}") == col(f"existing.{natural_key}"),
            "inner"
        )

        # Identify changed records by comparing all attributes
        # Build comparison expression
        from pyspark.sql.functions import when
        comparison_expr = None
        for c in compare_cols:
            if c == natural_key:
                continue
            col_comparison = (
                col(f"incoming.{c}").isNotNull() &
                col(f"existing.{c}").isNotNull() &
                (col(f"incoming.{c}") != col(f"existing.{c}"))
            ) | (
                col(f"incoming.{c}").isNull() & col(f"existing.{c}").isNotNull()
            ) | (
                col(f"incoming.{c}").isNotNull() & col(f"existing.{c}").isNull()
            )

            if comparison_expr is None:
                comparison_expr = col_comparison
            else:
                comparison_expr = comparison_expr | col_comparison

        # Get changed records (where any attribute differs)
        if comparison_expr is not None:
            changed_records = matched_records.filter(comparison_expr).select("incoming.*")
            changed_count = changed_records.count()
        else:
            changed_count = 0

        print(f"  SCD Type 2 Analysis for {table_name}:")
        print(f"    - New records: {new_count}")
        print(f"    - Changed records: {changed_count}")
        print(f"    - Existing unchanged: {existing_count - changed_count}")

        # Step 1: Expire changed records using direct PostgreSQL UPDATE
        if changed_count > 0:
            import psycopg2

            # Get natural keys of changed records
            changed_keys = changed_records.select(natural_key).distinct().collect()
            changed_key_values = [row[natural_key] for row in changed_keys]

            # Build UPDATE query to expire old records
            conn = psycopg2.connect(
                host=settings.postgres_warehouse.host,
                port=settings.postgres_warehouse.port,
                database=settings.postgres_warehouse.database,
                user=settings.postgres_warehouse.user,
                password=settings.postgres_warehouse.password
            )

            try:
                cursor = conn.cursor()

                # Create placeholders for IN clause
                placeholders = ','.join(['%s'] * len(changed_key_values))

                update_query = f"""
                    UPDATE {table_name}
                    SET effective_end_date = CURRENT_TIMESTAMP,
                        is_current = FALSE
                    WHERE {natural_key} IN ({placeholders})
                    AND is_current = TRUE
                """

                cursor.execute(update_query, changed_key_values)
                expired_count = cursor.rowcount
                conn.commit()

                print(f"    - Expired {expired_count} old records")

                cursor.close()
            finally:
                conn.close()

        # Step 2: Insert new and changed records
        records_to_insert = new_records if changed_count == 0 else new_records.union(changed_records)
        insert_count = records_to_insert.count()

        if insert_count > 0:
            records_to_insert.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="append",
                properties=properties
            )
            print(f"    - Inserted {insert_count} new version records")

    def run(self) -> None:
        """Execute the complete ETL pipeline"""
        print("=" * 60)
        print(f"ETL Job: {self.job_name}")
        print("=" * 60)
        print(f"Started at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print()

        self.start_time = datetime.utcnow()

        try:
            # Extract
            print("Step 1/3: Extracting data...")
            df_extracted = self.extract()
            print(f"✓ Extracted {df_extracted.count()} records")
            print()

            # Transform
            print("Step 2/3: Transforming data...")
            df_transformed = self.transform(df_extracted)
            print(f"✓ Transformed to {df_transformed.count()} records")
            print()

            # Load
            print("Step 3/3: Loading data to warehouse...")
            self.load(df_transformed)
            print("✓ Data loaded successfully")
            print()

            self.end_time = datetime.utcnow()
            duration = (self.end_time - self.start_time).total_seconds()

            print("=" * 60)
            print(f"ETL Job Completed: {self.job_name}")
            print(f"Duration: {duration:.2f} seconds")
            print(f"Completed at: {self.end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print("=" * 60)
            print()

        except Exception as e:
            print(f"\n✗ ETL Job Failed: {self.job_name}")
            print(f"Error: {str(e)}")
            raise e

        finally:
            self.spark.stop()


if __name__ == "__main__":
    print("Base ETL class - Use this as a parent for specific ETL jobs")
