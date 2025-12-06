"""
Schema Initialization Module
Automatically executes SQL schema files to ensure warehouse is properly set up
"""
import os
import sys
from pathlib import Path
from typing import List
import psycopg2
from psycopg2.extensions import connection

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from batch_processing.utils.logging_config import get_logger

logger = get_logger("schema_init")


class SchemaInitializer:
    """Handles automatic execution of SQL schema files"""

    def __init__(self):
        self.warehouse_config = settings.postgres_warehouse
        self.schema_dir = Path(__file__).parent.parent / "schema"

        # SQL files to execute in order
        self.schema_files = [
            "init_warehouse.sql",
            "add_scd_columns.sql",
            "create_tracking_table.sql"
        ]

    def get_connection(self) -> connection:
        """Get PostgreSQL connection to warehouse"""
        return psycopg2.connect(
            host=self.warehouse_config.host,
            port=self.warehouse_config.port,
            database=self.warehouse_config.database,
            user=self.warehouse_config.user,
            password=self.warehouse_config.password
        )

    def check_schema_initialized(self) -> bool:
        """
        Check if schema is already initialized by checking for key tables and columns

        Returns:
            bool: True if schema is initialized, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if dim_products has SCD columns
                    cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'dim_products'
                        AND column_name IN ('product_sk', 'record_hash', 'is_current', 'effective_from', 'effective_to')
                    """)
                    scd_columns = cursor.fetchall()

                    # Check if dim_customers has SCD columns
                    cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'dim_customers'
                        AND column_name IN ('customer_sk', 'record_hash', 'is_current', 'effective_from', 'effective_to')
                    """)
                    customer_scd_columns = cursor.fetchall()

                    # Schema is initialized if both dimension tables have all SCD columns
                    return len(scd_columns) == 5 and len(customer_scd_columns) == 5
        except Exception as e:
            logger.debug(f"Schema check failed: {e}")
            return False

    def execute_sql_file(self, file_path: Path) -> None:
        """
        Execute a SQL file

        Args:
            file_path: Path to SQL file
        """
        logger.info(f"Executing SQL file: {file_path.name}")

        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Split by semicolons but handle complex cases
        # Remove psql-specific commands that won't work with psycopg2
        lines = sql_content.split('\n')
        cleaned_lines = []
        for line in lines:
            # Skip psql meta-commands
            if line.strip().startswith('\\'):
                continue
            cleaned_lines.append(line)

        sql_content = '\n'.join(cleaned_lines)

        # Execute SQL
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Set autocommit for DDL statements
                    conn.autocommit = True

                    # Execute the entire script
                    cursor.execute(sql_content)

                    logger.info(f"✓ Successfully executed {file_path.name}")
        except Exception as e:
            # Log but don't fail - some statements might be idempotent
            logger.warning(f"Note: {file_path.name} execution completed with messages: {e}")

    def initialize_schema(self, force: bool = False) -> bool:
        """
        Initialize warehouse schema by executing SQL files

        Args:
            force: If True, execute schema files even if already initialized

        Returns:
            bool: True if schema was initialized, False if skipped
        """
        # Check if already initialized
        if not force and self.check_schema_initialized():
            logger.info("Schema already initialized (all required tables and columns exist)")
            return False

        logger.info("="*80)
        logger.info("INITIALIZING WAREHOUSE SCHEMA")
        logger.info("="*80)

        # Execute each schema file in order
        for sql_file in self.schema_files:
            file_path = self.schema_dir / sql_file

            if not file_path.exists():
                logger.warning(f"Schema file not found: {sql_file}")
                continue

            self.execute_sql_file(file_path)

        logger.info("="*80)
        logger.info("SCHEMA INITIALIZATION COMPLETE")
        logger.info("="*80)

        return True


def ensure_schema_initialized(force: bool = False) -> None:
    """
    Convenience function to ensure schema is initialized

    Args:
        force: If True, re-initialize schema even if already initialized
    """
    initializer = SchemaInitializer()
    initializer.initialize_schema(force=force)


if __name__ == "__main__":
    # Allow running this module directly for manual schema initialization
    import argparse

    parser = argparse.ArgumentParser(description="Initialize GlobalMart warehouse schema")
    parser.add_argument("--force", action="store_true", help="Force re-initialization")
    args = parser.parse_args()

    ensure_schema_initialized(force=args.force)
