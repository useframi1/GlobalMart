"""
Database Connection Management for Batch Processing
Handles connections to both real-time and warehouse PostgreSQL databases
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings


class DatabaseConnection:
    """Manages PostgreSQL connections for batch processing"""

    def __init__(self):
        self.realtime_config = settings.postgres_realtime
        self.warehouse_config = settings.postgres_warehouse

    @contextmanager
    def get_realtime_connection(self):
        """
        Get connection to real-time database (source for ETL)

        Yields:
            psycopg2.connection: Database connection
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.realtime_config.host,
                port=self.realtime_config.port,
                database=self.realtime_config.database,
                user=self.realtime_config.user,
                password=self.realtime_config.password
            )
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_warehouse_connection(self):
        """
        Get connection to warehouse database (target for ETL)

        Yields:
            psycopg2.connection: Database connection
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.warehouse_config.host,
                port=self.warehouse_config.port,
                database=self.warehouse_config.database,
                user=self.warehouse_config.user,
                password=self.warehouse_config.password
            )
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()

    def get_realtime_jdbc_url(self) -> str:
        """Get JDBC URL for Spark connection to real-time DB"""
        return self.realtime_config.jdbc_url

    def get_warehouse_jdbc_url(self) -> str:
        """Get JDBC URL for Spark connection to warehouse DB"""
        return self.warehouse_config.jdbc_url

    def get_realtime_jdbc_properties(self) -> dict:
        """Get JDBC properties for Spark read from real-time DB"""
        return {
            "user": self.realtime_config.user,
            "password": self.realtime_config.password,
            "driver": "org.postgresql.Driver"
        }

    def get_warehouse_jdbc_properties(self) -> dict:
        """Get JDBC properties for Spark write to warehouse DB"""
        return {
            "user": self.warehouse_config.user,
            "password": self.warehouse_config.password,
            "driver": "org.postgresql.Driver"
        }

    def test_connections(self) -> dict:
        """
        Test both database connections

        Returns:
            dict: Connection test results
        """
        results = {
            "realtime": {"status": "unknown", "error": None},
            "warehouse": {"status": "unknown", "error": None}
        }

        # Test real-time connection
        try:
            with self.get_realtime_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    results["realtime"]["status"] = "connected"
        except Exception as e:
            results["realtime"]["status"] = "failed"
            results["realtime"]["error"] = str(e)

        # Test warehouse connection
        try:
            with self.get_warehouse_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    results["warehouse"]["status"] = "connected"
        except Exception as e:
            results["warehouse"]["status"] = "failed"
            results["warehouse"]["error"] = str(e)

        return results


# Singleton instance
db_connection = DatabaseConnection()
