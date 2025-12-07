"""
Warehouse Connection Manager
Provides connections to both real-time and warehouse PostgreSQL databases
"""
import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, List

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings


class WarehouseConnection:
    """Manages connections to PostgreSQL databases"""

    def __init__(self):
        self.realtime_conn = None
        self.warehouse_conn = None

    def get_realtime_connection(self):
        """Get connection to real-time PostgreSQL database"""
        if self.realtime_conn is None or self.realtime_conn.closed:
            self.realtime_conn = psycopg2.connect(
                host=settings.postgres_realtime.host,
                port=settings.postgres_realtime.port,
                database=settings.postgres_realtime.database,
                user=settings.postgres_realtime.user,
                password=settings.postgres_realtime.password
            )
        return self.realtime_conn

    def get_warehouse_connection(self):
        """Get connection to warehouse PostgreSQL database"""
        if self.warehouse_conn is None or self.warehouse_conn.closed:
            self.warehouse_conn = psycopg2.connect(
                host=settings.postgres_warehouse.host,
                port=settings.postgres_warehouse.port,
                database=settings.postgres_warehouse.database,
                user=settings.postgres_warehouse.user,
                password=settings.postgres_warehouse.password
            )
        return self.warehouse_conn

    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        database: str = "warehouse",
        fetch: bool = True
    ) -> Optional[List[Dict]]:
        """
        Execute a query on specified database

        Args:
            query: SQL query to execute
            params: Query parameters for parameterized queries
            database: 'warehouse' or 'realtime'
            fetch: Whether to fetch results (SELECT) or just execute (INSERT/UPDATE)

        Returns:
            List of dictionaries for SELECT queries, None for others
        """
        conn = (
            self.get_warehouse_connection()
            if database == "warehouse"
            else self.get_realtime_connection()
        )

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            cursor.execute(query, params)

            if fetch:
                results = cursor.fetchall()
                return [dict(row) for row in results]
            else:
                conn.commit()
                return None
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()

    def close_all(self):
        """Close all database connections"""
        if self.realtime_conn and not self.realtime_conn.closed:
            self.realtime_conn.close()
            print("✓ Closed real-time database connection")

        if self.warehouse_conn and not self.warehouse_conn.closed:
            self.warehouse_conn.close()
            print("✓ Closed warehouse database connection")


if __name__ == "__main__":
    # Test connections
    print("Testing Warehouse Connection Manager...")
    print()

    wh = WarehouseConnection()

    try:
        # Test warehouse connection
        print("Testing warehouse connection...")
        results = wh.execute_query(
            "SELECT COUNT(*) as count FROM dim_date",
            database="warehouse"
        )
        print(f"✓ Warehouse connected - dim_date rows: {results[0]['count']}")

        # Test real-time connection
        print("\nTesting real-time connection...")
        results = wh.execute_query(
            "SELECT COUNT(*) as count FROM sales_per_minute",
            database="realtime"
        )
        print(f"✓ Real-time DB connected - sales_per_minute rows: {results[0]['count']}")

    finally:
        wh.close_all()

    print("\n✓ Connection test complete")
