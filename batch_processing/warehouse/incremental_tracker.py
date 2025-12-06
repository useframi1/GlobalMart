"""
Incremental Load Tracker
Manages tracking of last successful batch job runs for incremental processing
"""
import psycopg2
from datetime import datetime
from typing import Optional, Dict
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.warehouse.connection import db_connection


class IncrementalTracker:
    """Tracks incremental load progress for batch jobs"""

    def __init__(self):
        self.connection = db_connection

    def get_last_run_timestamp(self, job_name: str) -> datetime:
        """
        Get the last successful run timestamp for a job

        Args:
            job_name: Name of the batch job

        Returns:
            datetime: Last successful run timestamp
        """
        with self.connection.get_warehouse_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT last_run_timestamp
                    FROM batch_job_tracker
                    WHERE job_name = %s AND last_run_status = 'SUCCESS'
                """, (job_name,))

                result = cursor.fetchone()
                if result:
                    return result[0]
                else:
                    # Default to very old date if no record
                    return datetime(2024, 1, 1, 0, 0, 0)

    def start_job(self, job_name: str) -> None:
        """
        Mark job as started/running

        Args:
            job_name: Name of the batch job
        """
        with self.connection.get_warehouse_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO batch_job_tracker
                        (job_name, last_run_timestamp, last_run_status, started_at, updated_at)
                    VALUES
                        (%s, %s, 'RUNNING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (job_name)
                    DO UPDATE SET
                        last_run_status = 'RUNNING',
                        started_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                """, (job_name, datetime.utcnow()))
                conn.commit()

    def complete_job(self, job_name: str, stats: Dict) -> None:
        """
        Mark job as successfully completed with statistics

        Args:
            job_name: Name of the batch job
            stats: Dictionary with execution statistics
                - rows_processed: Total rows processed
                - rows_inserted: Rows inserted
                - rows_updated: Rows updated
                - execution_duration_sec: Duration in seconds
        """
        with self.connection.get_warehouse_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE batch_job_tracker
                    SET
                        last_run_timestamp = %s,
                        last_run_status = 'SUCCESS',
                        rows_processed = %s,
                        rows_inserted = %s,
                        rows_updated = %s,
                        errors_count = 0,
                        error_message = NULL,
                        execution_duration_sec = %s,
                        completed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_name = %s
                """, (
                    datetime.utcnow(),
                    stats.get('rows_processed', 0),
                    stats.get('rows_inserted', 0),
                    stats.get('rows_updated', 0),
                    stats.get('execution_duration_sec', 0),
                    job_name
                ))
                conn.commit()

    def fail_job(self, job_name: str, error_message: str) -> None:
        """
        Mark job as failed with error message

        Args:
            job_name: Name of the batch job
            error_message: Error description
        """
        with self.connection.get_warehouse_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE batch_job_tracker
                    SET
                        last_run_status = 'FAILED',
                        errors_count = errors_count + 1,
                        error_message = %s,
                        completed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_name = %s
                """, (error_message[:1000], job_name))  # Truncate long error messages
                conn.commit()

    def get_job_stats(self, job_name: str) -> Optional[Dict]:
        """
        Get statistics for a specific job

        Args:
            job_name: Name of the batch job

        Returns:
            Dict with job statistics or None
        """
        with self.connection.get_warehouse_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM batch_job_tracker WHERE job_name = %s
                """, (job_name,))

                result = cursor.fetchone()
                return dict(result) if result else None

    def get_all_jobs_status(self) -> list:
        """
        Get status of all batch jobs

        Returns:
            List of dictionaries with job status
        """
        with self.connection.get_warehouse_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM batch_job_tracker
                    ORDER BY updated_at DESC
                """)

                return [dict(row) for row in cursor.fetchall()]


# Singleton instance
tracker = IncrementalTracker()
