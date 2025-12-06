"""
Main Orchestrator for Batch Processing
Coordinates execution of all ETL and analytics jobs with dependency management
"""
import argparse
import sys
import time
from datetime import datetime
from typing import List, Dict, Set
from pyspark.sql import SparkSession

# Add project root to path
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import config from centralized settings
from config.settings import settings
from stream_processing.common.spark_session import create_spark_session
config = settings.batch

from batch_processing.utils.logging_config import get_logger
from batch_processing.utils.report_generator import BatchJobReport
from batch_processing.warehouse.schema_init import ensure_schema_initialized

# Import ETL jobs
from batch_processing.etl.dim_date_etl import DateDimensionETL
from batch_processing.etl.dim_geography_etl import GeographyDimensionETL
from batch_processing.etl.dim_customers_etl import CustomerDimensionETL
from batch_processing.etl.dim_products_etl import ProductDimensionETL
from batch_processing.etl.fact_sales_etl import SalesFactETL
from batch_processing.etl.fact_cart_events_etl import CartEventsFactETL
from batch_processing.etl.fact_product_views_etl import ProductViewsFactETL

# Import analytics jobs
from batch_processing.jobs.rfm_analysis_job import RFMAnalysisJob
from batch_processing.jobs.product_performance_job import ProductPerformanceJob
from batch_processing.jobs.sales_trends_job import SalesTrendsJob
from batch_processing.jobs.customer_segments_job import CustomerSegmentsJob


class BatchOrchestrator:
    """Orchestrates batch processing jobs with dependency management"""

    def __init__(self, dry_run: bool = False, full_reload: bool = False):
        self.dry_run = dry_run
        self.full_reload = full_reload
        self.logger = get_logger("batch_orchestrator")
        self.spark = None
        self.report = BatchJobReport("batch_orchestrator")

        # Track job execution state
        self.completed_jobs: Set[str] = set()
        self.failed_jobs: Set[str] = set()
        self.job_stats: Dict[str, dict] = {}

    def initialize_spark(self) -> SparkSession:
        """Initialize Spark session with configuration"""
        self.logger.info("Initializing Spark session")

        spark = create_spark_session(config.SPARK_APP_NAME)

        self.logger.info(f"Spark session initialized: {spark.version}")
        return spark

    def ensure_warehouse_schema(self) -> None:
        """Ensure warehouse schema is initialized"""
        self.logger.info("Checking warehouse schema initialization")
        try:
            ensure_schema_initialized()
        except Exception as e:
            self.logger.warning(f"Schema initialization check completed with notes: {e}")

    def get_job_instance(self, job_name: str):
        """Create job instance based on job name"""
        job_map = {
            # Dimension ETL
            "dim_date": lambda: DateDimensionETL(self.spark),
            "dim_geography": lambda: GeographyDimensionETL(self.spark),
            "dim_customers": lambda: CustomerDimensionETL(self.spark),
            "dim_products": lambda: ProductDimensionETL(self.spark),

            # Fact ETL
            "fact_sales": lambda: SalesFactETL(self.spark),
            "fact_cart_events": lambda: CartEventsFactETL(self.spark),
            "fact_product_views": lambda: ProductViewsFactETL(self.spark),

            # Analytics jobs
            "rfm_analysis": lambda: RFMAnalysisJob(self.spark),
            "product_performance": lambda: ProductPerformanceJob(self.spark),
            "sales_trends": lambda: SalesTrendsJob(self.spark),
            "customer_segments": lambda: CustomerSegmentsJob(self.spark),
        }

        if job_name not in job_map:
            raise ValueError(f"Unknown job: {job_name}")

        return job_map[job_name]()

    def check_dependencies(self, job_name: str) -> bool:
        """Check if all dependencies for a job are satisfied"""
        dependencies = config.JOB_DEPENDENCIES.get(job_name, [])

        for dep in dependencies:
            if dep not in self.completed_jobs:
                self.logger.debug(f"Job {job_name} waiting for dependency: {dep}")
                return False

        return True

    def execute_job(self, job_name: str) -> bool:
        """
        Execute a single job

        Returns:
            bool: True if successful, False if failed
        """
        self.logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Executing job: {job_name}")

        if self.dry_run:
            # Simulate execution
            time.sleep(0.1)
            self.completed_jobs.add(job_name)
            self.job_stats[job_name] = {"status": "dry_run", "duration_sec": 0}
            return True

        try:
            # Create job instance
            job = self.get_job_instance(job_name)

            # Execute job
            start_time = time.time()
            stats = job.run()
            duration = time.time() - start_time

            # Record success
            self.completed_jobs.add(job_name)
            self.job_stats[job_name] = {
                "status": "success",
                "duration_sec": int(duration),
                **stats
            }

            self.report.increment_stat("jobs_completed")
            self.logger.info(f"Job {job_name} completed successfully in {duration:.1f}s")

            return True

        except Exception as e:
            # Record failure
            self.failed_jobs.add(job_name)
            self.job_stats[job_name] = {
                "status": "failed",
                "error": str(e)
            }

            self.report.increment_stat("jobs_failed")
            self.logger.error(f"Job {job_name} failed: {e}", exc_info=True)

            return False

    def resolve_job_list(self, job_args: List[str]) -> List[str]:
        """
        Resolve job list from arguments (can include groups)

        Args:
            job_args: List of job names or group names

        Returns:
            List of individual job names in dependency order
        """
        jobs = []

        for arg in job_args:
            if arg in config.JOB_GROUPS:
                # It's a group - expand it
                jobs.extend(config.JOB_GROUPS[arg])
            else:
                # It's an individual job
                jobs.append(arg)

        # Remove duplicates while preserving order
        seen = set()
        unique_jobs = []
        for job in jobs:
            if job not in seen:
                seen.add(job)
                unique_jobs.append(job)

        # Sort by dependencies (topological sort)
        sorted_jobs = self.topological_sort(unique_jobs)

        return sorted_jobs

    def topological_sort(self, jobs: List[str]) -> List[str]:
        """
        Sort jobs by dependencies (topological sort)

        Args:
            jobs: List of job names to sort

        Returns:
            Sorted list of jobs respecting dependencies
        """
        # Build dependency graph for requested jobs
        graph = {job: [] for job in jobs}
        in_degree = {job: 0 for job in jobs}

        for job in jobs:
            deps = config.JOB_DEPENDENCIES.get(job, [])
            for dep in deps:
                if dep in jobs:  # Only consider dependencies within requested jobs
                    graph[dep].append(job)
                    in_degree[job] += 1

        # Topological sort using Kahn's algorithm
        queue = [job for job in jobs if in_degree[job] == 0]
        sorted_jobs = []

        while queue:
            # Sort queue for deterministic order
            queue.sort()
            job = queue.pop(0)
            sorted_jobs.append(job)

            for dependent in graph[job]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(sorted_jobs) != len(jobs):
            raise ValueError("Circular dependency detected in job graph")

        return sorted_jobs

    def run(self, jobs: List[str]) -> int:
        """
        Run batch processing jobs

        Args:
            jobs: List of job names or groups to run

        Returns:
            int: Exit code (0 for success, 1 for failure)
        """

        try:
            # Resolve job list
            job_list = self.resolve_job_list(jobs)

            self.logger.info("="*80)
            self.logger.info("GLOBALMART BATCH PROCESSING")
            self.logger.info("="*80)
            self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'PRODUCTION'}")
            self.logger.info(f"Full Reload: {'YES' if self.full_reload else 'NO (Incremental)'}")
            self.logger.info(f"Jobs to execute ({len(job_list)}): {', '.join(job_list)}")
            self.logger.info("="*80)

            # Ensure warehouse schema is initialized
            if not self.dry_run:
                self.ensure_warehouse_schema()

            # Initialize Spark
            if not self.dry_run:
                self.spark = self.initialize_spark()

            # Execute jobs in order
            for job_name in job_list:
                # Check dependencies
                if not self.check_dependencies(job_name):
                    self.logger.error(f"Dependencies not satisfied for job: {job_name}")
                    self.failed_jobs.add(job_name)
                    continue

                # Execute job
                success = self.execute_job(job_name)

                if not success:
                    self.logger.warning(f"Job {job_name} failed, continuing with next jobs...")

            # Generate summary report
            self.report.complete("SUCCESS" if not self.failed_jobs else "PARTIAL")

            self.logger.info("="*80)
            self.logger.info("BATCH PROCESSING SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"Total jobs: {len(job_list)}")
            self.logger.info(f"Completed: {len(self.completed_jobs)}")
            self.logger.info(f"Failed: {len(self.failed_jobs)}")

            if self.failed_jobs:
                self.logger.warning(f"Failed jobs: {', '.join(self.failed_jobs)}")

            self.logger.info("="*80)

            # Print text report
            print("\n" + self.report.format_text_report())

            # Return exit code
            return 1 if self.failed_jobs else 0

        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}", exc_info=True)
            self.report.complete("FAILED")
            return 1

        finally:
            # Cleanup
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")


def main():
    """Main entry point with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="GlobalMart Batch Processing Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all jobs
  python main.py --jobs all

  # Run specific job groups
  python main.py --jobs dimensions facts analytics

  # Run specific jobs
  python main.py --jobs rfm_analysis product_performance

  # Dry run (validate without executing)
  python main.py --jobs all --dry-run

  # Force full reload (ignore incremental)
  python main.py --jobs all --full-reload

Available job groups:
  dimensions  - Date, Geography, Customers, Products
  facts       - Sales, Cart Events, Product Views
  analytics   - RFM, Product Performance, Sales Trends, Customer Segments
  all         - All jobs in dependency order

Available individual jobs:
  dim_date, dim_geography, dim_customers, dim_products,
  fact_sales, fact_cart_events, fact_product_views,
  rfm_analysis, product_performance, sales_trends, customer_segments
        """
    )

    parser.add_argument(
        "--jobs",
        nargs="+",
        required=True,
        help="Job names or groups to execute"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate job dependencies without executing"
    )

    parser.add_argument(
        "--full-reload",
        action="store_true",
        help="Force full reload (ignore incremental timestamps)"
    )

    args = parser.parse_args()

    # Create orchestrator and run
    orchestrator = BatchOrchestrator(
        dry_run=args.dry_run,
        full_reload=args.full_reload
    )

    exit_code = orchestrator.run(args.jobs)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
