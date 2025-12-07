"""
Batch Processing Main Entry Point
Orchestrates ETL and batch analytics jobs
"""
import sys
import os
import argparse
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from batch_processing.etl.dim_date_etl import DimDateETL
from batch_processing.etl.dim_geography_etl import DimGeographyETL
from batch_processing.etl.dim_customers_etl import DimCustomersETL
from batch_processing.etl.dim_products_etl import DimProductsETL
from batch_processing.etl.fact_sales_etl import FactSalesETL
from batch_processing.etl.fact_product_views_etl import FactProductViewsETL
from batch_processing.etl.fact_cart_events_etl import FactCartEventsETL
from batch_processing.jobs.rfm_analysis_job import RFMAnalysisJob
from batch_processing.jobs.product_performance_job import ProductPerformanceJob
from batch_processing.jobs.sales_trends_job import SalesTrendsJob
from batch_processing.jobs.customer_segments_job import CustomerSegmentsJob


class BatchProcessingOrchestrator:
    """Orchestrates batch processing ETL and analytics jobs"""

    def __init__(self):
        self.etl_jobs = {}
        self.analytics_jobs = {}

    def register_etl_job(self, name: str, job_class, **kwargs):
        """Register an ETL job"""
        self.etl_jobs[name] = {"class": job_class, "kwargs": kwargs}
        print(f"✓ Registered ETL job: {name}")

    def register_analytics_job(self, name: str, job_class, **kwargs):
        """Register an analytics job"""
        self.analytics_jobs[name] = {"class": job_class, "kwargs": kwargs}
        print(f"✓ Registered analytics job: {name}")

    def run_etl(self, job_names: list = None):
        """
        Run ETL jobs

        Args:
            job_names: List of specific ETL jobs to run, or None for all
        """
        jobs_to_run = job_names if job_names else list(self.etl_jobs.keys())

        print("\n" + "=" * 60)
        print("Running ETL Jobs")
        print("=" * 60)
        print()

        for job_name in jobs_to_run:
            if job_name not in self.etl_jobs:
                print(f"✗ ETL job '{job_name}' not found")
                continue

            job_config = self.etl_jobs[job_name]
            job_instance = job_config["class"](**job_config["kwargs"])

            try:
                job_instance.run()
            except Exception as e:
                print(f"✗ ETL job '{job_name}' failed: {str(e)}")
                print("Continuing with remaining jobs...")
                print()

    def run_analytics(self, job_names: list = None):
        """
        Run analytics jobs

        Args:
            job_names: List of specific analytics jobs to run, or None for all
        """
        jobs_to_run = job_names if job_names else list(self.analytics_jobs.keys())

        print("\n" + "=" * 60)
        print("Running Analytics Jobs")
        print("=" * 60)
        print()

        for job_name in jobs_to_run:
            if job_name not in self.analytics_jobs:
                print(f"✗ Analytics job '{job_name}' not found")
                continue

            job_config = self.analytics_jobs[job_name]
            job_instance = job_config["class"](**job_config["kwargs"])

            try:
                job_instance.run()
            except Exception as e:
                print(f"✗ Analytics job '{job_name}' failed: {str(e)}")
                print("Continuing with remaining jobs...")
                print()

    def run_all(self, skip_etl: bool = False, skip_analytics: bool = False):
        """
        Run all ETL and analytics jobs

        Args:
            skip_etl: Skip ETL jobs
            skip_analytics: Skip analytics jobs
        """
        if not skip_etl:
            self.run_etl()

        if not skip_analytics:
            self.run_analytics()


def main():
    """Main entry point for batch processing"""
    parser = argparse.ArgumentParser(description="GlobalMart Batch Processing Orchestrator")
    parser.add_argument(
        "--mode",
        choices=["etl", "analytics", "all"],
        default="all",
        help="Processing mode: etl, analytics, or all (default: all)"
    )
    parser.add_argument(
        "--jobs",
        nargs="+",
        help="Specific jobs to run (space-separated)"
    )
    parser.add_argument(
        "--skip-dimensions",
        action="store_true",
        help="Skip dimension ETL jobs (only run fact ETL)"
    )

    args = parser.parse_args()

    # Print header
    print("=" * 60)
    print("GlobalMart Batch Processing Orchestrator")
    print("=" * 60)
    print(f"Run date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()

    # Create orchestrator
    orchestrator = BatchProcessingOrchestrator()

    # Register ETL jobs
    print("Registering ETL jobs...")
    if not args.skip_dimensions:
        orchestrator.register_etl_job("dim_date", DimDateETL)
        orchestrator.register_etl_job("dim_geography", DimGeographyETL)
        orchestrator.register_etl_job("dim_customers", DimCustomersETL)
        orchestrator.register_etl_job("dim_products", DimProductsETL)
    orchestrator.register_etl_job("fact_sales", FactSalesETL)
    orchestrator.register_etl_job("fact_product_views", FactProductViewsETL)
    orchestrator.register_etl_job("fact_cart_events", FactCartEventsETL)
    print()

    # Register analytics jobs
    print("Registering analytics jobs...")
    orchestrator.register_analytics_job("rfm_analysis", RFMAnalysisJob)
    orchestrator.register_analytics_job("product_performance", ProductPerformanceJob)
    orchestrator.register_analytics_job("sales_trends", SalesTrendsJob)
    orchestrator.register_analytics_job("customer_segments", CustomerSegmentsJob)
    print()

    # Run jobs based on mode
    if args.mode == "etl":
        orchestrator.run_etl(args.jobs)
    elif args.mode == "analytics":
        orchestrator.run_analytics(args.jobs)
    else:  # all
        if args.jobs:
            print("Warning: --jobs option is ignored when mode is 'all'")
        orchestrator.run_all()

    print()
    print("=" * 60)
    print("Batch Processing Complete")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
