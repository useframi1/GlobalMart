"""
Stream Processing Main Entry Point
Runs all stream processing jobs or specific jobs based on configuration
"""
import sys
import os
import argparse
from typing import List
import threading

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stream_processing.jobs.sales_aggregator import SalesAggregator
from stream_processing.jobs.cart_analyzer import CartAnalyzer
from stream_processing.jobs.product_view_analyzer import ProductViewAnalyzer
from stream_processing.jobs.anomaly_detector import AnomalyDetector
from stream_processing.jobs.inventory_tracker import InventoryTracker


class StreamProcessingOrchestrator:
    """Orchestrates multiple stream processing jobs"""

    def __init__(self, output_mode: str = "postgres"):
        """
        Initialize orchestrator

        Args:
            output_mode: 'console' for testing, 'postgres' for production
        """
        self.output_mode = output_mode
        self.jobs = {}
        self.threads = []

    def register_job(self, name: str, job_instance):
        """Register a stream processing job"""
        self.jobs[name] = job_instance
        print(f"✓ Registered job: {name}")

    def run_job(self, name: str):
        """Run a specific job in a separate thread"""
        if name not in self.jobs:
            print(f"✗ Job '{name}' not found")
            return

        print(f"\nStarting job: {name}")
        job = self.jobs[name]

        # Run job in separate thread
        thread = threading.Thread(
            target=job.run,
            args=(self.output_mode,),
            name=name,
            daemon=True
        )
        thread.start()
        self.threads.append(thread)

    def run_all_jobs(self):
        """Run all registered jobs concurrently"""
        print("\nStarting all stream processing jobs...")
        print(f"Output mode: {self.output_mode}")
        print()

        for name in self.jobs.keys():
            self.run_job(name)

        print()
        print("=" * 60)
        print("All Stream Processing Jobs Running")
        print("=" * 60)
        print()
        print(f"Active jobs: {len(self.threads)}")
        for i, name in enumerate(self.jobs.keys(), 1):
            print(f"  {i}. {name}")
        print()
        print("Press Ctrl+C to stop all jobs")
        print("=" * 60)
        print()

        # Wait for all threads (they run indefinitely until interrupted)
        try:
            for thread in self.threads:
                thread.join()
        except KeyboardInterrupt:
            print("\n\nShutting down all stream processing jobs...")
            print("This may take a few moments...")
            # Threads are daemon threads, so they'll terminate with main process

    def run_selected_jobs(self, job_names: List[str]):
        """Run selected jobs"""
        print(f"\nStarting selected jobs: {', '.join(job_names)}")
        print(f"Output mode: {self.output_mode}")
        print()

        for name in job_names:
            self.run_job(name)

        print()
        print("=" * 60)
        print("Selected Stream Processing Jobs Running")
        print("=" * 60)
        print()
        print(f"Active jobs: {len(self.threads)}")
        for i, name in enumerate(job_names, 1):
            print(f"  {i}. {name}")
        print()
        print("Press Ctrl+C to stop all jobs")
        print("=" * 60)
        print()

        try:
            for thread in self.threads:
                thread.join()
        except KeyboardInterrupt:
            print("\n\nShutting down stream processing jobs...")


def main():
    """Main entry point for stream processing"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="GlobalMart Stream Processing Orchestrator")
    parser.add_argument(
        "--mode",
        choices=["console", "postgres"],
        default="postgres",
        help="Output mode: console for testing, postgres for production (default: postgres)"
    )
    parser.add_argument(
        "--jobs",
        nargs="+",
        choices=["sales", "cart", "product_views", "anomaly", "inventory", "all"],
        default=["all"],
        help="Jobs to run (default: all)"
    )

    args = parser.parse_args()

    # Print header
    print("=" * 60)
    print("GlobalMart Stream Processing Orchestrator")
    print("=" * 60)
    print()

    # Create orchestrator
    orchestrator = StreamProcessingOrchestrator(output_mode=args.mode)

    # Register all available jobs
    print("Registering stream processing jobs...")
    print()

    orchestrator.register_job("sales", SalesAggregator())
    orchestrator.register_job("cart", CartAnalyzer())
    orchestrator.register_job("product_views", ProductViewAnalyzer())
    orchestrator.register_job("anomaly", AnomalyDetector())
    orchestrator.register_job("inventory", InventoryTracker())

    print()

    # Run jobs based on arguments
    if "all" in args.jobs:
        orchestrator.run_all_jobs()
    else:
        orchestrator.run_selected_jobs(args.jobs)


if __name__ == "__main__":
    main()
