"""
Report Generator for Batch Processing
Generates summary reports for batch job execution
"""
from datetime import datetime
from typing import Dict, List
import json


class BatchJobReport:
    """Generates execution reports for batch jobs"""

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.start_time = datetime.utcnow()
        self.end_time = None
        self.status = "RUNNING"
        self.stats = {
            "rows_processed": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
            "rows_skipped": 0,
            "errors_count": 0,
            "warnings_count": 0
        }
        self.errors = []
        self.warnings = []
        self.metadata = {}

    def set_metadata(self, key: str, value: any):
        """Add metadata to the report"""
        self.metadata[key] = value

    def increment_stat(self, stat_name: str, value: int = 1):
        """Increment a statistic"""
        if stat_name in self.stats:
            self.stats[stat_name] += value
        else:
            self.stats[stat_name] = value

    def add_error(self, error_message: str, context: str = ""):
        """Add an error to the report"""
        self.errors.append({
            "timestamp": datetime.utcnow().isoformat(),
            "message": error_message,
            "context": context
        })
        self.increment_stat("errors_count")

    def add_warning(self, warning_message: str, context: str = ""):
        """Add a warning to the report"""
        self.warnings.append({
            "timestamp": datetime.utcnow().isoformat(),
            "message": warning_message,
            "context": context
        })
        self.increment_stat("warnings_count")

    def complete(self, status: str = "SUCCESS"):
        """Mark the job as completed"""
        self.end_time = datetime.utcnow()
        self.status = status

    def get_duration_seconds(self) -> int:
        """Get job duration in seconds"""
        if self.end_time:
            return int((self.end_time - self.start_time).total_seconds())
        return int((datetime.utcnow() - self.start_time).total_seconds())

    def get_summary(self) -> Dict:
        """Get summary report as dictionary"""
        return {
            "job_name": self.job_name,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.get_duration_seconds(),
            "statistics": self.stats,
            "errors_count": len(self.errors),
            "warnings_count": len(self.warnings),
            "metadata": self.metadata
        }

    def get_detailed_report(self) -> Dict:
        """Get detailed report including errors and warnings"""
        report = self.get_summary()
        report["errors"] = self.errors
        report["warnings"] = self.warnings
        return report

    def format_text_report(self) -> str:
        """Format report as readable text"""
        lines = []
        lines.append("=" * 80)
        lines.append(f"BATCH JOB REPORT: {self.job_name}")
        lines.append("=" * 80)
        lines.append(f"Status: {self.status}")
        lines.append(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        if self.end_time:
            lines.append(f"End Time: {self.end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        lines.append(f"Duration: {self.get_duration_seconds()} seconds")
        lines.append("")
        lines.append("Statistics:")

        for key, value in self.stats.items():
            lines.append(f"  {key}: {value:,}")

        if self.metadata:
            lines.append("")
            lines.append("Metadata:")
            for key, value in self.metadata.items():
                lines.append(f"  {key}: {value}")

        if self.errors:
            lines.append("")
            lines.append(f"Errors ({len(self.errors)}):")
            for i, error in enumerate(self.errors[:10], 1):  # Show first 10 errors
                lines.append(f"  {i}. [{error['context']}] {error['message']}")

            if len(self.errors) > 10:
                lines.append(f"  ... and {len(self.errors) - 10} more errors")

        if self.warnings:
            lines.append("")
            lines.append(f"Warnings ({len(self.warnings)}):")
            for i, warning in enumerate(self.warnings[:10], 1):  # Show first 10 warnings
                lines.append(f"  {i}. [{warning['context']}] {warning['message']}")

            if len(self.warnings) > 10:
                lines.append(f"  ... and {len(self.warnings) - 10} more warnings")

        lines.append("=" * 80)

        return "\n".join(lines)

    def save_to_file(self, file_path: str, format: str = "json"):
        """
        Save report to file

        Args:
            file_path: Path to save the report
            format: Format to use ('json' or 'text')
        """
        if format == "json":
            with open(file_path, 'w') as f:
                json.dump(self.get_detailed_report(), f, indent=2)
        elif format == "text":
            with open(file_path, 'w') as f:
                f.write(self.format_text_report())
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'json' or 'text'")


def generate_consolidated_report(job_reports: List[BatchJobReport]) -> str:
    """
    Generate a consolidated report from multiple job reports

    Args:
        job_reports: List of BatchJobReport instances

    Returns:
        str: Formatted consolidated report
    """
    lines = []
    lines.append("=" * 100)
    lines.append("CONSOLIDATED BATCH PROCESSING REPORT")
    lines.append("=" * 100)
    lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append(f"Total Jobs: {len(job_reports)}")
    lines.append("")

    # Summary statistics
    total_duration = sum(report.get_duration_seconds() for report in job_reports)
    total_rows_processed = sum(report.stats.get("rows_processed", 0) for report in job_reports)
    total_errors = sum(len(report.errors) for report in job_reports)
    total_warnings = sum(len(report.warnings) for report in job_reports)

    success_count = sum(1 for report in job_reports if report.status == "SUCCESS")
    failed_count = sum(1 for report in job_reports if report.status == "FAILED")

    lines.append("Overall Summary:")
    lines.append(f"  Total Duration: {total_duration} seconds ({total_duration / 60:.1f} minutes)")
    lines.append(f"  Total Rows Processed: {total_rows_processed:,}")
    lines.append(f"  Successful Jobs: {success_count}")
    lines.append(f"  Failed Jobs: {failed_count}")
    lines.append(f"  Total Errors: {total_errors}")
    lines.append(f"  Total Warnings: {total_warnings}")
    lines.append("")

    # Individual job summaries
    lines.append("Individual Job Results:")
    lines.append("-" * 100)

    for report in job_reports:
        status_symbol = "✓" if report.status == "SUCCESS" else "✗"
        lines.append(f"{status_symbol} {report.job_name}")
        lines.append(f"  Status: {report.status}")
        lines.append(f"  Duration: {report.get_duration_seconds()}s")
        lines.append(f"  Rows Processed: {report.stats.get('rows_processed', 0):,}")
        lines.append(f"  Errors: {len(report.errors)}, Warnings: {len(report.warnings)}")
        lines.append("")

    lines.append("=" * 100)

    return "\n".join(lines)
