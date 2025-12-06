"""
Error Handling Utilities for Batch Processing
Provides decorators and context managers for consistent error handling
"""
import functools
import traceback
from typing import Callable, Any
from datetime import datetime


class BatchJobError(Exception):
    """Base exception for batch job errors"""
    pass


class DataQualityError(BatchJobError):
    """Exception for data quality issues"""
    pass


class ETLError(BatchJobError):
    """Exception for ETL process errors"""
    pass


def retry_on_failure(max_retries: int = 3, delay_seconds: int = 5):
    """
    Decorator to retry a function on failure

    Args:
        max_retries: Maximum number of retry attempts
        delay_seconds: Delay between retries in seconds

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            import time

            last_exception = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        print(f"Attempt {attempt + 1} failed: {str(e)}")
                        print(f"Retrying in {delay_seconds} seconds...")
                        time.sleep(delay_seconds)
                    else:
                        print(f"All {max_retries} attempts failed")

            raise last_exception

        return wrapper
    return decorator


def log_and_continue(logger, default_return=None):
    """
    Decorator to log errors and continue execution (returns default value)

    Args:
        logger: Logger instance
        default_return: Default value to return on error

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                logger.warning(f"Continuing execution with default return value: {default_return}")
                return default_return

        return wrapper
    return decorator


class ErrorCollector:
    """Collects errors during batch processing without stopping execution"""

    def __init__(self, logger):
        self.logger = logger
        self.errors = []
        self.warnings = []

    def add_error(self, error: Exception, context: str = ""):
        """Add an error to the collection"""
        error_info = {
            "timestamp": datetime.utcnow(),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context,
            "traceback": traceback.format_exc()
        }
        self.errors.append(error_info)
        self.logger.error(f"Error collected: {context} - {str(error)}")

    def add_warning(self, message: str, context: str = ""):
        """Add a warning to the collection"""
        warning_info = {
            "timestamp": datetime.utcnow(),
            "message": message,
            "context": context
        }
        self.warnings.append(warning_info)
        self.logger.warning(f"Warning: {context} - {message}")

    def has_errors(self) -> bool:
        """Check if any errors were collected"""
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        """Check if any warnings were collected"""
        return len(self.warnings) > 0

    def get_summary(self) -> dict:
        """Get summary of collected errors and warnings"""
        return {
            "total_errors": len(self.errors),
            "total_warnings": len(self.warnings),
            "errors": self.errors,
            "warnings": self.warnings
        }

    def log_summary(self):
        """Log summary of errors and warnings"""
        if not self.has_errors() and not self.has_warnings():
            self.logger.info("No errors or warnings collected")
            return

        self.logger.info("=" * 80)
        self.logger.info("ERROR AND WARNING SUMMARY")
        self.logger.info(f"Total Errors: {len(self.errors)}")
        self.logger.info(f"Total Warnings: {len(self.warnings)}")

        if self.has_errors():
            self.logger.info("\nErrors:")
            for i, error in enumerate(self.errors, 1):
                self.logger.info(f"  {i}. [{error['error_type']}] {error['context']}: {error['error_message']}")

        if self.has_warnings():
            self.logger.info("\nWarnings:")
            for i, warning in enumerate(self.warnings, 1):
                self.logger.info(f"  {i}. {warning['context']}: {warning['message']}")

        self.logger.info("=" * 80)
