"""
Centralized Logging Configuration for Batch Processing
Provides consistent logging setup across all batch ETL jobs
"""
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler


def get_logger(job_name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Get or create a logger for a batch job

    Args:
        job_name: Name of the batch job (e.g., 'rfm_analysis', 'dim_customers')
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(f"batch.{job_name}")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "logs", "batch")
    os.makedirs(log_dir, exist_ok=True)

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File handler with rotation (max 10MB, keep 5 backups)
    file_path = os.path.join(log_dir, f"{job_name}.log")
    file_handler = RotatingFileHandler(
        file_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)

    # Daily log file (for easy daily review)
    daily_log_path = os.path.join(log_dir, f"{job_name}_{datetime.now().strftime('%Y%m%d')}.log")
    daily_handler = logging.FileHandler(daily_log_path)
    daily_handler.setLevel(logging.INFO)
    daily_handler.setFormatter(detailed_formatter)

    # Console handler (stdout)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(daily_handler)
    logger.addHandler(console_handler)

    return logger


def log_job_start(logger: logging.Logger, job_name: str, config: dict = None) -> None:
    """
    Log the start of a batch job with configuration details

    Args:
        logger: Logger instance
        job_name: Name of the batch job
        config: Optional configuration dictionary to log
    """
    logger.info("=" * 80)
    logger.info(f"STARTING BATCH JOB: {job_name}")
    logger.info(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")

    if config:
        logger.info("Configuration:")
        for key, value in config.items():
            logger.info(f"  {key}: {value}")

    logger.info("=" * 80)


def log_job_end(logger: logging.Logger, job_name: str, stats: dict) -> None:
    """
    Log the end of a batch job with statistics

    Args:
        logger: Logger instance
        job_name: Name of the batch job
        stats: Dictionary with job statistics
    """
    logger.info("=" * 80)
    logger.info(f"COMPLETED BATCH JOB: {job_name}")
    logger.info(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info("Statistics:")

    for key, value in stats.items():
        logger.info(f"  {key}: {value}")

    logger.info("=" * 80)


def log_error(logger: logging.Logger, error: Exception, context: str = "") -> None:
    """
    Log an error with context and stack trace

    Args:
        logger: Logger instance
        error: Exception instance
        context: Additional context about where/why the error occurred
    """
    logger.error("!" * 80)
    logger.error(f"ERROR OCCURRED: {type(error).__name__}")

    if context:
        logger.error(f"Context: {context}")

    logger.error(f"Message: {str(error)}")
    logger.error("Stack Trace:", exc_info=True)
    logger.error("!" * 80)
