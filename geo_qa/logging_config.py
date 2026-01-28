"""Logging configuration for the QA pipeline."""

import logging
import sys
from pathlib import Path


def setup_logging(log_dir: Path, log_level: str = "INFO") -> None:
    """
    Configure logging with both file and console handlers.

    File handler: DEBUG level, detailed format, in log_dir/run.log
    Console handler: INFO level, simpler format

    Args:
        log_dir: Directory where log file will be created
        log_level: Logging level for console (default: INFO)
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "run.log"

    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    simple_formatter = logging.Formatter(
        fmt="%(levelname)s: %(message)s",
    )

    # File handler - DEBUG level, detailed format
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)

    # Console handler - INFO level (or specified), simple format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    console_handler.setFormatter(simple_formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture everything, handlers will filter

    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Add our handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Log startup message
    root_logger.info(f"Logging initialized. Log file: {log_file}")
    root_logger.debug(f"Console log level: {log_level}, File log level: DEBUG")
