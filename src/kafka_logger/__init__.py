"""Structured JSON logging configuration.

Call setup_logging() once at application startup (in entry points).
All modules should use: logger = logging.getLogger(__name__)
"""

import logging
import os
import sys
from datetime import datetime

from pythonjsonlogger import json as json_log

LOG_DIR = "logs"


def setup_logging(level: int = logging.INFO) -> None:
    """Configure structured JSON logging to both console and file."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now():%Y-%m-%d_%H-%M-%S}.log")

    json_formatter = json_log.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"},
    )

    # Console handler — human-readable
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)

    # File handler — structured JSON (one object per line)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(json_formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    logging.getLogger(__name__).info("Logging initialized. File: %s", log_file)
