# functions/logger.py

from __future__ import annotations

import logging
import json
import sys
import os
import uuid
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

import pytz

from functions.constants import (
    LOGGING_ENABLED,
    LOG_LEVEL,
    LOG_DIR,
    LOG_MAX_BYTES,
    LOG_BACKUP_COUNT,
    LOG_RETENTION_DAYS,
)

# ======================================================
# TIMEZONE
# ======================================================

CHICAGO_TZ = pytz.timezone("America/Chicago")

# ======================================================
# RUN CONTEXT
# ======================================================

RUN_ID = uuid.uuid4().hex
_RUN_START_TIME = time.monotonic()

RUN_TIMESTAMP = datetime.now(CHICAGO_TZ).strftime("%y%m%d_%H%M%S")
RUN_LOG_FILE = f"run_{RUN_TIMESTAMP}_{RUN_ID[:8]}.log"

# ======================================================
# DURATION FORMATTER
# ======================================================

def format_duration(seconds: float) -> str:
    """
    Format duration as HH:mm:ss:ms (no days).
    """
    total_ms = int(seconds * 1000)

    ms = total_ms % 1000
    total_seconds = total_ms // 1000

    sec = total_seconds % 60
    total_minutes = total_seconds // 60

    minute = total_minutes % 60
    hour = total_minutes // 60

    return f"{hour:02}:{minute:02}:{sec:02}:{ms:03}"

# ======================================================
# FORMATTER
# ======================================================

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log: dict = {
            "timestamp": datetime.now(CHICAGO_TZ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": RUN_ID,
        }

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            log.update(extra_fields)

        return json.dumps(log, default=str)

# ======================================================
# HANDLERS
# ======================================================

def _create_file_handler() -> logging.Handler:
    os.makedirs(LOG_DIR, exist_ok=True)

    handler = RotatingFileHandler(
        filename=os.path.join(LOG_DIR, RUN_LOG_FILE),
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(JsonFormatter())
    handler.setLevel(LOG_LEVEL)
    handler.name = "file"
    return handler


def _create_console_handler() -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.setLevel(LOG_LEVEL)
    handler.name = "console"
    return handler

# ======================================================
# LOGGER FACTORY
# ======================================================

def get_logger(name: str = "app") -> logging.Logger:
    """
    Get or create a logger.

    - File logging is always enabled (per-run file)
    - Console logging is opt-in via enable_console_logging()
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    if not LOGGING_ENABLED:
        logger.disabled = True
        return logger

    if not any(getattr(h, "name", None) == "file" for h in logger.handlers):
        logger.addHandler(_create_file_handler())

    logger.propagate = False
    return logger

# ======================================================
# CONSOLE TOGGLE
# ======================================================

def enable_console_logging(logger: logging.Logger) -> None:
    if not any(getattr(h, "name", None) == "console" for h in logger.handlers):
        logger.addHandler(_create_console_handler())


def disable_console_logging(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        if getattr(handler, "name", None) == "console":
            logger.removeHandler(handler)

# ======================================================
# LOG RETENTION CLEANUP
# ======================================================

def cleanup_old_logs() -> None:
    """
    Delete log files older than LOG_RETENTION_DAYS.
    Safe and non-fatal.
    """
    if not LOGGING_ENABLED:
        return

    try:
        retention_seconds = LOG_RETENTION_DAYS * 24 * 60 * 60
        now = time.time()

        if not os.path.isdir(LOG_DIR):
            return

        for filename in os.listdir(LOG_DIR):
            file_path = os.path.join(LOG_DIR, filename)

            if not os.path.isfile(file_path):
                continue

            try:
                mtime = os.path.getmtime(file_path)
            except OSError:
                continue

            if now - mtime > retention_seconds:
                os.remove(file_path)

    except Exception:
        # Never crash app because of logging
        pass

# ======================================================
# RUN BOUNDARY HELPERS
# ======================================================

def log_run_start() -> None:
    """
    Mark the start of a run and perform retention cleanup.
    """
    cleanup_old_logs()

    logger = get_logger("system")
    logger.info(
        "===== RUN START =====",
        extra={
            "extra_fields": {
                "event": "run_start",
            }
        },
    )


def log_run_end() -> None:
    """
    Mark the end of a run with formatted duration.
    """
    duration = format_duration(time.monotonic() - _RUN_START_TIME)

    logger = get_logger("system")
    logger.info(
        "===== RUN END =====",
        extra={
            "extra_fields": {
                "event": "run_end",
                "duration": duration,
            }
        },
    )
