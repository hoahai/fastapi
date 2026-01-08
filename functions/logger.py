# functions/logger.py

from __future__ import annotations

import logging
import json
import sys
import os
import uuid
import time
from contextvars import ContextVar
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

RUN_LOG_FILE = "api.log"

# ======================================================
# REQUEST CONTEXT
# ======================================================

_REQUEST_ID: ContextVar[str | None] = ContextVar("request_id", default=None)


def set_request_id(request_id: str) -> ContextVar.Token:
    return _REQUEST_ID.set(request_id)


def reset_request_id(token: ContextVar.Token) -> None:
    _REQUEST_ID.reset(token)


def get_request_id() -> str | None:
    return _REQUEST_ID.get()


class RequestIdFilter(logging.Filter):
    def __init__(self, request_id: str) -> None:
        super().__init__()
        self.request_id = request_id

    def filter(self, record: logging.LogRecord) -> bool:
        return get_request_id() == self.request_id


_LOGGER_REGISTRY: set[logging.Logger] = set()
_ACTIVE_DEBUG_HANDLERS: list[logging.Handler] = []

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

        request_id = get_request_id()
        if request_id:
            log["request_id"] = request_id

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            log.update(extra_fields)

        return json.dumps(log, default=str)

# ======================================================
# HANDLERS
# ======================================================

def _create_file_handler() -> logging.Handler:
    os.makedirs(LOG_DIR, exist_ok=True)

    handler_level = logging.INFO if LOG_LEVEL == "DEBUG" else LOG_LEVEL
    handler = RotatingFileHandler(
        filename=os.path.join(LOG_DIR, RUN_LOG_FILE),
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(JsonFormatter())
    handler.setLevel(handler_level)
    handler.name = "file"
    return handler


def create_request_debug_handler(request_id: str) -> logging.Handler:
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now(CHICAGO_TZ).strftime("%y%m%d_%H%M%S")
    filename = f"run_debug_{timestamp}_{request_id}.log"

    handler = logging.FileHandler(
        filename=os.path.join(LOG_DIR, filename),
        encoding="utf-8",
    )
    handler.setFormatter(JsonFormatter())
    handler.setLevel(logging.DEBUG)
    handler.name = f"debug-{request_id}"
    handler.addFilter(RequestIdFilter(request_id))
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

    for handler in _ACTIVE_DEBUG_HANDLERS:
        if handler not in logger.handlers:
            logger.addHandler(handler)

    logger.propagate = False
    _LOGGER_REGISTRY.add(logger)
    return logger


def add_debug_handler(handler: logging.Handler) -> None:
    if handler in _ACTIVE_DEBUG_HANDLERS:
        return

    _ACTIVE_DEBUG_HANDLERS.append(handler)
    for logger in _LOGGER_REGISTRY:
        if handler not in logger.handlers:
            logger.addHandler(handler)


def remove_debug_handler(handler: logging.Handler) -> None:
    if handler in _ACTIVE_DEBUG_HANDLERS:
        _ACTIVE_DEBUG_HANDLERS.remove(handler)

    for logger in _LOGGER_REGISTRY:
        if handler in logger.handlers:
            logger.removeHandler(handler)
    handler.close()

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
