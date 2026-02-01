# shared/logger.py

from __future__ import annotations

import logging
import json
import sys
import os
import uuid
import time
import threading
import queue
import urllib.request
import socket
from contextvars import ContextVar
from datetime import datetime
from logging.handlers import RotatingFileHandler

import pytz

from shared.constants import (
    LOGGING_ENABLED,
    LOG_LEVEL,
    AXIOM_LOG_LEVEL,
    LOG_DIR,
    LOG_MAX_BYTES,
    LOG_BACKUP_COUNT,
    LOG_RETENTION_DAYS,
)
from shared.tenant import get_env, get_tenant_id

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
_CLIENT_ID: ContextVar[str | None] = ContextVar("client_id", default=None)


def set_request_id(request_id: str) -> ContextVar.Token:
    return _REQUEST_ID.set(request_id)


def reset_request_id(token: ContextVar.Token) -> None:
    _REQUEST_ID.reset(token)


def get_request_id() -> str | None:
    return _REQUEST_ID.get()


def set_client_id(client_id: str) -> ContextVar.Token:
    return _CLIENT_ID.set(client_id)


def reset_client_id(token: ContextVar.Token) -> None:
    _CLIENT_ID.reset(token)


def get_client_id() -> str | None:
    return _CLIENT_ID.get()


class RequestIdFilter(logging.Filter):
    def __init__(self, request_id: str) -> None:
        super().__init__()
        self.request_id = request_id

    def filter(self, record: logging.LogRecord) -> bool:
        return get_request_id() == self.request_id


_LOGGER_REGISTRY: set[logging.Logger] = set()
_ACTIVE_DEBUG_HANDLERS: list[logging.Handler] = []


def _log_file_enabled() -> bool:
    return os.getenv("LOG_FILE_ENABLED", "true").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


_HOST_CONTEXT: dict[str, str] | None = None


def _get_host_context() -> dict[str, str]:
    global _HOST_CONTEXT
    if _HOST_CONTEXT is not None:
        return _HOST_CONTEXT

    hostname = os.getenv("HOSTNAME") or socket.gethostname()
    app_env = os.getenv("APP_ENV", "").strip()
    service = os.getenv("SERVICE_NAME") or os.getenv("RENDER_SERVICE_NAME", "")
    deployment_id = os.getenv("DEPLOYMENT_ID") or os.getenv("RENDER_INSTANCE_ID", "")
    region = os.getenv("REGION") or os.getenv("RENDER_REGION", "")

    context = {
        "host": hostname,
        "app_env": app_env,
        "service": service.strip(),
        "deployment_id": deployment_id.strip(),
        "region": region.strip(),
    }
    _HOST_CONTEXT = {key: value for key, value in context.items() if value}
    return _HOST_CONTEXT


def _get_axiom_config() -> tuple[bool, str, str, str, int, float]:
    token = get_env("AXIOM_API_TOKEN", "")
    dataset = get_env("AXIOM_DATASET", "")
    api_url = get_env("AXIOM_API_URL", "https://api.axiom.co").rstrip("/")
    batch_size = int(get_env("AXIOM_BATCH_SIZE", "25"))
    flush_seconds = float(get_env("AXIOM_FLUSH_SECONDS", "2.0"))
    enabled = bool(token and dataset)
    return enabled, token, dataset, api_url, batch_size, flush_seconds


def _get_axiom_level() -> int:
    level_name = AXIOM_LOG_LEVEL.upper()
    level = logging.getLevelName(level_name)
    return level if isinstance(level, int) else logging.INFO

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

        log.update(_get_host_context())

        tenant_id = get_tenant_id()
        if tenant_id:
            log["tenant_id"] = tenant_id

        request_id = get_request_id()
        if request_id:
            log["request_id"] = request_id

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            log.update(extra_fields)

        return json.dumps(log, default=str)


class AxiomHandler(logging.Handler):
    def __init__(
        self,
        token: str,
        dataset: str,
        api_url: str,
        batch_size: int,
        flush_seconds: float,
    ) -> None:
        super().__init__()
        self._token = token
        self._dataset = dataset
        self._api_url = api_url
        self._batch_size = max(1, batch_size)
        self._flush_seconds = max(0.5, flush_seconds)
        self._queue: queue.SimpleQueue[dict] = queue.SimpleQueue()
        self._worker = threading.Thread(
            target=self._run,
            name="axiom-log-worker",
            daemon=True,
        )
        self._worker.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload = self.format(record)
            if isinstance(payload, str):
                item = json.loads(payload)
            elif isinstance(payload, dict):
                item = payload
            else:
                item = {"message": str(payload)}
            self._queue.put(item)
        except Exception:
            # Never crash app because of logging
            pass

    def _run(self) -> None:
        batch: list[dict] = []
        last_flush = time.monotonic()

        while True:
            try:
                item = self._queue.get(timeout=self._flush_seconds)
                batch.append(item)
            except queue.Empty:
                pass

            now = time.monotonic()
            should_flush = (
                batch
                and (
                    len(batch) >= self._batch_size
                    or (now - last_flush) >= self._flush_seconds
                )
            )
            if should_flush:
                self._send(batch)
                batch = []
                last_flush = now

    def _send(self, items: list[dict]) -> None:
        url = f"{self._api_url}/v1/datasets/{self._dataset}/ingest"
        data = json.dumps(items).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "fastapi-logger",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                response.read()
        except Exception:
            # Never crash app because of logging
            pass


class AxiomRouterHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self._handlers: dict[str, AxiomHandler] = {}
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        enabled, token, dataset, api_url, batch_size, flush_seconds = _get_axiom_config()
        if not enabled:
            return

        key = f"{token}:{dataset}:{api_url}:{batch_size}:{flush_seconds}"
        handler = self._handlers.get(key)
        if handler is None:
            with self._lock:
                handler = self._handlers.get(key)
                if handler is None:
                    handler = AxiomHandler(
                        token=token,
                        dataset=dataset,
                        api_url=api_url,
                        batch_size=batch_size,
                        flush_seconds=flush_seconds,
                    )
                    handler.setFormatter(self.formatter or JsonFormatter())
                    handler.setLevel(_get_axiom_level())
                    handler.name = f"axiom-{len(self._handlers) + 1}"
                    self._handlers[key] = handler

        handler.emit(record)

# ======================================================
# HANDLERS
# ======================================================

def _create_file_handler() -> logging.Handler:
    if not _log_file_enabled():
        handler = logging.NullHandler()
        handler.name = "file-null"
        return handler

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
    if not _log_file_enabled():
        handler = logging.NullHandler()
        handler.name = f"debug-null-{request_id}"
        return handler

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


def _create_axiom_handler() -> logging.Handler:
    handler = AxiomRouterHandler()
    handler.setFormatter(JsonFormatter())
    handler.setLevel(_get_axiom_level())
    handler.name = "axiom"
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
        handler = _create_file_handler()
        if not isinstance(handler, logging.NullHandler):
            logger.addHandler(handler)

    if not any(getattr(h, "name", None) == "axiom" for h in logger.handlers):
        handler = _create_axiom_handler()
        if not isinstance(handler, logging.NullHandler):
            logger.addHandler(handler)

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
