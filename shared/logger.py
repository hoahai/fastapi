# shared/logger.py

from __future__ import annotations

import os
import json
import logging
import queue
import socket
import sys
import threading
import time
import urllib.request
import uuid
from contextvars import ContextVar
from datetime import datetime

import pytz

from shared.constants import (
    LOGGING_ENABLED,
    LOG_LEVEL,
    AXIOM_LOG_LEVEL,
)
from shared.tenant import get_app_scoped_env, get_tenant_id

# ======================================================
# TIMEZONE
# ======================================================

CHICAGO_TZ = pytz.timezone("America/Chicago")

# ======================================================
# RUN CONTEXT
# ======================================================

RUN_ID = uuid.uuid4().hex
_RUN_START_TIME = time.monotonic()

# ======================================================
# REQUEST CONTEXT
# ======================================================

_REQUEST_ID: ContextVar[str | None] = ContextVar("request_id", default=None)
_CLIENT_ID: ContextVar[str | None] = ContextVar("client_id", default=None)
_APP_SCOPE: ContextVar[str | None] = ContextVar("app_scope", default=None)


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


def set_log_app_scope(app_scope: str) -> ContextVar.Token:
    return _APP_SCOPE.set(str(app_scope or "").strip().lower() or None)


def reset_log_app_scope(token: ContextVar.Token) -> None:
    _APP_SCOPE.reset(token)


def get_log_app_scope() -> str | None:
    return _APP_SCOPE.get()


_LOGGER_REGISTRY: set[logging.Logger] = set()


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
    app_scope = get_log_app_scope()
    if not app_scope:
        return False, "", "", "https://api.axiom.co", 25, 2.0

    def _resolve_axiom_value(
        key: str,
        default: str,
    ) -> str:
        return str(get_app_scoped_env(app_scope, key, default) or default).strip()

    token = _resolve_axiom_value("AXIOM_API_TOKEN", "")
    dataset = _resolve_axiom_value("AXIOM_DATASET", "")
    api_url = _resolve_axiom_value("AXIOM_API_URL", "https://api.axiom.co").rstrip("/")

    try:
        batch_size = int(_resolve_axiom_value("AXIOM_BATCH_SIZE", "25"))
    except ValueError:
        batch_size = 25

    try:
        flush_seconds = float(_resolve_axiom_value("AXIOM_FLUSH_SECONDS", "2.0"))
    except ValueError:
        flush_seconds = 2.0

    enabled = bool(token and dataset)
    return enabled, token, dataset, api_url, batch_size, flush_seconds


def _get_axiom_level() -> int:
    level_name = AXIOM_LOG_LEVEL.upper()
    level = logging.getLevelName(level_name)
    return level if isinstance(level, int) else logging.INFO


def _level_name_to_level_no(level_name: str) -> int:
    normalized = str(level_name or "").strip().upper()
    mapping = {
        "CRITICAL": logging.CRITICAL,
        "FATAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "TRACE": 5,
    }
    return mapping.get(normalized, logging.INFO)


def _level_no_to_otel_severity_number(level_no: int) -> int:
    if level_no >= logging.CRITICAL:
        return 21
    if level_no >= logging.ERROR:
        return 17
    if level_no >= logging.WARNING:
        return 13
    if level_no >= logging.INFO:
        return 9
    if level_no >= logging.DEBUG:
        return 5
    return 1


def _level_no_to_status_code(level_no: int) -> str:
    if level_no >= logging.ERROR:
        return "ERROR"
    if level_no >= logging.WARNING:
        return "WARN"
    return "OK"


def _build_severity_fields(
    *,
    level_name: str,
    level_no: int | None = None,
) -> dict[str, object]:
    normalized_level_name = str(level_name or "").strip().upper() or "INFO"
    resolved_level_no = (
        int(level_no)
        if isinstance(level_no, int)
        else _level_name_to_level_no(normalized_level_name)
    )
    severity_number = _level_no_to_otel_severity_number(resolved_level_no)
    status_code = _level_no_to_status_code(resolved_level_no)
    return {
        "level": normalized_level_name,
        "@level": normalized_level_name,
        "severity": normalized_level_name,
        "@severity": normalized_level_name,
        "severityText": normalized_level_name,
        "severityNumber": severity_number,
        "status.code": status_code,
    }


AXIOM_FAILURE_FALLBACK_EMAIL = "truonghoahai@gmail.com"
AXIOM_FAILURE_ALERT_COOLDOWN_SECONDS = 300.0
_AXIOM_FAILURE_ALERTS: dict[str, float] = {}
_AXIOM_FAILURE_ALERTS_LOCK = threading.Lock()


def _post_axiom_batch(
    *,
    token: str,
    dataset: str,
    api_url: str,
    items: list[dict],
) -> None:
    url = f"{api_url}/v1/datasets/{dataset}/ingest"
    data = json.dumps(items).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "fastapi-logger",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=5) as response:
        response.read()


def _build_axiom_failure_alert_key(
    *,
    app_scope: str | None,
    dataset: str,
    api_url: str,
) -> str:
    return f"{str(app_scope or '')}:{dataset}:{api_url}"


def _should_send_axiom_failure_alert(
    *,
    key: str,
    force: bool,
) -> bool:
    if force:
        return True
    now = time.monotonic()
    with _AXIOM_FAILURE_ALERTS_LOCK:
        last_sent = _AXIOM_FAILURE_ALERTS.get(key, 0.0)
        if (now - last_sent) < AXIOM_FAILURE_ALERT_COOLDOWN_SECONDS:
            return False
        _AXIOM_FAILURE_ALERTS[key] = now
    return True


def _mark_axiom_failure_alert_sent(
    *,
    key: str,
) -> None:
    now = time.monotonic()
    with _AXIOM_FAILURE_ALERTS_LOCK:
        _AXIOM_FAILURE_ALERTS[key] = now


def _send_axiom_failure_email(
    *,
    app_scope: str | None,
    dataset: str,
    api_url: str,
    items: list[dict],
    error: Exception,
    force: bool = False,
) -> None:
    alert_key = _build_axiom_failure_alert_key(
        app_scope=app_scope,
        dataset=dataset,
        api_url=api_url,
    )
    if not _should_send_axiom_failure_alert(key=alert_key, force=force):
        return

    sample = items[0] if items else {}
    tenant_id = sample.get("tenant_id") if isinstance(sample, dict) else None
    sample_message = sample.get("message") if isinstance(sample, dict) else None

    subject = "[Axiom Error] Ingest fallback alert"
    body_lines = [
        "Axiom ingest failed. Fallback email alert was triggered.",
        "",
        f"App scope: {app_scope or 'unknown'}",
        f"Tenant: {tenant_id or 'unknown'}",
        f"Dataset: {dataset}",
        f"API URL: {api_url}",
        f"Batch size: {len(items)}",
        f"Error: {str(error)}",
    ]
    if sample_message:
        body_lines.append(f"Sample message: {sample_message}")
    body = "\n".join(body_lines)

    try:
        from shared.email import send_google_ads_result_email

        send_google_ads_result_email(
            subject,
            body,
            to_addresses=[AXIOM_FAILURE_FALLBACK_EMAIL],
            app_name=app_scope,
            log_to_axiom_on_error=False,
        )
        _mark_axiom_failure_alert_sent(key=alert_key)
    except Exception:
        # Never crash app because of fallback alerts
        pass


def _build_test_axiom_event(
    *,
    message: str,
    level: str,
) -> dict:
    severity_fields = _build_severity_fields(level_name=level)
    event: dict[str, object] = {
        "timestamp": datetime.now(CHICAGO_TZ).isoformat(),
        "logger": "axiom-test",
        "message": message,
        "run_id": RUN_ID,
    }
    event.update(severity_fields)
    event.update(_get_host_context())

    tenant_id = get_tenant_id()
    if tenant_id:
        event["tenant_id"] = tenant_id

    request_id = get_request_id()
    if request_id:
        event["request_id"] = request_id

    return event


def send_axiom_test_log(
    *,
    message: str,
    level: str = "INFO",
    force_error: bool = False,
) -> dict[str, object]:
    """
    Send one synchronous test log to Axiom.
    """
    app_scope = get_log_app_scope()
    enabled, token, dataset, api_url, _, _ = _get_axiom_config()
    if not enabled:
        raise RuntimeError("Axiom logging is not configured for this app scope")

    target_api_url = api_url
    if force_error:
        target_api_url = "http://127.0.0.1:9"

    item = _build_test_axiom_event(
        message=message,
        level=level,
    )

    try:
        _post_axiom_batch(
            token=token,
            dataset=dataset,
            api_url=target_api_url,
            items=[item],
        )
        return {
            "sent": True,
            "dataset": dataset,
            "apiUrl": target_api_url,
            "forcedError": force_error,
        }
    except Exception as exc:
        _send_axiom_failure_email(
            app_scope=app_scope,
            dataset=dataset,
            api_url=target_api_url,
            items=[item],
            error=exc,
            force=True,
        )
        raise RuntimeError(f"Axiom ingest failed: {str(exc)}") from exc

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
        severity_fields = _build_severity_fields(
            level_name=record.levelname,
            level_no=record.levelno,
        )
        log: dict = {
            "timestamp": datetime.now(CHICAGO_TZ).isoformat(),
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": RUN_ID,
        }
        log.update(severity_fields)

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

        # Keep canonical severity fields stable for Axiom highlighting,
        # even if extra_fields contains custom level/severity keys.
        log.update(severity_fields)

        return json.dumps(log, default=str)


class AxiomHandler(logging.Handler):
    def __init__(
        self,
        token: str,
        dataset: str,
        api_url: str,
        batch_size: int,
        flush_seconds: float,
        app_scope: str | None = None,
    ) -> None:
        super().__init__()
        self._token = token
        self._dataset = dataset
        self._api_url = api_url
        self._app_scope = app_scope
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
        try:
            _post_axiom_batch(
                token=self._token,
                dataset=self._dataset,
                api_url=self._api_url,
                items=items,
            )
        except Exception as exc:
            _send_axiom_failure_email(
                app_scope=self._app_scope,
                dataset=self._dataset,
                api_url=self._api_url,
                items=items,
                error=exc,
            )
            # Never crash app because of logging
            pass


class AxiomRouterHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self._handlers: dict[str, AxiomHandler] = {}
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        app_scope = get_log_app_scope()
        enabled, token, dataset, api_url, batch_size, flush_seconds = _get_axiom_config()
        if not enabled:
            return

        key = f"{app_scope}:{token}:{dataset}:{api_url}:{batch_size}:{flush_seconds}"
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
                        app_scope=app_scope,
                    )
                    handler.setFormatter(self.formatter or JsonFormatter())
                    handler.setLevel(_get_axiom_level())
                    handler.name = f"axiom-{len(self._handlers) + 1}"
                    self._handlers[key] = handler

        handler.emit(record)

# ======================================================
# HANDLERS
# ======================================================

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

    - Console logging is opt-in via enable_console_logging()
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    if not LOGGING_ENABLED:
        logger.disabled = True
        return logger

    if not any(getattr(h, "name", None) == "axiom" for h in logger.handlers):
        handler = _create_axiom_handler()
        if not isinstance(handler, logging.NullHandler):
            logger.addHandler(handler)

    logger.propagate = False
    _LOGGER_REGISTRY.add(logger)
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
# RUN BOUNDARY HELPERS
# ======================================================

def log_run_start() -> None:
    """
    Mark the start of a run.
    """
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
