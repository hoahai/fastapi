from __future__ import annotations

from datetime import datetime
import time
import uuid
from zoneinfo import ZoneInfo

from fastapi import Request

from shared.tenant import get_timezone
from shared.utils import format_hms


def ensure_request_id(request: Request) -> str:
    request_id = getattr(request.state, "request_id", None)
    if not request_id:
        request_id = uuid.uuid4().hex[:8]
        request.state.request_id = request_id
    return request_id


def build_meta(
    request: Request,
    *,
    duration_s: float | None = None,
) -> dict[str, object]:
    if duration_s is None:
        start_time = getattr(request.state, "start_time", None)
        if isinstance(start_time, (int, float)):
            duration_s = time.perf_counter() - start_time
        else:
            duration_s = 0.0

    return {
        "timestamp": datetime.now(ZoneInfo(get_timezone())).isoformat(),
        "duration_ms": int(duration_s * 1000),
        "duration_hms": format_hms(duration_s),
        "client_id": getattr(request.state, "client_id", "Not Found"),
        "request_id": ensure_request_id(request),
    }


def normalize_error_payload(raw: object | None) -> dict[str, object]:
    detail_raw: object | None
    if isinstance(raw, dict):
        raw_dict = dict(raw)
        detail_raw = raw_dict.get("detail", raw_dict)
    else:
        detail_raw = raw

    detail = _normalize_error_detail(detail_raw)
    return {
        "detail": detail,
        "message": "Request failed",
    }


def _normalize_error_detail(raw: object | None) -> dict[str, object]:
    if isinstance(raw, dict):
        detail = dict(raw)

        if "errors" not in detail and isinstance(detail.get("items"), list):
            detail["errors"] = detail["items"]

        error_value = detail.get("error")
        message_value = detail.get("message")
        messages_value = detail.get("messages")
        errors_value = detail.get("errors")

        if not isinstance(errors_value, list):
            errors_value = []
        detail["errors"] = errors_value

        if not isinstance(messages_value, list):
            if isinstance(message_value, str) and message_value:
                messages_value = [message_value]
            else:
                messages_value = []
        else:
            messages_value = [str(item) for item in messages_value if str(item)]
        detail["messages"] = messages_value

        if not isinstance(message_value, str) or not message_value:
            if messages_value:
                message_value = "; ".join(messages_value)
            elif isinstance(error_value, str) and error_value:
                message_value = error_value
            else:
                message_value = "Request failed"
            detail["message"] = message_value

        if not isinstance(error_value, str) or not error_value:
            detail["error"] = "Invalid payload" if errors_value else "Request failed"

        return detail

    if isinstance(raw, list):
        messages = [str(item) for item in raw if str(item)]
        message = "; ".join(messages) if messages else "Validation error"
        return {
            "error": "Invalid payload",
            "message": message,
            "messages": messages or [message],
            "errors": raw,
        }

    if isinstance(raw, str):
        message = raw or "Request failed"
        return {
            "error": "Request failed",
            "message": message,
            "messages": [message],
            "errors": [],
        }

    message = "Request failed"
    return {
        "error": "Request failed",
        "message": message,
        "messages": [message],
        "errors": [],
    }


def wrap_success(
    data: object,
    request: Request,
    *,
    duration_s: float | None = None,
) -> dict[str, object]:
    return {
        "meta": build_meta(request, duration_s=duration_s),
        "data": data,
    }


def wrap_error(
    error: object | None,
    request: Request,
    *,
    duration_s: float | None = None,
) -> dict[str, object]:
    return {
        "meta": build_meta(request, duration_s=duration_s),
        "error": normalize_error_payload(error),
    }
