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
    if isinstance(raw, dict):
        payload = dict(raw)
    elif raw is None:
        payload = {}
    else:
        payload = {"detail": raw}

    if "message" not in payload:
        detail = payload.get("detail")
        if isinstance(detail, str) and detail:
            message = detail
        elif isinstance(detail, list) and detail:
            message = "Validation error"
        else:
            err = payload.get("error")
            if isinstance(err, str) and err:
                message = err
            else:
                message = "Request failed"
        payload["message"] = message

    return payload


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
