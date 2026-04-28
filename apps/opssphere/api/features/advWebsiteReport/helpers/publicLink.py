from __future__ import annotations

import base64
from datetime import datetime
import hashlib
import hmac
import json
import os
import time

from apps.opssphere.api.helpers.config import get_public_config
from shared.tenant import get_app_scoped_env

_TOKEN_VERSION = 1
_DEFAULT_SIGNED_REPORT_TTL_DAYS = 30


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _base64url_decode(text: str) -> bytes:
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("Invalid token payload.")
    padding = "=" * ((4 - len(raw) % 4) % 4)
    try:
        return base64.urlsafe_b64decode(raw + padding)
    except Exception as exc:
        raise ValueError("Invalid token encoding.") from exc


def _normalize_account_code(value: object) -> str:
    text = str(value or "").strip().upper()
    if not text:
        raise ValueError("accountCode is required in token payload.")
    return text


def _normalize_tenant_id(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        raise ValueError("tenantId is required in token payload.")
    return text


def resolve_public_signing_secret() -> str:
    public_config = get_public_config()
    config_secret = str(public_config.get("signing_secret") or "").strip()
    if config_secret:
        return config_secret

    env_secret = str(
        get_app_scoped_env("OpsSphere", "PUBLIC_SIGNING_SECRET")
        or os.getenv("OPSSPHERE_PUBLIC_SIGNING_SECRET")
        or ""
    ).strip()
    if env_secret:
        return env_secret
    raise ValueError(
        "Missing public signing secret. Set opssphere.public.signing_secret "
        "or OPSSPHERE_PUBLIC_SIGNING_SECRET."
    )


def get_signed_report_ttl_days() -> int:
    public_config = get_public_config()
    try:
        value = int(public_config.get("signed_report_ttl_days") or 0)
    except (TypeError, ValueError):
        value = 0
    return value if value > 0 else _DEFAULT_SIGNED_REPORT_TTL_DAYS


def create_signed_report_token(
    *,
    tenant_id: str,
    account_code: str,
    month: int,
    year: int,
    start_date: str = "",
    end_date: str = "",
    ttl_days: int,
    secret: str,
) -> tuple[str, dict[str, object]]:
    now = int(time.time())
    ttl_seconds = max(1, int(ttl_days) * 24 * 60 * 60)
    payload: dict[str, object] = {
        "v": _TOKEN_VERSION,
        "tenantId": _normalize_tenant_id(tenant_id),
        "accountCode": _normalize_account_code(account_code),
        "month": int(month),
        "year": int(year),
        "iat": now,
        "exp": now + ttl_seconds,
        "report": "opssphere.advWebsiteReport.cta",
    }
    start_date_text = str(start_date or "").strip()
    end_date_text = str(end_date or "").strip()
    if start_date_text or end_date_text:
        if not start_date_text or not end_date_text:
            raise ValueError("start_date and end_date must be provided together.")
        try:
            payload["startDate"] = datetime.strptime(start_date_text, "%Y-%m-%d").strftime(
                "%Y-%m-%d"
            )
            payload["endDate"] = datetime.strptime(end_date_text, "%Y-%m-%d").strftime(
                "%Y-%m-%d"
            )
        except ValueError as exc:
            raise ValueError("start_date and end_date must be in YYYY-MM-DD format.") from exc
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    signature = hmac.new(
        str(secret).encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).digest()
    token = f"{_base64url_encode(payload_bytes)}.{_base64url_encode(signature)}"
    return token, payload


def decode_signed_report_payload_without_verify(token: str) -> dict[str, object]:
    parts = str(token or "").split(".")
    if len(parts) != 2:
        raise ValueError("Invalid token format.")
    payload_bytes = _base64url_decode(parts[0])
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise ValueError("Invalid token payload.") from exc
    if not isinstance(payload, dict):
        raise ValueError("Invalid token payload.")
    return payload


def verify_signed_report_token(token: str, *, secret: str) -> dict[str, object]:
    parts = str(token or "").split(".")
    if len(parts) != 2:
        raise ValueError("Invalid token format.")
    payload_b64, signature_b64 = parts
    payload_bytes = _base64url_decode(payload_b64)
    provided_signature = _base64url_decode(signature_b64)
    expected_signature = hmac.new(
        str(secret).encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(provided_signature, expected_signature):
        raise ValueError("Invalid token signature.")

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise ValueError("Invalid token payload.") from exc
    if not isinstance(payload, dict):
        raise ValueError("Invalid token payload.")

    if int(payload.get("v") or 0) != _TOKEN_VERSION:
        raise ValueError("Unsupported token version.")
    if str(payload.get("report") or "") != "opssphere.advWebsiteReport.cta":
        raise ValueError("Invalid token report scope.")

    tenant_id = _normalize_tenant_id(payload.get("tenantId"))
    account_code = _normalize_account_code(payload.get("accountCode"))
    try:
        month = int(payload.get("month"))
        year = int(payload.get("year"))
        exp = int(payload.get("exp"))
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid token claims.") from exc
    if month < 1 or month > 12 or year < 2000 or year > 2100:
        raise ValueError("Invalid token period.")
    # Backward compatibility for legacy tokens using `date`.
    legacy_date_text = str(payload.get("date") or "").strip()
    start_date_text = str(payload.get("startDate") or "").strip()
    end_date_text = str(payload.get("endDate") or "").strip()
    if legacy_date_text and not start_date_text and not end_date_text:
        start_date_text = legacy_date_text
        end_date_text = legacy_date_text
    if start_date_text or end_date_text:
        if not start_date_text or not end_date_text:
            raise ValueError("Invalid token date range.")
        try:
            start_date_text = datetime.strptime(start_date_text, "%Y-%m-%d").strftime(
                "%Y-%m-%d"
            )
            end_date_text = datetime.strptime(end_date_text, "%Y-%m-%d").strftime(
                "%Y-%m-%d"
            )
        except ValueError as exc:
            raise ValueError("Invalid token date range.") from exc
        if start_date_text > end_date_text:
            raise ValueError("Invalid token date range.")

    now = int(time.time())
    if exp <= now:
        raise ValueError("Token expired.")

    payload["tenantId"] = tenant_id
    payload["accountCode"] = account_code
    payload["month"] = month
    payload["year"] = year
    payload["startDate"] = start_date_text
    payload["endDate"] = end_date_text
    payload["exp"] = exp
    return payload
