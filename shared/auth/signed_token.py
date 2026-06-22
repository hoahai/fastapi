from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any, Mapping


_TOKEN_VERSION = "v1"


def _base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("Token segment is empty")
    padding = "=" * (-len(normalized) % 4)
    return base64.urlsafe_b64decode((normalized + padding).encode("ascii"))


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def sign_json_token(*, payload: Mapping[str, Any], secret: str) -> str:
    normalized_secret = str(secret or "").strip()
    if not normalized_secret:
        raise ValueError("secret is required")

    payload_json = _canonical_json(payload)
    payload_segment = _base64url_encode(payload_json)
    signing_input = f"{_TOKEN_VERSION}.{payload_segment}".encode("utf-8")
    signature = hmac.new(
        normalized_secret.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    return f"{_TOKEN_VERSION}.{payload_segment}.{_base64url_encode(signature)}"


def verify_json_token(*, token: str, secret: str) -> dict[str, Any]:
    normalized_secret = str(secret or "").strip()
    if not normalized_secret:
        raise ValueError("secret is required")

    parts = [part.strip() for part in str(token or "").split(".") if part.strip()]
    if len(parts) != 3:
        raise ValueError("Invalid token format")

    version, payload_segment, signature_segment = parts
    if version != _TOKEN_VERSION:
        raise ValueError("Unsupported token version")

    signing_input = f"{version}.{payload_segment}".encode("utf-8")
    expected_signature = hmac.new(
        normalized_secret.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    provided_signature = _base64url_decode(signature_segment)
    if not hmac.compare_digest(expected_signature, provided_signature):
        raise ValueError("Invalid token signature")

    payload = json.loads(_base64url_decode(payload_segment).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Token payload must be an object")
    return payload

