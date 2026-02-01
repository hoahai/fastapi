from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request

from shared.utils import load_env
from shared.tenant import get_env

# =========================================================
# ENV
# =========================================================

load_env()


# =========================================================
# EMAIL SENDER
# =========================================================


def send_google_ads_result_email(
    subject: str,
    body: str,
    *,
    html: str | None = None,
    attachments: list[dict] | None = None,
    app_name: str | None = None,
    return_response: bool = False,
    return_payload: bool = False,
):
    """
    Send Google Ads mutation result email using Zoho Mail API.
    """

    access_token = _get_zoho_access_token()
    account_id = get_env("ZOHO_ACCOUNT_ID")
    mail_base_url = str(
        get_env(
            "ZOHO_MAIL_BASE_URL",
            "https://mail.zoho.com",
        )
        or ""
    ).strip()
    mail_base_url = mail_base_url.rstrip("/") if mail_base_url else "https://mail.zoho.com"
    email_from = str(
        get_env(
            "EMAIL_FROM",
            "noreply@theautoadagency.com",
        )
        or ""
    ).strip()
    email_to_raw = get_env(
        "EMAIL_TO",
        "hai@theautoadagency.com",
    )
    email_to = [e.strip() for e in str(email_to_raw).split(",") if e.strip()]

    if not account_id:
        raise RuntimeError("ZOHO_ACCOUNT_ID is not configured")
    if not email_from:
        raise RuntimeError("EMAIL_FROM is not configured")
    if not email_to:
        raise RuntimeError("EMAIL_TO is not configured")

    subject_value = subject

    from_value = email_from
    to_value = ",".join(email_to)

    payload: dict[str, object] = {
        "fromAddress": from_value,
        "toAddress": to_value,
        "subject": subject_value,
        "content": html if html is not None else body,
        "mailFormat": "html" if html is not None else "plaintext",
    }
    if attachments:
        _validate_zoho_attachments(attachments)
        payload["attachments"] = attachments

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{mail_base_url}/api/accounts/{account_id}/messages",
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Zoho-oauthtoken {access_token}",
        },
    )

    if return_payload:
        return {
            "payload": payload,
            "headers": {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "Zoho-oauthtoken [redacted]",
            },
        }

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body_bytes = resp.read()
            response_text = body_bytes.decode("utf-8") if body_bytes else ""
            if resp.status >= 400:
                raise RuntimeError(
                    f"Zoho Mail API error {resp.status}: {response_text}"
                )
            if return_response:
                return {"status": resp.status, "body": response_text}
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"Zoho Mail API error {exc.code}: {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Zoho Mail connection error: {exc.reason}") from exc


_ZOHO_ACCESS_TOKEN: str | None = None
_ZOHO_ACCESS_TOKEN_EXPIRES_AT = 0.0


def _get_zoho_access_token() -> str:
    global _ZOHO_ACCESS_TOKEN, _ZOHO_ACCESS_TOKEN_EXPIRES_AT

    now = time.time()
    if _ZOHO_ACCESS_TOKEN and now < _ZOHO_ACCESS_TOKEN_EXPIRES_AT:
        return _ZOHO_ACCESS_TOKEN

    client_id = get_env("ZOHO_CLIENT_ID")
    client_secret = get_env("ZOHO_CLIENT_SECRET")
    refresh_token = get_env("ZOHO_REFRESH_TOKEN")
    accounts_base_url = str(
        get_env(
            "ZOHO_ACCOUNTS_BASE_URL",
            "https://accounts.zoho.com",
        )
        or ""
    ).strip()
    accounts_base_url = (
        accounts_base_url.rstrip("/") if accounts_base_url else "https://accounts.zoho.com"
    )

    missing: list[str] = []
    if not client_id:
        missing.append("ZOHO_CLIENT_ID")
    if not client_secret:
        missing.append("ZOHO_CLIENT_SECRET")
    if not refresh_token:
        missing.append("ZOHO_REFRESH_TOKEN")
    if missing:
        raise RuntimeError(
            "Zoho OAuth is not configured; missing: " + ", ".join(missing)
        )

    params = {
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    data = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(
        f"{accounts_base_url}/oauth/v2/token",
        data=data,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body_bytes = resp.read()
            response_text = body_bytes.decode("utf-8") if body_bytes else ""
            token_payload = json.loads(response_text) if response_text else {}
            if resp.status >= 400:
                raise RuntimeError(
                    f"Zoho OAuth error {resp.status}: {response_text}"
                )
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"Zoho OAuth error {exc.code}: {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Zoho OAuth connection error: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("Zoho OAuth response was not valid JSON") from exc

    access_token = token_payload.get("access_token")
    if not access_token:
        raise RuntimeError(f"Zoho OAuth response missing access_token: {token_payload}")

    expires_in = token_payload.get("expires_in")
    try:
        expires_in_seconds = int(expires_in) if expires_in is not None else 3600
    except (TypeError, ValueError):
        expires_in_seconds = 3600

    if expires_in_seconds > 100000:
        expires_in_seconds = max(1, expires_in_seconds // 1000)

    _ZOHO_ACCESS_TOKEN = access_token
    _ZOHO_ACCESS_TOKEN_EXPIRES_AT = now + max(0, expires_in_seconds - 60)
    return access_token


def _validate_zoho_attachments(attachments: list[dict]) -> None:
    if not isinstance(attachments, list):
        raise RuntimeError("Zoho attachments must be a list of objects")

    required = {"storeName", "attachmentName", "attachmentPath"}
    for attachment in attachments:
        if not isinstance(attachment, dict):
            raise RuntimeError("Zoho attachments must be a list of objects")
        if not required.issubset(attachment.keys()):
            raise RuntimeError(
                "Zoho attachments require storeName, attachmentName, attachmentPath"
            )
