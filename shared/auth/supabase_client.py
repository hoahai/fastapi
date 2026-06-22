from __future__ import annotations

import json
import socket
from datetime import datetime, timezone
from time import sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from shared.auth.config import (
    get_supabase_anon_key,
    get_supabase_service_role_key,
    get_supabase_url,
)


class SupabaseClientError(RuntimeError):
    pass


class SupabaseAuthError(SupabaseClientError):
    pass


def _is_sb_api_key(value: str) -> bool:
    text = str(value or "").strip()
    return text.startswith("sb_secret_") or text.startswith("sb_publishable_")


def _is_jwt_like(value: str) -> bool:
    text = str(value or "").strip()
    return text.count(".") == 2


class SupabaseRestClient:
    _REQUEST_TIMEOUT_SECONDS = 15
    _TIMEOUT_RETRY_ATTEMPTS = 3
    _TIMEOUT_RETRY_BACKOFF_SECONDS = (0.25, 0.75)

    @staticmethod
    def _is_timeout_url_error(exc: URLError) -> bool:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return True
        return "timed out" in str(reason or "").lower()

    def __init__(self) -> None:
        self.base_url = get_supabase_url()
        self.anon_key = get_supabase_anon_key()
        self.service_role_key = get_supabase_service_role_key()

    def _require_configured(self) -> None:
        if not self.base_url:
            raise SupabaseClientError("SUPABASE_URL is not configured")
        if not self.anon_key:
            raise SupabaseClientError("SUPABASE_ANON_KEY is not configured")
        if not self.service_role_key:
            raise SupabaseClientError("SUPABASE_SERVICE_ROLE_KEY is not configured")

    def _reload_from_env(self) -> None:
        # Keep client in sync with runtime env, especially for local/dev shell changes.
        self.base_url = get_supabase_url()
        self.anon_key = get_supabase_anon_key()
        self.service_role_key = get_supabase_service_role_key()

    def _build_headers(
        self,
        *,
        bearer_token: str | None,
        use_service_role: bool,
        force_key_as_bearer: bool = False,
    ) -> dict[str, str]:
        selected_key = self.service_role_key if use_service_role else self.anon_key
        headers: dict[str, str] = {"apikey": selected_key}

        if bearer_token:
            # Authorization bearer is only for real JWTs (for example user access tokens).
            headers["Authorization"] = f"Bearer {bearer_token}"
            return headers

        if force_key_as_bearer:
            headers["Authorization"] = f"Bearer {selected_key}"
            return headers

        # Backward compatibility for legacy JWT-format anon/service keys.
        # For sb_* keys, never send Authorization bearer with the key itself.
        if _is_jwt_like(selected_key) and not _is_sb_api_key(selected_key):
            headers["Authorization"] = f"Bearer {selected_key}"

        return headers

    def _request(
        self,
        *,
        path: str,
        method: str = "GET",
        query: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
        bearer_token: str | None = None,
        use_service_role: bool = False,
        force_key_as_bearer: bool = False,
    ) -> Any:
        self._reload_from_env()
        self._require_configured()
        qs = ""
        if query:
            qs = "?" + urlencode(query)

        headers = self._build_headers(
            bearer_token=bearer_token,
            use_service_role=use_service_role,
            force_key_as_bearer=force_key_as_bearer,
        )

        data: bytes | None = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            headers["Prefer"] = "return=representation"
            data = json.dumps(body).encode("utf-8")

        request = Request(
            url=f"{self.base_url}{path}{qs}",
            method=method.upper(),
            headers=headers,
            data=data,
        )
        last_timeout_error: Exception | None = None
        for attempt in range(self._TIMEOUT_RETRY_ATTEMPTS):
            try:
                with urlopen(request, timeout=self._REQUEST_TIMEOUT_SECONDS) as response:
                    payload = response.read().decode("utf-8")
                    if not payload.strip():
                        return None
                    return json.loads(payload)
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                message = f"Supabase request failed ({exc.code}): {detail}"
                if path.startswith("/auth/v1/"):
                    raise SupabaseAuthError(message) from exc
                raise SupabaseClientError(message) from exc
            except TimeoutError as exc:
                last_timeout_error = exc
                if attempt < self._TIMEOUT_RETRY_ATTEMPTS - 1:
                    sleep(self._TIMEOUT_RETRY_BACKOFF_SECONDS[min(attempt, len(self._TIMEOUT_RETRY_BACKOFF_SECONDS) - 1)])
                    continue
                message = f"Supabase request timed out for {path}"
                if path.startswith("/auth/v1/"):
                    raise SupabaseAuthError(message) from exc
                raise SupabaseClientError(message) from exc
            except URLError as exc:
                if self._is_timeout_url_error(exc):
                    last_timeout_error = exc
                    if attempt < self._TIMEOUT_RETRY_ATTEMPTS - 1:
                        sleep(self._TIMEOUT_RETRY_BACKOFF_SECONDS[min(attempt, len(self._TIMEOUT_RETRY_BACKOFF_SECONDS) - 1)])
                        continue
                    message = f"Supabase request timed out for {path}"
                    if path.startswith("/auth/v1/"):
                        raise SupabaseAuthError(message) from exc
                    raise SupabaseClientError(message) from exc
                raise SupabaseClientError(f"Supabase request failed: {exc}") from exc

        message = f"Supabase request timed out for {path}"
        if path.startswith("/auth/v1/"):
            raise SupabaseAuthError(message) from last_timeout_error
        raise SupabaseClientError(message) from last_timeout_error

    def get_user_from_token(self, access_token: str) -> dict[str, Any]:
        payload = self._request(
            path="/auth/v1/user",
            method="GET",
            bearer_token=access_token,
            use_service_role=False,
        )
        if not isinstance(payload, dict):
            raise SupabaseAuthError("Invalid Supabase auth response")
        return payload

    def select_single(self, *, table: str, filters: dict[str, str], select: str = "*") -> dict[str, Any] | None:
        rows = self.select_many_query(table=table, query={**{"select": select, "limit": "1"}, **{key: f"eq.{value}" for key, value in filters.items()}})
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0]
        if not isinstance(row, dict):
            return None
        return row

    def select_many(self, *, table: str, filters: dict[str, str], select: str = "*") -> list[dict[str, Any]]:
        rows = self.select_many_query(
            table=table,
            query={"select": select, **{key: f"eq.{value}" for key, value in filters.items()}},
        )
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

    def select_many_query(self, *, table: str, query: dict[str, str]) -> Any:
        return self._request(
            path=f"/rest/v1/{table}",
            method="GET",
            query=query,
            use_service_role=True,
        )

    def select_single_query(self, *, table: str, query: dict[str, str], select: str = "*") -> dict[str, Any] | None:
        rows = self.select_many_query(table=table, query={"select": select, "limit": "1", **query})
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0]
        if not isinstance(row, dict):
            return None
        return row

    def insert_row(self, *, table: str, row: dict[str, Any]) -> dict[str, Any]:
        result = self._request(
            path=f"/rest/v1/{table}",
            method="POST",
            body=row,
            use_service_role=True,
        )
        if isinstance(result, list) and result and isinstance(result[0], dict):
            return result[0]
        if isinstance(result, dict):
            return result
        raise SupabaseClientError(f"Unexpected insert response for table '{table}'")

    def patch_rows(self, *, table: str, filters: dict[str, str], patch: dict[str, Any]) -> list[dict[str, Any]]:
        result = self.patch_rows_query(table=table, query={key: f"eq.{value}" for key, value in filters.items()}, patch=patch)
        if not isinstance(result, list):
            return []
        return [row for row in result if isinstance(row, dict)]

    def patch_rows_query(self, *, table: str, query: dict[str, str], patch: dict[str, Any]) -> Any:
        return self._request(
            path=f"/rest/v1/{table}",
            method="PATCH",
            query=query,
            body=patch,
            use_service_role=True,
        )

    def delete_rows(self, *, table: str, filters: dict[str, str]) -> list[dict[str, Any]]:
        result = self.delete_rows_query(table=table, query={key: f"eq.{value}" for key, value in filters.items()})
        if not isinstance(result, list):
            return []
        return [row for row in result if isinstance(row, dict)]

    def delete_rows_query(self, *, table: str, query: dict[str, str]) -> Any:
        return self._request(
            path=f"/rest/v1/{table}",
            method="DELETE",
            query=query,
            use_service_role=True,
        )

    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def get_auth_user_by_id(self, *, user_id: str) -> dict[str, Any] | None:
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return None
        result = self._request(
            path=f"/auth/v1/admin/users/{normalized_user_id}",
            method="GET",
            use_service_role=True,
            force_key_as_bearer=True,
        )
        if not isinstance(result, dict):
            return None
        return result

    def update_auth_user_by_id(self, *, user_id: str, attributes: dict[str, Any]) -> dict[str, Any] | None:
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            raise SupabaseClientError("user_id is required")
        payload = attributes if isinstance(attributes, dict) else {}
        if not payload:
            raise SupabaseClientError("attributes payload is required")

        result = self._request(
            path=f"/auth/v1/admin/users/{normalized_user_id}",
            method="PUT",
            body=payload,
            use_service_role=True,
            force_key_as_bearer=True,
        )
        if not isinstance(result, dict):
            return None
        return result

    def send_password_recovery_email(self, *, email: str, redirect_to: str | None = None) -> None:
        normalized_email = str(email or "").strip().lower()
        if not normalized_email:
            raise SupabaseClientError("Email is required for password recovery")
        normalized_redirect_to = str(redirect_to or "").strip()
        body: dict[str, Any] = {"email": normalized_email}
        if normalized_redirect_to:
            body["redirect_to"] = normalized_redirect_to
        self._request(
            path="/auth/v1/recover",
            method="POST",
            body=body,
            use_service_role=True,
            force_key_as_bearer=True,
        )


supabase_client = SupabaseRestClient()
