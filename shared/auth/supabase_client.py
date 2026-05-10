from __future__ import annotations

import json
from datetime import datetime, timezone
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


class SupabaseRestClient:
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

    def _request(
        self,
        *,
        path: str,
        method: str = "GET",
        query: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
        bearer_token: str | None = None,
        use_service_role: bool = False,
    ) -> Any:
        self._require_configured()
        qs = ""
        if query:
            qs = "?" + urlencode(query)

        token = self.service_role_key if use_service_role else self.anon_key
        headers = {
            "apikey": self.anon_key,
            "Authorization": f"Bearer {bearer_token or token}",
        }

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
        try:
            with urlopen(request, timeout=15) as response:
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
        except URLError as exc:
            raise SupabaseClientError(f"Supabase request failed: {exc}") from exc

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
        query = {"select": select, "limit": "1"}
        for key, value in filters.items():
            query[key] = f"eq.{value}"
        rows = self._request(
            path=f"/rest/v1/{table}",
            method="GET",
            query=query,
            use_service_role=True,
        )
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0]
        if not isinstance(row, dict):
            return None
        return row

    def select_many(self, *, table: str, filters: dict[str, str], select: str = "*") -> list[dict[str, Any]]:
        query = {"select": select}
        for key, value in filters.items():
            query[key] = f"eq.{value}"
        rows = self._request(
            path=f"/rest/v1/{table}",
            method="GET",
            query=query,
            use_service_role=True,
        )
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

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
        query: dict[str, str] = {}
        for key, value in filters.items():
            query[key] = f"eq.{value}"
        result = self._request(
            path=f"/rest/v1/{table}",
            method="PATCH",
            query=query,
            body=patch,
            use_service_role=True,
        )
        if not isinstance(result, list):
            return []
        return [row for row in result if isinstance(row, dict)]

    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()


supabase_client = SupabaseRestClient()
