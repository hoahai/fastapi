from __future__ import annotations

from typing import Any, Protocol


class AuthDataProvider(Protocol):
    """Provider boundary for identity verification + auth data persistence.

    Route handlers and repositories should depend on this contract instead of
    calling provider-specific clients directly.
    """

    name: str

    def verify_access_token(self, access_token: str) -> dict[str, Any]:
        ...

    def select_single(
        self,
        *,
        table: str,
        filters: dict[str, str],
        select: str = "*",
    ) -> dict[str, Any] | None:
        ...

    def select_many(
        self,
        *,
        table: str,
        filters: dict[str, str],
        select: str = "*",
    ) -> list[dict[str, Any]]:
        ...

    def insert_row(self, *, table: str, row: dict[str, Any]) -> dict[str, Any]:
        ...

    def patch_rows(
        self,
        *,
        table: str,
        filters: dict[str, str],
        patch: dict[str, Any],
    ) -> list[dict[str, Any]]:
        ...

    def now_iso(self) -> str:
        ...
