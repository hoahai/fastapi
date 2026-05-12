from __future__ import annotations

from typing import Any

from shared.auth.supabase_client import supabase_client


class SupabaseAuthProvider:
    """Supabase implementation of the auth provider contract."""

    name = "supabase"

    def verify_access_token(self, access_token: str) -> dict[str, Any]:
        return supabase_client.get_user_from_token(access_token)

    def get_auth_user_by_id(self, *, user_id: str) -> dict[str, Any] | None:
        return supabase_client.get_auth_user_by_id(user_id=user_id)

    def select_single(
        self,
        *,
        table: str,
        filters: dict[str, str],
        select: str = "*",
    ) -> dict[str, Any] | None:
        return supabase_client.select_single(table=table, filters=filters, select=select)

    def select_many(
        self,
        *,
        table: str,
        filters: dict[str, str],
        select: str = "*",
    ) -> list[dict[str, Any]]:
        return supabase_client.select_many(table=table, filters=filters, select=select)

    def insert_row(self, *, table: str, row: dict[str, Any]) -> dict[str, Any]:
        return supabase_client.insert_row(table=table, row=row)

    def patch_rows(
        self,
        *,
        table: str,
        filters: dict[str, str],
        patch: dict[str, Any],
    ) -> list[dict[str, Any]]:
        return supabase_client.patch_rows(table=table, filters=filters, patch=patch)

    def delete_rows(
        self,
        *,
        table: str,
        filters: dict[str, str],
    ) -> list[dict[str, Any]]:
        return supabase_client.delete_rows(table=table, filters=filters)

    def now_iso(self) -> str:
        return supabase_client.now_iso()
