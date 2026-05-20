from __future__ import annotations

from collections.abc import Iterable

from shared.auth.page_permissions_catalog import normalize_page_keys
from shared.auth.providers import get_auth_provider
from shared.auth.supabase_client import SupabaseClientError

_TABLE_NAME = "user_page_permissions"


def _provider():
    return get_auth_provider()


def _is_missing_table_error(exc: Exception) -> bool:
    detail = str(exc).lower()
    return _TABLE_NAME in detail and ("does not exist" in detail or "not found" in detail)


def _list_rows(*, tenant_id: str, app_id: str) -> list[dict[str, object]]:
    return _provider().select_many(
        table=_TABLE_NAME,
        filters={
            "tenant_id": tenant_id,
            "app_id": app_id,
            "active": "true",
        },
        select="id,tenant_id,app_id,user_id,page_key,active,created_at,updated_at",
    )


def list_page_permissions_for_scope(
    *,
    tenant_id: str,
    app_id: str,
    user_ids: set[str] | None = None,
) -> list[dict[str, object]]:
    try:
        rows = _list_rows(tenant_id=tenant_id, app_id=app_id)
    except SupabaseClientError as exc:
        if _is_missing_table_error(exc):
            return []
        raise

    if user_ids:
        rows = [row for row in rows if str(row.get("user_id") or "").strip() in user_ids]
    return rows


def list_page_permissions_for_user(
    *,
    user_id: str,
    tenant_id: str,
) -> list[dict[str, object]]:
    normalized_user_id = str(user_id or "").strip()
    normalized_tenant_id = str(tenant_id or "").strip()
    if not normalized_user_id or not normalized_tenant_id:
        return []

    provider = _provider()
    try:
        rows = provider.select_many(
            table=_TABLE_NAME,
            filters={
                "user_id": normalized_user_id,
                "tenant_id": normalized_tenant_id,
                "active": "true",
            },
            select="id,tenant_id,app_id,user_id,page_key,active,created_at,updated_at",
        )
    except SupabaseClientError as exc:
        if _is_missing_table_error(exc):
            return []
        raise

    app_rows = provider.select_many(
        table="apps",
        filters={"active": "true"},
        select="id,code,name,active",
    )
    app_code_by_id = {
        str(row.get("id") or "").strip(): str(row.get("code") or "").strip().lower()
        for row in app_rows
        if str(row.get("id") or "").strip() and str(row.get("code") or "").strip()
    }

    tenant_rows = provider.select_many(
        table="tenants",
        filters={"active": "true"},
        select="id,slug,name,active",
    )
    tenant_slug_by_id = {
        str(row.get("id") or "").strip(): str(row.get("slug") or "").strip().lower()
        for row in tenant_rows
        if str(row.get("id") or "").strip() and str(row.get("slug") or "").strip()
    }

    payload: list[dict[str, object]] = []
    for row in rows:
        app_id = str(row.get("app_id") or "").strip()
        app_code = app_code_by_id.get(app_id)
        if not app_id or not app_code:
            continue
        tenant_id_value = str(row.get("tenant_id") or "").strip()
        tenant_slug = tenant_slug_by_id.get(tenant_id_value)
        page_key = str(row.get("page_key") or "").strip().lower()
        if not page_key:
            continue
        payload.append(
            {
                "tenantId": tenant_id_value,
                "tenantSlug": tenant_slug,
                "appId": app_id,
                "appCode": app_code,
                "pageKey": page_key,
                "updatedAt": row.get("updated_at") or row.get("created_at"),
            }
        )
    return payload


def list_page_keys_for_user_scope(
    *,
    user_id: str,
    tenant_id: str,
    app_id: str,
) -> set[str]:
    normalized_user_id = str(user_id or "").strip()
    normalized_tenant_id = str(tenant_id or "").strip()
    normalized_app_id = str(app_id or "").strip()
    if not normalized_user_id or not normalized_tenant_id or not normalized_app_id:
        return set()

    provider = _provider()
    try:
        rows = provider.select_many(
            table=_TABLE_NAME,
            filters={
                "user_id": normalized_user_id,
                "tenant_id": normalized_tenant_id,
                "app_id": normalized_app_id,
                "active": "true",
            },
            select="page_key",
        )
    except SupabaseClientError as exc:
        if _is_missing_table_error(exc):
            return set()
        raise

    page_keys: set[str] = set()
    for row in rows:
        key = str((row or {}).get("page_key") or "").strip().lower()
        if key:
            page_keys.add(key)
    return page_keys


def replace_page_permissions_for_user(
    *,
    tenant_id: str,
    app_id: str,
    app_code: str,
    user_id: str,
    page_keys: Iterable[object],
) -> list[str]:
    normalized_tenant_id = str(tenant_id or "").strip()
    normalized_app_id = str(app_id or "").strip()
    normalized_user_id = str(user_id or "").strip()
    normalized_app_code = str(app_code or "").strip().lower()
    if not normalized_tenant_id or not normalized_app_id or not normalized_user_id or not normalized_app_code:
        raise SupabaseClientError("tenant_id, app_id, app_code, and user_id are required")

    provider = _provider()
    normalized_page_keys = normalize_page_keys(app_code=normalized_app_code, page_keys=page_keys)
    now_iso = provider.now_iso()

    try:
        provider.delete_rows(
            table=_TABLE_NAME,
            filters={
                "tenant_id": normalized_tenant_id,
                "app_id": normalized_app_id,
                "user_id": normalized_user_id,
            },
        )
        for page_key in normalized_page_keys:
            provider.insert_row(
                table=_TABLE_NAME,
                row={
                    "tenant_id": normalized_tenant_id,
                    "app_id": normalized_app_id,
                    "user_id": normalized_user_id,
                    "page_key": page_key,
                    "active": True,
                    "updated_at": now_iso,
                },
            )
    except SupabaseClientError as exc:
        if _is_missing_table_error(exc):
            raise SupabaseClientError(
                "Page-permission storage is not ready. Apply the user_page_permissions migration first."
            ) from exc
        raise

    return normalized_page_keys
