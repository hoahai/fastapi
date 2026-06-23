from __future__ import annotations

import hashlib
import json
import secrets
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal
from uuid import uuid4

from apps.leavesphere.api.v1.helpers.dbQueries import (
    get_employee_managers,
    get_employees_by_ids,
    get_pto_transactions,
    get_pto_types,
)
from apps.leavesphere.api.v1.helpers.approvalWorkflow import finalize_pending_pto_action
from shared.auth.quick_approval_url import build_quick_approval_url
from shared.auth.signed_token import decode_json_token_payload_unverified, sign_json_token, verify_json_token
from shared.auth.supabase_client import SupabaseClientError, supabase_client
from shared.tenantDataCache import (
    delete_tenant_shared_cache_values_by_prefix,
    get_tenant_shared_cache_value,
    set_tenant_shared_cache_value,
)
from shared.tenant import TenantConfigError, get_app_scoped_env, get_env, set_tenant_context, reset_tenant_context

_GRANT_TABLE = "leave_sphere_quick_approval_grants"
_APP_CODE = "leavesphere"
_ROLE_MANAGER = "manager"
_ROLE_ADMIN = "admin"
_ROLE_PRIORITY = {_ROLE_MANAGER: 0, _ROLE_ADMIN: 1}
# Short-lived caches keep the background refresh fast without making the
# greeting, avatar, or request preview permanently stale.
_CONTACT_CACHE_BUCKET = "leavesphere_quick_approval"
_CONTACT_CACHE_TTL_SECONDS = 900
_REFRESH_CACHE_TTL_SECONDS = 900
_TRANSACTION_CACHE_TTL_SECONDS = 900


@dataclass(frozen=True)
class QuickApprovalRecipient:
    role: Literal["manager", "admin"]
    name: str
    email: str
    employee_id: str | None
    picture_url: str | None = None


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _normalize_email(value: object | None) -> str:
    return _normalize_text(value).lower()


def _coerce_number(value: object | None) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if parsed.is_integer():
        return int(parsed)
    return round(parsed, 2)


def _normalize_preview_date(value: object | None) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return text


def _normalize_cache_component(value: object | None) -> str:
    text = _normalize_text(value).lower()
    return text or "unknown"


def _build_employee_contact(*, employee_id: str, row: dict[str, object]) -> dict[str, object]:
    first_name = _normalize_text(row.get("firstName"))
    last_name = _normalize_text(row.get("lastName"))
    email = _normalize_email(row.get("email"))
    full_name = " ".join(part for part in (first_name, last_name) if part).strip() or email or employee_id
    return {
        "employeeId": employee_id,
        "employeeName": full_name,
        "pictureUrl": _normalize_text(row.get("pictureUrl")) or None,
    }


def _read_employee_contact_cache(*, tenant_id: str, employee_id: str) -> dict[str, object] | None:
    cache_key = f"employee-contact::{_normalize_text(employee_id)}"
    value, found = get_tenant_shared_cache_value(
        bucket=_CONTACT_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_CONTACT_CACHE_TTL_SECONDS,
        tenant_id=tenant_id,
    )
    if not found or not isinstance(value, dict):
        return None
    return value


def _write_employee_contact_cache(*, tenant_id: str, employee_id: str, value: dict[str, object]) -> None:
    cache_key = f"employee-contact::{_normalize_text(employee_id)}"
    set_tenant_shared_cache_value(
        bucket=_CONTACT_CACHE_BUCKET,
        cache_key=cache_key,
        value=value,
        tenant_id=tenant_id,
    )


def _load_employee_contact(*, tenant_id: str, employee_id: str, use_cache: bool = True) -> dict[str, object] | None:
    normalized_tenant_id = _normalize_text(tenant_id)
    normalized_employee_id = _normalize_text(employee_id)
    if not normalized_tenant_id or not normalized_employee_id:
        return None
    if use_cache:
        cached = _read_employee_contact_cache(tenant_id=normalized_tenant_id, employee_id=normalized_employee_id)
        if cached is not None:
            return cached

    rows = get_employees_by_ids(employee_ids=[normalized_employee_id])
    if not rows:
        return None
    contact = _build_employee_contact(employee_id=normalized_employee_id, row=rows[0])
    _write_employee_contact_cache(tenant_id=normalized_tenant_id, employee_id=normalized_employee_id, value=contact)
    return contact


def _build_refresh_cache_key(
    *,
    request_id: str,
    recipient_role: str,
    recipient_email: str,
    recipient_employee_id: str | None,
    grant_jti: str,
) -> str:
    payload = {
        "grant_jti": _normalize_text(grant_jti),
        "recipient_email": _normalize_email(recipient_email),
        "recipient_employee_id": _normalize_text(recipient_employee_id) or None,
        "recipient_role": _normalize_cache_component(recipient_role),
        "request_id": _normalize_text(request_id),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return f"refresh::{payload['request_id']}::{digest}"


def _read_quick_approval_refresh_cache(
    *,
    tenant_id: str,
    request_id: str,
    recipient_role: str,
    recipient_email: str,
    recipient_employee_id: str | None,
    grant_jti: str,
) -> dict[str, object] | None:
    cache_key = _build_refresh_cache_key(
        request_id=request_id,
        recipient_role=recipient_role,
        recipient_email=recipient_email,
        recipient_employee_id=recipient_employee_id,
        grant_jti=grant_jti,
    )
    value, found = get_tenant_shared_cache_value(
        bucket=_CONTACT_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_REFRESH_CACHE_TTL_SECONDS,
        tenant_id=tenant_id,
    )
    if not found or not isinstance(value, dict):
        return None
    return value


def _write_quick_approval_refresh_cache(
    *,
    tenant_id: str,
    request_id: str,
    recipient_role: str,
    recipient_email: str,
    recipient_employee_id: str | None,
    grant_jti: str,
    value: dict[str, object],
) -> None:
    cache_key = _build_refresh_cache_key(
        request_id=request_id,
        recipient_role=recipient_role,
        recipient_email=recipient_email,
        recipient_employee_id=recipient_employee_id,
        grant_jti=grant_jti,
    )
    cache_value = deepcopy(value)
    cache_value.pop("debugTrace", None)
    set_tenant_shared_cache_value(
        bucket=_CONTACT_CACHE_BUCKET,
        cache_key=cache_key,
        value=cache_value,
        tenant_id=tenant_id,
    )


def _clear_quick_approval_refresh_cache(*, tenant_id: str, request_id: str) -> int:
    return delete_tenant_shared_cache_values_by_prefix(
        bucket=_CONTACT_CACHE_BUCKET,
        cache_key_prefix=f"refresh::{_normalize_text(request_id)}::",
        tenant_id=tenant_id,
    )


def clear_quick_approval_refresh_cache(*, tenant_id: str, request_id: str) -> int:
    return _clear_quick_approval_refresh_cache(tenant_id=tenant_id, request_id=request_id)


def _build_transaction_cache_key(*, request_id: str) -> str:
    return f"transaction::{_normalize_text(request_id)}"


def _read_quick_approval_transaction_cache(*, tenant_id: str, request_id: str) -> dict[str, object] | None:
    cache_key = _build_transaction_cache_key(request_id=request_id)
    value, found = get_tenant_shared_cache_value(
        bucket=_CONTACT_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_TRANSACTION_CACHE_TTL_SECONDS,
        tenant_id=tenant_id,
    )
    if not found or not isinstance(value, dict):
        return None
    return value


def _write_quick_approval_transaction_cache(
    *,
    tenant_id: str,
    request_id: str,
    value: dict[str, object],
) -> None:
    cache_key = _build_transaction_cache_key(request_id=request_id)
    cache_value = deepcopy(value)
    set_tenant_shared_cache_value(
        bucket=_CONTACT_CACHE_BUCKET,
        cache_key=cache_key,
        value=cache_value,
        tenant_id=tenant_id,
    )


def _clear_quick_approval_transaction_cache(*, tenant_id: str, request_id: str) -> int:
    return delete_tenant_shared_cache_values_by_prefix(
        bucket=_CONTACT_CACHE_BUCKET,
        cache_key_prefix=f"transaction::{_normalize_text(request_id)}",
        tenant_id=tenant_id,
    )


def clear_quick_approval_transaction_cache(*, tenant_id: str, request_id: str) -> int:
    return _clear_quick_approval_transaction_cache(tenant_id=tenant_id, request_id=request_id)


def _parse_iso_datetime(value: object | None) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _grant_payload_from_row(row: dict[str, object]) -> dict[str, object]:
    issued_at = _parse_iso_datetime(row.get("issued_at")) or _now_utc()
    expires_at = _parse_iso_datetime(row.get("expires_at")) or _now_utc()
    return {
        "tenant_id": _normalize_text(row.get("tenant_id")),
        "tenant_slug": _normalize_text(row.get("tenant_slug")),
        "app_code": _APP_CODE,
        "request_id": _normalize_text(row.get("request_id")),
        "recipient_email": _normalize_text(row.get("recipient_email")).lower(),
        "recipient_name": _normalize_text(row.get("recipient_name")) or None,
        "recipient_role": _normalize_text(row.get("recipient_role")).lower(),
        "recipient_employee_id": _normalize_text(row.get("recipient_employee_id")) or None,
        "recipient_picture_url": _normalize_text(row.get("recipient_picture_url")) or None,
        "jti": _normalize_text(row.get("jti")),
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }


def _grant_delivery_from_row(row: dict[str, object], *, base_url: str | None) -> dict[str, object] | None:
    token = _normalize_text(row.get("token"))
    if not token:
        secret = _get_secret()
        if not secret:
            return None
        token = sign_json_token(payload=_grant_payload_from_row(row), secret=secret)
    expires_at = _normalize_text(row.get("expires_at"))
    return {
        "grant_id": _normalize_text(row.get("id")) or None,
        "jti": _normalize_text(row.get("jti")) or None,
        "tenant_id": _normalize_text(row.get("tenant_id")) or None,
        "tenant_slug": _normalize_text(row.get("tenant_slug")) or None,
        "token": token,
        "url": build_quick_approval_url(token=token, base_url=base_url or ""),
        "recipient_email": _normalize_text(row.get("recipient_email")).lower(),
        "recipient_name": _normalize_text(row.get("recipient_name")) or None,
        "recipient_role": _normalize_text(row.get("recipient_role")).lower() or None,
        "recipient_picture_url": _normalize_text(row.get("recipient_picture_url")) or None,
        "expires_at": expires_at or None,
    }


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _get_secret() -> str | None:
    secret = (
        get_app_scoped_env(_APP_CODE, "QUICK_APPROVAL_SECRET")
        or get_env("LEAVESPHERE_QUICK_APPROVAL_SECRET")
        or get_env("QUICK_APPROVAL_SECRET")
    )
    normalized = _normalize_text(secret)
    return normalized or None


def _get_ttl_hours() -> int:
    raw = (
        get_app_scoped_env(_APP_CODE, "QUICK_APPROVAL_TTL_HOURS")
        or get_env("LEAVESPHERE_QUICK_APPROVAL_TTL_HOURS")
        or get_env("QUICK_APPROVAL_TTL_HOURS")
    )
    try:
        hours = int(_normalize_text(raw) or "48")
    except ValueError:
        hours = 48
    return max(hours, 1)


def _get_app_id() -> str | None:
    try:
        row = supabase_client.select_single_query(
            table="apps",
            query={
                "code": f"eq.{_APP_CODE}",
                "active": "eq.true",
            },
            select="id,code",
        )
    except SupabaseClientError:
        return _APP_CODE
    if not row:
        return _APP_CODE
    app_id = _normalize_text(row.get("id"))
    code = _normalize_text(row.get("code")).lower()
    if not app_id or code != _APP_CODE:
        return _APP_CODE
    return app_id


def _resolve_tenant_id(*, tenant_slug: str) -> str | None:
    normalized_tenant_slug = _normalize_text(tenant_slug)
    if not normalized_tenant_slug:
        return None
    try:
        row = supabase_client.select_single_query(
            table="tenants",
            query={
                "slug": f"eq.{normalized_tenant_slug}",
                "active": "eq.true",
            },
            select="id,slug",
        )
    except SupabaseClientError:
        return None
    if not row:
        return None
    tenant_id = _normalize_text(row.get("id"))
    resolved_slug = _normalize_text(row.get("slug"))
    if not tenant_id or resolved_slug.lower() != normalized_tenant_slug.lower():
        return None
    return tenant_id


def _resolve_tenant_scope(*, tenant_id: str | None, tenant_slug: str | None) -> tuple[str, str]:
    normalized_tenant_slug = _normalize_text(tenant_slug)
    normalized_tenant_id = _normalize_text(tenant_id)
    if not normalized_tenant_id and normalized_tenant_slug:
        normalized_tenant_id = _resolve_tenant_id(tenant_slug=normalized_tenant_slug) or normalized_tenant_slug
    return normalized_tenant_id, normalized_tenant_slug


def _get_tenant_active_user_ids(*, tenant_id: str) -> set[str]:
    try:
        rows = supabase_client.select_many_query(
            table="tenant_users",
            query={
                "select": "user_id,status",
                "tenant_id": f"eq.{tenant_id}",
                "status": "eq.active",
            },
        )
    except SupabaseClientError:
        return set()
    user_ids: set[str] = set()
    for row in rows or []:
        user_id = _normalize_text((row or {}).get("user_id"))
        if user_id:
            user_ids.add(user_id)
    return user_ids


def _load_profile_contact(*, user_id: str) -> QuickApprovalRecipient | None:
    try:
        profile = supabase_client.select_single_query(
            table="profiles",
            query={
                "user_id": f"eq.{user_id}",
            },
            select="user_id,email,full_name",
        )
    except SupabaseClientError:
        profile = None
    if profile is None:
        auth_user = supabase_client.get_auth_user_by_id(user_id=user_id) or {}
        email = _normalize_email(auth_user.get("email"))
        full_name = _normalize_text(auth_user.get("user_metadata", {}).get("full_name")) if isinstance(auth_user.get("user_metadata"), dict) else ""
        if not email or "@" not in email:
            return None
        name = full_name or email
        return QuickApprovalRecipient(role=_ROLE_ADMIN, name=name, email=email, employee_id=None)

    email = _normalize_email(profile.get("email"))
    if not email or "@" not in email:
        return None
    name = _normalize_text(profile.get("full_name")) or email
    return QuickApprovalRecipient(role=_ROLE_ADMIN, name=name, email=email, employee_id=None)


def _load_admin_recipients(*, tenant_id: str) -> list[QuickApprovalRecipient]:
    app_id = _get_app_id()
    if not app_id:
        return []

    user_ids: set[str] = set()
    try:
        role_rows = supabase_client.select_many_query(
            table="tenant_app_roles",
            query={
                "select": "user_id,role",
                "tenant_id": f"eq.{tenant_id}",
                "app_id": f"eq.{app_id}",
            },
        )
    except SupabaseClientError:
        role_rows = []
    for row in role_rows or []:
        user_id = _normalize_text((row or {}).get("user_id"))
        role = _normalize_text((row or {}).get("role")).lower()
        if user_id and role == _ROLE_ADMIN:
            user_ids.add(user_id)

    super_admin_ids: set[str] = set()
    try:
        global_role_rows = supabase_client.select_many_query(
            table="user_global_roles",
            query={
                "select": "user_id,role,active",
                "active": "eq.true",
            },
        )
    except SupabaseClientError:
        global_role_rows = []
    for row in global_role_rows or []:
        user_id = _normalize_text((row or {}).get("user_id"))
        role = _normalize_text((row or {}).get("role")).lower()
        if user_id and role == "super_admin":
            user_ids.add(user_id)
            super_admin_ids.add(user_id)

    tenant_active_user_ids = _get_tenant_active_user_ids(tenant_id=tenant_id)
    recipients: list[QuickApprovalRecipient] = []
    for user_id in sorted(user_ids):
        is_active_member = not tenant_active_user_ids or user_id in tenant_active_user_ids
        if not is_active_member and user_id not in super_admin_ids:
            continue
        profile = _load_profile_contact(user_id=user_id)
        if profile is None:
            continue
        recipients.append(profile)
    return recipients


def _load_manager_recipients(*, employee_id: str) -> list[QuickApprovalRecipient]:
    manager_rows = get_employee_managers(employee_id=employee_id)
    if not manager_rows:
        return []

    manager_ids = []
    for row in manager_rows:
        manager_id = _normalize_text(row.get("managerId"))
        if manager_id:
            manager_ids.append(manager_id)
    if not manager_ids:
        return []

    manager_employees = get_employees_by_ids(employee_ids=manager_ids)
    employees_by_id = {str(row.get("id") or "").strip(): row for row in manager_employees if str(row.get("id") or "").strip()}
    recipients: list[QuickApprovalRecipient] = []
    for manager_id in manager_ids:
        employee = employees_by_id.get(manager_id)
        if not employee:
            continue
        if int(employee.get("active") or 0) != 1:
            continue
        email = _normalize_email(employee.get("email"))
        if not email or "@" not in email:
            continue
        first_name = _normalize_text(employee.get("firstName"))
        last_name = _normalize_text(employee.get("lastName"))
        full_name = " ".join(part for part in (first_name, last_name) if part).strip() or email
        recipients.append(
            QuickApprovalRecipient(
                role=_ROLE_MANAGER,
                name=full_name,
                email=email,
                employee_id=manager_id,
                picture_url=_normalize_text(employee.get("pictureUrl")) or None,
            )
        )
    return recipients


def _dedupe_recipients(*groups: list[QuickApprovalRecipient]) -> list[QuickApprovalRecipient]:
    merged: dict[str, QuickApprovalRecipient] = {}
    for group in groups:
        for recipient in group:
            email_key = _normalize_email(recipient.email)
            if not email_key:
                continue
            current = merged.get(email_key)
            if current is None:
                merged[email_key] = recipient
                continue
            if _ROLE_PRIORITY.get(recipient.role, 0) > _ROLE_PRIORITY.get(current.role, 0):
                merged[email_key] = recipient
    return list(merged.values())


def _build_request_snapshot(*, tenant_id: str | None, transaction: dict) -> dict[str, object]:
    employee_id = _normalize_text(transaction.get("employeeId"))
    employee_name = ""
    picture_url = None
    if employee_id:
        contact = _load_employee_contact(
            tenant_id=_normalize_text(tenant_id),
            employee_id=employee_id,
        )
        if contact:
            employee_name = _normalize_text(contact.get("employeeName"))
            picture_url = _normalize_text(contact.get("pictureUrl")) or None

    pto_type_code = _normalize_text(transaction.get("ptoTypeCode")).upper()
    pto_type_label = pto_type_code
    if pto_type_code:
        pto_rows = get_pto_types(code=pto_type_code)
        if pto_rows:
            pto_type_label = _normalize_text(pto_rows[0].get("name")) or pto_type_code

    hours = transaction.get("hours")

    return {
        "requestId": _normalize_text(transaction.get("id")) or None,
        "employeeId": employee_id or None,
        "employeeName": employee_name or employee_id or "Employee",
        "ptoTypeCode": pto_type_code or None,
        "ptoTypeLabel": pto_type_label or "PTO",
        "startDate": _normalize_preview_date(transaction.get("startDate")),
        "endDate": _normalize_preview_date(transaction.get("endDate")),
        "hoursRequested": _coerce_number(hours),
        "reason": _normalize_text(transaction.get("description")) or None,
        "currentStatus": "pending",
    }


def _create_grant(
    *,
    tenant_id: str,
    tenant_slug: str,
    request_id: str,
    snapshot: dict[str, object],
    recipient: QuickApprovalRecipient,
    base_url: str | None,
    created_by_user_id: str | None = None,
    debug_trace: dict[str, object] | None = None,
) -> dict[str, object] | None:
    secret = _get_secret()
    app_id = _get_app_id()
    normalized_tenant_id = _normalize_text(tenant_id)
    normalized_tenant_slug = _normalize_text(tenant_slug)
    normalized_request_id = _normalize_text(request_id)
    if (
        not secret
        or not app_id
        or not normalized_tenant_id
        or not normalized_tenant_slug
        or not normalized_request_id
    ):
        if isinstance(debug_trace, dict):
            debug_trace["reason"] = (
                "missing_secret"
                if not secret
                else "missing_app_id"
                if not app_id
                else "missing_tenant"
                if not normalized_tenant_id or not normalized_tenant_slug
                else "missing_request_id"
            )
        return None

    ttl_hours = _get_ttl_hours()
    now = _now_utc()
    expires_at = now + timedelta(hours=ttl_hours)
    jti = secrets.token_urlsafe(24)
    grant_id = str(uuid4())
    payload = {
        "tenant_id": normalized_tenant_id,
        "tenant_slug": normalized_tenant_slug,
        "app_code": _APP_CODE,
        "request_id": normalized_request_id,
        "recipient_email": recipient.email,
        "recipient_name": recipient.name,
        "recipient_role": recipient.role,
        "recipient_employee_id": recipient.employee_id,
        "recipient_picture_url": recipient.picture_url,
        "jti": jti,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    token = sign_json_token(payload=payload, secret=secret)
    row = {
        "id": grant_id,
        "tenant_id": normalized_tenant_id,
        "tenant_slug": normalized_tenant_slug,
        "app_id": app_id,
        "request_id": normalized_request_id,
        "token": token,
        "recipient_employee_id": recipient.employee_id,
        "recipient_email": recipient.email,
        "recipient_role": recipient.role,
        "jti": jti,
        "request_snapshot_json": snapshot,
        "issued_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
        "created_by_user_id": created_by_user_id,
        "updated_at": now.isoformat(),
    }
    try:
        supabase_client.insert_row(table=_GRANT_TABLE, row=row)
    except SupabaseClientError as exc:
        if isinstance(debug_trace, dict):
            debug_trace["reason"] = "grant_insert_failed"
            debug_trace["error"] = str(exc)
            debug_trace["app_id"] = app_id
            debug_trace["tenant_id"] = normalized_tenant_id
            debug_trace["tenant_slug"] = normalized_tenant_slug
            debug_trace["request_id"] = normalized_request_id
        return None

    return {
        "grant_id": grant_id,
        "jti": jti,
        "tenant_id": normalized_tenant_id,
        "tenant_slug": normalized_tenant_slug,
        "token": token,
        "url": build_quick_approval_url(token=token, base_url=base_url or ""),
        "recipient_email": recipient.email,
        "recipient_name": recipient.name,
        "recipient_role": recipient.role,
        "recipient_picture_url": recipient.picture_url,
        "expires_at": expires_at.isoformat(),
    }


def _find_active_grant(
    *,
    tenant_id: str,
    tenant_slug: str,
    request_id: str,
    recipient_email: str,
    recipient_role: str,
    recipient_employee_id: str | None = None,
) -> dict[str, object] | None:
    normalized_tenant_id = _normalize_text(tenant_id)
    normalized_tenant_slug = _normalize_text(tenant_slug)
    normalized_request_id = _normalize_text(request_id)
    normalized_email = _normalize_email(recipient_email)
    normalized_role = _normalize_text(recipient_role).lower()
    normalized_employee_id = _normalize_text(recipient_employee_id) or None
    if (
        not normalized_tenant_id
        or not normalized_tenant_slug
        or not normalized_request_id
        or not normalized_email
        or not normalized_role
    ):
        return None

    query: dict[str, str] = {
        "tenant_id": f"eq.{normalized_tenant_id}",
        "tenant_slug": f"eq.{normalized_tenant_slug}",
        "request_id": f"eq.{normalized_request_id}",
        "recipient_email": f"eq.{normalized_email}",
        "recipient_role": f"eq.{normalized_role}",
        "used_at": "is.null",
        "revoked_at": "is.null",
    }
    if normalized_employee_id:
        query["recipient_employee_id"] = f"eq.{normalized_employee_id}"
    else:
        query["recipient_employee_id"] = "is.null"

    try:
        rows = supabase_client.select_many_query(
            table=_GRANT_TABLE,
            query={
                **query,
                "select": "id,tenant_id,tenant_slug,app_id,request_id,token,recipient_employee_id,recipient_email,recipient_role,jti,issued_at,expires_at,used_at,used_action,used_reason,revoked_at,created_by_user_id",
            },
        )
    except SupabaseClientError:
        return None

    if not isinstance(rows, list) or not rows:
        return None

    active_rows: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        state, _ = _grant_state(row)
        if state == "ready":
            active_rows.append(row)

    if not active_rows:
        return None

    active_rows.sort(
        key=lambda row: _parse_iso_datetime(row.get("issued_at")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return active_rows[0]


def issue_quick_approval_deliveries(
    *,
    transaction: dict,
    tenant_id: str,
    tenant_slug: str,
    quick_approval_base_url: str | None = None,
    created_by_user_id: str | None = None,
) -> list[dict[str, object]]:
    normalized_tenant_id, normalized_tenant_slug = _resolve_tenant_scope(
        tenant_id=tenant_id,
        tenant_slug=tenant_slug,
    )
    if not normalized_tenant_id or not normalized_tenant_slug:
        return []

    employee_id = _normalize_text(transaction.get("employeeId"))
    if not employee_id:
        return []

    recipients = _dedupe_recipients(
        _load_manager_recipients(employee_id=employee_id),
        _load_admin_recipients(tenant_id=normalized_tenant_id),
    )
    deliveries: list[dict[str, object]] = []
    for recipient in recipients:
        delivery = resolve_quick_approval_delivery(
            transaction=transaction,
            tenant_id=normalized_tenant_id,
            tenant_slug=normalized_tenant_slug,
            recipient_email=recipient.email,
            recipient_role=recipient.role,
            recipient_employee_id=recipient.employee_id,
            recipient_name=recipient.name,
            recipient_picture_url=recipient.picture_url,
            quick_approval_base_url=quick_approval_base_url,
            created_by_user_id=created_by_user_id,
        )
        if delivery is not None:
            deliveries.append(delivery)
    return deliveries


def resolve_quick_approval_delivery(
    *,
    transaction: dict,
    tenant_id: str,
    tenant_slug: str,
    recipient_email: str,
    recipient_role: str,
    recipient_employee_id: str | None = None,
    recipient_name: str | None = None,
    recipient_picture_url: str | None = None,
    quick_approval_base_url: str | None = None,
    created_by_user_id: str | None = None,
    debug_trace: dict[str, object] | None = None,
) -> dict[str, object] | None:
    normalized_tenant_id, normalized_tenant_slug = _resolve_tenant_scope(
        tenant_id=tenant_id,
        tenant_slug=tenant_slug,
    )
    normalized_request_id = _normalize_text(transaction.get("id"))
    if not normalized_tenant_id or not normalized_tenant_slug or not normalized_request_id:
        if isinstance(debug_trace, dict):
            debug_trace["reason"] = "missing_tenant_or_request"
        return None

    existing = _find_active_grant(
        tenant_id=normalized_tenant_id,
        tenant_slug=normalized_tenant_slug,
        request_id=normalized_request_id,
        recipient_email=recipient_email,
        recipient_role=recipient_role,
        recipient_employee_id=recipient_employee_id,
    )
    if existing is not None:
        delivery = _grant_delivery_from_row(existing, base_url=quick_approval_base_url)
        if delivery is not None:
            delivery["recipient_name"] = recipient_name or delivery.get("recipient_name")
            delivery["recipient_picture_url"] = recipient_picture_url or delivery.get("recipient_picture_url")
            delivery["recipient_email"] = _normalize_email(recipient_email)
            delivery["recipient_role"] = _normalize_text(recipient_role).lower() or None
            delivery["tenant_id"] = normalized_tenant_id
            delivery["tenant_slug"] = normalized_tenant_slug
        elif isinstance(debug_trace, dict):
            debug_trace["reason"] = "existing_grant_unusable"
        return delivery

    recipient = QuickApprovalRecipient(
        role=_normalize_text(recipient_role).lower() or _ROLE_MANAGER,
        name=recipient_name or _normalize_email(recipient_email) or "Recipient",
        email=_normalize_email(recipient_email),
        employee_id=_normalize_text(recipient_employee_id) or None,
        picture_url=_normalize_text(recipient_picture_url) or None,
    )
    return _create_grant(
        tenant_id=normalized_tenant_id,
        tenant_slug=normalized_tenant_slug,
        request_id=normalized_request_id,
        snapshot=_build_request_snapshot(tenant_id=normalized_tenant_id, transaction=transaction),
        recipient=recipient,
        base_url=quick_approval_base_url,
        created_by_user_id=created_by_user_id,
        debug_trace=debug_trace,
    )


def _get_grant_by_token(*, token: str, debug_trace: dict[str, object] | None = None) -> dict[str, object] | None:
    bootstrap_token = None
    try:
        bootstrap_claims = decode_json_token_payload_unverified(token=token)
    except ValueError as exc:
        if isinstance(debug_trace, dict):
            debug_trace["reason"] = "token_bootstrap_failed"
            debug_trace["error"] = str(exc)
        return None

    bootstrap_tenant_slug = _normalize_text(bootstrap_claims.get("tenant_slug"))
    if bootstrap_tenant_slug:
        try:
            bootstrap_token = set_tenant_context(bootstrap_tenant_slug)
        except TenantConfigError as exc:
            if isinstance(debug_trace, dict):
                debug_trace["reason"] = "tenant_bootstrap_failed"
                debug_trace["tenant_slug"] = bootstrap_tenant_slug
                debug_trace["error"] = str(exc)

    try:
        secret = _get_secret()
        if not secret:
            if isinstance(debug_trace, dict):
                debug_trace["reason"] = "missing_secret"
            return None
        try:
            claims = verify_json_token(token=token, secret=secret)
        except ValueError as exc:
            if isinstance(debug_trace, dict):
                debug_trace["reason"] = "token_verification_failed"
                debug_trace["error"] = str(exc)
            return None

        jti = _normalize_text(claims.get("jti"))
        tenant_id = _normalize_text(claims.get("tenant_id"))
        tenant_slug = _normalize_text(claims.get("tenant_slug"))
        if not jti or not tenant_id or not tenant_slug:
            if isinstance(debug_trace, dict):
                debug_trace["reason"] = "missing_token_claims"
                debug_trace["claims"] = sorted(key for key in claims.keys())
            return None

        row = supabase_client.select_single_query(
            table=_GRANT_TABLE,
            query={
                "jti": f"eq.{jti}",
                "tenant_id": f"eq.{tenant_id}",
                "tenant_slug": f"eq.{tenant_slug}",
            },
            select="id,tenant_id,tenant_slug,app_id,request_id,recipient_employee_id,recipient_email,recipient_role,jti,request_snapshot_json,issued_at,expires_at,used_at,used_action,used_reason,revoked_at,created_by_user_id",
        )
        if not row:
            if isinstance(debug_trace, dict):
                debug_trace["reason"] = "grant_not_found"
                debug_trace["jti"] = jti
                debug_trace["tenant_id"] = tenant_id
                debug_trace["tenant_slug"] = tenant_slug
            return None
        if _normalize_text(row.get("jti")) != jti:
            if isinstance(debug_trace, dict):
                debug_trace["reason"] = "grant_jti_mismatch"
            return None
        if _normalize_text(row.get("tenant_slug")) != tenant_slug:
            if isinstance(debug_trace, dict):
                debug_trace["reason"] = "grant_tenant_slug_mismatch"
            return None
        return {
            "claims": claims,
            "grant": row,
        }
    finally:
        if bootstrap_token is not None:
            reset_tenant_context(bootstrap_token)


def _grant_state(row: dict[str, object]) -> tuple[str, str | None]:
    revoked_at = _normalize_text(row.get("revoked_at"))
    used_at = _normalize_text(row.get("used_at"))
    used_action = _normalize_text(row.get("used_action")).lower() or None
    expires_at = _normalize_text(row.get("expires_at"))
    if revoked_at:
        return "invalid", None
    if expires_at:
        try:
            expires = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if expires <= _now_utc():
                return "expired", None
        except ValueError:
            return "invalid", None
    if used_at:
        if used_action == "approved":
            return "already_handled", "approved"
        if used_action == "rejected":
            return "already_handled", "rejected"
        return "already_handled", None
    return "ready", None


def _current_transaction(*, tenant_id: str | None, transaction_id: str) -> dict | None:
    normalized_tenant_id = _normalize_text(tenant_id)
    normalized_transaction_id = _normalize_text(transaction_id)
    if normalized_tenant_id:
        cached_transaction = _read_quick_approval_transaction_cache(
            tenant_id=normalized_tenant_id,
            request_id=normalized_transaction_id,
        )
        if cached_transaction is not None:
            return cached_transaction
    rows = get_pto_transactions(transaction_id=transaction_id)
    if not rows:
        return None
    row = rows[0]
    if not isinstance(row, dict):
        return None
    if normalized_tenant_id:
        _write_quick_approval_transaction_cache(
            tenant_id=normalized_tenant_id,
            request_id=normalized_transaction_id,
            value=row,
        )
    return row


def _hydrate_recipient_contact(
    *,
    tenant_id: str,
    recipient_role: str,
    recipient_employee_id: str | None,
    recipient_name: str | None,
    recipient_picture_url: str | None,
) -> tuple[str | None, str | None]:
    if recipient_role != _ROLE_MANAGER:
        return recipient_name, recipient_picture_url
    normalized_employee_id = _normalize_text(recipient_employee_id)
    if not normalized_employee_id:
        return recipient_name, recipient_picture_url
    if recipient_name and recipient_picture_url:
        return recipient_name, recipient_picture_url
    contact = _load_employee_contact(tenant_id=tenant_id, employee_id=normalized_employee_id)
    if not contact:
        return recipient_name, recipient_picture_url
    return (
        recipient_name or _normalize_text(contact.get("employeeName")) or None,
        recipient_picture_url or _normalize_text(contact.get("pictureUrl")) or None,
    )


def _to_preview(*, tenant_id: str | None, transaction: dict, hydrate_contact: bool = False) -> dict[str, object]:
    snapshot = transaction.get("request_snapshot_json")
    if isinstance(snapshot, dict) and snapshot:
        preview = dict(snapshot)
        preview["startDate"] = _normalize_preview_date(preview.get("startDate"))
        preview["endDate"] = _normalize_preview_date(preview.get("endDate"))
    else:
        employee_id = _normalize_text(transaction.get("employeeId"))
        employee_name = employee_id or "Employee"
        picture_url = None
        if employee_id:
            contact = None
            if hydrate_contact and tenant_id:
                contact = _load_employee_contact(tenant_id=tenant_id, employee_id=employee_id)
            if contact:
                employee_name = _normalize_text(contact.get("employeeName")) or employee_name
                picture_url = _normalize_text(contact.get("pictureUrl")) or None
            else:
                employees = get_employees_by_ids(employee_ids=[employee_id])
                if employees:
                    employee = employees[0]
                    first_name = _normalize_text(employee.get("firstName"))
                    last_name = _normalize_text(employee.get("lastName"))
                    employee_name = " ".join(part for part in (first_name, last_name) if part).strip() or _normalize_text(employee.get("email")) or employee_name
                    picture_url = _normalize_text(employee.get("pictureUrl")) or None

        pto_type_code = _normalize_text(transaction.get("ptoTypeCode")).upper() or None
        pto_type_label = pto_type_code or "PTO"
        if pto_type_code:
            pto_rows = get_pto_types(code=pto_type_code)
            if pto_rows:
                pto_type_label = _normalize_text(pto_rows[0].get("name")) or pto_type_code

        preview = {
            "requestId": _normalize_text(transaction.get("id")) or None,
            "employeeId": employee_id or None,
            "employeeName": employee_name,
            "ptoTypeCode": pto_type_code,
            "ptoTypeLabel": pto_type_label,
            "startDate": _normalize_preview_date(transaction.get("startDate")),
            "endDate": _normalize_preview_date(transaction.get("endDate")),
            "hoursRequested": _coerce_number(transaction.get("hours")),
            "reason": _normalize_text(transaction.get("description")) or None,
            "currentStatus": _normalize_text(transaction.get("status")).lower() or "pending",
            "pictureUrl": picture_url,
        }

    if hydrate_contact and tenant_id:
        employee_id = _normalize_text(preview.get("employeeId"))
        if employee_id:
            contact = _load_employee_contact(tenant_id=tenant_id, employee_id=employee_id)
            if contact:
                preview["employeeName"] = _normalize_text(preview.get("employeeName")) or _normalize_text(contact.get("employeeName"))
                preview["pictureUrl"] = _normalize_text(preview.get("pictureUrl")) or _normalize_text(contact.get("pictureUrl")) or None

    preview["pictureUrl"] = _normalize_text(preview.get("pictureUrl")) or None
    return preview


def load_quick_approval_context(*, token: str, debug: bool = False, refresh: bool = False) -> dict[str, object]:
    debug_trace: dict[str, object] | None = {} if debug else None
    grant_payload = _get_grant_by_token(token=token, debug_trace=debug_trace)
    if grant_payload is None:
        response: dict[str, object] = {"state": "invalid", "preview": None, "handledDecision": None, "handledNote": None, "message": "This quick approval link is invalid."}
        if debug_trace is not None:
            response["debugTrace"] = debug_trace
        return response

    claims = grant_payload["claims"]
    grant = grant_payload["grant"]
    tenant_id = _normalize_text(grant.get("tenant_id"))
    tenant_slug = _normalize_text(grant.get("tenant_slug")) or _normalize_text(claims.get("tenant_slug"))
    request_id = _normalize_text(grant.get("request_id"))
    if not tenant_id or not tenant_slug or not request_id:
        response: dict[str, object] = {"state": "invalid", "preview": None, "handledDecision": None, "handledNote": None, "message": "This quick approval link is invalid."}
        if debug_trace is not None:
            debug_trace["reason"] = "missing_grant_fields"
            response["debugTrace"] = debug_trace
        return response

    tenant_token = set_tenant_context(tenant_slug)
    try:
        state, handled_decision = _grant_state(grant)
        recipient_role = _normalize_text(grant.get("recipient_role")).lower()
        recipient_email = _normalize_text(grant.get("recipient_email")).lower() or None
        recipient_name = _normalize_text(grant.get("recipient_name")) or _normalize_text(claims.get("recipient_name")) or None
        recipient_picture_url = _normalize_text(grant.get("recipient_picture_url")) or _normalize_text(claims.get("recipient_picture_url")) or None
        recipient_employee_id = _normalize_text(grant.get("recipient_employee_id")) or None
        grant_jti = _normalize_text(grant.get("jti"))
        handled_at = _normalize_text(grant.get("used_at")) or None
        if debug_trace is not None:
            debug_trace["grant_state"] = state
            debug_trace["request_id"] = request_id
            debug_trace["tenant_id"] = tenant_id
            debug_trace["tenant_slug"] = tenant_slug
        if refresh and state == "ready" and grant_jti and recipient_email:
            cached_refresh = _read_quick_approval_refresh_cache(
                tenant_id=tenant_id,
                request_id=request_id,
                recipient_role=recipient_role,
                recipient_email=recipient_email,
                recipient_employee_id=recipient_employee_id,
                grant_jti=grant_jti,
            )
            if cached_refresh is not None:
                cached_response = deepcopy(cached_refresh)
                cached_response["recipientRole"] = cached_response.get("recipientRole") or recipient_role or None
                cached_response["recipientEmail"] = cached_response.get("recipientEmail") or recipient_email
                cached_response["recipientName"] = cached_response.get("recipientName") or recipient_name
                cached_response["recipientPictureUrl"] = cached_response.get("recipientPictureUrl") or recipient_picture_url
                cached_response["recipientName"], cached_response["recipientPictureUrl"] = _hydrate_recipient_contact(
                    tenant_id=tenant_id,
                    recipient_role=recipient_role,
                    recipient_employee_id=recipient_employee_id,
                    recipient_name=_normalize_text(cached_response.get("recipientName")) or recipient_name,
                    recipient_picture_url=_normalize_text(cached_response.get("recipientPictureUrl")) or recipient_picture_url,
                )
                if debug_trace is not None:
                    debug_trace["reason"] = "refresh_cache_hit"
                    debug_trace["cache_hit"] = True
                    debug_trace["preview_source"] = "refresh_cache"
                    cached_response["debugTrace"] = debug_trace
                return cached_response
        if refresh and state == "already_handled" and grant_jti and recipient_email:
            cached_refresh = _read_quick_approval_refresh_cache(
                tenant_id=tenant_id,
                request_id=request_id,
                recipient_role=recipient_role,
                recipient_email=recipient_email,
                recipient_employee_id=recipient_employee_id,
                grant_jti=grant_jti,
            )
            if cached_refresh is not None:
                cached_response = deepcopy(cached_refresh)
                cached_response["recipientRole"] = cached_response.get("recipientRole") or recipient_role or None
                cached_response["recipientEmail"] = cached_response.get("recipientEmail") or recipient_email
                cached_response["recipientName"] = cached_response.get("recipientName") or recipient_name
                cached_response["recipientPictureUrl"] = cached_response.get("recipientPictureUrl") or recipient_picture_url
                cached_response["recipientName"], cached_response["recipientPictureUrl"] = _hydrate_recipient_contact(
                    tenant_id=tenant_id,
                    recipient_role=recipient_role,
                    recipient_employee_id=recipient_employee_id,
                    recipient_name=_normalize_text(cached_response.get("recipientName")) or recipient_name,
                    recipient_picture_url=_normalize_text(cached_response.get("recipientPictureUrl")) or recipient_picture_url,
                )
                if debug_trace is not None:
                    debug_trace["reason"] = "refresh_cache_hit"
                    debug_trace["cache_hit"] = True
                    debug_trace["preview_source"] = "refresh_cache"
                    cached_response["debugTrace"] = debug_trace
                return cached_response
        preview = _to_preview(tenant_id=tenant_id, transaction=grant, hydrate_contact=False)
        preview["currentStatus"] = "pending" if state == "ready" else preview.get("currentStatus") or "pending"
        if not refresh and state != "ready":
            handled_note = _normalize_text(grant.get("used_reason")) or None
            if state == "already_handled" and not handled_note:
                transaction = _current_transaction(tenant_id=tenant_id, transaction_id=request_id)
                if transaction is not None:
                    handled_note = _normalize_text(transaction.get("approverNote")) or None
                    handled_at = handled_at or _normalize_text(transaction.get("dateUpdated")) or None
                    if debug_trace is not None:
                        debug_trace["handled_note_source"] = "live_request"
            elif state == "already_handled" and debug_trace is not None:
                debug_trace["handled_note_source"] = "grant_used_reason"
            if state == "already_handled" and handled_decision and isinstance(preview, dict):
                preview["currentStatus"] = handled_decision
            response = {
                "state": state,
                "preview": None if state == "invalid" else preview,
                "handledDecision": handled_decision,
                "handledNote": handled_note,
                "handledAt": handled_at,
                "message": (
                    "This quick approval link has expired."
                    if state == "expired"
                    else "This request has already been handled."
                    if state == "already_handled"
                    else "This quick approval link is invalid."
                ),
                "recipientRole": _normalize_text(grant.get("recipient_role")) or None,
                "recipientEmail": recipient_email,
                "recipientName": recipient_name,
                "recipientPictureUrl": recipient_picture_url,
            }
            if debug_trace is not None:
                debug_trace["reason"] = state
                debug_trace["preview_source"] = "grant_snapshot"
                response["debugTrace"] = debug_trace
            if state == "already_handled" and grant_jti and recipient_email:
                _write_quick_approval_refresh_cache(
                    tenant_id=tenant_id,
                    request_id=request_id,
                    recipient_role=recipient_role,
                    recipient_email=recipient_email,
                    recipient_employee_id=_normalize_text(grant.get("recipient_employee_id")) or None,
                    grant_jti=grant_jti,
                    value=response,
                )
            return response

        if not refresh:
            response = {
                "state": "ready",
                "preview": preview,
                "handledDecision": None,
                "handledNote": None,
                "handledAt": None,
                "message": None,
                "recipientRole": recipient_role or None,
                "recipientEmail": recipient_email,
                "recipientName": recipient_name,
                "recipientPictureUrl": recipient_picture_url,
                "canAct": True,
            }
            if debug_trace is not None:
                debug_trace["reason"] = "snapshot_ready"
                debug_trace["preview_source"] = "grant_snapshot"
                response["debugTrace"] = debug_trace
            return response

        transaction = _current_transaction(tenant_id=tenant_id, transaction_id=request_id)
        if transaction is None:
            preview["currentStatus"] = _normalize_text(preview.get("currentStatus")).lower() or "pending"
            response = {
                "state": "ready",
                "preview": preview,
                "handledDecision": None,
                "handledNote": None,
                "handledAt": None,
                "message": "This quick approval link is valid, but the live request record could not be loaded.",
                "recipientRole": recipient_role or None,
                "recipientEmail": recipient_email,
                "recipientName": recipient_name,
                "recipientPictureUrl": recipient_picture_url,
                "canAct": False,
            }
            if debug_trace is not None:
                debug_trace["reason"] = "transaction_not_found"
                response["debugTrace"] = debug_trace
            return response

        current_status = _normalize_text(transaction.get("status")).lower() or "pending"
        if debug_trace is not None:
            debug_trace["transaction_found"] = True
            debug_trace["transaction_status"] = current_status
        recipient_name, recipient_picture_url = _hydrate_recipient_contact(
            tenant_id=tenant_id,
            recipient_role=recipient_role,
            recipient_employee_id=recipient_employee_id,
            recipient_name=recipient_name,
            recipient_picture_url=recipient_picture_url,
        )
        if current_status != "pending":
            preview = _to_preview(tenant_id=tenant_id, transaction=transaction, hydrate_contact=True)
            preview["currentStatus"] = current_status
            handled = "approved" if current_status == "approved" else "rejected" if current_status == "rejected" else None
            response = {
                "state": "already_handled",
                "preview": preview,
                "handledDecision": handled,
                "handledNote": _normalize_text(transaction.get("approverNote")) or None,
                "handledAt": _normalize_text(transaction.get("dateUpdated")) or handled_at,
                "message": "This request has already been handled.",
                "recipientRole": recipient_role or None,
                "recipientEmail": recipient_email,
                "recipientName": recipient_name,
                "recipientPictureUrl": recipient_picture_url,
            }
            if debug_trace is not None:
                debug_trace["reason"] = "transaction_not_pending"
                response["debugTrace"] = debug_trace
            if grant_jti and recipient_email:
                _write_quick_approval_refresh_cache(
                    tenant_id=tenant_id,
                    request_id=request_id,
                    recipient_role=recipient_role,
                    recipient_email=recipient_email,
                    recipient_employee_id=recipient_employee_id,
                    grant_jti=grant_jti,
                    value=response,
                )
            return response

        if recipient_role == _ROLE_MANAGER:
            recipient_employee_id = _normalize_text(grant.get("recipient_employee_id"))
            employee_id = _normalize_text(transaction.get("employeeId"))
            preview = _to_preview(tenant_id=tenant_id, transaction=transaction, hydrate_contact=True)
            preview["currentStatus"] = current_status
            if not recipient_employee_id or not employee_id:
                response = {
                    "state": "ready",
                    "preview": preview,
                    "handledDecision": None,
                    "handledNote": None,
                    "handledAt": None,
                    "message": "This quick approval link is valid for preview, but the manager assignment could not be verified.",
                    "recipientRole": recipient_role,
                    "recipientEmail": recipient_email,
                    "recipientName": recipient_name,
                    "recipientPictureUrl": recipient_picture_url,
                    "canAct": False,
                }
                if debug_trace is not None:
                    debug_trace["reason"] = "missing_manager_assignment"
                    debug_trace["recipient_employee_id"] = recipient_employee_id or None
                    debug_trace["employee_id"] = employee_id or None
                    response["debugTrace"] = debug_trace
                return response
            active_manager_rows = get_employee_managers(employee_id=employee_id, manager_id=recipient_employee_id)
            if not active_manager_rows:
                response = {
                    "state": "ready",
                    "preview": preview,
                    "handledDecision": None,
                    "handledNote": None,
                    "handledAt": None,
                    "message": "This quick approval link is valid for preview, but this manager is no longer assigned to the request.",
                    "recipientRole": recipient_role,
                    "recipientEmail": recipient_email,
                    "recipientName": recipient_name,
                    "recipientPictureUrl": recipient_picture_url,
                    "canAct": False,
                }
                if debug_trace is not None:
                    debug_trace["reason"] = "manager_not_assigned"
                    debug_trace["recipient_employee_id"] = recipient_employee_id or None
                    debug_trace["employee_id"] = employee_id or None
                    response["debugTrace"] = debug_trace
                return response

        preview = _to_preview(tenant_id=tenant_id, transaction=transaction, hydrate_contact=True)
        preview["currentStatus"] = current_status
        response = {
            "state": "ready",
            "preview": preview,
            "handledDecision": None,
            "handledNote": None,
            "handledAt": None,
            "message": None,
            "recipientRole": recipient_role or None,
            "recipientEmail": recipient_email,
            "recipientName": recipient_name,
            "recipientPictureUrl": recipient_picture_url,
            "canAct": True,
        }
        if debug_trace is not None:
            debug_trace["reason"] = "ready"
            debug_trace["preview_source"] = "live_request"
            debug_trace["transaction_found"] = True
            debug_trace["transaction_status"] = current_status
            response["debugTrace"] = debug_trace
        if refresh and state == "ready" and grant_jti and recipient_email:
            _write_quick_approval_refresh_cache(
                tenant_id=tenant_id,
                request_id=request_id,
                recipient_role=recipient_role,
                recipient_email=recipient_email,
                recipient_employee_id=recipient_employee_id,
                grant_jti=grant_jti,
                value=response,
            )
        return response
    finally:
        reset_tenant_context(tenant_token)


def _consume_grant(*, grant_id: str, jti: str, tenant_id: str, request_id: str, action: str, reason: str | None) -> bool:
    now_iso = _now_utc().isoformat()
    updated = supabase_client.patch_rows_query(
        table=_GRANT_TABLE,
        query={
            "id": f"eq.{grant_id}",
            "jti": f"eq.{jti}",
            "tenant_id": f"eq.{tenant_id}",
            "used_at": "is.null",
            "revoked_at": "is.null",
        },
        patch={
            "used_at": now_iso,
            "used_action": action,
            "used_reason": reason,
            "updated_at": now_iso,
        },
    )
    if updated:
        _clear_quick_approval_refresh_cache(tenant_id=tenant_id, request_id=request_id)
    return bool(updated)


def _restore_grant(*, grant_id: str, jti: str, tenant_id: str, request_id: str) -> None:
    now_iso = _now_utc().isoformat()
    try:
        supabase_client.patch_rows_query(
            table=_GRANT_TABLE,
            query={
                "id": f"eq.{grant_id}",
                "jti": f"eq.{jti}",
                "tenant_id": f"eq.{tenant_id}",
            },
            patch={
                "used_at": None,
                "used_action": None,
                "used_reason": None,
                "updated_at": now_iso,
            },
        )
        _clear_quick_approval_refresh_cache(tenant_id=tenant_id, request_id=request_id)
    except SupabaseClientError:
        return


def revoke_quick_approval_grant(*, grant_id: str, tenant_id: str) -> None:
    now_iso = _now_utc().isoformat()
    try:
        row = supabase_client.select_single_query(
            table=_GRANT_TABLE,
            query={
                "id": f"eq.{grant_id}",
                "tenant_id": f"eq.{tenant_id}",
            },
            select="id,request_id",
        )
        supabase_client.patch_rows_query(
            table=_GRANT_TABLE,
            query={
                "id": f"eq.{grant_id}",
                "tenant_id": f"eq.{tenant_id}",
                "revoked_at": "is.null",
            },
            patch={
                "revoked_at": now_iso,
                "updated_at": now_iso,
            },
        )
        request_id = _normalize_text(row.get("request_id")) if isinstance(row, dict) else ""
        if request_id:
            _clear_quick_approval_refresh_cache(tenant_id=tenant_id, request_id=request_id)
    except SupabaseClientError:
        return


def submit_quick_approval_decision(*, token: str, decision: Literal["approved", "rejected"], reason: str | None = None) -> dict[str, object]:
    grant_payload = _get_grant_by_token(token=token)
    if grant_payload is None:
        return {"state": "invalid", "decision": None, "message": "This quick approval link is invalid."}

    grant = grant_payload["grant"]
    claims = grant_payload["claims"]
    tenant_id = _normalize_text(grant.get("tenant_id"))
    tenant_slug = _normalize_text(grant.get("tenant_slug")) or _normalize_text(claims.get("tenant_slug"))
    request_id = _normalize_text(grant.get("request_id"))
    grant_id = _normalize_text(grant.get("id"))
    jti = _normalize_text(grant.get("jti"))
    if not tenant_id or not tenant_slug or not request_id or not grant_id or not jti:
        return {"state": "invalid", "decision": None, "message": "This quick approval link is invalid."}

    tenant_token = set_tenant_context(tenant_slug)
    try:
        state, handled_decision = _grant_state(grant)
        if state == "expired":
            return {"state": "expired", "decision": None, "message": "This quick approval link has expired."}
        if state == "invalid":
            return {"state": "invalid", "decision": None, "message": "This quick approval link is invalid."}
        if state == "already_handled":
            return {"state": "already_handled", "decision": handled_decision, "message": "This request has already been handled."}

        transaction = _current_transaction(tenant_id=tenant_id, transaction_id=request_id)
        if transaction is None:
            return {"state": "invalid", "decision": None, "message": "This quick approval link is invalid."}
        if _normalize_text(transaction.get("status")).lower() != "pending":
            handled = "approved" if _normalize_text(transaction.get("status")).lower() == "approved" else "rejected" if _normalize_text(transaction.get("status")).lower() == "rejected" else None
            return {"state": "already_handled", "decision": handled, "message": "This request has already been handled."}

        recipient_role = _normalize_text(grant.get("recipient_role")).lower()
        if recipient_role == _ROLE_MANAGER:
            recipient_employee_id = _normalize_text(grant.get("recipient_employee_id"))
            employee_id = _normalize_text(transaction.get("employeeId"))
            if not recipient_employee_id or not employee_id:
                return {"state": "invalid", "decision": None, "message": "This quick approval link is invalid."}
            active_manager_rows = get_employee_managers(employee_id=employee_id, manager_id=recipient_employee_id)
            if not active_manager_rows:
                return {"state": "invalid", "decision": None, "message": "This quick approval link is invalid."}

        normalized_reason = _normalize_text(reason) or None
        if not _consume_grant(grant_id=grant_id, jti=jti, tenant_id=tenant_id, request_id=request_id, action=decision, reason=normalized_reason):
            return {"state": "already_handled", "decision": None, "message": "This request has already been handled."}

        try:
            approver_id = _normalize_text(grant.get("recipient_employee_id")) or None
            finalize_pending_pto_action(
                transaction_id=request_id,
                action="approved" if decision == "approved" else "rejected",
                approver_id=approver_id,
                approver_note=normalized_reason,
                transaction=transaction,
                send_status_email=True,
                note_label_override="Approver note / reason",
            )
        except ValueError:
            _restore_grant(grant_id=grant_id, jti=jti, tenant_id=tenant_id, request_id=request_id)
            raise

        return {"state": "success", "decision": decision, "message": None}
    finally:
        reset_tenant_context(tenant_token)
