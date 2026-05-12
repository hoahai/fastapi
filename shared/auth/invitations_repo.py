from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from shared.auth.config import get_invite_ttl_hours
from shared.auth.profile_repo import upsert_profile_basic_info
from shared.auth.providers import get_auth_provider

VALID_TRADSPHERE_ROLES = (
    "tradsphere.viewer",
    "tradsphere.editor",
    "tradsphere.admin",
)


@dataclass(frozen=True)
class InvitationRecord:
    id: str
    token: str
    email: str
    tenant_id: str
    app_id: str
    role: str
    status: str
    expires_at: str
    accepted_at: str | None
    created_at: str | None


def _provider():
    return get_auth_provider()


def get_active_app(*, app_code: str) -> dict[str, str] | None:
    row = _provider().select_single(
        table="apps",
        filters={"code": app_code, "active": "true"},
        select="id,code",
    )
    if not row:
        return None
    app_id = str(row.get("id") or "").strip()
    code = str(row.get("code") or "").strip().lower()
    if not app_id or code != app_code:
        return None
    return {"id": app_id, "code": code}


def list_active_apps() -> list[dict[str, object]]:
    rows = _provider().select_many(
        table="apps",
        filters={"active": "true"},
        select="id,code,name,active,created_at",
    )
    items: list[dict[str, object]] = []
    for row in rows:
        app_id = str(row.get("id") or "").strip()
        app_code = str(row.get("code") or "").strip().lower()
        if not app_id or not app_code:
            continue
        items.append(
            {
                "id": app_id,
                "code": app_code,
                "name": row.get("name"),
                "active": bool(row.get("active", True)),
                "createdAt": row.get("created_at"),
            }
        )
    items.sort(
        key=lambda item: (
            0 if str(item.get("code") or "").strip().lower() == "tradsphere" else 1,
            str(item.get("code") or "").strip().lower(),
        )
    )
    return items


def create_invitation(
    *,
    email: str,
    tenant_id: str,
    app_id: str,
    role: str,
    invited_by_user_id: str,
    expires_at: datetime | None = None,
) -> dict[str, object]:
    token = secrets.token_urlsafe(32)
    resolved_expires_at = expires_at or (datetime.now(timezone.utc) + timedelta(hours=get_invite_ttl_hours()))

    row = {
        "token": token,
        "email": email,
        "tenant_id": tenant_id,
        "app_id": app_id,
        "role": role,
        "status": "pending",
        "invited_by_user_id": invited_by_user_id,
        "expires_at": resolved_expires_at.isoformat(),
    }
    created = _provider().insert_row(table="invitations", row=row)
    return {
        "id": created.get("id"),
        "token": token,
        "email": email,
        "role": role,
        "status": created.get("status") or "pending",
        "expires_at": created.get("expires_at") or resolved_expires_at.isoformat(),
    }


def get_invitation_by_token(token: str) -> dict[str, object] | None:
    row = _provider().select_single(
        table="invitations",
        filters={"token": token},
        select="id,token,email,tenant_id,app_id,role,status,expires_at,accepted_at,accepted_by_user_id,created_at",
    )
    if not row:
        return None
    return row


def get_invitation_by_id(*, invitation_id: str, tenant_id: str | None = None) -> dict[str, object] | None:
    filters: dict[str, str] = {"id": invitation_id}
    if tenant_id:
        filters["tenant_id"] = tenant_id
    row = _provider().select_single(
        table="invitations",
        filters=filters,
        select="id,token,email,tenant_id,app_id,role,status,expires_at,accepted_at,accepted_by_user_id,created_at",
    )
    if not row:
        return None
    return row


def list_invitations(*, tenant_id: str, app_id: str, status: str | None = None) -> list[dict[str, object]]:
    filters = {"tenant_id": tenant_id, "app_id": app_id}
    if status:
        filters["status"] = status
    return _provider().select_many(
        table="invitations",
        filters=filters,
        select="id,token,email,tenant_id,app_id,role,status,expires_at,accepted_at,accepted_by_user_id,created_at",
    )


def list_invitations_scoped(
    *,
    tenant_ids: set[str] | None = None,
    app_ids: set[str] | None = None,
    status: str | None = None,
) -> list[dict[str, object]]:
    rows = _provider().select_many(
        table="invitations",
        filters={},
        select="id,token,email,tenant_id,app_id,role,status,expires_at,accepted_at,accepted_by_user_id,created_at",
    )
    results = rows
    if tenant_ids:
        results = [row for row in results if str(row.get("tenant_id") or "").strip() in tenant_ids]
    if app_ids:
        results = [row for row in results if str(row.get("app_id") or "").strip() in app_ids]
    if status:
        normalized_status = str(status or "").strip().lower()
        results = [row for row in results if str(row.get("status") or "").strip().lower() == normalized_status]
    return results


def patch_invitation_status(*, invitation_id: str, status: str, extra_patch: dict[str, object] | None = None) -> None:
    patch: dict[str, object] = {"status": status}
    if extra_patch:
        patch.update(extra_patch)
    _provider().patch_rows(
        table="invitations",
        filters={"id": invitation_id},
        patch=patch,
    )


def activate_tenant_user(*, tenant_id: str, user_id: str) -> None:
    provider = _provider()
    existing = provider.select_single(
        table="tenant_users",
        filters={"tenant_id": tenant_id, "user_id": user_id},
        select="id,status",
    )
    if existing:
        provider.patch_rows(
            table="tenant_users",
            filters={"id": str(existing.get("id") or "")},
            patch={"status": "active", "updated_at": provider.now_iso()},
        )
        return

    provider.insert_row(
        table="tenant_users",
        row={
            "tenant_id": tenant_id,
            "user_id": user_id,
            "status": "active",
        },
    )


def upsert_tenant_app_role(*, tenant_id: str, user_id: str, app_id: str, role: str) -> None:
    provider = _provider()
    existing = provider.select_single(
        table="tenant_app_roles",
        filters={"tenant_id": tenant_id, "user_id": user_id, "app_id": app_id},
        select="id,role",
    )
    if existing:
        provider.patch_rows(
            table="tenant_app_roles",
            filters={"id": str(existing.get("id") or "")},
            patch={"role": role, "updated_at": provider.now_iso()},
        )
        return

    provider.insert_row(
        table="tenant_app_roles",
        row={
            "tenant_id": tenant_id,
            "user_id": user_id,
            "app_id": app_id,
            "role": role,
        },
    )


def mark_invitation_accepted(*, invitation_id: str, accepted_by_user_id: str) -> None:
    provider = _provider()
    provider.patch_rows(
        table="invitations",
        filters={"id": invitation_id},
        patch={
            "status": "accepted",
            "accepted_at": provider.now_iso(),
            "accepted_by_user_id": accepted_by_user_id,
        },
    )


def upsert_profile_for_invited_user(
    *,
    user_id: str,
    email: str | None,
    full_name: str | None,
) -> None:
    upsert_profile_basic_info(
        provider=_provider(),
        user_id=user_id,
        email=email,
        full_name=full_name,
    )
