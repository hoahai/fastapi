from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Body, HTTPException, Request

from shared.auth.config import get_invite_base_url, get_invite_ttl_hours
from shared.auth.dependencies import authenticate_bearer, authorize_bearer_for_tenant_app
from shared.auth.permissions_cache import permission_cache
from shared.auth.supabase_client import SupabaseClientError, supabase_client

router = APIRouter(prefix="/invitations")


def _require_admin(request: Request) -> tuple[str, str]:
    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    if "tradsphere.admin" not in result.access.permissions:
        raise HTTPException(status_code=403, detail="Forbidden")
    return result.principal.user_id, result.access.tenant_id


def _invite_url(token: str) -> str:
    base = get_invite_base_url().rstrip("/")
    if not base:
        return f"/auth/invite/{token}"
    return f"{base}/auth/invite/{token}"


@router.post("")
def create_invitation_route(
    request: Request,
    payload: dict = Body(...),
):
    """
    Create invitation for tenant app role.

    Example request:
        POST /api/auth/v1/invitations

    Requirements:
        - Requires bearer JWT
        - Requires X-Tenant-Id
        - Requires tradsphere.admin role
    """
    invited_by_user_id, tenant_id = _require_admin(request)

    email = str(payload.get("email") or "").strip().lower()
    app_code = str(payload.get("appCode") or "tradsphere").strip().lower()
    role = str(payload.get("role") or "").strip()

    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    if app_code != "tradsphere":
        raise HTTPException(status_code=400, detail="Only tradsphere app is supported in Phase 1")
    if role not in {"tradsphere.viewer", "tradsphere.editor", "tradsphere.admin"}:
        raise HTTPException(status_code=400, detail="Invalid role")

    app_row = supabase_client.select_single(
        table="apps",
        filters={"code": app_code, "active": "true"},
        select="id,code",
    )
    if not app_row:
        raise HTTPException(status_code=400, detail="Requested app is not enabled")

    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=get_invite_ttl_hours())

    row = {
        "token": token,
        "email": email,
        "tenant_id": tenant_id,
        "app_id": str(app_row.get("id") or "").strip(),
        "role": role,
        "status": "pending",
        "invited_by_user_id": invited_by_user_id,
        "expires_at": expires_at.isoformat(),
    }

    try:
        created = supabase_client.insert_row(table="invitations", row=row)
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {
        "id": created.get("id"),
        "email": email,
        "role": role,
        "status": created.get("status") or "pending",
        "expiresAt": created.get("expires_at") or expires_at.isoformat(),
        "inviteUrl": _invite_url(token),
    }


@router.get("/{token}")
def get_invitation_route(token: str):
    """
    Resolve invitation token metadata.

    Example request:
        GET /api/auth/v1/invitations/<token>

    Requirements:
        - Public endpoint
        - Token must exist
        - No sensitive values returned
    """
    row = supabase_client.select_single(
        table="invitations",
        filters={"token": token},
        select="id,email,tenant_id,app_id,role,status,expires_at,accepted_at,created_at",
    )
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    return {
        "id": row.get("id"),
        "email": row.get("email"),
        "tenantId": row.get("tenant_id"),
        "appId": row.get("app_id"),
        "role": row.get("role"),
        "status": row.get("status"),
        "expiresAt": row.get("expires_at"),
        "acceptedAt": row.get("accepted_at"),
        "createdAt": row.get("created_at"),
    }


@router.post("/{token}/accept")
def accept_invitation_route(request: Request, token: str):
    """
    Accept invitation for authenticated user.

    Example request:
        POST /api/auth/v1/invitations/<token>/accept

    Requirements:
        - Requires bearer JWT
        - Invitation must be pending and not expired
    """
    principal = authenticate_bearer(request)
    user_id = principal.user_id

    row = supabase_client.select_single(
        table="invitations",
        filters={"token": token},
        select="id,email,tenant_id,app_id,role,status,expires_at",
    )
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    status = str(row.get("status") or "").strip().lower()
    if status != "pending":
        raise HTTPException(status_code=400, detail="Invitation is not pending")

    expires_at_raw = str(row.get("expires_at") or "").strip()
    try:
        expires_at = datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invitation expiration is invalid")
    if datetime.now(timezone.utc) > expires_at:
        supabase_client.patch_rows(
            table="invitations",
            filters={"id": str(row.get("id") or "")},
            patch={"status": "expired"},
        )
        raise HTTPException(status_code=400, detail="Invitation has expired")

    tenant_id = str(row.get("tenant_id") or "").strip()
    app_id = str(row.get("app_id") or "").strip()
    role = str(row.get("role") or "").strip()
    invite_email = str(row.get("email") or "").strip().lower()
    principal_email = str(principal.email or "").strip().lower()
    if invite_email and principal_email and invite_email != principal_email:
        raise HTTPException(status_code=403, detail="Invite email does not match authenticated user")

    tenant_user_row = supabase_client.select_single(
        table="tenant_users",
        filters={"tenant_id": tenant_id, "user_id": user_id},
        select="id,status",
    )
    if tenant_user_row:
        supabase_client.patch_rows(
            table="tenant_users",
            filters={"id": str(tenant_user_row.get("id") or "")},
            patch={"status": "active", "updated_at": supabase_client.now_iso()},
        )
    else:
        supabase_client.insert_row(
            table="tenant_users",
            row={
                "tenant_id": tenant_id,
                "user_id": user_id,
                "status": "active",
            },
        )

    role_row = supabase_client.select_single(
        table="tenant_app_roles",
        filters={"tenant_id": tenant_id, "user_id": user_id, "app_id": app_id},
        select="id,role",
    )
    if role_row:
        supabase_client.patch_rows(
            table="tenant_app_roles",
            filters={"id": str(role_row.get("id") or "")},
            patch={"role": role, "updated_at": supabase_client.now_iso()},
        )
    else:
        supabase_client.insert_row(
            table="tenant_app_roles",
            row={
                "tenant_id": tenant_id,
                "user_id": user_id,
                "app_id": app_id,
                "role": role,
            },
        )

    supabase_client.patch_rows(
        table="invitations",
        filters={"id": str(row.get("id") or "")},
        patch={
            "status": "accepted",
            "accepted_at": supabase_client.now_iso(),
            "accepted_by_user_id": user_id,
        },
    )

    permission_cache.invalidate(user_id=user_id)

    return {
        "status": "accepted",
        "tenantId": tenant_id,
        "appId": app_id,
        "role": role,
    }


@router.post("/{invitation_id}/revoke")
def revoke_invitation_route(request: Request, invitation_id: str):
    """
    Revoke a pending invitation.

    Example request:
        POST /api/auth/v1/invitations/<id>/revoke

    Requirements:
        - Requires bearer JWT
        - Requires X-Tenant-Id
        - Requires tradsphere.admin role
    """
    _, tenant_id = _require_admin(request)

    row = supabase_client.select_single(
        table="invitations",
        filters={"id": invitation_id, "tenant_id": tenant_id},
        select="id,status",
    )
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    supabase_client.patch_rows(
        table="invitations",
        filters={"id": invitation_id},
        patch={"status": "revoked"},
    )

    return {"status": "revoked", "id": invitation_id}
