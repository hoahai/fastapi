from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Body, HTTPException, Query, Request

from shared.auth.admin_repo import (
    list_tenant_users_with_app_role,
    set_tenant_app_role,
    set_tenant_user_status,
)
from shared.auth.config import get_invite_base_url, get_invite_ttl_hours
from shared.auth.dependencies import authorize_bearer_for_tenant_app
from shared.auth.invite_url import build_invite_url
from shared.auth.invitations_repo import (
    VALID_TRADSPHERE_ROLES,
    create_invitation,
    get_active_app,
    get_invitation_by_id,
    list_invitations,
    patch_invitation_status,
)
from shared.auth.permissions_cache import permission_cache
from shared.auth.supabase_client import SupabaseClientError

router = APIRouter(prefix="/admin")

VALID_MEMBERSHIP_STATUSES = {"active", "pending", "disabled"}


def _require_admin(request: Request):
    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    if "tradsphere.admin" not in result.access.permissions:
        raise HTTPException(status_code=403, detail="Forbidden")
    return result


def _invite_url(token: str) -> str:
    return build_invite_url(token=token, base_url=get_invite_base_url())


@router.get("/roles")
def list_roles_route(request: Request):
    """
    Return assignable TradSphere roles for admin user-management workflows.

    Example request:
        GET /api/auth/v1/admin/roles

    Example response:
        {
          "roles": [
            "tradsphere.viewer",
            "tradsphere.editor",
            "tradsphere.admin"
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires tradsphere.admin permission
    """
    _require_admin(request)
    return {"roles": list(VALID_TRADSPHERE_ROLES)}


@router.get("/users")
def list_users_route(request: Request):
    """
    List tenant members and their TradSphere role/access status.

    Example request:
        GET /api/auth/v1/admin/users

    Example response:
        {
          "items": [
            {
              "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
              "email": "admin@company.com",
              "fullName": "Workspace Admin",
              "status": "active",
              "role": "tradsphere.admin",
              "createdAt": "2026-05-10T08:00:00+00:00",
              "updatedAt": "2026-05-10T08:00:00+00:00",
              "roleUpdatedAt": "2026-05-10T08:00:00+00:00"
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires tradsphere.admin permission
    """
    result = _require_admin(request)
    items = list_tenant_users_with_app_role(tenant_id=result.access.tenant_id, app_id=result.access.app_id)
    return {"items": items}


@router.patch("/users/{user_id}")
def update_user_access_route(
    request: Request,
    user_id: str,
    payload: dict = Body(...),
):
    """
    Update tenant membership status and/or TradSphere role for a user.

    Example request:
        PATCH /api/auth/v1/admin/users/762fec4b-1da3-4ba7-80e2-dd21622b6e0d

    Example response:
        {
          "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
          "status": "disabled",
          "role": "tradsphere.viewer",
          "permissionCacheInvalidated": true
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires tradsphere.admin permission
        - status must be one of: active, pending, disabled
        - role must be a valid TradSphere role when provided
    """
    result = _require_admin(request)
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        raise HTTPException(status_code=400, detail="user_id is required")

    next_status_raw = payload.get("status")
    next_role_raw = payload.get("role")

    next_status = str(next_status_raw or "").strip().lower() if next_status_raw is not None else ""
    next_role = str(next_role_raw or "").strip() if next_role_raw is not None else ""

    if not next_status and not next_role:
        raise HTTPException(status_code=400, detail="At least one of status or role is required")

    if next_status and next_status not in VALID_MEMBERSHIP_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid status")
    if next_role and next_role not in VALID_TRADSPHERE_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")

    try:
        if next_status:
            set_tenant_user_status(
                tenant_id=result.access.tenant_id,
                user_id=normalized_user_id,
                status=next_status,
            )

        if next_role:
            set_tenant_app_role(
                tenant_id=result.access.tenant_id,
                user_id=normalized_user_id,
                app_id=result.access.app_id,
                role=next_role,
            )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    permission_cache.invalidate(user_id=normalized_user_id)

    refreshed = list_tenant_users_with_app_role(tenant_id=result.access.tenant_id, app_id=result.access.app_id)
    row = next((item for item in refreshed if str(item.get("userId") or "") == normalized_user_id), None)
    if row is None:
        row = {
            "userId": normalized_user_id,
            "status": next_status or "pending",
            "role": next_role or None,
        }

    return {
        **row,
        "permissionCacheInvalidated": True,
    }


@router.get("/invitations")
def list_invitations_route(
    request: Request,
    status: str | None = Query(default=None),
):
    """
    List invitations for the active tenant and TradSphere app.

    Example request:
        GET /api/auth/v1/admin/invitations?status=pending

    Example response:
        {
          "items": [
            {
              "id": "0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d",
              "email": "new.user@company.com",
              "role": "tradsphere.viewer",
              "status": "pending",
              "expiresAt": "2026-05-13T08:00:00+00:00",
              "createdAt": "2026-05-10T08:00:00+00:00",
              "acceptedAt": null,
              "inviteUrl": "https://workspace.example.com/auth/invite/abc123"
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires tradsphere.admin permission
        - Optional status filter: pending, accepted, revoked, expired
    """
    result = _require_admin(request)
    normalized_status = str(status or "").strip().lower() or None
    if normalized_status and normalized_status not in {"pending", "accepted", "revoked", "expired"}:
        raise HTTPException(status_code=400, detail="Invalid status filter")

    items = list_invitations(
        tenant_id=result.access.tenant_id,
        app_id=result.access.app_id,
        status=normalized_status,
    )

    return {
        "items": [
            {
                "id": row.get("id"),
                "email": row.get("email"),
                "role": row.get("role"),
                "status": row.get("status"),
                "expiresAt": row.get("expires_at"),
                "createdAt": row.get("created_at"),
                "acceptedAt": row.get("accepted_at"),
                "inviteUrl": _invite_url(str(row.get("token") or "")),
            }
            for row in items
        ]
    }


@router.post("/invitations")
def create_invitation_admin_route(
    request: Request,
    payload: dict = Body(...),
):
    """
    Create an invitation and return invite URL for admin distribution.

    Example request:
        POST /api/auth/v1/admin/invitations

    Example response:
        {
          "id": "0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d",
          "email": "new.user@company.com",
          "role": "tradsphere.viewer",
          "status": "pending",
          "expiresAt": "2026-05-13T08:00:00+00:00",
          "inviteUrl": "https://workspace.example.com/auth/invite/abc123"
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires tradsphere.admin permission
        - appCode currently supports tradsphere only
    """
    result = _require_admin(request)

    email = str(payload.get("email") or "").strip().lower()
    app_code = str(payload.get("appCode") or "tradsphere").strip().lower()
    role = str(payload.get("role") or "").strip()
    expires_hours = payload.get("expirationHours")

    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    if app_code != "tradsphere":
        raise HTTPException(status_code=400, detail="Only tradsphere app is supported in Phase 1")
    if role not in VALID_TRADSPHERE_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")

    app_row = get_active_app(app_code=app_code)
    if not app_row:
        raise HTTPException(status_code=400, detail="Requested app is not enabled")

    ttl_hours = get_invite_ttl_hours()
    if expires_hours is not None:
        try:
            parsed = int(str(expires_hours).strip())
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="expirationHours must be an integer") from exc
        ttl_hours = max(parsed, 1)

    expires_at = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)

    try:
        created = create_invitation(
            email=email,
            tenant_id=result.access.tenant_id,
            app_id=str(app_row.get("id") or ""),
            role=role,
            invited_by_user_id=result.principal.user_id,
            expires_at=expires_at,
        )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    token = str(created.get("token") or "")
    return {
        "id": created.get("id"),
        "email": email,
        "role": role,
        "status": created.get("status") or "pending",
        "expiresAt": created.get("expires_at"),
        "inviteUrl": _invite_url(token),
    }


@router.post("/invitations/{invitation_id}/revoke")
def revoke_invitation_admin_route(request: Request, invitation_id: str):
    """
    Revoke a pending invitation from the active tenant/app.

    Example request:
        POST /api/auth/v1/admin/invitations/0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d/revoke

    Example response:
        {
          "status": "revoked",
          "id": "0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d"
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires tradsphere.admin permission
    """
    result = _require_admin(request)

    row = get_invitation_by_id(invitation_id=invitation_id, tenant_id=result.access.tenant_id)
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    accepted_by_user_id = str(row.get("accepted_by_user_id") or "").strip()
    try:
        patch_invitation_status(invitation_id=invitation_id, status="revoked")
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if accepted_by_user_id:
        permission_cache.invalidate(user_id=accepted_by_user_id)

    return {"status": "revoked", "id": invitation_id}
