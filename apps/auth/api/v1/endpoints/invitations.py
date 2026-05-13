from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Body, HTTPException, Request

from shared.auth.config import get_invite_base_url
from shared.auth.dependencies import authenticate_bearer, authorize_bearer_for_tenant_app
from shared.auth.invite_url import build_invite_url
from shared.auth.invitations_repo import (
    VALID_TRADSPHERE_ROLES,
    activate_tenant_user,
    create_invitation,
    get_active_app,
    get_invitation_by_id,
    get_invitation_by_token,
    list_invitation_assignments,
    mark_invitation_accepted,
    patch_invitation_status,
    upsert_tenant_app_role,
    upsert_profile_for_invited_user,
)
from shared.auth.permissions_cache import permission_cache
from shared.auth.profile_repo import compose_full_name
from shared.auth.roles import normalize_role_key
from shared.auth.supabase_client import SupabaseClientError

router = APIRouter(prefix="/invitations")


def _require_admin(request: Request) -> tuple[str, str, str]:
    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    if "tradsphere.admin" not in result.access.permissions:
        raise HTTPException(status_code=403, detail="Forbidden")
    return result.principal.user_id, result.access.tenant_id, result.access.tenant_slug


def _invite_url(token: str) -> str:
    return build_invite_url(token=token, base_url=get_invite_base_url())


def _parse_invitation_expiry(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invitation expiration is invalid") from exc


@router.post("")
def create_invitation_route(
    request: Request,
    payload: dict = Body(...),
):
    """
    Create a pending TradSphere invitation and return a reusable invite URL.

    Example request:
        POST /api/auth/v1/invitations

    Example response:
        {
          "id": "0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d",
          "email": "new.user@company.com",
          "role": "viewer",
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
    invited_by_user_id, tenant_id, _ = _require_admin(request)

    email = str(payload.get("email") or "").strip().lower()
    app_code = str(payload.get("appCode") or "tradsphere").strip().lower()
    role = normalize_role_key(str(payload.get("role") or "").strip())

    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    if app_code != "tradsphere":
        raise HTTPException(status_code=400, detail="Only tradsphere app is supported in Phase 1")
    if role not in VALID_TRADSPHERE_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")

    app_row = get_active_app(app_code=app_code)
    if not app_row:
        raise HTTPException(status_code=400, detail="Requested app is not enabled")

    try:
        created = create_invitation(
            email=email,
            tenant_id=tenant_id,
            app_id=str(app_row.get("id") or ""),
            role=role,
            invited_by_user_id=invited_by_user_id,
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


@router.get("/{token}")
def get_invitation_route(token: str):
    """
    Resolve invitation metadata for invite landing and acceptance UI.

    Example request:
        GET /api/auth/v1/invitations/abc123

    Example response:
        {
          "id": "0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d",
          "email": "new.user@company.com",
          "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
          "appId": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
          "role": "viewer",
          "status": "pending",
          "expiresAt": "2026-05-13T08:00:00+00:00",
          "acceptedAt": null,
          "createdAt": "2026-05-10T08:00:00+00:00"
        }

    Requirements:
        - Public endpoint
        - Token must exist
        - Returns no secrets/tokens beyond invitation token route lookup
    """
    row = get_invitation_by_token(token)
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    invitation_id = str(row.get("id") or "").strip()
    assignments = list_invitation_assignments(invitation_id=invitation_id)
    resolved_assignments = [
        {
            "tenantId": str(item.get("tenant_id") or "").strip(),
            "appId": str(item.get("app_id") or "").strip(),
            "role": normalize_role_key(str(item.get("role") or "").strip()),
        }
        for item in assignments
        if str(item.get("tenant_id") or "").strip()
        and str(item.get("app_id") or "").strip()
        and normalize_role_key(str(item.get("role") or "").strip())
    ]
    if not resolved_assignments:
        resolved_assignments = [
            {
                "tenantId": str(row.get("tenant_id") or "").strip(),
                "appId": str(row.get("app_id") or "").strip(),
                "role": normalize_role_key(str(row.get("role") or "").strip()),
            }
        ]

    return {
        "id": row.get("id"),
        "email": row.get("email"),
        "tenantId": row.get("tenant_id"),
        "appId": row.get("app_id"),
        "role": normalize_role_key(str(row.get("role") or "").strip()),
        "status": row.get("status"),
        "expiresAt": row.get("expires_at"),
        "acceptedAt": row.get("accepted_at"),
        "createdAt": row.get("created_at"),
        "assignments": resolved_assignments,
    }


@router.post("/{token}/accept")
def accept_invitation_route(
    request: Request,
    token: str,
    payload: dict | None = Body(default=None),
):
    """
    Accept a pending invitation for the currently authenticated user.

    Example request:
        POST /api/auth/v1/invitations/abc123/accept

    Example request body:
        {
          "profile": {
            "firstName": "Alex",
            "lastName": "Johnson",
            "fullName": "Alex Johnson"
          }
        }

    Example response:
        {
          "status": "accepted",
          "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
          "appId": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
          "role": "viewer"
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Invitation must be pending and not expired
        - Signed-in user email must match invite email when invite email is present
    """
    principal = authenticate_bearer(request)
    user_id = principal.user_id

    row = get_invitation_by_token(token)
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    status = str(row.get("status") or "").strip().lower()
    if status != "pending":
        raise HTTPException(status_code=400, detail="Invitation is not pending")

    expires_at_raw = str(row.get("expires_at") or "").strip()
    expires_at = _parse_invitation_expiry(expires_at_raw)
    if datetime.now(timezone.utc) > expires_at:
        patch_invitation_status(invitation_id=str(row.get("id") or ""), status="expired")
        raise HTTPException(status_code=400, detail="Invitation has expired")

    invitation_id = str(row.get("id") or "").strip()
    tenant_id = str(row.get("tenant_id") or "").strip()
    app_id = str(row.get("app_id") or "").strip()
    role = normalize_role_key(str(row.get("role") or "").strip())
    assignments = list_invitation_assignments(invitation_id=invitation_id)
    resolved_assignments = [
        (
            str(item.get("tenant_id") or "").strip(),
            str(item.get("app_id") or "").strip(),
            normalize_role_key(str(item.get("role") or "").strip()),
        )
        for item in assignments
        if str(item.get("tenant_id") or "").strip()
        and str(item.get("app_id") or "").strip()
        and normalize_role_key(str(item.get("role") or "").strip())
    ]
    if not resolved_assignments:
        resolved_assignments = [(tenant_id, app_id, role)]
    invite_email = str(row.get("email") or "").strip().lower()
    principal_email = str(principal.email or "").strip().lower()
    if invite_email and principal_email and invite_email != principal_email:
        raise HTTPException(status_code=403, detail="Invite email does not match authenticated user")

    body = payload if isinstance(payload, dict) else {}
    profile_payload = body.get("profile")
    profile = profile_payload if isinstance(profile_payload, dict) else {}
    full_name = compose_full_name(
        full_name=str(profile.get("fullName") or body.get("fullName") or "").strip() or None,
        first_name=str(profile.get("firstName") or body.get("firstName") or "").strip() or None,
        last_name=str(profile.get("lastName") or body.get("lastName") or "").strip() or None,
    )
    if full_name is not None and len(full_name) > 255:
        raise HTTPException(status_code=400, detail="Full name exceeds maximum length")

    try:
        upsert_profile_for_invited_user(
            user_id=user_id,
            email=principal.email or invite_email or None,
            full_name=full_name,
        )
        for assignment_tenant_id, assignment_app_id, assignment_role in resolved_assignments:
            activate_tenant_user(tenant_id=assignment_tenant_id, user_id=user_id)
            upsert_tenant_app_role(
                tenant_id=assignment_tenant_id,
                user_id=user_id,
                app_id=assignment_app_id,
                role=assignment_role,
            )
        mark_invitation_accepted(invitation_id=invitation_id, accepted_by_user_id=user_id)
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    permission_cache.invalidate(user_id=user_id)

    return {
        "status": "accepted",
        "tenantId": tenant_id,
        "appId": app_id,
        "role": role,
        "assignments": [
            {
                "tenantId": assignment_tenant_id,
                "appId": assignment_app_id,
                "role": assignment_role,
            }
            for assignment_tenant_id, assignment_app_id, assignment_role in resolved_assignments
        ],
    }


@router.post("/{invitation_id}/revoke")
def revoke_invitation_route(request: Request, invitation_id: str):
    """
    Revoke an invitation belonging to the active tenant.

    Example request:
        POST /api/auth/v1/invitations/0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d/revoke

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
    _, tenant_id, _ = _require_admin(request)

    row = get_invitation_by_id(invitation_id=invitation_id, tenant_id=tenant_id)
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
