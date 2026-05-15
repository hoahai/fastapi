from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Body, HTTPException, Query, Request

from shared.auth.admin_repo import find_user_id_by_email, is_auth_user_active, list_tenant_users_with_app_role
from shared.auth.config import get_invite_base_url
from shared.auth.dependencies import authenticate_bearer, authorize_bearer_for_tenant_app
from shared.auth.invite_url import build_invite_url
from shared.auth.invitations_repo import (
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
from shared.auth.profile_repo import compose_full_name, select_profile_for_user, split_full_name
from shared.auth.providers import get_auth_provider
from shared.auth.roles import ROLE_ADMIN, ROLE_EDITOR, ROLE_VIEWER, normalize_role_key
from shared.auth.supabase_client import SupabaseClientError

router = APIRouter(prefix="/invitations")
VALID_SCOPED_ROLE_KEYS = {ROLE_VIEWER, ROLE_EDITOR, ROLE_ADMIN}


def _normalize_app_code(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    if not normalized.replace("-", "").replace("_", "").isalnum():
        return ""
    return normalized


def _resolve_target_app_code(
    *,
    request: Request,
    override_app_code: str | None = None,
    fallback_app_code: str = "tradsphere",
) -> str:
    override = _normalize_app_code(override_app_code)
    if override:
        return override
    header_code = _normalize_app_code(request.headers.get("x-app-code"))
    if header_code:
        return header_code
    fallback = _normalize_app_code(fallback_app_code)
    return fallback or "tradsphere"


def _require_admin(
    request: Request,
    *,
    app_code: str | None = None,
) -> tuple[str, str, str, str, str, bool]:
    target_app_code = _resolve_target_app_code(request=request, override_app_code=app_code)
    result = authorize_bearer_for_tenant_app(request=request, app_code=target_app_code)
    permissions = set(result.access.permissions)
    is_super_admin = "workspace.super_admin" in permissions or str(result.access.role or "").strip().lower() == "super_admin"
    if f"{target_app_code}.admin" not in permissions and not is_super_admin:
        raise HTTPException(status_code=403, detail="Forbidden")
    return (
        result.principal.user_id,
        result.access.tenant_id,
        result.access.tenant_slug,
        result.access.app_id,
        result.access.app_code,
        is_super_admin,
    )


def _invite_url(token: str) -> str:
    return build_invite_url(token=token, base_url=get_invite_base_url())


def _parse_invitation_expiry(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invitation expiration is invalid") from exc


def _serialize_scoped_user_row(
    *,
    row: dict[str, object],
    tenant_id: str,
    app_id: str,
) -> dict[str, object] | None:
    user_id = str(row.get("userId") or "").strip()
    if not user_id:
        return None
    email = str(row.get("email") or "").strip().lower() or None
    full_name = str(row.get("fullName") or "").strip() or None
    first_name, last_name = split_full_name(full_name)
    status = str(row.get("status") or "").strip().lower() or "active"

    app_assignments = row.get("appAssignments")
    assignments = app_assignments if isinstance(app_assignments, list) else []
    current_assignment = next(
        (
            item
            for item in assignments
            if isinstance(item, dict)
            and str(item.get("tenantId") or "").strip() == tenant_id
            and str(item.get("appId") or "").strip() == app_id
        ),
        None,
    )
    if not isinstance(current_assignment, dict):
        return None

    role = normalize_role_key(str(current_assignment.get("role") or "").strip()) or None
    return {
        "userId": user_id,
        "email": email,
        "fullName": full_name,
        "firstName": first_name,
        "lastName": last_name,
        "status": status,
        "role": role,
        "assignedInScope": True,
    }


@router.post("")
def create_invitation_route(
    request: Request,
    payload: dict = Body(...),
):
    """
    Create a pending app-scoped invitation and return a reusable invite URL.

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
        - Requires current app admin permission for app-scope invites
        - Non-super admins can invite only existing active users
        - Brand-new user invites are restricted to workspace super admins
        - Tenant/app admins can assign only: viewer, editor, admin
        - super_admin cannot be assigned from this endpoint
    """
    payload_app_code = _normalize_app_code(payload.get("appCode"))
    invited_by_user_id, tenant_id, _, _, app_code, is_super_admin = _require_admin(
        request,
        app_code=payload_app_code or None,
    )

    email = str(payload.get("email") or "").strip().lower()
    role = normalize_role_key(str(payload.get("role") or "").strip())

    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    if role not in VALID_SCOPED_ROLE_KEYS:
        raise HTTPException(status_code=400, detail="Invalid role")
    if not is_super_admin:
        existing_user_id = find_user_id_by_email(email=email)
        if not existing_user_id:
            raise HTTPException(
                status_code=403,
                detail="Only Super Admin can invite brand-new users. Tenant/App admin can invite active existing users only.",
            )
        if not is_auth_user_active(user_id=existing_user_id):
            raise HTTPException(
                status_code=403,
                detail="Tenant/App admin can invite active existing users only.",
            )

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


@router.get("/users")
def list_scoped_invitation_users_route(
    request: Request,
    query: str | None = Query(default=None),
):
    """
    List users currently assigned to the active tenant + app scope for app-scoped access management UI.

    Example request:
        GET /api/auth/v1/invitations/users

    Example request (with filter):
        GET /api/auth/v1/invitations/users?query=alex@example.com

    Example response:
        {
          "items": [
            {
              "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
              "email": "alex@example.com",
              "fullName": "Alex Johnson",
              "firstName": "Alex",
              "lastName": "Johnson",
              "status": "active",
              "role": "editor",
              "assignedInScope": true
            }
          ],
          "scope": {
            "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
            "appCode": "shiftzy"
          }
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission or workspace.super_admin
    """
    _, tenant_id, _, app_id, app_code, _ = _require_admin(request)
    app_rows = list_tenant_users_with_app_role(tenant_id=tenant_id, app_id=app_id)
    search_text = str(query or "").strip().lower()

    items: list[dict[str, object]] = []
    for row in app_rows:
        if not isinstance(row, dict):
            continue
        normalized = _serialize_scoped_user_row(row=row, tenant_id=tenant_id, app_id=app_id)
        if not normalized:
            continue
        if search_text:
            haystack = " ".join(
                [
                    str(normalized.get("email") or ""),
                    str(normalized.get("fullName") or ""),
                    str(normalized.get("userId") or ""),
                    str(normalized.get("role") or ""),
                ]
            ).lower()
            if search_text not in haystack:
                continue
        items.append(normalized)

    items.sort(key=lambda item: (str(item.get("email") or "~").lower(), str(item.get("userId") or "")))
    return {
        "items": items,
        "scope": {
            "tenantId": tenant_id,
            "appCode": app_code,
        },
    }


@router.get("/users/lookup")
def lookup_active_existing_user_route(
    request: Request,
    email: str = Query(...),
):
    """
    Lookup an active existing user by email for app-scoped access-add flows.

    Example request:
        GET /api/auth/v1/invitations/users/lookup?email=alex@example.com

    Example response:
        {
          "items": [
            {
              "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
              "email": "alex@example.com",
              "fullName": "Alex Johnson",
              "firstName": "Alex",
              "lastName": "Johnson",
              "status": "active",
              "role": null,
              "assignedInScope": false
            }
          ],
          "scope": {
            "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
            "appCode": "shiftzy"
          }
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission or workspace.super_admin
        - Returns only active existing users
    """
    _, tenant_id, _, app_id, app_code, _ = _require_admin(request)
    normalized_email = str(email or "").strip().lower()
    if not normalized_email or "@" not in normalized_email:
        raise HTTPException(status_code=400, detail="Valid email is required")

    user_id = find_user_id_by_email(email=normalized_email)
    if not user_id or not is_auth_user_active(user_id=user_id):
        return {
            "items": [],
            "scope": {
                "tenantId": tenant_id,
                "appCode": app_code,
            },
        }

    scoped_rows = list_tenant_users_with_app_role(tenant_id=tenant_id, app_id=app_id)
    for row in scoped_rows:
        if not isinstance(row, dict):
            continue
        scoped_item = _serialize_scoped_user_row(row=row, tenant_id=tenant_id, app_id=app_id)
        if scoped_item and str(scoped_item.get("userId") or "").strip() == user_id:
            return {
                "items": [scoped_item],
                "scope": {
                    "tenantId": tenant_id,
                    "appCode": app_code,
                },
            }

    profile = select_profile_for_user(provider=get_auth_provider(), user_id=user_id) or {}
    full_name = str(profile.get("full_name") or "").strip() or None
    first_name, last_name = split_full_name(full_name)
    profile_email = str(profile.get("email") or "").strip().lower() or normalized_email
    return {
        "items": [
            {
                "userId": user_id,
                "email": profile_email,
                "fullName": full_name,
                "firstName": first_name,
                "lastName": last_name,
                "status": "active",
                "role": None,
                "assignedInScope": False,
            }
        ],
        "scope": {
            "tenantId": tenant_id,
            "appCode": app_code,
        },
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
        - Requires current app admin permission
    """
    _, tenant_id, _, _, _, _ = _require_admin(request)

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
