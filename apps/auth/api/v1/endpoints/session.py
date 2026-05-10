from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from shared.auth.config import is_debug_endpoint_enabled
from shared.auth.dependencies import authorize_bearer_for_tenant_app

router = APIRouter(prefix="/session")
debug_router = APIRouter(prefix="/debug")


@router.get("/me")
def get_session_me(request: Request):
    """
    Return authenticated user info and tenant/app access for current tenant slug.

    Example request:
        GET /api/auth/v1/session/me

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id tenant slug
        - Validates membership and app access for TradSphere
    """
    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    return {
        "user": {
            "id": result.principal.user_id,
            "email": result.principal.email,
        },
        "tenant": {
            "id": result.access.tenant_id,
            "slug": result.access.tenant_slug,
        },
        "app": {
            "id": result.access.app_id,
            "code": result.access.app_code,
        },
        "role": result.access.role,
        "permissions": sorted(result.access.permissions),
    }


@debug_router.get("/whoami")
def get_debug_whoami(request: Request):
    """
    Dev-only auth debug endpoint.

    Example request:
        GET /api/auth/v1/debug/whoami

    Requirements:
        - Enabled only when AUTH_ENABLE_DEBUG_ENDPOINTS=true
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id tenant slug
        - Does not return sensitive tokens/secrets
    """
    if not is_debug_endpoint_enabled():
        raise HTTPException(status_code=404, detail="Not Found")

    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    return {
        "authMode": "supabase_jwt",
        "user": {
            "id": result.principal.user_id,
            "email": result.principal.email,
        },
        "tenant": {
            "id": result.access.tenant_id,
            "slug": result.access.tenant_slug,
        },
        "role": result.access.role,
        "permissions": sorted(result.access.permissions),
    }

