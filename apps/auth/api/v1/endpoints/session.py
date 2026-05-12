from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request

from shared.auth.config import is_debug_endpoint_enabled
from shared.auth.dependencies import authorize_bearer_for_tenant_app
from shared.auth.profile_repo import compose_full_name, select_profile_for_user, split_full_name, upsert_profile_basic_info
from shared.auth.providers import get_auth_provider
from shared.auth.supabase_client import SupabaseClientError

router = APIRouter(prefix="/session")
debug_router = APIRouter(prefix="/debug")


def _provider():
    return get_auth_provider()


def _parse_profile_full_name(payload: dict | None) -> str | None:
    body = payload if isinstance(payload, dict) else {}
    profile_payload = body.get("profile")
    profile = profile_payload if isinstance(profile_payload, dict) else {}
    return compose_full_name(
        full_name=str(profile.get("fullName") or body.get("fullName") or "").strip() or None,
        first_name=str(profile.get("firstName") or body.get("firstName") or "").strip() or None,
        last_name=str(profile.get("lastName") or body.get("lastName") or "").strip() or None,
    )


def _build_user_payload(*, user_id: str, email: str | None) -> dict[str, object]:
    profile = select_profile_for_user(provider=_provider(), user_id=user_id) or {}
    profile_full_name = str(profile.get("full_name") or "").strip() or None
    first_name, last_name = split_full_name(profile_full_name)
    resolved_email = str(profile.get("email") or "").strip() or (str(email or "").strip() or None)

    return {
        "id": user_id,
        "email": resolved_email,
        "fullName": profile_full_name,
        "firstName": first_name,
        "lastName": last_name,
    }


@router.get("/me")
def get_session_me(request: Request):
    """
    Return authenticated user info and tenant/app access for current tenant slug.

    Example request:
        GET /api/auth/v1/session/me

    Example response:
        {
          "user": {
            "id": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
            "email": "alex@example.com",
            "fullName": "Alex Johnson",
            "firstName": "Alex",
            "lastName": "Johnson"
          },
          "tenant": {
            "id": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
            "slug": "taaa"
          },
          "app": {
            "id": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
            "code": "tradsphere"
          },
          "role": "tradsphere.viewer",
          "permissions": [
            "tradsphere.viewer"
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id tenant slug
        - Validates membership and app access for TradSphere
    """
    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    return {
        "user": _build_user_payload(
            user_id=result.principal.user_id,
            email=result.principal.email,
        ),
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


@router.patch("/me/profile")
def patch_session_me_profile(
    request: Request,
    payload: dict | None = Body(default=None),
):
    """
    Update basic profile info for the currently authenticated tenant user.

    Example request:
        PATCH /api/auth/v1/session/me/profile

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
          "user": {
            "id": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
            "email": "alex@example.com",
            "fullName": "Alex Johnson",
            "firstName": "Alex",
            "lastName": "Johnson"
          }
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id tenant slug
        - Validates membership and app access for TradSphere
        - Accepts either `fullName` or `firstName`/`lastName`
    """
    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    full_name = _parse_profile_full_name(payload)
    if full_name is None:
        raise HTTPException(status_code=400, detail="Profile name is required")
    if len(full_name) > 255:
        raise HTTPException(status_code=400, detail="Profile name exceeds maximum length")

    try:
        upsert_profile_basic_info(
            provider=_provider(),
            user_id=result.principal.user_id,
            email=result.principal.email,
            full_name=full_name,
        )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {
        "user": _build_user_payload(
            user_id=result.principal.user_id,
            email=result.principal.email,
        )
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
        "user": _build_user_payload(
            user_id=result.principal.user_id,
            email=result.principal.email,
        ),
        "tenant": {
            "id": result.access.tenant_id,
            "slug": result.access.tenant_slug,
        },
        "role": result.access.role,
        "permissions": sorted(result.access.permissions),
    }
