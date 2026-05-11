from __future__ import annotations

from shared.auth.providers import get_auth_provider
from shared.auth.supabase_client import SupabaseClientError


def _provider():
    return get_auth_provider()


def _as_non_empty_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _select_profile_for_user(*, provider, user_id: str) -> dict[str, object] | None:
    profile = provider.select_single(
        table="profiles",
        filters={"user_id": user_id},
        select="user_id,email,full_name,created_at,updated_at",
    )
    if profile:
        return profile

    # Optional compatibility fallback for deployments where profile PK is `id`.
    # Some schemas do not expose `profiles.id`; ignore that case gracefully.
    try:
        profile = provider.select_single(
            table="profiles",
            filters={"id": user_id},
            select="id,user_id,email,full_name,created_at,updated_at",
        )
    except SupabaseClientError as exc:
        if "column profiles.id does not exist" in str(exc):
            return None
        raise
    if profile:
        return profile
    return None


def _select_invitation_email_for_user(*, provider, tenant_id: str, user_id: str) -> str | None:
    rows = provider.select_many(
        table="invitations",
        filters={"tenant_id": tenant_id, "accepted_by_user_id": user_id},
        select="email,accepted_at,created_at",
    )
    candidates: list[tuple[str, str]] = []
    for row in rows:
        email = _as_non_empty_text(row.get("email"))
        if not email:
            continue
        timestamp = _as_non_empty_text(row.get("accepted_at")) or _as_non_empty_text(row.get("created_at")) or ""
        candidates.append((timestamp, email))
    if not candidates:
        return None
    candidates.sort(key=lambda entry: entry[0], reverse=True)
    return candidates[0][1]


def list_tenant_users_with_app_role(*, tenant_id: str, app_id: str) -> list[dict[str, object]]:
    provider = _provider()
    membership_rows = provider.select_many(
        table="tenant_users",
        filters={"tenant_id": tenant_id},
        select="id,user_id,status,created_at,updated_at",
    )
    role_rows = provider.select_many(
        table="tenant_app_roles",
        filters={"tenant_id": tenant_id, "app_id": app_id},
        select="id,user_id,role,created_at,updated_at",
    )

    membership_by_user: dict[str, dict[str, object]] = {}
    role_by_user: dict[str, dict[str, object]] = {}

    for row in membership_rows:
        user_id = str(row.get("user_id") or "").strip()
        if user_id:
            membership_by_user[user_id] = row

    for row in role_rows:
        user_id = str(row.get("user_id") or "").strip()
        if user_id:
            role_by_user[user_id] = row

    user_ids = sorted(set(membership_by_user.keys()) | set(role_by_user.keys()))

    results: list[dict[str, object]] = []
    for user_id in user_ids:
        profile = _select_profile_for_user(provider=provider, user_id=user_id)
        invitation_email = _select_invitation_email_for_user(provider=provider, tenant_id=tenant_id, user_id=user_id)
        resolved_email = _as_non_empty_text(profile.get("email") if profile else None) or invitation_email
        membership = membership_by_user.get(user_id) or {}
        app_role = role_by_user.get(user_id) or {}

        results.append(
            {
                "userId": user_id,
                "email": resolved_email,
                "fullName": profile.get("full_name") if profile else None,
                "status": membership.get("status") or "pending",
                "role": app_role.get("role") or None,
                "createdAt": membership.get("created_at") or profile.get("created_at") if profile else None,
                "updatedAt": membership.get("updated_at") or profile.get("updated_at") if profile else None,
                "roleUpdatedAt": app_role.get("updated_at") or app_role.get("created_at"),
            }
        )

    results.sort(key=lambda row: (str(row.get("email") or "~").lower(), str(row.get("userId") or "")))
    return results


def set_tenant_user_status(*, tenant_id: str, user_id: str, status: str) -> None:
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
            patch={"status": status, "updated_at": provider.now_iso()},
        )
        return

    provider.insert_row(
        table="tenant_users",
        row={
            "tenant_id": tenant_id,
            "user_id": user_id,
            "status": status,
        },
    )


def set_tenant_app_role(*, tenant_id: str, user_id: str, app_id: str, role: str) -> None:
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
