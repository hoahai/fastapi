from __future__ import annotations

from shared.auth.supabase_client import SupabaseClientError


def _as_non_empty_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def split_full_name(full_name: str | None) -> tuple[str | None, str | None]:
    normalized = _as_non_empty_text(full_name)
    if not normalized:
        return None, None

    parts = normalized.split()
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


def compose_full_name(
    *,
    full_name: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> str | None:
    normalized_full_name = _as_non_empty_text(full_name)
    if normalized_full_name is not None:
        return normalized_full_name

    normalized_first_name = _as_non_empty_text(first_name)
    normalized_last_name = _as_non_empty_text(last_name)
    combined = " ".join(part for part in [normalized_first_name, normalized_last_name] if part)
    return _as_non_empty_text(combined)


def _is_missing_column_error(exc: SupabaseClientError, *, table: str, column: str) -> bool:
    message = str(exc).lower()
    return (
        f"column {table}.{column} does not exist" in message
        or f'column "{column}" of relation "{table}" does not exist' in message
    )


def select_profile_for_user(*, provider, user_id: str) -> dict[str, object] | None:
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
        if _is_missing_column_error(exc, table="profiles", column="id"):
            return None
        raise
    if profile:
        return profile
    return None


def upsert_profile_basic_info(
    *,
    provider,
    user_id: str,
    email: str | None,
    full_name: str | None,
) -> None:
    normalized_email = _as_non_empty_text(email)
    normalized_full_name = _as_non_empty_text(full_name)
    existing = select_profile_for_user(provider=provider, user_id=user_id)

    patch: dict[str, object] = {"updated_at": provider.now_iso()}
    if normalized_email is not None:
        patch["email"] = normalized_email
    if normalized_full_name is not None:
        patch["full_name"] = normalized_full_name

    if existing:
        existing_id = _as_non_empty_text(existing.get("id"))
        if existing_id is not None:
            provider.patch_rows(
                table="profiles",
                filters={"id": existing_id},
                patch=patch,
            )
            return
        provider.patch_rows(
            table="profiles",
            filters={"user_id": user_id},
            patch=patch,
        )
        return

    if normalized_email is None:
        raise ValueError("Cannot create profile without email. Ask the user to save their profile once after login.")

    row: dict[str, object] = {"user_id": user_id}
    if normalized_email is not None:
        row["email"] = normalized_email
    if normalized_full_name is not None:
        row["full_name"] = normalized_full_name

    try:
        provider.insert_row(table="profiles", row=row)
    except SupabaseClientError as exc:
        if not _is_missing_column_error(exc, table="profiles", column="user_id"):
            raise

        fallback_row: dict[str, object] = {"id": user_id}
        if normalized_email is not None:
            fallback_row["email"] = normalized_email
        if normalized_full_name is not None:
            fallback_row["full_name"] = normalized_full_name
        provider.insert_row(table="profiles", row=fallback_row)
