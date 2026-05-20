from __future__ import annotations

import json
import re

from shared.db import execute_many, fetch_all, run_transaction
from shared.normalization import (
    normalize_compact_token as _normalize_compact_token,
    normalize_input_text as _normalize_input_text,
    normalize_optional_input_text as _normalize_optional_input_text,
)
from shared.tenantDataCache import (
    delete_tenant_shared_cache_values_by_prefix,
    get_tenant_shared_cache_value,
    set_tenant_shared_cache_value,
)

from apps.tradsphere.api.v1.helpers.config import (
    get_db_read_cache_ttl_seconds,
    get_db_tables,
)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
_DB_READ_CACHE_BUCKET = "db_reads"
_DB_READ_CACHE_PREFIX = "tradsphere_db_reads::"
_INV_CHECKLIST_READ_CACHE_PREFIX = _DB_READ_CACHE_PREFIX + "inv_checklist_"
_INV_CHECKLIST_NOTE_DETAIL_CACHE_SCOPE = "inv_checklist_note_detail"
_INV_CHECKLIST_NOTE_DETAIL_CACHE_PREFIX = _DB_READ_CACHE_PREFIX + _INV_CHECKLIST_NOTE_DETAIL_CACHE_SCOPE + "::"
_SCHEDULE_EXISTS_CACHE_BUCKET = "db_reads"
_SCHEDULE_EXISTS_CACHE_PREFIX = "tradsphere_validation::schedule_has_estnum::"
_PDF_SCHEDULE_CACHE_PREFIX = "tradsphere_pdf::schedules_data::"
_EST_NUMS_CREATED_COLUMN_CANDIDATES = (
    "createdAt",
    "created_at",
    "createdDate",
    "created_on",
    "dateCreated",
    "createdOn",
)
_INV_NOTE_ATTACHMENT_OPTIONAL_COLUMNS = (
    "storageProvider",
    "providerAssetId",
    "providerPublicId",
    "providerResourceType",
    "accessUrl",
    "originalFileName",
    "mimeType",
    "fileSize",
    "uploadedBy",
    "tenantSlug",
    "ownerEntityType",
    "ownerEntityId",
    "deletedAt",
)
_APP_ATTACHMENT_OPTIONAL_COLUMNS = (
    "tenantSlug",
    "storageKey",
    "providerMetadata",
    "uploadedBy",
    "deletedAt",
)
_APP_CODE_TRADSPHERE = "tradsphere"
_OWNER_ENTITY_TYPE_INVOICE_CHECKLIST_NOTE = "invoice_checklist_note"


def _quote_identifier(name: str) -> str:
    cleaned = str(name or "").strip()
    if not _IDENTIFIER_RE.fullmatch(cleaned):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return f"`{cleaned}`"


def _quote_table_name(table_name: str) -> str:
    parts = [part.strip() for part in str(table_name or "").split(".") if part.strip()]
    if not parts:
        raise ValueError("Invalid table name")
    return ".".join(_quote_identifier(part) for part in parts)


def _normalize_bool(value: object, *, default: bool = True) -> int:
    if value is None:
        return 1 if default else 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if bool(value) else 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return 1
        if text in {"0", "false", "no", "n", "off"}:
            return 0
    raise ValueError("active must be boolean-like")


def _normalize_account_code(value: object) -> str:
    return _normalize_compact_token(value).upper()


def _normalize_media_type(value: object) -> str:
    return _normalize_compact_token(value).upper()


def _normalize_contact_type(value: object) -> str:
    return _normalize_compact_token(value).upper()


def _normalize_email(value: object) -> str:
    return _normalize_compact_token(value).lower()


def _normalize_phone_search(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return "".join(ch for ch in text if ch.isdigit())


def _build_phone_match_sql(column: str) -> str:
    return (
        "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
        f"LOWER(COALESCE({column}, '')), "
        "' ', ''), "
        "'(', ''), "
        "')', ''), "
        "'-', ''), "
        "'.', ''), "
        "'+', ''), "
        "'/', '')"
    )


def _build_in_placeholders(values: list[object]) -> str:
    if not values:
        raise ValueError("Cannot build IN placeholder for empty values")
    return ", ".join(["%s"] * len(values))


def _cache_ttl_seconds(*, ttl_key: str) -> int:
    return max(int(get_db_read_cache_ttl_seconds(key=ttl_key)), 0)


def _normalized_text_cache_values(values: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return sorted(normalized)


def _normalized_int_cache_values(values: list[int]) -> list[int]:
    seen: set[int] = set()
    normalized: list[int] = []
    for value in values:
        parsed = int(value)
        if parsed in seen:
            continue
        seen.add(parsed)
        normalized.append(parsed)
    return sorted(normalized)


def _build_db_read_cache_key(
    cache_scope: str,
    *parts: str,
) -> str:
    cleaned_parts = [str(part or "").strip() for part in parts]
    return _DB_READ_CACHE_PREFIX + str(cache_scope).strip() + "::" + "::".join(cleaned_parts)


def _get_cached_list(
    cache_key: str,
    *,
    ttl_key: str,
) -> list[dict] | None:
    cached_value, cache_hit = get_tenant_shared_cache_value(
        bucket=_DB_READ_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_cache_ttl_seconds(ttl_key=ttl_key),
    )
    if cache_hit and isinstance(cached_value, list):
        return cached_value
    return None


def _get_cached_dict(
    cache_key: str,
    *,
    ttl_key: str,
) -> dict | None:
    cached_value, cache_hit = get_tenant_shared_cache_value(
        bucket=_DB_READ_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_cache_ttl_seconds(ttl_key=ttl_key),
    )
    if cache_hit and isinstance(cached_value, dict):
        return cached_value
    return None


def _set_cached_value(cache_key: str, value: object) -> None:
    set_tenant_shared_cache_value(
        bucket=_DB_READ_CACHE_BUCKET,
        cache_key=cache_key,
        value=value,
    )


def _build_db_read_cache_prefix(cache_scope: str) -> str:
    return _DB_READ_CACHE_PREFIX + str(cache_scope).strip() + "::"


def _invalidate_db_read_cache_scopes(*cache_scopes: str) -> int:
    removed = 0
    for cache_scope in cache_scopes:
        scope = str(cache_scope or "").strip()
        if not scope:
            continue
        removed += int(
            delete_tenant_shared_cache_values_by_prefix(
                bucket=_DB_READ_CACHE_BUCKET,
                cache_key_prefix=_build_db_read_cache_prefix(scope),
            )
            or 0
        )
    return removed


def _invalidate_schedule_exists_cache(*, est_nums: list[int] | None = None) -> int:
    normalized = _normalized_int_cache_values([int(item) for item in (est_nums or [])])
    if not normalized:
        return int(
            delete_tenant_shared_cache_values_by_prefix(
                bucket=_SCHEDULE_EXISTS_CACHE_BUCKET,
                cache_key_prefix=_SCHEDULE_EXISTS_CACHE_PREFIX,
            )
            or 0
        )

    removed = 0
    for est_num in normalized:
        removed += int(
            delete_tenant_shared_cache_values_by_prefix(
                bucket=_SCHEDULE_EXISTS_CACHE_BUCKET,
                cache_key_prefix=f"{_SCHEDULE_EXISTS_CACHE_PREFIX}{est_num}",
            )
            or 0
        )
    return removed


def _invalidate_pdf_schedule_cache() -> int:
    return int(
        delete_tenant_shared_cache_values_by_prefix(
            bucket=_DB_READ_CACHE_BUCKET,
            cache_key_prefix=_PDF_SCHEDULE_CACHE_PREFIX,
        )
        or 0
    )


def _invalidate_accounts_related_cache() -> None:
    _invalidate_db_read_cache_scopes(
        "accounts",
        "accounts_directory",
        "invoice_checklist_expected_rows",
    )


def _invalidate_est_nums_related_cache() -> None:
    _invalidate_db_read_cache_scopes(
        "est_nums",
        "est_nums_search",
        "stations",
        "station_account_codes",
        "schedule_timeline",
        "invoice_checklist_expected_rows",
    )
    _invalidate_pdf_schedule_cache()


def _invalidate_schedules_related_cache(*, est_nums: list[int] | None = None) -> None:
    _invalidate_db_read_cache_scopes(
        "schedules",
        "schedule_weeks",
        "schedule_timeline",
        "stations",
        "station_account_codes",
        "invoice_checklist_expected_rows",
    )
    _invalidate_schedule_exists_cache(est_nums=est_nums)
    _invalidate_pdf_schedule_cache()


def _invalidate_schedule_weeks_related_cache() -> None:
    _invalidate_db_read_cache_scopes(
        "schedule_weeks",
        "schedule_timeline",
    )
    _invalidate_pdf_schedule_cache()


def _invalidate_delivery_methods_related_cache() -> None:
    _invalidate_db_read_cache_scopes(
        "delivery_methods",
        "stations",
        "station_detail",
    )


def _invalidate_stations_related_cache() -> None:
    _invalidate_db_read_cache_scopes(
        "stations",
        "station_media_types",
        "station_detail",
        "station_contacts_detail",
        "stations_contacts",
        "contacts",
        "contacts_by_station_codes",
    )
    _invalidate_pdf_schedule_cache()


def _invalidate_contacts_related_cache() -> None:
    _invalidate_db_read_cache_scopes(
        "contacts",
        "contacts_selector",
        "contacts_by_station_codes",
        "station_contacts_detail",
    )


def _invalidate_stations_contacts_related_cache() -> None:
    _invalidate_db_read_cache_scopes(
        "stations_contacts",
        "contacts",
        "contacts_by_station_codes",
        "station_contacts_detail",
    )


def _invalidate_inv_checklist_note_detail_cache() -> None:
    delete_tenant_shared_cache_values_by_prefix(
        bucket=_DB_READ_CACHE_BUCKET,
        cache_key_prefix=_INV_CHECKLIST_NOTE_DETAIL_CACHE_PREFIX,
    )


def _invalidate_inv_checklist_list_cache() -> int:
    return _invalidate_db_read_cache_scopes("inv_checklists")


def _invalidate_inv_checklist_row_cache(*, checklist_ids: list[str] | None = None) -> int:
    normalized_ids = _normalized_text_cache_values(
        [str(item or "").strip() for item in (checklist_ids or [])]
    )
    if not normalized_ids:
        return _invalidate_db_read_cache_scopes("inv_checklist_row")

    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    removed = 0
    for checklist_id in normalized_ids:
        removed += int(
            delete_tenant_shared_cache_values_by_prefix(
                bucket=_DB_READ_CACHE_BUCKET,
                cache_key_prefix=_build_db_read_cache_key(
                    "inv_checklist_row",
                    "schema=v1",
                    f"table={checklist_table}",
                    f"checklist_id={checklist_id}",
                ),
            )
            or 0
        )
    return removed


def _invalidate_inv_checklist_station_scopes(
    *,
    include_checklists_scope: bool,
) -> int:
    scopes: list[str] = [
        "inv_checklist_stations",
        "inv_checklist_station_rows",
        "inv_checklist_station_search",
    ]
    if include_checklists_scope:
        scopes.insert(0, "inv_checklists")
    return _invalidate_db_read_cache_scopes(*scopes)


def _invalidate_inv_checklist_all_related_scopes() -> int:
    return int(
        delete_tenant_shared_cache_values_by_prefix(
            bucket=_DB_READ_CACHE_BUCKET,
            cache_key_prefix=_INV_CHECKLIST_READ_CACHE_PREFIX,
        )
        or 0
    )


def invalidate_inv_checklist_related_cache_for_bulk_write() -> int:
    return _invalidate_inv_checklist_all_related_scopes()


def _get_table_columns(
    *,
    table_name_quoted: str,
) -> list[str]:
    cache_key = _build_db_read_cache_key(
        "table_columns",
        f"table={table_name_quoted}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_table_columns_ttl_time",
    )
    if cached_rows is not None:
        normalized_cached: list[str] = []
        for row in cached_rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or "").strip()
            if name:
                normalized_cached.append(name)
        if normalized_cached:
            return normalized_cached

    rows = fetch_all(f"SHOW COLUMNS FROM {table_name_quoted}")
    normalized: list[str] = []
    for row in rows:
        field = str(row.get("Field") or "").strip()
        if field:
            normalized.append(field)

    _set_cached_value(
        cache_key,
        [{"name": name} for name in normalized],
    )
    return normalized


def resolve_est_nums_created_column() -> str | None:
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    columns = _get_table_columns(table_name_quoted=est_nums_table)
    if not columns:
        return None

    by_lower = {str(column).strip().lower(): str(column).strip() for column in columns}
    for candidate in _EST_NUMS_CREATED_COLUMN_CANDIDATES:
        resolved = by_lower.get(candidate.lower())
        if resolved:
            return resolved
    return None


def _get_inv_note_attachment_columns(
    *,
    attachment_table: str,
) -> set[str]:
    try:
        columns = _get_table_columns(table_name_quoted=attachment_table)
        normalized = {str(column).strip() for column in columns if str(column).strip()}
        if normalized:
            return normalized
    except Exception:
        pass

    # Safe fallback for local/unit-test contexts without live DB metadata.
    return {
        "id",
        "noteId",
        "url",
        "fileName",
        "fileType",
        "dateCreated",
        "dateUpdated",
    }


def _has_column(columns: set[str], column_name: str) -> bool:
    return str(column_name).strip() in columns


def _column_or_null(*, alias: str, column_name: str, output_alias: str, columns: set[str]) -> str:
    if _has_column(columns, column_name):
        return f"{alias}.{column_name} AS {output_alias}"
    return f"NULL AS {output_alias}"


def _inv_note_attachment_select_clause(*, alias: str, columns: set[str]) -> list[str]:
    access_url_expression = (
        f"COALESCE({alias}.accessUrl, {alias}.url)"
        if _has_column(columns, "accessUrl")
        else f"{alias}.url"
    )
    original_file_name_expression = (
        f"COALESCE({alias}.originalFileName, {alias}.fileName)"
        if _has_column(columns, "originalFileName")
        else f"{alias}.fileName"
    )
    mime_type_expression = (
        f"COALESCE({alias}.mimeType, {alias}.fileType)"
        if _has_column(columns, "mimeType")
        else f"{alias}.fileType"
    )
    file_size_expression = (
        f"{alias}.fileSize"
        if _has_column(columns, "fileSize")
        else "NULL"
    )
    storage_key_expression = (
        f"{alias}.providerPublicId"
        if _has_column(columns, "providerPublicId")
        else "NULL"
    )
    return [
        f"{alias}.id AS attachmentId",
        f"{alias}.noteId AS attachmentNoteId",
        f"{alias}.url AS attachmentUrl",
        f"{alias}.fileName AS attachmentFileName",
        f"{alias}.fileType AS attachmentFileType",
        f"{access_url_expression} AS attachmentAccessUrl",
        f"{original_file_name_expression} AS attachmentOriginalFileName",
        f"{mime_type_expression} AS attachmentMimeType",
        f"{file_size_expression} AS attachmentFileSize",
        f"{storage_key_expression} AS attachmentStorageKey",
        "NULL AS attachmentProviderMetadata",
        _column_or_null(
            alias=alias,
            column_name="storageProvider",
            output_alias="attachmentStorageProvider",
            columns=columns,
        ),
        _column_or_null(
            alias=alias,
            column_name="providerAssetId",
            output_alias="attachmentProviderAssetId",
            columns=columns,
        ),
        _column_or_null(
            alias=alias,
            column_name="providerPublicId",
            output_alias="attachmentProviderPublicId",
            columns=columns,
        ),
        _column_or_null(
            alias=alias,
            column_name="providerResourceType",
            output_alias="attachmentProviderResourceType",
            columns=columns,
        ),
        _column_or_null(
            alias=alias,
            column_name="uploadedBy",
            output_alias="attachmentUploadedBy",
            columns=columns,
        ),
        _column_or_null(
            alias=alias,
            column_name="tenantSlug",
            output_alias="attachmentTenantSlug",
            columns=columns,
        ),
        _column_or_null(
            alias=alias,
            column_name="ownerEntityType",
            output_alias="attachmentOwnerEntityType",
            columns=columns,
        ),
        _column_or_null(
            alias=alias,
            column_name="ownerEntityId",
            output_alias="attachmentOwnerEntityId",
            columns=columns,
        ),
        _column_or_null(
            alias=alias,
            column_name="deletedAt",
            output_alias="attachmentDeletedAt",
            columns=columns,
        ),
        f"{alias}.dateCreated AS attachmentDateCreated",
        f"{alias}.dateUpdated AS attachmentDateUpdated",
        "'legacy_inv_note_attachment' AS attachmentSource",
    ]


def _get_app_attachment_table_name(*, tables: dict[str, str]) -> str:
    return _quote_table_name(str(tables.get("APPATTACHMENTS") or "AppAttachment"))


def _get_app_attachment_columns(
    *,
    app_attachment_table: str,
) -> set[str]:
    try:
        columns = _get_table_columns(table_name_quoted=app_attachment_table)
        normalized = {str(column).strip() for column in columns if str(column).strip()}
        if normalized:
            return normalized
    except Exception:
        return set()
    return set()


def _app_attachment_select_clause(*, alias: str, columns: set[str]) -> list[str]:
    note_id_expression = (
        f"CASE WHEN {alias}.ownerEntityId REGEXP '^[0-9]+$' THEN CAST({alias}.ownerEntityId AS UNSIGNED) ELSE NULL END"
    )
    tenant_slug_expression = (
        f"{alias}.tenantSlug"
        if _has_column(columns, "tenantSlug")
        else "NULL"
    )
    storage_key_expression = (
        f"{alias}.storageKey"
        if _has_column(columns, "storageKey")
        else "NULL"
    )
    provider_metadata_expression = (
        f"{alias}.providerMetadata"
        if _has_column(columns, "providerMetadata")
        else "NULL"
    )
    uploaded_by_expression = (
        f"{alias}.uploadedBy"
        if _has_column(columns, "uploadedBy")
        else "NULL"
    )
    deleted_at_expression = (
        f"{alias}.deletedAt"
        if _has_column(columns, "deletedAt")
        else "NULL"
    )
    return [
        f"{alias}.id AS attachmentId",
        f"{note_id_expression} AS attachmentNoteId",
        f"{alias}.accessUrl AS attachmentUrl",
        f"{alias}.originalFileName AS attachmentFileName",
        f"{alias}.mimeType AS attachmentFileType",
        f"{alias}.accessUrl AS attachmentAccessUrl",
        f"{alias}.originalFileName AS attachmentOriginalFileName",
        f"{alias}.mimeType AS attachmentMimeType",
        f"{alias}.fileSize AS attachmentFileSize",
        f"{storage_key_expression} AS attachmentStorageKey",
        f"{provider_metadata_expression} AS attachmentProviderMetadata",
        f"{alias}.storageProvider AS attachmentStorageProvider",
        "NULL AS attachmentProviderAssetId",
        f"{storage_key_expression} AS attachmentProviderPublicId",
        "NULL AS attachmentProviderResourceType",
        f"{uploaded_by_expression} AS attachmentUploadedBy",
        f"{tenant_slug_expression} AS attachmentTenantSlug",
        f"{alias}.ownerEntityType AS attachmentOwnerEntityType",
        f"{alias}.ownerEntityId AS attachmentOwnerEntityId",
        f"{deleted_at_expression} AS attachmentDeletedAt",
        f"{alias}.dateCreated AS attachmentDateCreated",
        f"{alias}.dateUpdated AS attachmentDateUpdated",
        "'app_attachment' AS attachmentSource",
    ]


def _append_inv_note_attachment_ownership_clauses(
    *,
    where_clauses: list[str],
    params: list[object],
    alias: str,
    note_alias: str,
    columns: set[str],
    tenant_slug: str | None,
) -> None:
    if _has_column(columns, "deletedAt"):
        where_clauses.append(f"{alias}.deletedAt IS NULL")
    if _has_column(columns, "ownerEntityType"):
        where_clauses.append(
            "("
            f"{alias}.ownerEntityType IS NULL "
            f"OR {alias}.ownerEntityType = '' "
            f"OR {alias}.ownerEntityType = 'invoice_checklist_note'"
            ")"
        )
    if _has_column(columns, "ownerEntityId"):
        where_clauses.append(
            "("
            f"{alias}.ownerEntityId IS NULL "
            f"OR {alias}.ownerEntityId = '' "
            f"OR {alias}.ownerEntityId = CAST({note_alias}.id AS CHAR)"
            ")"
        )

    normalized_tenant_slug = str(tenant_slug or "").strip().lower()
    if _has_column(columns, "tenantSlug") and normalized_tenant_slug:
        where_clauses.append(f"LOWER({alias}.tenantSlug) = %s")
        params.append(normalized_tenant_slug)


def get_accounts(
    *,
    account_codes: list[str] | None = None,
    active_only: bool = False,
) -> list[dict]:
    tables = get_db_tables()
    accounts_table = _quote_table_name(tables["ACCOUNTS"])
    master_accounts_table = _quote_table_name(tables["MASTERACCOUNTS"])

    where_clauses: list[str] = []
    params: list[object] = []

    normalized_codes = [_normalize_account_code(code) for code in (account_codes or [])]
    normalized_codes = _normalized_text_cache_values(
        [code for code in normalized_codes if code]
    )
    if normalized_codes:
        placeholders = _build_in_placeholders(normalized_codes)
        where_clauses.append(f"UPPER(t.accountCode) IN ({placeholders})")
        params.extend(normalized_codes)

    if active_only:
        where_clauses.append("COALESCE(m.active, 0) = 1")

    cache_key = _build_db_read_cache_key(
        "accounts",
        f"accounts_table={accounts_table}",
        f"master_accounts_table={master_accounts_table}",
        f"active_only={int(bool(active_only))}",
        "account_codes=" + (",".join(normalized_codes) if normalized_codes else "*"),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_accounts_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT DISTINCT "
        "t.accountCode AS accountCode, "
        "t.billingType AS billingType, "
        "t.market AS market, "
        "t.note AS note, "
        "m.name AS name, "
        "m.logoUrl AS logoUrl, "
        "COALESCE(m.active, 0) AS active "
        f"FROM {accounts_table} t "
        f"LEFT JOIN {master_accounts_table} m "
        "ON UPPER(m.code) = UPPER(t.accountCode)"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY t.accountCode ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def get_accounts_directory(
    *,
    account_codes: list[str] | None = None,
    active_only: bool = False,
) -> list[dict]:
    tables = get_db_tables()
    accounts_table = _quote_table_name(tables["ACCOUNTS"])
    master_accounts_table = _quote_table_name(tables["MASTERACCOUNTS"])

    where_clauses: list[str] = []
    params: list[object] = []

    normalized_codes = [_normalize_account_code(code) for code in (account_codes or [])]
    normalized_codes = _normalized_text_cache_values(
        [code for code in normalized_codes if code]
    )
    if normalized_codes:
        placeholders = _build_in_placeholders(normalized_codes)
        where_clauses.append(f"UPPER(t.accountCode) IN ({placeholders})")
        params.extend(normalized_codes)

    if active_only:
        where_clauses.append("COALESCE(m.active, 0) = 1")

    cache_key = _build_db_read_cache_key(
        "accounts_directory",
        f"accounts_table={accounts_table}",
        f"master_accounts_table={master_accounts_table}",
        f"active_only={int(bool(active_only))}",
        "account_codes=" + (",".join(normalized_codes) if normalized_codes else "*"),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_accounts_directory_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT DISTINCT "
        "t.accountCode AS accountCode, "
        "m.name AS accountName, "
        "t.billingType AS billingType, "
        "COALESCE(m.active, 0) AS active "
        f"FROM {accounts_table} t "
        f"LEFT JOIN {master_accounts_table} m "
        "ON UPPER(m.code) = UPPER(t.accountCode)"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY t.accountCode ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def insert_accounts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    accounts_table = _quote_table_name(tables["ACCOUNTS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        account_code = _normalize_account_code(item.get("accountCode"))
        if not account_code:
            raise ValueError("accountCode is required")
        billing_type = _normalize_input_text(item.get("billingType") or "Calendar") or "Calendar"
        market = _normalize_optional_input_text(item.get("market"))
        note = _normalize_optional_input_text(item.get("note"))
        values.append((account_code, billing_type, market, note))

    query = (
        f"INSERT INTO {accounts_table} (accountCode, billingType, market, note) "
        "VALUES (%s, %s, %s, %s)"
    )
    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_accounts_related_cache()
    return inserted


def update_accounts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    accounts_table = _quote_table_name(tables["ACCOUNTS"])

    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        account_code = _normalize_account_code(item.get("accountCode"))
        if not account_code:
            raise ValueError("accountCode is required for update")

        fields: list[str] = []
        params: list[object] = []

        if "billingType" in item:
            billing_type = _normalize_input_text(item.get("billingType"))
            if not billing_type:
                raise ValueError("billingType cannot be empty")
            fields.append("billingType = %s")
            params.append(billing_type)

        if "market" in item:
            market = _normalize_optional_input_text(item.get("market"))
            fields.append("market = %s")
            params.append(market)

        if "note" in item:
            note = _normalize_optional_input_text(item.get("note"))
            fields.append("note = %s")
            params.append(note)

        if not fields:
            raise ValueError(
                f"No updatable fields provided for accountCode '{account_code}'"
            )

        params.append(account_code)
        query = f"UPDATE {accounts_table} SET " + ", ".join(fields) + " WHERE accountCode = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    updated = run_transaction(_work)
    if int(updated or 0) > 0:
        _invalidate_accounts_related_cache()
    return updated


def get_est_nums(
    *,
    est_nums: list[int] | None = None,
    account_codes: list[str] | None = None,
) -> list[dict]:
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])

    where_clauses: list[str] = []
    params: list[object] = []

    normalized_est_nums = _normalized_int_cache_values(
        [int(item) for item in (est_nums or [])]
    )
    if normalized_est_nums:
        placeholders = _build_in_placeholders(normalized_est_nums)
        where_clauses.append(f"estNum IN ({placeholders})")
        params.extend(normalized_est_nums)

    normalized_account_codes = _normalized_text_cache_values([
        _normalize_account_code(item) for item in (account_codes or [])
    ])
    normalized_account_codes = [item for item in normalized_account_codes if item]
    if normalized_account_codes:
        placeholders = _build_in_placeholders(normalized_account_codes)
        where_clauses.append(f"UPPER(accountCode) IN ({placeholders})")
        params.extend(normalized_account_codes)

    cache_key = _build_db_read_cache_key(
        "est_nums",
        f"est_nums_table={est_nums_table}",
        "est_nums=" + (",".join(map(str, normalized_est_nums)) if normalized_est_nums else "*"),
        "account_codes="
        + (",".join(normalized_account_codes) if normalized_account_codes else "*"),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_est_nums_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT estNum, accountCode, flightStart, flightEnd, mediaType, buyer, note "
        f"FROM {est_nums_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY estNum ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def search_est_nums(
    *,
    query: str | None,
    limit: int,
    offset: int,
    created_from: str | None = None,
    created_to: str | None = None,
) -> dict[str, object]:
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    master_accounts_table = _quote_table_name(tables["MASTERACCOUNTS"])
    created_column = resolve_est_nums_created_column()

    normalized_query = str(query or "").strip()
    normalized_query_upper = normalized_query.upper()
    has_query = bool(normalized_query)

    where_clauses: list[str] = []
    params: list[object] = []

    if has_query:
        like_value = f"%{normalized_query_upper}%"
        where_clauses.append(
            "("
            "CAST(en.estNum AS CHAR) LIKE %s "
            "OR UPPER(en.accountCode) LIKE %s "
            "OR UPPER(COALESCE(ma.name, '')) LIKE %s "
            "OR UPPER(COALESCE(en.buyer, '')) LIKE %s "
            "OR UPPER(COALESCE(en.mediaType, '')) LIKE %s "
            "OR UPPER(COALESCE(en.note, '')) LIKE %s "
            "OR DATE_FORMAT(en.flightStart, '%%Y-%%m-%%d') LIKE %s "
            "OR DATE_FORMAT(en.flightEnd, '%%Y-%%m-%%d') LIKE %s"
            ")"
        )
        params.extend([like_value] * 8)

    if (created_from or created_to) and not created_column:
        raise ValueError(
            "EstNum created-date filtering is unavailable because no created timestamp column was found. "
            "Expected one of: createdAt, created_at, createdDate, created_on, dateCreated, createdOn."
        )

    if created_from and created_column:
        where_clauses.append(f"DATE(en.{_quote_identifier(created_column)}) >= %s")
        params.append(created_from)
    if created_to and created_column:
        where_clauses.append(f"DATE(en.{_quote_identifier(created_column)}) <= %s")
        params.append(created_to)

    if not where_clauses:
        raise ValueError("At least one search filter is required")

    where_sql = " WHERE " + " AND ".join(where_clauses)
    query_suffix_parts = [
        f"q={normalized_query_upper or '*'}",
        f"created_from={created_from or '*'}",
        f"created_to={created_to or '*'}",
        f"limit={int(limit)}",
        f"offset={int(offset)}",
        f"created_column={created_column or 'none'}",
    ]
    cache_key = _build_db_read_cache_key(
        "est_nums_search",
        f"est_nums_table={est_nums_table}",
        f"master_accounts_table={master_accounts_table}",
        *query_suffix_parts,
    )
    cached = _get_cached_dict(
        cache_key,
        ttl_key="db_est_nums_search_ttl_time",
    )
    if cached is not None:
        cached_items = cached.get("items")
        cached_total = cached.get("total")
        if isinstance(cached_items, list):
            try:
                normalized_total = int(cached_total or 0)
            except (TypeError, ValueError):
                normalized_total = 0
            return {
                "items": cached_items,
                "total": normalized_total,
                "createdColumn": created_column,
            }

    from_sql = (
        f" FROM {est_nums_table} en "
        f"LEFT JOIN {master_accounts_table} ma ON UPPER(ma.code) = UPPER(en.accountCode)"
    )
    count_query = "SELECT COUNT(*) AS total" + from_sql + where_sql
    count_rows = fetch_all(count_query, tuple(params))
    total = 0
    if count_rows:
        try:
            total = int(count_rows[0].get("total") or 0)
        except (TypeError, ValueError):
            total = 0

    created_select = ""
    if created_column:
        created_select = f", en.{_quote_identifier(created_column)} AS createdAt"
    select_query = (
        "SELECT "
        "en.estNum AS estNum, "
        "en.accountCode AS accountCode, "
        "en.flightStart AS flightStart, "
        "en.flightEnd AS flightEnd, "
        "en.mediaType AS mediaType, "
        "en.buyer AS buyer, "
        "en.note AS note, "
        "ma.name AS accountName"
        + created_select
        + from_sql
        + where_sql
        + " ORDER BY en.estNum DESC"
        + " LIMIT %s OFFSET %s"
    )
    rows = fetch_all(select_query, tuple([*params, int(limit), int(offset)]))
    _set_cached_value(
        cache_key,
        {
            "items": rows,
            "total": total,
        },
    )
    return {
        "items": rows,
        "total": total,
        "createdColumn": created_column,
    }


def get_scheduled_est_nums(est_nums: list[int]) -> set[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for item in est_nums or []:
        value = int(item)
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    if not normalized:
        return set()

    ttl_seconds = max(
        int(get_db_read_cache_ttl_seconds(key="db_schedule_exists_ttl_time")),
        0,
    )
    matched: set[int] = set()
    cache_misses: list[int] = []

    for est_num in normalized:
        cache_key = f"{_SCHEDULE_EXISTS_CACHE_PREFIX}{est_num}"
        cached_value, cache_hit = get_tenant_shared_cache_value(
            bucket=_SCHEDULE_EXISTS_CACHE_BUCKET,
            cache_key=cache_key,
            ttl_seconds=ttl_seconds,
        )
        if not cache_hit:
            cache_misses.append(est_num)
            continue

        exists = False
        if isinstance(cached_value, bool):
            exists = cached_value
        elif isinstance(cached_value, (int, float)):
            exists = bool(cached_value)
        elif isinstance(cached_value, str):
            exists = cached_value.strip().lower() in {"1", "true", "yes", "y", "on"}

        if exists:
            matched.add(est_num)

    if not cache_misses:
        return matched

    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    chunk_size = 500
    found_in_db: set[int] = set()

    for start in range(0, len(cache_misses), chunk_size):
        chunk = cache_misses[start : start + chunk_size]
        placeholders = _build_in_placeholders(chunk)
        query = (
            "SELECT DISTINCT estNum "
            f"FROM {schedules_table} "
            f"WHERE estNum IN ({placeholders})"
        )
        rows = fetch_all(query, tuple(chunk))
        for row in rows:
            raw_value = row.get("estNum")
            if raw_value is None:
                continue
            found_in_db.add(int(raw_value))

    for est_num in cache_misses:
        exists = est_num in found_in_db
        set_tenant_shared_cache_value(
            bucket=_SCHEDULE_EXISTS_CACHE_BUCKET,
            cache_key=f"{_SCHEDULE_EXISTS_CACHE_PREFIX}{est_num}",
            value=exists,
        )
        if exists:
            matched.add(est_num)

    return matched


def insert_est_nums(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        est_num_raw = item.get("estNum")
        if est_num_raw is None:
            raise ValueError("estNum is required")
        est_num = int(est_num_raw)
        account_code = _normalize_account_code(item.get("accountCode"))
        if not account_code:
            raise ValueError("accountCode is required")
        flight_start = _normalize_input_text(item.get("flightStart"))
        if not flight_start:
            raise ValueError("flightStart is required")
        flight_end = _normalize_input_text(item.get("flightEnd"))
        if not flight_end:
            raise ValueError("flightEnd is required")
        media_type = _normalize_media_type(item.get("mediaType"))
        if not media_type:
            raise ValueError("mediaType is required")
        buyer = _normalize_input_text(item.get("buyer"))
        if not buyer:
            raise ValueError("buyer is required")
        note = _normalize_optional_input_text(item.get("note"))
        values.append((est_num, account_code, flight_start, flight_end, media_type, buyer, note))

    query = (
        f"INSERT INTO {est_nums_table} (estNum, accountCode, flightStart, flightEnd, mediaType, buyer, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )
    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_est_nums_related_cache()
    return inserted


def update_est_nums(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        est_num_raw = item.get("estNum")
        if est_num_raw is None:
            raise ValueError("estNum is required for update")
        est_num = int(est_num_raw)

        fields: list[str] = []
        params: list[object] = []

        if "accountCode" in item:
            account_code = _normalize_account_code(item.get("accountCode"))
            if not account_code:
                raise ValueError("accountCode cannot be empty")
            fields.append("accountCode = %s")
            params.append(account_code)

        if "mediaType" in item:
            media_type = _normalize_media_type(item.get("mediaType"))
            if not media_type:
                raise ValueError("mediaType cannot be empty")
            fields.append("mediaType = %s")
            params.append(media_type)

        if "flightStart" in item:
            flight_start = _normalize_input_text(item.get("flightStart"))
            if not flight_start:
                raise ValueError("flightStart cannot be empty")
            fields.append("flightStart = %s")
            params.append(flight_start)

        if "flightEnd" in item:
            flight_end = _normalize_input_text(item.get("flightEnd"))
            if not flight_end:
                raise ValueError("flightEnd cannot be empty")
            fields.append("flightEnd = %s")
            params.append(flight_end)

        if "buyer" in item:
            buyer = _normalize_input_text(item.get("buyer"))
            if not buyer:
                raise ValueError("buyer cannot be empty")
            fields.append("buyer = %s")
            params.append(buyer)

        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))

        if not fields:
            raise ValueError(f"No updatable fields provided for estNum '{est_num}'")

        params.append(est_num)
        query = f"UPDATE {est_nums_table} SET " + ", ".join(fields) + " WHERE estNum = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    updated = run_transaction(_work)
    if int(updated or 0) > 0:
        _invalidate_est_nums_related_cache()
    return updated


def get_schedules(
    *,
    ids: list[str] | None = None,
    schedule_ids: list[str] | None = None,
    est_nums: list[int] | None = None,
    billing_codes: list[str] | None = None,
    media_types: list[str] | None = None,
    station_codes: list[str] | None = None,
    broadcast_month: int | None = None,
    broadcast_year: int | None = None,
    start_date_from: str | None = None,
    start_date_to: str | None = None,
    end_date_from: str | None = None,
    end_date_to: str | None = None,
) -> list[dict]:
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])

    where_clauses: list[str] = []
    params: list[object] = []

    normalized_ids = _normalized_text_cache_values(
        [str(item or "").strip() for item in (ids or [])]
    )
    normalized_ids = [item for item in normalized_ids if item]
    if normalized_ids:
        placeholders = _build_in_placeholders(normalized_ids)
        where_clauses.append(f"id IN ({placeholders})")
        params.extend(normalized_ids)

    normalized_schedule_ids = _normalized_text_cache_values(
        [str(item or "").strip() for item in (schedule_ids or [])]
    )
    normalized_schedule_ids = [item for item in normalized_schedule_ids if item]
    if normalized_schedule_ids:
        placeholders = _build_in_placeholders(normalized_schedule_ids)
        where_clauses.append(f"scheduleId IN ({placeholders})")
        params.extend(normalized_schedule_ids)

    normalized_est_nums = _normalized_int_cache_values(
        [int(item) for item in (est_nums or [])]
    )
    if normalized_est_nums:
        placeholders = _build_in_placeholders(normalized_est_nums)
        where_clauses.append(f"estNum IN ({placeholders})")
        params.extend(normalized_est_nums)

    normalized_billing_codes = _normalized_text_cache_values(
        [str(item or "").strip() for item in (billing_codes or [])]
    )
    normalized_billing_codes = [item for item in normalized_billing_codes if item]
    if normalized_billing_codes:
        placeholders = _build_in_placeholders(normalized_billing_codes)
        where_clauses.append(f"billingCode IN ({placeholders})")
        params.extend(normalized_billing_codes)

    normalized_media_types = _normalized_text_cache_values(
        [_normalize_media_type(item) for item in (media_types or [])]
    )
    normalized_media_types = [item for item in normalized_media_types if item]
    if normalized_media_types:
        placeholders = _build_in_placeholders(normalized_media_types)
        where_clauses.append(f"UPPER(mediaType) IN ({placeholders})")
        params.extend(normalized_media_types)

    normalized_station_codes = _normalized_text_cache_values([
        _normalize_account_code(item) for item in (station_codes or [])
    ])
    normalized_station_codes = [item for item in normalized_station_codes if item]
    if normalized_station_codes:
        placeholders = _build_in_placeholders(normalized_station_codes)
        where_clauses.append(f"UPPER(stationCode) IN ({placeholders})")
        params.extend(normalized_station_codes)

    if broadcast_month is not None:
        where_clauses.append("broadcastMonth = %s")
        params.append(int(broadcast_month))

    if broadcast_year is not None:
        where_clauses.append("broadcastYear = %s")
        params.append(int(broadcast_year))

    if start_date_from is not None:
        where_clauses.append("startDate >= %s")
        params.append(str(start_date_from))
    if start_date_to is not None:
        where_clauses.append("startDate <= %s")
        params.append(str(start_date_to))
    if end_date_from is not None:
        where_clauses.append("endDate >= %s")
        params.append(str(end_date_from))
    if end_date_to is not None:
        where_clauses.append("endDate <= %s")
        params.append(str(end_date_to))

    cache_key = _build_db_read_cache_key(
        "schedules",
        f"schedules_table={schedules_table}",
        "ids=" + (",".join(normalized_ids) if normalized_ids else "*"),
        "schedule_ids=" + (",".join(normalized_schedule_ids) if normalized_schedule_ids else "*"),
        "est_nums=" + (",".join(map(str, normalized_est_nums)) if normalized_est_nums else "*"),
        "billing_codes="
        + (",".join(normalized_billing_codes) if normalized_billing_codes else "*"),
        "media_types=" + (",".join(normalized_media_types) if normalized_media_types else "*"),
        "station_codes="
        + (",".join(normalized_station_codes) if normalized_station_codes else "*"),
        f"broadcast_month={int(broadcast_month) if broadcast_month is not None else '*'}",
        f"broadcast_year={int(broadcast_year) if broadcast_year is not None else '*'}",
        f"start_date_from={str(start_date_from) if start_date_from is not None else '*'}",
        f"start_date_to={str(start_date_to) if start_date_to is not None else '*'}",
        f"end_date_from={str(end_date_from) if end_date_from is not None else '*'}",
        f"end_date_to={str(end_date_to) if end_date_to is not None else '*'}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_schedules_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT "
        "id, scheduleId, lineNum, estNum, billingCode, mediaType, stationCode, "
        "broadcastMonth, broadcastYear, startDate, endDate, totalSpot, totalGross, rateGross, "
        "length, runtime, programName, days, daypart, rtg "
        f"FROM {schedules_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY broadcastYear ASC, broadcastMonth ASC, startDate ASC, id ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def list_schedule_invoice_checklist_expected_rows(
    *,
    broadcast_year: int,
    broadcast_month: int,
) -> list[dict]:
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    tradsphere_accounts_table = _quote_table_name(tables["ACCOUNTS"])

    normalized_broadcast_year = int(broadcast_year)
    normalized_broadcast_month = int(broadcast_month)

    cache_key = _build_db_read_cache_key(
        "invoice_checklist_expected_rows",
        "schema=v1",
        f"schedules_table={schedules_table}",
        f"est_nums_table={est_nums_table}",
        f"tradsphere_accounts_table={tradsphere_accounts_table}",
        f"broadcast_year={normalized_broadcast_year}",
        f"broadcast_month={normalized_broadcast_month}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_schedules_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT DISTINCT "
        "UPPER(TRIM(en.accountCode)) AS accountCode, "
        "s.estNum AS estNum, "
        "UPPER(TRIM(s.stationCode)) AS stationCode "
        f"FROM {schedules_table} s "
        f"INNER JOIN {est_nums_table} en ON en.estNum = s.estNum "
        f"INNER JOIN {tradsphere_accounts_table} ta ON UPPER(ta.accountCode) = UPPER(en.accountCode) "
        "WHERE s.broadcastYear = %s "
        "AND s.broadcastMonth = %s "
        "AND COALESCE(TRIM(en.accountCode), '') <> '' "
        "AND COALESCE(TRIM(s.stationCode), '') <> '' "
        "ORDER BY accountCode ASC, s.estNum ASC, stationCode ASC"
    )
    rows = fetch_all(query, (normalized_broadcast_year, normalized_broadcast_month))
    _set_cached_value(cache_key, rows)
    return rows


def get_schedule_timeline_rows(
    *,
    account_code: str,
    start_date: str,
    end_date: str,
    timezone: str,
) -> list[dict]:
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    schedules_weeks_table = _quote_table_name(tables["SCHEDULESWEEKS"])
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    stations_table = _quote_table_name(tables["STATIONS"])

    normalized_account_code = _normalize_account_code(account_code)
    if not normalized_account_code:
        raise ValueError("accountCode is required")

    normalized_start_date = str(start_date or "").strip()
    normalized_end_date = str(end_date or "").strip()
    if not normalized_start_date or not normalized_end_date:
        raise ValueError("startDate and endDate are required")

    normalized_timezone = str(timezone or "").strip() or "America/Chicago"

    cache_key = _build_db_read_cache_key(
        "schedule_timeline",
        "schema=v2",
        f"schedules_table={schedules_table}",
        f"schedules_weeks_table={schedules_weeks_table}",
        f"est_nums_table={est_nums_table}",
        f"stations_table={stations_table}",
        f"account_code={normalized_account_code}",
        f"start_date={normalized_start_date}",
        f"end_date={normalized_end_date}",
        f"timezone={normalized_timezone}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_schedule_timeline_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    week_start_expr = "DATE_SUB(sw.weekStart, INTERVAL WEEKDAY(sw.weekStart) DAY)"
    query = (
        "SELECT DISTINCT "
        "s.stationCode AS stationCode, "
        "st.name AS stationName, "
        "s.estNum AS estNum, "
        "s.mediaType AS mediaType, "
        f"{week_start_expr} AS weekStart "
        f"FROM {schedules_weeks_table} sw "
        f"INNER JOIN {schedules_table} s ON s.id = sw.scheduleId "
        f"INNER JOIN {est_nums_table} en ON en.estNum = s.estNum "
        f"LEFT JOIN {stations_table} st ON UPPER(st.code) = UPPER(s.stationCode) "
        "WHERE UPPER(en.accountCode) = %s "
        "AND sw.weekEnd >= %s "
        "AND sw.weekStart <= %s "
        "AND COALESCE(sw.spots, 0) > 0 "
        "ORDER BY s.stationCode ASC, s.estNum ASC, weekStart ASC"
    )
    rows = fetch_all(
        query,
        (
            normalized_account_code,
            normalized_start_date,
            normalized_end_date,
        ),
    )
    _set_cached_value(cache_key, rows)
    return rows


def get_schedules_by_match_keys(match_keys: list[str]) -> list[dict]:
    normalized_match_keys = [str(item or "").strip() for item in (match_keys or [])]
    normalized_match_keys = [item for item in normalized_match_keys if item]
    if not normalized_match_keys:
        return []

    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    placeholders = _build_in_placeholders(normalized_match_keys)
    query = (
        "SELECT id, matchKey "
        f"FROM {schedules_table} "
        f"WHERE matchKey IN ({placeholders})"
    )
    return fetch_all(query, tuple(normalized_match_keys))


def insert_schedules(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    values: list[tuple[object, ...]] = []
    for item in items:
        schedule_row_id = _normalize_input_text(item.get("id"))
        if not schedule_row_id:
            raise ValueError("id is required")
        schedule_business_id = _normalize_input_text(item.get("scheduleId"))
        if not schedule_business_id:
            raise ValueError("scheduleId is required")
        line_num = item.get("lineNum")
        if line_num is None:
            raise ValueError("lineNum is required")
        est_num = item.get("estNum")
        if est_num is None:
            raise ValueError("estNum is required")
        billing_code = _normalize_input_text(item.get("billingCode"))
        if not billing_code:
            raise ValueError("billingCode is required")
        media_type = _normalize_media_type(item.get("mediaType"))
        if not media_type:
            raise ValueError("mediaType is required")
        station_code = _normalize_account_code(item.get("stationCode"))
        if not station_code:
            raise ValueError("stationCode is required")
        broadcast_month = item.get("broadcastMonth")
        if broadcast_month is None:
            raise ValueError("broadcastMonth is required")
        broadcast_year = item.get("broadcastYear")
        if broadcast_year is None:
            raise ValueError("broadcastYear is required")
        start_date = _normalize_input_text(item.get("startDate"))
        if not start_date:
            raise ValueError("startDate is required")
        end_date = _normalize_input_text(item.get("endDate"))
        if not end_date:
            raise ValueError("endDate is required")
        total_spot = item.get("totalSpot")
        if total_spot is None:
            raise ValueError("totalSpot is required")
        total_gross = item.get("totalGross")
        if total_gross is None:
            raise ValueError("totalGross is required")
        rate_gross = item.get("rateGross")
        if rate_gross is None:
            raise ValueError("rateGross is required")
        length = item.get("length")
        if length is None:
            raise ValueError("length is required")
        runtime = _normalize_input_text(item.get("runtime"))
        if not runtime:
            raise ValueError("runtime is required")
        days = _normalize_input_text(item.get("days"))
        if not days:
            raise ValueError("days is required")
        daypart = _normalize_input_text(item.get("daypart"))
        if not daypart:
            raise ValueError("daypart is required")
        match_key = _normalize_input_text(item.get("matchKey"))
        if not match_key:
            raise ValueError("matchKey is required")
        values.append(
            (
                schedule_row_id,
                schedule_business_id,
                int(line_num),
                int(est_num),
                billing_code,
                media_type,
                station_code,
                int(broadcast_month),
                int(broadcast_year),
                start_date,
                end_date,
                int(total_spot),
                total_gross,
                rate_gross,
                int(length),
                runtime,
                _normalize_optional_input_text(item.get("programName")),
                days,
                daypart,
                item.get("rtg"),
                match_key,
            )
        )

    query = (
        f"INSERT INTO {schedules_table} "
        "(id, scheduleId, lineNum, estNum, billingCode, mediaType, stationCode, broadcastMonth, broadcastYear, "
        "startDate, endDate, totalSpot, totalGross, rateGross, length, runtime, programName, days, daypart, rtg, matchKey) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "scheduleId = VALUES(scheduleId), "
        "lineNum = VALUES(lineNum), "
        "estNum = VALUES(estNum), "
        "billingCode = VALUES(billingCode), "
        "mediaType = VALUES(mediaType), "
        "stationCode = VALUES(stationCode), "
        "broadcastMonth = VALUES(broadcastMonth), "
        "broadcastYear = VALUES(broadcastYear), "
        "startDate = VALUES(startDate), "
        "endDate = VALUES(endDate), "
        "totalSpot = VALUES(totalSpot), "
        "totalGross = VALUES(totalGross), "
        "rateGross = VALUES(rateGross), "
        "length = VALUES(length), "
        "runtime = VALUES(runtime), "
        "programName = VALUES(programName), "
        "days = VALUES(days), "
        "daypart = VALUES(daypart), "
        "rtg = VALUES(rtg), "
        "matchKey = VALUES(matchKey)"
    )
    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_schedules_related_cache()
    return inserted


def update_schedules(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        schedule_row_id = _normalize_input_text(item.get("id"))
        if not schedule_row_id:
            raise ValueError("id is required for schedules update")
        fields: list[str] = []
        params: list[object] = []

        if "scheduleId" in item:
            schedule_business_id = _normalize_input_text(item.get("scheduleId"))
            if not schedule_business_id:
                raise ValueError("scheduleId cannot be empty")
            fields.append("scheduleId = %s")
            params.append(schedule_business_id)
        if "lineNum" in item:
            line_num = item.get("lineNum")
            if line_num is None:
                raise ValueError("lineNum cannot be null")
            fields.append("lineNum = %s")
            params.append(int(line_num))
        if "estNum" in item:
            est_num = item.get("estNum")
            if est_num is None:
                raise ValueError("estNum cannot be null")
            fields.append("estNum = %s")
            params.append(int(est_num))
        if "billingCode" in item:
            billing_code = _normalize_input_text(item.get("billingCode"))
            if not billing_code:
                raise ValueError("billingCode cannot be empty")
            fields.append("billingCode = %s")
            params.append(billing_code)
        if "mediaType" in item:
            media_type = _normalize_media_type(item.get("mediaType"))
            if not media_type:
                raise ValueError("mediaType cannot be empty")
            fields.append("mediaType = %s")
            params.append(media_type)
        if "stationCode" in item:
            station_code = _normalize_account_code(item.get("stationCode"))
            if not station_code:
                raise ValueError("stationCode cannot be empty")
            fields.append("stationCode = %s")
            params.append(station_code)
        if "broadcastMonth" in item:
            broadcast_month = item.get("broadcastMonth")
            if broadcast_month is None:
                raise ValueError("broadcastMonth cannot be null")
            fields.append("broadcastMonth = %s")
            params.append(int(broadcast_month))
        if "broadcastYear" in item:
            broadcast_year = item.get("broadcastYear")
            if broadcast_year is None:
                raise ValueError("broadcastYear cannot be null")
            fields.append("broadcastYear = %s")
            params.append(int(broadcast_year))
        if "startDate" in item:
            start_date = _normalize_input_text(item.get("startDate"))
            if not start_date:
                raise ValueError("startDate cannot be empty")
            fields.append("startDate = %s")
            params.append(start_date)
        if "endDate" in item:
            end_date = _normalize_input_text(item.get("endDate"))
            if not end_date:
                raise ValueError("endDate cannot be empty")
            fields.append("endDate = %s")
            params.append(end_date)
        if "totalSpot" in item:
            total_spot = item.get("totalSpot")
            if total_spot is None:
                raise ValueError("totalSpot cannot be null")
            fields.append("totalSpot = %s")
            params.append(int(total_spot))
        if "totalGross" in item:
            total_gross = item.get("totalGross")
            if total_gross is None:
                raise ValueError("totalGross cannot be null")
            fields.append("totalGross = %s")
            params.append(total_gross)
        if "rateGross" in item:
            rate_gross = item.get("rateGross")
            if rate_gross is None:
                raise ValueError("rateGross cannot be null")
            fields.append("rateGross = %s")
            params.append(rate_gross)
        if "length" in item:
            length = item.get("length")
            if length is None:
                raise ValueError("length cannot be null")
            fields.append("length = %s")
            params.append(int(length))
        if "runtime" in item:
            runtime = _normalize_input_text(item.get("runtime"))
            if not runtime:
                raise ValueError("runtime cannot be empty")
            fields.append("runtime = %s")
            params.append(runtime)
        if "programName" in item:
            fields.append("programName = %s")
            params.append(_normalize_optional_input_text(item.get("programName")))
        if "days" in item:
            days = _normalize_input_text(item.get("days"))
            if not days:
                raise ValueError("days cannot be empty")
            fields.append("days = %s")
            params.append(days)
        if "daypart" in item:
            daypart = _normalize_input_text(item.get("daypart"))
            if not daypart:
                raise ValueError("daypart cannot be empty")
            fields.append("daypart = %s")
            params.append(daypart)
        if "rtg" in item:
            fields.append("rtg = %s")
            params.append(item.get("rtg"))
        if "matchKey" in item:
            match_key = _normalize_input_text(item.get("matchKey"))
            if not match_key:
                raise ValueError("matchKey cannot be empty")
            fields.append("matchKey = %s")
            params.append(match_key)

        if not fields:
            raise ValueError(f"No updatable fields provided for schedule id '{schedule_row_id}'")

        params.append(schedule_row_id)
        query = f"UPDATE {schedules_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    updated = run_transaction(_work)
    if int(updated or 0) > 0:
        _invalidate_schedules_related_cache()
    return updated


def get_schedule_weeks(
    *,
    ids: list[int] | None = None,
    schedule_ids: list[str] | None = None,
    week_start_from: str | None = None,
    week_start_to: str | None = None,
    week_end_from: str | None = None,
    week_end_to: str | None = None,
) -> list[dict]:
    tables = get_db_tables()
    schedules_weeks_table = _quote_table_name(tables["SCHEDULESWEEKS"])
    where_clauses: list[str] = []
    params: list[object] = []

    normalized_ids = _normalized_int_cache_values([int(item) for item in (ids or [])])
    if normalized_ids:
        placeholders = _build_in_placeholders(normalized_ids)
        where_clauses.append(f"id IN ({placeholders})")
        params.extend(normalized_ids)

    normalized_schedule_ids = _normalized_text_cache_values(
        [str(item or "").strip() for item in (schedule_ids or [])]
    )
    normalized_schedule_ids = [item for item in normalized_schedule_ids if item]
    if normalized_schedule_ids:
        placeholders = _build_in_placeholders(normalized_schedule_ids)
        where_clauses.append(f"scheduleId IN ({placeholders})")
        params.extend(normalized_schedule_ids)

    if week_start_from is not None:
        where_clauses.append("weekStart >= %s")
        params.append(str(week_start_from))
    if week_start_to is not None:
        where_clauses.append("weekStart <= %s")
        params.append(str(week_start_to))
    if week_end_from is not None:
        where_clauses.append("weekEnd >= %s")
        params.append(str(week_end_from))
    if week_end_to is not None:
        where_clauses.append("weekEnd <= %s")
        params.append(str(week_end_to))

    cache_key = _build_db_read_cache_key(
        "schedule_weeks",
        f"schedules_weeks_table={schedules_weeks_table}",
        "ids=" + (",".join(map(str, normalized_ids)) if normalized_ids else "*"),
        "schedule_ids="
        + (",".join(normalized_schedule_ids) if normalized_schedule_ids else "*"),
        f"week_start_from={str(week_start_from) if week_start_from is not None else '*'}",
        f"week_start_to={str(week_start_to) if week_start_to is not None else '*'}",
        f"week_end_from={str(week_end_from) if week_end_from is not None else '*'}",
        f"week_end_to={str(week_end_to) if week_end_to is not None else '*'}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_schedule_weeks_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT id, scheduleId, weekStart, weekEnd, spots "
        f"FROM {schedules_weeks_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY weekStart ASC, scheduleId ASC, id ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def insert_schedule_weeks(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    schedules_weeks_table = _quote_table_name(tables["SCHEDULESWEEKS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        schedule_row_id = _normalize_input_text(item.get("scheduleId"))
        if not schedule_row_id:
            raise ValueError("scheduleId is required")
        week_start = _normalize_input_text(item.get("weekStart"))
        if not week_start:
            raise ValueError("weekStart is required")
        week_end = _normalize_input_text(item.get("weekEnd"))
        if not week_end:
            raise ValueError("weekEnd is required")
        spots = item.get("spots")
        if spots is None:
            raise ValueError("spots is required")
        values.append((schedule_row_id, week_start, week_end, int(spots)))

    query = (
        f"INSERT INTO {schedules_weeks_table} "
        "(scheduleId, weekStart, weekEnd, spots) "
        "VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "weekEnd = VALUES(weekEnd), "
        "spots = VALUES(spots)"
    )
    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_schedule_weeks_related_cache()
    return inserted


def update_schedule_weeks(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    schedules_weeks_table = _quote_table_name(tables["SCHEDULESWEEKS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        week_row_id = item.get("id")
        if week_row_id is None:
            raise ValueError("id is required for schedule weeks update")
        fields: list[str] = []
        params: list[object] = []

        if "scheduleId" in item:
            schedule_row_id = _normalize_input_text(item.get("scheduleId"))
            if not schedule_row_id:
                raise ValueError("scheduleId cannot be empty")
            fields.append("scheduleId = %s")
            params.append(schedule_row_id)
        if "weekStart" in item:
            week_start = _normalize_input_text(item.get("weekStart"))
            if not week_start:
                raise ValueError("weekStart cannot be empty")
            fields.append("weekStart = %s")
            params.append(week_start)
        if "weekEnd" in item:
            week_end = _normalize_input_text(item.get("weekEnd"))
            if not week_end:
                raise ValueError("weekEnd cannot be empty")
            fields.append("weekEnd = %s")
            params.append(week_end)
        if "spots" in item:
            spots = item.get("spots")
            if spots is None:
                raise ValueError("spots cannot be null")
            fields.append("spots = %s")
            params.append(int(spots))

        if not fields:
            raise ValueError(f"No updatable fields provided for schedule week id '{week_row_id}'")

        params.append(int(week_row_id))
        query = f"UPDATE {schedules_weeks_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    updated = run_transaction(_work)
    if int(updated or 0) > 0:
        _invalidate_schedule_weeks_related_cache()
    return updated


def get_delivery_methods(*, ids: list[int] | None = None) -> list[dict]:
    tables = get_db_tables()
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    where_clauses: list[str] = []
    params: list[object] = []
    normalized_ids = _normalized_int_cache_values([int(item) for item in (ids or [])])
    if normalized_ids:
        placeholders = _build_in_placeholders(normalized_ids)
        where_clauses.append(f"id IN ({placeholders})")
        params.extend(normalized_ids)

    cache_key = _build_db_read_cache_key(
        "delivery_methods",
        f"delivery_methods_table={delivery_methods_table}",
        "ids=" + (",".join(map(str, normalized_ids)) if normalized_ids else "*"),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_delivery_methods_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT id, name, url, username, password, deadline, note "
        f"FROM {delivery_methods_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY id ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def insert_delivery_methods(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        name = _normalize_input_text(item.get("name"))
        if not name:
            raise ValueError("name is required")
        url = _normalize_input_text(item.get("url"))
        if not url:
            raise ValueError("url is required")
        username = _normalize_input_text(item.get("username"))
        if not username:
            raise ValueError("username is required")
        deadline = _normalize_input_text(item.get("deadline") or "10 AM") or "10 AM"
        password = item.get("password")
        note = _normalize_optional_input_text(item.get("note"))
        values.append((name, url, username, password, deadline, note))

    query = (
        f"INSERT INTO {delivery_methods_table} "
        "(name, url, username, password, deadline, note) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "name = VALUES(name), "
        "password = VALUES(password), "
        "note = VALUES(note)"
    )
    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_delivery_methods_related_cache()
    return inserted


def get_stations(
    *,
    codes: list[str] | None = None,
    account_codes: list[str] | None = None,
    est_nums: list[int] | None = None,
    station_name: str | None = None,
    affiliation: str | None = None,
    media_types: list[str] | None = None,
    languages: list[str] | None = None,
    delivery_method_detail: bool = True,
) -> list[dict]:
    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    normalized_codes = _normalized_text_cache_values(
        [_normalize_account_code(code) for code in (codes or [])]
    )
    normalized_codes = [code for code in normalized_codes if code]

    where_clauses: list[str] = []
    params: list[object] = []

    if normalized_codes:
        code_placeholders = _build_in_placeholders(normalized_codes)
        where_clauses.append(f"UPPER(s.code) IN ({code_placeholders})")
        params.extend(normalized_codes)

    normalized_account_codes = _normalized_text_cache_values([
        _normalize_account_code(item) for item in (account_codes or [])
    ])
    normalized_account_codes = [item for item in normalized_account_codes if item]

    normalized_est_nums = _normalized_int_cache_values(
        [int(item) for item in (est_nums or [])]
    )
    normalized_station_name = str(station_name or "").strip().lower()
    normalized_affiliation = str(affiliation or "").strip().lower()
    normalized_media_types = _normalized_text_cache_values(
        [_normalize_media_type(item) for item in (media_types or [])]
    )
    normalized_media_types = [item for item in normalized_media_types if item]
    normalized_languages = _normalized_text_cache_values(
        [str(item or "").strip().upper() for item in (languages or [])]
    )
    normalized_languages = [item for item in normalized_languages if item]

    if normalized_est_nums or normalized_account_codes:
        exists_clauses = ["UPPER(sc.stationCode) = UPPER(s.code)"]
        exists_params: list[object] = []

        if normalized_est_nums:
            placeholders = _build_in_placeholders(normalized_est_nums)
            exists_clauses.append(f"sc.estNum IN ({placeholders})")
            exists_params.extend(normalized_est_nums)

        exists_from = f"FROM {schedules_table} sc "
        if normalized_account_codes:
            placeholders = _build_in_placeholders(normalized_account_codes)
            exists_from += f"JOIN {est_nums_table} en ON en.estNum = sc.estNum "
            exists_clauses.append(f"UPPER(en.accountCode) IN ({placeholders})")
            exists_params.extend(normalized_account_codes)

        where_clauses.append(
            "EXISTS (SELECT 1 "
            + exists_from
            + "WHERE "
            + " AND ".join(exists_clauses)
            + ")"
        )
        params.extend(exists_params)

    if normalized_station_name:
        where_clauses.append("LOWER(COALESCE(s.name, '')) LIKE %s")
        params.append(f"%{normalized_station_name}%")

    if normalized_affiliation:
        where_clauses.append("LOWER(COALESCE(s.affiliation, '')) LIKE %s")
        params.append(f"%{normalized_affiliation}%")

    if normalized_media_types:
        placeholders = _build_in_placeholders(normalized_media_types)
        where_clauses.append(f"UPPER(COALESCE(s.mediaType, '')) IN ({placeholders})")
        params.extend(normalized_media_types)

    if normalized_languages:
        placeholders = _build_in_placeholders(normalized_languages)
        where_clauses.append(f"UPPER(COALESCE(s.language, '')) IN ({placeholders})")
        params.extend(normalized_languages)

    if not where_clauses:
        return []

    cache_key = _build_db_read_cache_key(
        "stations",
        f"stations_table={stations_table}",
        f"delivery_methods_table={delivery_methods_table}",
        f"schedules_table={schedules_table}",
        f"est_nums_table={est_nums_table}",
        "codes=" + (",".join(normalized_codes) if normalized_codes else "*"),
        "account_codes="
        + (",".join(normalized_account_codes) if normalized_account_codes else "*"),
        "est_nums=" + (",".join(map(str, normalized_est_nums)) if normalized_est_nums else "*"),
        f"station_name={normalized_station_name or '*'}",
        f"affiliation={normalized_affiliation or '*'}",
        "media_types="
        + (",".join(normalized_media_types) if normalized_media_types else "*"),
        "languages="
        + (",".join(normalized_languages) if normalized_languages else "*"),
        f"delivery_method_detail={int(bool(delivery_method_detail))}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_stations_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    select_fields = (
        "s.code AS code, "
        "s.name AS name, "
        "s.affiliation AS affiliation, "
        "s.mediaType AS mediaType, "
        "CASE WHEN UPPER(s.mediaType) = 'CA' THEN s.syscode ELSE NULL END AS syscode, "
        "s.language AS language, "
        "s.ownership AS ownership, "
        "s.deliveryMethodId AS deliveryMethodId, "
        "s.note AS note, "
        "d.name AS deliveryMethodName"
    )
    from_clause = (
        f"FROM {stations_table} s "
        f"LEFT JOIN {delivery_methods_table} d ON s.deliveryMethodId = d.id "
    )
    if delivery_method_detail:
        select_fields += (
            ", d.url AS deliveryMethodUrl, "
            "d.username AS deliveryMethodUsername, "
            "d.deadline AS deliveryMethodDeadline, "
            "d.note AS deliveryMethodNote"
        )

    query = (
        "SELECT "
        + select_fields
        + " "
        + from_clause
        + "WHERE "
        + " AND ".join(where_clauses)
        + " ORDER BY s.code ASC"
    )
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def get_station_media_types(*, codes: list[str]) -> dict[str, str]:
    normalized_codes = _normalized_text_cache_values(
        [_normalize_account_code(code) for code in (codes or [])]
    )
    normalized_codes = [code for code in normalized_codes if code]
    if not normalized_codes:
        return {}

    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    cache_key = _build_db_read_cache_key(
        "station_media_types",
        f"stations_table={stations_table}",
        "codes=" + ",".join(normalized_codes),
    )
    cached_mapped = _get_cached_dict(
        cache_key,
        ttl_key="db_station_media_types_ttl_time",
    )
    if cached_mapped is not None:
        normalized_mapped: dict[str, str] = {}
        for code, media_type in cached_mapped.items():
            code_text = str(code or "").strip().upper()
            media_type_text = str(media_type or "").strip().upper()
            if not code_text or not media_type_text:
                continue
            normalized_mapped[code_text] = media_type_text
        return normalized_mapped

    placeholders = _build_in_placeholders(normalized_codes)
    query = (
        "SELECT UPPER(code) AS code, UPPER(mediaType) AS mediaType "
        f"FROM {stations_table} "
        f"WHERE UPPER(code) IN ({placeholders})"
    )
    rows = fetch_all(query, tuple(normalized_codes))

    mapped: dict[str, str] = {}
    for row in rows:
        code = str(row.get("code") or "").strip().upper()
        media_type = str(row.get("mediaType") or "").strip().upper()
        if not code or not media_type:
            continue
        mapped[code] = media_type
    _set_cached_value(cache_key, mapped)
    return mapped


def get_station_account_codes(
    *,
    station_codes: list[str],
    account_codes: list[str] | None = None,
    est_nums: list[int] | None = None,
) -> dict[str, list[str]]:
    normalized_station_codes = _normalized_text_cache_values(
        [_normalize_account_code(code) for code in station_codes]
    )
    normalized_station_codes = [code for code in normalized_station_codes if code]
    if not normalized_station_codes:
        return {}

    normalized_account_codes = _normalized_text_cache_values(
        [_normalize_account_code(item) for item in (account_codes or [])]
    )
    normalized_account_codes = [item for item in normalized_account_codes if item]
    normalized_est_nums = _normalized_int_cache_values([int(item) for item in (est_nums or [])])

    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    est_nums_table = _quote_table_name(tables["ESTNUMS"])

    where_clauses: list[str] = []
    params: list[object] = []

    station_placeholders = _build_in_placeholders(normalized_station_codes)
    where_clauses.append(f"UPPER(sc.stationCode) IN ({station_placeholders})")
    params.extend(normalized_station_codes)

    if normalized_est_nums:
        placeholders = _build_in_placeholders(normalized_est_nums)
        where_clauses.append(f"sc.estNum IN ({placeholders})")
        params.extend(normalized_est_nums)

    if normalized_account_codes:
        placeholders = _build_in_placeholders(normalized_account_codes)
        where_clauses.append(f"UPPER(en.accountCode) IN ({placeholders})")
        params.extend(normalized_account_codes)

    cache_key = _build_db_read_cache_key(
        "station_account_codes",
        f"schedules_table={schedules_table}",
        f"est_nums_table={est_nums_table}",
        "station_codes=" + ",".join(normalized_station_codes),
        "account_codes="
        + (",".join(normalized_account_codes) if normalized_account_codes else "*"),
        "est_nums=" + (",".join(map(str, normalized_est_nums)) if normalized_est_nums else "*"),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_station_account_codes_ttl_time",
    )
    if cached_rows is not None:
        grouped: dict[str, list[str]] = {}
        for row in cached_rows:
            if not isinstance(row, dict):
                continue
            station_code = str(row.get("stationCode") or "").strip().upper()
            account_code = str(row.get("accountCode") or "").strip().upper()
            if not station_code or not account_code:
                continue
            grouped.setdefault(station_code, []).append(account_code)
        for station_code, codes in grouped.items():
            grouped[station_code] = sorted(set(codes))
        return grouped

    query = (
        "SELECT DISTINCT "
        "UPPER(sc.stationCode) AS stationCode, "
        "UPPER(en.accountCode) AS accountCode "
        f"FROM {schedules_table} sc "
        f"JOIN {est_nums_table} en ON en.estNum = sc.estNum "
        "WHERE " + " AND ".join(where_clauses) + " "
        "ORDER BY stationCode ASC, accountCode ASC"
    )
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)

    grouped: dict[str, list[str]] = {}
    for row in rows:
        station_code = str(row.get("stationCode") or "").strip().upper()
        account_code = str(row.get("accountCode") or "").strip().upper()
        if not station_code or not account_code:
            continue
        grouped.setdefault(station_code, []).append(account_code)
    return grouped


def insert_stations(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        code = _normalize_account_code(item.get("code"))
        if not code:
            raise ValueError("code is required")
        name = _normalize_input_text(item.get("name"))
        if not name:
            raise ValueError("name is required")
        media_type = _normalize_media_type(item.get("mediaType"))
        if not media_type:
            raise ValueError("mediaType is required")
        language = _normalize_input_text(item.get("language"))
        if not language:
            raise ValueError("language is required")
        delivery_method_id = item.get("deliveryMethodId")
        if delivery_method_id is None:
            raise ValueError("deliveryMethodId is required")
        syscode = item.get("syscode")
        if syscode is not None:
            syscode = int(syscode)
            if syscode < 0:
                raise ValueError("syscode must be an unsigned integer")
        values.append(
            (
                code,
                name,
                _normalize_optional_input_text(item.get("affiliation")),
                media_type,
                syscode,
                language,
                _normalize_optional_input_text(item.get("ownership")),
                int(delivery_method_id),
                _normalize_optional_input_text(item.get("note")),
            )
        )

    query = (
        f"INSERT INTO {stations_table} "
        "(code, name, affiliation, mediaType, syscode, language, ownership, deliveryMethodId, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "name = VALUES(name), "
        "affiliation = VALUES(affiliation), "
        "mediaType = VALUES(mediaType), "
        "syscode = VALUES(syscode), "
        "language = VALUES(language), "
        "ownership = VALUES(ownership), "
        "deliveryMethodId = VALUES(deliveryMethodId), "
        "note = VALUES(note)"
    )
    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_stations_related_cache()
    return inserted


def update_stations(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        code = _normalize_account_code(item.get("code"))
        if not code:
            raise ValueError("code is required for stations update")
        fields: list[str] = []
        params: list[object] = []
        if "name" in item:
            name = _normalize_input_text(item.get("name"))
            if not name:
                raise ValueError("name cannot be empty")
            fields.append("name = %s")
            params.append(name)
        if "affiliation" in item:
            fields.append("affiliation = %s")
            params.append(_normalize_optional_input_text(item.get("affiliation")))
        if "mediaType" in item:
            media_type = _normalize_media_type(item.get("mediaType"))
            if not media_type:
                raise ValueError("mediaType cannot be empty")
            fields.append("mediaType = %s")
            params.append(media_type)
        if "syscode" in item:
            syscode = item.get("syscode")
            if syscode is None:
                fields.append("syscode = %s")
                params.append(None)
            else:
                parsed_syscode = int(syscode)
                if parsed_syscode < 0:
                    raise ValueError("syscode must be an unsigned integer")
                fields.append("syscode = %s")
                params.append(parsed_syscode)
        if "language" in item:
            language = _normalize_input_text(item.get("language"))
            if not language:
                raise ValueError("language cannot be empty")
            fields.append("language = %s")
            params.append(language)
        if "ownership" in item:
            fields.append("ownership = %s")
            params.append(_normalize_optional_input_text(item.get("ownership")))
        if "deliveryMethodId" in item:
            delivery_method_id = item.get("deliveryMethodId")
            if delivery_method_id is None:
                raise ValueError("deliveryMethodId cannot be null")
            fields.append("deliveryMethodId = %s")
            params.append(int(delivery_method_id))
        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))
        if not fields:
            raise ValueError(f"No updatable fields provided for station '{code}'")
        params.append(code)
        query = f"UPDATE {stations_table} SET " + ", ".join(fields) + " WHERE code = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    updated = run_transaction(_work)
    if int(updated or 0) > 0:
        _invalidate_stations_related_cache()
    return updated


def update_delivery_methods(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        delivery_method_id = item.get("id")
        if delivery_method_id is None:
            raise ValueError("id is required for delivery method update")
        fields: list[str] = []
        params: list[object] = []
        if "name" in item:
            name = _normalize_input_text(item.get("name"))
            if not name:
                raise ValueError("name cannot be empty")
            fields.append("name = %s")
            params.append(name)
        if "url" in item:
            url = _normalize_input_text(item.get("url"))
            if not url:
                raise ValueError("url cannot be empty")
            fields.append("url = %s")
            params.append(url)
        if "username" in item:
            username = _normalize_input_text(item.get("username"))
            if not username:
                raise ValueError("username cannot be empty")
            fields.append("username = %s")
            params.append(username)
        if "password" in item:
            fields.append("password = %s")
            params.append(item.get("password"))
        if "deadline" in item:
            deadline = _normalize_input_text(item.get("deadline"))
            if not deadline:
                raise ValueError("deadline cannot be empty")
            fields.append("deadline = %s")
            params.append(deadline)
        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))
        if not fields:
            raise ValueError(
                f"No updatable fields provided for delivery method id '{delivery_method_id}'"
            )
        params.append(int(delivery_method_id))
        query = f"UPDATE {delivery_methods_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    updated = run_transaction(_work)
    if int(updated or 0) > 0:
        _invalidate_delivery_methods_related_cache()
    return updated


def get_contacts(
    *,
    emails: list[str] | None = None,
    name: str | None = None,
    company: str | None = None,
    phone: str | None = None,
    station: str | None = None,
    contact_type: str | None = None,
    active: bool | None = None,
) -> list[dict]:
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    stations_table = _quote_table_name(tables["STATIONS"])
    where_clauses: list[str] = []
    params: list[object] = []

    normalized_emails = [_normalize_email(item) for item in (emails or [])]
    normalized_emails = [item for item in normalized_emails if item]
    if normalized_emails:
        email_like_clauses: list[str] = []
        for email_value in normalized_emails:
            email_like_clauses.append("LOWER(COALESCE(c.email, '')) LIKE %s")
            params.append(f"%{email_value}%")
        where_clauses.append("(" + " OR ".join(email_like_clauses) + ")")

    normalized_name = str(name or "").strip().lower()
    if normalized_name:
        like_value = f"%{normalized_name}%"
        where_clauses.append(
            "("
            "LOWER(COALESCE(c.firstName, '')) LIKE %s "
            "OR LOWER(COALESCE(c.lastName, '')) LIKE %s "
            "OR LOWER(CONCAT_WS(' ', COALESCE(c.firstName, ''), COALESCE(c.lastName, ''))) LIKE %s"
            ")"
        )
        params.extend([like_value, like_value, like_value])

    normalized_company = str(company or "").strip().lower()
    if normalized_company:
        where_clauses.append("LOWER(COALESCE(c.company, '')) LIKE %s")
        params.append(f"%{normalized_company}%")

    normalized_phone = _normalize_phone_search(phone)
    if normalized_phone:
        phone_like_value = f"%{normalized_phone}%"
        where_clauses.append(
            "("
            f"{_build_phone_match_sql('c.office')} LIKE %s "
            f"OR {_build_phone_match_sql('c.cell')} LIKE %s"
            ")"
        )
        params.extend([phone_like_value, phone_like_value])

    normalized_station = str(station or "").strip()
    if normalized_station:
        normalized_station_upper = normalized_station.upper()
        normalized_station_lower = normalized_station.lower()
        where_clauses.append(
            "EXISTS ("
            "SELECT 1 "
            f"FROM {stations_contacts_table} sc_filter "
            f"LEFT JOIN {stations_table} s_filter ON UPPER(s_filter.code) = UPPER(sc_filter.stationCode) "
            "WHERE sc_filter.contactId = c.id "
            "AND sc_filter.active = 1 "
            "AND ("
            "UPPER(COALESCE(sc_filter.stationCode, '')) LIKE %s "
            "OR LOWER(COALESCE(s_filter.name, '')) LIKE %s"
            ")"
            ")"
        )
        params.extend([f"%{normalized_station_upper}%", f"%{normalized_station_lower}%"])

    normalized_contact_type = _normalize_contact_type(contact_type) if contact_type else ""
    if normalized_contact_type:
        where_clauses.append(
            "EXISTS ("
            "SELECT 1 "
            f"FROM {stations_contacts_table} sc_filter "
            "WHERE sc_filter.contactId = c.id "
            "AND sc_filter.active = 1 "
            "AND UPPER(sc_filter.contactType) = %s"
            ")"
        )
        params.append(normalized_contact_type)

    if active is not None:
        where_clauses.append("c.active = %s")
        params.append(1 if active else 0)

    query = (
        "SELECT "
        "c.id AS id, "
        "c.firstName AS firstName, "
        "c.lastName AS lastName, "
        "c.company AS company, "
        "c.jobTitle AS jobTitle, "
        "c.office AS office, "
        "c.cell AS cell, "
        "c.email AS email, "
        "c.active AS active, "
        "c.note AS note, "
        "GROUP_CONCAT(DISTINCT UPPER(sc.stationCode) ORDER BY UPPER(sc.stationCode) SEPARATOR ',') "
        "AS stationCodes "
        f"FROM {contacts_table} c "
        f"LEFT JOIN {stations_contacts_table} sc ON sc.contactId = c.id AND sc.active = 1"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += (
        " GROUP BY c.id, c.firstName, c.lastName, c.company, c.jobTitle, c.office, c.cell, "
        "c.email, c.active, c.note ORDER BY c.id ASC"
    )
    return fetch_all(query, tuple(params))


def get_contacts_by_station_codes(
    *,
    station_codes: list[str],
    contact_types: list[str] | None = None,
) -> list[dict]:
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])

    normalized_station_codes = [_normalize_account_code(code) for code in station_codes]
    normalized_station_codes = [code for code in normalized_station_codes if code]
    if not normalized_station_codes:
        return []

    params: list[object] = []
    where_clauses: list[str] = []

    station_placeholders = _build_in_placeholders(normalized_station_codes)
    where_clauses.append(f"UPPER(sc.stationCode) IN ({station_placeholders})")
    params.extend(normalized_station_codes)

    normalized_contact_types = [
        _normalize_contact_type(item) for item in (contact_types or [])
    ]
    normalized_contact_types = [item for item in normalized_contact_types if item]
    if normalized_contact_types:
        type_placeholders = _build_in_placeholders(normalized_contact_types)
        where_clauses.append(f"UPPER(sc.contactType) IN ({type_placeholders})")
        params.extend(normalized_contact_types)

    where_clauses.append("sc.active = 1")
    where_clauses.append("c.active = 1")

    cache_key = _build_db_read_cache_key(
        "contacts_by_station_codes",
        f"contacts_table={contacts_table}",
        f"stations_contacts_table={stations_contacts_table}",
        "station_codes=" + ",".join(normalized_station_codes),
        "contact_types="
        + (",".join(normalized_contact_types) if normalized_contact_types else "*"),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_contacts_by_station_codes_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT "
        "sc.stationCode AS stationCode, "
        "c.id AS id, "
        "c.email AS email, "
        "c.firstName AS firstName, "
        "c.lastName AS lastName, "
        "c.company AS company, "
        "c.jobTitle AS jobTitle, "
        "c.office AS office, "
        "c.cell AS cell, "
        "c.active AS active, "
        "c.note AS note, "
        "sc.contactType AS contactType, "
        "sc.primaryContact AS primaryContact, "
        "sc.note AS contactTypeNote "
        f"FROM {stations_contacts_table} sc "
        f"INNER JOIN {contacts_table} c ON sc.contactId = c.id "
        "WHERE "
        + " AND ".join(where_clauses)
        + " ORDER BY sc.stationCode ASC, sc.primaryContact DESC, c.id ASC"
    )
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def get_contacts_selector(
    *,
    active_only: bool = True,
) -> list[dict]:
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    where_clauses: list[str] = []
    params: list[object] = []

    if active_only:
        where_clauses.append("c.active = 1")

    cache_key = _build_db_read_cache_key(
        "contacts_selector",
        f"contacts_table={contacts_table}",
        f"active_only={int(bool(active_only))}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_contacts_selector_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT "
        "c.id AS contactId, "
        "c.email AS contactEmail, "
        "c.firstName AS firstName, "
        "c.lastName AS lastName, "
        "c.company AS company, "
        "c.jobTitle AS jobTitle, "
        "c.office AS office, "
        "c.cell AS cell, "
        "c.note AS note, "
        "c.active AS active "
        f"FROM {contacts_table} c"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += (
        " ORDER BY "
        "COALESCE(c.firstName, '') ASC, "
        "COALESCE(c.lastName, '') ASC, "
        "COALESCE(c.email, '') ASC, "
        "c.id ASC"
    )
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def insert_contacts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        email = _normalize_email(item.get("email"))
        if not email:
            raise ValueError("email is required")
        first_name = item.get("firstName")
        if first_name is None:
            first_name = ""
        else:
            first_name = _normalize_input_text(first_name)
        values.append(
            (
                first_name,
                _normalize_optional_input_text(item.get("lastName")),
                _normalize_optional_input_text(item.get("company")),
                _normalize_optional_input_text(item.get("jobTitle")),
                _normalize_optional_input_text(item.get("office")),
                _normalize_optional_input_text(item.get("cell")),
                email,
                _normalize_bool(item.get("active"), default=True),
                _normalize_optional_input_text(item.get("note")),
            )
        )
    query = (
        f"INSERT INTO {contacts_table} "
        "(firstName, lastName, company, jobTitle, office, cell, email, active, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_contacts_related_cache()
    return inserted


def update_contacts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        contact_id = item.get("id")
        if contact_id is None:
            raise ValueError("id is required for contacts update")
        fields: list[str] = []
        params: list[object] = []
        if "email" in item:
            email = _normalize_email(item.get("email"))
            if not email:
                raise ValueError("email cannot be empty")
            fields.append("email = %s")
            params.append(email)
        if "firstName" in item:
            fields.append("firstName = %s")
            first_name = item.get("firstName")
            if first_name is None:
                first_name = ""
            else:
                first_name = _normalize_input_text(first_name)
            params.append(first_name)
        if "lastName" in item:
            fields.append("lastName = %s")
            params.append(_normalize_optional_input_text(item.get("lastName")))
        if "company" in item:
            fields.append("company = %s")
            params.append(_normalize_optional_input_text(item.get("company")))
        if "jobTitle" in item:
            fields.append("jobTitle = %s")
            params.append(_normalize_optional_input_text(item.get("jobTitle")))
        if "office" in item:
            fields.append("office = %s")
            params.append(_normalize_optional_input_text(item.get("office")))
        if "cell" in item:
            fields.append("cell = %s")
            params.append(_normalize_optional_input_text(item.get("cell")))
        if "active" in item:
            fields.append("active = %s")
            params.append(_normalize_bool(item.get("active"), default=True))
        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))
        if not fields:
            raise ValueError(f"No updatable fields provided for contact id '{contact_id}'")
        params.append(int(contact_id))
        query = f"UPDATE {contacts_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    updated = run_transaction(_work)
    if int(updated or 0) > 0:
        _invalidate_contacts_related_cache()
    return updated


def find_existing_emails(
    *,
    emails: list[str],
    exclude_ids: list[int] | None = None,
) -> list[dict]:
    normalized_emails = [_normalize_email(item) for item in emails]
    normalized_emails = [item for item in normalized_emails if item]
    if not normalized_emails:
        return []
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    placeholders = _build_in_placeholders(normalized_emails)
    params: list[object] = list(normalized_emails)
    where_clauses = [f"LOWER(email) IN ({placeholders})"]

    normalized_exclude_ids = [int(item) for item in (exclude_ids or [])]
    if normalized_exclude_ids:
        exclude_placeholders = _build_in_placeholders(normalized_exclude_ids)
        where_clauses.append(f"id NOT IN ({exclude_placeholders})")
        params.extend(normalized_exclude_ids)

    query = (
        "SELECT id, email "
        f"FROM {contacts_table} "
        "WHERE " + " AND ".join(where_clauses)
    )
    return fetch_all(query, tuple(params))


def get_stations_contacts(
    *,
    ids: list[int] | None = None,
    station_codes: list[str] | None = None,
    contact_ids: list[int] | None = None,
    active: bool | None = None,
) -> list[dict]:
    tables = get_db_tables()
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    stations_table = _quote_table_name(tables["STATIONS"])
    where_clauses: list[str] = []
    params: list[object] = []

    normalized_ids = _normalized_int_cache_values([int(item) for item in (ids or [])])
    if normalized_ids:
        placeholders = _build_in_placeholders(normalized_ids)
        where_clauses.append(f"sc.id IN ({placeholders})")
        params.extend(normalized_ids)

    normalized_station_codes = [
        _normalize_account_code(item) for item in (station_codes or [])
    ]
    normalized_station_codes = [item for item in normalized_station_codes if item]
    if normalized_station_codes:
        placeholders = _build_in_placeholders(normalized_station_codes)
        where_clauses.append(f"UPPER(sc.stationCode) IN ({placeholders})")
        params.extend(normalized_station_codes)

    normalized_contact_ids = _normalized_int_cache_values([int(item) for item in (contact_ids or [])])
    if normalized_contact_ids:
        placeholders = _build_in_placeholders(normalized_contact_ids)
        where_clauses.append(f"sc.contactId IN ({placeholders})")
        params.extend(normalized_contact_ids)

    if active is not None:
        where_clauses.append("sc.active = %s")
        params.append(1 if active else 0)

    cache_key = _build_db_read_cache_key(
        "stations_contacts",
        f"stations_contacts_table={stations_contacts_table}",
        f"stations_table={stations_table}",
        "ids=" + (",".join(map(str, normalized_ids)) if normalized_ids else "*"),
        "station_codes="
        + (",".join(normalized_station_codes) if normalized_station_codes else "*"),
        "contact_ids="
        + (",".join(map(str, normalized_contact_ids)) if normalized_contact_ids else "*"),
        f"active={int(active) if active is not None else '*'}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_stations_contacts_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT "
        "sc.id AS id, "
        "UPPER(sc.stationCode) AS stationCode, "
        "s.name AS stationName, "
        "UPPER(COALESCE(s.mediaType, '')) AS mediaType, "
        "s.language AS language, "
        "s.affiliation AS affiliation, "
        "CASE WHEN UPPER(COALESCE(s.mediaType, '')) = 'CA' THEN s.syscode ELSE NULL END AS syscode, "
        "sc.contactId AS contactId, "
        "sc.contactType AS contactType, "
        "sc.primaryContact AS primaryContact, "
        "sc.note AS note, "
        "sc.active AS active "
        f"FROM {stations_contacts_table} sc "
        f"LEFT JOIN {stations_table} s ON UPPER(s.code) = UPPER(sc.stationCode)"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY sc.id ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def get_station_detail_row(*, code: str) -> dict | None:
    normalized_code = _normalize_account_code(code)
    if not normalized_code:
        return None

    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    cache_key = _build_db_read_cache_key(
        "station_detail",
        f"stations_table={stations_table}",
        f"delivery_methods_table={delivery_methods_table}",
        f"code={normalized_code}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_station_detail_ttl_time",
    )
    if cached_rows is not None:
        if not cached_rows:
            return None
        first_row = cached_rows[0]
        if isinstance(first_row, dict):
            return first_row

    query = (
        "SELECT "
        "s.code AS code, "
        "s.name AS name, "
        "s.affiliation AS affiliation, "
        "s.mediaType AS mediaType, "
        "CASE WHEN UPPER(s.mediaType) = 'CA' THEN s.syscode ELSE NULL END AS syscode, "
        "s.language AS language, "
        "s.ownership AS ownership, "
        "s.deliveryMethodId AS deliveryMethodId, "
        "s.note AS note, "
        "d.id AS deliveryMethodIdValue, "
        "d.name AS deliveryMethodName, "
        "d.url AS deliveryMethodUrl, "
        "d.username AS deliveryMethodUsername, "
        "d.password AS deliveryMethodPassword, "
        "d.deadline AS deliveryMethodDeadline, "
        "d.note AS deliveryMethodNote "
        f"FROM {stations_table} s "
        f"LEFT JOIN {delivery_methods_table} d ON d.id = s.deliveryMethodId "
        "WHERE UPPER(s.code) = UPPER(%s) "
        "LIMIT 1"
    )
    rows = fetch_all(query, (normalized_code,))
    _set_cached_value(cache_key, rows)
    if not rows:
        return None
    return rows[0]


def get_station_contacts_detail_rows(
    *,
    station_code: str,
    active_only: bool = True,
) -> list[dict]:
    normalized_code = _normalize_account_code(station_code)
    if not normalized_code:
        return []

    tables = get_db_tables()
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    contacts_table = _quote_table_name(tables["CONTACTS"])
    cache_key = _build_db_read_cache_key(
        "station_contacts_detail",
        f"stations_contacts_table={stations_contacts_table}",
        f"contacts_table={contacts_table}",
        f"station_code={normalized_code}",
        f"active_only={int(bool(active_only))}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_station_contacts_detail_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    where_clauses = ["UPPER(sc.stationCode) = UPPER(%s)"]
    params: list[object] = [normalized_code]
    if active_only:
        where_clauses.append("sc.active = 1")

    query = (
        "SELECT "
        "sc.id AS linkId, "
        "sc.stationCode AS stationCode, "
        "sc.contactId AS linkContactId, "
        "sc.contactType AS contactType, "
        "sc.primaryContact AS primaryContact, "
        "sc.note AS contactTypeNote, "
        "sc.active AS linkActive, "
        "c.id AS id, "
        "c.email AS email, "
        "c.firstName AS firstName, "
        "c.lastName AS lastName, "
        "c.company AS company, "
        "c.jobTitle AS jobTitle, "
        "c.office AS office, "
        "c.cell AS cell, "
        "c.active AS active, "
        "c.note AS note "
        f"FROM {stations_contacts_table} sc "
        f"LEFT JOIN {contacts_table} c ON c.id = sc.contactId "
        "WHERE "
        + " AND ".join(where_clauses)
        + " ORDER BY sc.primaryContact DESC, sc.id ASC"
    )
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def save_station_detail_bundle(
    *,
    mode: str,
    station_code: str,
    station: dict,
    delivery_method: dict | None,
    contacts: list[dict],
    contact_links: list[dict],
) -> dict:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in {"create", "update"}:
        raise ValueError("mode must be create or update")

    normalized_station_code = _normalize_account_code(station_code)
    if not normalized_station_code:
        raise ValueError("station code is required")

    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    contacts_table = _quote_table_name(tables["CONTACTS"])
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])

    station_payload = dict(station or {})
    delivery_payload = dict(delivery_method or {})
    contact_rows = [dict(item or {}) for item in (contacts or [])]
    link_rows = [dict(item or {}) for item in (contact_links or [])]

    if _normalize_account_code(station_payload.get("code")) != normalized_station_code:
        raise ValueError("station.code must match stationCode")

    def _fetch_one(cursor, query: str, params: tuple[object, ...]) -> dict | None:
        cursor.execute(query, params)
        row = cursor.fetchone()
        if not row:
            return None
        return dict(row)

    def _resolve_delivery_method_id(
        cursor,
        *,
        current_station_delivery_method_id: int | None,
    ) -> tuple[int | None, bool, bool]:
        desired_id_raw = delivery_payload.get("id")
        desired_name = _normalize_input_text(delivery_payload.get("name"))
        desired_url = _normalize_input_text(delivery_payload.get("url"))
        desired_username = _normalize_input_text(delivery_payload.get("username"))
        desired_deadline = _normalize_input_text(delivery_payload.get("deadline"))
        desired_note = _normalize_optional_input_text(delivery_payload.get("note"))
        desired_password = delivery_payload.get("password")
        desired_id: int | None = int(desired_id_raw) if desired_id_raw is not None else None

        has_delivery_payload = any(
            [
                desired_id_raw is not None,
                bool(desired_name),
                bool(desired_url),
                bool(desired_username),
                bool(desired_deadline),
                bool(str(desired_password).strip()) if desired_password is not None else False,
                bool(desired_note),
            ]
        )
        has_inline_delivery_details = any(
            [
                bool(desired_name),
                bool(desired_url),
                bool(desired_username),
                bool(desired_deadline),
                bool(str(desired_password).strip()) if desired_password is not None else False,
                bool(desired_note),
            ]
        )

        if desired_name is None:
            desired_name = ""
        if desired_url is None:
            desired_url = ""
        if desired_username is None:
            desired_username = ""
        if desired_deadline is None:
            desired_deadline = ""

        created = False
        updated = False

        if not has_delivery_payload:
            if normalized_mode == "create":
                return None, created, updated
            return current_station_delivery_method_id, created, updated
        if desired_id is not None and not has_inline_delivery_details:
            return desired_id, created, updated

        def _find_matching_delivery_method_id() -> int | None:
            if not desired_url or not desired_username or not desired_deadline:
                return None
            query = (
                "SELECT id "
                f"FROM {delivery_methods_table} "
                "WHERE url = %s AND username = %s AND deadline = %s "
                "LIMIT 1"
            )
            row = _fetch_one(cursor, query, (desired_url, desired_username, desired_deadline))
            if not row:
                return None
            return int(row["id"])

        def _insert_delivery_method() -> int:
            insert_query = (
                f"INSERT INTO {delivery_methods_table} "
                "(name, url, username, password, deadline, note) "
                "VALUES (%s, %s, %s, %s, %s, %s)"
            )
            cursor.execute(
                insert_query,
                (
                    desired_name,
                    desired_url,
                    desired_username,
                    desired_password,
                    desired_deadline,
                    desired_note,
                ),
            )
            return int(cursor.lastrowid)

        def _count_station_refs(delivery_method_id: int) -> int:
            count_query = (
                "SELECT COUNT(*) AS total "
                f"FROM {stations_table} "
                "WHERE deliveryMethodId = %s"
            )
            row = _fetch_one(cursor, count_query, (delivery_method_id,))
            return int((row or {}).get("total") or 0)

        if desired_id is None:
            resolved_existing = _find_matching_delivery_method_id()
            if resolved_existing is not None:
                return resolved_existing, created, updated
            resolved_id = _insert_delivery_method()
            created = True
            return resolved_id, created, updated

        current_row = _fetch_one(
            cursor,
            (
                "SELECT id, name, url, username, password, deadline, note "
                f"FROM {delivery_methods_table} "
                "WHERE id = %s "
                "LIMIT 1"
            ),
            (desired_id,),
        )
        if not current_row:
            raise ValueError(f"Unknown deliveryMethodId values: {desired_id}")

        current_name = _normalize_input_text(current_row.get("name")) or ""
        current_url = _normalize_input_text(current_row.get("url")) or ""
        current_username = _normalize_input_text(current_row.get("username")) or ""
        current_deadline = _normalize_input_text(current_row.get("deadline")) or ""
        current_note = _normalize_optional_input_text(current_row.get("note"))

        password_changed = desired_password is not None and str(desired_password).strip() != ""
        is_changed = (
            desired_name != current_name
            or desired_url != current_url
            or desired_username != current_username
            or desired_deadline != current_deadline
            or desired_note != current_note
            or password_changed
        )
        if not is_changed:
            return desired_id, created, updated

        refs = _count_station_refs(desired_id)
        is_shared = refs > 1
        if (
            current_station_delivery_method_id is not None
            and desired_id != int(current_station_delivery_method_id)
        ):
            is_shared = True

        if is_shared:
            resolved_existing = _find_matching_delivery_method_id()
            if resolved_existing is not None:
                return resolved_existing, created, updated
            resolved_id = _insert_delivery_method()
            created = True
            return resolved_id, created, updated

        update_fields: list[str] = [
            "name = %s",
            "url = %s",
            "username = %s",
            "deadline = %s",
            "note = %s",
        ]
        params: list[object] = [
            desired_name,
            desired_url,
            desired_username,
            desired_deadline,
            desired_note,
        ]
        if password_changed:
            update_fields.append("password = %s")
            params.append(desired_password)
        params.append(desired_id)
        cursor.execute(
            f"UPDATE {delivery_methods_table} SET " + ", ".join(update_fields) + " WHERE id = %s",
            tuple(params),
        )
        updated = True
        return desired_id, created, updated

    def _work(cursor) -> dict:
        summary = {
            "deliveryMethodCreated": False,
            "deliveryMethodUpdated": False,
            "stationCreated": False,
            "stationUpdated": False,
            "contactsCreated": 0,
            "contactsUpdated": 0,
            "linksCreatedOrReactivated": 0,
            "linksUpdated": 0,
            "linksDeactivated": 0,
        }

        current_station_row = _fetch_one(
            cursor,
            (
                "SELECT code, deliveryMethodId "
                f"FROM {stations_table} "
                "WHERE UPPER(code) = UPPER(%s) "
                "LIMIT 1 FOR UPDATE"
            ),
            (normalized_station_code,),
        )

        if normalized_mode == "create":
            if current_station_row:
                raise ValueError(f"Station '{normalized_station_code}' already exists")
        else:
            if not current_station_row:
                raise ValueError(f"Unknown stationCode values: {normalized_station_code}")

        current_delivery_method_id = (
            int(current_station_row.get("deliveryMethodId"))
            if current_station_row and current_station_row.get("deliveryMethodId") is not None
            else None
        )
        resolved_delivery_method_id, dm_created, dm_updated = _resolve_delivery_method_id(
            cursor,
            current_station_delivery_method_id=current_delivery_method_id,
        )
        summary["deliveryMethodCreated"] = dm_created
        summary["deliveryMethodUpdated"] = dm_updated

        if normalized_mode == "create":
            cursor.execute(
                f"INSERT INTO {stations_table} "
                "(code, name, affiliation, mediaType, syscode, language, ownership, deliveryMethodId, note) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    normalized_station_code,
                    _normalize_input_text(station_payload.get("name")),
                    _normalize_optional_input_text(station_payload.get("affiliation")),
                    _normalize_media_type(station_payload.get("mediaType")),
                    station_payload.get("syscode"),
                    _normalize_input_text(station_payload.get("language")),
                    _normalize_optional_input_text(station_payload.get("ownership")),
                    resolved_delivery_method_id,
                    _normalize_optional_input_text(station_payload.get("note")),
                ),
            )
            summary["stationCreated"] = True
        else:
            cursor.execute(
                f"UPDATE {stations_table} "
                "SET name = %s, affiliation = %s, mediaType = %s, syscode = %s, "
                "language = %s, ownership = %s, deliveryMethodId = %s, note = %s "
                "WHERE UPPER(code) = UPPER(%s)",
                (
                    _normalize_input_text(station_payload.get("name")),
                    _normalize_optional_input_text(station_payload.get("affiliation")),
                    _normalize_media_type(station_payload.get("mediaType")),
                    station_payload.get("syscode"),
                    _normalize_input_text(station_payload.get("language")),
                    _normalize_optional_input_text(station_payload.get("ownership")),
                    resolved_delivery_method_id,
                    _normalize_optional_input_text(station_payload.get("note")),
                    normalized_station_code,
                ),
            )
            summary["stationUpdated"] = True

        contact_keys_by_email: dict[str, str] = {}
        create_contacts_rows: list[dict] = []
        update_contacts_rows: list[dict] = []
        resolved_contact_ids_by_client_key: dict[str, int] = {}
        resolved_contact_ids_from_payload: set[int] = set()
        explicit_contact_ids: set[int] = set()

        for row in contact_rows:
            row_id = row.get("id")
            if row_id is None:
                continue
            explicit_contact_ids.add(int(row_id))

        existing_explicit_contact_ids: set[int] = set()
        if explicit_contact_ids:
            explicit_contact_id_values = sorted(explicit_contact_ids)
            placeholders = _build_in_placeholders(explicit_contact_id_values)
            cursor.execute(
                (
                    "SELECT id "
                    f"FROM {contacts_table} "
                    f"WHERE id IN ({placeholders}) "
                    "FOR UPDATE"
                ),
                tuple(explicit_contact_id_values),
            )
            existing_explicit_contact_ids = {
                int(item.get("id"))
                for item in (cursor.fetchall() or [])
                if item.get("id") is not None
            }
            missing_explicit_contact_ids = sorted(
                explicit_contact_ids - existing_explicit_contact_ids
            )
            if missing_explicit_contact_ids:
                raise ValueError(
                    "Unknown contactId values: "
                    + ", ".join(map(str, missing_explicit_contact_ids))
                )

        for index, row in enumerate(contact_rows):
            row_id = row.get("id")
            row_client_key = str(row.get("clientKey") or "").strip()
            row_email = _normalize_email(row.get("email")) if "email" in row else ""
            contact_ref = (
                f"id:{int(row_id)}"
                if row_id is not None
                else (f"clientKey:{row_client_key}" if row_client_key else f"index:{index}")
            )

            if row_email:
                owner = contact_keys_by_email.get(row_email)
                if owner and owner != contact_ref:
                    raise ValueError(
                        f"Duplicate contacts found: email '{row_email}' appears multiple times in payload"
                    )
                contact_keys_by_email[row_email] = contact_ref

            if row_id is None:
                create_contacts_rows.append(
                    {
                        "index": index,
                        "clientKey": row_client_key,
                        "email": row_email,
                        "firstName": _normalize_input_text(row.get("firstName") or ""),
                        "lastName": _normalize_optional_input_text(row.get("lastName")),
                        "company": _normalize_optional_input_text(row.get("company")),
                        "jobTitle": _normalize_optional_input_text(row.get("jobTitle")),
                        "office": _normalize_optional_input_text(row.get("office")),
                        "cell": _normalize_optional_input_text(row.get("cell")),
                        "active": _normalize_bool(row.get("active"), default=True),
                        "note": _normalize_optional_input_text(row.get("note")),
                    }
                )
                continue

            parsed_id = int(row_id)
            if parsed_id not in existing_explicit_contact_ids:
                raise ValueError(f"Unknown contactId values: {parsed_id}")

            resolved_contact_ids_from_payload.add(parsed_id)
            if row_client_key:
                resolved_contact_ids_by_client_key[row_client_key] = parsed_id

            update_contacts_rows.append(
                {
                    "id": parsed_id,
                    "email": row_email if "email" in row else None,
                    "firstName": _normalize_input_text(row.get("firstName") or "")
                    if "firstName" in row
                    else None,
                    "lastName": _normalize_optional_input_text(row.get("lastName"))
                    if "lastName" in row
                    else None,
                    "company": _normalize_optional_input_text(row.get("company"))
                    if "company" in row
                    else None,
                    "jobTitle": _normalize_optional_input_text(row.get("jobTitle"))
                    if "jobTitle" in row
                    else None,
                    "office": _normalize_optional_input_text(row.get("office"))
                    if "office" in row
                    else None,
                    "cell": _normalize_optional_input_text(row.get("cell"))
                    if "cell" in row
                    else None,
                    "active": _normalize_bool(row.get("active"), default=True)
                    if "active" in row
                    else None,
                    "note": _normalize_optional_input_text(row.get("note"))
                    if "note" in row
                    else None,
                }
            )

        email_values = sorted(contact_keys_by_email.keys())
        if email_values:
            placeholders = _build_in_placeholders(email_values)
            cursor.execute(
                (
                    "SELECT id, LOWER(email) AS email "
                    f"FROM {contacts_table} "
                    f"WHERE LOWER(email) IN ({placeholders})"
                ),
                tuple(email_values),
            )
            existing_email_rows = cursor.fetchall() or []
            existing_email_map = {
                str(item.get("email") or "").strip().lower(): int(item.get("id"))
                for item in existing_email_rows
                if item.get("id") is not None and str(item.get("email") or "").strip()
            }
            for email, owner in contact_keys_by_email.items():
                existing_id = existing_email_map.get(email)
                if existing_id is None:
                    continue
                if owner.startswith("id:"):
                    current_id = int(owner.split(":", 1)[1])
                    if current_id == existing_id:
                        continue
                raise ValueError(
                    f"Duplicate contacts found: email '{email}' already exists on contact id {existing_id}"
                )

        for row in create_contacts_rows:
            if not row["email"]:
                raise ValueError("contacts[].email is required for new contacts")
            cursor.execute(
                f"INSERT INTO {contacts_table} "
                "(firstName, lastName, company, jobTitle, office, cell, email, active, note) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    row["firstName"],
                    row["lastName"],
                    row["company"],
                    row["jobTitle"],
                    row["office"],
                    row["cell"],
                    row["email"],
                    row["active"],
                    row["note"],
                ),
            )
            created_contact_id = int(cursor.lastrowid)
            summary["contactsCreated"] += 1
            resolved_contact_ids_from_payload.add(created_contact_id)
            if row["clientKey"]:
                resolved_contact_ids_by_client_key[row["clientKey"]] = created_contact_id

        for row in update_contacts_rows:
            fields: list[str] = []
            params: list[object] = []
            if row["email"] is not None:
                fields.append("email = %s")
                params.append(row["email"])
            if row["firstName"] is not None:
                fields.append("firstName = %s")
                params.append(row["firstName"])
            if row["lastName"] is not None:
                fields.append("lastName = %s")
                params.append(row["lastName"])
            if row["company"] is not None:
                fields.append("company = %s")
                params.append(row["company"])
            if row["jobTitle"] is not None:
                fields.append("jobTitle = %s")
                params.append(row["jobTitle"])
            if row["office"] is not None:
                fields.append("office = %s")
                params.append(row["office"])
            if row["cell"] is not None:
                fields.append("cell = %s")
                params.append(row["cell"])
            if row["active"] is not None:
                fields.append("active = %s")
                params.append(row["active"])
            if row["note"] is not None:
                fields.append("note = %s")
                params.append(row["note"])
            if not fields:
                continue
            params.append(row["id"])
            cursor.execute(
                f"UPDATE {contacts_table} SET " + ", ".join(fields) + " WHERE id = %s",
                tuple(params),
            )
            summary["contactsUpdated"] += int(cursor.rowcount or 0)

        cursor.execute(
            (
                "SELECT id, stationCode, contactId, contactType, primaryContact, note, active "
                f"FROM {stations_contacts_table} "
                "WHERE UPPER(stationCode) = UPPER(%s)"
            ),
            (normalized_station_code,),
        )
        existing_link_rows = cursor.fetchall() or []
        existing_link_by_fingerprint: dict[tuple[int, str], dict] = {}
        active_existing_fingerprints: set[tuple[int, str]] = set()
        for row in existing_link_rows:
            contact_id = row.get("contactId")
            if contact_id is None:
                continue
            contact_type = _normalize_contact_type(row.get("contactType"))
            fingerprint = (int(contact_id), contact_type)
            existing_link_by_fingerprint.setdefault(fingerprint, row)
            if _normalize_bool(row.get("active"), default=True):
                active_existing_fingerprints.add(fingerprint)

        desired_link_payload: dict[tuple[int, str], dict] = {}
        for index, row in enumerate(link_rows):
            explicit_contact_id = row.get("contactId")
            client_key = str(row.get("contactClientKey") or "").strip()

            resolved_contact_id: int | None = None
            if explicit_contact_id is not None:
                resolved_contact_id = int(explicit_contact_id)
            if client_key:
                mapped_contact_id = resolved_contact_ids_by_client_key.get(client_key)
                if mapped_contact_id is None:
                    raise ValueError(
                        f"Ambiguous contact link resolution at contactLinks[{index}]: "
                        f"unknown contactClientKey '{client_key}'"
                    )
                if resolved_contact_id is not None and resolved_contact_id != mapped_contact_id:
                    raise ValueError(
                        f"Ambiguous contact link resolution at contactLinks[{index}]: "
                        f"contactId '{resolved_contact_id}' does not match contactClientKey '{client_key}'"
                    )
                resolved_contact_id = mapped_contact_id

            if resolved_contact_id is None:
                raise ValueError(
                    f"Ambiguous contact link resolution at contactLinks[{index}]: "
                    "either contactId or contactClientKey is required"
                )

            if resolved_contact_id not in resolved_contact_ids_from_payload:
                existing_contact = _fetch_one(
                    cursor,
                    f"SELECT id FROM {contacts_table} WHERE id = %s LIMIT 1",
                    (resolved_contact_id,),
                )
                if not existing_contact:
                    raise ValueError(f"Unknown contactId values: {resolved_contact_id}")

            contact_type = _normalize_contact_type(row.get("contactType"))
            fingerprint = (resolved_contact_id, contact_type)
            if fingerprint in desired_link_payload:
                raise ValueError(
                    f"Duplicate contactLinks found for contactId '{resolved_contact_id}' and contactType '{contact_type}'"
                )

            desired_link_payload[fingerprint] = {
                "stationCode": normalized_station_code,
                "contactId": resolved_contact_id,
                "contactType": contact_type,
                "primaryContact": _normalize_bool(row.get("primaryContact"), default=False),
                "note": _normalize_optional_input_text(row.get("note")),
                "active": _normalize_bool(row.get("active"), default=True),
            }

        for fingerprint, payload in desired_link_payload.items():
            existing_link = existing_link_by_fingerprint.get(fingerprint)
            if existing_link:
                cursor.execute(
                    f"UPDATE {stations_contacts_table} "
                    "SET primaryContact = %s, note = %s, active = %s "
                    "WHERE id = %s",
                    (
                        payload["primaryContact"],
                        payload["note"],
                        payload["active"],
                        int(existing_link["id"]),
                    ),
                )
                summary["linksUpdated"] += int(cursor.rowcount or 0)
                continue

            cursor.execute(
                f"INSERT INTO {stations_contacts_table} "
                "(stationCode, contactId, contactType, primaryContact, note, active) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    payload["stationCode"],
                    payload["contactId"],
                    payload["contactType"],
                    payload["primaryContact"],
                    payload["note"],
                    payload["active"],
                ),
            )
            summary["linksCreatedOrReactivated"] += 1

        for fingerprint in active_existing_fingerprints:
            if fingerprint in desired_link_payload:
                continue
            existing_link = existing_link_by_fingerprint.get(fingerprint)
            if not existing_link:
                continue
            cursor.execute(
                f"UPDATE {stations_contacts_table} SET active = %s WHERE id = %s",
                (0, int(existing_link["id"])),
            )
            summary["linksDeactivated"] += int(cursor.rowcount or 0)

        return {
            "stationCode": normalized_station_code,
            "deliveryMethodId": resolved_delivery_method_id,
            "summary": summary,
        }

    result = run_transaction(_work, cursor_kwargs={"dictionary": True})
    summary = result.get("summary") if isinstance(result, dict) else None
    if isinstance(summary, dict):
        has_station_or_delivery_change = bool(
            summary.get("deliveryMethodCreated")
            or summary.get("deliveryMethodUpdated")
            or summary.get("stationCreated")
            or summary.get("stationUpdated")
        )
        has_contact_change = int(summary.get("contactsCreated") or 0) > 0 or int(
            summary.get("contactsUpdated") or 0
        ) > 0
        has_link_change = (
            int(summary.get("linksCreatedOrReactivated") or 0) > 0
            or int(summary.get("linksUpdated") or 0) > 0
            or int(summary.get("linksDeactivated") or 0) > 0
        )
        if has_station_or_delivery_change:
            _invalidate_stations_related_cache()
            _invalidate_delivery_methods_related_cache()
        if has_contact_change:
            _invalidate_contacts_related_cache()
        if has_link_change:
            _invalidate_stations_contacts_related_cache()
    return result


def insert_stations_contacts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        station_code = _normalize_account_code(item.get("stationCode"))
        if not station_code:
            raise ValueError("stationCode is required")
        contact_id = item.get("contactId")
        if contact_id is None:
            raise ValueError("contactId is required")
        contact_type = _normalize_contact_type(item.get("contactType"))
        if not contact_type:
            raise ValueError("contactType is required")
        values.append(
            (
                station_code,
                int(contact_id),
                contact_type,
                _normalize_bool(item.get("primaryContact"), default=False),
                _normalize_optional_input_text(item.get("note")),
                _normalize_bool(item.get("active"), default=True),
            )
        )
    query = (
        f"INSERT INTO {stations_contacts_table} "
        "(stationCode, contactId, contactType, primaryContact, note, active) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "contactType = VALUES(contactType), "
        "primaryContact = VALUES(primaryContact), "
        "note = VALUES(note), "
        "active = VALUES(active)"
    )
    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_stations_contacts_related_cache()
    return inserted


def update_stations_contacts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        row_id = item.get("id")
        if row_id is None:
            raise ValueError("id is required for stationsContacts update")
        fields: list[str] = []
        params: list[object] = []
        if "stationCode" in item:
            station_code = _normalize_account_code(item.get("stationCode"))
            if not station_code:
                raise ValueError("stationCode cannot be empty")
            fields.append("stationCode = %s")
            params.append(station_code)
        if "contactId" in item:
            contact_id = item.get("contactId")
            if contact_id is None:
                raise ValueError("contactId cannot be null")
            fields.append("contactId = %s")
            params.append(int(contact_id))
        if "contactType" in item:
            contact_type = _normalize_contact_type(item.get("contactType"))
            if not contact_type:
                raise ValueError("contactType cannot be empty")
            fields.append("contactType = %s")
            params.append(contact_type)
        if "primaryContact" in item:
            fields.append("primaryContact = %s")
            params.append(_normalize_bool(item.get("primaryContact"), default=False))
        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))
        if "active" in item:
            fields.append("active = %s")
            params.append(_normalize_bool(item.get("active"), default=True))
        if not fields:
            raise ValueError(
                f"No updatable fields provided for stationsContacts id '{row_id}'"
            )
        params.append(int(row_id))
        query = (
            f"UPDATE {stations_contacts_table} SET "
            + ", ".join(fields)
            + " WHERE id = %s"
        )
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    updated = run_transaction(_work)
    if int(updated or 0) > 0:
        _invalidate_stations_contacts_related_cache()
    return updated


def get_all_tradsphere_account_codes() -> list[str]:
    tables = get_db_tables()
    accounts_table = _quote_table_name(tables["ACCOUNTS"])
    rows = fetch_all(f"SELECT accountCode FROM {accounts_table}")
    out: list[str] = []
    for row in rows:
        code = _normalize_account_code(row.get("accountCode"))
        if code:
            out.append(code)
    return sorted(set(out))


def get_all_master_account_codes() -> list[str]:
    tables = get_db_tables()
    master_accounts_table = _quote_table_name(tables["MASTERACCOUNTS"])
    rows = fetch_all(f"SELECT code FROM {master_accounts_table}")
    out: list[str] = []
    for row in rows:
        code = _normalize_account_code(row.get("code"))
        if code:
            out.append(code)
    return sorted(set(out))


def get_all_station_codes() -> list[str]:
    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    rows = fetch_all(f"SELECT code FROM {stations_table}")
    out: list[str] = []
    for row in rows:
        code = _normalize_account_code(row.get("code"))
        if code:
            out.append(code)
    return sorted(set(out))


def get_all_delivery_method_ids() -> list[int]:
    tables = get_db_tables()
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    rows = fetch_all(f"SELECT id FROM {delivery_methods_table}")
    out: list[int] = []
    for row in rows:
        raw = row.get("id")
        if raw is None:
            continue
        out.append(int(raw))
    return sorted(set(out))


def get_all_contact_ids() -> list[int]:
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    rows = fetch_all(f"SELECT id FROM {contacts_table}")
    out: list[int] = []
    for row in rows:
        raw = row.get("id")
        if raw is None:
            continue
        out.append(int(raw))
    return sorted(set(out))


def get_all_stations_contacts_ids() -> list[int]:
    tables = get_db_tables()
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    rows = fetch_all(f"SELECT id FROM {stations_contacts_table}")
    out: list[int] = []
    for row in rows:
        raw = row.get("id")
        if raw is None:
            continue
        out.append(int(raw))
    return sorted(set(out))


def get_all_est_nums() -> list[int]:
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    rows = fetch_all(f"SELECT estNum FROM {est_nums_table}")
    out: list[int] = []
    for row in rows:
        raw = row.get("estNum")
        if raw is None:
            continue
        out.append(int(raw))
    return sorted(set(out))


def get_all_schedule_ids() -> list[str]:
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    rows = fetch_all(f"SELECT id FROM {schedules_table}")
    out: list[str] = []
    for row in rows:
        schedule_row_id = str(row.get("id") or "").strip()
        if schedule_row_id:
            out.append(schedule_row_id)
    return sorted(set(out))


def list_inv_checklists(
    *,
    account_code: str | None = None,
    year: int | None = None,
    month: int | None = None,
    status: str | None = None,
) -> list[dict]:
    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])

    where_clauses: list[str] = []
    params: list[object] = []

    normalized_account_code = _normalize_account_code(account_code) if account_code else ""
    if normalized_account_code:
        where_clauses.append("UPPER(c.accountCode) = UPPER(%s)")
        params.append(normalized_account_code)

    if year is not None:
        where_clauses.append("c.year = %s")
        params.append(int(year))

    if month is not None:
        where_clauses.append("c.month = %s")
        params.append(int(month))

    normalized_status = _normalize_optional_input_text(status)
    if normalized_status is not None:
        where_clauses.append("c.status = %s")
        params.append(normalized_status)

    cache_key = _build_db_read_cache_key(
        "inv_checklists",
        "schema=v1",
        f"checklist_table={checklist_table}",
        f"station_table={station_table}",
        f"account_code={normalized_account_code or '*'}",
        f"year={int(year) if year is not None else '*'}",
        f"month={int(month) if month is not None else '*'}",
        f"status={normalized_status if normalized_status is not None else '*'}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_inv_checklists_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT "
        "c.id AS id, "
        "c.accountCode AS accountCode, "
        "c.year AS year, "
        "c.month AS month, "
        "c.status AS status, "
        "c.note AS note, "
        "c.dateCreated AS dateCreated, "
        "c.dateUpdated AS dateUpdated, "
        "COALESCE(s.stationCount, 0) AS stationCount "
        f"FROM {checklist_table} c "
        "LEFT JOIN ("
        "SELECT checklistId, COUNT(*) AS stationCount "
        f"FROM {station_table} "
        "GROUP BY checklistId"
        ") s ON s.checklistId = c.id"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY c.year DESC, c.month DESC, c.accountCode ASC, c.dateUpdated DESC"

    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def get_inv_checklist_row(*, checklist_id: str) -> dict | None:
    checklist_id_text = str(checklist_id or "").strip()
    if not checklist_id_text:
        return None
    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])

    cache_key = _build_db_read_cache_key(
        "inv_checklist_row",
        "schema=v1",
        f"table={checklist_table}",
        f"checklist_id={checklist_id_text}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_inv_checklist_row_ttl_time",
    )
    if cached_rows is not None:
        if not cached_rows:
            return None
        first_row = cached_rows[0]
        if isinstance(first_row, dict):
            return first_row

    rows = fetch_all(
        (
            "SELECT "
            "id, accountCode, year, month, status, note, dateCreated, dateUpdated "
            f"FROM {checklist_table} "
            "WHERE id = %s "
            "LIMIT 1"
        ),
        (checklist_id_text,),
    )
    _set_cached_value(cache_key, rows)
    if not rows:
        return None
    return rows[0]


def list_inv_checklist_rows_by_ids(*, checklist_ids: list[str]) -> list[dict]:
    normalized_ids = _normalized_text_cache_values(
        [str(item or "").strip() for item in (checklist_ids or [])]
    )
    if not normalized_ids:
        return []

    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    placeholders = _build_in_placeholders(normalized_ids)
    query = (
        "SELECT "
        "id, accountCode, year, month, status, note, dateCreated, dateUpdated "
        f"FROM {checklist_table} "
        f"WHERE id IN ({placeholders}) "
        "ORDER BY id ASC"
    )
    return fetch_all(query, tuple(normalized_ids))


def get_inv_checklist_detail_rows(
    *,
    checklist_id: str,
    include_notes: bool = True,
    include_attachments: bool = False,
) -> list[dict]:
    checklist_id_text = str(checklist_id or "").strip()
    if not checklist_id_text:
        return []
    include_notes_value = bool(include_notes or include_attachments)
    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    select_parts = [
        "c.id AS checklistId",
        "c.accountCode AS checklistAccountCode",
        "c.year AS checklistYear",
        "c.month AS checklistMonth",
        "c.status AS checklistStatus",
        "c.note AS checklistNote",
        "c.dateCreated AS checklistDateCreated",
        "c.dateUpdated AS checklistDateUpdated",
        "s.id AS stationRowId",
        "s.estNum AS stationEstNum",
        "s.stationCode AS stationCode",
        "s.status AS stationStatus",
        "s.dateCreated AS stationDateCreated",
        "s.dateUpdated AS stationDateUpdated",
    ]

    query_parts = [
        "SELECT " + ", ".join(select_parts),
        f"FROM {checklist_table} c",
        f"LEFT JOIN {station_table} s ON s.checklistId = c.id",
    ]
    order_parts = ["s.id ASC"]

    if include_notes_value:
        note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
        query_parts[0] += (
            ", n.id AS noteId"
            ", n.amount AS noteAmount"
            ", n.note AS noteText"
            ", n.dateCreated AS noteDateCreated"
            ", n.dateUpdated AS noteDateUpdated"
        )
        query_parts.append(f"LEFT JOIN {note_table} n ON n.checklistStationId = s.id")
        order_parts.append("n.id ASC")

    query_parts.append("WHERE c.id = %s")
    query_parts.append("ORDER BY " + ", ".join(order_parts))
    query = " ".join(query_parts)
    return fetch_all(query, (checklist_id_text,))


def insert_inv_checklist(item: dict) -> int:
    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    query = (
        f"INSERT INTO {checklist_table} "
        "(id, accountCode, year, month, status, note) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )
    values = (
        str(item.get("id") or "").strip(),
        _normalize_account_code(item.get("accountCode")),
        int(item.get("year")),
        int(item.get("month")),
        _normalize_optional_input_text(item.get("status")),
        _normalize_optional_input_text(item.get("note")),
    )
    inserted = execute_many(query, [values])
    if int(inserted or 0) > 0:
        _invalidate_inv_checklist_list_cache()
        _invalidate_inv_checklist_row_cache(
            checklist_ids=[str(item.get("id") or "").strip()]
        )
    return inserted


def insert_inv_checklists_for_sync(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    query = (
        f"INSERT INTO {checklist_table} "
        "(id, accountCode, year, month, status, note) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE id = id"
    )
    values: list[tuple[object, ...]] = []
    checklist_ids: list[str] = []
    for item in items:
        checklist_id = str(item.get("id") or "").strip()
        checklist_ids.append(checklist_id)
        values.append(
            (
                checklist_id,
                _normalize_account_code(item.get("accountCode")),
                int(item.get("year")),
                int(item.get("month")),
                _normalize_optional_input_text(item.get("status")),
                _normalize_optional_input_text(item.get("note")),
            )
        )

    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_inv_checklist_list_cache()
        _invalidate_inv_checklist_row_cache(checklist_ids=checklist_ids)
    return inserted


def update_inv_checklist(
    *,
    checklist_id: str,
    fields: dict[str, object],
) -> int:
    if not fields:
        return 0
    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])

    updates: list[str] = []
    params: list[object] = []
    if "accountCode" in fields:
        updates.append("accountCode = %s")
        params.append(_normalize_account_code(fields.get("accountCode")))
    if "year" in fields:
        updates.append("year = %s")
        params.append(int(fields.get("year")))
    if "month" in fields:
        updates.append("month = %s")
        params.append(int(fields.get("month")))
    if "status" in fields:
        updates.append("status = %s")
        params.append(_normalize_optional_input_text(fields.get("status")))
    if "note" in fields:
        updates.append("note = %s")
        params.append(_normalize_optional_input_text(fields.get("note")))
    if not updates:
        return 0

    params.append(str(checklist_id or "").strip())
    query = (
        f"UPDATE {checklist_table} "
        "SET " + ", ".join(updates) + " "
        "WHERE id = %s"
    )
    updated = run_transaction(
        lambda cursor: (
            cursor.execute(query, tuple(params)) or int(cursor.rowcount or 0)
        )
    )
    if int(updated or 0) > 0:
        _invalidate_inv_checklist_list_cache()
        _invalidate_inv_checklist_row_cache(checklist_ids=[str(checklist_id or "").strip()])
    return updated


def delete_inv_checklist(*, checklist_id: str) -> int:
    checklist_id_text = str(checklist_id or "").strip()
    if not checklist_id_text:
        return 0
    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    deleted = run_transaction(
        lambda cursor: (
            cursor.execute(
                f"DELETE FROM {checklist_table} WHERE id = %s",
                (checklist_id_text,),
            )
            or int(cursor.rowcount or 0)
        )
    )
    if int(deleted or 0) > 0:
        _invalidate_inv_checklist_all_related_scopes()
        _invalidate_inv_checklist_note_detail_cache()
    return deleted


def get_inv_checklist_station_row(*, station_row_id: int) -> dict | None:
    station_row_id_int = int(station_row_id)
    tables = get_db_tables()
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    rows = fetch_all(
        (
            "SELECT "
            "id, checklistId, estNum, stationCode, status, dateCreated, dateUpdated "
            f"FROM {station_table} "
            "WHERE id = %s "
            "LIMIT 1"
        ),
        (station_row_id_int,),
    )
    if not rows:
        return None
    return rows[0]


def list_inv_checklist_station_rows_by_ids(*, station_row_ids: list[int]) -> list[dict]:
    normalized_ids = _normalized_int_cache_values([int(item) for item in (station_row_ids or [])])
    if not normalized_ids:
        return []

    tables = get_db_tables()
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    placeholders = _build_in_placeholders(normalized_ids)
    query = (
        "SELECT "
        "id, checklistId, estNum, stationCode, status, dateCreated, dateUpdated "
        f"FROM {station_table} "
        f"WHERE id IN ({placeholders}) "
        "ORDER BY id ASC"
    )
    return fetch_all(query, tuple(normalized_ids))


def list_inv_checklist_stations(
    *,
    checklist_id: str | None = None,
    station_row_id: int | None = None,
    est_num: int | None = None,
    station_code: str | None = None,
    status: str | None = None,
) -> list[dict]:
    tables = get_db_tables()
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    where_clauses: list[str] = []
    params: list[object] = []

    checklist_id_text = str(checklist_id or "").strip()
    if checklist_id_text:
        where_clauses.append("checklistId = %s")
        params.append(checklist_id_text)

    if station_row_id is not None:
        where_clauses.append("id = %s")
        params.append(int(station_row_id))

    if est_num is not None:
        where_clauses.append("estNum = %s")
        params.append(int(est_num))

    normalized_station_code = _normalize_account_code(station_code) if station_code else ""
    if normalized_station_code:
        where_clauses.append("UPPER(stationCode) = UPPER(%s)")
        params.append(normalized_station_code)

    normalized_status = _normalize_optional_input_text(status)
    if normalized_status is not None:
        where_clauses.append("status = %s")
        params.append(normalized_status)

    cache_key = _build_db_read_cache_key(
        "inv_checklist_stations",
        "schema=v1",
        f"table={station_table}",
        f"checklist_id={checklist_id_text or '*'}",
        f"station_row_id={int(station_row_id) if station_row_id is not None else '*'}",
        f"est_num={int(est_num) if est_num is not None else '*'}",
        f"station_code={normalized_station_code or '*'}",
        f"status={normalized_status if normalized_status is not None else '*'}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_inv_checklist_stations_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT "
        "id, checklistId, estNum, stationCode, status, dateCreated, dateUpdated "
        f"FROM {station_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY id ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def list_inv_checklist_station_search_rows(*, checklist_ids: list[str]) -> list[dict]:
    normalized_ids = _normalized_text_cache_values(
        [str(raw or "").strip() for raw in checklist_ids]
    )
    if not normalized_ids:
        return []

    tables = get_db_tables()
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    cache_key = _build_db_read_cache_key(
        "inv_checklist_station_search",
        "schema=v1",
        f"table={station_table}",
        "checklist_ids=" + ",".join(normalized_ids),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_inv_checklist_station_search_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    placeholders = ", ".join(["%s"] * len(normalized_ids))
    query = (
        "SELECT "
        "checklistId, estNum, stationCode "
        f"FROM {station_table} "
        f"WHERE checklistId IN ({placeholders}) "
        "ORDER BY checklistId ASC, id ASC"
    )
    rows = fetch_all(query, tuple(normalized_ids))
    _set_cached_value(cache_key, rows)
    return rows


def list_inv_checklist_station_rows_for_checklists(*, checklist_ids: list[str]) -> list[dict]:
    normalized_ids = _normalized_text_cache_values(
        [str(raw or "").strip() for raw in checklist_ids]
    )
    if not normalized_ids:
        return []

    tables = get_db_tables()
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    cache_key = _build_db_read_cache_key(
        "inv_checklist_station_rows",
        "schema=v1",
        f"table={station_table}",
        "checklist_ids=" + ",".join(normalized_ids),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_inv_checklist_station_rows_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    placeholders = ", ".join(["%s"] * len(normalized_ids))
    query = (
        "SELECT "
        "id, checklistId, estNum, stationCode, status, dateCreated, dateUpdated "
        f"FROM {station_table} "
        f"WHERE checklistId IN ({placeholders}) "
        "ORDER BY checklistId ASC, id ASC"
    )
    rows = fetch_all(query, tuple(normalized_ids))
    _set_cached_value(cache_key, rows)
    return rows


def insert_inv_checklist_station(item: dict) -> int:
    tables = get_db_tables()
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    query = (
        f"INSERT INTO {station_table} "
        "(checklistId, estNum, stationCode, status) "
        "VALUES (%s, %s, %s, %s)"
    )

    def _work(cursor) -> int:
        cursor.execute(
            query,
            (
                str(item.get("checklistId") or "").strip(),
                int(item.get("estNum")),
                _normalize_account_code(item.get("stationCode")),
                _normalize_optional_input_text(item.get("status")),
            ),
        )
        return int(cursor.lastrowid or 0)

    inserted = run_transaction(_work)
    if int(inserted or 0) > 0:
        _invalidate_inv_checklist_station_scopes(include_checklists_scope=True)
    return inserted


def insert_inv_checklist_stations_for_sync(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    query = (
        f"INSERT INTO {station_table} "
        "(checklistId, estNum, stationCode, status) "
        "VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE status = status"
    )
    values: list[tuple[object, ...]] = []
    for item in items:
        values.append(
            (
                str(item.get("checklistId") or "").strip(),
                int(item.get("estNum")),
                _normalize_account_code(item.get("stationCode")),
                _normalize_optional_input_text(item.get("status")),
            )
        )

    inserted = execute_many(query, values)
    if int(inserted or 0) > 0:
        _invalidate_inv_checklist_station_scopes(include_checklists_scope=True)
    return inserted


def save_inv_checklist_bulk_changes(
    *,
    checklist_creates: list[dict],
    checklist_updates: list[dict],
    checklist_deletes: list[str],
    station_creates: list[dict],
    station_updates: list[dict],
    station_deletes: list[int],
    note_creates: list[dict],
    note_updates: list[dict],
    note_deletes: list[int],
    tenant_slug: str | None = None,
) -> dict:
    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    legacy_attachment_table = _quote_table_name(tables["INVNOTEATTACHMENTS"])
    legacy_attachment_columns = _get_inv_note_attachment_columns(
        attachment_table=legacy_attachment_table,
    )
    app_attachment_table = _get_app_attachment_table_name(tables=tables)
    app_attachment_columns = _get_app_attachment_columns(
        app_attachment_table=app_attachment_table,
    )
    tenant_slug_value = str(tenant_slug or "").strip().lower()

    checklist_ids_created: dict[str, str] = {}
    station_ids_created: dict[str, int] = {}
    note_ids_created: dict[str, int] = {}

    def _work(cursor) -> dict:
        if checklist_creates:
            values: list[tuple[object, ...]] = []
            for row in checklist_creates:
                values.append(
                    (
                        str(row["id"]),
                        _normalize_account_code(row["accountCode"]),
                        int(row["year"]),
                        int(row["month"]),
                        _normalize_optional_input_text(row.get("status")),
                        _normalize_optional_input_text(row.get("note")),
                    )
                )
                checklist_ids_created[str(row["clientChecklistId"])] = str(row["id"])
            cursor.executemany(
                (
                    f"INSERT INTO {checklist_table} "
                    "(id, accountCode, year, month, status, note) "
                    "VALUES (%s, %s, %s, %s, %s, %s)"
                ),
                values,
            )

        for row in checklist_updates:
            fields: list[str] = []
            params: list[object] = []
            if "status" in row:
                fields.append("status = %s")
                params.append(_normalize_optional_input_text(row.get("status")))
            if "note" in row:
                fields.append("note = %s")
                params.append(_normalize_optional_input_text(row.get("note")))
            if not fields:
                continue
            params.append(str(row["checklistId"]))
            cursor.execute(
                f"UPDATE {checklist_table} SET " + ", ".join(fields) + " WHERE id = %s",
                tuple(params),
            )

        for row in station_creates:
            cursor.execute(
                (
                    f"INSERT INTO {station_table} "
                    "(checklistId, estNum, stationCode, status) "
                    "VALUES (%s, %s, %s, %s)"
                ),
                (
                    str(row["checklistId"]),
                    int(row["estNum"]),
                    _normalize_account_code(row["stationCode"]),
                    _normalize_optional_input_text(row.get("status")),
                ),
            )
            station_ids_created[str(row["clientStationId"])] = int(cursor.lastrowid or 0)

        for row in station_updates:
            fields: list[str] = []
            params: list[object] = []
            if "status" in row:
                fields.append("status = %s")
                params.append(_normalize_optional_input_text(row.get("status")))
            if "estNum" in row:
                fields.append("estNum = %s")
                params.append(int(row["estNum"]))
            if "stationCode" in row:
                fields.append("stationCode = %s")
                params.append(_normalize_account_code(row["stationCode"]))
            if not fields:
                continue
            params.append(int(row["stationRowId"]))
            cursor.execute(
                f"UPDATE {station_table} SET " + ", ".join(fields) + " WHERE id = %s",
                tuple(params),
            )

        for row in note_creates:
            station_id_value = row.get("checklistStationId")
            if station_id_value is None:
                client_station_id = str(row.get("checklistStationClientId") or "").strip()
                station_id_value = station_ids_created.get(client_station_id)
            if station_id_value is None:
                raise ValueError("Unknown checklist station reference for note create")
            columns = ["checklistStationId"]
            values: list[object] = [int(station_id_value)]
            if "amount" in row:
                columns.append("amount")
                values.append(row.get("amount"))
            if "note" in row:
                columns.append("note")
                values.append(_normalize_input_text(row.get("note")))
            cursor.execute(
                (
                    f"INSERT INTO {note_table} "
                    f"({', '.join(columns)}) "
                    f"VALUES ({', '.join(['%s'] * len(values))})"
                ),
                tuple(values),
            )
            note_ids_created[str(row["clientNoteId"])] = int(cursor.lastrowid or 0)

        for row in note_updates:
            fields: list[str] = []
            params: list[object] = []
            if "amount" in row:
                fields.append("amount = %s")
                params.append(row.get("amount"))
            if "note" in row:
                fields.append("note = %s")
                params.append(_normalize_input_text(row.get("note")))
            if not fields:
                continue
            params.append(int(row["noteId"]))
            cursor.execute(
                f"UPDATE {note_table} SET " + ", ".join(fields) + " WHERE id = %s",
                tuple(params),
            )

        if note_deletes:
            normalized_note_ids = _normalized_int_cache_values([int(item) for item in note_deletes])
            placeholders = _build_in_placeholders([int(item) for item in normalized_note_ids])
            owner_entity_ids = [str(item) for item in normalized_note_ids]
            owner_placeholders = _build_in_placeholders(owner_entity_ids)

            if app_attachment_columns:
                app_where_clauses = [
                    "appCode = %s",
                    "ownerEntityType = %s",
                    f"ownerEntityId IN ({owner_placeholders})",
                ]
                app_params: list[object] = [
                    _APP_CODE_TRADSPHERE,
                    _OWNER_ENTITY_TYPE_INVOICE_CHECKLIST_NOTE,
                    *owner_entity_ids,
                ]
                if _has_column(app_attachment_columns, "tenantSlug") and tenant_slug_value:
                    app_where_clauses.append("LOWER(tenantSlug) = %s")
                    app_params.append(tenant_slug_value)

                app_where_sql = " AND ".join(app_where_clauses)
                if _has_column(app_attachment_columns, "deletedAt"):
                    cursor.execute(
                        (
                            f"UPDATE {app_attachment_table} "
                            "SET deletedAt = CURRENT_TIMESTAMP "
                            "WHERE "
                            + app_where_sql
                            + " AND deletedAt IS NULL"
                        ),
                        tuple(app_params),
                    )
                else:
                    cursor.execute(
                        (
                            f"DELETE FROM {app_attachment_table} "
                            "WHERE "
                            + app_where_sql
                        ),
                        tuple(app_params),
                    )

            if legacy_attachment_columns:
                if _has_column(legacy_attachment_columns, "deletedAt"):
                    cursor.execute(
                        (
                            f"UPDATE {legacy_attachment_table} "
                            "SET deletedAt = CURRENT_TIMESTAMP "
                            f"WHERE noteId IN ({placeholders}) AND deletedAt IS NULL"
                        ),
                        tuple(normalized_note_ids),
                    )
                else:
                    cursor.execute(
                        f"DELETE FROM {legacy_attachment_table} WHERE noteId IN ({placeholders})",
                        tuple(normalized_note_ids),
                    )

            cursor.execute(
                f"DELETE FROM {note_table} WHERE id IN ({placeholders})",
                tuple(normalized_note_ids),
            )

        if station_deletes:
            normalized_station_ids = _normalized_int_cache_values([int(item) for item in station_deletes])
            placeholders = _build_in_placeholders([int(item) for item in normalized_station_ids])
            cursor.execute(
                f"DELETE FROM {station_table} WHERE id IN ({placeholders})",
                tuple(normalized_station_ids),
            )

        if checklist_deletes:
            normalized_checklist_ids = _normalized_text_cache_values(checklist_deletes)
            placeholders = _build_in_placeholders(normalized_checklist_ids)
            cursor.execute(
                f"DELETE FROM {checklist_table} WHERE id IN ({placeholders})",
                tuple(normalized_checklist_ids),
            )

        return {
            "checklistIds": checklist_ids_created,
            "stationIds": station_ids_created,
            "noteIds": note_ids_created,
        }

    result = run_transaction(_work)

    if (
        checklist_creates
        or checklist_updates
        or checklist_deletes
        or station_creates
        or station_updates
        or station_deletes
        or note_creates
        or note_updates
        or note_deletes
    ):
        _invalidate_inv_checklist_all_related_scopes()
        _invalidate_inv_checklist_note_detail_cache()

    return result


def update_inv_checklist_station(
    *,
    station_row_id: int,
    fields: dict[str, object],
) -> int:
    if not fields:
        return 0
    tables = get_db_tables()
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    updates: list[str] = []
    params: list[object] = []
    if "estNum" in fields:
        updates.append("estNum = %s")
        params.append(int(fields.get("estNum")))
    if "stationCode" in fields:
        updates.append("stationCode = %s")
        params.append(_normalize_account_code(fields.get("stationCode")))
    if "status" in fields:
        updates.append("status = %s")
        params.append(_normalize_optional_input_text(fields.get("status")))
    if not updates:
        return 0

    params.append(int(station_row_id))
    query = (
        f"UPDATE {station_table} "
        "SET " + ", ".join(updates) + " "
        "WHERE id = %s"
    )
    updated = run_transaction(
        lambda cursor: (
            cursor.execute(query, tuple(params)) or int(cursor.rowcount or 0)
        )
    )
    if int(updated or 0) > 0:
        _invalidate_inv_checklist_station_scopes(include_checklists_scope=False)
        _invalidate_inv_checklist_note_detail_cache()
    return updated


def delete_inv_checklist_station(*, station_row_id: int) -> int:
    station_row_id_int = int(station_row_id)
    tables = get_db_tables()
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    deleted = run_transaction(
        lambda cursor: (
            cursor.execute(
                f"DELETE FROM {station_table} WHERE id = %s",
                (station_row_id_int,),
            )
            or int(cursor.rowcount or 0)
        )
    )
    if int(deleted or 0) > 0:
        _invalidate_inv_checklist_station_scopes(include_checklists_scope=True)
        _invalidate_inv_checklist_note_detail_cache()
    return deleted


def get_inv_checklist_note_row(*, note_id: int) -> dict | None:
    note_id_int = int(note_id)
    tables = get_db_tables()
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    rows = fetch_all(
        (
            "SELECT "
            "id, checklistStationId, amount, note, dateCreated, dateUpdated "
            f"FROM {note_table} "
            "WHERE id = %s "
            "LIMIT 1"
        ),
        (note_id_int,),
    )
    if not rows:
        return None
    return rows[0]


def list_inv_checklist_note_rows_by_ids(*, note_ids: list[int]) -> list[dict]:
    normalized_ids = _normalized_int_cache_values([int(item) for item in (note_ids or [])])
    if not normalized_ids:
        return []

    tables = get_db_tables()
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    placeholders = _build_in_placeholders(normalized_ids)
    query = (
        "SELECT "
        "id, checklistStationId, amount, note, dateCreated, dateUpdated "
        f"FROM {note_table} "
        f"WHERE id IN ({placeholders}) "
        "ORDER BY id ASC"
    )
    return fetch_all(query, tuple(normalized_ids))


def list_inv_checklist_notes(
    *,
    checklist_station_id: int | None = None,
    note_id: int | None = None,
) -> list[dict]:
    tables = get_db_tables()
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    where_clauses: list[str] = []
    params: list[object] = []

    if checklist_station_id is not None:
        where_clauses.append("checklistStationId = %s")
        params.append(int(checklist_station_id))

    if note_id is not None:
        where_clauses.append("id = %s")
        params.append(int(note_id))

    query = (
        "SELECT "
        "id, checklistStationId, amount, note, dateCreated, dateUpdated "
        f"FROM {note_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY id ASC"
    return fetch_all(query, tuple(params))


def list_inv_checklist_note_detail_rows(
    *,
    checklist_station_id: int | None = None,
    note_id: int | None = None,
    est_num: int | None = None,
    station_code: str | None = None,
    checklist_id: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    tables = get_db_tables()
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    attachment_table = _quote_table_name(tables["INVNOTEATTACHMENTS"])
    attachment_columns = _get_inv_note_attachment_columns(
        attachment_table=attachment_table,
    )
    attachment_select = _inv_note_attachment_select_clause(
        alias="a",
        columns=attachment_columns,
    )
    attachment_join_condition = "a.noteId = n.id"
    if _has_column(attachment_columns, "deletedAt"):
        attachment_join_condition += " AND a.deletedAt IS NULL"

    where_clauses: list[str] = []
    params: list[object] = []

    if checklist_station_id is not None:
        where_clauses.append("n.checklistStationId = %s")
        params.append(int(checklist_station_id))

    if note_id is not None:
        where_clauses.append("n.id = %s")
        params.append(int(note_id))

    if est_num is not None:
        where_clauses.append("s.estNum = %s")
        params.append(int(est_num))

    normalized_station_code = _normalize_account_code(station_code) if station_code else ""
    if normalized_station_code:
        where_clauses.append("UPPER(s.stationCode) = UPPER(%s)")
        params.append(normalized_station_code)

    checklist_id_text = str(checklist_id or "").strip()
    if checklist_id_text:
        where_clauses.append("s.checklistId = %s")
        params.append(checklist_id_text)

    cacheable_station_match_query = (
        est_num is not None
        and bool(normalized_station_code)
        and note_id is None
        and checklist_station_id is None
    )
    cache_key: str | None = None
    if cacheable_station_match_query:
        cache_key = _build_db_read_cache_key(
            _INV_CHECKLIST_NOTE_DETAIL_CACHE_SCOPE,
            "schema=v1",
            f"checklist_table={checklist_table}",
            f"station_table={station_table}",
            f"note_table={note_table}",
            f"attachment_table={attachment_table}",
            f"est_num={int(est_num)}",
            f"station_code={normalized_station_code}",
            f"checklist_id={checklist_id_text or '*'}",
            f"limit={int(limit) if limit is not None else '*'}",
        )
        cached_rows = _get_cached_list(
            cache_key,
            ttl_key="db_invoice_checklist_notes_ttl_time",
        )
        if cached_rows is not None:
            return cached_rows

    query = (
        "SELECT "
        "n.id AS noteId, "
        "n.checklistStationId AS checklistStationId, "
        "n.amount AS amount, "
        "n.note AS note, "
        "n.dateCreated AS dateCreated, "
        "n.dateUpdated AS dateUpdated, "
        "s.id AS stationRowId, "
        "s.checklistId AS checklistId, "
        "c.year AS checklistYear, "
        "c.month AS checklistMonth, "
        "s.estNum AS estNum, "
        "s.stationCode AS stationCode, "
        + ", ".join(attachment_select)
        + " "
        f"FROM {note_table} n "
        f"JOIN {station_table} s ON s.id = n.checklistStationId "
        f"JOIN {checklist_table} c ON c.id = s.checklistId "
        f"LEFT JOIN {attachment_table} a ON {attachment_join_condition} "
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY COALESCE(n.dateUpdated, n.dateCreated) DESC, n.id DESC, a.id ASC"
    if limit is not None:
        query += " LIMIT %s"
        params.append(int(limit))
    rows = fetch_all(query, tuple(params))
    if cache_key:
        _set_cached_value(cache_key, rows)
    return rows


def insert_inv_checklist_note(item: dict) -> int:
    tables = get_db_tables()
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    columns = ["checklistStationId"]
    values: list[object] = [int(item.get("checklistStationId"))]
    amount_value = item.get("amount")
    note_value = _normalize_input_text(item.get("note"))
    if amount_value is not None:
        columns.append("amount")
        values.append(amount_value)
    if note_value is not None:
        columns.append("note")
        values.append(note_value)
    if len(columns) == 1:
        raise ValueError("At least one of amount or note is required")
    query = (
        f"INSERT INTO {note_table} "
        f"({', '.join(columns)}) "
        f"VALUES ({', '.join(['%s'] * len(values))})"
    )

    def _work(cursor) -> int:
        cursor.execute(query, tuple(values))
        return int(cursor.lastrowid or 0)

    inserted = run_transaction(_work)
    if int(inserted or 0) > 0:
        _invalidate_inv_checklist_note_detail_cache()
    return inserted


def update_inv_checklist_note(
    *,
    note_id: int,
    fields: dict[str, object],
) -> int:
    if not fields:
        return 0
    tables = get_db_tables()
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    updates: list[str] = []
    params: list[object] = []
    if "amount" in fields:
        updates.append("amount = %s")
        params.append(fields.get("amount"))
    if "note" in fields:
        updates.append("note = %s")
        params.append(_normalize_input_text(fields.get("note")))
    if not updates:
        return 0

    params.append(int(note_id))
    query = (
        f"UPDATE {note_table} "
        "SET " + ", ".join(updates) + " "
        "WHERE id = %s"
    )
    updated = run_transaction(
        lambda cursor: (
            cursor.execute(query, tuple(params)) or int(cursor.rowcount or 0)
        )
    )
    if int(updated or 0) > 0:
        _invalidate_inv_checklist_note_detail_cache()
    return updated


def delete_inv_checklist_note(*, note_id: int) -> int:
    note_id_int = int(note_id)
    tables = get_db_tables()
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    deleted = run_transaction(
        lambda cursor: (
            cursor.execute(
                f"DELETE FROM {note_table} WHERE id = %s",
                (note_id_int,),
            )
            or int(cursor.rowcount or 0)
        )
    )
    if int(deleted or 0) > 0:
        _invalidate_inv_checklist_note_detail_cache()
    return deleted


def _list_app_note_attachments(
    *,
    tables: dict[str, str],
    note_id: int | None,
    note_ids: list[int] | None,
    attachment_id: int | None,
    tenant_slug: str | None,
    include_deleted: bool,
) -> list[dict]:
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    app_attachment_table = _get_app_attachment_table_name(tables=tables)
    app_columns = _get_app_attachment_columns(app_attachment_table=app_attachment_table)
    if not app_columns:
        return []

    select_clause = _app_attachment_select_clause(alias="aa", columns=app_columns)
    owner_id_expr = "CAST(n.id AS CHAR)"
    where_clauses: list[str] = [
        "aa.appCode = %s",
        "aa.ownerEntityType = %s",
        f"aa.ownerEntityId = {owner_id_expr}",
    ]
    params: list[object] = [
        _APP_CODE_TRADSPHERE,
        _OWNER_ENTITY_TYPE_INVOICE_CHECKLIST_NOTE,
    ]

    if attachment_id is not None:
        where_clauses.append("aa.id = %s")
        params.append(int(attachment_id))
    if note_id is not None:
        where_clauses.append("n.id = %s")
        params.append(int(note_id))
    if note_ids:
        placeholders = _build_in_placeholders([int(item) for item in note_ids])
        where_clauses.append(f"n.id IN ({placeholders})")
        params.extend([int(item) for item in note_ids])
    if _has_column(app_columns, "deletedAt") and not include_deleted:
        where_clauses.append("aa.deletedAt IS NULL")
    if _has_column(app_columns, "tenantSlug"):
        tenant_slug_value = str(tenant_slug or "").strip().lower()
        if tenant_slug_value:
            where_clauses.append("LOWER(aa.tenantSlug) = %s")
            params.append(tenant_slug_value)

    query = (
        "SELECT "
        + ", ".join(select_clause)
        + " "
        f"FROM {app_attachment_table} aa "
        f"JOIN {note_table} n ON n.id = CAST(aa.ownerEntityId AS UNSIGNED) "
        f"JOIN {station_table} s ON s.id = n.checklistStationId "
        f"JOIN {checklist_table} c ON c.id = s.checklistId "
        "WHERE "
        + " AND ".join(where_clauses)
        + " ORDER BY aa.id ASC"
    )
    return fetch_all(query, tuple(params))


def _list_legacy_note_attachments(
    *,
    tables: dict[str, str],
    note_id: int | None,
    note_ids: list[int] | None,
    attachment_id: int | None,
    tenant_slug: str | None,
    include_deleted: bool,
) -> list[dict]:
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    attachment_table = _quote_table_name(tables["INVNOTEATTACHMENTS"])
    attachment_columns = _get_inv_note_attachment_columns(
        attachment_table=attachment_table,
    )
    if not attachment_columns:
        return []
    select_clause = _inv_note_attachment_select_clause(
        alias="a",
        columns=attachment_columns,
    )
    where_clauses: list[str] = []
    params: list[object] = []

    if note_id is not None:
        where_clauses.append("a.noteId = %s")
        params.append(int(note_id))
    if note_ids:
        placeholders = _build_in_placeholders([int(item) for item in note_ids])
        where_clauses.append(f"a.noteId IN ({placeholders})")
        params.extend([int(item) for item in note_ids])
    if attachment_id is not None:
        where_clauses.append("a.id = %s")
        params.append(int(attachment_id))

    _append_inv_note_attachment_ownership_clauses(
        where_clauses=where_clauses,
        params=params,
        alias="a",
        note_alias="n",
        columns=attachment_columns,
        tenant_slug=tenant_slug,
    )
    if _has_column(attachment_columns, "deletedAt") and include_deleted:
        where_clauses = [clause for clause in where_clauses if clause != "a.deletedAt IS NULL"]

    query = (
        "SELECT "
        + ", ".join(select_clause)
        + " "
        f"FROM {attachment_table} a "
        f"JOIN {note_table} n ON n.id = a.noteId "
        f"JOIN {station_table} s ON s.id = n.checklistStationId "
        f"JOIN {checklist_table} c ON c.id = s.checklistId"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY a.id ASC"
    return fetch_all(query, tuple(params))


def _normalize_note_ids(note_ids: list[int] | None) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for raw in note_ids or []:
        parsed = int(raw)
        if parsed in seen:
            continue
        seen.add(parsed)
        normalized.append(parsed)
    return normalized


def get_inv_note_attachment_row(*, attachment_id: int, tenant_slug: str | None = None) -> dict | None:
    rows = list_inv_note_attachments(
        attachment_id=int(attachment_id),
        tenant_slug=tenant_slug,
    )
    if not rows:
        return None
    return rows[0]


def list_inv_note_attachments(
    *,
    note_id: int | None = None,
    note_ids: list[int] | None = None,
    attachment_id: int | None = None,
    tenant_slug: str | None = None,
    include_deleted: bool = False,
) -> list[dict]:
    tables = get_db_tables()
    note_id_value = int(note_id) if note_id is not None else None
    attachment_id_value = int(attachment_id) if attachment_id is not None else None
    normalized_note_ids = _normalize_note_ids(note_ids)

    app_rows = _list_app_note_attachments(
        tables=tables,
        note_id=note_id_value,
        note_ids=normalized_note_ids,
        attachment_id=attachment_id_value,
        tenant_slug=tenant_slug,
        include_deleted=include_deleted,
    )
    legacy_rows = _list_legacy_note_attachments(
        tables=tables,
        note_id=note_id_value,
        note_ids=normalized_note_ids,
        attachment_id=attachment_id_value,
        tenant_slug=tenant_slug,
        include_deleted=include_deleted,
    )

    merged: list[dict] = []
    seen_ids: set[int] = set()
    for row in app_rows + legacy_rows:
        attachment_row_id = row.get("attachmentId")
        if attachment_row_id is None:
            continue
        parsed_id = int(attachment_row_id)
        if parsed_id in seen_ids:
            continue
        seen_ids.add(parsed_id)
        merged.append(row)

    merged.sort(key=lambda row: int(row.get("attachmentId") or 0))
    return merged


def insert_inv_note_attachment(item: dict) -> int:
    tables = get_db_tables()
    app_attachment_table = _get_app_attachment_table_name(tables=tables)
    app_columns = _get_app_attachment_columns(app_attachment_table=app_attachment_table)
    note_id_value = int(item.get("noteId"))
    owner_entity_id_value = str(item.get("ownerEntityId") or str(note_id_value)).strip()
    app_code_value = str(item.get("appCode") or _APP_CODE_TRADSPHERE).strip().lower()
    owner_entity_type_value = str(
        item.get("ownerEntityType") or _OWNER_ENTITY_TYPE_INVOICE_CHECKLIST_NOTE
    ).strip().lower()
    storage_provider_value = str(item.get("storageProvider") or "external").strip().lower()
    access_url_value = _normalize_input_text(item.get("accessUrl") or item.get("url"))
    original_file_name_value = _normalize_optional_input_text(
        item.get("originalFileName") or item.get("fileName")
    )
    mime_type_value = _normalize_optional_input_text(item.get("mimeType") or item.get("fileType"))
    file_size_value = item.get("fileSize")
    storage_key_value = _normalize_optional_input_text(
        item.get("storageKey") or item.get("providerPublicId")
    )
    tenant_slug_value = _normalize_optional_input_text(item.get("tenantSlug"))
    uploaded_by_value = _normalize_optional_input_text(item.get("uploadedBy"))
    provider_metadata_value = item.get("providerMetadata")
    if provider_metadata_value is None:
        provider_metadata_value = {
            "providerAssetId": item.get("providerAssetId"),
            "providerPublicId": item.get("providerPublicId"),
            "providerResourceType": item.get("providerResourceType"),
        }
    provider_metadata_payload = json.dumps(provider_metadata_value or {}, ensure_ascii=True)

    if app_columns:
        columns: list[str] = [
            "appCode",
            "ownerEntityType",
            "ownerEntityId",
            "storageProvider",
            "accessUrl",
        ]
        values: list[object] = [
            app_code_value,
            owner_entity_type_value,
            owner_entity_id_value,
            storage_provider_value,
            access_url_value,
        ]
        if _has_column(app_columns, "tenantSlug"):
            columns.append("tenantSlug")
            values.append(tenant_slug_value)
        if _has_column(app_columns, "storageKey"):
            columns.append("storageKey")
            values.append(storage_key_value)
        if _has_column(app_columns, "originalFileName"):
            columns.append("originalFileName")
            values.append(original_file_name_value)
        if _has_column(app_columns, "mimeType"):
            columns.append("mimeType")
            values.append(mime_type_value)
        if _has_column(app_columns, "fileSize"):
            columns.append("fileSize")
            values.append(int(file_size_value) if file_size_value is not None else None)
        if _has_column(app_columns, "providerMetadata"):
            columns.append("providerMetadata")
            values.append(provider_metadata_payload)
        if _has_column(app_columns, "uploadedBy"):
            columns.append("uploadedBy")
            values.append(uploaded_by_value)

        placeholders = ", ".join(["%s"] * len(columns))
        query = (
            f"INSERT INTO {app_attachment_table} "
            f"({', '.join(columns)}) "
            f"VALUES ({placeholders})"
        )

        def _work_app(cursor) -> int:
            cursor.execute(query, tuple(values))
            return int(cursor.lastrowid or 0)

        inserted = run_transaction(_work_app)
        if int(inserted or 0) > 0:
            _invalidate_inv_checklist_note_detail_cache()
        return inserted

    attachment_table = _quote_table_name(tables["INVNOTEATTACHMENTS"])
    attachment_columns = _get_inv_note_attachment_columns(
        attachment_table=attachment_table,
    )
    columns = ["noteId", "url", "fileName", "fileType"]
    values = [
        note_id_value,
        _normalize_input_text(item.get("url") or access_url_value),
        _normalize_optional_input_text(item.get("fileName") or original_file_name_value),
        _normalize_optional_input_text(item.get("fileType") or mime_type_value),
    ]
    for optional_column in _INV_NOTE_ATTACHMENT_OPTIONAL_COLUMNS:
        if optional_column == "deletedAt":
            continue
        if not _has_column(attachment_columns, optional_column):
            continue
        values_map = {
            "storageProvider": storage_provider_value,
            "providerAssetId": (item.get("providerAssetId") or None),
            "providerPublicId": (item.get("providerPublicId") or storage_key_value),
            "providerResourceType": (item.get("providerResourceType") or None),
            "accessUrl": access_url_value,
            "originalFileName": original_file_name_value,
            "mimeType": mime_type_value,
            "fileSize": int(file_size_value) if file_size_value is not None else None,
            "uploadedBy": uploaded_by_value,
            "tenantSlug": tenant_slug_value,
            "ownerEntityType": owner_entity_type_value,
            "ownerEntityId": owner_entity_id_value,
        }
        columns.append(optional_column)
        values.append(values_map.get(optional_column))

    placeholders = ", ".join(["%s"] * len(columns))
    query = (
        f"INSERT INTO {attachment_table} "
        f"({', '.join(columns)}) "
        f"VALUES ({placeholders})"
    )

    def _work_legacy(cursor) -> int:
        cursor.execute(query, tuple(values))
        return int(cursor.lastrowid or 0)

    inserted = run_transaction(_work_legacy)
    if int(inserted or 0) > 0:
        _invalidate_inv_checklist_note_detail_cache()
    return inserted


def update_inv_note_attachment(
    *,
    attachment_id: int,
    fields: dict[str, object],
    tenant_slug: str | None = None,
) -> int:
    if not fields:
        return 0
    current = get_inv_note_attachment_row(
        attachment_id=int(attachment_id),
        tenant_slug=tenant_slug,
    )
    if current is None:
        return 0
    source = str(current.get("attachmentSource") or "").strip().lower()
    if source == "app_attachment":
        tables = get_db_tables()
        app_attachment_table = _get_app_attachment_table_name(tables=tables)
        app_columns = _get_app_attachment_columns(app_attachment_table=app_attachment_table)
        if not app_columns:
            return 0
        updates: list[str] = []
        params: list[object] = []
        if "url" in fields:
            updates.append("accessUrl = %s")
            params.append(_normalize_input_text(fields.get("url")))
        if "fileName" in fields:
            updates.append("originalFileName = %s")
            params.append(_normalize_optional_input_text(fields.get("fileName")))
        if "fileType" in fields:
            updates.append("mimeType = %s")
            params.append(_normalize_optional_input_text(fields.get("fileType")))
        if not updates:
            return 0
        params.append(int(attachment_id))
        query = (
            f"UPDATE {app_attachment_table} "
            "SET " + ", ".join(updates) + " "
            "WHERE id = %s"
        )
        updated = run_transaction(
            lambda cursor: (
                cursor.execute(query, tuple(params)) or int(cursor.rowcount or 0)
            )
        )
        if int(updated or 0) > 0:
            _invalidate_inv_checklist_note_detail_cache()
        return int(updated or 0)

    tables = get_db_tables()
    attachment_table = _quote_table_name(tables["INVNOTEATTACHMENTS"])
    attachment_columns = _get_inv_note_attachment_columns(attachment_table=attachment_table)
    updates = []
    params: list[object] = []
    if "url" in fields:
        updates.append("url = %s")
        params.append(_normalize_input_text(fields.get("url")))
    if "fileName" in fields:
        updates.append("fileName = %s")
        params.append(_normalize_optional_input_text(fields.get("fileName")))
    if "fileType" in fields:
        updates.append("fileType = %s")
        params.append(_normalize_optional_input_text(fields.get("fileType")))
    for optional_column in _INV_NOTE_ATTACHMENT_OPTIONAL_COLUMNS:
        if optional_column in {"deletedAt"}:
            continue
        if optional_column not in fields or not _has_column(attachment_columns, optional_column):
            continue
        updates.append(f"{optional_column} = %s")
        params.append(fields.get(optional_column))
    if not updates:
        return 0
    params.append(int(attachment_id))
    query = (
        f"UPDATE {attachment_table} "
        "SET " + ", ".join(updates) + " "
        "WHERE id = %s"
    )
    updated = run_transaction(
        lambda cursor: (
            cursor.execute(query, tuple(params)) or int(cursor.rowcount or 0)
        )
    )
    if int(updated or 0) > 0:
        _invalidate_inv_checklist_note_detail_cache()
    return int(updated or 0)


def delete_inv_note_attachment(*, attachment_id: int, tenant_slug: str | None = None) -> int:
    target_row = get_inv_note_attachment_row(
        attachment_id=int(attachment_id),
        tenant_slug=tenant_slug,
    )
    if target_row is None:
        return 0
    source = str(target_row.get("attachmentSource") or "").strip().lower()
    tables = get_db_tables()
    if source == "app_attachment":
        app_attachment_table = _get_app_attachment_table_name(tables=tables)
        app_columns = _get_app_attachment_columns(app_attachment_table=app_attachment_table)
        if not app_columns:
            return 0
        if _has_column(app_columns, "deletedAt"):
            query = f"UPDATE {app_attachment_table} SET deletedAt = CURRENT_TIMESTAMP WHERE id = %s"
        else:
            query = f"DELETE FROM {app_attachment_table} WHERE id = %s"
        deleted = run_transaction(
            lambda cursor: (
                cursor.execute(query, (int(attachment_id),))
                or int(cursor.rowcount or 0)
            )
        )
        if int(deleted or 0) > 0:
            _invalidate_inv_checklist_note_detail_cache()
        return int(deleted or 0)

    attachment_table = _quote_table_name(tables["INVNOTEATTACHMENTS"])
    attachment_columns = _get_inv_note_attachment_columns(attachment_table=attachment_table)
    checklist_table = _quote_table_name(tables["INVCHECKLISTS"])
    station_table = _quote_table_name(tables["INVCHECKLISTSTATIONS"])
    note_table = _quote_table_name(tables["INVCHECKLISTNOTES"])
    where_clauses = ["a.id = %s"]
    params: list[object] = [int(attachment_id)]
    _append_inv_note_attachment_ownership_clauses(
        where_clauses=where_clauses,
        params=params,
        alias="a",
        note_alias="n",
        columns=attachment_columns,
        tenant_slug=tenant_slug,
    )
    where_sql = " AND ".join(where_clauses)
    if _has_column(attachment_columns, "deletedAt"):
        query = (
            f"UPDATE {attachment_table} a "
            f"JOIN {note_table} n ON n.id = a.noteId "
            f"JOIN {station_table} s ON s.id = n.checklistStationId "
            f"JOIN {checklist_table} c ON c.id = s.checklistId "
            "SET a.deletedAt = CURRENT_TIMESTAMP "
            "WHERE "
            + where_sql
        )
    else:
        query = (
            f"DELETE a FROM {attachment_table} a "
            f"JOIN {note_table} n ON n.id = a.noteId "
            f"JOIN {station_table} s ON s.id = n.checklistStationId "
            f"JOIN {checklist_table} c ON c.id = s.checklistId "
            "WHERE "
            + where_sql
        )
    deleted = run_transaction(
        lambda cursor: (
            cursor.execute(query, tuple(params))
            or int(cursor.rowcount or 0)
        )
    )
    if int(deleted or 0) > 0:
        _invalidate_inv_checklist_note_detail_cache()
    return int(deleted or 0)


def soft_delete_app_attachments_for_invoice_note(
    *,
    note_id: int,
    tenant_slug: str | None = None,
) -> int:
    tables = get_db_tables()
    app_attachment_table = _get_app_attachment_table_name(tables=tables)
    app_columns = _get_app_attachment_columns(app_attachment_table=app_attachment_table)
    if not app_columns:
        return 0

    params: list[object] = [
        _APP_CODE_TRADSPHERE,
        _OWNER_ENTITY_TYPE_INVOICE_CHECKLIST_NOTE,
        str(int(note_id)),
    ]
    where_clauses: list[str] = [
        "appCode = %s",
        "ownerEntityType = %s",
        "ownerEntityId = %s",
    ]
    if _has_column(app_columns, "tenantSlug"):
        tenant_slug_value = str(tenant_slug or "").strip().lower()
        if tenant_slug_value:
            where_clauses.append("LOWER(tenantSlug) = %s")
            params.append(tenant_slug_value)

    where_sql = " AND ".join(where_clauses)
    if _has_column(app_columns, "deletedAt"):
        query = (
            f"UPDATE {app_attachment_table} "
            "SET deletedAt = CURRENT_TIMESTAMP "
            "WHERE "
            + where_sql
            + " AND deletedAt IS NULL"
        )
    else:
        query = (
            f"DELETE FROM {app_attachment_table} "
            "WHERE "
            + where_sql
        )
    deleted = run_transaction(
        lambda cursor: (
            cursor.execute(query, tuple(params))
            or int(cursor.rowcount or 0)
        )
    )
    if int(deleted or 0) > 0:
        _invalidate_inv_checklist_note_detail_cache()
    return int(deleted or 0)


def count_inv_note_attachments(
    *,
    note_id: int,
    include_deleted: bool = False,
    tenant_slug: str | None = None,
) -> int:
    rows = list_inv_note_attachments(
        note_id=int(note_id),
        tenant_slug=tenant_slug,
        include_deleted=include_deleted,
    )
    return len(rows)
