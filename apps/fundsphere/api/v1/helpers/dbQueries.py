from __future__ import annotations
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import re
import uuid
from zoneinfo import ZoneInfo

from shared.db import fetch_all, run_transaction
from shared.tenant import get_tenant_id, get_timezone
from shared.tenantDataCache import (
    delete_tenant_shared_cache_values_by_prefix,
    get_shared_cache_ttl_seconds,
    get_tenant_shared_cache_value,
    set_tenant_shared_cache_value,
)

from apps.fundsphere.api.v1.helpers.config import get_db_tables


# ============================================================
# CONSTANTS
# ============================================================

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")

_ACCOUNT_CODE_CANDIDATES = ("accountCode", "account_code", "code")
_ACCOUNT_NAME_CANDIDATES = ("accountName", "account_name", "name")
_ACTIVE_CANDIDATES = ("active", "isActive", "is_active", "enabled")
_SERVICE_ID_CANDIDATES = ("serviceId", "service_id", "id")
_SERVICE_NAME_CANDIDATES = ("serviceName", "service_name", "name")
_COMMISSION_CANDIDATES = ("commission", "commissionRate", "commission_rate")
_SERVICE_DEPARTMENT_CODE_CANDIDATES = ("departmentCode", "department_code")
_DEPARTMENT_CODE_CANDIDATES = ("code", "departmentCode", "department_code")
_DEPARTMENT_NAME_CANDIDATES = ("departmentName", "department_name", "name")
_DEPARTMENT_LISTING_ORDER_CANDIDATES = (
    "depListingOrder",
    "departmentListingOrder",
    "listingOrder",
    "listing_order",
    "order",
)
_BUDGET_ID_CANDIDATES = ("id", "budgetId", "budget_id")
_BUDGET_ACCOUNT_CODE_CANDIDATES = ("accountCode", "account_code")
_BUDGET_SERVICE_ID_CANDIDATES = ("serviceId", "service_id")
_BUDGET_YEAR_CANDIDATES = ("year",)
_BUDGET_MONTH_CANDIDATES = ("month",)
_BUDGET_SUBSERVICE_CANDIDATES = ("subService", "sub_service")
_BUDGET_GROSS_AMOUNT_CANDIDATES = ("grossAmount", "gross_amount")
_BUDGET_COMMISSION_CANDIDATES = ("commission", "commissionRate", "commission_rate")
_BUDGET_NET_ADJUSTMENT_CANDIDATES = ("netAdjustment", "net_adjustment")
_BUDGET_NOTE_CANDIDATES = ("note",)
_HISTORY_ID_CANDIDATES = ("id", "historyId", "history_id")
_HISTORY_BUDGET_ID_CANDIDATES = ("budgetId", "budget_id")
_HISTORY_FIELD_CANDIDATES = ("field", "fieldName", "columnName", "column", "key")
_HISTORY_OLD_VALUE_CANDIDATES = (
    "oldValue",
    "old_value",
    "beforeValue",
    "previousValue",
    "fromValue",
)
_HISTORY_NEW_VALUE_CANDIDATES = (
    "newValue",
    "new_value",
    "afterValue",
    "currentValue",
    "toValue",
)
_HISTORY_NOTE_CANDIDATES = ("note", "notes", "description", "detail", "message")
_HISTORY_SOURCE_CANDIDATES = ("source", "action", "event", "type")
_HISTORY_CREATED_AT_CANDIDATES = ("dateCreated", "createdAt", "created_at", "timestamp")
_HISTORY_UPDATED_AT_CANDIDATES = ("dateUpdated", "updatedAt", "updated_at")
_HISTORY_TENANT_ID_CANDIDATES = ("tenantId", "tenant_id")
_HISTORY_CHANGED_BY_CANDIDATES = ("changedBy", "updatedBy", "createdBy", "actor")
_HISTORY_ACCOUNT_CODE_CANDIDATES = ("accountCode", "account_code")
_HISTORY_SERVICE_ID_CANDIDATES = ("serviceId", "service_id")
_HISTORY_MONTH_CANDIDATES = ("month",)
_HISTORY_YEAR_CANDIDATES = ("year",)
_BUDGET_CHANGE_HISTORY_BUDGET_ID_CANDIDATES = ("budgetId", "budget_id")
_BUDGET_CHANGE_HISTORY_ACTION_TYPE_CANDIDATES = ("actionType", "action_type")
_BUDGET_CHANGE_HISTORY_CHANGED_FIELDS_CANDIDATES = (
    "changedFields",
    "changed_fields",
    "fields",
)
_BUDGET_CHANGE_HISTORY_OLD_DATA_CANDIDATES = ("oldData", "old_data")
_BUDGET_CHANGE_HISTORY_NEW_DATA_CANDIDATES = ("newData", "new_data")
_BUDGET_CHANGE_HISTORY_CHANGED_BY_CANDIDATES = ("changedBy", "changed_by")
_BUDGET_CHANGE_HISTORY_NOTE_CANDIDATES = ("note", "notes")
_BUDGET_CHANGE_HISTORY_CREATED_AT_CANDIDATES = (
    "dateCreated",
    "createdAt",
    "created_at",
    "timestamp",
)
_BUDGET_CHANGE_HISTORY_ID_CANDIDATES = ("id", "historyId", "history_id")
_BUDGET_CHANGE_FIELD_DISPLAY_ORDER = (
    "subService",
    "grossAmount",
    "commission",
    "netAdjustment",
    "note",
)
_APP_NAME = "FundSphere"
_SHARED_DB_READ_CACHE_BUCKET = "db_reads"
_ACCOUNTS_CACHE_KEY_PREFIX = "accounts::"
_SERVICES_CACHE_KEY_PREFIX = "services::"
_BUDGET_DATA_CACHE_KEY_PREFIX = "budget_data::"
_DEFAULT_DB_READ_CACHE_TTL_SECONDS = 300
_NUMERIC_SQL_TYPE_TOKENS = (
    "int",
    "decimal",
    "numeric",
    "float",
    "double",
    "real",
)


# ============================================================
# HELPERS
# ============================================================


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


def _resolve_column_name(
    available: dict[str, str],
    candidates: tuple[str, ...],
) -> str | None:
    for candidate in candidates:
        resolved = available.get(candidate.lower())
        if resolved:
            return resolved
    return None


def _resolve_accounts_columns(accounts_table: str) -> tuple[str, str, str | None]:
    quoted_table = _quote_table_name(accounts_table)
    columns = fetch_all(f"SHOW COLUMNS FROM {quoted_table}")

    available: dict[str, str] = {}
    for column in columns:
        name = str(column.get("Field") or "").strip()
        if name:
            available[name.lower()] = name

    code_col = _resolve_column_name(available, _ACCOUNT_CODE_CANDIDATES)
    name_col = _resolve_column_name(available, _ACCOUNT_NAME_CANDIDATES)
    active_col = _resolve_column_name(available, _ACTIVE_CANDIDATES)

    if not code_col:
        raise ValueError("Accounts table is missing account code column")
    if not name_col:
        raise ValueError("Accounts table is missing account name column")

    return code_col, name_col, active_col


def _load_table_columns(table_name: str) -> dict[str, str]:
    quoted_table = _quote_table_name(table_name)
    columns = fetch_all(f"SHOW COLUMNS FROM {quoted_table}")
    available: dict[str, str] = {}
    for column in columns:
        name = str(column.get("Field") or "").strip()
        if name:
            available[name.lower()] = name
    return available


def _load_table_column_specs(table_name: str) -> dict[str, dict[str, object]]:
    quoted_table = _quote_table_name(table_name)
    rows = fetch_all(f"SHOW COLUMNS FROM {quoted_table}")
    specs: dict[str, dict[str, object]] = {}
    for row in rows:
        name = str(row.get("Field") or "").strip()
        if not name:
            continue
        specs[name.lower()] = row
    return specs


def _resolve_service_identity_columns(services_table: str) -> tuple[str, str]:
    service_columns = _load_table_columns(services_table)
    service_id_col = _resolve_column_name(service_columns, _SERVICE_ID_CANDIDATES)
    service_name_col = _resolve_column_name(service_columns, _SERVICE_NAME_CANDIDATES)

    missing: list[str] = []
    if not service_id_col:
        missing.append("services.serviceId")
    if not service_name_col:
        missing.append("services.serviceName")
    if missing:
        raise ValueError(
            "Budget create is missing required DB columns: " + ", ".join(missing)
        )

    return service_id_col, service_name_col


def _resolve_services_columns(
    services_table: str,
    departments_table: str,
) -> dict[str, str]:
    service_columns = _load_table_columns(services_table)
    department_columns = _load_table_columns(departments_table)

    service_id_col = _resolve_column_name(service_columns, _SERVICE_ID_CANDIDATES)
    service_name_col = _resolve_column_name(service_columns, _SERVICE_NAME_CANDIDATES)
    commission_col = _resolve_column_name(service_columns, _COMMISSION_CANDIDATES)
    service_department_code_col = _resolve_column_name(
        service_columns,
        _SERVICE_DEPARTMENT_CODE_CANDIDATES,
    )
    department_code_col = _resolve_column_name(
        department_columns,
        _DEPARTMENT_CODE_CANDIDATES,
    )
    department_name_col = _resolve_column_name(
        department_columns,
        _DEPARTMENT_NAME_CANDIDATES,
    )
    department_listing_order_col = _resolve_column_name(
        department_columns,
        _DEPARTMENT_LISTING_ORDER_CANDIDATES,
    )

    missing: list[str] = []
    if not service_id_col:
        missing.append("services.serviceId")
    if not service_name_col:
        missing.append("services.serviceName")
    if not commission_col:
        missing.append("services.commission")
    if not service_department_code_col:
        missing.append("services.departmentCode")
    if not department_code_col:
        missing.append("departments.code")
    if not department_name_col:
        missing.append("departments.departmentName")
    if not department_listing_order_col:
        missing.append("departments.depListingOrder")
    if missing:
        raise ValueError(f"Services sync is missing required DB columns: {', '.join(missing)}")

    return {
        "service_id_col": service_id_col or "",
        "service_name_col": service_name_col or "",
        "commission_col": commission_col or "",
        "service_department_code_col": service_department_code_col or "",
        "department_code_col": department_code_col or "",
        "department_name_col": department_name_col or "",
        "department_listing_order_col": department_listing_order_col or "",
    }


def _resolve_budget_data_columns(
    *,
    accounts_table: str,
    budgets_table: str,
    services_table: str,
    departments_table: str,
) -> dict[str, str]:
    account_columns = _load_table_columns(accounts_table)
    budget_columns = _load_table_columns(budgets_table)
    service_columns = _load_table_columns(services_table)
    department_columns = _load_table_columns(departments_table)

    account_code_col = _resolve_column_name(account_columns, _ACCOUNT_CODE_CANDIDATES)
    account_name_col = _resolve_column_name(account_columns, _ACCOUNT_NAME_CANDIDATES)
    service_id_col = _resolve_column_name(service_columns, _SERVICE_ID_CANDIDATES)
    service_name_col = _resolve_column_name(service_columns, _SERVICE_NAME_CANDIDATES)
    service_department_code_col = _resolve_column_name(
        service_columns,
        _SERVICE_DEPARTMENT_CODE_CANDIDATES,
    )
    department_code_col = _resolve_column_name(
        department_columns,
        _DEPARTMENT_CODE_CANDIDATES,
    )
    department_listing_order_col = _resolve_column_name(
        department_columns,
        _DEPARTMENT_LISTING_ORDER_CANDIDATES,
    )
    budget_id_col = _resolve_column_name(budget_columns, _BUDGET_ID_CANDIDATES)
    budget_account_code_col = _resolve_column_name(
        budget_columns,
        _BUDGET_ACCOUNT_CODE_CANDIDATES,
    )
    budget_service_id_col = _resolve_column_name(
        budget_columns,
        _BUDGET_SERVICE_ID_CANDIDATES,
    )
    budget_year_col = _resolve_column_name(budget_columns, _BUDGET_YEAR_CANDIDATES)
    budget_month_col = _resolve_column_name(budget_columns, _BUDGET_MONTH_CANDIDATES)
    budget_sub_service_col = _resolve_column_name(
        budget_columns,
        _BUDGET_SUBSERVICE_CANDIDATES,
    )
    budget_gross_amount_col = _resolve_column_name(
        budget_columns,
        _BUDGET_GROSS_AMOUNT_CANDIDATES,
    )
    budget_commission_col = _resolve_column_name(
        budget_columns,
        _BUDGET_COMMISSION_CANDIDATES,
    )
    budget_net_adjustment_col = _resolve_column_name(
        budget_columns,
        _BUDGET_NET_ADJUSTMENT_CANDIDATES,
    )
    budget_note_col = _resolve_column_name(budget_columns, _BUDGET_NOTE_CANDIDATES)

    missing: list[str] = []
    if not account_code_col:
        missing.append("accounts.accountCode")
    if not account_name_col:
        missing.append("accounts.accountName")
    if not service_id_col:
        missing.append("services.serviceId")
    if not service_name_col:
        missing.append("services.serviceName")
    if not service_department_code_col:
        missing.append("services.departmentCode")
    if not department_code_col:
        missing.append("departments.code")
    if not department_listing_order_col:
        missing.append("departments.depListingOrder")
    if not budget_id_col:
        missing.append("budgets.id")
    if not budget_account_code_col:
        missing.append("budgets.accountCode")
    if not budget_service_id_col:
        missing.append("budgets.serviceId")
    if not budget_year_col:
        missing.append("budgets.year")
    if not budget_month_col:
        missing.append("budgets.month")
    if not budget_sub_service_col:
        missing.append("budgets.subService")
    if not budget_gross_amount_col:
        missing.append("budgets.grossAmount")
    if not budget_commission_col:
        missing.append("budgets.commission")
    if not budget_net_adjustment_col:
        missing.append("budgets.netAdjustment")
    if not budget_note_col:
        missing.append("budgets.note")
    if missing:
        raise ValueError(
            "Budget data load is missing required DB columns: " + ", ".join(missing)
        )

    return {
        "account_code_col": account_code_col or "",
        "account_name_col": account_name_col or "",
        "service_id_col": service_id_col or "",
        "service_name_col": service_name_col or "",
        "service_department_code_col": service_department_code_col or "",
        "department_code_col": department_code_col or "",
        "department_listing_order_col": department_listing_order_col or "",
        "budget_id_col": budget_id_col or "",
        "budget_account_code_col": budget_account_code_col or "",
        "budget_service_id_col": budget_service_id_col or "",
        "budget_year_col": budget_year_col or "",
        "budget_month_col": budget_month_col or "",
        "budget_sub_service_col": budget_sub_service_col or "",
        "budget_gross_amount_col": budget_gross_amount_col or "",
        "budget_commission_col": budget_commission_col or "",
        "budget_net_adjustment_col": budget_net_adjustment_col or "",
        "budget_note_col": budget_note_col or "",
    }


def _resolve_budget_update_columns(budgets_table: str) -> dict[str, str]:
    budget_columns = _load_table_columns(budgets_table)
    budget_id_col = _resolve_column_name(budget_columns, _BUDGET_ID_CANDIDATES)
    budget_account_code_col = _resolve_column_name(
        budget_columns,
        _BUDGET_ACCOUNT_CODE_CANDIDATES,
    )
    budget_service_id_col = _resolve_column_name(
        budget_columns,
        _BUDGET_SERVICE_ID_CANDIDATES,
    )
    budget_year_col = _resolve_column_name(budget_columns, _BUDGET_YEAR_CANDIDATES)
    budget_month_col = _resolve_column_name(budget_columns, _BUDGET_MONTH_CANDIDATES)
    budget_sub_service_col = _resolve_column_name(
        budget_columns,
        _BUDGET_SUBSERVICE_CANDIDATES,
    )
    budget_gross_amount_col = _resolve_column_name(
        budget_columns,
        _BUDGET_GROSS_AMOUNT_CANDIDATES,
    )
    budget_commission_col = _resolve_column_name(
        budget_columns,
        _BUDGET_COMMISSION_CANDIDATES,
    )
    budget_net_adjustment_col = _resolve_column_name(
        budget_columns,
        _BUDGET_NET_ADJUSTMENT_CANDIDATES,
    )
    budget_note_col = _resolve_column_name(budget_columns, _BUDGET_NOTE_CANDIDATES)

    missing: list[str] = []
    if not budget_id_col:
        missing.append("budgets.id")
    if not budget_account_code_col:
        missing.append("budgets.accountCode")
    if not budget_service_id_col:
        missing.append("budgets.serviceId")
    if not budget_year_col:
        missing.append("budgets.year")
    if not budget_month_col:
        missing.append("budgets.month")
    if not budget_sub_service_col:
        missing.append("budgets.subService")
    if not budget_gross_amount_col:
        missing.append("budgets.grossAmount")
    if not budget_commission_col:
        missing.append("budgets.commission")
    if not budget_net_adjustment_col:
        missing.append("budgets.netAdjustment")
    if not budget_note_col:
        missing.append("budgets.note")
    if missing:
        raise ValueError(
            "Budget update is missing required DB columns: " + ", ".join(missing)
        )

    return {
        "budget_id_col": budget_id_col or "",
        "budget_account_code_col": budget_account_code_col or "",
        "budget_service_id_col": budget_service_id_col or "",
        "budget_year_col": budget_year_col or "",
        "budget_month_col": budget_month_col or "",
        "budget_sub_service_col": budget_sub_service_col or "",
        "budget_gross_amount_col": budget_gross_amount_col or "",
        "budget_commission_col": budget_commission_col or "",
        "budget_net_adjustment_col": budget_net_adjustment_col or "",
        "budget_note_col": budget_note_col or "",
    }


def _resolve_change_history_columns(change_histories_table: str) -> dict[str, str]:
    history_columns = _load_table_columns(change_histories_table)
    return {
        "id": _resolve_column_name(history_columns, _HISTORY_ID_CANDIDATES) or "",
        "budget_id": _resolve_column_name(
            history_columns,
            _HISTORY_BUDGET_ID_CANDIDATES,
        )
        or "",
        "field": _resolve_column_name(history_columns, _HISTORY_FIELD_CANDIDATES) or "",
        "old_value": _resolve_column_name(
            history_columns,
            _HISTORY_OLD_VALUE_CANDIDATES,
        )
        or "",
        "new_value": _resolve_column_name(
            history_columns,
            _HISTORY_NEW_VALUE_CANDIDATES,
        )
        or "",
        "note": _resolve_column_name(history_columns, _HISTORY_NOTE_CANDIDATES) or "",
        "source": _resolve_column_name(history_columns, _HISTORY_SOURCE_CANDIDATES) or "",
        "created_at": _resolve_column_name(
            history_columns,
            _HISTORY_CREATED_AT_CANDIDATES,
        )
        or "",
        "updated_at": _resolve_column_name(
            history_columns,
            _HISTORY_UPDATED_AT_CANDIDATES,
        )
        or "",
        "tenant_id": _resolve_column_name(
            history_columns,
            _HISTORY_TENANT_ID_CANDIDATES,
        )
        or "",
        "changed_by": _resolve_column_name(
            history_columns,
            _HISTORY_CHANGED_BY_CANDIDATES,
        )
        or "",
        "account_code": _resolve_column_name(
            history_columns,
            _HISTORY_ACCOUNT_CODE_CANDIDATES,
        )
        or "",
        "service_id": _resolve_column_name(
            history_columns,
            _HISTORY_SERVICE_ID_CANDIDATES,
        )
        or "",
        "month": _resolve_column_name(history_columns, _HISTORY_MONTH_CANDIDATES) or "",
        "year": _resolve_column_name(history_columns, _HISTORY_YEAR_CANDIDATES) or "",
    }


def _to_decimal(value: object, *, scale: int) -> Decimal:
    if value is None or value == "":
        dec_value = Decimal("0")
    elif isinstance(value, Decimal):
        dec_value = value
    else:
        try:
            dec_value = Decimal(str(value).strip())
        except (InvalidOperation, ValueError):
            dec_value = Decimal("0")

    precision = Decimal("1").scaleb(-scale)
    return dec_value.quantize(precision, rounding=ROUND_HALF_UP)


def _build_accounts_cache_key(accounts_table: str) -> str:
    return f"{_ACCOUNTS_CACHE_KEY_PREFIX}{str(accounts_table or '').strip().lower()}"


def _build_services_cache_key(services_table: str, departments_table: str) -> str:
    return (
        f"{_SERVICES_CACHE_KEY_PREFIX}"
        f"{str(services_table or '').strip().lower()}::"
        f"{str(departments_table or '').strip().lower()}"
    )


def _build_budget_data_cache_key_prefix(
    *,
    budgets_table: str,
) -> str:
    return (
        f"{_BUDGET_DATA_CACHE_KEY_PREFIX}"
        f"{str(budgets_table or '').strip().lower()}::"
    )


def _normalize_budget_bucket(
    *,
    account_code: object,
    month: object,
    year: object,
) -> tuple[str, int, int] | None:
    normalized_account_code = str(account_code or "").strip().upper()
    if not normalized_account_code:
        return None
    try:
        normalized_month = int(month)
        normalized_year = int(year)
    except (TypeError, ValueError):
        return None
    if normalized_month < 1 or normalized_month > 12 or normalized_year <= 0:
        return None
    return normalized_account_code, normalized_month, normalized_year


def _build_budget_data_cache_key(
    *,
    budgets_table: str,
    account_code: str,
    month: int,
    year: int,
) -> str:
    base_prefix = _build_budget_data_cache_key_prefix(
        budgets_table=budgets_table,
    )
    return f"{base_prefix}{account_code.upper()}::{month}/{year}"


def _build_budget_data_buckets(
    *,
    account_codes: list[str],
    periods: list[tuple[int, int]],
) -> list[tuple[str, int, int]]:
    buckets: list[tuple[str, int, int]] = []
    seen: set[tuple[str, int, int]] = set()
    for account_code in account_codes:
        for month, year in periods:
            bucket = _normalize_budget_bucket(
                account_code=account_code,
                month=month,
                year=year,
            )
            if not bucket or bucket in seen:
                continue
            seen.add(bucket)
            buckets.append(bucket)
    buckets.sort(key=lambda item: (item[0], item[2], item[1]))
    return buckets


def _is_valid_cached_rows(value: object) -> bool:
    if not isinstance(value, list):
        return False
    return all(isinstance(row, dict) for row in value)


def _to_int(value: object, *, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _budget_data_sort_key(row: dict[str, object]) -> tuple[object, ...]:
    return (
        str(row.get("accountName") or "").strip(),
        -_to_int(row.get("year")),
        -_to_int(row.get("month")),
        _to_int(row.get("depListingOrder")),
        str(row.get("serviceName") or "").strip(),
        str(row.get("subService") or "").strip(),
    )


def _fetch_master_budget_control_budget_rows_for_buckets(
    *,
    accounts_table: str,
    budgets_table: str,
    services_table: str,
    departments_table: str,
    budget_change_histories_table: str,
    buckets: list[tuple[str, int, int]],
) -> list[dict]:
    if not buckets:
        return []

    columns = _resolve_budget_data_columns(
        accounts_table=accounts_table,
        budgets_table=budgets_table,
        services_table=services_table,
        departments_table=departments_table,
    )

    quoted_accounts_table = _quote_table_name(accounts_table)
    quoted_budgets_table = _quote_table_name(budgets_table)
    quoted_services_table = _quote_table_name(services_table)
    quoted_departments_table = _quote_table_name(departments_table)

    account_code_expr = _quote_identifier(columns["account_code_col"])
    account_name_expr = _quote_identifier(columns["account_name_col"])
    service_id_expr = _quote_identifier(columns["service_id_col"])
    service_name_expr = _quote_identifier(columns["service_name_col"])
    service_department_code_expr = _quote_identifier(columns["service_department_code_col"])
    department_code_expr = _quote_identifier(columns["department_code_col"])
    department_listing_order_expr = _quote_identifier(
        columns["department_listing_order_col"]
    )
    budget_id_expr = _quote_identifier(columns["budget_id_col"])
    budget_account_code_expr = _quote_identifier(columns["budget_account_code_col"])
    budget_service_id_expr = _quote_identifier(columns["budget_service_id_col"])
    budget_year_expr = _quote_identifier(columns["budget_year_col"])
    budget_month_expr = _quote_identifier(columns["budget_month_col"])
    budget_sub_service_expr = _quote_identifier(columns["budget_sub_service_col"])
    budget_gross_amount_expr = _quote_identifier(columns["budget_gross_amount_col"])
    budget_commission_expr = _quote_identifier(columns["budget_commission_col"])
    budget_net_adjustment_expr = _quote_identifier(columns["budget_net_adjustment_col"])
    budget_note_expr = _quote_identifier(columns["budget_note_col"])

    bucket_conditions: list[str] = []
    bucket_params: list[object] = []
    for account_code, month, year in buckets:
        bucket_conditions.append(
            f"(a.{account_code_expr} = %s AND b.{budget_month_expr} = %s AND b.{budget_year_expr} = %s)"
        )
        bucket_params.extend([account_code, month, year])

    query = (
        "SELECT "
        f"a.{account_code_expr} AS accountCode, "
        f"COALESCE(b.{budget_id_expr}, '') AS budgetId, "
        f"a.{account_name_expr} AS accountName, "
        f"b.{budget_year_expr} AS year, "
        f"b.{budget_month_expr} AS month, "
        f"d.{department_listing_order_expr} AS depListingOrder, "
        f"s.{service_name_expr} AS serviceName, "
        f"COALESCE(b.{budget_sub_service_expr}, '') AS subService, "
        f"COALESCE(b.{budget_gross_amount_expr}, 0) AS grossAmount, "
        f"COALESCE(b.{budget_commission_expr}, 0) AS commission, "
        f"COALESCE(b.{budget_net_adjustment_expr}, 0) AS netAdjustment, "
        f"COALESCE(b.{budget_note_expr}, '') AS note "
        f"FROM {quoted_services_table} s "
        f"INNER JOIN {quoted_departments_table} d "
        f"ON s.{service_department_code_expr} = d.{department_code_expr} "
        f"INNER JOIN {quoted_budgets_table} b "
        f"ON s.{service_id_expr} = b.{budget_service_id_expr} "
        f"INNER JOIN {quoted_accounts_table} a "
        f"ON b.{budget_account_code_expr} = a.{account_code_expr} "
        f"WHERE {' OR '.join(bucket_conditions)} "
        f"ORDER BY a.{account_name_expr} ASC, "
        f"b.{budget_year_expr} DESC, "
        f"b.{budget_month_expr} DESC, "
        f"d.{department_listing_order_expr} ASC, "
        f"s.{service_name_expr} ASC, "
        f"b.{budget_sub_service_expr} ASC"
    )
    rows = fetch_all(query, tuple(bucket_params))
    if not rows:
        return []

    budget_ids = [
        str(row.get("budgetId") or "").strip()
        for row in rows
        if str(row.get("budgetId") or "").strip()
    ]
    unique_budget_ids = list(dict.fromkeys(budget_ids))
    if not unique_budget_ids:
        for row in rows:
            row["changeHistories"] = ""
        return rows

    history_summary_map: dict[str, str] = {}
    try:
        history_summary_map = _get_budget_change_history_summary_map(
            budget_ids=unique_budget_ids,
            budget_change_histories_table=budget_change_histories_table,
        )
    except Exception:
        history_summary_map = {}

    for row in rows:
        budget_id = str(row.get("budgetId") or "").strip()
        row["changeHistories"] = history_summary_map.get(budget_id, "")
    return rows


def _refresh_budget_data_cache_buckets(
    *,
    tables: dict[str, str],
    buckets: list[tuple[str, int, int]],
) -> int:
    if not buckets:
        return 0

    normalized_buckets: list[tuple[str, int, int]] = []
    seen: set[tuple[str, int, int]] = set()
    for account_code, month, year in buckets:
        bucket = _normalize_budget_bucket(
            account_code=account_code,
            month=month,
            year=year,
        )
        if not bucket or bucket in seen:
            continue
        seen.add(bucket)
        normalized_buckets.append(bucket)
    normalized_buckets.sort(key=lambda item: (item[0], item[2], item[1]))
    if not normalized_buckets:
        return 0

    rows = _fetch_master_budget_control_budget_rows_for_buckets(
        accounts_table=tables["ACCOUNTS"],
        budgets_table=tables["BUDGETS"],
        services_table=tables["SERVICES"],
        departments_table=tables["DEPARTMENTS"],
        budget_change_histories_table=(
            str(tables.get("BUDGETCHANGEHISTORIES") or "").strip()
            or "BudgetChangeHistories"
        ),
        buckets=normalized_buckets,
    )
    rows_by_bucket: dict[tuple[str, int, int], list[dict]] = {}
    for row in rows:
        bucket = _normalize_budget_bucket(
            account_code=row.get("accountCode"),
            month=row.get("month"),
            year=row.get("year"),
        )
        if not bucket:
            continue
        rows_by_bucket.setdefault(bucket, []).append(row)

    updated_count = 0
    for account_code, month, year in normalized_buckets:
        cache_key = _build_budget_data_cache_key(
            budgets_table=tables["BUDGETS"],
            account_code=account_code,
            month=month,
            year=year,
        )
        bucket_rows = rows_by_bucket.get((account_code, month, year), [])
        set_tenant_shared_cache_value(
            bucket=_SHARED_DB_READ_CACHE_BUCKET,
            cache_key=cache_key,
            value=bucket_rows,
        )
        updated_count += 1
    return updated_count


# ============================================================
# QUERIES
# ============================================================


def get_master_budget_control_accounts(*, refresh_cache: bool = False) -> list[dict]:
    tables = get_db_tables()
    accounts_table = tables["ACCOUNTS"]
    cache_ttl_seconds = get_shared_cache_ttl_seconds(
        key="db_accounts_ttl_time",
        default_seconds=_DEFAULT_DB_READ_CACHE_TTL_SECONDS,
        app_name=_APP_NAME,
    )
    cache_key = _build_accounts_cache_key(accounts_table)

    if not refresh_cache:
        cached_rows, cache_hit = get_tenant_shared_cache_value(
            bucket=_SHARED_DB_READ_CACHE_BUCKET,
            cache_key=cache_key,
            ttl_seconds=cache_ttl_seconds,
        )
        if cache_hit and _is_valid_cached_rows(cached_rows):
            return cached_rows

    code_col, name_col, active_col = _resolve_accounts_columns(accounts_table)
    quoted_table = _quote_table_name(accounts_table)

    code_expr = _quote_identifier(code_col)
    name_expr = _quote_identifier(name_col)
    active_expr = _quote_identifier(active_col) if active_col else "1"

    query = (
        "SELECT "
        f"{code_expr} AS accountCode, "
        f"{name_expr} AS accountName, "
        f"{active_expr} AS active "
        f"FROM {quoted_table} "
        "ORDER BY accountCode ASC"
    )

    rows = fetch_all(query)
    set_tenant_shared_cache_value(
        bucket=_SHARED_DB_READ_CACHE_BUCKET,
        cache_key=cache_key,
        value=rows,
    )
    return rows


def get_master_budget_control_services(*, refresh_cache: bool = False) -> list[dict]:
    tables = get_db_tables(require_services=True)
    services_table = tables["SERVICES"]
    departments_table = tables["DEPARTMENTS"]

    cache_ttl_seconds = get_shared_cache_ttl_seconds(
        key="db_services_ttl_time",
        default_seconds=_DEFAULT_DB_READ_CACHE_TTL_SECONDS,
        app_name=_APP_NAME,
    )
    cache_key = _build_services_cache_key(services_table, departments_table)

    if not refresh_cache:
        cached_rows, cache_hit = get_tenant_shared_cache_value(
            bucket=_SHARED_DB_READ_CACHE_BUCKET,
            cache_key=cache_key,
            ttl_seconds=cache_ttl_seconds,
        )
        if cache_hit and _is_valid_cached_rows(cached_rows):
            return cached_rows

    columns = _resolve_services_columns(
        services_table=services_table,
        departments_table=departments_table,
    )
    quoted_services_table = _quote_table_name(services_table)
    quoted_departments_table = _quote_table_name(departments_table)

    dep_listing_order_expr = _quote_identifier(columns["department_listing_order_col"])
    dep_code_expr = _quote_identifier(columns["department_code_col"])
    dep_name_expr = _quote_identifier(columns["department_name_col"])
    service_id_expr = _quote_identifier(columns["service_id_col"])
    service_name_expr = _quote_identifier(columns["service_name_col"])
    commission_expr = _quote_identifier(columns["commission_col"])
    service_dep_code_expr = _quote_identifier(columns["service_department_code_col"])

    query = (
        "SELECT "
        f"d.{dep_listing_order_expr} AS depListingOrder, "
        f"d.{dep_code_expr} AS departmentCode, "
        f"d.{dep_name_expr} AS departmentName, "
        f"s.{service_id_expr} AS serviceId, "
        f"s.{service_name_expr} AS serviceName, "
        f"s.{commission_expr} AS commission "
        f"FROM {quoted_services_table} s "
        f"INNER JOIN {quoted_departments_table} d "
        f"ON s.{service_dep_code_expr} = d.{dep_code_expr} "
        f"ORDER BY d.{dep_listing_order_expr} ASC, s.{service_name_expr} ASC"
    )

    rows = fetch_all(query)
    set_tenant_shared_cache_value(
        bucket=_SHARED_DB_READ_CACHE_BUCKET,
        cache_key=cache_key,
        value=rows,
    )
    return rows


def get_master_budget_control_budget_data(
    *,
    account_codes: list[str],
    periods: list[tuple[int, int]],
    refresh_cache: bool = False,
) -> list[dict]:
    if not account_codes or not periods:
        return []

    tables = get_db_tables(require_services=True)
    accounts_table = tables["ACCOUNTS"]
    budgets_table = tables["BUDGETS"]
    services_table = tables["SERVICES"]
    departments_table = tables["DEPARTMENTS"]
    cache_ttl_seconds = get_shared_cache_ttl_seconds(
        key="db_budget_data_ttl_time",
        default_seconds=_DEFAULT_DB_READ_CACHE_TTL_SECONDS,
        app_name=_APP_NAME,
    )
    requested_buckets = _build_budget_data_buckets(
        account_codes=account_codes,
        periods=periods,
    )
    if not requested_buckets:
        return []

    bucket_rows_map: dict[tuple[str, int, int], list[dict]] = {}
    missing_buckets = list(requested_buckets)
    if not refresh_cache:
        missing_buckets = []
        for account_code, month, year in requested_buckets:
            cache_key = _build_budget_data_cache_key(
                budgets_table=budgets_table,
                account_code=account_code,
                month=month,
                year=year,
            )
            cached_rows, cache_hit = get_tenant_shared_cache_value(
                bucket=_SHARED_DB_READ_CACHE_BUCKET,
                cache_key=cache_key,
                ttl_seconds=cache_ttl_seconds,
            )
            if cache_hit and _is_valid_cached_rows(cached_rows):
                bucket_rows_map[(account_code, month, year)] = cached_rows
            else:
                missing_buckets.append((account_code, month, year))

    if missing_buckets:
        fetched_rows = _fetch_master_budget_control_budget_rows_for_buckets(
            accounts_table=accounts_table,
            budgets_table=budgets_table,
            services_table=services_table,
            departments_table=departments_table,
            budget_change_histories_table=(
                str(tables.get("BUDGETCHANGEHISTORIES") or "").strip()
                or "BudgetChangeHistories"
            ),
            buckets=missing_buckets,
        )
        fetched_rows_by_bucket: dict[tuple[str, int, int], list[dict]] = {}
        for row in fetched_rows:
            bucket = _normalize_budget_bucket(
                account_code=row.get("accountCode"),
                month=row.get("month"),
                year=row.get("year"),
            )
            if not bucket:
                continue
            fetched_rows_by_bucket.setdefault(bucket, []).append(row)

        for account_code, month, year in missing_buckets:
            bucket = (account_code, month, year)
            bucket_rows = fetched_rows_by_bucket.get(bucket, [])
            cache_key = _build_budget_data_cache_key(
                budgets_table=budgets_table,
                account_code=account_code,
                month=month,
                year=year,
            )
            set_tenant_shared_cache_value(
                bucket=_SHARED_DB_READ_CACHE_BUCKET,
                cache_key=cache_key,
                value=bucket_rows,
            )
            bucket_rows_map[bucket] = bucket_rows

    combined_rows: list[dict] = []
    for account_code, month, year in requested_buckets:
        combined_rows.extend(bucket_rows_map.get((account_code, month, year), []))
    combined_rows.sort(key=_budget_data_sort_key)
    for row in combined_rows:
        row.pop("depListingOrder", None)
    return combined_rows


def validate_master_budget_control_budget_refs(
    *,
    budget_ids: list[str],
    account_codes: list[str],
    service_ids: list[str],
) -> dict[str, list[str]]:
    tables = get_db_tables(require_services=True)
    accounts_table = tables["ACCOUNTS"]
    services_table = tables["SERVICES"]
    budgets_table = tables["BUDGETS"]

    missing_budget_ids: list[str] = []
    invalid_account_codes: list[str] = []
    invalid_service_ids: list[str] = []

    if budget_ids:
        budget_columns = _resolve_budget_update_columns(budgets_table)
        quoted_budgets_table = _quote_table_name(budgets_table)
        budget_id_expr = _quote_identifier(budget_columns["budget_id_col"])
        placeholders = ", ".join(["%s"] * len(budget_ids))
        rows = fetch_all(
            (
                "SELECT "
                f"{budget_id_expr} AS budgetId "
                f"FROM {quoted_budgets_table} "
                f"WHERE {budget_id_expr} IN ({placeholders})"
            ),
            tuple(budget_ids),
        )
        existing_budget_ids = {
            str(row.get("budgetId") or "").strip()
            for row in rows
            if str(row.get("budgetId") or "").strip()
        }
        for budget_id in budget_ids:
            if budget_id not in existing_budget_ids:
                missing_budget_ids.append(budget_id)

    if account_codes:
        account_code_col, _account_name_col, _active_col = _resolve_accounts_columns(
            accounts_table
        )
        quoted_accounts_table = _quote_table_name(accounts_table)
        account_code_expr = _quote_identifier(account_code_col)
        placeholders = ", ".join(["%s"] * len(account_codes))
        rows = fetch_all(
            (
                "SELECT "
                f"{account_code_expr} AS accountCode "
                f"FROM {quoted_accounts_table} "
                f"WHERE {account_code_expr} IN ({placeholders})"
            ),
            tuple(account_codes),
        )
        existing_account_codes = {
            str(row.get("accountCode") or "").strip().upper()
            for row in rows
            if str(row.get("accountCode") or "").strip()
        }
        for account_code in account_codes:
            if account_code.upper() not in existing_account_codes:
                invalid_account_codes.append(account_code)

    if service_ids:
        service_id_col, _service_name_col = _resolve_service_identity_columns(services_table)
        quoted_services_table = _quote_table_name(services_table)
        service_id_expr = _quote_identifier(service_id_col)
        placeholders = ", ".join(["%s"] * len(service_ids))
        rows = fetch_all(
            (
                "SELECT "
                f"{service_id_expr} AS serviceId "
                f"FROM {quoted_services_table} "
                f"WHERE {service_id_expr} IN ({placeholders})"
            ),
            tuple(service_ids),
        )
        existing_service_ids = {
            str(row.get("serviceId") or "").strip()
            for row in rows
            if str(row.get("serviceId") or "").strip()
        }
        for service_id in service_ids:
            if service_id not in existing_service_ids:
                invalid_service_ids.append(service_id)

    return {
        "missingBudgetIds": missing_budget_ids,
        "invalidAccountCodes": invalid_account_codes,
        "invalidServiceIds": invalid_service_ids,
    }


def _normalize_budget_unique_key(
    *,
    account_code: object,
    month: object,
    year: object,
    service_id: object,
    sub_service: object,
) -> tuple[str, int, int, str, str] | None:
    normalized_account_code = str(account_code or "").strip().upper()
    normalized_service_id = str(service_id or "").strip()
    normalized_sub_service = str(sub_service or "").strip()

    try:
        normalized_month = int(month)
        normalized_year = int(year)
    except (TypeError, ValueError):
        return None

    if (
        not normalized_account_code
        or not normalized_service_id
        or normalized_month < 1
        or normalized_month > 12
        or normalized_year <= 0
    ):
        return None

    return (
        normalized_account_code,
        normalized_month,
        normalized_year,
        normalized_service_id,
        normalized_sub_service,
    )


def validate_master_budget_control_budget_duplicates(
    *,
    changes: list[dict[str, object]] | None = None,
    creates: list[dict[str, object]] | None = None,
    deletes: list[dict[str, object]] | None = None,
) -> dict[str, list[dict[str, object]]]:
    change_items = changes or []
    create_items = creates or []
    delete_items = deletes or []
    if not change_items and not create_items:
        return {"duplicateKeys": []}

    tables = get_db_tables(require_services=True)
    budgets_table = tables["BUDGETS"]
    budget_columns = _resolve_budget_update_columns(budgets_table)
    quoted_budgets_table = _quote_table_name(budgets_table)
    budget_id_expr = _quote_identifier(budget_columns["budget_id_col"])
    budget_account_code_expr = _quote_identifier(budget_columns["budget_account_code_col"])
    budget_service_id_expr = _quote_identifier(budget_columns["budget_service_id_col"])
    budget_month_expr = _quote_identifier(budget_columns["budget_month_col"])
    budget_year_expr = _quote_identifier(budget_columns["budget_year_col"])
    budget_sub_service_expr = _quote_identifier(budget_columns["budget_sub_service_col"])

    budget_ids_for_lookup: list[str] = []
    seen_budget_ids: set[str] = set()
    for item in change_items + delete_items:
        budget_id = str(item.get("budgetId") or "").strip()
        if not budget_id or budget_id in seen_budget_ids:
            continue
        seen_budget_ids.add(budget_id)
        budget_ids_for_lookup.append(budget_id)

    existing_rows_by_budget_id: dict[str, dict[str, object]] = {}
    if budget_ids_for_lookup:
        placeholders = ", ".join(["%s"] * len(budget_ids_for_lookup))
        existing_rows = fetch_all(
            (
                "SELECT "
                f"{budget_id_expr} AS budgetId, "
                f"{budget_account_code_expr} AS accountCode, "
                f"{budget_service_id_expr} AS serviceId, "
                f"{budget_month_expr} AS month, "
                f"{budget_year_expr} AS year, "
                f"{budget_sub_service_expr} AS subService "
                f"FROM {quoted_budgets_table} "
                f"WHERE {budget_id_expr} IN ({placeholders})"
            ),
            tuple(budget_ids_for_lookup),
        )
        for row in existing_rows:
            budget_id = str(row.get("budgetId") or "").strip()
            if not budget_id:
                continue
            existing_rows_by_budget_id[budget_id] = row

    delete_budget_ids = {
        str(item.get("budgetId") or "").strip()
        for item in delete_items
        if str(item.get("budgetId") or "").strip()
    }

    operations: list[dict[str, object]] = []
    for change in change_items:
        budget_id = str(change.get("budgetId") or "").strip()
        if not budget_id:
            continue
        existing_row = existing_rows_by_budget_id.get(budget_id)
        if not isinstance(existing_row, dict):
            continue
        unique_key = _normalize_budget_unique_key(
            account_code=existing_row.get("accountCode"),
            month=existing_row.get("month"),
            year=existing_row.get("year"),
            service_id=existing_row.get("serviceId"),
            sub_service=change.get("subService"),
        )
        if unique_key is None:
            continue
        operations.append(
            {
                "type": "update",
                "budgetId": budget_id,
                "row": change.get("row"),
                "key": unique_key,
            }
        )

    for create in create_items:
        unique_key = _normalize_budget_unique_key(
            account_code=create.get("accountCode"),
            month=create.get("month"),
            year=create.get("year"),
            service_id=create.get("serviceId"),
            sub_service=create.get("subService"),
        )
        if unique_key is None:
            continue
        operations.append(
            {
                "type": "create",
                "budgetId": "",
                "row": create.get("row"),
                "key": unique_key,
            }
        )

    if not operations:
        return {"duplicateKeys": []}

    operations_by_key: dict[tuple[str, int, int, str, str], list[dict[str, object]]] = {}
    for operation in operations:
        operation_key = operation.get("key")
        if not isinstance(operation_key, tuple):
            continue
        operations_by_key.setdefault(operation_key, []).append(operation)

    duplicate_keys: list[dict[str, object]] = []
    duplicate_fingerprints: set[tuple[tuple[str, int, int, str, str], str, str]] = set()

    def _extract_rows(key_operations: list[dict[str, object]]) -> list[int]:
        rows: list[int] = []
        seen_rows: set[int] = set()
        for operation in key_operations:
            row_value = operation.get("row")
            if isinstance(row_value, bool):
                continue
            if isinstance(row_value, int):
                row_number = row_value
            else:
                try:
                    row_number = int(str(row_value).strip())
                except (TypeError, ValueError):
                    continue
            if row_number <= 0 or row_number in seen_rows:
                continue
            seen_rows.add(row_number)
            rows.append(row_number)
        rows.sort()
        return rows

    def _append_duplicate(
        *,
        unique_key: tuple[str, int, int, str, str],
        source: str,
        existing_budget_id: str = "",
        rows: list[int] | None = None,
    ) -> None:
        fingerprint = (unique_key, source, existing_budget_id)
        if fingerprint in duplicate_fingerprints:
            return
        duplicate_fingerprints.add(fingerprint)
        normalized_rows = rows or []
        duplicate_keys.append(
            {
                "accountCode": unique_key[0],
                "month": unique_key[1],
                "year": unique_key[2],
                "serviceId": unique_key[3],
                "subService": unique_key[4],
                "source": source,
                "existingBudgetId": existing_budget_id,
                "rows": normalized_rows,
            }
        )

    for unique_key, key_operations in operations_by_key.items():
        unique_update_budget_ids = {
            str(op.get("budgetId") or "").strip()
            for op in key_operations
            if str(op.get("budgetId") or "").strip()
        }
        create_count = sum(
            1 for op in key_operations if not str(op.get("budgetId") or "").strip()
        )
        if create_count + len(unique_update_budget_ids) > 1:
            _append_duplicate(
                unique_key=unique_key,
                source="request",
                rows=_extract_rows(key_operations),
            )

    candidate_keys = list(operations_by_key.keys())
    if candidate_keys:
        where_clauses: list[str] = []
        params: list[object] = []
        for account_code, month_value, year_value, service_id, sub_service in candidate_keys:
            where_clauses.append(
                "("
                f"{budget_account_code_expr} = %s AND "
                f"{budget_month_expr} = %s AND "
                f"{budget_year_expr} = %s AND "
                f"{budget_service_id_expr} = %s AND "
                f"{budget_sub_service_expr} = %s"
                ")"
            )
            params.extend(
                [account_code, month_value, year_value, service_id, sub_service]
            )

        existing_duplicate_rows = fetch_all(
            (
                "SELECT "
                f"{budget_id_expr} AS budgetId, "
                f"{budget_account_code_expr} AS accountCode, "
                f"{budget_month_expr} AS month, "
                f"{budget_year_expr} AS year, "
                f"{budget_service_id_expr} AS serviceId, "
                f"{budget_sub_service_expr} AS subService "
                f"FROM {quoted_budgets_table} "
                f"WHERE {' OR '.join(where_clauses)}"
            ),
            tuple(params),
        )

        for row in existing_duplicate_rows:
            existing_budget_id = str(row.get("budgetId") or "").strip()
            if not existing_budget_id or existing_budget_id in delete_budget_ids:
                continue

            unique_key = _normalize_budget_unique_key(
                account_code=row.get("accountCode"),
                month=row.get("month"),
                year=row.get("year"),
                service_id=row.get("serviceId"),
                sub_service=row.get("subService"),
            )
            if unique_key is None:
                continue

            key_operations = operations_by_key.get(unique_key) or []
            if not key_operations:
                continue

            unique_update_budget_ids = {
                str(op.get("budgetId") or "").strip()
                for op in key_operations
                if str(op.get("budgetId") or "").strip()
            }
            create_count = sum(
                1 for op in key_operations if not str(op.get("budgetId") or "").strip()
            )

            if existing_budget_id in unique_update_budget_ids and create_count == 0 and len(
                unique_update_budget_ids
            ) == 1:
                continue

            _append_duplicate(
                unique_key=unique_key,
                source="database",
                existing_budget_id=existing_budget_id,
                rows=_extract_rows(key_operations),
            )

    return {"duplicateKeys": duplicate_keys}


def _build_history_insert_plan(
    history_columns: dict[str, str],
) -> tuple[list[str], list[str]]:
    plan: list[tuple[str, str]] = [
        ("id", history_columns.get("id") or ""),
        ("budget_id", history_columns.get("budget_id") or ""),
        ("field", history_columns.get("field") or ""),
        ("old_value", history_columns.get("old_value") or ""),
        ("new_value", history_columns.get("new_value") or ""),
        ("note", history_columns.get("note") or ""),
        ("source", history_columns.get("source") or ""),
        ("created_at", history_columns.get("created_at") or ""),
        ("updated_at", history_columns.get("updated_at") or ""),
        ("tenant_id", history_columns.get("tenant_id") or ""),
        ("changed_by", history_columns.get("changed_by") or ""),
        ("account_code", history_columns.get("account_code") or ""),
        ("service_id", history_columns.get("service_id") or ""),
        ("month", history_columns.get("month") or ""),
        ("year", history_columns.get("year") or ""),
    ]

    semantics: list[str] = []
    columns: list[str] = []
    for semantic, column_name in plan:
        if not column_name:
            continue
        semantics.append(semantic)
        columns.append(column_name)
    return semantics, columns


def _resolve_budget_change_history_columns(
    budget_change_histories_table: str,
) -> dict[str, str]:
    columns = _load_table_columns(budget_change_histories_table)

    budget_id_col = _resolve_column_name(
        columns,
        _BUDGET_CHANGE_HISTORY_BUDGET_ID_CANDIDATES,
    )
    action_type_col = _resolve_column_name(
        columns,
        _BUDGET_CHANGE_HISTORY_ACTION_TYPE_CANDIDATES,
    )
    old_data_col = _resolve_column_name(
        columns,
        _BUDGET_CHANGE_HISTORY_OLD_DATA_CANDIDATES,
    )
    new_data_col = _resolve_column_name(
        columns,
        _BUDGET_CHANGE_HISTORY_NEW_DATA_CANDIDATES,
    )
    changed_by_col = _resolve_column_name(
        columns,
        _BUDGET_CHANGE_HISTORY_CHANGED_BY_CANDIDATES,
    )
    note_col = _resolve_column_name(columns, _BUDGET_CHANGE_HISTORY_NOTE_CANDIDATES)
    changed_fields_col = _resolve_column_name(
        columns,
        _BUDGET_CHANGE_HISTORY_CHANGED_FIELDS_CANDIDATES,
    )

    missing: list[str] = []
    if not budget_id_col:
        missing.append("budgetId")
    if not action_type_col:
        missing.append("actionType")
    if not old_data_col:
        missing.append("oldData")
    if not new_data_col:
        missing.append("newData")
    if not changed_by_col:
        missing.append("changedBy")
    if not note_col:
        missing.append("note")
    if missing:
        raise ValueError(
            "Budget change history table is missing required columns: "
            + ", ".join(missing)
        )

    return {
        "budget_id_col": budget_id_col or "",
        "action_type_col": action_type_col or "",
        "changed_fields_col": changed_fields_col or "",
        "old_data_col": old_data_col or "",
        "new_data_col": new_data_col or "",
        "changed_by_col": changed_by_col or "",
        "note_col": note_col or "",
    }


def _build_budget_change_history_insert_plan(
    budget_change_history_columns: dict[str, str],
) -> tuple[list[str], list[str]]:
    plan: list[tuple[str, str]] = [
        ("budget_id", budget_change_history_columns.get("budget_id_col") or ""),
        ("action_type", budget_change_history_columns.get("action_type_col") or ""),
        (
            "changed_fields",
            budget_change_history_columns.get("changed_fields_col") or "",
        ),
        ("old_data", budget_change_history_columns.get("old_data_col") or ""),
        ("new_data", budget_change_history_columns.get("new_data_col") or ""),
        ("changed_by", budget_change_history_columns.get("changed_by_col") or ""),
        ("note", budget_change_history_columns.get("note_col") or ""),
    ]

    semantics: list[str] = []
    columns: list[str] = []
    for semantic, column_name in plan:
        if not column_name:
            continue
        semantics.append(semantic)
        columns.append(column_name)
    return semantics, columns


def _parse_json_object(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}

    raw = str(value).strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None:
        return []

    raw = str(value).strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _normalize_history_value(value: object) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    text = str(value).strip()
    return text if text else "n/a"


def _order_budget_change_fields(field_names: list[str]) -> list[str]:
    normalized_fields: list[str] = []
    seen: set[str] = set()
    for raw_name in field_names:
        field_name = str(raw_name or "").strip()
        if not field_name or field_name in seen:
            continue
        normalized_fields.append(field_name)
        seen.add(field_name)

    normalized_set = set(normalized_fields)
    ordered_fields: list[str] = []
    for field_name in _BUDGET_CHANGE_FIELD_DISPLAY_ORDER:
        if field_name in normalized_set:
            ordered_fields.append(field_name)

    for field_name in normalized_fields:
        if field_name not in _BUDGET_CHANGE_FIELD_DISPLAY_ORDER:
            ordered_fields.append(field_name)
    return ordered_fields


def _format_history_date_label(value: object) -> str:
    try:
        tenant_tz = ZoneInfo(get_timezone())
    except Exception:
        tenant_tz = ZoneInfo("UTC")

    def _format_datetime(dt: datetime) -> str:
        if dt.tzinfo is None:
            localized = dt.replace(tzinfo=tenant_tz)
        else:
            localized = dt.astimezone(tenant_tz)
        return (
            f"{localized.month}/{localized.day}/{localized.year % 100:02d} "
            f"{localized.hour}:{localized.minute:02d}"
        )

    if isinstance(value, datetime):
        return _format_datetime(value)

    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return _format_datetime(parsed)
    except ValueError:
        return raw


def _get_budget_change_history_summary_map(
    *,
    budget_ids: list[str],
    budget_change_histories_table: str,
) -> dict[str, str]:
    if not budget_ids:
        return {}

    available = _load_table_columns(budget_change_histories_table)
    budget_id_col = _resolve_column_name(
        available,
        _BUDGET_CHANGE_HISTORY_BUDGET_ID_CANDIDATES,
    )
    action_type_col = _resolve_column_name(
        available,
        _BUDGET_CHANGE_HISTORY_ACTION_TYPE_CANDIDATES,
    )
    changed_fields_col = _resolve_column_name(
        available,
        _BUDGET_CHANGE_HISTORY_CHANGED_FIELDS_CANDIDATES,
    )
    old_data_col = _resolve_column_name(
        available,
        _BUDGET_CHANGE_HISTORY_OLD_DATA_CANDIDATES,
    )
    new_data_col = _resolve_column_name(
        available,
        _BUDGET_CHANGE_HISTORY_NEW_DATA_CANDIDATES,
    )
    changed_by_col = _resolve_column_name(
        available,
        _BUDGET_CHANGE_HISTORY_CHANGED_BY_CANDIDATES,
    )
    note_col = _resolve_column_name(
        available,
        _BUDGET_CHANGE_HISTORY_NOTE_CANDIDATES,
    )
    created_at_col = _resolve_column_name(
        available,
        _BUDGET_CHANGE_HISTORY_CREATED_AT_CANDIDATES,
    )
    history_id_col = _resolve_column_name(
        available,
        _BUDGET_CHANGE_HISTORY_ID_CANDIDATES,
    )
    if (
        not budget_id_col
        or not action_type_col
        or not old_data_col
        or not new_data_col
        or not changed_by_col
    ):
        return {}
    if not created_at_col:
        return {}

    quoted_table = _quote_table_name(budget_change_histories_table)
    budget_id_expr = _quote_identifier(budget_id_col)
    action_type_expr = _quote_identifier(action_type_col)
    changed_fields_expr = (
        _quote_identifier(changed_fields_col)
        if changed_fields_col
        else "NULL"
    )
    old_data_expr = _quote_identifier(old_data_col)
    new_data_expr = _quote_identifier(new_data_col)
    changed_by_expr = _quote_identifier(changed_by_col)
    note_expr = _quote_identifier(note_col) if note_col else "NULL"
    created_at_expr = _quote_identifier(created_at_col)

    select_parts = [
        f"{budget_id_expr} AS budgetId",
        f"{action_type_expr} AS actionType",
        f"{changed_fields_expr} AS changedFields",
        f"{old_data_expr} AS oldData",
        f"{new_data_expr} AS newData",
        f"{changed_by_expr} AS changedBy",
        f"{note_expr} AS note",
        f"{created_at_expr} AS dateCreated",
    ]

    order_parts = [f"{created_at_expr} DESC"]
    if history_id_col:
        order_parts.append(f"{_quote_identifier(history_id_col)} DESC")

    placeholders = ", ".join(["%s"] * len(budget_ids))
    query = (
        "SELECT "
        + ", ".join(select_parts)
        + f" FROM {quoted_table} "
        + f"WHERE {budget_id_expr} IN ({placeholders}) "
        + "ORDER BY "
        + ", ".join(order_parts)
    )
    rows = fetch_all(query, tuple(budget_ids))

    budget_to_lines: dict[str, list[str]] = {}

    for row in rows:
        budget_id = str(row.get("budgetId") or "").strip()
        if not budget_id:
            continue

        changed_by = str(row.get("changedBy") or "").strip() or "unknown"
        old_data = _parse_json_object(row.get("oldData"))
        new_data = _parse_json_object(row.get("newData"))
        changed_fields = _order_budget_change_fields(
            _parse_json_string_list(row.get("changedFields"))
        )
        action_type = str(row.get("actionType") or "").strip().upper() or "UPDATE"

        change_entries: list[str] = []
        if action_type == "CREATE":
            if changed_fields:
                for field_name in changed_fields:
                    new_value = _normalize_history_value(new_data.get(field_name))
                    change_entries.append(f"{field_name} ({new_value})")
            line_body = (
                f"{changed_by} created budget {', '.join(change_entries)}"
                if change_entries
                else f"{changed_by} created budget"
            )
        elif action_type == "DELETE":
            if changed_fields:
                for field_name in changed_fields:
                    old_value = _normalize_history_value(old_data.get(field_name))
                    change_entries.append(f"{field_name} ({old_value})")
            line_body = (
                f"{changed_by} deleted budget {', '.join(change_entries)}"
                if change_entries
                else f"{changed_by} deleted budget"
            )
        else:
            if changed_fields:
                for field_name in changed_fields:
                    old_value = _normalize_history_value(old_data.get(field_name))
                    new_value = _normalize_history_value(new_data.get(field_name))
                    change_entries.append(f"{field_name}({old_value} -> {new_value})")
            line_body = (
                f"{changed_by} changed {', '.join(change_entries)}"
                if change_entries
                else f"{changed_by} changed budget"
            )

        date_label = _format_history_date_label(row.get("dateCreated"))
        line = f"{date_label}: {line_body}".strip() if date_label else line_body
        history_note = str(row.get("note") or "").strip()
        if history_note:
            line = f"{line} - {history_note}"
        budget_to_lines.setdefault(budget_id, []).append(line)

    summary_map: dict[str, str] = {}
    for budget_id, lines in budget_to_lines.items():
        summary_map[budget_id] = "\n".join(lines)

    return summary_map


def _should_generate_budget_id(
    *,
    budget_id_column_name: str,
    budget_column_specs: dict[str, dict[str, object]],
) -> bool:
    spec = budget_column_specs.get(str(budget_id_column_name or "").lower(), {})
    type_lower = str(spec.get("Type") or "").strip().lower()
    extra_lower = str(spec.get("Extra") or "").strip().lower()
    default_value = spec.get("Default")

    if "auto_increment" in extra_lower:
        return False
    if default_value not in (None, ""):
        return False
    if any(token in type_lower for token in _NUMERIC_SQL_TYPE_TOKENS):
        return False
    return True


def apply_budget_mutations_with_history(
    *,
    changes: list[dict[str, object]],
    creates: list[dict[str, object]] | None = None,
    deletes: list[dict[str, object]] | None = None,
    changed_by: str = "system",
    source_action: str = "system",
    change_note: str | None = None,
) -> dict[str, object]:
    create_items = creates or []
    delete_items = deletes or []
    if not changes and not create_items and not delete_items:
        return {
            "updatedRowCount": 0,
            "createdRowCount": 0,
            "deletedRowCount": 0,
            "historyRowCount": 0,
            "updatedBudgetIds": [],
            "createdBudgetIds": [],
            "deletedBudgetIds": [],
        }

    tables = get_db_tables(require_services=True)
    budgets_table = tables["BUDGETS"]
    services_table = tables["SERVICES"]
    change_histories_table = tables["CHANGEHISTORIES"]
    budget_change_histories_table = (
        str(tables.get("BUDGETCHANGEHISTORIES") or "").strip() or "BudgetChangeHistories"
    )

    budget_columns = _resolve_budget_update_columns(budgets_table)
    budget_column_specs = _load_table_column_specs(budgets_table)
    history_columns = _resolve_change_history_columns(change_histories_table)
    history_semantics, history_insert_columns = _build_history_insert_plan(history_columns)
    budget_change_history_columns = _resolve_budget_change_history_columns(
        budget_change_histories_table
    )
    (
        budget_change_history_semantics,
        budget_change_history_insert_columns,
    ) = _build_budget_change_history_insert_plan(budget_change_history_columns)
    can_insert_history = bool(history_columns.get("budget_id")) and bool(
        history_columns.get("field")
        or history_columns.get("old_value")
        or history_columns.get("new_value")
        or history_columns.get("note")
    )

    quoted_budgets_table = _quote_table_name(budgets_table)
    quoted_services_table = _quote_table_name(services_table)
    quoted_history_table = _quote_table_name(change_histories_table)
    quoted_budget_change_history_table = _quote_table_name(budget_change_histories_table)

    budget_id_expr = _quote_identifier(budget_columns["budget_id_col"])
    budget_account_code_expr = _quote_identifier(budget_columns["budget_account_code_col"])
    budget_service_id_expr = _quote_identifier(budget_columns["budget_service_id_col"])
    budget_month_expr = _quote_identifier(budget_columns["budget_month_col"])
    budget_year_expr = _quote_identifier(budget_columns["budget_year_col"])
    sub_service_expr = _quote_identifier(budget_columns["budget_sub_service_col"])
    gross_amount_expr = _quote_identifier(budget_columns["budget_gross_amount_col"])
    commission_expr = _quote_identifier(budget_columns["budget_commission_col"])
    net_adjustment_expr = _quote_identifier(budget_columns["budget_net_adjustment_col"])
    note_expr = _quote_identifier(budget_columns["budget_note_col"])
    service_id_col, service_name_col = _resolve_service_identity_columns(services_table)
    service_id_expr = _quote_identifier(service_id_col)
    service_name_expr = _quote_identifier(service_name_col)
    select_existing_budget_query = (
        "SELECT "
        f"b.{budget_account_code_expr} AS accountCode, "
        f"b.{budget_month_expr} AS month, "
        f"b.{budget_year_expr} AS year, "
        f"b.{budget_service_id_expr} AS serviceId, "
        f"s.{service_name_expr} AS serviceName, "
        f"b.{sub_service_expr} AS subService, "
        f"b.{gross_amount_expr} AS grossAmount, "
        f"b.{commission_expr} AS commission, "
        f"b.{net_adjustment_expr} AS netAdjustment, "
        f"b.{note_expr} AS note "
        f"FROM {quoted_budgets_table} b "
        f"LEFT JOIN {quoted_services_table} s "
        f"ON b.{budget_service_id_expr} = s.{service_id_expr} "
        f"WHERE b.{budget_id_expr} = %s"
    )
    delete_query = f"DELETE FROM {quoted_budgets_table} WHERE {budget_id_expr} = %s"

    update_assignments = [
        f"{sub_service_expr} = %s",
        f"{gross_amount_expr} = %s",
        f"{commission_expr} = %s",
        f"{net_adjustment_expr} = %s",
        f"{note_expr} = %s",
    ]
    update_query = (
        f"UPDATE {quoted_budgets_table} "
        f"SET {', '.join(update_assignments)} "
        f"WHERE {budget_id_expr} = %s"
    )

    generate_budget_id = _should_generate_budget_id(
        budget_id_column_name=budget_columns["budget_id_col"],
        budget_column_specs=budget_column_specs,
    )
    insert_columns: list[str] = []
    if generate_budget_id:
        insert_columns.append(budget_columns["budget_id_col"])
    insert_columns.extend(
        [
            budget_columns["budget_account_code_col"],
            budget_columns["budget_service_id_col"],
            budget_columns["budget_sub_service_col"],
            budget_columns["budget_gross_amount_col"],
            budget_columns["budget_commission_col"],
            budget_columns["budget_net_adjustment_col"],
            budget_columns["budget_note_col"],
            budget_columns["budget_month_col"],
            budget_columns["budget_year_col"],
        ]
    )
    insert_placeholders = ", ".join(["%s"] * len(insert_columns))
    quoted_insert_columns = ", ".join(_quote_identifier(column) for column in insert_columns)
    insert_query = (
        f"INSERT INTO {quoted_budgets_table} ({quoted_insert_columns}) "
        f"VALUES ({insert_placeholders})"
    )

    history_insert_query = ""
    if can_insert_history and history_insert_columns:
        quoted_columns = ", ".join(_quote_identifier(column) for column in history_insert_columns)
        placeholders = ", ".join(["%s"] * len(history_insert_columns))
        history_insert_query = (
            f"INSERT INTO {quoted_history_table} ({quoted_columns}) "
            f"VALUES ({placeholders})"
        )
    quoted_budget_change_columns = ", ".join(
        _quote_identifier(column) for column in budget_change_history_insert_columns
    )
    budget_change_placeholders = ", ".join(
        ["%s"] * len(budget_change_history_insert_columns)
    )
    budget_change_history_insert_query = (
        f"INSERT INTO {quoted_budget_change_history_table} "
        f"({quoted_budget_change_columns}) "
        f"VALUES ({budget_change_placeholders})"
    )

    tenant_id = get_tenant_id()
    now = datetime.utcnow()
    normalized_source_action = str(source_action or "").strip() or "system"
    normalized_changed_by = str(changed_by or "").strip() or normalized_source_action
    normalized_change_note: str | None = None
    if change_note is not None:
        cleaned_change_note = str(change_note).strip()
        normalized_change_note = cleaned_change_note or None

    def _work(cursor) -> dict[str, object]:
        history_rows: list[tuple] = []
        budget_change_history_rows: list[tuple] = []
        updated_budget_ids: list[str] = []
        created_budget_ids: list[str] = []
        deleted_budget_ids: list[str] = []
        affected_buckets: set[tuple[str, int, int]] = set()
        history_error: str | None = None

        for delete in delete_items:
            budget_id = str(delete.get("budgetId") or "").strip()
            if not budget_id:
                continue
            delete_change_note = (
                str(delete.get("changeNote") or "").strip() or normalized_change_note
            )

            cursor.execute(select_existing_budget_query, (budget_id,))
            existing_row = cursor.fetchone()
            if not existing_row:
                raise ValueError(f"Budget not found for delete: {budget_id}")
            old_account_code = str(existing_row[0] or "").strip().upper()
            old_month_value = _to_int(existing_row[1], default=0)
            old_year_value = _to_int(existing_row[2], default=0)
            old_service_id = str(existing_row[3] or "").strip()
            old_service_name = str(existing_row[4] or "").strip()
            old_sub_service = str(existing_row[5] or "").strip()
            old_gross_amount = _to_decimal(existing_row[6], scale=2)
            old_commission = _to_decimal(existing_row[7], scale=4)
            old_net_adjustment = _to_decimal(existing_row[8], scale=2)
            old_note = str(existing_row[9] or "").strip()
            delete_bucket = _normalize_budget_bucket(
                account_code=old_account_code,
                month=old_month_value,
                year=old_year_value,
            )
            if delete_bucket:
                affected_buckets.add(delete_bucket)

            cursor.execute(delete_query, (budget_id,))
            if int(cursor.rowcount or 0) <= 0:
                raise ValueError(f"Budget not found for delete: {budget_id}")
            deleted_budget_ids.append(budget_id)

            delete_diffs = [
                {"field": "subService", "oldValue": old_sub_service, "newValue": ""},
                {
                    "field": "grossAmount",
                    "oldValue": f"{old_gross_amount:.2f}",
                    "newValue": "",
                },
                {
                    "field": "commission",
                    "oldValue": f"{old_commission:.4f}",
                    "newValue": "",
                },
                {
                    "field": "netAdjustment",
                    "oldValue": f"{old_net_adjustment:.2f}",
                    "newValue": "",
                },
                {"field": "note", "oldValue": old_note, "newValue": ""},
            ]

            for diff in delete_diffs:
                field_name = str(diff.get("field") or "").strip()
                old_value = str(diff.get("oldValue") or "")
                note_value = f"delete {field_name}: {old_value}"
                value_map: dict[str, object] = {
                    "id": str(uuid.uuid4()),
                    "budget_id": budget_id,
                    "field": field_name,
                    "old_value": old_value,
                    "new_value": "",
                    "note": note_value,
                    "source": normalized_source_action,
                    "created_at": now,
                    "updated_at": now,
                    "tenant_id": tenant_id,
                    "changed_by": normalized_changed_by,
                    "account_code": old_account_code or None,
                    "service_id": old_service_id or None,
                    "month": old_month_value if old_month_value > 0 else None,
                    "year": old_year_value if old_year_value > 0 else None,
                }
                history_rows.append(tuple(value_map.get(key) for key in history_semantics))

            delete_old_data: dict[str, object] = {
                "accountCode": old_account_code or None,
                "month": old_month_value if old_month_value > 0 else None,
                "year": old_year_value if old_year_value > 0 else None,
                "serviceId": old_service_id or None,
                "serviceName": old_service_name or None,
                "subService": old_sub_service,
                "grossAmount": f"{old_gross_amount:.2f}",
                "commission": f"{old_commission:.4f}",
                "netAdjustment": f"{old_net_adjustment:.2f}",
                "note": old_note,
            }
            budget_change_history_value_map = {
                "budget_id": budget_id,
                "action_type": "DELETE",
                "changed_fields": json.dumps(
                    ["subService", "grossAmount", "commission", "netAdjustment", "note"],
                    ensure_ascii=False,
                ),
                "old_data": json.dumps(delete_old_data, ensure_ascii=False),
                "new_data": None,
                "changed_by": normalized_changed_by,
                "note": delete_change_note,
            }
            budget_change_history_rows.append(
                tuple(
                    budget_change_history_value_map.get(key)
                    for key in budget_change_history_semantics
                )
            )

        for change in changes:
            budget_id = str(change.get("budgetId") or "").strip()
            if not budget_id:
                continue
            change_note_value = (
                str(change.get("changeNote") or "").strip() or normalized_change_note
            )

            sub_service = str(change.get("subService") or "").strip()
            gross_amount = _to_decimal(change.get("grossAmount"), scale=2)
            commission = _to_decimal(change.get("commission"), scale=4)
            net_adjustment = _to_decimal(change.get("netAdjustment"), scale=2)
            note = str(change.get("note") or "").strip()

            update_params: list[object] = [
                sub_service,
                gross_amount,
                commission,
                net_adjustment,
                note,
                budget_id,
            ]
            cursor.execute(
                update_query,
                tuple(update_params),
            )
            if int(cursor.rowcount or 0) <= 0:
                raise ValueError(f"Budget not found for update: {budget_id}")
            update_bucket = _normalize_budget_bucket(
                account_code=change.get("accountCode"),
                month=change.get("month"),
                year=change.get("year"),
            )
            if update_bucket:
                affected_buckets.add(update_bucket)

            updated_budget_ids.append(budget_id)

            diffs = change.get("diffs")
            if not isinstance(diffs, list) or not diffs:
                continue
            diffs = [diff for diff in diffs if isinstance(diff, dict)]
            if not diffs:
                continue

            for diff in diffs:
                field_name = str(diff.get("field") or "").strip()
                old_value = str(diff.get("oldValue") or "")
                new_value = str(diff.get("newValue") or "")
                note_value = f"{field_name}: {old_value} -> {new_value}"

                value_map: dict[str, object] = {
                    "id": str(uuid.uuid4()),
                    "budget_id": budget_id,
                    "field": field_name,
                    "old_value": old_value,
                    "new_value": new_value,
                    "note": note_value,
                    "source": normalized_source_action,
                    "created_at": now,
                    "updated_at": now,
                    "tenant_id": tenant_id,
                    "changed_by": normalized_changed_by,
                    "account_code": str(change.get("accountCode") or "").strip() or None,
                    "service_id": str(change.get("serviceId") or "").strip() or None,
                    "month": change.get("month"),
                    "year": change.get("year"),
                }
                history_rows.append(tuple(value_map.get(key) for key in history_semantics))

            new_data_map: dict[str, object] = {
                "subService": sub_service,
                "grossAmount": f"{gross_amount:.2f}",
                "commission": f"{commission:.4f}",
                "netAdjustment": f"{net_adjustment:.2f}",
                "note": note,
            }
            old_data_map: dict[str, object] = dict(new_data_map)
            changed_fields: list[str] = []
            for diff in diffs:
                if not isinstance(diff, dict):
                    continue
                field_name = str(diff.get("field") or "").strip()
                if not field_name:
                    continue
                if field_name not in changed_fields:
                    changed_fields.append(field_name)
                if field_name in old_data_map:
                    old_data_map[field_name] = diff.get("oldValue")
                if field_name in new_data_map:
                    new_data_map[field_name] = diff.get("newValue")

            budget_change_history_value_map: dict[str, object] = {
                "budget_id": budget_id,
                "action_type": "UPDATE",
                "changed_fields": json.dumps(changed_fields, ensure_ascii=False),
                "old_data": json.dumps(old_data_map, ensure_ascii=False),
                "new_data": json.dumps(new_data_map, ensure_ascii=False),
                "changed_by": normalized_changed_by,
                "note": change_note_value,
            }
            budget_change_history_rows.append(
                tuple(
                    budget_change_history_value_map.get(key)
                    for key in budget_change_history_semantics
                )
            )

            if not (history_insert_query and can_insert_history):
                continue

        for create in create_items:
            account_code = str(create.get("accountCode") or "").strip()
            service_id = str(create.get("serviceId") or "").strip()
            month_value = int(create.get("month") or 0)
            year_value = int(create.get("year") or 0)
            create_change_note = (
                str(create.get("changeNote") or "").strip() or normalized_change_note
            )
            sub_service = str(create.get("subService") or "").strip()
            gross_amount = _to_decimal(create.get("grossAmount"), scale=2)
            commission = _to_decimal(create.get("commission"), scale=4)
            net_adjustment = _to_decimal(create.get("netAdjustment"), scale=2)
            note = str(create.get("note") or "").strip()

            create_budget_id = str(uuid.uuid4()) if generate_budget_id else ""
            insert_params: list[object] = []
            if generate_budget_id:
                insert_params.append(create_budget_id)
            insert_params.extend(
                [
                    account_code,
                    service_id,
                    sub_service,
                    gross_amount,
                    commission,
                    net_adjustment,
                    note,
                    month_value,
                    year_value,
                ]
            )

            cursor.execute(insert_query, tuple(insert_params))

            if int(cursor.rowcount or 0) <= 0:
                raise ValueError("Budget insert did not affect any row")

            if not create_budget_id:
                last_row_id = cursor.lastrowid
                create_budget_id = (
                    str(last_row_id).strip()
                    if last_row_id not in (None, 0, "0")
                    else ""
                )
            created_budget_ids.append(create_budget_id)
            create_bucket = _normalize_budget_bucket(
                account_code=account_code,
                month=month_value,
                year=year_value,
            )
            if create_bucket:
                affected_buckets.add(create_bucket)

            diffs = create.get("diffs")
            if not isinstance(diffs, list) or not diffs:
                diffs = []
                if sub_service:
                    diffs.append(
                        {"field": "subService", "oldValue": "", "newValue": sub_service}
                    )
                diffs.append(
                    {
                        "field": "grossAmount",
                        "oldValue": "",
                        "newValue": f"{gross_amount:.2f}",
                    }
                )
                diffs.append(
                    {
                        "field": "commission",
                        "oldValue": "",
                        "newValue": f"{commission:.4f}",
                    }
                )
                diffs.append(
                    {
                        "field": "netAdjustment",
                        "oldValue": "",
                        "newValue": f"{net_adjustment:.2f}",
                    }
                )
                if note:
                    diffs.append({"field": "note", "oldValue": "", "newValue": note})
            else:
                diffs = [diff for diff in diffs if isinstance(diff, dict)]
            filtered_diffs: list[dict[str, object]] = []
            for diff in diffs:
                field_name = str(diff.get("field") or "").strip()
                if not field_name:
                    continue

                old_value_text = str(diff.get("oldValue") or "").strip()
                new_value_text = str(diff.get("newValue") or "").strip()
                if old_value_text == new_value_text:
                    continue

                filtered_diffs.append(
                    {
                        "field": field_name,
                        "oldValue": old_value_text,
                        "newValue": new_value_text,
                    }
                )
            diffs = filtered_diffs

            for diff in diffs:
                field_name = str(diff.get("field") or "").strip()
                old_value = str(diff.get("oldValue") or "")
                new_value = str(diff.get("newValue") or "")
                note_value = f"create {field_name}: {new_value}"
                value_map: dict[str, object] = {
                    "id": str(uuid.uuid4()),
                    "budget_id": create_budget_id,
                    "field": field_name,
                    "old_value": old_value,
                    "new_value": new_value,
                    "note": note_value,
                    "source": normalized_source_action,
                    "created_at": now,
                    "updated_at": now,
                    "tenant_id": tenant_id,
                    "changed_by": normalized_changed_by,
                    "account_code": account_code or None,
                    "service_id": service_id or None,
                    "month": month_value,
                    "year": year_value,
                }
                history_rows.append(tuple(value_map.get(key) for key in history_semantics))

            create_new_data: dict[str, object] = {
                "subService": sub_service,
                "grossAmount": f"{gross_amount:.2f}",
                "commission": f"{commission:.4f}",
                "netAdjustment": f"{net_adjustment:.2f}",
                "note": note,
            }
            create_old_data: dict[str, object] = {
                "subService": None,
                "grossAmount": None,
                "commission": None,
                "netAdjustment": None,
                "note": None,
            }
            create_changed_fields: list[str] = []
            for diff in diffs:
                field_name = str(diff.get("field") or "").strip()
                if not field_name:
                    continue
                if field_name not in create_changed_fields:
                    create_changed_fields.append(field_name)
            budget_change_history_value_map = {
                "budget_id": create_budget_id,
                "action_type": "CREATE",
                "changed_fields": json.dumps(create_changed_fields, ensure_ascii=False),
                "old_data": json.dumps(create_old_data, ensure_ascii=False),
                "new_data": json.dumps(create_new_data, ensure_ascii=False),
                "changed_by": normalized_changed_by,
                "note": create_change_note,
            }
            budget_change_history_rows.append(
                tuple(
                    budget_change_history_value_map.get(key)
                    for key in budget_change_history_semantics
                )
            )

            if not (history_insert_query and can_insert_history):
                continue

        history_count = 0
        if history_insert_query and history_rows:
            try:
                cursor.executemany(history_insert_query, history_rows)
                history_count = int(cursor.rowcount or 0)
            except Exception as exc:
                history_error = str(exc)

        budget_change_history_count = 0
        if budget_change_history_rows:
            cursor.executemany(
                budget_change_history_insert_query,
                budget_change_history_rows,
            )
            budget_change_history_count = int(cursor.rowcount or 0)

        return {
            "updatedRowCount": len(updated_budget_ids),
            "createdRowCount": len(created_budget_ids),
            "deletedRowCount": len(deleted_budget_ids),
            "historyRowCount": budget_change_history_count,
            "updatedBudgetIds": updated_budget_ids,
            "createdBudgetIds": created_budget_ids,
            "deletedBudgetIds": deleted_budget_ids,
            "historyInsertError": history_error,
            "legacyHistoryRowCount": history_count,
            "_affectedBuckets": list(affected_buckets),
        }

    result = run_transaction(_work)
    affected_buckets_raw = result.pop("_affectedBuckets", [])
    affected_buckets = (
        [
            bucket
            for bucket in affected_buckets_raw
            if isinstance(bucket, tuple) and len(bucket) == 3
        ]
        if isinstance(affected_buckets_raw, list)
        else []
    )
    if result.get("updatedRowCount") or result.get("createdRowCount") or result.get("deletedRowCount"):
        if affected_buckets:
            try:
                _refresh_budget_data_cache_buckets(
                    tables=tables,
                    buckets=affected_buckets,
                )
            except Exception:
                cache_key_prefix = _build_budget_data_cache_key_prefix(
                    budgets_table=tables["BUDGETS"],
                )
                delete_tenant_shared_cache_values_by_prefix(
                    bucket=_SHARED_DB_READ_CACHE_BUCKET,
                    cache_key_prefix=cache_key_prefix,
                )
        else:
            cache_key_prefix = _build_budget_data_cache_key_prefix(
                budgets_table=tables["BUDGETS"],
            )
            delete_tenant_shared_cache_values_by_prefix(
                bucket=_SHARED_DB_READ_CACHE_BUCKET,
                cache_key_prefix=cache_key_prefix,
            )
    return result


def update_master_budget_control_budget_data(
    *,
    changes: list[dict[str, object]],
    creates: list[dict[str, object]] | None = None,
    deletes: list[dict[str, object]] | None = None,
    changed_by: str = "budgetData.update",
) -> dict[str, object]:
    """
    Backward-compatible wrapper for masterBudgetControl budget update/create/delete flow.
    """
    return apply_budget_mutations_with_history(
        changes=changes,
        creates=creates,
        deletes=deletes,
        changed_by=changed_by,
        source_action="budgetData.update",
    )
