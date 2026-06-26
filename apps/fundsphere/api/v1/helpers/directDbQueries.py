from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import json
import re
import uuid

import mysql.connector

from apps.fundsphere.api.v1.helpers.directConfig import get_direct_db_tables
from shared.db import execute_write, fetch_all, run_transaction
from shared.normalization import (
    normalize_input_text,
    normalize_optional_input_text,
    normalize_optional_note_text,
)
from shared.tenantDataCache import delete_tenant_shared_cache_values_by_prefix


_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
_BUDGET_MUTABLE_FIELDS = ("subService", "grossAmount", "commission", "netAdjustment", "note")
_DEPARTMENT_FIELDS = ("name", "color", "listingOrder")
_ACCOUNT_FIELDS = ("name", "logoUrl", "conseroId", "conseroName", "strataName", "active", "endDate")
_SERVICE_FIELDS = ("name", "departmentCode", "description", "commission", "netAdjustment", "active")


class ConflictError(ValueError):
    pass


class NotFoundError(ValueError):
    pass


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


def _normalize_db_value(value: object) -> object:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _normalize_db_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_db_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_db_value(item) for item in value]
    return value


def _normalize_row(row: dict[str, object]) -> dict[str, object]:
    return {str(key): _normalize_db_value(value) for key, value in row.items()}


def _parse_json_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


def _parse_json_object(value: object) -> dict[str, object]:
    parsed = _parse_json_value(value)
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(value: object) -> list[object]:
    parsed = _parse_json_value(value)
    return parsed if isinstance(parsed, list) else []


def _parse_bool(value: object, *, default: bool | None = None) -> bool:
    if value is None:
        if default is None:
            raise ValueError("active must be boolean-like")
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = normalize_input_text(value).lower()
    if not text and default is not None:
        return default
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError("active must be boolean-like")


def _parse_int(value: object, *, field: str, required: bool = True) -> int | None:
    if value is None:
        if required:
            raise ValueError(f"{field} is required")
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc


def _parse_decimal(value: object, *, field: str, required: bool = True) -> Decimal | None:
    if value is None or value == "":
        if required:
            raise ValueError(f"{field} is required")
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a valid number") from exc


def _parse_datetime(value: object, *, field: str, required: bool = False) -> datetime | None:
    if value is None or value == "":
        if required:
            raise ValueError(f"{field} is required")
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    text = normalize_input_text(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed_date = date.fromisoformat(text[:10])
        except ValueError as exc:
            raise ValueError(f"{field} must be ISO datetime or date") from exc
        return datetime.combine(parsed_date, time.min)


def _get_tables() -> dict[str, str]:
    return get_direct_db_tables()


def _get_optional_employee_table() -> str | None:
    tables = get_direct_db_tables(require_employees=False)
    return tables.get("EMPLOYEES")


def _resolve_changed_by(request=None) -> str:
    if request is not None:
        header_value = str(getattr(request, "headers", {}).get("x-user-name") or "").strip()
        if header_value:
            return header_value

        principal = getattr(getattr(request, "state", None), "auth_principal", None)
        if principal is not None:
            for attr in ("email", "user_id"):
                value = str(getattr(principal, attr, "") or "").strip()
                if value:
                    return value

        client_id = str(getattr(getattr(request, "state", None), "client_id", "") or "").strip()
        if client_id:
            return client_id

    return "fundsphere.direct"


def _fetch_one(query: str, params: tuple[object, ...] = ()) -> dict[str, object] | None:
    rows = fetch_all(query, params)
    if not rows:
        return None
    return _normalize_row(rows[0])


def _execute_write(query: str, params: tuple[object, ...]) -> int:
    return execute_write(query, params)


def _require_rowcount(rowcount: int, *, message: str) -> None:
    if rowcount <= 0:
        raise NotFoundError(message)


def _invalidate_cache(prefixes: tuple[str, ...]) -> None:
    for prefix in prefixes:
        delete_tenant_shared_cache_values_by_prefix(
            bucket="db_reads",
            cache_key_prefix=prefix,
        )


def _accounts_table() -> str:
    return _get_tables()["ACCOUNTS"]


def _departments_table() -> str:
    return _get_tables()["DEPARTMENTS"]


def _services_table() -> str:
    return _get_tables()["SERVICES"]


def _account_reps_table() -> str:
    return _get_tables()["ACCOUNTREPS"]


def _budgets_table() -> str:
    return _get_tables()["BUDGETS"]


def _budget_change_histories_table() -> str:
    return _get_tables()["BUDGETCHANGEHISTORIES"]


def _employees_table() -> str | None:
    return _get_tables().get("EMPLOYEES")


def _ensure_account_exists(account_code: str) -> None:
    table = _quote_table_name(_accounts_table())
    row = _fetch_one(
        f"SELECT code FROM {table} WHERE code = %s",
        (account_code,),
    )
    if row is None:
        raise NotFoundError(f"Unknown accountCode values: {account_code}")


def _ensure_department_exists(department_code: str) -> None:
    table = _quote_table_name(_departments_table())
    row = _fetch_one(
        f"SELECT code FROM {table} WHERE code = %s",
        (department_code,),
    )
    if row is None:
        raise NotFoundError(f"Unknown departmentCode values: {department_code}")


def _ensure_service_exists(service_id: str) -> None:
    table = _quote_table_name(_services_table())
    row = _fetch_one(
        f"SELECT id FROM {table} WHERE id = %s",
        (service_id,),
    )
    if row is None:
        raise NotFoundError(f"Unknown serviceId values: {service_id}")


def _ensure_employee_exists(employee_id: str) -> None:
    employee_table = _employees_table()
    if not employee_table:
        return
    row = _fetch_one(
        f"SELECT id FROM {_quote_table_name(employee_table)} WHERE id = %s",
        (employee_id,),
    )
    if row is None:
        raise NotFoundError(f"Unknown employeeId values: {employee_id}")


def _serialize_budget_change_row(row: dict[str, object]) -> dict[str, object]:
    serialized = _normalize_row(row)
    serialized["changedFields"] = _parse_json_list(serialized.get("changedFields"))
    serialized["oldData"] = _parse_json_object(serialized.get("oldData"))
    serialized["newData"] = _parse_json_object(serialized.get("newData"))
    return serialized


def _db_reads_prefix(table_name: str) -> str:
    return f"{table_name.strip().lower()}"


def _accounts_cache_prefix() -> str:
    return f"accounts::{_db_reads_prefix(_accounts_table())}"


def _services_cache_prefix() -> str:
    return f"services::{_db_reads_prefix(_services_table())}::{_db_reads_prefix(_departments_table())}"


def _budget_data_cache_prefix() -> str:
    return f"budget_data::{_db_reads_prefix(_budgets_table())}::"


def _invalidate_account_related_caches() -> None:
    _invalidate_cache(
        (
            _accounts_cache_prefix(),
            _budget_data_cache_prefix(),
        )
    )


def _invalidate_department_related_caches() -> None:
    _invalidate_cache(
        (
            _services_cache_prefix(),
            _budget_data_cache_prefix(),
        )
    )


def _invalidate_service_related_caches() -> None:
    _invalidate_cache(
        (
            _services_cache_prefix(),
            _budget_data_cache_prefix(),
        )
    )


# ============================================================================
# ACCOUNTS
# ============================================================================


def list_accounts(*, code: str | None = None, active: bool = True) -> list[dict[str, object]]:
    table = _quote_table_name(_accounts_table())
    where_parts: list[str] = []
    params: list[object] = []

    code_text = normalize_optional_input_text(code)
    if code_text:
        where_parts.append("code = %s")
        params.append(code_text.upper())

    if active:
        where_parts.append("active = 1")

    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    query = (
        "SELECT dateCreated, dateUpdated, code, name, logoUrl, conseroId, "
        "conseroName, strataName, active, endDate "
        f"FROM {table}{where_sql} ORDER BY code ASC"
    )
    return [_normalize_row(row) for row in fetch_all(query, tuple(params))]


def create_account(payload: dict[str, object]) -> dict[str, object]:
    code = normalize_input_text(payload.get("code")).upper()
    name = normalize_input_text(payload.get("name"))
    if not code:
        raise ValueError("code is required")
    if not name:
        raise ValueError("name is required")

    logo_url = normalize_optional_input_text(payload.get("logoUrl"))
    consero_id = normalize_optional_input_text(payload.get("conseroId"))
    consero_name = normalize_optional_input_text(payload.get("conseroName"))
    strata_name = normalize_optional_input_text(payload.get("strataName"))
    active = 1 if _parse_bool(payload.get("active"), default=True) else 0
    end_date = _parse_datetime(payload.get("endDate"), field="endDate", required=False)

    query = (
        f"INSERT INTO {_quote_table_name(_accounts_table())} "
        "(code, name, logoUrl, conseroId, conseroName, strataName, active, endDate) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    try:
        rowcount = _execute_write(
            query,
            (code, name, logo_url, consero_id, consero_name, strata_name, active, end_date),
        )
    except mysql.connector.Error as exc:
        if "duplicate" in str(exc).lower():
            raise ConflictError(f"Account already exists: {code}") from exc
        raise
    _require_rowcount(rowcount, message=f"Account already exists: {code}")
    _invalidate_account_related_caches()
    return {"inserted": rowcount, "code": code}


def update_account(*, code: str, payload: dict[str, object]) -> dict[str, object]:
    code = normalize_input_text(code).upper()
    if not code:
        raise ValueError("code is required")

    assignments: list[str] = []
    params: list[object] = []
    field_map = {
        "name": lambda value: normalize_input_text(value),
        "logoUrl": lambda value: normalize_optional_input_text(value),
        "conseroId": lambda value: normalize_optional_input_text(value),
        "conseroName": lambda value: normalize_optional_input_text(value),
        "strataName": lambda value: normalize_optional_input_text(value),
        "active": lambda value: 1 if _parse_bool(value, default=True) else 0,
        "endDate": lambda value: _parse_datetime(value, field="endDate", required=False),
    }

    for field in _ACCOUNT_FIELDS:
        if field not in payload:
            continue
        if field == "name":
            value = normalize_input_text(payload.get(field))
            if not value:
                raise ValueError("name cannot be empty")
        else:
            value = field_map[field](payload.get(field))
        assignments.append(f"{field} = %s")
        params.append(value)

    if not assignments:
        raise ValueError("No updatable fields provided")

    params.append(code)
    query = (
        f"UPDATE {_quote_table_name(_accounts_table())} "
        f"SET {', '.join(assignments)} "
        "WHERE code = %s"
    )
    try:
        rowcount = _execute_write(query, tuple(params))
    except mysql.connector.Error as exc:
        if "duplicate" in str(exc).lower():
            raise ConflictError(f"Account already exists: {code}") from exc
        raise
    _require_rowcount(rowcount, message="Account not found")
    _invalidate_account_related_caches()
    return {"updated": rowcount, "code": code}


def get_account(*, code: str) -> dict[str, object] | None:
    table = _quote_table_name(_accounts_table())
    row = _fetch_one(
        f"SELECT dateCreated, dateUpdated, code, name, logoUrl, conseroId, "
        f"conseroName, strataName, active, endDate FROM {table} WHERE code = %s",
        (normalize_input_text(code).upper(),),
    )
    return row


# ============================================================================
# DEPARTMENTS
# ============================================================================


def list_departments(*, code: str | None = None, name: str | None = None) -> list[dict[str, object]]:
    table = _quote_table_name(_departments_table())
    where_parts: list[str] = []
    params: list[object] = []

    code_text = normalize_optional_input_text(code)
    if code_text:
        where_parts.append("code = %s")
        params.append(code_text.upper())

    name_text = normalize_optional_input_text(name)
    if name_text:
        where_parts.append("LOWER(name) LIKE LOWER(%s)")
        params.append(f"%{name_text}%")

    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    query = (
        "SELECT dateCreated, dateUpdated, code, name, color, listingOrder "
        f"FROM {table}{where_sql} ORDER BY "
        "COALESCE(listingOrder, 255) ASC, code ASC"
    )
    return [_normalize_row(row) for row in fetch_all(query, tuple(params))]


def create_department(payload: dict[str, object]) -> dict[str, object]:
    code = normalize_input_text(payload.get("code")).upper()
    name = normalize_input_text(payload.get("name"))
    if not code:
        raise ValueError("code is required")
    if not name:
        raise ValueError("name is required")

    color = normalize_optional_input_text(payload.get("color"))
    listing_order = _parse_int(payload.get("listingOrder"), field="listingOrder", required=False)
    query = (
        f"INSERT INTO {_quote_table_name(_departments_table())} "
        "(code, name, color, listingOrder) VALUES (%s, %s, %s, %s)"
    )
    try:
        rowcount = _execute_write(query, (code, name, color, listing_order))
    except mysql.connector.Error as exc:
        if "duplicate" in str(exc).lower():
            raise ConflictError(f"Department already exists: {code}") from exc
        raise
    _require_rowcount(rowcount, message=f"Department already exists: {code}")
    _invalidate_department_related_caches()
    return {"inserted": rowcount, "code": code}


def update_department(*, code: str, payload: dict[str, object]) -> dict[str, object]:
    code = normalize_input_text(code).upper()
    if not code:
        raise ValueError("code is required")

    assignments: list[str] = []
    params: list[object] = []
    for field in _DEPARTMENT_FIELDS:
        if field not in payload:
            continue
        if field == "name":
            value = normalize_input_text(payload.get(field))
            if not value:
                raise ValueError("name cannot be empty")
        elif field == "listingOrder":
            value = _parse_int(payload.get(field), field="listingOrder", required=False)
        else:
            value = normalize_optional_input_text(payload.get(field))
        assignments.append(f"{field} = %s")
        params.append(value)

    if not assignments:
        raise ValueError("No updatable fields provided")

    params.append(code)
    query = (
        f"UPDATE {_quote_table_name(_departments_table())} "
        f"SET {', '.join(assignments)} WHERE code = %s"
    )
    try:
        rowcount = _execute_write(query, tuple(params))
    except mysql.connector.Error as exc:
        if "duplicate" in str(exc).lower():
            raise ConflictError(f"Department already exists: {code}") from exc
        raise
    _require_rowcount(rowcount, message="Department not found")
    _invalidate_department_related_caches()
    return {"updated": rowcount, "code": code}


def get_department(*, code: str) -> dict[str, object] | None:
    table = _quote_table_name(_departments_table())
    return _fetch_one(
        f"SELECT dateCreated, dateUpdated, code, name, color, listingOrder "
        f"FROM {table} WHERE code = %s",
        (normalize_input_text(code).upper(),),
    )


# ============================================================================
# SERVICES
# ============================================================================


def list_services(
    *,
    id: str | None = None,
    department_code: str | None = None,
    active: bool = True,
) -> list[dict[str, object]]:
    services_table = _quote_table_name(_services_table())
    departments_table = _quote_table_name(_departments_table())
    where_parts: list[str] = []
    params: list[object] = []

    service_id_text = normalize_optional_input_text(id)
    if service_id_text:
        where_parts.append("s.id = %s")
        params.append(service_id_text)

    department_code_text = normalize_optional_input_text(department_code)
    if department_code_text:
        where_parts.append("s.departmentCode = %s")
        params.append(department_code_text.upper())

    if active:
        where_parts.append("s.active = 1")

    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    query = (
        "SELECT "
        "s.dateCreated, s.dateUpdated, s.id, s.name, s.conseroId, s.departmentCode, "
        "d.name AS departmentName, d.listingOrder AS departmentListingOrder, "
        "s.description, s.commission, s.netAdjustment, s.active "
        f"FROM {services_table} s "
        f"INNER JOIN {departments_table} d ON s.departmentCode = d.code"
        f"{where_sql} "
        "ORDER BY COALESCE(d.listingOrder, 255) ASC, s.name ASC"
    )
    return [_normalize_row(row) for row in fetch_all(query, tuple(params))]


def get_service(*, service_id: str) -> dict[str, object] | None:
    services_table = _quote_table_name(_services_table())
    departments_table = _quote_table_name(_departments_table())
    return _fetch_one(
        "SELECT "
        "s.dateCreated, s.dateUpdated, s.id, s.name, s.conseroId, s.departmentCode, "
        "d.name AS departmentName, d.listingOrder AS departmentListingOrder, "
        "s.description, s.commission, s.netAdjustment, s.active "
        f"FROM {services_table} s "
        f"INNER JOIN {departments_table} d ON s.departmentCode = d.code "
        "WHERE s.id = %s",
        (normalize_input_text(service_id),),
    )


def create_service(payload: dict[str, object]) -> dict[str, object]:
    service_id = normalize_optional_input_text(payload.get("id")) or str(uuid.uuid4())
    name = normalize_input_text(payload.get("name"))
    department_code = normalize_input_text(payload.get("departmentCode")).upper()
    if not name:
        raise ValueError("name is required")
    if not department_code:
        raise ValueError("departmentCode is required")

    _ensure_department_exists(department_code)
    consero_id = normalize_optional_input_text(payload.get("conseroId"))
    description = normalize_optional_input_text(payload.get("description"))
    commission = _parse_decimal(payload.get("commission"), field="commission", required=False) or Decimal("0")
    net_adjustment = _parse_decimal(payload.get("netAdjustment"), field="netAdjustment", required=False) or Decimal("0")
    active = 1 if _parse_bool(payload.get("active"), default=True) else 0

    query = (
        f"INSERT INTO {_quote_table_name(_services_table())} "
        "(id, name, conseroId, departmentCode, description, commission, netAdjustment, active) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    try:
        rowcount = _execute_write(
            query,
            (
                service_id,
                name,
                consero_id,
                department_code,
                description,
                commission,
                net_adjustment,
                active,
            ),
        )
    except mysql.connector.Error as exc:
        if "duplicate" in str(exc).lower():
            raise ConflictError(f"Service already exists: {service_id}") from exc
        raise
    _require_rowcount(rowcount, message=f"Service already exists: {service_id}")
    _invalidate_service_related_caches()
    return {"inserted": rowcount, "id": service_id}


def update_service(*, service_id: str, payload: dict[str, object]) -> dict[str, object]:
    service_id = normalize_input_text(service_id)
    if not service_id:
        raise ValueError("id is required")

    assignments: list[str] = []
    params: list[object] = []
    for field in _SERVICE_FIELDS:
        if field not in payload:
            continue
        if field == "name":
            value = normalize_input_text(payload.get(field))
            if not value:
                raise ValueError("name cannot be empty")
        elif field == "departmentCode":
            value = normalize_input_text(payload.get(field)).upper()
            if not value:
                raise ValueError("departmentCode cannot be empty")
            _ensure_department_exists(value)
        elif field in {"commission", "netAdjustment"}:
            value = _parse_decimal(payload.get(field), field=field, required=False)
            if value is None:
                value = Decimal("0")
        elif field == "active":
            value = 1 if _parse_bool(payload.get(field), default=True) else 0
        else:
            value = normalize_optional_input_text(payload.get(field))
        assignments.append(f"{field} = %s")
        params.append(value)

    if not assignments:
        raise ValueError("No updatable fields provided")

    params.append(service_id)
    query = (
        f"UPDATE {_quote_table_name(_services_table())} "
        f"SET {', '.join(assignments)} WHERE id = %s"
    )
    try:
        rowcount = _execute_write(query, tuple(params))
    except mysql.connector.Error as exc:
        if "duplicate" in str(exc).lower():
            raise ConflictError(f"Service already exists: {service_id}") from exc
        raise
    _require_rowcount(rowcount, message="Service not found")
    _invalidate_service_related_caches()
    return {"updated": rowcount, "id": service_id}


# ============================================================================
# ACCOUNT REPS
# ============================================================================


def list_account_reps(
    *,
    id: str | None = None,
    account_code: str | None = None,
    employee_id: str | None = None,
) -> list[dict[str, object]]:
    table = _quote_table_name(_account_reps_table())
    where_parts: list[str] = []
    params: list[object] = []

    id_text = normalize_optional_input_text(id)
    if id_text:
        where_parts.append("id = %s")
        params.append(id_text)

    account_code_text = normalize_optional_input_text(account_code)
    if account_code_text:
        where_parts.append("accountCode = %s")
        params.append(account_code_text.upper())

    employee_id_text = normalize_optional_input_text(employee_id)
    if employee_id_text:
        where_parts.append("employeeId = %s")
        params.append(employee_id_text)

    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    query = (
        "SELECT dateCreated, dateUpdated, id, accountCode, employeeId "
        f"FROM {table}{where_sql} ORDER BY accountCode ASC, employeeId ASC"
    )
    return [_normalize_row(row) for row in fetch_all(query, tuple(params))]


def get_account_rep(*, rep_id: str) -> dict[str, object] | None:
    table = _quote_table_name(_account_reps_table())
    return _fetch_one(
        f"SELECT dateCreated, dateUpdated, id, accountCode, employeeId FROM {table} WHERE id = %s",
        (normalize_input_text(rep_id),),
    )


def create_account_rep(payload: dict[str, object]) -> dict[str, object]:
    rep_id = normalize_optional_input_text(payload.get("id")) or str(uuid.uuid4())
    account_code = normalize_input_text(payload.get("accountCode")).upper()
    employee_id = normalize_input_text(payload.get("employeeId"))
    if not account_code:
        raise ValueError("accountCode is required")
    if not employee_id:
        raise ValueError("employeeId is required")

    _ensure_account_exists(account_code)
    _ensure_employee_exists(employee_id)

    query = (
        f"INSERT INTO {_quote_table_name(_account_reps_table())} "
        "(id, accountCode, employeeId) VALUES (%s, %s, %s)"
    )
    try:
        rowcount = _execute_write(query, (rep_id, account_code, employee_id))
    except mysql.connector.Error as exc:
        if "duplicate" in str(exc).lower():
            raise ConflictError(
                f"Account rep already exists for accountCode={account_code} and employeeId={employee_id}"
            ) from exc
        raise
    _require_rowcount(rowcount, message="Account rep already exists")
    return {"inserted": rowcount, "id": rep_id}


def delete_account_rep(
    *,
    rep_id: str | None = None,
    account_code: str | None = None,
    employee_id: str | None = None,
) -> dict[str, object]:
    table = _quote_table_name(_account_reps_table())
    where_clause = ""
    params: tuple[object, ...] = tuple()

    rep_id_text = normalize_optional_input_text(rep_id)
    if rep_id_text:
        where_clause = "id = %s"
        params = (rep_id_text,)
    else:
        account_code_text = normalize_optional_input_text(account_code)
        employee_id_text = normalize_optional_input_text(employee_id)
        if not account_code_text or not employee_id_text:
            raise ValueError("id or accountCode + employeeId is required")
        where_clause = "accountCode = %s AND employeeId = %s"
        params = (account_code_text.upper(), employee_id_text)

    rowcount = _execute_write(
        f"DELETE FROM {table} WHERE {where_clause}",
        params,
    )
    _require_rowcount(rowcount, message="Account rep not found")
    return {"deleted": rowcount}


# ============================================================================
# BUDGETS + AUDIT HISTORY
# ============================================================================


def _budget_select_query(where_sql: str) -> str:
    budgets_table = _quote_table_name(_budgets_table())
    accounts_table = _quote_table_name(_accounts_table())
    services_table = _quote_table_name(_services_table())
    departments_table = _quote_table_name(_departments_table())
    return (
        "SELECT "
        "b.dateCreated, b.dateUpdated, b.id, b.accountCode, a.name AS accountName, "
        "b.month, b.year, b.serviceId, s.name AS serviceName, s.departmentCode, "
        "d.name AS departmentName, b.subService, b.grossAmount, b.commission, "
        "b.netAdjustment, b.netAmount, b.note "
        f"FROM {budgets_table} b "
        f"INNER JOIN {accounts_table} a ON b.accountCode = a.code "
        f"INNER JOIN {services_table} s ON b.serviceId = s.id "
        f"INNER JOIN {departments_table} d ON s.departmentCode = d.code "
        f"{where_sql} "
        "ORDER BY b.accountCode ASC, b.year DESC, b.month DESC, "
        "COALESCE(d.listingOrder, 255) ASC, s.name ASC, b.subService ASC"
    )


def list_budgets(
    *,
    id: str | None = None,
    account_code: str | None = None,
    service_id: str | None = None,
    department_code: str | None = None,
    month: int | None = None,
    year: int | None = None,
    sub_service: str | None = None,
) -> list[dict[str, object]]:
    where_parts: list[str] = []
    params: list[object] = []

    id_text = normalize_optional_input_text(id)
    if id_text:
        where_parts.append("b.id = %s")
        params.append(id_text)

    account_code_text = normalize_optional_input_text(account_code)
    if account_code_text:
        where_parts.append("b.accountCode = %s")
        params.append(account_code_text.upper())

    service_id_text = normalize_optional_input_text(service_id)
    if service_id_text:
        where_parts.append("b.serviceId = %s")
        params.append(service_id_text)

    department_code_text = normalize_optional_input_text(department_code)
    if department_code_text:
        where_parts.append("s.departmentCode = %s")
        params.append(department_code_text.upper())

    if month is not None:
        where_parts.append("b.month = %s")
        params.append(_parse_int(month, field="month"))
    if year is not None:
        where_parts.append("b.year = %s")
        params.append(_parse_int(year, field="year"))

    sub_service_text = normalize_optional_input_text(sub_service)
    if sub_service_text:
        where_parts.append("b.subService = %s")
        params.append(sub_service_text)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    query = _budget_select_query(where_sql)
    return [_normalize_row(row) for row in fetch_all(query, tuple(params))]


def get_budget(*, budget_id: str) -> dict[str, object] | None:
    row = _fetch_one(
        _budget_select_query("WHERE b.id = %s"),
        (normalize_input_text(budget_id),),
    )
    return row


def _build_budget_history_insert_query() -> str:
    table = _quote_table_name(_budget_change_histories_table())
    return (
        f"INSERT INTO {table} "
        "(budgetId, actionType, changedFields, oldData, newData, changedBy, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )


def _insert_budget_history(
    cursor,
    *,
    budget_id: str,
    action_type: str,
    changed_fields: list[str],
    old_data: dict[str, object] | None,
    new_data: dict[str, object] | None,
    changed_by: str,
    note: str | None,
) -> None:
    cursor.execute(
        _build_budget_history_insert_query(),
        (
            budget_id,
            action_type,
            json.dumps(changed_fields, ensure_ascii=False),
            json.dumps(old_data, ensure_ascii=False) if old_data is not None else None,
            json.dumps(new_data, ensure_ascii=False) if new_data is not None else None,
            changed_by,
            note,
        ),
    )


def _budget_mutation_common_values(payload: dict[str, object]) -> tuple[str, int, int, str, str, Decimal, Decimal, Decimal, str | None]:
    account_code = normalize_input_text(payload.get("accountCode")).upper()
    service_id = normalize_input_text(payload.get("serviceId"))
    sub_service = normalize_optional_input_text(payload.get("subService")) or ""
    month = _parse_int(payload.get("month"), field="month")
    year = _parse_int(payload.get("year"), field="year")
    gross_amount = _parse_decimal(payload.get("grossAmount"), field="grossAmount", required=True) or Decimal("0")
    commission = _parse_decimal(payload.get("commission"), field="commission", required=False) or Decimal("0")
    net_adjustment = _parse_decimal(payload.get("netAdjustment"), field="netAdjustment", required=False) or Decimal("0")
    note = normalize_optional_note_text(payload.get("note"))

    if not account_code:
        raise ValueError("accountCode is required")
    if not service_id:
        raise ValueError("serviceId is required")
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")
    if year < 1000 or year > 9999:
        raise ValueError("year must be a 4-digit year")
    return (
        account_code,
        month,
        year,
        service_id,
        sub_service,
        gross_amount,
        commission,
        net_adjustment,
        note,
    )


def create_budget(*, payload: dict[str, object], request=None) -> dict[str, object]:
    (
        account_code,
        month,
        year,
        service_id,
        sub_service,
        gross_amount,
        commission,
        net_adjustment,
        note,
    ) = _budget_mutation_common_values(payload)
    budget_id = normalize_optional_input_text(payload.get("id")) or str(uuid.uuid4())
    changed_by = _resolve_changed_by(request)

    _ensure_account_exists(account_code)
    _ensure_service_exists(service_id)

    budgets_table = _quote_table_name(_budgets_table())
    insert_query = (
        f"INSERT INTO {budgets_table} "
        "(id, accountCode, month, year, serviceId, subService, grossAmount, commission, netAdjustment, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    def _work(cursor):
        try:
            cursor.execute(
                insert_query,
                (
                    budget_id,
                    account_code,
                    month,
                    year,
                    service_id,
                    sub_service,
                    gross_amount,
                    commission,
                    net_adjustment,
                    note,
                ),
            )
        except mysql.connector.Error as exc:
            if "duplicate" in str(exc).lower():
                raise ConflictError(
                    "Budget already exists for the selected account, month, year, service, and sub-service"
                ) from exc
            raise

        _insert_budget_history(
            cursor,
            budget_id=budget_id,
            action_type="CREATE",
            changed_fields=list(_BUDGET_MUTABLE_FIELDS) + ["accountCode", "month", "year", "serviceId"],
            old_data={},
            new_data={
                "accountCode": account_code,
                "month": month,
                "year": year,
                "serviceId": service_id,
                "subService": sub_service,
                "grossAmount": str(gross_amount),
                "commission": str(commission),
                "netAdjustment": str(net_adjustment),
                "note": note,
            },
            changed_by=changed_by,
            note=normalize_optional_note_text(payload.get("changeNote")),
        )
        return int(cursor.rowcount or 0)

    rowcount = run_transaction(_work)
    _require_rowcount(rowcount, message="Budget insert did not affect any row")
    _invalidate_cache(
        (
            f"budget_data::{_budgets_table().strip().lower()}::",
        )
    )
    return {"inserted": rowcount, "id": budget_id}


def update_budget(*, budget_id: str, payload: dict[str, object], request=None) -> dict[str, object]:
    budget_id = normalize_input_text(budget_id)
    if not budget_id:
        raise ValueError("id is required")

    changed_by = _resolve_changed_by(request)
    existing_row = get_budget(budget_id=budget_id)
    if existing_row is None:
        raise NotFoundError("Budget not found")

    updates: list[str] = []
    params: list[object] = []
    old_data: dict[str, object] = {}
    new_data: dict[str, object] = {}
    changed_fields: list[str] = []

    mutable_value_parser = {
        "subService": lambda value: normalize_optional_input_text(value) or "",
        "grossAmount": lambda value: _parse_decimal(value, field="grossAmount", required=True) or Decimal("0"),
        "commission": lambda value: _parse_decimal(value, field="commission", required=False) or Decimal("0"),
        "netAdjustment": lambda value: _parse_decimal(value, field="netAdjustment", required=False) or Decimal("0"),
        "note": lambda value: normalize_optional_note_text(value),
    }

    for field in _BUDGET_MUTABLE_FIELDS:
        if field not in payload:
            continue
        value = mutable_value_parser[field](payload.get(field))
        current_value = existing_row.get(field)
        current_compare = _normalize_db_value(current_value)
        next_compare = _normalize_db_value(value)
        if current_compare == next_compare:
            continue
        updates.append(f"{field} = %s")
        params.append(value)
        changed_fields.append(field)
        old_data[field] = current_compare
        new_data[field] = next_compare

    if not updates:
        raise ValueError("No updatable fields provided")

    params.append(budget_id)
    update_query = (
        f"UPDATE {_quote_table_name(_budgets_table())} "
        f"SET {', '.join(updates)} WHERE id = %s"
    )

    def _work(cursor):
        cursor.execute(update_query, tuple(params))
        if int(cursor.rowcount or 0) <= 0:
            raise NotFoundError("Budget not found")

        _insert_budget_history(
            cursor,
            budget_id=budget_id,
            action_type="UPDATE",
            changed_fields=changed_fields,
            old_data=old_data,
            new_data=new_data,
            changed_by=changed_by,
            note=normalize_optional_note_text(payload.get("changeNote")),
        )
        return int(cursor.rowcount or 0)

    rowcount = run_transaction(_work)
    _invalidate_cache(
        (
            f"budget_data::{_budgets_table().strip().lower()}::",
        )
    )
    return {"updated": rowcount, "id": budget_id}


def delete_budget(*, budget_id: str, request=None) -> dict[str, object]:
    budget_id = normalize_input_text(budget_id)
    if not budget_id:
        raise ValueError("id is required")

    changed_by = _resolve_changed_by(request)
    existing_row = get_budget(budget_id=budget_id)
    if existing_row is None:
        raise NotFoundError("Budget not found")

    old_data = {
        "accountCode": existing_row.get("accountCode"),
        "month": existing_row.get("month"),
        "year": existing_row.get("year"),
        "serviceId": existing_row.get("serviceId"),
        "subService": existing_row.get("subService"),
        "grossAmount": existing_row.get("grossAmount"),
        "commission": existing_row.get("commission"),
        "netAdjustment": existing_row.get("netAdjustment"),
        "note": existing_row.get("note"),
    }

    delete_query = f"DELETE FROM {_quote_table_name(_budgets_table())} WHERE id = %s"

    def _work(cursor):
        cursor.execute(delete_query, (budget_id,))
        if int(cursor.rowcount or 0) <= 0:
            raise NotFoundError("Budget not found")
        _insert_budget_history(
            cursor,
            budget_id=budget_id,
            action_type="DELETE",
            changed_fields=list(_BUDGET_MUTABLE_FIELDS) + ["accountCode", "month", "year", "serviceId"],
            old_data=old_data,
            new_data=None,
            changed_by=changed_by,
            note=normalize_optional_note_text(None),
        )
        return int(cursor.rowcount or 0)

    rowcount = run_transaction(_work)
    _invalidate_cache(
        (
            f"budget_data::{_budgets_table().strip().lower()}::",
        )
    )
    return {"deleted": rowcount, "id": budget_id}


def list_budget_change_histories(
    *,
    id: str | None = None,
    budget_id: str | None = None,
    action_type: str | None = None,
    changed_by: str | None = None,
    account_code: str | None = None,
    service_id: str | None = None,
    month: int | None = None,
    year: int | None = None,
) -> list[dict[str, object]]:
    table = _quote_table_name(_budget_change_histories_table())
    budgets_table = _quote_table_name(_budgets_table())
    where_parts: list[str] = []
    params: list[object] = []

    id_text = normalize_optional_input_text(id)
    if id_text:
        where_parts.append("h.id = %s")
        params.append(id_text)

    budget_id_text = normalize_optional_input_text(budget_id)
    if budget_id_text:
        where_parts.append("h.budgetId = %s")
        params.append(budget_id_text)

    action_type_text = normalize_optional_input_text(action_type)
    if action_type_text:
        where_parts.append("h.actionType = %s")
        params.append(action_type_text.upper())

    changed_by_text = normalize_optional_input_text(changed_by)
    if changed_by_text:
        where_parts.append("LOWER(h.changedBy) LIKE LOWER(%s)")
        params.append(f"%{changed_by_text}%")

    account_code_text = normalize_optional_input_text(account_code)
    if account_code_text:
        where_parts.append("b.accountCode = %s")
        params.append(account_code_text.upper())

    service_id_text = normalize_optional_input_text(service_id)
    if service_id_text:
        where_parts.append("b.serviceId = %s")
        params.append(service_id_text)

    if month is not None:
        where_parts.append("b.month = %s")
        params.append(_parse_int(month, field="month"))
    if year is not None:
        where_parts.append("b.year = %s")
        params.append(_parse_int(year, field="year"))

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    query = (
        "SELECT "
        "h.dateCreated, h.dateUpdated, h.id, h.budgetId, h.actionType, "
        "h.changedFields, h.oldData, h.newData, h.changedBy, h.note, "
        "b.accountCode, b.serviceId, b.month, b.year "
        f"FROM {table} h "
        f"LEFT JOIN {budgets_table} b ON h.budgetId = b.id "
        f"{where_sql} "
        "ORDER BY h.dateCreated DESC, h.id DESC"
    )
    rows = fetch_all(query, tuple(params))
    return [_serialize_budget_change_row(_normalize_row(row)) for row in rows]


def get_budget_change_history(*, history_id: str) -> dict[str, object] | None:
    rows = list_budget_change_histories(id=history_id)
    return rows[0] if rows else None
