from datetime import date

from datetime import timedelta
import calendar

from collections import defaultdict

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from apps.spendsphere.api.v1.helpers.config import (
    get_acceleration_scope_types,
    get_adtypes,
)
from apps.spendsphere.api.v1.helpers.db_queries import (
    get_accelerations,
    get_existing_acceleration_keys,
    insert_accelerations,
    soft_delete_accelerations_by_ids,
    update_accelerations,
)
from apps.spendsphere.api.v1.helpers.ggAd import (
    get_ggad_accounts,
    get_ggad_budgets,
)
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    normalize_account_codes,
    validate_account_codes,
)

router = APIRouter()


# ============================================================
# ACCELERATIONS
# ============================================================


@router.get(
    "/accelerations",
    summary="List active accelerations for account codes",
    description=(
        "Returns active accelerations for the specified account codes. "
        "Optionally filter by a month/year."
    ),
)
def get_accelerations_route(
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    account_code: str | None = Query(None, alias="accountCode"),
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
    include_all: bool = Query(
        False,
        alias="includeAll",
        description="Include inactive accelerations when true.",
    ),
):
    """
    Example request:
        GET /api/spendsphere/v1/accelerations?accountCodes=TAAA

    Example request (filter by month/year):
        GET /api/spendsphere/v1/accelerations?accountCodes=TAAA&month=1&year=2026

    Example request (include inactive accelerations):
        GET /api/spendsphere/v1/accelerations?accountCodes=TAAA&month=1&year=2026&includeAll=true

    Note:
    When month/year are provided, results include any accelerations that
    overlap the range (startDate <= end_date AND endDate >= start_date).

    Example response:
        [
          {
            "id": 12,
            "accountCode": "TAAA",
            "scopeLevel": "ACCOUNT",
            "scopeValue": "TAAA",
            "startDate": "2026-01-01",
            "endDate": "2026-01-31",
            "multiplier": 120.0,
            "note": "Front-load for January",
            "active": 1
          }
        ]
    """
    requested_codes = normalize_account_codes(account_codes)
    if not requested_codes and isinstance(account_code, str) and account_code.strip():
        requested_codes = normalize_account_codes([account_code])
    if not requested_codes:
        raise HTTPException(
            status_code=400,
            detail="accountCodes is required",
        )

    if (month is None) != (year is None):
        raise HTTPException(
            status_code=400,
            detail="month and year must be provided together",
        )

    if month is not None:
        if month < 1 or month > 12:
            raise HTTPException(status_code=400, detail="Invalid month")
        if year < 2000 or year > 2100:
            raise HTTPException(status_code=400, detail="Invalid year")
        start_date = date(year, month, 1)
        end_date = date(year, month, calendar.monthrange(year, month)[1])
    else:
        start_date = None
        end_date = None

    validate_account_codes(
        requested_codes,
        include_all=include_all,
        month=month,
        year=year,
    )

    data = get_accelerations(
        requested_codes,
        start_date=start_date,
        end_date=end_date,
        include_all=include_all,
    )
    if not data:
        return []

    sanitized = []
    for row in data:
        if isinstance(row, dict):
            sanitized.append(
                {
                    k: v
                    for k, v in row.items()
                    if k not in {"dateCreated", "dateUpdated"}
                }
            )
        else:
            sanitized.append(row)

    return sanitized


class AccelerationPayload(BaseModel):
    accountCode: str
    scopeLevel: str
    scopeValue: str
    startDate: date
    endDate: date
    multiplier: float
    note: str | None = Field(default=None, max_length=2048)

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "accountCode": "TAAA",
                    "scopeLevel": "ACCOUNT",
                    "scopeValue": "TAAA",
                    "startDate": "2026-01-01",
                    "endDate": "2026-01-31",
                    "multiplier": 120.0,
                }
            ]
        },
    )


class AccelerationMonthPayload(BaseModel):
    accountCodes: list[str]
    scopeLevel: str
    scopeValue: str = Field(alias="scope_value")
    multiplier: float
    month: int
    year: int
    dayFront: int = Field(alias="day_front")
    note: str | None = Field(default=None, max_length=2048)

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "accountCodes": [],
                    "scopeLevel": "AD_TYPE",
                    "scope_value": "SEM",
                    "multiplier": 120.0,
                    "month": 1,
                    "year": 2026,
                    "dayFront": 15,
                }
            ]
        },
    )


class AccelerationMonthAccountsPayload(BaseModel):
    accountCodes: list[str] = Field(default_factory=list)
    scopeLevel: str
    scopeValue: str = Field(alias="scope_value")
    multiplier: float
    month: int | None = None
    year: int | None = None
    startDate: date | None = None
    endDate: date | None = None
    note: str | None = Field(default=None, max_length=2048)

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "accountCodes": ["TAAA", "LACS"],
                    "scopeLevel": "AD_TYPE",
                    "scope_value": "SEM",
                    "multiplier": 120.0,
                    "month": 1,
                    "year": 2026,
                    "startDate": None,
                    "endDate": None,
                }
            ]
        },
    )


class AccelerationIdsPayload(BaseModel):
    ids: list[object] = Field(default_factory=list)

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {"ids": [101, 102]}
            ]
        },
    )


def _normalize_codes(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        code = value.strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def _resolve_account_codes(
    account_codes: list[str],
    *,
    month: int | None = None,
    year: int | None = None,
    as_of: date | None = None,
) -> list[str]:
    if not account_codes:
        resolved_accounts = validate_account_codes(
            None,
            month=month,
            year=year,
            as_of=as_of,
        )
        if not resolved_accounts:
            # Fallback when active-period data is empty/unavailable for the period:
            # use non-zzz Google Ads accounts so bulk routes remain operable.
            resolved_accounts = [
                row
                for row in validate_account_codes(
                    None,
                    include_all=True,
                    month=month,
                    year=year,
                    as_of=as_of,
                )
                if not bool(row.get("inactiveByName"))
            ]
        return [
            str(a.get("code", "")).strip().upper()
            for a in resolved_accounts
            if str(a.get("code", "")).strip()
        ]

    validated = validate_account_codes(
        account_codes,
        month=month,
        year=year,
        as_of=as_of,
    )
    return [
        str(a.get("code", "")).strip().upper()
        for a in validated
        if str(a.get("code", "")).strip()
    ]


def _normalize_and_validate_rows(
    rows: list[dict],
    *,
    enforce_multiplier_min: bool = True,
    validate_account_codes_rows: bool = True,
) -> list[dict]:
    if not rows:
        raise HTTPException(status_code=400, detail="No accelerations provided")

    allowed_scopes = {s.upper() for s in get_acceleration_scope_types()}
    allowed_adtypes = {k.upper() for k in get_adtypes().keys()}

    errors: list[dict] = []

    for idx, data in enumerate(rows):
        account_code = str(data.get("accountCode", "")).strip().upper()
        scope_type = str(data.get("scopeLevel", "")).strip().upper()
        scope_value = str(data.get("scopeValue", "")).strip()
        multiplier = data.get("multiplier")
        start_date = data.get("startDate")
        end_date = data.get("endDate")
        if isinstance(multiplier, bool) or not isinstance(multiplier, (int, float)):
            errors.append(
                {
                    "index": idx,
                    "field": "multiplier",
                    "value": multiplier,
                    "message": "multiplier must be a number",
                }
            )
        elif enforce_multiplier_min and multiplier < 100:
            errors.append(
                {
                    "index": idx,
                    "field": "multiplier",
                    "value": multiplier,
                    "message": "multiplier must be >= 100",
                }
            )
        if start_date and end_date and start_date > end_date:
            errors.append(
                {
                    "index": idx,
                    "field": "startDate",
                    "value": str(start_date),
                    "message": "startDate must be on or before endDate",
                }
            )

        if scope_type not in allowed_scopes:
            errors.append(
                {
                    "index": idx,
                    "field": "scopeLevel",
                    "value": scope_type,
                    "allowed": sorted(allowed_scopes),
                }
            )

        if scope_type in {"AD_TYPE", "BUDGET"} and not scope_value:
            errors.append(
                {
                    "index": idx,
                    "field": "scopeValue",
                    "value": scope_value,
                    "message": "scopeValue is required for AD_TYPE and BUDGET scopes",
                }
            )
        if scope_type == "AD_TYPE":
            scope_value = scope_value.upper()
            if scope_value not in allowed_adtypes:
                errors.append(
                    {
                        "index": idx,
                        "field": "scopeValue",
                        "value": scope_value,
                        "allowed": sorted(allowed_adtypes),
                    }
                )
        elif scope_type == "ACCOUNT":
            if account_code and scope_value and scope_value.upper() != account_code:
                errors.append(
                    {
                        "index": idx,
                        "field": "scopeValue",
                        "value": scope_value,
                        "message": "scopeValue must match accountCode for ACCOUNT scope",
                    }
                )
            scope_value = account_code

        data["accountCode"] = account_code
        data["scopeLevel"] = scope_type
        data["scopeValue"] = scope_value

    if errors:
        raise HTTPException(status_code=400, detail={"error": "Invalid payload", "items": errors})

    if validate_account_codes_rows:
        deduped_codes = _normalize_codes(
            [
                r.get("accountCode")
                for r in rows
                if isinstance(r.get("accountCode"), str)
            ]
        )
        start_dates = [
            row.get("startDate")
            for row in rows
            if isinstance(row.get("startDate"), date)
        ]
        validation_as_of = min(start_dates) if start_dates else None
        validate_account_codes(deduped_codes, as_of=validation_as_of)

    return rows


def _validate_budget_scope_values(rows: list[dict]) -> None:
    budget_rows = [
        (idx, row)
        for idx, row in enumerate(rows)
        if str(row.get("scopeLevel", "")).strip().upper() == "BUDGET"
    ]
    if not budget_rows:
        return

    account_codes = sorted(
        {
            str(row.get("accountCode", "")).strip().upper()
            for _, row in budget_rows
            if str(row.get("accountCode", "")).strip()
        }
    )
    errors: list[dict] = []
    gg_accounts = get_ggad_accounts()
    gg_by_code = {
        str(a.get("accountCode", "")).strip().upper(): a
        for a in gg_accounts
        if str(a.get("accountCode", "")).strip()
    }

    missing_accounts = [code for code in account_codes if code not in gg_by_code]
    if missing_accounts:
        errors.extend(
            {
                "index": idx,
                "field": "accountCode",
                "value": row.get("accountCode"),
                "message": "No Google Ads account found for accountCode",
            }
            for idx, row in budget_rows
            if str(row.get("accountCode", "")).strip().upper() in missing_accounts
        )

    accounts = [gg_by_code[code] for code in account_codes if code in gg_by_code]
    budgets = get_ggad_budgets(accounts) if accounts else []
    budget_ids_by_account: dict[str, set[str]] = defaultdict(set)
    for budget in budgets:
        account_code = str(budget.get("accountCode", "")).strip().upper()
        budget_id = str(budget.get("budgetId", "")).strip()
        if account_code and budget_id:
            budget_ids_by_account[account_code].add(budget_id)

    for idx, row in budget_rows:
        account_code = str(row.get("accountCode", "")).strip().upper()
        scope_value = str(row.get("scopeValue", "")).strip()
        if account_code in missing_accounts:
            continue
        if scope_value not in budget_ids_by_account.get(account_code, set()):
            errors.append(
                {
                    "index": idx,
                    "field": "scopeValue",
                    "value": scope_value,
                    "message": "scopeValue is not a valid budgetId for accountCode",
                }
            )

    if errors:
        raise HTTPException(
            status_code=400, detail={"error": "Invalid payload", "items": errors}
        )


def _validate_update_keys_exist(rows: list[dict]) -> None:
    existing_keys = get_existing_acceleration_keys(rows)
    missing: list[dict] = []
    for idx, row in enumerate(rows):
        key = (
            row.get("accountCode"),
            row.get("scopeLevel"),
            row.get("scopeValue"),
            row.get("startDate"),
            row.get("endDate"),
        )
        if key not in existing_keys:
            missing.append(
                {
                    "index": idx,
                    "message": "Acceleration not found for update",
                    "key": {
                        "accountCode": row.get("accountCode"),
                        "scopeLevel": row.get("scopeLevel"),
                        "scopeValue": row.get("scopeValue"),
                        "startDate": row.get("startDate"),
                        "endDate": row.get("endDate"),
                    },
                }
            )

    if missing:
        raise HTTPException(
            status_code=400,
            detail={"error": "Update keys not found", "items": missing},
        )


@router.post(
    "/accelerations",
    summary="Create accelerations (bulk)",
    description=(
        "Bulk insert accelerations. All fields are required per item. "
        "Values are normalized and validated against tenant config."
    ),
    responses={
        200: {
            "description": "Insert summary",
            "content": {"application/json": {"example": {"inserted": 2}}},
        },
        400: {
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": {
                            "error": "Invalid payload",
                            "items": [
                                {
                                    "index": 0,
                                    "field": "scopeLevel",
                                    "value": "BAD",
                                    "allowed": ["ACCOUNT", "AD_TYPE", "BUDGET"],
                                }
                            ],
                        }
                    }
                }
            },
        },
    },
)
def create_accelerations(request_payload: list[AccelerationPayload]):
    """
    Example request:
        POST /api/spendsphere/v1/accelerations
        [
          {
            "accountCode": "TAAA",
            "scopeLevel": "ACCOUNT",
            "scopeValue": "TAAA",
            "startDate": "2026-01-01",
            "endDate": "2026-01-31",
            "multiplier": 120.0
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
          "data": {"inserted": 1}
        }
    """
    rows = _normalize_and_validate_rows(
        [
            {
                **p.model_dump(exclude_unset=True),
                "_note_provided": "note" in p.model_fields_set,
            }
            for p in request_payload
        ]
    )
    _validate_budget_scope_values(rows)
    inserted = insert_accelerations(rows)
    return {"inserted": inserted}


@router.put(
    "/accelerations",
    summary="Update accelerations (bulk)",
    description=(
        "Bulk update accelerations by unique key "
        "(accountCode, scopeLevel, scopeValue, startDate, endDate)."
    ),
    responses={
        200: {
            "description": "Update summary",
            "content": {"application/json": {"example": {"updated": 2}}},
        }
    },
)
def update_accelerations_route(request_payload: list[AccelerationPayload]):
    """
    Example request:
        PUT /api/spendsphere/v1/accelerations
        [
          {
            "accountCode": "TAAA",
            "scopeLevel": "ACCOUNT",
            "scopeValue": "TAAA",
            "startDate": "2026-01-01",
            "endDate": "2026-01-31",
            "multiplier": 150.0
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
          "data": {"updated": 1}
        }
    """
    rows = _normalize_and_validate_rows(
        [
            {
                **p.model_dump(exclude_unset=True),
                "_note_provided": "note" in p.model_fields_set,
            }
            for p in request_payload
        ]
    )
    _validate_budget_scope_values(rows)
    _validate_update_keys_exist(rows)
    updated = update_accelerations(rows)
    return {"updated": updated}


@router.delete(
    "/accelerations",
    summary="Soft delete accelerations by ids",
    description="Sets active = 0 for the requested acceleration ids.",
)
def delete_accelerations_by_ids(request_payload: AccelerationIdsPayload):
    """
    Example request:
        DELETE /api/spendsphere/v1/accelerations
        {
          "ids": [101, 102]
        }

    Example response:
        {
          "deleted": 2
        }
    """
    payload = AccelerationIdsPayload.model_validate(
        request_payload, from_attributes=True
    )
    ids = [value for value in payload.ids if value is not None]
    if not ids:
        raise HTTPException(
            status_code=400,
            detail={"error": "ids must contain at least one value"},
        )
    deleted = soft_delete_accelerations_by_ids(ids)
    return {"deleted": deleted}


@router.post(
    "/accelerations/by-month",
    summary="Create accelerations by month (bulk)",
    description=(
        "Bulk insert accelerations using month/year/dayFront to compute start/end dates. "
        "accountCodes may be provided as a list; when accountCodes is an empty list, "
        "the route resolves all eligible account codes for the month/year. "
        "startDate is always the first day of the month; endDate counts dayFront "
        "inclusively from startDate."
    ),
)
def create_accelerations_by_month(request_payload: list[AccelerationMonthPayload]):
    """
    Example request:
        POST /api/spendsphere/v1/accelerations/by-month
        [
          {
            "accountCodes": ["TAAA", "LACS"],
            "scopeLevel": "AD_TYPE",
            "scope_value": "SEM",
            "multiplier": 120.0,
            "month": 1,
            "year": 2026,
            "dayFront": 15
          }
        ]

    Example request (resolve all eligible accounts):
        POST /api/spendsphere/v1/accelerations/by-month
        [
          {
            "accountCodes": [],
            "scopeLevel": "AD_TYPE",
            "scope_value": "SEM",
            "multiplier": 120.0,
            "month": 1,
            "year": 2026,
            "dayFront": 15
          }
        ]

    Example response:
        {
          "summary": {"requested": 2, "inserted": 2},
          "accelerations": [
            {
              "accountCode": "TAAA",
              "scopeLevel": "AD_TYPE",
              "scopeValue": "SEM",
              "startDate": "2026-01-01",
              "endDate": "2026-01-15",
              "multiplier": 120.0
            },
            {
              "accountCode": "LACS",
              "scopeLevel": "AD_TYPE",
              "scopeValue": "SEM",
              "startDate": "2026-01-01",
              "endDate": "2026-01-15",
              "multiplier": 120.0
            }
          ]
        }
    """
    if not request_payload:
        raise HTTPException(status_code=400, detail="No accelerations provided")

    rows: list[dict] = []
    errors: list[dict] = []
    for idx, item in enumerate(request_payload):
        data = item.model_dump()
        month = data.get("month")
        year = data.get("year")
        day_front = data.get("dayFront")
        multiplier = data.get("multiplier")
        raw_account_codes = data.get("accountCodes") or []
        use_all_accounts = len(raw_account_codes) == 0
        account_field = "accountCodes"
        account_input = raw_account_codes
        account_codes = [
            code.strip().upper()
            for code in raw_account_codes
            if isinstance(code, str) and code.strip()
        ]
        account_codes = _normalize_codes(account_codes)

        if not account_codes and not use_all_accounts:
            errors.append(
                {
                    "index": idx,
                    "field": "accountCodes",
                    "value": account_input,
                    "message": "accountCodes must contain non-empty strings",
                }
            )
            continue

        if not isinstance(month, int) or month < 1 or month > 12:
            errors.append(
                {"index": idx, "field": "month", "value": month, "message": "month must be 1-12"}
            )
            continue
        if not isinstance(year, int) or year < 2000 or year > 2100:
            errors.append(
                {"index": idx, "field": "year", "value": year, "message": "year must be 2000-2100"}
            )
            continue
        if not isinstance(day_front, int) or day_front < 0:
            errors.append(
                {
                    "index": idx,
                    "field": "dayFront",
                    "value": day_front,
                    "message": "dayFront must be >= 0",
                }
            )
            continue
        if isinstance(multiplier, bool) or not isinstance(multiplier, (int, float)):
            errors.append(
                {
                    "index": idx,
                    "field": "multiplier",
                    "value": multiplier,
                    "message": "multiplier must be a number",
                }
            )
            continue
        if multiplier < 100:
            errors.append(
                {
                    "index": idx,
                    "field": "multiplier",
                    "value": multiplier,
                    "message": "multiplier must be >= 100",
                }
            )
            continue

        try:
            start_date = date(year, month, 1)
        except ValueError:
            errors.append(
                {"index": idx, "field": "month", "value": month, "message": "invalid month/year"}
            )
            continue

        try:
            account_codes = _resolve_account_codes(
                [] if use_all_accounts else account_codes,
                month=month,
                year=year,
                as_of=start_date,
            )
        except HTTPException as exc:
            errors.append(
                {
                    "index": idx,
                    "field": account_field,
                    "value": account_input,
                    "message": "Invalid accountCodes",
                    "detail": exc.detail,
                }
            )
            continue
        if not account_codes:
            errors.append(
                {
                    "index": idx,
                    "field": account_field,
                    "value": account_input,
                    "message": "No accountCodes resolved for acceleration creation",
                }
            )
            continue

        end_offset_days = max(day_front - 1, 0)
        end_date = start_date + timedelta(days=end_offset_days)

        scope_type = data.get("scopeLevel")
        scope_value = data.get("scopeValue")
        note = data.get("note")
        for account_code in account_codes:
            if str(scope_type).strip().upper() == "ACCOUNT":
                scope_value = account_code
            row = {
                "accountCode": account_code,
                "scopeLevel": scope_type,
                "scopeValue": scope_value,
                "startDate": start_date,
                "endDate": end_date,
                "multiplier": multiplier,
                "note": note,
            }
            rows.append(row)

    if errors:
        raise HTTPException(status_code=400, detail={"error": "Invalid payload", "items": errors})

    rows = _normalize_and_validate_rows(rows, validate_account_codes_rows=False)
    _validate_budget_scope_values(rows)
    inserted = insert_accelerations(rows)
    return {
        "summary": {"requested": len(rows), "inserted": inserted},
        "accelerations": rows,
    }


@router.post(
    "/accelerations/by-month/accounts",
    summary="Create accelerations for multiple accounts by month",
    description=(
        "Creates one acceleration per accountCode. If accountCodes is empty, "
        "uses all active account codes that have matching Google Ads accounts."
    ),
)
def create_accelerations_by_month_accounts(
    request_payload: AccelerationMonthAccountsPayload,
):
    """
    Example request:
        POST /api/spendsphere/v1/accelerations/by-month/accounts
        {
          "accountCodes": [],
          "scopeLevel": "AD_TYPE",
          "scope_value": "SEM",
          "multiplier": 120.0,
          "month": 1,
          "year": 2026
        }

    Example request (custom dates):
        POST /api/spendsphere/v1/accelerations/by-month/accounts
        {
          "accountCodes": ["TAAA"],
          "scopeLevel": "ACCOUNT",
          "scope_value": "TAAA",
          "multiplier": 110.0,
          "startDate": "2026-01-05",
          "endDate": "2026-01-20"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
          "data": {"inserted": 5}
        }
    """
    start_date = request_payload.startDate
    end_date = request_payload.endDate

    if (start_date and not end_date) or (end_date and not start_date):
        raise HTTPException(
            status_code=400,
            detail="startDate and endDate must be provided together",
        )

    if start_date and end_date:
        if start_date > end_date:
            raise HTTPException(
                status_code=400,
                detail="startDate must be on or before endDate",
            )
    else:
        month = request_payload.month
        year = request_payload.year
        if not isinstance(month, int) or month < 1 or month > 12:
            raise HTTPException(status_code=400, detail="month must be 1-12")
        if not isinstance(year, int) or year < 2000 or year > 2100:
            raise HTTPException(status_code=400, detail="year must be 2000-2100")
        start_date = date(year, month, 1)
        end_date = date(year, month, calendar.monthrange(year, month)[1])

    account_codes = _resolve_account_codes(
        request_payload.accountCodes,
        month=start_date.month,
        year=start_date.year,
        as_of=start_date,
    )
    if not account_codes:
        raise HTTPException(
            status_code=400,
            detail="No accountCodes resolved for acceleration creation",
        )

    rows: list[dict] = []
    scope_type = request_payload.scopeLevel
    scope_value = request_payload.scopeValue
    note = request_payload.note
    for code in account_codes:
        row = {
            "accountCode": code,
            "scopeLevel": scope_type,
            "scopeValue": scope_value,
            "startDate": start_date,
            "endDate": end_date,
            "multiplier": request_payload.multiplier,
            "note": note,
        }
        if str(scope_type).strip().upper() == "ACCOUNT":
            row["scopeValue"] = code
        rows.append(row)

    rows = _normalize_and_validate_rows(rows, validate_account_codes_rows=False)
    _validate_budget_scope_values(rows)
    inserted = insert_accelerations(rows)
    return {"inserted": inserted}
