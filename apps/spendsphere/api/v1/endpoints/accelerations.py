from datetime import date

from datetime import timedelta
import calendar

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from apps.spendsphere.api.v1.helpers.config import (
    get_acceleration_scope_types,
    get_adtypes,
)
from apps.spendsphere.api.v1.helpers.db_queries import (
    get_accounts,
    get_accelerations,
    insert_accelerations,
    soft_delete_accelerations,
    update_accelerations,
)
from apps.spendsphere.api.v1.helpers.ggAd import get_ggad_accounts
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    require_account_code,
    validate_account_codes,
)

router = APIRouter()


# ============================================================
# ACCELERATIONS
# ============================================================


@router.get(
    "/accelerations/{account_code}",
    summary="List active accelerations for an account",
    description=(
        "Returns active accelerations for the specified account code. "
        "Optionally filter by a date range or a month/year."
    ),
)
def get_accelerations_route(
    account_code: str,
    include_all: bool = False,
    start_date: date | None = None,
    end_date: date | None = None,
    month: int | None = None,
    year: int | None = None,
):
    """
    Example request:
        GET /api/spendsphere/v1/accelerations/TAAA?include_all=false

    Example request (filter by dates):
        GET /api/spendsphere/v1/accelerations/TAAA?start_date=2026-01-01&end_date=2026-01-31

    Example request (filter by month/year):
        GET /api/spendsphere/v1/accelerations/TAAA?month=1&year=2026

    Note:
    When start_date/end_date are provided, results include any accelerations that
    overlap the range (startDate <= end_date AND endDate >= start_date).

    Example response:
        [
          {
            "id": 12,
            "accountCode": "TAAA",
            "scopeType": "ACCOUNT",
            "scopeValue": "TAAA",
            "startDate": "2026-01-01",
            "endDate": "2026-01-31",
            "multiplier": 120.0,
            "dateCreated": "2026-01-01 00:00:00",
            "dateUpdated": "2026-01-10 08:12:00"
          }
        ]
    """
    account_code = require_account_code(account_code)

    if (start_date and not end_date) or (end_date and not start_date):
        raise HTTPException(
            status_code=400,
            detail="start_date and end_date must be provided together",
        )

    if (month is None) != (year is None):
        raise HTTPException(
            status_code=400,
            detail="month and year must be provided together",
        )

    if (start_date or end_date) and (month or year):
        raise HTTPException(
            status_code=400,
            detail="Provide either start_date/end_date or month/year, not both",
        )

    if month is not None:
        if month < 1 or month > 12:
            raise HTTPException(status_code=400, detail="Invalid month")
        if year < 2000 or year > 2100:
            raise HTTPException(status_code=400, detail="Invalid year")
        start_date = date(year, month, 1)
        end_date = date(year, month, calendar.monthrange(year, month)[1])

    data = get_accelerations(
        account_code,
        include_all=include_all,
        start_date=start_date,
        end_date=end_date,
    )
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No accelerations found for account_code '{account_code}'",
        )

    return data


class AccelerationPayload(BaseModel):
    accountCode: str
    scopeType: str
    scopeValue: str
    startDate: date
    endDate: date
    multiplier: float

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "accountCode": "TAAA",
                    "scopeType": "ACCOUNT",
                    "scopeValue": "TAAA",
                    "startDate": "2026-01-01",
                    "endDate": "2026-01-31",
                    "multiplier": 120.0,
                }
            ]
        },
    )


class AccelerationMonthPayload(BaseModel):
    accountCode: str | list[str]
    scopeType: str
    scopeValue: str = Field(alias="scope_value")
    multiplier: float
    month: int
    year: int
    dayFront: int = Field(alias="day_front")

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "accountCode": ["TAAA", "LACS"],
                    "scopeType": "AD_TYPE",
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
    scopeType: str
    scopeValue: str = Field(alias="scope_value")
    multiplier: float
    month: int | None = None
    year: int | None = None
    startDate: date | None = None
    endDate: date | None = None

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "accountCodes": ["TAAA", "LACS"],
                    "scopeType": "AD_TYPE",
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


def _resolve_account_codes(account_codes: list[str]) -> list[str]:
    if not account_codes:
        accounts = get_accounts(None, include_all=False)
        gg_accounts = get_ggad_accounts()
        gg_codes = {str(a.get("accountCode", "")).strip().upper() for a in gg_accounts}
        resolved = [
            str(a.get("code", "")).strip().upper()
            for a in accounts
            if str(a.get("code", "")).strip().upper() in gg_codes
        ]
        return [c for c in resolved if c]

    validate_account_codes(account_codes)
    return _normalize_codes(account_codes)


def _normalize_and_validate_rows(rows: list[dict]) -> list[dict]:
    if not rows:
        raise HTTPException(status_code=400, detail="No accelerations provided")

    allowed_scopes = {s.upper() for s in get_acceleration_scope_types()}
    allowed_adtypes = {k.upper() for k in get_adtypes().keys()}

    errors: list[dict] = []

    for idx, data in enumerate(rows):
        account_code = str(data.get("accountCode", "")).strip().upper()
        scope_type = str(data.get("scopeType", "")).strip().upper()
        scope_value = str(data.get("scopeValue", "")).strip()
        multiplier = data.get("multiplier")
        if isinstance(multiplier, bool) or not isinstance(multiplier, (int, float)):
            errors.append(
                {
                    "index": idx,
                    "field": "multiplier",
                    "value": multiplier,
                    "message": "multiplier must be a number",
                }
            )
        elif multiplier < 0:
            errors.append(
                {
                    "index": idx,
                    "field": "multiplier",
                    "value": multiplier,
                    "message": "multiplier must be >= 0",
                }
            )

        if scope_type not in allowed_scopes:
            errors.append(
                {
                    "index": idx,
                    "field": "scopeType",
                    "value": scope_type,
                    "allowed": sorted(allowed_scopes),
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
        data["scopeType"] = scope_type
        data["scopeValue"] = scope_value

    if errors:
        raise HTTPException(status_code=400, detail={"error": "Invalid payload", "items": errors})

    deduped_codes = _normalize_codes(
        [
            r.get("accountCode")
            for r in rows
            if isinstance(r.get("accountCode"), str)
        ]
    )
    validate_account_codes(deduped_codes)

    return rows


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
                                    "field": "scopeType",
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
            "scopeType": "ACCOUNT",
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
    rows = _normalize_and_validate_rows([p.model_dump() for p in request_payload])
    inserted = insert_accelerations(rows)
    return {"inserted": inserted}


@router.put(
    "/accelerations",
    summary="Update accelerations (bulk)",
    description=(
        "Bulk update accelerations by unique key "
        "(accountCode, scopeType, scopeValue, startDate, endDate)."
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
            "scopeType": "ACCOUNT",
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
    rows = _normalize_and_validate_rows([p.model_dump() for p in request_payload])
    updated = update_accelerations(rows)
    return {"updated": updated}


@router.delete(
    "/accelerations",
    summary="Soft delete accelerations (bulk)",
    description=(
        "Bulk soft-delete accelerations by unique key. "
        "Sets active = 0; does not remove rows."
    ),
    responses={
        200: {
            "description": "Delete summary",
            "content": {"application/json": {"example": {"deleted": 2}}},
        }
    },
)
def delete_accelerations(request_payload: list[AccelerationPayload]):
    """
    Example request:
        DELETE /api/spendsphere/v1/accelerations
        [
          {
            "accountCode": "TAAA",
            "scopeType": "ACCOUNT",
            "scopeValue": "TAAA",
            "startDate": "2026-01-01",
            "endDate": "2026-01-31",
            "multiplier": 120.0
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
          "data": {"deleted": 1}
        }
    """
    rows = _normalize_and_validate_rows([p.model_dump() for p in request_payload])
    deleted = soft_delete_accelerations(rows)
    return {"deleted": deleted}


@router.post(
    "/accelerations/by-month",
    summary="Create accelerations by month (bulk)",
    description=(
        "Bulk insert accelerations using month/year/dayFront to compute start/end dates. "
        "accountCode may be a string or a list of strings. "
        "startDate is always the first day of the month; endDate is startDate + dayFront."
    ),
)
def create_accelerations_by_month(request_payload: list[AccelerationMonthPayload]):
    """
    Example request:
        POST /api/spendsphere/v1/accelerations/by-month
        [
          {
            "accountCode": ["TAAA", "LACS"],
            "scopeType": "AD_TYPE",
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
              "scopeType": "AD_TYPE",
              "scopeValue": "SEM",
              "startDate": "2026-01-01",
              "endDate": "2026-01-16",
              "multiplier": 120.0
            },
            {
              "accountCode": "LACS",
              "scopeType": "AD_TYPE",
              "scopeValue": "SEM",
              "startDate": "2026-01-01",
              "endDate": "2026-01-16",
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
        raw_account_code = data.get("accountCode")
        if isinstance(raw_account_code, list):
            account_codes = [
                code.strip().upper()
                for code in raw_account_code
                if isinstance(code, str) and code.strip()
            ]
        elif isinstance(raw_account_code, str):
            account_codes = [raw_account_code.strip().upper()] if raw_account_code.strip() else []
        else:
            account_codes = []
        account_codes = _normalize_codes(account_codes)

        if not account_codes:
            errors.append(
                {
                    "index": idx,
                    "field": "accountCode",
                    "value": raw_account_code,
                    "message": "accountCode must be a non-empty string or list of strings",
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
        if multiplier < 0:
            errors.append(
                {
                    "index": idx,
                    "field": "multiplier",
                    "value": multiplier,
                    "message": "multiplier must be >= 0",
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

        end_date = start_date + timedelta(days=day_front)

        scope_type = data.get("scopeType")
        scope_value = data.get("scopeValue")
        for account_code in account_codes:
            if str(scope_type).strip().upper() == "ACCOUNT":
                scope_value = account_code
            row = {
                "accountCode": account_code,
                "scopeType": scope_type,
                "scopeValue": scope_value,
                "startDate": start_date,
                "endDate": end_date,
                "multiplier": multiplier,
            }
            rows.append(row)

    if errors:
        raise HTTPException(status_code=400, detail={"error": "Invalid payload", "items": errors})

    rows = _normalize_and_validate_rows(rows)
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
          "scopeType": "AD_TYPE",
          "scope_value": "SEM",
          "multiplier": 120.0,
          "month": 1,
          "year": 2026
        }

    Example request (custom dates):
        POST /api/spendsphere/v1/accelerations/by-month/accounts
        {
          "accountCodes": ["TAAA"],
          "scopeType": "ACCOUNT",
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
    account_codes = _resolve_account_codes(request_payload.accountCodes)
    if not account_codes:
        raise HTTPException(
            status_code=400,
            detail="No accountCodes resolved for acceleration creation",
        )

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

    rows: list[dict] = []
    scope_type = request_payload.scopeType
    scope_value = request_payload.scopeValue
    for code in account_codes:
        row = {
            "accountCode": code,
            "scopeType": scope_type,
            "scopeValue": scope_value,
            "startDate": start_date,
            "endDate": end_date,
            "multiplier": request_payload.multiplier,
        }
        if str(scope_type).strip().upper() == "ACCOUNT":
            row["scopeValue"] = code
        rows.append(row)

    rows = _normalize_and_validate_rows(rows)
    inserted = insert_accelerations(rows)
    return {"inserted": inserted}
