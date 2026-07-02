from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request
from pydantic import BaseModel, field_validator

from apps.fundsphere.api.v1.helpers.dbQueries import get_master_budget_control_budget_matrix_rows


# ============================================================
# ROUTER
# ============================================================

router = APIRouter(prefix="/budgetMatrix")


# ============================================================
# CONSTANTS
# ============================================================

_PERIOD_RE = r"^(\d{1,2})\/(\d{4})$"


# ============================================================
# MODELS
# ============================================================


class BudgetMatrixLoadRequest(BaseModel):
    accountCodes: list[str]
    periods: list[str]
    serviceIds: list[str] = []

    @field_validator("accountCodes")
    @classmethod
    def _validate_account_codes(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_code in value:
            code = str(raw_code or "").strip().upper()
            if not code or code in seen:
                continue
            seen.add(code)
            normalized.append(code)
        if not normalized:
            raise ValueError("At least one accountCode is required")
        return normalized

    @field_validator("periods")
    @classmethod
    def _validate_periods(cls, value: list[str]) -> list[str]:
        import re

        normalized: list[str] = []
        seen: set[str] = set()
        for raw_period in value:
            period = str(raw_period or "").strip()
            if not period:
                continue
            match = re.fullmatch(_PERIOD_RE, period)
            if not match:
                raise ValueError("Periods must use m/yyyy format")
            month = int(match.group(1))
            year = int(match.group(2))
            if month < 1 or month > 12:
                raise ValueError("Periods must use valid month numbers")
            normalized_period = f"{month}/{year}"
            if normalized_period in seen:
                continue
            seen.add(normalized_period)
            normalized.append(normalized_period)
        if not normalized:
            raise ValueError("At least one period is required")
        return normalized

    @field_validator("serviceIds")
    @classmethod
    def _validate_service_ids(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_service_id in value:
            service_id = str(raw_service_id or "").strip()
            if not service_id or service_id in seen:
                continue
            seen.add(service_id)
            normalized.append(service_id)
        return normalized


# ============================================================
# HELPERS
# ============================================================


def _parse_period_value(value: str) -> tuple[int, int]:
    import re

    match = re.fullmatch(_PERIOD_RE, value)
    if not match:
        raise ValueError(f"Invalid period: {value}")
    month = int(match.group(1))
    year = int(match.group(2))
    if month < 1 or month > 12:
        raise ValueError(f"Invalid period month: {value}")
    return month, year


def _reject_unknown_query_params(request: Request, *, allowed: set[str]) -> None:
    unknown = sorted({key for key in request.query_params.keys() if key not in allowed})
    if unknown:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Unknown query params are not allowed",
                "unknownQueryParams": unknown,
            },
        )


# ============================================================
# ROUTES
# ============================================================


@router.post("/load")
def load_budget_matrix_route(
    request: Request,
    payload: BudgetMatrixLoadRequest = Body(...),
):
    """
    Load a Fundsphere budget matrix for the selected accounts and periods.

    Example request:
        POST /api/fundsphere/v1/masterBudgetControl/budgetMatrix/load
        {
          "accountCodes": ["ACH", "CAMK"],
          "periods": ["6/2026", "7/2026"],
          "serviceIds": ["7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7"]
        }

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 21,
            "timestamp": "2026-06-30T09:00:00.000Z"
          },
          "data": {
            "accountCodes": ["ACH", "CAMK"],
            "periods": [
              {"month": 6, "year": 2026, "value": "6/2026", "label": "June'26"},
              {"month": 7, "year": 2026, "value": "7/2026", "label": "July'26"}
            ],
            "rows": [
              {
                "budgetId": "3a8f40df-9490-4a7d-a2ff-7d2863e95b1f",
                "accountCode": "ACH",
                "accountName": "Acme Holdings",
                "year": 2026,
                "month": 6,
                "serviceId": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
                "serviceName": "Paid Media",
                "departmentCode": "MKT",
                "departmentName": "Marketing",
                "departmentListingOrder": 10,
                "subService": "Search",
                "grossAmount": 50000,
                "commission": 0,
                "netAdjustment": 0,
                "netAmount": 50000,
                "note": "Q2 plan"
              }
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - accountCodes is required and must contain at least one account code
        - periods is required and must contain at least one period in m/yyyy format
        - serviceIds is optional; when present, only matching service rows are returned
        - Unknown query params are rejected (400)
    """
    _reject_unknown_query_params(request, allowed=set())

    try:
        periods = [_parse_period_value(value) for value in payload.periods]
        rows = get_master_budget_control_budget_matrix_rows(
            account_codes=payload.accountCodes,
            periods=periods,
            service_ids=payload.serviceIds,
        )
        return {
            "accountCodes": payload.accountCodes,
            "periods": [
                {
                    "month": month,
                    "year": year,
                    "value": f"{month}/{year}",
                    "label": _format_period_label(month=month, year=year),
                }
                for month, year in periods
            ],
            "rows": rows,
            "rowCount": len(rows),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _format_period_label(*, month: int, year: int) -> str:
    import calendar

    month_name = calendar.month_name[month]
    year_suffix = str(year)[-2:]
    return f"{month_name}'{year_suffix}"
