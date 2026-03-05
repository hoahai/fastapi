from __future__ import annotations

from typing import Callable

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from apps.spendsphere.api.v1.deps import require_feature
from apps.spendsphere.api.v1.endpoints.custom.spreadsheetParser_nucar import (
    calculate_nucar_spreadsheet_budgets,
    get_nucar_recommended_budgets,
    sync_nucar_master_budget_sheet,
)
from apps.spendsphere.api.v1.helpers.db_queries import (
    get_masterbudgets,
    soft_delete_masterbudget,
    upsert_masterbudgets,
)
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    normalize_account_codes,
    validate_account_codes,
)
from shared.tenant import get_tenant_id
from shared.utils import get_current_period

_budget_managements_feature_dependency = require_feature("budget_managements")


class BudgetManagementUpsertItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str | None = None
    accountCode: str = Field(min_length=1)
    serviceId: str = Field(min_length=1)
    subService: str | None = None
    netAmount: float


class BudgetManagementUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    month: int | None = None
    year: int | None = None
    rows: list[BudgetManagementUpsertItem] = Field(min_length=1)


def _resolve_period(month: int | None, year: int | None) -> tuple[int, int]:
    if (month is None) != (year is None):
        raise HTTPException(
            status_code=400,
            detail="month and year must be provided together",
        )
    if month is None and year is None:
        period = get_current_period()
        month = period["month"]
        year = period["year"]
    if month is None or year is None:
        raise HTTPException(status_code=400, detail="Invalid period")
    if not 1 <= month <= 12:
        raise HTTPException(status_code=400, detail="month must be between 1 and 12")
    if not 2000 <= year <= 2100:
        raise HTTPException(status_code=400, detail="year must be between 2000 and 2100")
    return month, year


def _resolve_spreadsheet_parser(
    tenant_id: str | None,
) -> Callable[[list[str], int, int], list[dict[str, object]]] | None:
    parsers = {
        "nucar": calculate_nucar_spreadsheet_budgets,
    }
    key = str(tenant_id or "").strip().lower()
    return parsers.get(key)


def _resolve_recommended_budget_parser(
    tenant_id: str | None,
) -> Callable[[str, str | None, int, int], list[dict[str, object]]] | None:
    parsers = {
        "nucar": get_nucar_recommended_budgets,
    }
    key = str(tenant_id or "").strip().lower()
    return parsers.get(key)


def _resolve_master_budget_sheet_syncer(
    tenant_id: str | None,
) -> Callable[..., dict[str, object]] | None:
    parsers = {
        "nucar": sync_nucar_master_budget_sheet,
    }
    key = str(tenant_id or "").strip().lower()
    return parsers.get(key)


def ensure_budget_managements_access() -> None:
    _budget_managements_feature_dependency()


def get_budget_managements(
    account_codes: list[str] | None = None,
    month: int | None = None,
    year: int | None = None,
):
    """
    Get budgets and optional tenant-calculated spreadsheet budgets.

    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament?accountCodes=NUCAR&month=1&year=2026

    Example response:
        {
          "budgets": [],
          "calculatedBudgets": [
            {
              "accountCode": "NUCAR",
              "calculatedBudget": 2500.0,
              "source": "spreadsheet",
              "sourceRows": 3
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
    """
    requested_codes = normalize_account_codes(account_codes)
    if not requested_codes:
        raise HTTPException(status_code=400, detail="accountCodes is required")

    month, year = _resolve_period(month, year)
    validate_account_codes(requested_codes, month=month, year=year)

    budgets = get_masterbudgets(requested_codes, month, year)
    payload: dict[str, object] = {"budgets": budgets}

    parser = _resolve_spreadsheet_parser(get_tenant_id())
    if parser is not None:
        payload["calculatedBudgets"] = parser(requested_codes, month, year)

    return payload


def get_recommended_budget_managements(
    account_code: str,
    month: int,
    year: int,
    service_id: str | None = None,
):
    """
    Get recommended budgets from tenant-specific spreadsheet parser.

    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament/recommended?accountCode=ALAM&serviceId=SEM&month=2&year=2026

    Example request (all SERVICE_BUDGETS):
        GET /api/spendsphere/v1/uis/budgetManagament/recommended?accountCode=ALAM&month=2&year=2026

    Example response:
        {
          "accountCode": "ALAM",
          "serviceId": "SEM",
          "serviceName": "Google Search",
          "amount": 800.0
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
    """
    requested_codes = normalize_account_codes([account_code])
    if len(requested_codes) != 1:
        raise HTTPException(status_code=400, detail="accountCode must contain exactly one code")
    normalized_account_code = requested_codes[0]

    month, year = _resolve_period(month, year)
    validate_account_codes([normalized_account_code], month=month, year=year)

    parser = _resolve_recommended_budget_parser(get_tenant_id())
    if parser is None:
        raise HTTPException(
            status_code=404,
            detail="Recommended budget parser not configured for this tenant",
        )
    try:
        results = parser(normalized_account_code, service_id, month, year)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if service_id is not None:
        if results:
            return results[0]
        return {
            "accountCode": normalized_account_code,
            "serviceId": service_id,
            "serviceName": service_id,
            "amount": None,
        }
    return results


def sync_budget_management_master_budget_sheet(
    month: int | None = None,
    year: int | None = None,
    refresh_google_ads_caches: bool = False,
):
    """
    Build NuCar master-budget pivot rows and refresh the target Google Sheet tab.

    Example request:
        POST /api/spendsphere/v1/uis/budgetManagament/masterBudgetDataSync?month=3&year=2026

    Example request (default current month/year):
        POST /api/spendsphere/v1/uis/budgetManagament/masterBudgetDataSync

    Example response:
        {
          "period": {"month": 3, "year": 2026},
          "spreadsheetId": "1heDhjHoLYjsoM9fOazW3KQisaCYOG6zPZs-_X3PUCs8",
          "sheetName": "2.3 Master Budget Data",
          "startRow": 9,
          "rowCount": 2,
          "rows": [
            {
              "budgetId": "14644368953",
              "amount": 1794.0,
              "scheduleStatus": "-"
            },
            {
              "budgetId": "14650372785",
              "amount": 1121.25,
              "scheduleStatus": "-"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - month/year are optional and default to current tenant period
        - month/year must be provided together when specified
        - Route is available only for tenants with a configured sync handler
    """
    month, year = _resolve_period(month, year)

    sync_handler = _resolve_master_budget_sheet_syncer(get_tenant_id())
    if sync_handler is None:
        raise HTTPException(
            status_code=404,
            detail="Master budget sheet sync is not configured for this tenant",
        )

    try:
        result = sync_handler(
            month,
            year,
            refresh_google_ads_caches=refresh_google_ads_caches,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"period": {"month": month, "year": year}, **result}


def create_budget_managements(payload: BudgetManagementUpsertRequest):
    """
    Create budget rows for a period.

    Example request:
        POST /api/spendsphere/v1/uis/budgetManagament
        {
          "month": 1,
          "year": 2026,
          "rows": [
            {
              "accountCode": "NUCAR",
              "serviceId": "SEM",
              "subService": null,
              "netAmount": 1000
            }
          ]
        }

    Example response:
        {
          "period": {"month": 1, "year": 2026},
          "updated": 0,
          "inserted": 1
        }
    """
    month, year = _resolve_period(payload.month, payload.year)

    rows: list[dict[str, object]] = []
    account_codes: list[str] = []
    for item in payload.rows:
        if item.id:
            raise HTTPException(
                status_code=400,
                detail="id must be empty for create",
            )
        account_codes.append(item.accountCode)
        rows.append(item.model_dump())

    validate_account_codes(account_codes, month=month, year=year)
    result = upsert_masterbudgets(rows, month=month, year=year)
    return {"period": {"month": month, "year": year}, **result}


def update_budget_managements(payload: BudgetManagementUpsertRequest):
    """
    Update budget rows for a period.

    Example request:
        PUT /api/spendsphere/v1/uis/budgetManagament
        {
          "month": 1,
          "year": 2026,
          "rows": [
            {
              "id": "65c8d225-9f8f-4d13-8558-d6698f239a45",
              "accountCode": "NUCAR",
              "serviceId": "SEM",
              "subService": null,
              "netAmount": 1200
            }
          ]
        }

    Example response:
        {
          "period": {"month": 1, "year": 2026},
          "updated": 1,
          "inserted": 0
        }
    """
    month, year = _resolve_period(payload.month, payload.year)

    rows: list[dict[str, object]] = []
    account_codes: list[str] = []
    for item in payload.rows:
        if not item.id:
            raise HTTPException(
                status_code=400,
                detail="id is required for update",
            )
        account_codes.append(item.accountCode)
        rows.append(item.model_dump())

    validate_account_codes(account_codes, month=month, year=year)
    result = upsert_masterbudgets(rows, month=month, year=year)
    return {"period": {"month": month, "year": year}, **result}


def soft_delete_budget_management(
    budget_id: str,
    account_code: str,
    month: int | None = None,
    year: int | None = None,
):
    """
    Soft delete a budget by setting `netAmount` to 0.

    Example request:
        DELETE /api/spendsphere/v1/uis/budgetManagament/65c8d225-9f8f-4d13-8558-d6698f239a45?accountCode=NUCAR&month=1&year=2026

    Example response:
        {
          "budgetId": "65c8d225-9f8f-4d13-8558-d6698f239a45",
          "accountCode": "NUCAR",
          "month": 1,
          "year": 2026,
          "softDeleted": true
        }
    """
    period_month, period_year = _resolve_period(month, year)
    validate_account_codes([account_code], month=period_month, year=period_year)

    affected = soft_delete_masterbudget(
        budget_id=budget_id,
        account_code=account_code,
        month=period_month,
        year=period_year,
    )
    if affected <= 0:
        raise HTTPException(status_code=404, detail="Budget not found")

    return {
        "budgetId": budget_id,
        "accountCode": account_code,
        "month": period_month,
        "year": period_year,
        "softDeleted": True,
    }
