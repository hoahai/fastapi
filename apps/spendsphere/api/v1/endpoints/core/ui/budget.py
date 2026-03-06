from fastapi import APIRouter, Depends, Path, Query

from apps.spendsphere.api.v1.endpoints.custom import budgetManagements

router = APIRouter(
    prefix="/uis/budgetManagament",
    dependencies=[Depends(budgetManagements.ensure_budget_managements_access)],
)


@router.get(
    "",
    summary="Get budget management rows and calculated budgets",
    description=(
        "Returns saved master-budget rows and tenant-specific calculated budget values "
        "for selected account codes and period."
    ),
)
def get_budget_managements_ui(
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
):
    """
    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament?accountCodes=NUCAR&month=1&year=2026
        Header: X-Tenant-Id: nucar

    Example response:
        {
          "budgets": [
            {
              "id": "65c8d225-9f8f-4d13-8558-d6698f239a45",
              "accountCode": "NUCAR",
              "serviceId": "SEM",
              "subService": null,
              "netAmount": 1000.0
            }
          ],
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
        - accountCodes is required
        - month/year must be provided together when specified
    """
    return budgetManagements.get_budget_managements(
        account_codes=account_codes,
        month=month,
        year=year,
    )


@router.get(
    "/budgetOverview",
    summary="Get DB budget rows for all accounts in a period",
    description=(
        "Returns master-budget DB rows (all active accounts) with account name, "
        "service, amount, note, and previous-month underspent "
        "(previous DB budget - previous Google spend), sorted by accountCode "
        "then adType priority (SEM > PM > DIS > VID > DM)."
    ),
)
def get_budget_management_db_budgets_ui(
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
):
    """
    Get budget-management DB rows for all active accounts in a period.

    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament/budgetOverview?month=3&year=2026
        Header: X-Tenant-Id: nucar

    Example request (default current period):
        GET /api/spendsphere/v1/uis/budgetManagament/budgetOverview
        Header: X-Tenant-Id: nucar

    Example response:
        {
          "period": {"month": 3, "year": 2026},
          "previousPeriod": {"month": 2, "year": 2026},
          "tableData": [
            {
              "budgetId": "65c8d225-9f8f-4d13-8558-d6698f239a45",
              "accountCode": "NUCAR",
              "accountName": "NuCar",
              "serviceId": "c6ac34bc-0fc0-46a6-9723-e83780ebb938",
              "service": "Search Engine Marketing",
              "amount": "1500.00",
              "note": "March launch support",
              "previousMonthUnderspent": "250.00",
              "dataNo": 0
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - month/year are optional and default to current tenant period
        - month/year must be provided together when specified
    """
    return budgetManagements.get_budget_management_db_rows(
        month=month,
        year=year,
    )


@router.get(
    "/recommended",
    summary="Get recommended budget values for one account",
    description=(
        "Returns tenant-specific recommended budget rows for one account and period. "
        "When serviceId is provided, returns only one recommended item."
    ),
)
def get_recommended_budget_managements_ui(
    account_code: str = Query(..., alias="accountCode", min_length=1),
    month: int = Query(..., description="Month (1-12)."),
    year: int = Query(..., description="Year (e.g., 2026)."),
    service_id: str | None = Query(None, alias="serviceId", min_length=1),
):
    """
    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament/recommended?accountCode=ALAM&serviceId=SEM&month=2&year=2026
        Header: X-Tenant-Id: nucar

    Example request (all services):
        GET /api/spendsphere/v1/uis/budgetManagament/recommended?accountCode=ALAM&month=2&year=2026
        Header: X-Tenant-Id: nucar

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
        - accountCode must be a single non-empty code
        - month/year are required
    """
    return budgetManagements.get_recommended_budget_managements(
        account_code=account_code,
        month=month,
        year=year,
        service_id=service_id,
    )


@router.post(
    "/masterBudgetDataSync",
    summary="Sync master budget pivot data to Google Sheets",
    description=(
        "Builds tenant-specific master-budget pivot rows and rewrites the configured "
        "sheet tab for the selected period."
    ),
)
def sync_budget_management_master_budget_sheet_ui(
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
    refresh_google_ads_caches: bool = Query(
        False,
        description=(
            "When true, refreshes cached Google Ads clients/campaigns/budgets "
            "before rebuilding pivot rows."
        ),
    ),
):
    """
    Example request:
        POST /api/spendsphere/v1/uis/budgetManagament/masterBudgetDataSync?month=3&year=2026
        Header: X-Tenant-Id: nucar

    Example request (with Google Ads cache refresh):
        POST /api/spendsphere/v1/uis/budgetManagament/masterBudgetDataSync?month=3&year=2026&refresh_google_ads_caches=true
        Header: X-Tenant-Id: nucar

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
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - month/year are optional and default to current tenant period
        - month/year must be provided together when specified
    """
    return budgetManagements.sync_budget_management_master_budget_sheet(
        month=month,
        year=year,
        refresh_google_ads_caches=refresh_google_ads_caches,
    )


@router.post(
    "",
    summary="Create budget management rows",
    description="Creates master-budget rows for a selected period.",
)
def create_budget_managements_ui(
    payload: budgetManagements.BudgetManagementUpsertRequest,
):
    """
    Example request:
        POST /api/spendsphere/v1/uis/budgetManagament
        Header: X-Tenant-Id: nucar
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

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - Each row must not include id for create
        - month/year are optional and default to current tenant period
    """
    return budgetManagements.create_budget_managements(payload)


@router.put(
    "",
    summary="Update budget management rows",
    description="Updates existing master-budget rows for a selected period.",
)
def update_budget_managements_ui(
    payload: budgetManagements.BudgetManagementUpsertRequest,
):
    """
    Example request:
        PUT /api/spendsphere/v1/uis/budgetManagament
        Header: X-Tenant-Id: nucar
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

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - Each row must include id for update
        - month/year are optional and default to current tenant period
    """
    return budgetManagements.update_budget_managements(payload)


@router.delete(
    "/{budget_id}",
    summary="Soft delete a budget management row",
    description="Soft deletes one master-budget row by setting net amount to zero.",
)
def soft_delete_budget_management_ui(
    budget_id: str = Path(..., min_length=1),
    account_code: str = Query(..., alias="accountCode"),
    month: int | None = Query(None),
    year: int | None = Query(None),
):
    """
    Example request:
        DELETE /api/spendsphere/v1/uis/budgetManagament/65c8d225-9f8f-4d13-8558-d6698f239a45?accountCode=NUCAR&month=1&year=2026
        Header: X-Tenant-Id: nucar

    Example response:
        {
          "budgetId": "65c8d225-9f8f-4d13-8558-d6698f239a45",
          "accountCode": "NUCAR",
          "month": 1,
          "year": 2026,
          "softDeleted": true
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - accountCode is required
        - month/year must be provided together when specified
    """
    return budgetManagements.soft_delete_budget_management(
        budget_id=budget_id,
        account_code=account_code,
        month=month,
        year=year,
    )
