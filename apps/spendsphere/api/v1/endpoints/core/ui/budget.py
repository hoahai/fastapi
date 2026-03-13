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
    "/selections",
    summary="Get budget management selection data",
    description=(
        "Returns period options, active services, and active accounts "
        "for the budget management UI."
    ),
)
def get_budget_management_selections_ui(
    months_before: int = Query(
        2, description="Number of months before current period to include."
    ),
    months_after: int = Query(
        1, description="Number of months after current period to include."
    ),
    refresh_service_cache: bool = Query(
        False,
        description="When true, refreshes active service cache before loading.",
    ),
):
    """
    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament/selections
        Header: X-Tenant-Id: nucar

    Example request (custom period window):
        GET /api/spendsphere/v1/uis/budgetManagament/selections?months_before=3&months_after=2
        Header: X-Tenant-Id: nucar

    Example request (force service cache refresh):
        GET /api/spendsphere/v1/uis/budgetManagament/selections?refresh_service_cache=true
        Header: X-Tenant-Id: nucar

    Example response:
        {
          "periods": {
            "currentPeriod": "3/2026",
            "monthsArray": [
              {"month": 1, "year": 2026, "period": "1/2026"},
              {"month": 2, "year": 2026, "period": "2/2026"},
              {"month": 3, "year": 2026, "period": "3/2026"},
              {"month": 4, "year": 2026, "period": "4/2026"}
            ]
          },
          "services": [
            {
              "id": "c6ac34bc-0fc0-46a6-9723-e83780ebb938",
              "name": "Search Engine Marketing",
              "adTypeCode": "SEM"
            }
          ],
          "accounts": [
            {
              "id": "6563107233",
              "descriptiveName": "NUCAR_NuCar",
              "accountCode": "NUCAR",
              "accountName": "NuCar"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - months_before and months_after must be >= 0
    """
    return budgetManagements.get_budget_management_selections_data(
        months_before=months_before,
        months_after=months_after,
        refresh_service_cache=refresh_service_cache,
    )


@router.get(
    "/load",
    summary="Get DB budget rows for all accounts in a period",
    description=(
        "Returns master-budget DB rows (all active accounts) with account name, "
        "service, amount, note, previous-month underspent, and separate spentData "
        "(previous DB budget - previous Google spend), plus recommended rows for "
        "the same account scope/period, sorted by accountCode "
        "then adType priority (SEM > PM > DIS > VID > DM)."
    ),
)
def get_budget_management_db_budgets_ui(
    account_codes: list[str] | None = Query(
        None,
        alias="accountCodes",
        description="Optional account codes. Empty means all active accounts.",
    ),
    account_code: str | None = Query(
        None,
        alias="accountCode",
        description="Legacy single account code alias. Optional.",
    ),
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
    fresh_data: bool = Query(
        False,
        description=(
            "When true, bypasses budget-management data + Google Ads mapping "
            "caches and fetches fresh data."
        ),
    ),
    fresh_spent_data: bool = Query(
        False,
        description=(
            "When true, bypasses spend caches and fetches fresh Google Ads spend "
            "before building spentData/underspent values."
        ),
    ),
):
    """
    Get budget-management DB rows for all active accounts in a period.

    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament/load?month=3&year=2026
        Header: X-Tenant-Id: nucar

    Example request (filter specific accounts):
        GET /api/spendsphere/v1/uis/budgetManagament/load?accountCodes=NUCAR&accountCodes=ALAM&month=3&year=2026
        Header: X-Tenant-Id: nucar

    Example request (legacy single-account alias):
        GET /api/spendsphere/v1/uis/budgetManagament/load?accountCode=NUCAR&month=3&year=2026
        Header: X-Tenant-Id: nucar

    Example request (default current period):
        GET /api/spendsphere/v1/uis/budgetManagament/load
        Header: X-Tenant-Id: nucar

    Example request (force fresh data):
        GET /api/spendsphere/v1/uis/budgetManagament/load?month=3&year=2026&fresh_data=true
        Header: X-Tenant-Id: nucar

    Example request (force fresh spend only):
        GET /api/spendsphere/v1/uis/budgetManagament/load?month=3&year=2026&fresh_spent_data=true
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
              "adTypeCode": "SEM",
              "serviceId": "c6ac34bc-0fc0-46a6-9723-e83780ebb938",
              "service": "Search Engine Marketing",
              "amount": "1500.00",
              "note": "March launch support",
              "previousMonthUnderspent": "250.00",
              "dataNo": 0
            }
          ],
          "spentData": [
            {
              "accountCode": "NUCAR",
              "adTypeCode": "SEM",
              "spent": "320.25"
            }
          ],
          "recommended": [
            {
              "accountCode": "NUCAR",
              "serviceId": "SEM",
              "serviceName": "Google Search",
              "amount": 800.0
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - Supports accountCodes and legacy accountCode; empty means all active accounts
        - month/year are optional and default to current tenant period
        - month/year must be provided together when specified
        - `fresh_data=true` bypasses cache-first behavior for this route
        - `fresh_spent_data=true` bypasses cached payload and refreshes spend data
    """
    merged_account_codes = list(account_codes or [])
    if account_code is not None:
        merged_account_codes.append(account_code)

    return budgetManagements.get_budget_management_db_rows(
        account_codes=merged_account_codes,
        month=month,
        year=year,
        fresh_data=fresh_data,
        fresh_spent_data=fresh_spent_data,
    )


@router.get(
    "/recommended",
    summary="Get recommended budget values",
    description=(
        "Returns tenant-specific recommended budget rows for the selected period. "
        "When accountCode is provided, returns one account; otherwise returns all active accounts."
    ),
)
def get_recommended_budget_managements_ui(
    account_code: str | None = Query(None, alias="accountCode", min_length=1),
    month: int = Query(..., description="Month (1-12)."),
    year: int = Query(..., description="Year (e.g., 2026)."),
    service_id: str | None = Query(None, alias="serviceId", min_length=1),
):
    """
    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament/recommended?accountCode=ALAM&serviceId=SEM&month=2&year=2026
        Header: X-Tenant-Id: nucar

    Example request (single account, all services):
        GET /api/spendsphere/v1/uis/budgetManagament/recommended?accountCode=ALAM&month=2&year=2026
        Header: X-Tenant-Id: nucar

    Example request (all active accounts):
        GET /api/spendsphere/v1/uis/budgetManagament/recommended?month=2&year=2026
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
    "/duplicate",
    summary="Duplicate budget management rows between periods",
    description=(
        "Duplicates master-budget rows from one period to another for selected "
        "account codes."
    ),
)
def duplicate_budget_managements_ui(
    payload: budgetManagements.BudgetManagementDuplicateRequest,
):
    """
    Example request:
        POST /api/spendsphere/v1/uis/budgetManagament/duplicate
        Header: X-Tenant-Id: nucar
        {
          "accountCodes": ["NUCAR", "ALAM"],
          "fromMonth": 2,
          "fromYear": 2026,
          "toMonth": 3,
          "toYear": 2026,
          "overwrite": false
        }

    Example response:
        {
          "inserted": 4
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - accountCodes is required
        - fromMonth/toMonth must be in 1..12
        - fromYear/toYear must be in 2000..2100
        - Supports `overwrite` and legacy `overried` in request body
    """
    return budgetManagements.duplicate_budget_managements(payload)


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


@router.post(
    "/update",
    summary="Apply budget management create/update/delete changes",
    description=(
        "Applies mixed create/update/delete changes for master-budget rows "
        "in one request."
    ),
)
def apply_budget_management_changes_ui(
    payload: budgetManagements.BudgetManagementChangesRequest,
):
    """
    Example request:
        POST /api/spendsphere/v1/uis/budgetManagament/update
        Header: X-Tenant-Id: nucar
        {
          "month": 3,
          "year": 2026,
          "changes": [
            {
              "op": "create",
              "accountCode": "NUCAR",
              "serviceId": "SEM",
              "amount": 1000
            },
            {
              "op": "update",
              "id": "65c8d225-9f8f-4d13-8558-d6698f239a45",
              "amount": 1200
            },
            {
              "op": "delete",
              "id": "71f1a9f9-6ca7-4cf7-8d7d-299f249f7812"
            }
          ]
        }

    Example response:
        {
          "period": {"month": 3, "year": 2026},
          "updated": 1,
          "inserted": 1,
          "deleted": 1,
          "appliedChanges": 3
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - month/year are optional and default to current tenant period
        - month/year must be provided together when specified
    """
    return budgetManagements.apply_budget_management_changes(payload)


@router.delete(
    "/{budget_id}",
    summary="Soft delete a budget management row",
    description="Soft deletes one master-budget row by setting gross amount to zero.",
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
