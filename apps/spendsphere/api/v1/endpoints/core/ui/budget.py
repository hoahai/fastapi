from fastapi import APIRouter, Depends, Path, Query

from apps.spendsphere.api.v1.endpoints.custom import budgetManagements

router = APIRouter(
    prefix="/uis/budgetManagament",
    dependencies=[Depends(budgetManagements.ensure_budget_managements_access)],
)


@router.get("")
def get_budget_managements_ui(
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
):
    return budgetManagements.get_budget_managements(
        account_codes=account_codes,
        month=month,
        year=year,
    )


@router.get("/recommended")
def get_recommended_budget_managements_ui(
    account_code: str = Query(..., alias="accountCode", min_length=1),
    month: int = Query(..., description="Month (1-12)."),
    year: int = Query(..., description="Year (e.g., 2026)."),
    service_id: str | None = Query(None, alias="serviceId", min_length=1),
):
    return budgetManagements.get_recommended_budget_managements(
        account_code=account_code,
        month=month,
        year=year,
        service_id=service_id,
    )


@router.post("/masterBudgetDataSync")
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
    return budgetManagements.sync_budget_management_master_budget_sheet(
        month=month,
        year=year,
        refresh_google_ads_caches=refresh_google_ads_caches,
    )


@router.post("")
def create_budget_managements_ui(
    payload: budgetManagements.BudgetManagementUpsertRequest,
):
    return budgetManagements.create_budget_managements(payload)


@router.put("")
def update_budget_managements_ui(
    payload: budgetManagements.BudgetManagementUpsertRequest,
):
    return budgetManagements.update_budget_managements(payload)


@router.delete("/{budget_id}")
def soft_delete_budget_management_ui(
    budget_id: str = Path(..., min_length=1),
    account_code: str = Query(..., alias="accountCode"),
    month: int | None = Query(None),
    year: int | None = Query(None),
):
    return budgetManagements.soft_delete_budget_management(
        budget_id=budget_id,
        account_code=account_code,
        month=month,
        year=year,
    )
