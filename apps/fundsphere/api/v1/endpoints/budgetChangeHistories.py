from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from apps.fundsphere.api.v1.endpoints._direct_common import parse_optional_int_query
from apps.fundsphere.api.v1.helpers.directDbQueries import (
    get_budget_change_history,
    list_budget_change_histories,
)

router = APIRouter(prefix="/budgetChangeHistories")


@router.get("")
def list_budget_change_histories_route(
    id: str | None = Query(None, alias="id"),
    budget_id: str | None = Query(None, alias="budgetId"),
    action_type: str | None = Query(None, alias="actionType"),
    changed_by: str | None = Query(None, alias="changedBy"),
    account_code: str | None = Query(None, alias="accountCode"),
    service_id: str | None = Query(None, alias="serviceId"),
    month: str | None = Query(None, alias="month"),
    year: str | None = Query(None, alias="year"),
):
    """
    Return FundSphere budget audit history rows.

    Example request:
        GET /api/fundsphere/v1/budgetChangeHistories

    Example request (single history row):
        GET /api/fundsphere/v1/budgetChangeHistories?id=1

    Example request (budget filter):
        GET /api/fundsphere/v1/budgetChangeHistories?budgetId=3a8f40df-9490-4a7d-a2ff-7d2863e95b1f&month=6&year=2026

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": 1,
              "budgetId": "3a8f40df-9490-4a7d-a2ff-7d2863e95b1f",
              "actionType": "UPDATE",
              "changedFields": ["grossAmount"],
              "oldData": {"grossAmount": 50000},
              "newData": {"grossAmount": 60000},
              "changedBy": "fundsphere.direct",
              "note": "Client scope expanded",
              "accountCode": "ACME01",
              "serviceId": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
              "month": 6,
              "year": 2026
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - id, budgetId, actionType, changedBy, accountCode, serviceId, month, and year are optional filters
        - month/year are accepted independently
        - BudgetChangeHistories is read-only
        - Unknown query params are rejected (400)
    """
    try:
        month_value = parse_optional_int_query(month, field="month")
        year_value = parse_optional_int_query(year, field="year")
        if id:
            history = get_budget_change_history(history_id=id)
            if history is None:
                raise HTTPException(status_code=404, detail="Budget change history not found")
            return history
        return list_budget_change_histories(
            id=id,
            budget_id=budget_id,
            action_type=action_type,
            changed_by=changed_by,
            account_code=account_code,
            service_id=service_id,
            month=month_value,
            year=year_value,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
