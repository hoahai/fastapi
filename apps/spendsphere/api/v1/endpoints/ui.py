import calendar
import math
from datetime import date, datetime
from decimal import Decimal

import pytz
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from apps.spendsphere.api.v1.endpoints.periods import (
    build_periods_data,
    validate_month_offsets,
)
from apps.spendsphere.api.v1.helpers.config import get_adtypes, get_service_mapping
from apps.spendsphere.api.v1.helpers.dataTransform import (
    generate_update_payloads,
    transform_google_ads_data,
)
from apps.spendsphere.api.v1.helpers.db_queries import (
    get_accelerations,
    get_allocations,
    get_masterbudgets,
    get_rollbreakdowns,
    upsert_allocations,
    upsert_rollbreakdowns,
)
from apps.spendsphere.api.v1.helpers.ggAd import (
    get_ggad_accounts,
    get_ggad_budgets,
    get_ggad_campaigns,
    get_ggad_spents,
    update_budgets,
    update_campaign_statuses,
)
from apps.spendsphere.api.v1.helpers.ggSheet import get_active_period, get_rollovers
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import require_account_code
from shared.tenant import get_timezone
from shared.utils import get_current_period, run_parallel

router = APIRouter()


def _run_budget_update(customer_id: str, updates: list[dict]) -> dict:
    return update_budgets(customer_id=customer_id, updates=updates)


def _run_campaign_update(customer_id: str, updates: list[dict]) -> dict:
    return update_campaign_statuses(customer_id=customer_id, updates=updates)


def _get_google_ads_clients(refresh_cache: bool) -> list[dict]:
    return get_ggad_accounts(refresh_cache=refresh_cache)


def _parse_optional_int(raw: str | None, name: str) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    value = str(raw).strip()
    if value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": f"Invalid {name}", "value": raw},
        ) from exc


def _resolve_period_date(month: int | None, year: int | None) -> date:
    tz = pytz.timezone(get_timezone())
    today = datetime.now(tz).date()
    if month is None or year is None:
        return today
    if month == today.month and year == today.year:
        return today
    return date(year, month, 1)


def _coerce_date(value: object) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        cleaned = value.strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(cleaned, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(cleaned).date()
        except ValueError:
            return None
    return None


def _resolve_period(month: int | None, year: int | None) -> tuple[int, int]:
    if month is not None and year is not None:
        return month, year
    period = get_current_period()
    return period["month"], period["year"]


def _normalize_optional_str(value: object | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _build_monthly_active_period(
    row: dict | None,
    *,
    month: int,
    year: int,
) -> dict:
    month_start = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    month_end = date(year, month, last_day)

    start_date = _coerce_date(row.get("startDate") if row else None)
    end_date = _coerce_date(row.get("endDate") if row else None)

    start_ok = True if start_date is None else start_date <= month_end
    end_ok = True if end_date is None else end_date >= month_start
    is_active = start_ok and end_ok

    response: dict[str, object] = {"isActive": is_active}
    message_parts: list[str] = []

    if start_date and month_start <= start_date <= month_end:
        response["startDate"] = start_date.isoformat()
        message_parts.append(
            f"Account will start on {start_date.strftime('%m/%d/%Y')}"
        )
    if end_date and month_start <= end_date <= month_end:
        response["endDate"] = end_date.isoformat()
        message_parts.append(
            f"Account last day on {end_date.strftime('%m/%d/%Y')} EOD"
        )
        message_parts.append(
            f"Daily budgets and pacing as of {end_date.strftime('%m/%d')}"
        )

    if message_parts:
        response["message"] = message_parts
    return response


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_accelerations_for_period(
    account_codes: list[str],
    period_date: date,
) -> list[dict]:
    return get_accelerations(account_codes, today=period_date)


def _build_master_budgets(master_budgets: list[dict]) -> dict:
    service_mapping = get_service_mapping()
    result: dict[str, object] = {}
    grand_total = Decimal("0")

    for mb in master_budgets:
        mapping = service_mapping.get(mb.get("serviceId"))
        if not mapping:
            continue
        ad_type = mapping.get("adTypeCode")
        if not ad_type:
            continue

        net_amount = Decimal(str(mb.get("netAmount", 0)))
        entry = result.setdefault(
            ad_type, {"budgets": [], "total": Decimal("0")}
        )
        entry["budgets"].append(
            {
                "serviceName": mb.get("serviceName") or mapping.get("serviceName"),
                "subService": mb.get("subService") or "",
                "netAmount": _to_float(net_amount),
                "adTypeCode": ad_type,
            }
        )
        entry["total"] += net_amount
        grand_total += net_amount

    for key, value in result.items():
        if isinstance(value, dict) and isinstance(value.get("total"), Decimal):
            value["total"] = _to_float(value.get("total"))

    result["grandTotalBudget"] = _to_float(grand_total)
    return result


def _build_roll_breakdown(rollbreakdowns: list[dict]) -> dict:
    result: dict[str, object] = {}
    grand_total = Decimal("0")

    for row in rollbreakdowns:
        ad_type = row.get("adTypeCode")
        if not ad_type:
            continue
        amount = Decimal(str(row.get("amount", 0)))
        if ad_type not in result:
            result[ad_type] = {
                "id": row.get("id"),
                "amount": _to_float(amount),
            }
        else:
            existing = result[ad_type]
            if isinstance(existing, dict):
                existing_amount = Decimal(str(existing.get("amount", 0)))
                existing["amount"] = _to_float(existing_amount + amount)
        grand_total += amount

    result["grandTotalRollBreakdown"] = _to_float(grand_total)
    return result


def _format_campaign_names(campaigns: list[dict]) -> str:
    names = sorted(
        {
            c.get("campaignName")
            for c in campaigns
            if c.get("campaignName")
        }
    )
    if not names:
        return ""
    return "<br/>".join(
        f'<span style="font-size: 15px;">{name}</span>' for name in names
    )


def _format_campaign_statuses(campaigns: list[dict]) -> str:
    statuses = sorted(
        {
            c.get("campaignStatus")
            or c.get("status")
            for c in campaigns
            if c.get("campaignStatus") or c.get("status")
        }
    )
    if not statuses:
        return ""
    if len(statuses) == 1:
        return statuses[0]
    return ", ".join(statuses)


def _build_table_data(
    rows: list[dict],
    budgets: list[dict],
    allocations: list[dict],
) -> list[dict]:
    budget_lookup = {b.get("budgetId"): b for b in budgets}
    allocation_lookup: dict[tuple[str | None, str | None], dict] = {}
    for a in allocations:
        allocation_lookup[(a.get("accountCode"), a.get("ggBudgetId"))] = {
            "id": a.get("id"),
            "allocation": _to_float(a.get("allocation")),
        }

    table_data: list[dict] = []
    for row in rows:
        budget_id = row.get("budgetId")
        budget_meta = budget_lookup.get(budget_id, {})
        campaigns = row.get("campaigns", [])
        allocation = allocation_lookup.get(
            (row.get("accountCode"), budget_id)
        )

        normalized_campaigns = [
            {
                "campaignId": c.get("campaignId"),
                "campaignName": c.get("campaignName"),
                "campaignStatus": c.get("status"),
            }
            for c in campaigns
        ]
        campaign_names = _format_campaign_names(normalized_campaigns)

        current_budget = budget_meta.get("amount")
        if current_budget is None:
            current_budget = row.get("budgetAmount")

        table_data.append(
            {
                "accountId": row.get("ggAccountId"),
                "name": row.get("budgetName"),
                "budgetId": budget_id,
                "explicitlyShared": budget_meta.get("explicitlyShared"),
                "status": budget_meta.get("status") or row.get("budgetStatus"),
                "currentBudget": _to_float(current_budget),
                "spent": _to_float(row.get("totalCost")),
                "adTypeCode": row.get("adTypeCode"),
                "allocation": allocation,
                "acceleration": {
                    "id": row.get("accelerationId"),
                    "multiplier": _to_float(
                        row.get("accelerationMultiplier")
                    ),
                },
                "campaigns": normalized_campaigns,
                "_campaignNames": campaign_names,
            }
        )

    def _sort_key(item: dict) -> tuple:
        allocation_value = item.get("allocation", {}) or {}
        allocation_amount = allocation_value.get("allocation")
        if allocation_amount is None:
            allocation_amount = -math.inf

        spent_value = item.get("spent")
        if spent_value is None:
            spent_value = -math.inf

        return (
            item.get("adTypeCode") or "",
            -allocation_amount,
            -spent_value,
            item.get("_campaignNames") or "",
        )

    table_data.sort(key=_sort_key)

    for idx, item in enumerate(table_data):
        item["dataNo"] = idx
        item.pop("_campaignNames", None)

    return table_data


class UiAllocationUpdate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str | None = None
    budgetId: str | None = Field(default=None, alias="ggBudgetId")
    currentAllocation: float | None = None
    newAllocation: float | None = None


class UiRollBreakUpdate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str | None = None
    accountCode: str | None = None
    adTypeCode: str | None = None
    currentAmount: float | None = None
    newAmount: float | None = None


class UiAllocationRollBreakUpdateRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    accountCode: str
    month: int
    year: int
    returnNewData: bool = Field(default=False, alias="includeData")
    updatedRollBreakdowns: list[UiRollBreakUpdate] = Field(
        default_factory=list, alias="updatedRollBreaks"
    )
    updatedAllocations: list[UiAllocationUpdate] = Field(default_factory=list)


# ============================================================
# UI
# ============================================================


@router.get(
    "/ui/selections",
    summary="Get Google Ads clients and period metadata",
    description="Returns Google Ads clients and period data in a single response.",
)
def get_ui_selections_route(
    months_before: int = Query(
        2, description="Number of months before the current month to include."
    ),
    months_after: int = Query(
        1, description="Number of months after the current month to include."
    ),
    refresh_cache: bool = Query(
        False, description="When true, refreshes the Google Ads client cache."
    ),
):
    """
    Example request:
    GET /api/spendsphere/v1/ui/selections
    Header: X-Tenant-Id: acme

    Example request (force refresh):
    GET /api/spendsphere/v1/ui/selections?refresh_cache=true
    Header: X-Tenant-Id: acme

    Example response:
    {
      "googleAdsClients": [
        {
          "id": "6563107233",
          "descriptiveName": "AUC_Autocity Credit",
          "accountCode": "AUC",
          "accountName": "Autocity Credit"
        }
      ],
      "periods": {
        "currentPeriod": "1/2026",
        "monthsArray": [
          {
            "month": 11,
            "year": 2025,
            "period": "11/2025"
          },
          {
            "month": 12,
            "year": 2025,
            "period": "12/2025"
          },
          {
            "month": 1,
            "year": 2026,
            "period": "1/2026"
          },
          {
            "month": 2,
            "year": 2026,
            "period": "2/2026"
          }
        ]
      }
    }
    """
    validate_month_offsets(months_before, months_after)

    tasks = [
        (_get_google_ads_clients, (refresh_cache,)),
        (build_periods_data, (months_before, months_after)),
    ]
    clients, periods = run_parallel(
        tasks=tasks,
        api_name="spendsphere_v1_ui_selections",
    )
    clients_sorted = sorted(
        clients, key=lambda client: (client.get("accountName") or "").casefold()
    )
    months_array = periods.get("monthsArray")
    if isinstance(months_array, list):
        months_array_sorted = sorted(
            months_array,
            key=lambda item: (item.get("year") or 0, item.get("month") or 0),
        )
        periods = {**periods, "monthsArray": months_array_sorted}
    return {
        "googleAdsClients": clients_sorted,
        "periods": periods,
    }


@router.get(
    "/ui/load",
    summary="Load SpendSphere UI data for a Google Ads account",
    description=(
        "Returns master budgets, rollovers, roll breakdown, and "
        "table data for the specified Google Ads account."
    ),
)
def load_ui_route(
    googleId: str = Query(..., description="Google Ads account ID."),
    month: str | None = Query(
        None, description="Optional month (1-12)."
    ),
    year: str | None = Query(
        None, description="Optional year (e.g., 2026)."
    ),
):
    """
    Example request:
    GET /api/spendsphere/v1/ui/load?googleId=6563107233&month=1&year=2026
    Header: X-Tenant-Id: acme

    Example request (current period):
    GET /api/spendsphere/v1/ui/load?googleId=6563107233
    Header: X-Tenant-Id: acme

    Example response:
    {
      "masterBudgets": {
        "grandTotalBudget": 9729.44,
        "PM": {
          "budgets": [
            {
              "serviceName": "Vehicle Listing Ads",
              "subService": "",
              "netAmount": 680,
              "adTypeCode": "PM"
            }
          ],
          "total": 680
        }
      },
      "rollOvers": 1000.0,
      "activePeriod": {
        "isActive": true,
        "endDate": "2026-01-31",
        "message": [
          "Account last day on 01/31/2026 EOD",
          "Daily budgets and pacing as of 01/31"
        ]
      },
        "rollBreakdown": {
        "grandTotalRollBreakdown": 1000,
        "SEM": {
          "id": "0ad1dc44-35f2-4fb6-9f1f-19527ab193e3",
          "amount": 1000
        }
      },
      "tableData": {
        "grandTotalSpent": 586.09,
        "data": [
          {
            "accountId": "6563107233",
            "name": "AUC_DIS_Remarketing",
            "budgetId": "15225876848",
            "explicitlyShared": false,
            "status": "ENABLED",
            "currentBudget": 65.4,
            "spent": 586.09,
            "adTypeCode": "DIS",
            "allocation": {
              "id": "b98d38c5-448f-4746-bed3-8dfdadd2959c",
              "allocation": 97.364
            },
            "acceleration": {
              "id": "3f5d9c0c-83a9-4a2d-8c7b-3cc5b1c1a021",
              "multiplier": 120
            },
            "campaigns": [
              {
                "campaignId": "21427314948",
                "campaignName": "AUC_DIS_Remarketing",
                "campaignStatus": "ENABLED"
              }
            ],
            "dataNo": 0
          }
        ]
      }
    }
    """
    google_id = googleId.strip() if isinstance(googleId, str) else ""
    if not google_id:
        raise HTTPException(
            status_code=400,
            detail={"error": "googleId is required"},
        )

    month_value = _parse_optional_int(month, "month")
    year_value = _parse_optional_int(year, "year")

    if (month_value is None) != (year_value is None):
        raise HTTPException(
            status_code=400,
            detail={"error": "month and year must be provided together"},
        )
    if month_value is not None and not 1 <= month_value <= 12:
        raise HTTPException(
            status_code=400,
            detail={"error": "month must be between 1 and 12"},
        )
    if year_value is not None and not 2000 <= year_value <= 2100:
        raise HTTPException(
            status_code=400,
            detail={"error": "year must be between 2000 and 2100"},
        )

    period_date = _resolve_period_date(month_value, year_value)

    accounts = get_ggad_accounts()
    account = next(
        (acc for acc in accounts if str(acc.get("id")) == google_id),
        None,
    )
    if not account:
        raise HTTPException(
            status_code=404,
            detail={"error": f"No Google Ads account found for {google_id}"},
        )

    account_code = account.get("accountCode")
    if not account_code:
        raise HTTPException(
            status_code=404,
            detail={"error": f"No account code found for {google_id}"},
        )

    account_codes = [account_code]

    master_budgets, allocations, rollbreakdowns, accelerations = run_parallel(
        tasks=[
            (get_masterbudgets, (account_codes, month_value, year_value)),
            (get_allocations, (account_codes, month_value, year_value)),
            (get_rollbreakdowns, (account_codes, month_value, year_value)),
            (_get_accelerations_for_period, (account_codes, period_date)),
        ],
        api_name="spendsphere_v1_ui_load_db",
    )

    campaigns, budgets, costs = run_parallel(
        tasks=[
            (get_ggad_campaigns, ([account],)),
            (get_ggad_budgets, ([account],)),
            (get_ggad_spents, ([account], month_value, year_value)),
        ],
        api_name="spendsphere_v1_ui_load_google_ads",
    )

    active_period = get_active_period(account_codes)
    rollovers = get_rollovers(account_codes, month_value, year_value)

    rows = transform_google_ads_data(
        master_budgets=master_budgets,
        campaigns=campaigns,
        budgets=budgets,
        costs=costs,
        allocations=allocations,
        rollovers=rollbreakdowns,
        accelerations=accelerations,
        activePeriod=active_period,
        today=period_date,
        include_transform_results=True,
    )

    master_budgets_payload = _build_master_budgets(master_budgets)
    roll_breakdown_payload = _build_roll_breakdown(rollbreakdowns)
    table_data = _build_table_data(rows, budgets, allocations)
    grand_total_spent = _to_float(
        sum(
            Decimal(str(item.get("spent", 0) or 0))
            for item in table_data
        )
    )

    rollovers_total = _to_float(
        sum(Decimal(str(r.get("amount", 0))) for r in rollovers)
    )

    period_month = month_value if month_value is not None else period_date.month
    period_year = year_value if year_value is not None else period_date.year
    active_period_row = active_period[0] if active_period else None
    active_period_payload = _build_monthly_active_period(
        active_period_row,
        month=period_month,
        year=period_year,
    )

    return {
        "masterBudgets": master_budgets_payload,
        "rollOvers": rollovers_total,
        "activePeriod": active_period_payload,
        "rollBreakdown": roll_breakdown_payload,
        "tableData": {
            "grandTotalSpent": grand_total_spent,
            "data": table_data,
        },
    }


@router.post(
    "/ui/update",
    summary="Update UI allocations and roll breakdowns",
    description=(
        "Upsert allocations (budget level) and roll breakdowns (ad type) "
        "for a single account."
    ),
)
def update_ui_allocations_rollbreaks(
    request_payload: UiAllocationRollBreakUpdateRequest,
):
    """
    Example request:
    POST /api/spendsphere/v1/ui/update
    {
      "accountCode": "TAAA",
      "month": 1,
      "year": 2026,
      "returnNewData": true,
      "updatedRollBreakdowns": [
        {
          "id": "0ad1dc44-35f2-4fb6-9f1f-19527ab193e3",
          "adTypeCode": "SEM",
          "currentAmount": 0,
          "newAmount": 100
        }
      ],
      "updatedAllocations": [
        {
          "id": "7d963d35-ae9c-4c33-9ce3-375d0a8e1287",
          "budgetId": "15225876848",
          "currentAllocation": 40,
          "newAllocation": 80
        }
      ]
    }

    Note: Google Ads budget/status mutations run only when the payload
    month/year match the current period.

    Example response:
    {
      "updatedAllocations": {"updated": 1, "inserted": 0},
      "updatedRollBreakdowns": {"updated": 1, "inserted": 0},
      "googleAdsUpdates": {
        "overallSummary": {
          "total": 1,
          "succeeded": 1,
          "failed": 0,
          "warnings": 0
        },
        "mutationResults": []
      },
      "rollBreakdown": {
        "grandTotalRollBreakdown": 1000,
        "SEM": {"id": "0ad1dc44-35f2-4fb6-9f1f-19527ab193e3", "amount": 1000}
      },
      "tableData": {
        "grandTotalSpent": 586.09,
        "data": []
      }
    }
    """
    try:
        request_payload = UiAllocationRollBreakUpdateRequest.model_validate(
            request_payload, from_attributes=True
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "Invalid payload", "errors": exc.errors()},
        ) from exc

    account_code = require_account_code(request_payload.accountCode)

    errors: list[dict[str, object]] = []

    if not 1 <= request_payload.month <= 12:
        errors.append(
            {
                "field": "month",
                "value": request_payload.month,
                "message": "month must be between 1 and 12",
            }
        )
    if not 2000 <= request_payload.year <= 2100:
        errors.append(
            {
                "field": "year",
                "value": request_payload.year,
                "message": "year must be between 2000 and 2100",
            }
        )

    allowed_adtypes = {
        str(key).strip().upper()
        for key in get_adtypes().keys()
        if str(key).strip()
    }

    allocation_rows: list[dict] = []
    for idx, item in enumerate(request_payload.updatedAllocations):
        row_id = _normalize_optional_str(item.id)
        budget_id = _normalize_optional_str(item.budgetId)
        allocation_value = _to_float(item.newAllocation)

        if allocation_value is None:
            errors.append(
                {
                    "index": idx,
                    "field": "updatedAllocations.newAllocation",
                    "value": item.newAllocation,
                    "message": "newAllocation is required and must be numeric",
                }
            )

        if not row_id and not budget_id:
            errors.append(
                {
                    "index": idx,
                    "field": "updatedAllocations.budgetId",
                    "message": "budgetId is required when id is not provided",
                }
            )

        if allocation_value is not None:
            allocation_rows.append(
                {
                    "id": row_id,
                    "accountCode": account_code,
                    "ggBudgetId": budget_id,
                    "allocation": allocation_value,
                }
            )

    rollbreak_rows: list[dict] = []
    for idx, item in enumerate(request_payload.updatedRollBreakdowns):
        row_id = _normalize_optional_str(item.id)
        item_account = _normalize_optional_str(item.accountCode)
        ad_type = _normalize_optional_str(item.adTypeCode)
        if ad_type:
            ad_type = ad_type.upper()
        amount_value = _to_float(item.newAmount)

        if item_account and item_account.upper() != account_code:
            errors.append(
                {
                    "index": idx,
                    "field": "updatedRollBreakdowns.accountCode",
                    "value": item.accountCode,
                    "message": "accountCode must match payload.accountCode",
                }
            )

        if not ad_type:
            errors.append(
                {
                    "index": idx,
                    "field": "updatedRollBreakdowns.adTypeCode",
                    "value": item.adTypeCode,
                    "message": "adTypeCode is required",
                }
            )
        elif allowed_adtypes and ad_type not in allowed_adtypes:
            errors.append(
                {
                    "index": idx,
                    "field": "updatedRollBreakdowns.adTypeCode",
                    "value": ad_type,
                    "allowed": sorted(allowed_adtypes),
                }
            )

        if amount_value is None:
            errors.append(
                {
                    "index": idx,
                    "field": "updatedRollBreakdowns.newAmount",
                    "value": item.newAmount,
                    "message": "newAmount is required and must be numeric",
                }
            )

        if ad_type and amount_value is not None:
            rollbreak_rows.append(
                {
                    "id": row_id,
                    "accountCode": account_code,
                    "adTypeCode": ad_type,
                    "amount": amount_value,
                }
            )

    if errors:
        raise HTTPException(status_code=400, detail={"error": "Invalid payload", "errors": errors})

    month_value, year_value = _resolve_period(
        request_payload.month, request_payload.year
    )

    allocations_result = upsert_allocations(
        allocation_rows,
        month=month_value,
        year=year_value,
    )
    rollbreaks_result = upsert_rollbreakdowns(
        rollbreak_rows,
        month=month_value,
        year=year_value,
    )

    current_period = get_current_period()
    is_current_period = (
        month_value == current_period["month"]
        and year_value == current_period["year"]
    )

    needs_google_data = request_payload.returnNewData or is_current_period
    account_codes = [account_code]
    period_date = _resolve_period_date(month_value, year_value)

    mutation_results: list[dict] = []
    overall_summary = {"total": 0, "succeeded": 0, "failed": 0, "warnings": 0}

    if needs_google_data:
        accounts = get_ggad_accounts()
        account = next(
            (
                acc
                for acc in accounts
                if str(acc.get("accountCode", "")).strip().upper() == account_code
            ),
            None,
        )
        if not account:
            raise HTTPException(
                status_code=404,
                detail={"error": f"No Google Ads account found for {account_code}"},
            )

        master_budgets, allocations, rollbreakdowns, accelerations = run_parallel(
            tasks=[
                (get_masterbudgets, (account_codes, month_value, year_value)),
                (get_allocations, (account_codes, month_value, year_value)),
                (get_rollbreakdowns, (account_codes, month_value, year_value)),
                (_get_accelerations_for_period, (account_codes, period_date)),
            ],
            api_name="spendsphere_v1_ui_update_db",
        )

        campaigns, budgets, costs = run_parallel(
            tasks=[
                (get_ggad_campaigns, ([account],)),
                (get_ggad_budgets, ([account],)),
                (get_ggad_spents, ([account], month_value, year_value)),
            ],
            api_name="spendsphere_v1_ui_update_google_ads",
        )

        active_period = get_active_period(account_codes)

        rows = transform_google_ads_data(
            master_budgets=master_budgets,
            campaigns=campaigns,
            budgets=budgets,
            costs=costs,
            allocations=allocations,
            rollovers=rollbreakdowns,
            accelerations=accelerations,
            activePeriod=active_period,
            today=period_date,
            include_transform_results=True,
        )

        if is_current_period:
            budget_payloads, campaign_payloads = generate_update_payloads(rows)

            mutation_tasks = []
            for budget_payload in budget_payloads:
                updates = budget_payload.get("updates", [])
                if updates:
                    mutation_tasks.append(
                        (_run_budget_update, (budget_payload["customer_id"], updates))
                    )

            for campaign_payload in campaign_payloads:
                updates = campaign_payload.get("updates", [])
                if updates:
                    mutation_tasks.append(
                        (_run_campaign_update, (campaign_payload["customer_id"], updates))
                    )

            mutation_results = (
                run_parallel(tasks=mutation_tasks, api_name="google_ads_mutation")
                if mutation_tasks
                else []
            )

            for result in mutation_results:
                summary = result.get("summary", {})
                overall_summary["total"] += summary.get("total", 0)
                overall_summary["succeeded"] += summary.get("succeeded", 0)
                overall_summary["failed"] += summary.get("failed", 0)
                overall_summary["warnings"] += summary.get("warnings", 0)

    response: dict[str, object] = {
        "updatedAllocations": allocations_result,
        "updatedRollBreakdowns": rollbreaks_result,
        "googleAdsUpdates": {
            "overallSummary": overall_summary,
            "mutationResults": mutation_results,
        },
    }

    if request_payload.returnNewData:
        roll_breakdown_payload = _build_roll_breakdown(rollbreakdowns)
        table_data = _build_table_data(rows, budgets, allocations)
        grand_total_spent = _to_float(
            sum(
                Decimal(str(item.get("spent", 0) or 0))
                for item in table_data
            )
        )

        response.update(
            {
                "rollBreakdown": roll_breakdown_payload,
                "tableData": {
                    "grandTotalSpent": grand_total_spent,
                    "data": table_data,
                },
            }
        )

    return response
