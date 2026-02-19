import calendar
import math
import re
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation

import pytz
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from apps.spendsphere.api.v1.endpoints.periods import (
    build_periods_data,
    validate_month_offsets,
)
from apps.spendsphere.api.v1.helpers.config import (
    get_acceleration_scope_types,
    get_adtypes,
    get_service_mapping,
)
from apps.spendsphere.api.v1.helpers.dataTransform import (
    generate_update_payloads,
    transform_google_ads_data,
)
from apps.spendsphere.api.v1.helpers.db_queries import (
    get_accelerations,
    get_accelerations_by_ids,
    get_accelerations_by_keys,
    get_accelerations_by_ids_active,
    get_allocations,
    get_masterbudgets,
    get_rollbreakdowns,
    insert_accelerations,
    soft_delete_accelerations_by_ids,
    update_acceleration_by_id,
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

    current_period = get_current_period()
    current_key = (current_period["year"], current_period["month"])
    target_key = (year, month)

    if target_key == current_key:
        return today
    if target_key < current_key:
        last_day = calendar.monthrange(year, month)[1]
        return date(year, month, last_day)
    return date(year, month, 1)


def _get_active_period_checked(
    account_codes: list[str],
    month: int,
    year: int,
    *,
    as_of: date,
) -> list[dict]:
    try:
        return get_active_period(
            account_codes,
            month,
            year,
            as_of=as_of,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "Overlapping active periods", "message": str(exc)},
        ) from exc


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


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, str) and value.strip():
        cleaned = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(cleaned)
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


def _normalize_string_list(values: object) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        candidates = [values]
    elif isinstance(values, list):
        candidates = values
    else:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        cleaned = candidate.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def _sanitize_acceleration_rows(rows: list[dict]) -> list[dict]:
    sanitized: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sanitized.append(
            {
                k: v
                for k, v in row.items()
                if k not in {"dateCreated", "dateUpdated"}
            }
        )
    return sanitized


def _build_validation_error_detail(errors: list[dict[str, object]]) -> dict[str, object]:
    seen: set[tuple[str, str | None, str | None]] = set()
    deduped: list[dict[str, object]] = []
    for item in errors:
        field = str(item.get("field") or "")
        message = str(item.get("message") or "")
        value = item.get("value")
        key = (field, message, str(value))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    messages: list[str] = []
    for item in deduped:
        field = str(item.get("field") or "")
        msg = str(item.get("message") or "Invalid value")
        if field:
            messages.append(f"{field}: {msg}")
        else:
            messages.append(msg)

    message = "; ".join(messages) if messages else "Invalid payload"
    return {
        "error": "Invalid payload",
        "message": message,
        "messages": messages,
        "errors": deduped,
    }


def _build_monthly_active_period(
    row: dict | None,
    *,
    month: int,
    year: int,
    as_of: date | None = None,
) -> dict:
    month_start = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    month_end = date(year, month, last_day)

    start_date = _coerce_date(row.get("startDate") if row else None)
    end_date = _coerce_date(row.get("endDate") if row else None)
    next_start_date = _coerce_date(row.get("nextStartDate") if row else None)

    if as_of is None:
        as_of = _resolve_period_date(month, year)

    if row and "isActive" in row:
        is_active = bool(row.get("isActive"))
    else:
        start_ok = True if start_date is None else start_date <= as_of
        end_ok = True if end_date is None else end_date >= as_of
        is_active = start_ok and end_ok

    response: dict[str, object] = {"isActive": is_active}
    message_parts: list[str] = []

    current_period = get_current_period()
    current_start = date(current_period["year"], current_period["month"], 1)
    current_last_day = calendar.monthrange(
        current_period["year"], current_period["month"]
    )[1]
    current_end = date(current_period["year"], current_period["month"], current_last_day)
    request_is_current = (year, month) == (
        current_period["year"],
        current_period["month"],
    )
    show_daily_message = (
        bool(end_date)
        and request_is_current
        and current_start <= end_date <= current_end
    )

    if start_date:
        response["startDate"] = start_date.isoformat()
    if end_date:
        response["endDate"] = end_date.isoformat()
    if next_start_date:
        response["nextStartDate"] = next_start_date.isoformat()

    if start_date and as_of < start_date:
        message_parts.append(
            f"Account will start on {start_date.strftime('%m/%d/%Y')}"
        )
    elif end_date and as_of <= end_date:
        message_parts.append(
            f"Account last day on {end_date.strftime('%m/%d/%Y')} EOD"
        )
    elif end_date and as_of > end_date:
        message_parts.append(
            f"Account ended on {end_date.strftime('%m/%d/%Y')} EOD"
        )
        if next_start_date and next_start_date > as_of:
            message_parts.append(
                f"Account will start on {next_start_date.strftime('%m/%d/%Y')}"
            )

    if show_daily_message:
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


def _to_decimal(value: object, *, fallback: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return fallback
    if isinstance(value, Decimal):
        return value
    cleaned = str(value).strip()
    if not cleaned:
        return fallback
    is_negative = cleaned.startswith("(") and cleaned.endswith(")")
    if is_negative:
        cleaned = cleaned[1:-1]
    cleaned = cleaned.replace("$", "").replace(",", "")
    cleaned = re.sub(r"\s+", "", cleaned)
    if cleaned in {"-", ".", "-.", ".-"}:
        return fallback
    try:
        parsed = Decimal(cleaned)
    except (InvalidOperation, TypeError, ValueError):
        return fallback
    return -parsed if is_negative else parsed


def _get_accelerations_for_period(
    account_codes: list[str],
    period_date: date,
) -> list[dict]:
    return get_accelerations(account_codes, today=period_date)


def _get_accelerations_for_month(
    account_codes: list[str],
    month: int,
    year: int,
) -> list[dict]:
    start_date = date(year, month, 1)
    end_date = date(year, month, calendar.monthrange(year, month)[1])
    return get_accelerations(
        account_codes,
        start_date=start_date,
        end_date=end_date,
    )


def _filter_accelerations_for_date(
    accelerations: list[dict],
    period_date: date,
) -> list[dict]:
    if not accelerations:
        return []
    filtered: list[dict] = []
    for row in accelerations:
        if not isinstance(row, dict):
            continue
        start_date = _coerce_date(row.get("startDate"))
        end_date = _coerce_date(row.get("endDate"))
        if start_date and period_date < start_date:
            continue
        if end_date and period_date > end_date:
            continue
        filtered.append(row)
    return filtered


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

    return _finalize_table_data(table_data)


def _build_table_data_fallback(
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    *,
    allocations: list[dict] | None = None,
    accelerations: list[dict] | None = None,
) -> list[dict]:
    if not campaigns:
        return []

    allocation_lookup: dict[tuple[str, str], dict] = {}
    for allocation in allocations or []:
        account_code = str(allocation.get("accountCode", "")).strip().upper()
        budget_id = str(allocation.get("ggBudgetId", "")).strip()
        if not account_code or not budget_id:
            continue
        allocation_lookup[(account_code, budget_id)] = {
            "id": allocation.get("id"),
            "allocation": _to_float(allocation.get("allocation")),
        }

    def _accel_sort_key(accel: dict) -> tuple[datetime, int]:
        updated = _coerce_datetime(accel.get("dateUpdated") or accel.get("dateCreated"))
        try:
            accel_id = int(accel.get("id") or 0)
        except (TypeError, ValueError):
            accel_id = 0
        return (updated or datetime.min, accel_id)

    def _pick_better(candidate: dict | None, current: dict | None) -> dict | None:
        if candidate is None:
            return current
        if current is None:
            return candidate
        return candidate if _accel_sort_key(candidate) > _accel_sort_key(current) else current

    account_accels: dict[str, dict] = {}
    ad_type_accels: dict[tuple[str, str], dict] = {}
    budget_accels: dict[tuple[str, str], dict] = {}

    for accel in accelerations or []:
        account_code = str(accel.get("accountCode", "")).strip().upper()
        scope_type = str(accel.get("scopeLevel", "")).strip().upper()
        scope_value = str(accel.get("scopeValue", "")).strip()

        if not account_code or not scope_type:
            continue

        if scope_type == "ACCOUNT":
            account_accels[account_code] = _pick_better(
                accel, account_accels.get(account_code)
            )
        elif scope_type == "AD_TYPE" and scope_value:
            key = (account_code, scope_value.upper())
            ad_type_accels[key] = _pick_better(accel, ad_type_accels.get(key))
        elif scope_type == "BUDGET" and scope_value:
            key = (account_code, scope_value)
            budget_accels[key] = _pick_better(accel, budget_accels.get(key))

    budget_lookup: dict[tuple[str | None, str | None], dict] = {
        (b.get("customerId"), b.get("budgetId")): b for b in budgets
    }

    cost_lookup: dict[tuple[str | None, str | None], Decimal] = {}
    for cost in costs:
        key = (cost.get("customerId"), cost.get("campaignId"))
        cost_lookup[key] = cost_lookup.get(key, Decimal("0")) + _to_decimal(
            cost.get("cost", 0)
        )

    grouped: dict[tuple[str | None, str | None], dict] = {}
    for campaign in campaigns:
        customer_id = campaign.get("customerId")
        budget_id = campaign.get("budgetId")
        if not customer_id or not budget_id:
            continue

        budget_meta = budget_lookup.get((customer_id, budget_id), {})
        account_code = str(
            budget_meta.get("accountCode") or campaign.get("accountCode") or ""
        ).strip().upper()

        key = (customer_id, budget_id)
        entry = grouped.get(key)
        if entry is None:
            allocation_value = allocation_lookup.get(
                (account_code, str(budget_id).strip())
            ) or {"id": None, "allocation": None}

            accel = None
            if account_code:
                accel = budget_accels.get((account_code, str(budget_id).strip()))
                if accel is None and campaign.get("adTypeCode"):
                    accel = ad_type_accels.get(
                        (account_code, str(campaign.get("adTypeCode")).upper())
                    )
                if accel is None:
                    accel = account_accels.get(account_code)
            acceleration_value = {
                "id": accel.get("id"),
                "multiplier": _to_float(accel.get("multiplier")),
            } if accel else {"id": None, "multiplier": None}

            entry = {
                "accountId": customer_id,
                "name": budget_meta.get("budgetName")
                or campaign.get("campaignName"),
                "budgetId": budget_id,
                "explicitlyShared": budget_meta.get("explicitlyShared"),
                "status": budget_meta.get("status") or campaign.get("status"),
                "currentBudget": _to_float(budget_meta.get("amount")),
                "spent": Decimal("0"),
                "adTypeCode": campaign.get("adTypeCode"),
                "allocation": allocation_value,
                "acceleration": acceleration_value,
                "campaigns": [],
                "_campaignNames": "",
            }
            grouped[key] = entry

        entry["campaigns"].append(
            {
                "campaignId": campaign.get("campaignId"),
                "campaignName": campaign.get("campaignName"),
                "campaignStatus": campaign.get("status"),
            }
        )

        entry["spent"] += cost_lookup.get(
            (customer_id, campaign.get("campaignId")),
            Decimal("0"),
        )

    for entry in grouped.values():
        entry["_campaignNames"] = _format_campaign_names(entry["campaigns"])
        entry["spent"] = _to_float(entry.get("spent"))

    return _finalize_table_data(list(grouped.values()))


def _finalize_table_data(table_data: list[dict]) -> list[dict]:
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


def _get_ui_context_with_mutations(
    account_code: str,
    month: int,
    year: int,
    *,
    period_date: date,
    return_new_data: bool,
    is_current_period: bool | None = None,
    allow_mutations: bool = True,
    refresh_budgets: bool = False,
    api_name_prefix: str = "spendsphere_v1_ui",
) -> dict[str, object]:
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

    account_codes = [account_code]

    master_budgets, allocations, rollbreakdowns, accelerations = run_parallel(
        tasks=[
            (get_masterbudgets, (account_codes, month, year)),
            (get_allocations, (account_codes, month, year)),
            (get_rollbreakdowns, (account_codes, month, year)),
            (_get_accelerations_for_period, (account_codes, period_date)),
        ],
        api_name=f"{api_name_prefix}_db",
    )

    def _get_budgets(accounts: list[dict]) -> list[dict]:
        return get_ggad_budgets(accounts, refresh_cache=refresh_budgets)

    campaigns, budgets, costs = run_parallel(
        tasks=[
            (get_ggad_campaigns, ([account],)),
            (_get_budgets, ([account],)),
            (get_ggad_spents, ([account], month, year)),
        ],
        api_name=f"{api_name_prefix}_google_ads",
    )

    active_period = _get_active_period_checked(
        account_codes,
        month,
        year,
        as_of=period_date,
    )

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

    if is_current_period is None:
        current_period = get_current_period()
        is_current_period = (
            month == current_period["month"]
            and year == current_period["year"]
        )

    mutation_results: list[dict] = []
    overall_summary = {"total": 0, "succeeded": 0, "failed": 0, "warnings": 0}

    if allow_mutations and is_current_period:
        budget_payloads, campaign_payloads = generate_update_payloads(rows)
        mutation_tasks: list[tuple] = []

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

        if return_new_data and mutation_tasks:
            budgets = get_ggad_budgets([account], refresh_cache=refresh_budgets)

        for result in mutation_results:
            summary = result.get("summary", {})
            overall_summary["total"] += summary.get("total", 0)
            overall_summary["succeeded"] += summary.get("succeeded", 0)
            overall_summary["failed"] += summary.get("failed", 0)
            overall_summary["warnings"] += summary.get("warnings", 0)

    return {
        "account": account,
        "rows": rows,
        "budgets": budgets,
        "allocations": allocations,
        "rollbreakdowns": rollbreakdowns,
        "accelerations": accelerations,
        "campaigns": campaigns,
        "costs": costs,
        "googleAdsUpdates": {
            "overallSummary": overall_summary,
            "mutationResults": mutation_results,
        },
    }


def _build_table_data_payload(
    rows: list[dict],
    *,
    budgets: list[dict],
    allocations: list[dict],
    campaigns: list[dict],
    costs: list[dict],
    accelerations: list[dict] | None = None,
) -> dict:
    if rows:
        table_data_rows = _build_table_data(rows, budgets, allocations)
    else:
        table_data_rows = _build_table_data_fallback(
            campaigns,
            budgets,
            costs,
            allocations=allocations,
            accelerations=accelerations,
        )
    grand_total_spent = _to_float(
        sum(Decimal(str(item.get("spent", 0) or 0)) for item in table_data_rows)
    )
    return {
        "grandTotalSpent": grand_total_spent,
        "data": table_data_rows,
    }


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


class UiAccelerationDate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    start: date
    end: date


class UiAccelerationUpsertItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    scopeLevel: str
    scopeValue: list[str] = Field(default_factory=list)
    date: UiAccelerationDate
    multiplier: float
    note: str | None = Field(default=None, max_length=2048)
    id: str | None = None


class UiAccelerationUpsertRequest(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "accountCode": "TAAA",
                    "month": 2,
                    "year": 2026,
                    "returnNewData": True,
                    "newAccelerations": [
                        {
                            "scopeLevel": "AD_TYPE",
                            "scopeValue": ["SEM"],
                            "date": {"start": "2026-02-10", "end": "2026-02-20"},
                            "multiplier": 150,
                            "note": "SEM promo window",
                            "id": "",
                        }
                    ],
                }
            ]
        },
    )
    accountCode: str
    month: int
    year: int
    returnNewData: bool = Field(default=False, alias="includeData")
    newAccelerations: list[UiAccelerationUpsertItem] = Field(default_factory=list)


class UiAccelerationDeleteRequest(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={"examples": [{"ids": [101, 102]}]},
    )
    ids: list[object] = Field(default_factory=list)


# ============================================================
# UI
# ============================================================


@router.get(
    "/uis/selections",
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
        GET /api/spendsphere/v1/uis/selections
        Header: X-Tenant-Id: acme

    Example request (force refresh):
        GET /api/spendsphere/v1/uis/selections?refresh_cache=true
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
    "/uis/load",
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
        GET /api/spendsphere/v1/uis/load?googleId=6563107233&month=1&year=2026
        Header: X-Tenant-Id: acme

    Example request (current period):
        GET /api/spendsphere/v1/uis/load?googleId=6563107233
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
          "rolloverTotal": 1000.0,
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
          "accelerations": [
            {
              "id": 12,
              "accountCode": "TAAA",
              "scopeLevel": "ACCOUNT",
              "scopeValue": "TAAA",
              "startDate": "2026-01-01",
              "endDate": "2026-01-31",
              "multiplier": 120.0,
              "note": "Front-load for January"
            }
          ],
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
    if month_value is None:
        month_value = period_date.month
    if year_value is None:
        year_value = period_date.year

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
            (_get_accelerations_for_month, (account_codes, month_value, year_value)),
        ],
        api_name="spendsphere_v1_ui_load_db",
    )
    active_accelerations = _filter_accelerations_for_date(accelerations, period_date)

    campaigns, budgets, costs = run_parallel(
        tasks=[
            (get_ggad_campaigns, ([account],)),
            (get_ggad_budgets, ([account],)),
            (get_ggad_spents, ([account], month_value, year_value)),
        ],
        api_name="spendsphere_v1_ui_load_google_ads",
    )

    active_period = _get_active_period_checked(
        account_codes,
        month_value,
        year_value,
        as_of=period_date,
    )
    rollovers = get_rollovers(
        account_codes,
        month_value,
        year_value,
        include_unrollable=False,
    )

    rows = transform_google_ads_data(
        master_budgets=master_budgets,
        campaigns=campaigns,
        budgets=budgets,
        costs=costs,
        allocations=allocations,
        rollovers=rollbreakdowns,
        accelerations=active_accelerations,
        activePeriod=active_period,
        today=period_date,
        include_transform_results=True,
    )

    master_budgets_payload = _build_master_budgets(master_budgets)
    roll_breakdown_payload = _build_roll_breakdown(rollbreakdowns)
    sanitized_accelerations = _sanitize_acceleration_rows(accelerations)
    table_data = _build_table_data_payload(
        rows,
        budgets=budgets,
        allocations=allocations,
        campaigns=campaigns,
        costs=costs,
        accelerations=active_accelerations,
    )

    rollovers_total = _to_float(
        sum(_to_decimal(r.get("amount", 0)) for r in rollovers)
    )

    active_period_row = active_period[0] if active_period else None
    active_period_payload = _build_monthly_active_period(
        active_period_row,
        month=month_value,
        year=year_value,
        as_of=period_date,
    )

    return {
        "masterBudgets": master_budgets_payload,
        "rolloverTotal": rollovers_total,
        "activePeriod": active_period_payload,
        "rollBreakdown": roll_breakdown_payload,
        "accelerations": sanitized_accelerations,
        "tableData": table_data,
    }


@router.post(
    "/uis/update",
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
        POST /api/spendsphere/v1/uis/update
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

    errors: list[dict[str, object]] = []
    account_code_raw = _normalize_optional_str(request_payload.accountCode)
    if not account_code_raw:
        errors.append(
            {
                "field": "accountCode",
                "value": request_payload.accountCode,
                "message": "accountCode is required",
            }
        )

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

    if errors:
        raise HTTPException(status_code=400, detail={"error": "Invalid payload", "errors": errors})

    account_code = require_account_code(
        account_code_raw or "",
        month=request_payload.month,
        year=request_payload.year,
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
    period_date = _resolve_period_date(month_value, year_value)

    mutation_results: list[dict] = []
    overall_summary = {"total": 0, "succeeded": 0, "failed": 0, "warnings": 0}

    if needs_google_data:
        ui_context = _get_ui_context_with_mutations(
            account_code,
            month_value,
            year_value,
            period_date=period_date,
            return_new_data=request_payload.returnNewData,
            is_current_period=is_current_period,
            refresh_budgets=request_payload.returnNewData,
            api_name_prefix="spendsphere_v1_ui_update",
        )
        rows = ui_context["rows"]
        budgets = ui_context["budgets"]
        allocations = ui_context["allocations"]
        accelerations = ui_context["accelerations"]
        rollbreakdowns = ui_context["rollbreakdowns"]
        campaigns = ui_context["campaigns"]
        costs = ui_context["costs"]
        google_ads_updates = ui_context["googleAdsUpdates"]
        overall_summary = google_ads_updates["overallSummary"]
        mutation_results = google_ads_updates["mutationResults"]

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
        table_data = _build_table_data_payload(
            rows,
            budgets=budgets,
            allocations=allocations,
            campaigns=campaigns,
            costs=costs,
            accelerations=accelerations,
        )

        response.update(
            {
                "rollBreakdown": roll_breakdown_payload,
                "tableData": table_data,
            }
        )

    return response


@router.post(
    "/uis/accelerations",
    summary="Create or update UI accelerations",
    description=(
        "Creates new accelerations when id is empty; updates an existing "
        "acceleration when id is provided. This route does not mutate "
        "Google Ads budgets or statuses."
    ),
)
def upsert_ui_acceleration(
    request_payload: UiAccelerationUpsertRequest,
):
    """
    Example request (create):
        POST /api/spendsphere/v1/uis/accelerations
        {
          "accountCode": "TAAA",
          "month": 2,
          "year": 2026,
          "returnNewData": true,
          "newAccelerations": [
            {
              "scopeLevel": "AD_TYPE",
              "scopeValue": ["SEM"],
              "date": {"start": "2026-02-10", "end": "2026-02-20"},
              "multiplier": 150,
              "note": "SEM promo window",
              "id": ""
            }
          ]
        }

    Example request (update):
        POST /api/spendsphere/v1/uis/accelerations
        {
          "accountCode": "TAAA",
          "month": 2,
          "year": 2026,
          "returnNewData": true,
          "newAccelerations": [
            {
              "scopeLevel": "AD_TYPE",
              "scopeValue": ["SEM"],
              "date": {"start": "2026-02-10", "end": "2026-02-20"},
              "multiplier": 150,
              "note": "SEM promo window",
              "id": "102"
            }
          ]
        }

    Example response:
        {
          "accelerations": [
            {
              "id": 102,
              "accountCode": "TAAA",
              "scopeLevel": "AD_TYPE",
              "scopeValue": "SEM",
              "startDate": "2026-02-10",
              "endDate": "2026-02-20",
              "multiplier": 150.0,
              "note": "SEM promo window",
              "active": 1
            }
          ],
          "tableData": {
            "grandTotalSpent": 586.09,
            "data": []
          }
        }
    """
    try:
        request_payload = UiAccelerationUpsertRequest.model_validate(
            request_payload, from_attributes=True
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "Invalid payload", "errors": exc.errors()},
        ) from exc

    errors: list[dict[str, object]] = []
    month_value = request_payload.month
    year_value = request_payload.year

    account_code_raw = _normalize_optional_str(request_payload.accountCode)
    account_code: str | None = None
    if not account_code_raw:
        errors.append(
            {
                "field": "accountCode",
                "value": request_payload.accountCode,
                "message": "accountCode is required",
            }
        )

    if not 1 <= month_value <= 12:
        errors.append(
            {
                "field": "month",
                "value": month_value,
                "message": "month must be between 1 and 12",
            }
        )
    if not 2000 <= year_value <= 2100:
        errors.append(
            {
                "field": "year",
                "value": year_value,
                "message": "year must be between 2000 and 2100",
            }
        )
    if not request_payload.newAccelerations:
        errors.append(
            {
                "field": "newAccelerations",
                "value": request_payload.newAccelerations,
                "message": "newAccelerations must contain at least one item",
            }
        )

    if not errors and account_code_raw:
        account_code = require_account_code(
            account_code_raw,
            month=month_value,
            year=year_value,
        )

    allowed_scopes = {s.upper() for s in get_acceleration_scope_types()}
    allowed_adtypes = {
        str(key).strip().upper()
        for key in get_adtypes().keys()
        if str(key).strip()
    }

    update_ids: list[str] = []
    for item in request_payload.newAccelerations:
        item_id = _normalize_optional_str(item.id)
        if item_id:
            update_ids.append(item_id)

    existing_by_id: dict[str, dict] = {}
    if update_ids:
        existing_rows = get_accelerations_by_ids_active(update_ids)
        existing_by_id = {
            str(row.get("id")): row
            for row in existing_rows
            if isinstance(row, dict) and row.get("id") is not None
        }

    create_rows: list[dict] = []
    update_rows: list[dict] = []

    for idx, item in enumerate(request_payload.newAccelerations):
        prefix = f"newAccelerations[{idx}]"
        acceleration_id = _normalize_optional_str(item.id)
        is_create = not acceleration_id
        note_provided = "note" in item.model_fields_set
        error_count_before = len(errors)

        scope_level = _normalize_optional_str(item.scopeLevel)
        if not scope_level:
            errors.append(
                {
                    "field": f"{prefix}.scopeLevel",
                    "value": item.scopeLevel,
                    "message": "scopeLevel is required",
                }
            )
            scope_level = ""
        scope_level = scope_level.upper()
        if scope_level and allowed_scopes and scope_level not in allowed_scopes:
            errors.append(
                {
                    "field": f"{prefix}.scopeLevel",
                    "value": scope_level,
                    "allowed": sorted(allowed_scopes),
                }
            )

        scope_values = _normalize_string_list(item.scopeValue)
        if is_create:
            if scope_level in {"AD_TYPE", "BUDGET"} and not scope_values:
                errors.append(
                    {
                        "field": f"{prefix}.scopeValue",
                        "value": item.scopeValue,
                        "message": "scopeValue is required",
                    }
                )
        elif len(scope_values) != 1:
            errors.append(
                {
                    "field": f"{prefix}.scopeValue",
                    "value": item.scopeValue,
                    "message": "scopeValue must contain exactly one value for update",
                }
            )

        start_date = item.date.start
        end_date = item.date.end
        if start_date and end_date and start_date > end_date:
            errors.append(
                {
                    "field": f"{prefix}.date",
                    "value": item.date.model_dump(),
                    "message": "date.start must be on or before date.end",
                }
            )

        multiplier_value = _to_float(item.multiplier)
        if multiplier_value is None:
            errors.append(
                {
                    "field": f"{prefix}.multiplier",
                    "value": item.multiplier,
                    "message": "multiplier is required and must be numeric",
                }
            )
        elif multiplier_value < 100:
            errors.append(
                {
                    "field": f"{prefix}.multiplier",
                    "value": multiplier_value,
                    "message": "multiplier must be >= 100",
                }
            )

        note_value = _normalize_optional_str(item.note)

        if not is_create:
            existing_row = existing_by_id.get(acceleration_id or "")
            if not existing_row:
                errors.append(
                    {
                        "field": f"{prefix}.id",
                        "value": item.id,
                        "message": "Acceleration not found",
                    }
                )
            else:
                existing_account = _normalize_optional_str(
                    existing_row.get("accountCode")
                )
                if existing_account:
                    existing_account = existing_account.upper()
                if existing_account and account_code and existing_account != account_code:
                    errors.append(
                        {
                            "field": f"{prefix}.id",
                            "value": item.id,
                            "message": "accountCode must match existing acceleration",
                        }
                    )

        if scope_level == "AD_TYPE":
            normalized_values: list[str] = []
            for value in scope_values:
                normalized = value.upper()
                if allowed_adtypes and normalized not in allowed_adtypes:
                    errors.append(
                        {
                            "field": f"{prefix}.scopeValue",
                            "value": value,
                            "allowed": sorted(allowed_adtypes),
                        }
                    )
                normalized_values.append(normalized)
            scope_values = normalized_values
        elif scope_level == "ACCOUNT":
            if not account_code:
                errors.append(
                    {
                        "field": "accountCode",
                        "value": request_payload.accountCode,
                        "message": "accountCode is required for ACCOUNT scope",
                    }
                )
            elif len(scope_values) > 1:
                errors.append(
                    {
                        "field": f"{prefix}.scopeValue",
                        "value": scope_values,
                        "message": "scopeValue must contain exactly one value for ACCOUNT scope",
                    }
                )
            elif scope_values and scope_values[0].upper() != account_code:
                errors.append(
                    {
                        "field": f"{prefix}.scopeValue",
                        "value": scope_values,
                        "message": "scopeValue must match accountCode for ACCOUNT scope",
                    }
                )
            scope_values = [account_code] if account_code else scope_values
        elif scope_level == "BUDGET":
            if not account_code:
                errors.append(
                    {
                        "field": "accountCode",
                        "value": request_payload.accountCode,
                        "message": "accountCode is required for BUDGET scope",
                    }
                )
            elif scope_values:
                gg_accounts = get_ggad_accounts()
                gg_account = next(
                    (
                        acc
                        for acc in gg_accounts
                        if str(acc.get("accountCode", "")).strip().upper()
                        == account_code
                    ),
                    None,
                )
                if not gg_account:
                    errors.append(
                        {
                            "field": "accountCode",
                            "value": request_payload.accountCode,
                            "message": "No Google Ads account found for accountCode",
                        }
                    )
                else:
                    budgets = get_ggad_budgets([gg_account])
                    valid_budget_ids = {
                        str(budget.get("budgetId", "")).strip()
                        for budget in budgets
                        if str(budget.get("budgetId", "")).strip()
                    }
                    for value in scope_values:
                        if value not in valid_budget_ids:
                            errors.append(
                                {
                                    "field": f"{prefix}.scopeValue",
                                    "value": value,
                                    "message": "scopeValue is not a valid budgetId for accountCode",
                                }
                            )

        if len(errors) > error_count_before:
            continue

        if is_create:
            for scope_value in scope_values:
                create_rows.append(
                    {
                        "accountCode": account_code,
                        "scopeLevel": scope_level,
                        "scopeValue": scope_value,
                        "startDate": start_date,
                        "endDate": end_date,
                        "multiplier": multiplier_value,
                        "note": note_value,
                    }
                )
        else:
            update_rows.append(
                {
                    "id": acceleration_id,
                    "scopeLevel": scope_level,
                    "scopeValue": scope_values[0] if scope_values else None,
                    "startDate": start_date,
                    "endDate": end_date,
                    "multiplier": multiplier_value,
                    "note": note_value,
                    "_note_provided": note_provided,
                }
            )

    if errors:
        raise HTTPException(
            status_code=400,
            detail=_build_validation_error_detail(errors),
        )

    period_date = _resolve_period_date(month_value, year_value)

    if create_rows:
        insert_accelerations(create_rows)
    if update_rows:
        for row in update_rows:
            update_acceleration_by_id(row)

    created_rows = get_accelerations_by_keys(create_rows) if create_rows else []
    updated_rows = (
        get_accelerations_by_ids([row["id"] for row in update_rows])
        if update_rows
        else []
    )

    ui_context = _get_ui_context_with_mutations(
        account_code,
        month=month_value,
        year=year_value,
        period_date=period_date,
        return_new_data=request_payload.returnNewData,
        allow_mutations=False,
        api_name_prefix="spendsphere_v1_ui_accelerations",
    )
    rows = ui_context["rows"]
    budgets = ui_context["budgets"]
    allocations = ui_context["allocations"]
    accelerations = ui_context["accelerations"]
    campaigns = ui_context["campaigns"]
    costs = ui_context["costs"]
    table_data = _build_table_data_payload(
        rows,
        budgets=budgets,
        allocations=allocations,
        campaigns=campaigns,
        costs=costs,
        accelerations=accelerations,
    )
    return {
        "accelerations": _sanitize_acceleration_rows(created_rows + updated_rows),
        "tableData": table_data,
    }


@router.delete(
    "/uis/accelerations",
    summary="Delete UI accelerations by ids",
    description="Soft deletes accelerations by id and returns updated table data.",
)
def delete_ui_accelerations(
    request_payload: UiAccelerationDeleteRequest,
):
    """
    Example request:
        DELETE /api/spendsphere/v1/uis/accelerations
        {
          "ids": [101, 102]
        }

    Example response:
        {
          "deleted": 2,
          "tableData": {
            "grandTotalSpent": 586.09,
            "data": []
          }
        }
    """
    try:
        request_payload = UiAccelerationDeleteRequest.model_validate(
            request_payload, from_attributes=True
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "Invalid payload", "errors": exc.errors()},
        ) from exc

    errors: list[dict[str, object]] = []
    ids = [
        _normalize_optional_str(value)
        for value in request_payload.ids
    ]
    ids = [value for value in ids if value]
    if not ids:
        errors.append(
            {
                "field": "ids",
                "value": request_payload.ids,
                "message": "ids must contain at least one value",
            }
        )

    rows = get_accelerations_by_ids(ids)
    if not rows and ids:
        errors.append(
            {
                "field": "ids",
                "value": ids,
                "message": "No accelerations found for ids",
            }
        )

    found_ids = {
        _normalize_optional_str(row.get("id"))
        for row in rows
        if isinstance(row, dict)
    }
    missing_ids = [value for value in ids if value not in found_ids]
    if missing_ids:
        errors.append(
            {
                "field": "ids",
                "value": missing_ids,
                "message": "Acceleration ids not found",
            }
        )

    account_codes = {
        str(row.get("accountCode", "")).strip().upper()
        for row in rows
        if isinstance(row, dict) and str(row.get("accountCode", "")).strip()
    }
    if len(account_codes) != 1:
        errors.append(
            {
                "field": "ids",
                "value": ids,
                "message": "ids must belong to a single accountCode",
                "accountCodes": sorted(account_codes),
            }
        )
        account_code = None
    else:
        account_code = next(iter(account_codes))

    month_years = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        start_date = _coerce_date(row.get("startDate"))
        if not start_date:
            errors.append(
                {
                    "field": "startDate",
                    "value": row.get("id"),
                    "message": "startDate is required to resolve table data",
                }
            )
            continue
        month_years.add((start_date.month, start_date.year))

    if len(month_years) != 1:
        errors.append(
            {
                "field": "ids",
                "value": ids,
                "message": "ids must belong to a single month/year",
                "monthYears": sorted(month_years),
            }
        )

    if errors:
        raise HTTPException(
            status_code=400,
            detail=_build_validation_error_detail(errors),
        )

    month_value, year_value = next(iter(month_years))
    period_date = _resolve_period_date(month_value, year_value)

    deleted = soft_delete_accelerations_by_ids(ids)

    ui_context = _get_ui_context_with_mutations(
        account_code,
        month=month_value,
        year=year_value,
        period_date=period_date,
        return_new_data=True,
        allow_mutations=False,
        api_name_prefix="spendsphere_v1_ui_accelerations",
    )
    table_data = _build_table_data_payload(
        ui_context["rows"],
        budgets=ui_context["budgets"],
        allocations=ui_context["allocations"],
        campaigns=ui_context["campaigns"],
        costs=ui_context["costs"],
        accelerations=ui_context["accelerations"],
    )

    return {"deleted": deleted, "tableData": table_data}
