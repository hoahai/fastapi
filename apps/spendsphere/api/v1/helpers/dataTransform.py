# api/v1/helpers/dataTransform.py

from decimal import Decimal
from collections import defaultdict
from datetime import datetime, date, time
import calendar
import pytz

from shared.constants import GGADS_MIN_BUDGET_DELTA
from shared.logger import get_logger
from apps.spendsphere.api.v1.helpers.config import get_service_mapping
from shared.tenant import get_timezone

logger = get_logger("Data Transform")


def _is_zzz_name(name: str | None) -> bool:
    return bool(name) and str(name).strip().lower().startswith("zzz.")

# ============================================================
# 1. MASTER BUDGET → AD TYPE MAPPING
# ============================================================


def master_budget_ad_type_mapping(master_budgets: list[dict]) -> list[dict]:
    """
    Aggregate master budgets by (accountCode, adTypeCode).
    """
    grouped = defaultdict(
        lambda: {
            "netAmount": Decimal("0"),
            "services": [],
        }
    )

    service_mapping = get_service_mapping()

    for mb in master_budgets:
        service_id = mb.get("serviceId")
        mapping = service_mapping.get(service_id)
        if not mapping:
            continue

        key = (mb.get("accountCode"), mapping["adTypeCode"])
        net_amount = Decimal(str(mb.get("netAmount", 0)))

        grouped[key]["netAmount"] += net_amount
        grouped[key]["services"].append(
            {
                "serviceId": service_id,
                "serviceName": mapping["serviceName"],
                "netAmount": net_amount,
            }
        )

    return [
        {
            "accountCode": account,
            "adTypeCode": ad_type,
            "netAmount": values["netAmount"],
            "services": values["services"],
        }
        for (account, ad_type), values in grouped.items()
    ]


# ============================================================
# 2. JOIN CAMPAIGNS (FLAT)
# ============================================================


def master_budget_campaigns_join(
    master_budget_data: list[dict],
    campaigns: list[dict],
) -> list[dict]:
    """
    Join master budget data with campaigns by (accountCode, adTypeCode).
    """
    lookup = defaultdict(list)

    for c in campaigns:
        key = (c.get("accountCode"), c.get("adTypeCode"))
        lookup[key].append(c)

    rows: list[dict] = []

    for mb in master_budget_data:
        key = (mb.get("accountCode"), mb.get("adTypeCode"))
        for c in lookup.get(key, []):
            rows.append(
                {
                    **mb,
                    "customerId": c.get("customerId"),
                    "accountName": c.get("accountName"),
                    "campaignId": c.get("campaignId"),
                    "campaignName": c.get("campaignName"),
                    "budgetId": c.get("budgetId"),
                    "campaignStatus": c.get("status"),
                }
            )

    return rows


# ============================================================
# 3. JOIN GOOGLE BUDGETS (FLAT)
# ============================================================


def master_budget_google_budgets_join(
    rows: list[dict],
    budgets: list[dict],
) -> list[dict]:
    """
    Join Google Ads budget metadata by budgetId.
    """
    lookup = {b.get("budgetId"): b for b in budgets}

    return [
        {
            **r,
            "budgetName": lookup.get(r.get("budgetId"), {}).get("budgetName"),
            "budgetStatus": lookup.get(r.get("budgetId"), {}).get("status"),
            "budgetAmount": Decimal(
                str(lookup.get(r.get("budgetId"), {}).get("amount", 0))
            ),
        }
        for r in rows
    ]


# ============================================================
# 4. GROUP BY BUDGET (CORE CHANGE)
# ============================================================


def group_campaigns_by_budget(
    master_budget_data: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    *,
    include_transform_results: bool,
) -> list[dict]:
    cost_lookup: dict[tuple[str | None, str | None], Decimal] = defaultdict(
        lambda: Decimal("0")
    )
    for c in costs:
        key = (c.get("customerId"), c.get("campaignId"))
        cost_lookup[key] += Decimal(str(c.get("cost", 0)))

    budget_lookup = {b.get("budgetId"): b for b in budgets}
    master_lookup = {
        (mb.get("accountCode"), mb.get("adTypeCode")): mb
        for mb in master_budget_data
    }

    grouped: dict[tuple[str, str], dict] = {}

    for c in campaigns:
        account_code = c.get("accountCode")
        ad_type = c.get("adTypeCode")
        master = master_lookup.get((account_code, ad_type))
        if not master:
            continue

        customer_id = c.get("customerId")
        budget_id = c.get("budgetId")
        if not budget_id:
            continue

        group_key = (customer_id, budget_id)

        if group_key not in grouped:
            budget_meta = budget_lookup.get(budget_id, {})
            group = {
                "ggAccountId": customer_id,
                "accountCode": master.get("accountCode"),
                "adTypeCode": master.get("adTypeCode"),
                "netAmount": master.get("netAmount"),
                "budgetId": budget_id,
                "budgetName": budget_meta.get("budgetName"),
                "budgetStatus": budget_meta.get("status"),
                "budgetAmount": Decimal(
                    str(budget_meta.get("amount", 0))
                ),
                "campaigns": [],
                "totalCost": Decimal("0"),
            }
            if include_transform_results:
                group["services"] = master.get("services", [])
                group["campaignNames"] = ""
                group["_campaign_names"] = []  # internal helper
            grouped[group_key] = group

        campaign_id = c.get("campaignId")
        campaign_name = c.get("campaignName")

        grouped[group_key]["campaigns"].append(
            {
                "campaignId": campaign_id,
                "campaignName": campaign_name,
                "status": c.get("status"),
                **(
                    {
                        "cost": cost_lookup.get(
                            (customer_id, campaign_id), Decimal("0")
                        )
                    }
                    if include_transform_results
                    else {}
                ),
            }
        )

        cost_value = cost_lookup.get((customer_id, campaign_id), Decimal("0"))
        grouped[group_key]["totalCost"] += cost_value

        if include_transform_results and campaign_name:
            grouped[group_key]["_campaign_names"].append(campaign_name)

    # finalize campaignNames
    if include_transform_results:
        for b in grouped.values():
            names = b.pop("_campaign_names", [])
            b["campaignNames"] = "\n".join(sorted(set(names)))

    return list(grouped.values())


# ============================================================
# 6. JOIN ALLOCATIONS & ROLLOVERS (BUDGET LEVEL)
# ============================================================


def budget_allocation_join(
    budgets: list[dict],
    allocations: list[dict],
) -> list[dict]:
    lookup = {
        (a.get("accountCode"), a.get("ggBudgetId")): Decimal(
            str(a.get("allocation", 0))
        )
        for a in allocations
    }

    for b in budgets:
        b["allocation"] = lookup.get((b.get("accountCode"), b["budgetId"]), None)

    return budgets


def budget_rollover_join(
    budgets: list[dict],
    rollovers: list[dict],
) -> list[dict]:
    lookup = {
        (r.get("accountCode"), r.get("adTypeCode")): Decimal(
            str(r.get("amount", 0))
        )
        for r in rollovers
    }

    for b in budgets:
        b["rolloverAmount"] = lookup.get(
            (b.get("accountCode"), b.get("adTypeCode")), Decimal("0")
        )

    return budgets


# ============================================================
# 7. JOIN ACTIVE PERIOD
# ============================================================


def _coerce_date(value) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(value.strip(), fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(value.strip()).date()
        except ValueError:
            return None
    return None


def _coerce_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, str) and value.strip():
        value = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def budget_activePeriod_join(
    budgets: list[dict],
    activePeriod: list[dict] | None,
    today: date | None = None,
) -> list[dict]:
    tz = pytz.timezone(get_timezone())
    now = datetime.now(tz)
    if not today:
        today = now.date()

    lookup: dict[str, dict] = {}

    for ap in activePeriod or []:
        account_code = ap.get("accountCode")
        if not account_code:
            continue
        lookup[str(account_code).upper()] = ap

    for b in budgets:
        account_code = str(b.get("accountCode", "")).upper()
        ap = lookup.get(account_code, {})

        start_date_raw = ap.get("startDate")
        end_date_raw = ap.get("endDate")
        start_date = _coerce_date(start_date_raw)
        end_date = _coerce_date(end_date_raw)

        if "isActive" in ap:
            is_active = bool(ap.get("isActive"))
        else:
            if start_date is None:
                start_ok = True
            else:
                start_dt = tz.localize(datetime.combine(start_date, time.min))
                start_ok = now >= start_dt

            if end_date is None:
                end_ok = True
            else:
                end_dt = tz.localize(datetime.combine(end_date, time.max))
                end_ok = now <= end_dt

            is_active = start_ok and end_ok

        b["startDate"] = start_date_raw
        b["endDate"] = end_date_raw
        b["isActive"] = is_active

    return budgets


# ============================================================
# 8. CALCULATE DAILY BUDGET (BUDGET LEVEL)
# ============================================================


def calculate_daily_budget(
    budgets: list[dict],
    today: date | None = None,
) -> list[dict]:
    """
    Calculate daily budget per budgetId.
    """

    if not today:
        tz = pytz.timezone(get_timezone())
        today = datetime.now(tz).date()

    days_in_month = calendar.monthrange(today.year, today.month)[1]
    month_days_left = days_in_month - today.day + 1

    for b in budgets:
        days_left_value = month_days_left
        end_date = _coerce_date(b.get("endDate"))
        if end_date and end_date.year == today.year and end_date.month == today.month:
            days_left_value = (end_date - today).days + 1
            if days_left_value < 0:
                days_left_value = 0
        days_left = Decimal(str(days_left_value))

        total_cost = b.get("totalCost")
        if total_cost is None:
            total_cost = sum(c.get("cost", 0) for c in b.get("campaigns", []))
        net = Decimal(str(b.get("netAmount", 0)))
        rollover = Decimal(str(b.get("rolloverAmount", 0)))
        allocation = b.get("allocation")

        b["totalCost"] = Decimal(str(total_cost)).quantize(Decimal("0.01"))

        # 🔹 Handle missing allocation
        if allocation is None:
            b["remainingBudget"] = None
            b["dailyBudget"] = None
            continue

        allocation_pct = Decimal(str(allocation)) / Decimal("100")
        remaining_base = (net + rollover) * allocation_pct - Decimal(str(total_cost))
        daily_base = remaining_base / days_left if days_left > 0 else Decimal("0")

        accel_multiplier = Decimal(str(b.get("accelerationMultiplier", 100)))
        allocation_pct_accel = allocation_pct * (accel_multiplier / Decimal("100"))

        remaining = (net + rollover) * allocation_pct_accel - Decimal(str(total_cost))
        daily = remaining / days_left if days_left > 0 else Decimal("0")

        b["remainingBudget"] = remaining.quantize(Decimal("0.01"))

        if accel_multiplier != Decimal("100"):
            b["dailyBudgetBase"] = daily_base.quantize(Decimal("0.01"))

        if b.get("isActive") is False:
            b["dailyBudget"] = Decimal("0.00")
        else:
            b["dailyBudget"] = daily.quantize(Decimal("0.01"))

    return budgets


# ============================================================
# 9. APPLY ACCELERATIONS
# ============================================================


def apply_budget_accelerations(
    budgets: list[dict],
    accelerations: list[dict] | None = None,
) -> list[dict]:
    if not accelerations:
        return budgets

    account_accels: dict[str, list[dict]] = defaultdict(list)
    ad_type_accels: dict[tuple[str, str], list[dict]] = defaultdict(list)
    budget_accels: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for accel in accelerations:
        account_code = str(accel.get("accountCode", "")).upper()
        scope_type = str(accel.get("scopeType", "")).upper()
        scope_value = str(accel.get("scopeValue", "")).strip()

        if not account_code or not scope_type:
            continue

        if scope_type == "ACCOUNT":
            account_accels[account_code].append(accel)
        elif scope_type == "AD_TYPE" and scope_value:
            ad_type_accels[(account_code, scope_value)].append(accel)
        elif scope_type == "BUDGET" and scope_value:
            budget_accels[(account_code, scope_value)].append(accel)

    def _accel_sort_key(a: dict) -> tuple[datetime, int]:
        updated = _coerce_datetime(a.get("dateUpdated") or a.get("dateCreated"))
        try:
            accel_id = int(a.get("id") or 0)
        except (TypeError, ValueError):
            accel_id = 0
        return (updated or datetime.min, accel_id)

    def _best(accels: list[dict] | None) -> dict | None:
        if not accels:
            return None
        return max(accels, key=_accel_sort_key)

    for b in budgets:
        account_code = str(b.get("accountCode", "")).upper()
        budget_id = str(b.get("budgetId", "")).strip()
        ad_type = str(b.get("adTypeCode", "")).strip()

        accel = None
        if budget_id:
            accel = _best(budget_accels.get((account_code, budget_id)))
        if accel is None and ad_type:
            accel = _best(ad_type_accels.get((account_code, ad_type)))
        if accel is None:
            accel = _best(account_accels.get(account_code))

        if not accel:
            continue

        multiplier = Decimal(str(accel.get("multiplier", 0)))
        if multiplier <= 0:
            continue

        b["accelerationId"] = accel.get("id")
        b["accelerationMultiplier"] = multiplier

    return budgets


# ============================================================
# 10. UPDATE PAYLOADS
# ============================================================


def generate_update_payloads(data: list[dict]) -> tuple[list[dict], list[dict]]:
    budget_updates: dict[str, list[dict]] = {}
    campaign_updates: dict[str, dict[str, dict]] = {}

    for row in data:
        customer_id = row["ggAccountId"]
        allocation = row.get("allocation")
        daily_budget_raw = row.get("dailyBudget")
        budget_amount_raw = row.get("budgetAmount")

        is_inactive = row.get("isActive") is False

        if allocation is None and not is_inactive:
            continue

        if allocation is None and is_inactive:
            expected_status = "PAUSED"
            daily_budget = None
        else:
            if daily_budget_raw is None:
                continue

            daily_budget = Decimal(daily_budget_raw)

            # Inline expected status logic
            if is_inactive:
                expected_status = "PAUSED"
            else:
                expected_status = (
                    "ENABLED" if daily_budget >= Decimal("0.01") else "PAUSED"
                )

        campaigns = row.get("campaigns", [])
        campaign_names = [
            c.get("campaignName")
            for c in campaigns
            if c.get("campaignName")
        ]
        zzz_campaigns = [
            c
            for c in campaigns
            if _is_zzz_name(c.get("campaignName"))
        ]
        all_campaigns_zzz = bool(campaigns) and len(zzz_campaigns) == len(campaigns)

        # -------------------------
        # Campaign status updates (independent)
        # -------------------------
        for campaign in campaigns:
            if _is_zzz_name(campaign.get("campaignName")):
                continue
            if campaign["status"] != expected_status:
                customer_updates = campaign_updates.setdefault(customer_id, {})
                customer_updates[str(campaign["campaignId"])] = {
                    "campaignId": campaign["campaignId"],
                    "campaignName": campaign.get("campaignName"),
                    "oldStatus": campaign["status"],
                    "newStatus": expected_status,
                    "accountCode": row.get("accountCode"),
                }

        # -------------------------
        # Budget updates (stricter rules)
        # -------------------------
        if all_campaigns_zzz:
            continue
        if daily_budget is None:
            continue
        if budget_amount_raw is None:
            continue

        budget_amount = Decimal(budget_amount_raw)

        # Enforce Google Ads minimum
        amount_to_set = (
            Decimal("0.01") if daily_budget <= Decimal("0") else daily_budget
        )

        # Skip small changes unless targeting 0.00/0.01
        if amount_to_set not in (Decimal("0"), Decimal("0.01")):
            if abs(amount_to_set - budget_amount) <= Decimal(
                str(GGADS_MIN_BUDGET_DELTA)
            ):
                continue

        # Only update when values differ (after min floor)
        if amount_to_set == budget_amount:
            continue

        budget_updates.setdefault(customer_id, []).append(
            {
                "budgetId": row["budgetId"],
                "accountCode": row.get("accountCode"),
                "customerName": row.get("accountName"),
                "campaignNames": campaign_names,
                "currentAmount": float(budget_amount),
                "newAmount": float(amount_to_set),
            }
        )

    budget_payloads = [
        {"customer_id": cid, "updates": updates}
        for cid, updates in budget_updates.items()
    ]

    campaign_payloads = [
        {"customer_id": cid, "updates": list(updates.values())}
        for cid, updates in campaign_updates.items()
    ]

    logger.debug(
        "Payload Data",
        extra={
            "extra_fields": {
                "operation": "generate_update_payloads",
                "budget_payloads": budget_payloads,
                "campaign_payloads": campaign_payloads,
            }
        },
    )

    return budget_payloads, campaign_payloads


# ============================================================
# 10. PIPELINE ORCHESTRATOR
# ============================================================


def _build_budget_rows(
    master_budgets: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    allocations: list[dict],
    rollovers: list[dict],
    accelerations: list[dict] | None = None,
    activePeriod: list[dict] | None = None,
    *,
    today: date | None = None,
    include_transform_results: bool = True,
) -> list[dict]:
    """
    Full Google Ads budget pipeline (BUDGET-CENTRIC).
    """

    step1 = master_budget_ad_type_mapping(master_budgets)
    step2 = group_campaigns_by_budget(
        step1,
        campaigns,
        budgets,
        costs,
        include_transform_results=include_transform_results,
    )
    step3 = budget_allocation_join(step2, allocations)
    step4 = budget_rollover_join(step3, rollovers)
    step5 = budget_activePeriod_join(step4, activePeriod)
    step6 = apply_budget_accelerations(step5, accelerations)
    step7 = calculate_daily_budget(
        step6,
        today=today,
    )

    if not include_transform_results:
        return list(step7)

    # --------------------------------------------------
    # SORT RESULTS (accountCode ASC, adTypeCode DESC)
    # --------------------------------------------------
    result = list(step7)
    result.sort(key=lambda r: (r.get("adTypeCode") or ""), reverse=True)
    result.sort(
        key=lambda r: (r.get("accountCode") is None, r.get("accountCode") or "")
    )

    return result


def transform_google_ads_data(
    master_budgets: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    allocations: list[dict],
    rollovers: list[dict],
    accelerations: list[dict] | None = None,
    activePeriod: list[dict] | None = None,
    *,
    today: date | None = None,
    include_transform_results: bool = True,
) -> list[dict]:
    """
    Full Google Ads budget pipeline (BUDGET-CENTRIC).
    """
    return _build_budget_rows(
        master_budgets,
        campaigns,
        budgets,
        costs,
        allocations,
        rollovers,
        accelerations=accelerations,
        activePeriod=activePeriod,
        today=today,
        include_transform_results=include_transform_results,
    )


def build_update_payloads_from_inputs(
    master_budgets: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    allocations: list[dict],
    rollovers: list[dict],
    accelerations: list[dict] | None = None,
    activePeriod: list[dict] | None = None,
    *,
    include_transform_results: bool = False,
) -> tuple[list[dict], list[dict], list[dict] | None]:
    """
    Build update payloads directly from raw inputs, optionally returning
    the transformed rows for debugging/inspection.
    """
    rows = _build_budget_rows(
        master_budgets,
        campaigns,
        budgets,
        costs,
        allocations,
        rollovers,
        accelerations=accelerations,
        activePeriod=activePeriod,
        include_transform_results=include_transform_results,
    )
    budget_payloads, campaign_payloads = generate_update_payloads(rows)
    if include_transform_results:
        return budget_payloads, campaign_payloads, rows
    return budget_payloads, campaign_payloads, None
