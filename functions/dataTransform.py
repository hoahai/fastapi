# functions/dataTransform.py

from decimal import Decimal
from collections import defaultdict
from datetime import datetime, date
import calendar
import pytz
import pandas as pd

from functions.constants import SERVICE_MAPPING, TIMEZONE
from functions.logger import get_logger

logger = get_logger("Data Transform")

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

    for mb in master_budgets:
        service_id = mb.get("serviceId")
        service_mapping = SERVICE_MAPPING.get(service_id)
        if not service_mapping:
            continue

        key = (mb.get("accountCode"), service_mapping["adTypeCode"])
        net_amount = Decimal(str(mb.get("netAmount", 0)))

        grouped[key]["netAmount"] += net_amount
        grouped[key]["services"].append(
            {
                "serviceId": service_id,
                "serviceName": service_mapping["serviceName"],
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
# 4. JOIN COSTS (CAMPAIGN LEVEL)
# ============================================================


def campaign_costs_join(
    rows: list[dict],
    costs: list[dict],
) -> list[dict]:
    """
    Sum costs per campaignId and join to rows.
    """
    if not rows:
        return []

    df_rows = pd.DataFrame(rows)
    df_costs = pd.DataFrame(costs)

    if df_costs.empty:
        df_rows["cost"] = Decimal("0")
        return df_rows.to_dict(orient="records")

    df_costs = (
        df_costs.groupby("campaignId", as_index=False)["cost"]
        .sum()
        .rename(columns={"cost": "cost"})
    )

    df = df_rows.merge(df_costs, on="campaignId", how="left")
    df["cost"] = df["cost"].fillna(0).apply(lambda x: Decimal(str(x)))

    return df.to_dict(orient="records")


# ============================================================
# 5. GROUP BY BUDGET (CORE CHANGE)
# ============================================================


def group_campaigns_by_budget(rows: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}

    for r in rows:
        budget_id = r.get("budgetId")
        if not budget_id:
            continue

        if budget_id not in grouped:
            grouped[budget_id] = {
                "ggAccountId": r.get("customerId"),
                "accountCode": r.get("accountCode"),
                "adTypeCode": r.get("adTypeCode"),
                "services": r.get("services", []),
                "netAmount": r.get("netAmount"),
                "budgetId": budget_id,
                "budgetName": r.get("budgetName"),
                "budgetStatus": r.get("budgetStatus"),
                "budgetAmount": r.get("budgetAmount", Decimal("0")),
                "campaigns": [],
                "campaignNames": "",
                "_campaign_names": [],  # internal helper
            }

        campaign_name = r.get("campaignName")

        grouped[budget_id]["campaigns"].append(
            {
                "campaignId": r.get("campaignId"),
                "campaignName": campaign_name,
                "status": r.get("campaignStatus"),
                "cost": r.get("cost", Decimal("0")),
            }
        )

        if campaign_name:
            grouped[budget_id]["_campaign_names"].append(campaign_name)

    # finalize campaignNames
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
        a.get("ggBudgetId"): Decimal(str(a.get("allocation", 0))) for a in allocations
    }

    for b in budgets:
        b["allocation"] = lookup.get(b["budgetId"], None)

    return budgets


def budget_rollover_join(
    budgets: list[dict],
    rollovers: list[dict],
) -> list[dict]:
    lookup = {r.get("adTypeCode"): Decimal(str(r.get("amount", 0))) for r in rollovers}

    for b in budgets:
        b["rolloverAmount"] = lookup.get(b["adTypeCode"], Decimal("0"))

    return budgets


# ============================================================
# 7. CALCULATE DAILY BUDGET (BUDGET LEVEL)
# ============================================================


def calculate_daily_budget(
    budgets: list[dict],
    today: date | None = None,
) -> list[dict]:
    """
    Calculate daily budget per budgetId.
    """

    if not today:
        tz = pytz.timezone(TIMEZONE)
        today = datetime.now(tz).date()

    days_in_month = calendar.monthrange(today.year, today.month)[1]
    days_left = Decimal(str(days_in_month - today.day + 1))

    for b in budgets:
        total_cost = sum(c["cost"] for c in b["campaigns"])
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

        remaining = (net + rollover) * allocation_pct - total_cost
        daily = remaining / days_left if days_left > 0 else Decimal("0")

        b["remainingBudget"] = remaining.quantize(Decimal("0.01"))
        b["dailyBudget"] = daily.quantize(Decimal("0.01"))

    return budgets


# ============================================================
# 8. UPDATE PAYLOADS
# ============================================================


def generate_update_payloads(data: list[dict]) -> tuple[list[dict], list[dict]]:
    budget_updates: dict[str, list[dict]] = {}
    campaign_updates: dict[str, list[dict]] = {}

    for row in data:
        customer_id = row["ggAccountId"]

        daily_budget_raw = row.get("dailyBudget")
        budget_amount_raw = row.get("budgetAmount")

        # dailyBudget is required for BOTH
        if daily_budget_raw is None:
            continue

        daily_budget = Decimal(daily_budget_raw)

        # Inline expected status logic
        expected_status = "ENABLED" if daily_budget >= Decimal("0.01") else "PAUSED"

        campaigns = row.get("campaigns", [])

        # -------------------------
        # Campaign status updates (independent)
        # -------------------------
        for campaign in campaigns:
            if campaign["status"] != expected_status:
                campaign_updates.setdefault(customer_id, []).append(
                    {"campaignId": campaign["campaignId"], "status": expected_status}
                )

        # -------------------------
        # Budget updates (stricter rules)
        # -------------------------
        if budget_amount_raw is None:
            continue

        budget_amount = Decimal(budget_amount_raw)

        # Only update when values differ
        if daily_budget == budget_amount:
            continue

        # Enforce Google Ads minimum
        amount_to_set = (
            Decimal("0.01") if daily_budget <= Decimal("0") else daily_budget
        )

        budget_updates.setdefault(customer_id, []).append(
            {
                "budgetId": row["budgetId"],
                "currentAmount": float(budget_amount),
                "newAmount": float(amount_to_set),
            }
        )

    budget_payloads = [
        {"customer_id": cid, "updates": updates}
        for cid, updates in budget_updates.items()
    ]

    campaign_payloads = [
        {"customer_id": cid, "updates": updates}
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
# 8. PIPELINE ORCHESTRATOR
# ============================================================


def transform_google_ads_data(
    master_budgets: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    allocations: list[dict],
    rollovers: list[dict],
) -> list[dict]:
    """
    Full Google Ads budget pipeline (BUDGET-CENTRIC).
    """

    step1 = master_budget_ad_type_mapping(master_budgets)
    step2 = master_budget_campaigns_join(step1, campaigns)
    step3 = master_budget_google_budgets_join(step2, budgets)
    step4 = campaign_costs_join(step3, costs)

    step5 = group_campaigns_by_budget(step4)
    step6 = budget_allocation_join(step5, allocations)
    step7 = budget_rollover_join(step6, rollovers)
    step8 = calculate_daily_budget(step7)

    # --------------------------------------------------
    # SORT RESULT USING PANDAS
    # --------------------------------------------------
    df = pd.DataFrame(step8)

    df_sorted = df.sort_values(
        by=["accountCode", "adTypeCode"], ascending=[True, False], na_position="last"
    )

    result = df_sorted.to_dict(orient="records")

    return result
