# functions/dataTransform.py

from decimal import Decimal
from collections import defaultdict
from datetime import datetime, date
import calendar
import pytz
import pandas as pd

from functions.constants import SERVICE_MAPPING, TIMEZONE


# ============================================================
# 1. MASTER BUDGET → AD TYPE MAPPING
# ============================================================

def master_budget_ad_type_mapping(master_budgets: list[dict]) -> list[dict]:
    """
    Aggregate master budgets by (accountCode, adTypeCode).
    """
    grouped = defaultdict(lambda: {
        "netAmount": Decimal("0"),
        "services": [],
    })

    for mb in master_budgets:
        service_id = mb.get("serviceId")
        service_mapping = SERVICE_MAPPING.get(service_id)
        if not service_mapping:
            continue

        key = (mb.get("accountCode"), service_mapping["adTypeCode"])
        net_amount = Decimal(str(mb.get("netAmount", 0)))

        grouped[key]["netAmount"] += net_amount
        grouped[key]["services"].append({
            "serviceId": service_id,
            "serviceName": service_mapping["serviceName"],
            "netAmount": net_amount,
        })

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
            rows.append({
                **mb,
                "customerId": c.get("customerId"),
                "campaignId": c.get("campaignId"),
                "campaignName": c.get("campaignName"),
                "budgetId": c.get("budgetId"),
                "campaignStatus": c.get("status"),
            })

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
            "budgetAmount": Decimal(str(
                lookup.get(r.get("budgetId"), {}).get("amount", 0)
            )),
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
        df_costs
        .groupby("campaignId", as_index=False)["cost"]
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
                "_campaign_names": [],   # internal helper
            }

        campaign_name = r.get("campaignName")

        grouped[budget_id]["campaigns"].append({
            "campaignId": r.get("campaignId"),
            "campaignName": campaign_name,
            "status": r.get("campaignStatus"),
            "cost": r.get("cost", Decimal("0")),
        })

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
        a.get("ggBudgetId"): Decimal(str(a.get("allocation", 0)))
        for a in allocations
    }

    for b in budgets:
        b["allocation"] = lookup.get(b["budgetId"], Decimal("100"))

    return budgets


def budget_rollover_join(
    budgets: list[dict],
    rollovers: list[dict],
) -> list[dict]:
    lookup = {
        r.get("adTypeCode"): Decimal(str(r.get("amount", 0)))
        for r in rollovers
    }

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
        allocation_pct = Decimal(str(b.get("allocation", 100))) / Decimal("100")

        remaining = (net + rollover) * allocation_pct - total_cost
        daily = remaining / days_left if days_left > 0 else Decimal("0")

        b["totalCost"] = total_cost.quantize(Decimal("0.01"))
        b["remainingBudget"] = remaining.quantize(Decimal("0.01"))
        b["dailyBudget"] = daily.quantize(Decimal("0.01"))

    return budgets


# ============================================================
# 8. PIPELINE ORCHESTRATOR
# ============================================================

def transform_google_ads_budget_pipeline(
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

    rows = master_budget_ad_type_mapping(master_budgets)
    rows = master_budget_campaigns_join(rows, campaigns)
    rows = master_budget_google_budgets_join(rows, budgets)
    rows = campaign_costs_join(rows, costs)

    budgets_grouped = group_campaigns_by_budget(rows)
    budgets_grouped = budget_allocation_join(budgets_grouped, allocations)
    budgets_grouped = budget_rollover_join(budgets_grouped, rollovers)
    budgets_grouped = calculate_daily_budget(budgets_grouped)

    # --------------------------------------------------
    # SORT RESULT USING PANDAS
    # --------------------------------------------------
    df = pd.DataFrame(budgets_grouped)

    df_sorted = df.sort_values(
        by=["accountCode", "adTypeCode"],
        ascending=[True, False],
        na_position="last"
    )

    result = df_sorted.to_dict(orient="records")

    return result
