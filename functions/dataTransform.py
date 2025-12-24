# functions/dataTransform.py
import pandas as pd
from decimal import Decimal
from collections import defaultdict
from datetime import datetime, date
import calendar
import pytz

from functions.constants import SERVICE_MAPPING, ADTYPES, TIMEZONE

def master_budget_ad_type_mapping(master_budgets: list[dict]) -> list[dict]:
    """
    Add adTypeCode and serviceName to master budget rows
    using SERVICE_MAPPING, then pivot by (accountCode, adTypeCode).

    Output:
    - netAmount per (accountCode, adTypeCode)
    - services array with {serviceId, serviceName, netAmount}
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

        account_code = mb.get("accountCode")
        ad_type_code = service_mapping["adTypeCode"]

        net_amount = Decimal(str(mb.get("netAmount", 0)))

        key = (account_code, ad_type_code)

        grouped[key]["netAmount"] += net_amount
        grouped[key]["services"].append({
            "serviceId": service_id,
            "serviceName": service_mapping["serviceName"],
            "netAmount": net_amount,
        })

    result = []

    for (account_code, ad_type_code), values in grouped.items():
        result.append({
            "accountCode": account_code,
            "adTypeCode": ad_type_code,
            "netAmount": values["netAmount"],
            "services": values["services"],
        })

    return result

def master_budget_campaigns_join(
    master_budget_data: list[dict],
    campaigns: list[dict]
) -> list[dict]:
    """
    Join master budget data with Google Ads campaigns
    based on accountCode and queryAdType (channelType).

    One master budget row can map to multiple campaigns.
    """
    # Build lookup: (accountCode, channelType) -> list[campaign]
    campaign_lookup = {}

    for c in campaigns:
        key = (c.get("accountCode"), c.get("adTypeCode"))
        campaign_lookup.setdefault(key, []).append(c)

    joined = []

    for mb in master_budget_data:
        key = (mb.get("accountCode"), mb.get("adTypeCode"))
        matched_campaigns = campaign_lookup.get(key, [])

        for campaign in matched_campaigns:
            joined.append({
                **mb,
                "customerId": campaign.get("customerId"),
                "campaignId": campaign.get("campaignId"),
                "campaignName": campaign.get("campaignName"),
                "budgetId": campaign.get("budgetId"),
                "campaignStatus": campaign.get("status"),
            })

    return joined

def master_budget_google_budgets_join(
    master_campaign_data: list[dict],
    budgets: list[dict]
) -> list[dict]:
    """
    Join Google Ads budget details to master-campaign data
    based on budgetId.
    """
    # Build lookup: budgetId -> budget
    budget_lookup = {
        b.get("budgetId"): b
        for b in budgets
    }

    joined = []

    for row in master_campaign_data:
        budget = budget_lookup.get(row.get("budgetId"))

        joined.append({
            **row,
            "budgetName": budget.get("budgetName") if budget else None,
            "budgetStatus": budget.get("status") if budget else None,
            "budgetAmount": budget.get("amount") if budget else None,
        })

    return joined

def campaign_costs_cal(
    master_campaign_data: list[dict],
    costs: list[dict]
) -> list[dict]:
    """
    Join summed Google Ads costs to master-campaign data by campaignId.
    """
    if not master_campaign_data:
        return []

    # Convert to DataFrames
    df_master = pd.DataFrame(master_campaign_data)
    df_costs = pd.DataFrame(costs)

    if df_costs.empty:
        df_master["totalCost"] = Decimal("0")
        return df_master.to_dict(orient="records")

    # Aggregate costs by campaignId
    df_costs_agg = (
        df_costs
        .groupby("campaignId", as_index=False)["cost"]
        .sum()
        .rename(columns={"cost": "totalCost"})
    )

    # Join back to master
    df_joined = df_master.merge(
        df_costs_agg,
        on="campaignId",
        how="left"
    )

    # Handle NaN → Decimal(0)
    df_joined["totalCost"] = (
        df_joined["totalCost"]
        .fillna(0)
        .apply(lambda x: Decimal(str(x)))
    )

    return df_joined.to_dict(orient="records")

def master_budget_allocation_join(
    master_gg_budget_data: list[dict],
    allocations: list[dict],
) -> list[dict]:
    """
    Join allocation data to master Google budget data
    using (accountCode, budgetId ↔ ggBudgetId).

    Adds `allocation` field to each master row.
    """

    # Build lookup: (accountCode, ggBudgetId) -> allocation
    allocation_lookup = {
        (a.get("ggBudgetId")): a.get("allocation")
        for a in allocations
    }

    joined = []

    for row in master_gg_budget_data:
        key = (row.get("budgetId"))
        allocation = allocation_lookup.get(key, Decimal("0"))

        joined.append({
            **row,
            "allocation": allocation,
        })

    return joined

def master_budget_rollover_join(
    master_gg_budget_data: list[dict],
    rollover_breakdown: list[dict],
) -> list[dict]:
    """
    Join rollover breakdown data to master Google budget data
    using (accountCode, adTypeCode).

    Adds `rolloverAmount` field to each master row.
    """

    # Build lookup: (accountCode, adTypeCode) -> rollover amount
    rollover_lookup = {
        (r.get("adTypeCode")): r.get("amount")
        for r in rollover_breakdown
    }

    joined = []

    for row in master_gg_budget_data:
        key = (row.get("adTypeCode"))
        rollover_amount = rollover_lookup.get(key, Decimal("0"))

        joined.append({
            **row,
            "rolloverAmount": rollover_amount,
        })

    return joined

def calculate_daily_budget(
    data: list[dict],
    today: date | None = None
) -> list[dict]:
    """
    Calculate daily budget for each row.

    Formula:
    dailyBudget =
    (
        (totalNetAmount + rolloverAmount) * allocation
        - totalCost
    ) / daysLeftInMonth
    """

    # ---- 0. Determine "today" using configured TIMEZONE (string → pytz)
    if not today:
        tz = pytz.timezone(TIMEZONE)
        today = datetime.now(tz).date()

    # ---- 1. Days left in month (including today)
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    days_left = Decimal(str(days_in_month - today.day + 1))

    # ---- 2. Group total netAmount by accountCode + adTypeCode
    total_net_by_group = defaultdict(Decimal)

    for row in data:
        key = (row["accountCode"], row["adTypeCode"])
        total_net_by_group[key] += Decimal(str(row["netAmount"]))

    # ---- 3. Calculate daily budget per row
    result = []

    for row in data:
        key = (row["accountCode"], row["adTypeCode"])

        total_net = total_net_by_group[key]
        rollover = Decimal(str(row.get("rolloverAmount", 0)))
        allocation_pct = Decimal(str(row.get("allocation", 0))) / Decimal("100")
        cost = Decimal(str(row.get("totalCost", 0)))

        remaining_budget = (total_net + rollover) * allocation_pct - cost

        daily_budget = (
            remaining_budget / days_left
            if days_left > 0
            else Decimal("0")
        )

        result.append({
            **row,
            "dailyBudget": daily_budget.quantize(Decimal("0.01")),
        })

    return result


def transform_google_ads_budget_pipeline(
    master_budgets: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    allocations: list[dict],
    rollovers: list[dict],
) -> list[dict]:
    """
    Full Google Ads budget vs spend transformation pipeline.
    """

    # 1. Map master budget → adTypeCode + queryAdType
    resuts = master_budget_ad_type_mapping(master_budgets)

    # 2. Join master budget → campaigns
    resuts = master_budget_campaigns_join(resuts, campaigns)

    # # 3. Join Google budgets
    resuts = master_budget_google_budgets_join(resuts, budgets)

    # # 4. Attach summed costs (pandas)
    resuts = campaign_costs_cal(resuts, costs)

    # # 5. Attach summed costs (pandas)
    resuts = master_budget_allocation_join(resuts, allocations)

    # # 6. Attach summed costs (pandas)
    resuts = master_budget_rollover_join(resuts, rollovers)

    # # 7. Cal Daily Budget
    resuts = calculate_daily_budget(resuts)

    return resuts




