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

        key = (
            mb.get("accountCode"),
            service_mapping["adTypeCode"],
        )

        net_amount = Decimal(str(mb.get("netAmount", 0)))

        grouped[key]["netAmount"] += net_amount
        grouped[key]["services"].append({
            "serviceId": service_id,
            "serviceName": service_mapping["serviceName"],
            "netAmount": net_amount,
        })

    return [
        {
            "accountCode": account_code,
            "adTypeCode": ad_type_code,
            "netAmount": values["netAmount"],
            "services": values["services"],
        }
        for (account_code, ad_type_code), values in grouped.items()
    ]


def master_budget_campaigns_join(
    master_budget_data: list[dict],
    campaigns: list[dict]
) -> list[dict]:
    """
    Join master budget data with Google Ads campaigns
    based on (accountCode, adTypeCode).
    """
    campaign_lookup: dict[tuple, list[dict]] = {}

    for c in campaigns:
        key = (c.get("accountCode"), c.get("adTypeCode"))
        campaign_lookup.setdefault(key, []).append(c)

    joined: list[dict] = []

    for mb in master_budget_data:
        key = (mb.get("accountCode"), mb.get("adTypeCode"))
        for campaign in campaign_lookup.get(key, []):
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
    budget_lookup = {
        b.get("budgetId"): b
        for b in budgets
    }

    return [
        {
            **row,
            "budgetName": budget_lookup.get(row.get("budgetId"), {}).get("budgetName"),
            "budgetStatus": budget_lookup.get(row.get("budgetId"), {}).get("status"),
            "budgetAmount": budget_lookup.get(row.get("budgetId"), {}).get("amount"),
        }
        for row in master_campaign_data
    ]


def campaign_costs_cal(
    master_campaign_data: list[dict],
    costs: list[dict]
) -> list[dict]:
    """
    Join summed Google Ads costs to master-campaign data by campaignId.
    """
    if not master_campaign_data:
        return []

    df_master = pd.DataFrame(master_campaign_data)
    df_costs = pd.DataFrame(costs)

    if df_costs.empty:
        df_master["totalCost"] = Decimal("0")
        return df_master.to_dict(orient="records")

    df_costs_agg = (
        df_costs
        .groupby("campaignId", as_index=False)["cost"]
        .sum()
        .rename(columns={"cost": "totalCost"})
    )

    df_joined = df_master.merge(
        df_costs_agg,
        on="campaignId",
        how="left"
    )

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
    using budgetId ↔ ggBudgetId.
    """
    allocation_lookup = {
        a.get("ggBudgetId"): a.get("allocation")
        for a in allocations
    }

    return [
        {
            **row,
            "allocation": allocation_lookup.get(
                row.get("budgetId"),
                Decimal("0"),
            ),
        }
        for row in master_gg_budget_data
    ]


def master_budget_rollover_join(
    master_gg_budget_data: list[dict],
    rollover_breakdown: list[dict],
) -> list[dict]:
    """
    Join rollover breakdown data to master Google budget data
    using adTypeCode.
    """
    rollover_lookup = {
        r.get("adTypeCode"): r.get("amount")
        for r in rollover_breakdown
    }

    return [
        {
            **row,
            "rolloverAmount": rollover_lookup.get(
                row.get("adTypeCode"),
                Decimal("0"),
            ),
        }
        for row in master_gg_budget_data
    ]


def calculate_daily_budget(
    data: list[dict],
    today: date | None = None
) -> list[dict]:
    """
    Calculate daily budget for each row.
    """
    if not today:
        tz = pytz.timezone(TIMEZONE)
        today = datetime.now(tz).date()

    days_in_month = calendar.monthrange(today.year, today.month)[1]
    days_left = Decimal(str(days_in_month - today.day + 1))

    total_net_by_group = defaultdict(Decimal)

    for row in data:
        key = (row["accountCode"], row["adTypeCode"])
        total_net_by_group[key] += Decimal(str(row["netAmount"]))

    result: list[dict] = []

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
    results = master_budget_ad_type_mapping(master_budgets)
    results = master_budget_campaigns_join(results, campaigns)
    results = master_budget_google_budgets_join(results, budgets)
    results = campaign_costs_cal(results, costs)
    results = master_budget_allocation_join(results, allocations)
    results = master_budget_rollover_join(results, rollovers)
    results = calculate_daily_budget(results)

    return results
