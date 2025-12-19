# functions/dataTransform.py
import pandas as pd
from decimal import Decimal

from functions.constants import SERVICE_MAPPING, ADTYPES

def master_budget_ad_type_mapping(master_budgets: list[dict]) -> list[dict]:
    """
    Add adTypeCode, serviceName, and queryAdType
    to master budget rows using SERVICE_MAPPING and ADTYPES
    """
    mapped = []

    for mb in master_budgets:
        service_id = mb.get("serviceId")
        service_mapping = SERVICE_MAPPING.get(service_id)

        if not service_mapping:
            # Optional: log warning here
            continue

        ad_type_code = service_mapping["adTypeCode"]

        mapped.append({
            **mb,
            "adTypeCode": ad_type_code,
            "serviceName": service_mapping["serviceName"]
        })

    return mapped

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
    step_1 = master_budget_ad_type_mapping(master_budgets)

    # 2. Join master budget → campaigns
    step_2 = master_budget_campaigns_join(step_1, campaigns)

    # 3. Join Google budgets
    step_3 = master_budget_google_budgets_join(step_2, budgets)

    # 4. Attach summed costs (pandas)
    step_4 = campaign_costs_cal(step_3, costs)

    # 5. Attach summed costs (pandas)
    step_5 = master_budget_allocation_join(step_4, allocations)

    # 5. Attach summed costs (pandas)
    final_result = master_budget_rollover_join(step_5, rollovers)

    return final_result




