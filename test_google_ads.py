from functions.ggAd import *
from functions.db_queries import *
from functions.dataTransform import *
import json
from decimal import Decimal
import pandas as pd

def json_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

if __name__ == "__main__":
    # Fetch master budgets (MySQL)
    master_budgets = get_masterbudgets('TAC')
    allocations = get_allocations('TAC')
    rollovers = get_rollbreakdowns('TAC')

    # Get Google Ads accounts under MCC
    accounts = get_ggad_accounts()
    accounts = [
        acc for acc in accounts
        if acc.get("accountCode") == "TAC"
    ]

    # Fetch Google Ads data
    campaigns = get_ggad_campaigns(accounts)
    budgets = get_ggad_budgets(accounts)
    costs = get_ggad_spents(accounts)

    # results = master_budget_ad_type_mapping(master_budgets)
    # results = master_budget_campaigns_join(master_budget_data = results, campaigns=campaigns)
    # results = master_budget_google_budgets_join(master_campaign_data = results, budgets=budgets)
    # results = campaign_costs_cal(master_campaign_data = results, costs=costs)

    # Run full transform
    results = transform_google_ads_budget_pipeline(
        master_budgets=master_budgets,
        campaigns=campaigns,
        budgets=budgets,
        costs=costs,
        allocations=allocations,
        rollovers=rollovers
    )

    print(f"\nFinal Result Rows: {len(results)}")
    # for r in results:
    #     print(r)
    # with open("results.json", "w", encoding="utf-8") as f:
    #   json.dump(
    #       results,
    #       f,
    #       indent=2,
    #       default=json_serializer
    #   )

    df = pd.DataFrame(results)

    df.to_excel(
        "results.xlsx",
        index=False,
        engine="openpyxl"
    )

