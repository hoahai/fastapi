# test_google_ads.py
from functions.ggAd import get_ggad_accounts, get_ggad_spents, get_ggad_budgets

if __name__ == "__main__":
    # 1️⃣ Get Google Ads accounts under MCC
    accounts = get_ggad_accounts()
    print(f"Found {len(accounts)} Google Ads accounts")
    # Filter by accountCode
    accounts = [
        acc for acc in accounts
        if acc.get("accountCode") == "AUC"
    ]

    # 2️⃣ Get budgets for those accounts
    budgets = get_ggad_budgets(accounts)

    print("\nCampaign Budgets:")
    for b in budgets:
        print(b)
