# functions/ggAd.py

import re
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from functions.utils import get_current_period, run_parallel_accounts
from functions.constants import ADTYPES


def get_client() -> GoogleAdsClient:
    """
    Create and return a Google Ads client using service account auth
    """
    return GoogleAdsClient.load_from_storage("secrets/google-ads.yaml")


def get_mcc_accounts() -> list[dict]:
    """
    Get all non-hidden, non-canceled Google Ads accounts under the MCC
    """
    client = get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          customer_client.id,
          customer_client.descriptive_name,
          customer_client.status
        FROM customer_client
        WHERE customer_client.hidden != TRUE
          AND customer_client.status = 'ENABLED'
    """

    results: list[dict] = []

    try:
        mcc_id = client.login_customer_id

        response = ga_service.search(
            customer_id=mcc_id,
            query=query
        )

        for row in response:
            results.append({
                "id": str(row.customer_client.id),
                "name": row.customer_client.descriptive_name,
                "status": row.customer_client.status.name,
            })

    except GoogleAdsException as ex:
        raise RuntimeError(
            f"Google Ads API error: {ex.failure}"
        )

    return results


def get_ggad_accounts() -> list[dict]:
    """
    Return normalized Google Ads accounts that follow naming convention:
    [zzz.][AccountCode]_[Account Name]
    """
    raw_accounts = get_mcc_accounts()

    if not raw_accounts:
        return []

    results: list[dict] = []

    ACCOUNT_NAME_PATTERN = re.compile(
        r"^(?:zzz\.)?(?P<code>[A-Za-z0-9]+)_(?P<name>.+)$"
    )

    for acc in raw_accounts:
        descriptive_name = acc.get("name", "").strip()

        # Must match naming convention
        match = ACCOUNT_NAME_PATTERN.match(descriptive_name)
        if not match:
            continue

        # Exclude zzz.* accounts explicitly
        if descriptive_name.lower().startswith("zzz."):
            continue

        results.append({
            "id": acc.get("id"),
            "descriptiveName": descriptive_name,
            "accountCode": match.group("code").strip(),
            "accountName": match.group("name").strip(),
        })

    return results


def get_ggad_budget(customer_id: str) -> list[dict]:
    """
    Get all non-removed campaign budgets for a single Google Ads account
    """
    client = get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          campaign_budget.id,
          campaign_budget.name,
          campaign_budget.explicitly_shared,
          campaign_budget.status,
          campaign_budget.amount_micros
        FROM campaign_budget
        WHERE campaign_budget.status != 'REMOVED'
    """

    results: list[dict] = []

    response = ga_service.search(
        customer_id=customer_id,
        query=query
    )

    for row in response:
        budget = row.campaign_budget

        results.append({
            "budgetId": str(budget.id),
            "budgetName": budget.name,
            "explicitlyShared": budget.explicitly_shared,
            "status": budget.status.name,
            "amount": budget.amount_micros / 1_000_000 if budget.amount_micros else 0,
        })

    return results


def get_ggad_budgets(accounts: list[dict]) -> list[dict]:
    """
    Get campaign budgets for multiple Google Ads accounts
    """

    def per_account_func(account: dict) -> list[dict]:
        budgets = get_ggad_budget(account["id"])

        return [
            {
                "customerId": account["id"],
                "accountCode": account.get("accountCode"),
                "accountName": account.get("accountName"),
                **b,
            }
            for b in budgets
        ]

    return run_parallel_accounts(
        accounts=accounts,
        per_account_func=per_account_func
    )


def get_ggad_campaign(customer_id: str) -> list[dict]:
    """
    Get campaigns for a single Google Ads account
    """
    client = get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          campaign.advertising_channel_type,
          campaign.id,
          campaign.name,
          campaign_budget.id,
          campaign.status
        FROM campaign
        WHERE campaign.experiment_type = 'BASE'
        ORDER BY
          campaign.advertising_channel_type ASC,
          campaign.status ASC,
          campaign.name ASC
    """

    results: list[dict] = []

    try:
        response = ga_service.search(
            customer_id=customer_id,
            query=query
        )

        for row in response:
            results.append({
                "campaignId": str(row.campaign.id),
                "campaignName": row.campaign.name,
                "status": row.campaign.status.name,
                "channelType": row.campaign.advertising_channel_type.name,
                "budgetId": str(row.campaign_budget.id)
                if row.campaign_budget.id else None,
            })

    except GoogleAdsException as ex:
        raise RuntimeError(f"Google Ads API error: {ex.failure}")

    return results


def get_ggad_campaigns(accounts: list[dict]) -> list[dict]:
    """
    Get Google Ads campaigns for multiple accounts,
    filtered by naming convention:
    [zzz.][accountCode]_[adTypeCode]_[Name]
    """

    ad_type_pattern = "|".join(map(re.escape, ADTYPES.keys()))

    def per_account_func(account: dict) -> list[dict]:
        campaigns = get_ggad_campaign(account["id"])
        account_code = account.get("accountCode")

        pattern = re.compile(
            rf"^(zzz\.)?{re.escape(account_code)}_({ad_type_pattern})_.+",
            re.IGNORECASE,
        )

        filtered: list[dict] = []

        for c in campaigns:
            name = c.get("campaignName", "")

            match = pattern.match(name)
            if not match:
                continue

            # Exclude zzz.* campaigns explicitly
            if name.lower().startswith("zzz."):
                continue

            filtered.append({
                "customerId": account["id"],
                "accountCode": account_code,
                "accountName": account.get("accountName"),
                "adTypeCode": match.group(2).upper(),
                **c,
            })

        return filtered

    return run_parallel_accounts(
        accounts=accounts,
        per_account_func=per_account_func
    )


def get_ggad_spent(customer_id: str) -> list[dict]:
    """
    Get campaign spend for a single Google Ads account
    for the current period
    """
    period = get_current_period()

    client = get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
          segments.year,
          segments.month,
          campaign.id,
          metrics.cost_micros,
          campaign_budget.id
        FROM campaign
        WHERE campaign.status != 'REMOVED'
          AND segments.date >= '{period["start_date"]}'
          AND segments.date <= '{period["end_date"]}'
        ORDER BY
          segments.year DESC,
          segments.month DESC,
          campaign.advertising_channel_type ASC
    """

    results: list[dict] = []

    try:
        response = ga_service.search(
            customer_id=customer_id,
            query=query
        )

        for row in response:
            results.append({
                "year": row.segments.year,
                "month": row.segments.month,
                "campaignId": str(row.campaign.id),
                "budgetId": str(row.campaign_budget.id)
                if row.campaign_budget.id else None,
                "cost": row.metrics.cost_micros / 1_000_000,
            })

    except GoogleAdsException as ex:
        raise RuntimeError(f"Google Ads API error: {ex.failure}")

    return results


def get_ggad_spents(accounts: list[dict]) -> list[dict]:
    """
    Get campaign spend for multiple Google Ads accounts
    for the current period (parallelized)
    """

    def per_account_func(account: dict) -> list[dict]:
        spents = get_ggad_spent(account["id"])

        return [
            {
                "customerId": account["id"],
                "accountCode": account.get("accountCode"),
                "accountName": account.get("accountName"),
                **s,
            }
            for s in spents
        ]

    return run_parallel_accounts(
        accounts=accounts,
        per_account_func=per_account_func
    )
