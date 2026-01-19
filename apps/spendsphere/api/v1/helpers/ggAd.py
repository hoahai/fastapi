# api/v1/helpers/ggAd.py

import re
from decimal import Decimal, ROUND_HALF_UP
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.field_mask_pb2 import FieldMask
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v22.errors.types.errors import GoogleAdsFailure

from shared.utils import (
    get_current_period,
    run_parallel_flatten,
)
from shared.tenant import get_env, TenantConfigError
from pathlib import Path
from shared.constants import (
    GGADS_MAX_PAUSED_CAMPAIGNS,
    GGADS_MIN_BUDGET,
    GGADS_MAX_BUDGET_MULTIPLIER,
    GGADS_ALLOWED_CAMPAIGN_STATUSES,
)
from shared.logger import get_logger
from apps.spendsphere.api.v1.helpers.config import get_adtypes

logger = get_logger("Google Ads")


# =====================
# CLIENT
# =====================


def get_client() -> GoogleAdsClient:
    """
    Create and return a Google Ads client using tenant config.
    """
    developer_token = get_env("developer_token")
    login_customer_id = get_env("login_customer_id")
    json_key_file_path = get_env("json_key_file_path")
    use_proto_plus = get_env("use_proto_plus", "true")

    missing = [
        key
        for key, value in (
            ("developer_token", developer_token),
            ("login_customer_id", login_customer_id),
            ("json_key_file_path", json_key_file_path),
        )
        if not value
    ]
    if missing:
        raise TenantConfigError(
            "Missing Google Ads tenant config keys: " + ", ".join(missing)
        )

    key_path = Path(str(json_key_file_path))
    if not key_path.is_file():
        raise TenantConfigError(
            f"Google Ads json_key_file_path not found: {key_path}"
        )

    config = {
        "developer_token": developer_token,
        "login_customer_id": login_customer_id,
        "json_key_file_path": str(key_path),
        "use_proto_plus": str(use_proto_plus).lower()
        in {"1", "true", "yes", "on"},
    }
    return GoogleAdsClient.load_from_dict(config)


# =====================
# ACCOUNTS
# =====================


def get_mcc_accounts() -> list[dict]:
    """
    Get all non-hidden, ENABLED Google Ads accounts under the MCC.
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
        response = ga_service.search(
            customer_id=client.login_customer_id,
            query=query,
        )

        for row in response:
            results.append(
                {
                    "id": str(row.customer_client.id),
                    "name": row.customer_client.descriptive_name,
                    "status": row.customer_client.status.name,
                }
            )

    except GoogleAdsException as ex:
        logger.error(
            "Failed to fetch MCC accounts",
            extra={"extra_fields": {"error": str(ex)}},
        )
        raise RuntimeError(f"Google Ads API error: {ex.failure}") from ex

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

    account_name_pattern = re.compile(
        r"^(?:zzz\.)?(?P<code>[A-Za-z0-9]+)_(?P<name>.+)$"
    )

    for acc in raw_accounts:
        descriptive_name = acc.get("name", "").strip()

        match = account_name_pattern.match(descriptive_name)
        if not match:
            continue

        if descriptive_name.lower().startswith("zzz."):
            continue

        results.append(
            {
                "id": acc.get("id"),
                "descriptiveName": descriptive_name,
                "accountCode": match.group("code").strip(),
                "accountName": match.group("name").strip(),
            }
        )

    return results


# =====================
# BUDGETS (READ)
# =====================


def get_ggad_budget(customer_id: str) -> list[dict]:
    """
    Get all non-removed campaign budgets for a single Google Ads account.
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

    try:
        response = ga_service.search(customer_id=customer_id, query=query)

        for row in response:
            b = row.campaign_budget
            results.append(
                {
                    "budgetId": str(b.id),
                    "budgetName": b.name,
                    "explicitlyShared": b.explicitly_shared,
                    "status": b.status.name,
                    "amount": b.amount_micros / 1_000_000 if b.amount_micros else 0,
                }
            )

    except GoogleAdsException as ex:
        raise RuntimeError(f"Google Ads API error: {ex.failure}") from ex

    return results


def get_ggad_budgets(accounts: list[dict]) -> list[dict]:
    """
    Get campaign budgets for multiple Google Ads accounts (parallelized).
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

    tasks = [(per_account_func, (account,)) for account in accounts]

    return run_parallel_flatten(tasks=tasks, api_name="google_ads")


# =====================
# CAMPAIGNS (READ)
# =====================


def get_ggad_campaign(customer_id: str) -> list[dict]:
    """
    Get campaigns for a single Google Ads account.
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
        response = ga_service.search(customer_id=customer_id, query=query)

        for row in response:
            results.append(
                {
                    "campaignId": str(row.campaign.id),
                    "campaignName": row.campaign.name,
                    "status": row.campaign.status.name,
                    "channelType": row.campaign.advertising_channel_type.name,
                    "budgetId": (
                        str(row.campaign_budget.id) if row.campaign_budget.id else None
                    ),
                }
            )

    except GoogleAdsException as ex:
        raise RuntimeError(f"Google Ads API error: {ex.failure}") from ex

    return results


def get_ggad_campaigns(accounts: list[dict]) -> list[dict]:
    """
    Get Google Ads campaigns for multiple accounts,
    filtered by naming convention:
    [zzz.][accountCode]_[adTypeCode]_[Name]
    """
    adtypes = get_adtypes()
    ad_type_pattern = "|".join(map(re.escape, adtypes.keys()))

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

            if name.lower().startswith("zzz."):
                continue

            filtered.append(
                {
                    "customerId": account["id"],
                    "accountCode": account_code,
                    "accountName": account.get("accountName"),
                    "adTypeCode": match.group(2).upper(),
                    **c,
                }
            )

        return filtered

    tasks = [(per_account_func, (account,)) for account in accounts]
    return run_parallel_flatten(tasks=tasks, api_name="google_ads")


# =====================
# SPEND
# =====================


def get_ggad_spent(customer_id: str) -> list[dict]:
    """
    Get campaign spend for a single Google Ads account for the current period.
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
        response = ga_service.search(customer_id=customer_id, query=query)

        for row in response:
            results.append(
                {
                    "year": row.segments.year,
                    "month": row.segments.month,
                    "campaignId": str(row.campaign.id),
                    "budgetId": (
                        str(row.campaign_budget.id) if row.campaign_budget.id else None
                    ),
                    "cost": row.metrics.cost_micros / 1_000_000,
                }
            )

    except GoogleAdsException as ex:
        raise RuntimeError(f"Google Ads API error: {ex.failure}") from ex

    return results


def get_ggad_spents(accounts: list[dict]) -> list[dict]:
    """
    Get campaign spend for multiple Google Ads accounts (parallelized).
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

    tasks = [(per_account_func, (account,)) for account in accounts]

    return run_parallel_flatten(tasks=tasks, api_name="google_ads")


# =====================
# VALIDATION
# =====================


def validate_updates(
    *,
    customer_id: str,
    updates: list[dict],
    mode: str,  # "campaign_status" | "budget"
) -> tuple[list[dict], list[dict]]:
    """
    Row-level validation and filtering.

    Returns:
        valid_updates, invalid_updates
    """
    if not updates:
        raise ValueError("No updates provided")

    valid: list[dict] = []
    invalid: list[dict] = []

    # =====================
    # CAMPAIGN STATUS
    # =====================
    if mode == "campaign_status":
        seen_ids = set()
        paused_count = 0

        for r in updates:
            try:
                if "campaignId" not in r:
                    raise ValueError("Missing campaignId")

                status_value = r.get("newStatus", r.get("status"))
                if not status_value:
                    raise ValueError("Missing newStatus or status")

                campaign_id = r["campaignId"]
                status = str(status_value).upper()

                if campaign_id in seen_ids:
                    raise ValueError("Duplicate campaignId")

                if status not in GGADS_ALLOWED_CAMPAIGN_STATUSES:
                    raise ValueError(f"Invalid status: {status}")

                if status == "PAUSED":
                    paused_count += 1
                    if paused_count > GGADS_MAX_PAUSED_CAMPAIGNS:
                        raise ValueError("Pause-all protection triggered")

                seen_ids.add(campaign_id)
                valid.append(r)

            except Exception as e:
                invalid.append({**r, "error": str(e)})
                logger.error(
                    "Campaign row excluded by validation",
                    extra={
                        "extra_fields": {
                            "operation": "validate_campaign_status",
                            "customerId": customer_id,
                            "reason": str(e),
                            "row": r,
                        }
                    },
                )

    # =====================
    # BUDGET
    # =====================
    elif mode == "budget":
        seen_ids = set()

        for r in updates:
            try:
                if not {"budgetId", "newAmount", "currentAmount"}.issubset(r):
                    raise ValueError("Missing budgetId, currentAmount, or newAmount")

                budget_id = r["budgetId"]
                new_amount = r["newAmount"]
                current_amount = r["currentAmount"]

                if budget_id in seen_ids:
                    raise ValueError("Duplicate budgetId")

                # ✅ BLOCK UPWARD spikes ONLY
                if new_amount > current_amount * GGADS_MAX_BUDGET_MULTIPLIER:
                    raise ValueError("Budget spike exceeds allowed multiplier")

                seen_ids.add(budget_id)
                valid.append(r)

            except Exception as e:
                invalid.append({**r, "error": str(e)})
                logger.error(
                    "Budget row excluded by validation",
                    extra={
                        "extra_fields": {
                            "operation": "validate_budget",
                            "customerId": customer_id,
                            "reason": str(e),
                            "row": r,
                            "intendedAmount": r.get("newAmount"),
                        }
                    },
                )

    else:
        raise ValueError(f"Unknown validation mode: {mode}")

    return valid, invalid


# =====================
# UPDATE BUDGETS
# =====================


def update_budgets(
    *,
    customer_id: str,
    updates: list[dict],
) -> dict:
    """
    Params:
        customer_id: str
        updates: [
            { budgetId, currentAmount, newAmount }
        ]
    """
    valid, invalid = validate_updates(
        customer_id=customer_id,
        updates=updates,
        mode="budget",
    )
    account_code = next(
        (r.get("accountCode") for r in updates if r.get("accountCode")), None
    )

    if not valid:
        return {
            "customerId": customer_id,
            "accountCode": account_code,
            "operation": "update_budgets",
            "summary": {
                "total": len(updates),
                "succeeded": 0,
                "failed": len(invalid),
            },
            "successes": [],
            "failures": invalid,
        }

    client = get_client()
    service = client.get_service("CampaignBudgetService")

    operations = []

    for r in valid:
        new_amount = r["newAmount"]
        if new_amount <= 0:
            new_amount = GGADS_MIN_BUDGET

        op = client.get_type("CampaignBudgetOperation")
        budget = op.update

        budget.resource_name = service.campaign_budget_path(
            customer_id,
            r["budgetId"],
        )
        # Quantize to cents to match Google Ads minimum money unit and avoid float drift.
        quantized_amount = Decimal(str(new_amount)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        budget.amount_micros = int(
            (quantized_amount * Decimal("1000000")).to_integral_value(
                rounding=ROUND_HALF_UP
            )
        )

        op.update_mask.CopyFrom(FieldMask(paths=["amount_micros"]))

        operations.append(op)

    # -------- v22 request object --------
    request = client.get_type("MutateCampaignBudgetsRequest")
    request.customer_id = customer_id
    request.operations.extend(operations)
    request.partial_failure = True

    response = service.mutate_campaign_budgets(request=request)

    successes = []
    failures = invalid.copy()
    successful_indices = set(range(len(valid)))

    # -------- parse partial failure (v22-safe) --------
    if response.partial_failure_error:
        failure_pb_cls = GoogleAdsFailure.pb()
        failure_pb = failure_pb_cls()

        for detail in response.partial_failure_error.details:
            if detail.Is(failure_pb_cls.DESCRIPTOR):
                detail.Unpack(failure_pb)

        for err in failure_pb.errors:
            idx = err.location.field_path_elements[0].index
            successful_indices.discard(idx)
            failures.append(
                {
                    "budgetId": valid[idx]["budgetId"],
                    "oldAmount": valid[idx].get("currentAmount"),
                    "newAmount": valid[idx].get("newAmount"),
                    "error": err.message,
                }
            )

    for i in successful_indices:
        successes.append(
            {
                "budgetId": valid[i]["budgetId"],
                "campaignNames": valid[i].get("campaignNames", []),
                "oldAmount": valid[i].get("currentAmount"),
                "newAmount": max(valid[i]["newAmount"], GGADS_MIN_BUDGET),
            }
        )

    return {
        "customerId": customer_id,
        "accountCode": account_code,
        "operation": "update_budgets",
        "summary": {
            "total": len(updates),
            "succeeded": len(successes),
            "failed": len(failures),
        },
        "successes": successes,
        "failures": failures,
    }


# =====================
# UPDATE CAMPAIGN STATUSES
# =====================


def update_campaign_statuses(
    *,
    customer_id: str,
    updates: list[dict],
) -> dict:
    """
    Params:
        customer_id: str
        updates: [
            { campaignId, status }
        ]
    """
    valid, invalid = validate_updates(
        customer_id=customer_id,
        updates=updates,
        mode="campaign_status",
    )
    account_code = next(
        (r.get("accountCode") for r in updates if r.get("accountCode")), None
    )

    if not valid:
        return {
            "customerId": customer_id,
            "accountCode": account_code,
            "operation": "update_campaign_statuses",
            "summary": {
                "total": len(updates),
                "succeeded": 0,
                "failed": len(invalid),
            },
            "successes": [],
            "failures": invalid,
        }

    client = get_client()
    service = client.get_service("CampaignService")

    operations = []

    for r in valid:
        op = client.get_type("CampaignOperation")
        campaign = op.update

        campaign.resource_name = service.campaign_path(
            customer_id,
            r["campaignId"],
        )
        new_status_value = r.get("newStatus", r.get("status"))
        campaign.status = (
            client.enums.CampaignStatusEnum.ENABLED
            if str(new_status_value).upper() == "ENABLED"
            else client.enums.CampaignStatusEnum.PAUSED
        )

        op.update_mask.CopyFrom(FieldMask(paths=["status"]))

        operations.append(op)

    # -------- campaigns are atomic --------
    request = client.get_type("MutateCampaignsRequest")
    request.customer_id = customer_id
    request.operations.extend(operations)

    try:
        service.mutate_campaigns(request=request)

        successes = [
            {
                "campaignId": r["campaignId"],
                "oldStatus": r.get("oldStatus"),
                "newStatus": r.get("newStatus", r.get("status")),
            }
            for r in valid
        ]
        failures = invalid.copy()

    except GoogleAdsException as ex:
        successes = []
        failures = invalid + [
            {
                "campaignId": r["campaignId"],
                "oldStatus": r.get("oldStatus"),
                "newStatus": r.get("newStatus", r.get("status")),
                "error": str(ex),
            }
            for r in valid
        ]

    return {
        "customerId": customer_id,
        "accountCode": account_code,
        "operation": "update_campaign_statuses",
        "summary": {
            "total": len(updates),
            "succeeded": len(successes),
            "failed": len(failures),
        },
        "successes": successes,
        "failures": failures,
    }
