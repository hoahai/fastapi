from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from apps.spendsphere.api.v1.helpers.dataTransform import (
    build_update_payloads_from_inputs,
)
from apps.spendsphere.api.v2.helpers.db_queries import (
    get_accelerations,
    get_allocations,
    get_masterbudgets,
    get_rollbreakdowns,
)
from apps.spendsphere.api.v1.helpers.ggSheet import get_active_period
from shared.email import send_google_ads_result_email
from shared.logger import get_logger
from shared.tenant import get_timezone
from shared.utils import run_parallel

# =========================================================
# LOGGER
# =========================================================

logger = get_logger("SpendSphere")

# =========================================================
# HELPERS
# =========================================================


def normalize_account_codes(account_code):
    """
    - None or ""        -> all accounts
    - "TAC"             -> single account
    - ["TAC", "TAAA"]   -> multiple accounts
    """
    if account_code is None:
        return None

    if isinstance(account_code, str):
        code = account_code.strip()
        return {code.upper()} if code else None

    if isinstance(account_code, list):
        cleaned = {
            code.strip().upper()
            for code in account_code
            if isinstance(code, str) and code.strip()
        }
        return cleaned if cleaned else None

    raise TypeError("account_codes must be None, str, or list[str]")


def _run_budget_update(customer_id: str, updates: list[dict]) -> dict:
    from apps.spendsphere.api.v1.helpers.ggAd import update_budgets

    return update_budgets(
        customer_id=customer_id,
        updates=updates,
    )


def _run_campaign_update(customer_id: str, updates: list[dict]) -> dict:
    from apps.spendsphere.api.v1.helpers.ggAd import update_campaign_statuses

    return update_campaign_statuses(
        customer_id=customer_id,
        updates=updates,
    )


# =========================================================
# PIPELINE
# =========================================================


def run_google_ads_budget_pipeline(
    *,
    account_codes: list[str] | str | None = None,
    dry_run: bool = False,
    include_transform_results: bool = False,
) -> dict:
    """
    Full Google Ads budget + campaign update pipeline.
    """
    from apps.spendsphere.api.v1.helpers.ggAd import (
        get_ggad_accounts,
        get_ggad_campaigns,
        get_ggad_budgets,
        get_ggad_spents,
    )

    account_code_filter = normalize_account_codes(account_codes)

    # =====================================================
    # 1. Database (parallel)
    # =====================================================
    master_budgets, allocations, rollbreakdowns, accelerations = run_parallel(
        tasks=[
            (get_masterbudgets, (account_codes,)),
            (get_allocations, (account_codes,)),
            (get_rollbreakdowns, (account_codes,)),
            (get_accelerations, (account_codes,)),
        ],
        api_name="database",
    )

    # =====================================================
    # 2. Google Ads data (parallel)
    # =====================================================
    accounts = get_ggad_accounts()

    if account_code_filter:
        accounts = [
            acc
            for acc in accounts
            if acc.get("accountCode", "").upper() in account_code_filter
        ]

    campaigns, budgets, costs = run_parallel(
        tasks=[
            (get_ggad_campaigns, (accounts,)),
            (get_ggad_budgets, (accounts,)),
            (get_ggad_spents, (accounts,)),
        ],
        api_name="google_ads",
    )

    # =====================================================
    # 3. Transform + Generate mutation payloads
    # =====================================================
    active_period = get_active_period(account_codes)

    (
        budget_payloads,
        campaign_payloads,
        results,
    ) = build_update_payloads_from_inputs(
        master_budgets=master_budgets,
        campaigns=campaigns,
        budgets=budgets,
        costs=costs,
        allocations=allocations,
        rollovers=rollbreakdowns,
        accelerations=accelerations,
        activePeriod=active_period,
        include_transform_results=include_transform_results,
    )

    # =====================================================
    # 4. Execute Google Ads mutations (parallel)
    # =====================================================
    mutation_results = []

    if dry_run:
        for payload in budget_payloads:
            updates = payload.get("updates", [])
            if not updates:
                continue

            account_code = next(
                (u.get("accountCode") for u in updates if u.get("accountCode")),
                None,
            )

            mutation_results.append(
                {
                    "customerId": payload["customer_id"],
                    "accountCode": account_code,
                    "operation": "update_budgets",
                    "summary": {
                        "total": len(updates),
                        "succeeded": len(updates),
                        "failed": 0,
                    },
                    "successes": [
                        {
                            "budgetId": u.get("budgetId"),
                            "campaignNames": u.get("campaignNames", []),
                            "oldAmount": u.get("currentAmount"),
                            "newAmount": u.get("newAmount"),
                        }
                        for u in updates
                    ],
                    "failures": [],
                }
            )

        for payload in campaign_payloads:
            updates = payload.get("updates", [])
            if not updates:
                continue

            account_code = next(
                (u.get("accountCode") for u in updates if u.get("accountCode")),
                None,
            )

            mutation_results.append(
                {
                    "customerId": payload["customer_id"],
                    "accountCode": account_code,
                    "operation": "update_campaign_statuses",
                    "summary": {
                        "total": len(updates),
                        "succeeded": len(updates),
                        "failed": 0,
                    },
                    "successes": [
                        {
                            "campaignId": u.get("campaignId"),
                            "oldStatus": u.get("oldStatus"),
                            "newStatus": u.get("newStatus"),
                        }
                        for u in updates
                    ],
                    "failures": [],
                }
            )
    else:
        tasks = []

        # -------------------------
        # Budget updates
        # -------------------------
        for payload in budget_payloads:
            customer_id = payload["customer_id"]
            updates = payload["updates"]

            if not updates:
                continue

            tasks.append((_run_budget_update, (customer_id, updates)))

        # -------------------------
        # Campaign updates
        # -------------------------
        for payload in campaign_payloads:
            customer_id = payload["customer_id"]
            updates = payload["updates"]

            if not updates:
                continue

            tasks.append((_run_campaign_update, (customer_id, updates)))

        mutation_results = run_parallel(
            tasks=tasks,
            api_name="google_ads_mutation",
        )

    # =====================================================
    # 5. Aggregate results
    # =====================================================
    overall_summary = {"total": 0, "succeeded": 0, "failed": 0}

    for r in mutation_results:
        overall_summary["total"] += r["summary"]["total"]
        overall_summary["succeeded"] += r["summary"]["succeeded"]
        overall_summary["failed"] += r["summary"]["failed"]

    pipeline_result = {
        "dry_run": dry_run,
        "account_codes": (
            sorted(account_code_filter) if account_code_filter else "ALL"
        ),
        "overall_summary": overall_summary,
        "mutation_results": mutation_results,
    }
    if include_transform_results:
        pipeline_result["transform_results"] = results

    # =====================================================
    # 6. Email FULL report on failures
    # =====================================================
    if overall_summary.get("failed", 0) > 0:
        local_time = datetime.now(ZoneInfo(get_timezone())).strftime(
            "%d/%m/%Y %H:%M:%S"
        )
        email_body = json.dumps(pipeline_result, indent=2, default=str)
        send_google_ads_result_email(
            subject=(
                "Spendsphere - Google Ads update report "
                f"(failures detected) {local_time}"
            ),
            body=email_body,
        )

    logger.debug(
        "Google Ads pipeline completed",
        extra={"extra_fields": pipeline_result},
    )

    return pipeline_result
