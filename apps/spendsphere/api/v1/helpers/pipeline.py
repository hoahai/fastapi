from __future__ import annotations

from decimal import InvalidOperation
from decimal import Decimal

from apps.spendsphere.api.v1.helpers.account_codes import (
    standardize_account_code,
    standardize_account_code_set,
)
from apps.spendsphere.api.v1.helpers.dataTransform import (
    build_update_payloads_from_inputs,
)
from apps.spendsphere.api.v1.helpers.config import (
    get_budget_warning_threshold,
    get_google_ads_inactive_prefixes,
    is_google_ads_inactive_name,
)
from apps.spendsphere.api.v1.helpers.email import build_google_ads_alert_email
from apps.spendsphere.api.v1.helpers.db_queries import (
    get_allocations,
    get_accelerations,
    get_masterbudgets,
    get_rollbreakdowns,
)
from apps.spendsphere.api.v1.helpers.ggSheet import get_active_period
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    filter_cached_google_ads_warnings,
)
from shared.email import send_google_ads_result_email
from shared.logger import get_logger
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
    - None or ""        → all accounts
    - "TAC"             → single account
    - ["TAC", "TAAA"]   → multiple accounts
    """
    if account_code is None:
        return None

    if isinstance(account_code, (str, list)):
        cleaned = standardize_account_code_set(account_code)
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


def _to_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    cleaned = str(value).strip()
    if not cleaned:
        return None
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1].strip()
    cleaned = cleaned.replace(",", "")
    try:
        return Decimal(cleaned)
    except (InvalidOperation, TypeError, ValueError):
        return None


def _extract_percentage(row: dict, *keys: str) -> Decimal | None:
    for key in keys:
        if key not in row:
            continue
        parsed = _to_decimal(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _format_percent(value: Decimal) -> str:
    return f"{float(value):,.2f}%"


def _collect_budget_threshold_warnings(
    *,
    budget_payloads: list[dict],
    threshold: Decimal,
) -> dict[str, list[dict]]:
    warnings_by_customer: dict[str, list[dict]] = {}
    threshold_display = f"${float(threshold):,.2f}"
    message = (
        "New budget amount exceeds configured threshold "
        f"({threshold_display})."
    )

    for payload in budget_payloads:
        customer_id = str(payload.get("customer_id", "")).strip()
        if not customer_id:
            continue
        for update in payload.get("updates", []) or []:
            try:
                new_amount = Decimal(str(update.get("newAmount")))
            except Exception:
                continue
            if new_amount <= threshold:
                continue

            warnings_by_customer.setdefault(customer_id, []).append(
                {
                    "budgetId": update.get("budgetId"),
                    "accountCode": update.get("accountCode"),
                    "campaignNames": update.get("campaignNames", []),
                    "currentAmount": update.get("currentAmount"),
                    "newAmount": update.get("newAmount"),
                    "threshold": float(threshold),
                    "warningCode": "BUDGET_AMOUNT_THRESHOLD_EXCEEDED",
                    "error": message,
                }
            )

    return warnings_by_customer


def _inject_budget_warnings(
    *,
    mutation_results: list[dict],
    warnings_by_customer: dict[str, list[dict]],
) -> int:
    if not warnings_by_customer:
        return 0

    total_warnings = 0
    for result in mutation_results:
        if result.get("operation") != "update_budgets":
            continue
        customer_id = str(result.get("customerId", "")).strip()
        if not customer_id:
            continue
        extra_warnings = warnings_by_customer.pop(customer_id, [])
        if not extra_warnings:
            continue
        result.setdefault("warnings", []).extend(extra_warnings)
        summary = result.setdefault("summary", {})
        summary["warnings"] = int(summary.get("warnings", 0) or 0) + len(extra_warnings)
        total_warnings += len(extra_warnings)

    for customer_id, extra_warnings in warnings_by_customer.items():
        if not extra_warnings:
            continue
        account_code = next(
            (w.get("accountCode") for w in extra_warnings if w.get("accountCode")),
            None,
        )
        mutation_results.append(
            {
                "customerId": customer_id,
                "accountCode": account_code,
                "operation": "update_budgets",
                "summary": {
                    "total": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "warnings": len(extra_warnings),
                },
                "successes": [],
                "failures": [],
                "warnings": extra_warnings,
            }
        )
        total_warnings += len(extra_warnings)

    return total_warnings


def _inject_budget_failures(
    *,
    mutation_results: list[dict],
    failures_by_customer: dict[str, list[dict]],
) -> int:
    if not failures_by_customer:
        return 0

    total_failures = 0
    for result in mutation_results:
        if result.get("operation") != "update_budgets":
            continue
        customer_id = str(result.get("customerId", "")).strip()
        if not customer_id:
            continue
        extra_failures = failures_by_customer.pop(customer_id, [])
        if not extra_failures:
            continue
        result.setdefault("failures", []).extend(extra_failures)
        summary = result.setdefault("summary", {})
        summary["failed"] = int(summary.get("failed", 0) or 0) + len(extra_failures)
        summary["total"] = int(summary.get("total", 0) or 0) + len(extra_failures)
        total_failures += len(extra_failures)

    for customer_id, extra_failures in failures_by_customer.items():
        if not extra_failures:
            continue
        account_code = next(
            (f.get("accountCode") for f in extra_failures if f.get("accountCode")),
            None,
        )
        mutation_results.append(
            {
                "customerId": customer_id,
                "accountCode": account_code,
                "operation": "update_budgets",
                "summary": {
                    "total": len(extra_failures),
                    "succeeded": 0,
                    "failed": len(extra_failures),
                    "warnings": 0,
                },
                "successes": [],
                "failures": extra_failures,
                "warnings": [],
            }
        )
        total_failures += len(extra_failures)

    return total_failures


def _filter_existing_mutation_warnings(
    *,
    mutation_results: list[dict],
) -> int:
    warnings_by_customer: dict[str, list[dict]] = {}
    for result in mutation_results:
        customer_id = str(result.get("customerId", "")).strip()
        warnings = [
            warning
            for warning in (result.get("warnings") or [])
            if isinstance(warning, dict)
        ]
        if not customer_id or not warnings:
            continue
        warnings_by_customer.setdefault(customer_id, []).extend(warnings)

    if not warnings_by_customer:
        return 0

    filtered_by_customer = filter_cached_google_ads_warnings(warnings_by_customer)
    total_warnings = 0

    for result in mutation_results:
        existing_warnings = [
            warning
            for warning in (result.get("warnings") or [])
            if isinstance(warning, dict)
        ]
        if not existing_warnings:
            continue
        customer_id = str(result.get("customerId", "")).strip()
        if not customer_id:
            continue
        warnings = filtered_by_customer.pop(customer_id, [])
        result["warnings"] = warnings
        summary = result.setdefault("summary", {})
        summary["warnings"] = len(warnings)
        total_warnings += summary["warnings"]

    return total_warnings


def _collect_budget_allocation_and_spend_issues(
    *,
    rows: list[dict],
) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    warnings_by_customer: dict[str, list[dict]] = {}
    failures_by_customer: dict[str, list[dict]] = {}
    inactive_prefixes = get_google_ads_inactive_prefixes()

    for row in rows:
        try:
            spend = Decimal(str(row.get("totalCost", 0)))
        except Exception:
            continue
        if spend <= 0:
            continue

        allocation = row.get("allocation")
        has_missing_or_zero_allocation = allocation is None
        if not has_missing_or_zero_allocation:
            try:
                has_missing_or_zero_allocation = (
                    Decimal(str(allocation)) == Decimal("0")
                )
            except Exception:
                has_missing_or_zero_allocation = False

        google_budget_amount: Decimal | None = None
        if row.get("budgetAmount") is not None:
            try:
                google_budget_amount = Decimal(str(row.get("budgetAmount")))
            except Exception:
                google_budget_amount = None

        allocated_budget_amount: Decimal | None = None
        allocated_budget_before_accel = row.get("allocatedBudgetBeforeAcceleration")
        if allocated_budget_before_accel is not None:
            try:
                allocated_budget_amount = Decimal(str(allocated_budget_before_accel))
            except Exception:
                allocated_budget_amount = None
        elif allocation is not None:
            try:
                net_amount = Decimal(str(row.get("netAmount", 0)))
                rollover_amount = Decimal(str(row.get("rolloverAmount", 0)))
                allocation_pct = Decimal(str(allocation)) / Decimal("100")
                allocated_budget_amount = (
                    (net_amount + rollover_amount) * allocation_pct
                ).quantize(Decimal("0.01"))
            except Exception:
                allocated_budget_amount = None

        has_budget_less_than_spend = (
            allocated_budget_amount is not None and allocated_budget_amount < spend
        )

        acceleration_multiplier = _to_decimal(
            row.get("accelerationMultiplier", Decimal("100"))
        ) or Decimal("100")
        accelerated_allocated_budget: Decimal | None = None
        if allocated_budget_amount is not None:
            accelerated_allocated_budget = (
                allocated_budget_amount * (acceleration_multiplier / Decimal("100"))
            ).quantize(Decimal("0.01"))

        spend_percentage = _extract_percentage(
            row,
            "%Spend",
            "spendPct",
            "spendPercent",
            "spendPercentage",
            "percentSpend",
            "pctSpend",
        )
        if (
            spend_percentage is None
            and allocated_budget_amount is not None
            and allocated_budget_amount > 0
        ):
            spend_percentage = (
                spend / allocated_budget_amount * Decimal("100")
            ).quantize(Decimal("0.01"))

        pacing_percentage = _extract_percentage(
            row,
            "pacing",
            "pacingPct",
            "pacingPercent",
            "pace",
        )
        if (
            pacing_percentage is None
            and accelerated_allocated_budget is not None
            and accelerated_allocated_budget > 0
        ):
            pacing_percentage = (
                spend / accelerated_allocated_budget * Decimal("100")
            ).quantize(Decimal("0.01"))

        has_pacing_over_100 = (
            pacing_percentage is not None and pacing_percentage > Decimal("100")
        )
        has_spend_pct_over_100 = (
            spend_percentage is not None and spend_percentage > Decimal("100")
        )
        if (
            not has_missing_or_zero_allocation
            and not has_budget_less_than_spend
            and not has_pacing_over_100
            and not has_spend_pct_over_100
        ):
            continue

        customer_id = str(row.get("ggAccountId", "")).strip()
        if not customer_id:
            continue

        campaigns = row.get("campaigns", [])
        all_campaigns_paused = bool(campaigns) and all(
            str(c.get("status", "")).strip().upper() == "PAUSED"
            for c in campaigns
        )
        all_campaigns_inactive_name = bool(campaigns) and all(
            is_google_ads_inactive_name(
                c.get("campaignName"),
                inactive_prefixes=inactive_prefixes,
            )
            for c in campaigns
        )
        if all_campaigns_paused or all_campaigns_inactive_name:
            continue

        campaign_names = [
            c.get("campaignName")
            for c in campaigns
            if c.get("campaignName")
        ]
        if not campaign_names:
            # Skip budgets that are not linked to any campaigns.
            continue
        spend_display = f"${float(spend):,.2f}"
        current_google_budget = (
            float(google_budget_amount) if google_budget_amount is not None else None
        )

        if has_missing_or_zero_allocation:
            allocation_error = (
                "Spend detected with missing allocation"
                if allocation is None
                else "Spend detected with 0 allocation"
            )
            issue = {
                "budgetId": row.get("budgetId"),
                "accountCode": row.get("accountCode"),
                "campaignNames": campaign_names,
                "currentAmount": current_google_budget,
                "newAmount": None,
                "spent": float(spend),
                "error": f"{allocation_error} ({spend_display}); budget update skipped.",
            }
            if allocation is None:
                failures_by_customer.setdefault(customer_id, []).append(
                    {
                        **issue,
                        "failureCode": "SPEND_WITH_MISSING_ALLOCATION",
                    }
                )
            else:
                warnings_by_customer.setdefault(customer_id, []).append(
                    {
                        **issue,
                        "warningCode": "SPEND_WITHOUT_ALLOCATION",
                    }
                )

        if has_budget_less_than_spend:
            budget_display = f"${float(allocated_budget_amount):,.2f}"
            warnings_by_customer.setdefault(customer_id, []).append(
                {
                    "budgetId": row.get("budgetId"),
                    "accountCode": row.get("accountCode"),
                    "campaignNames": campaign_names,
                    "currentAmount": float(allocated_budget_amount),
                    "newAmount": None,
                    "spent": float(spend),
                    "warningCode": "BUDGET_LESS_THAN_SPEND",
                    "error": (
                        "Allocated budget amount ((master budget + roll breakdown) x allocation, before acceleration) is lower than spend "
                        f"({budget_display} < {spend_display})."
                    ),
                }
            )

        if has_pacing_over_100 and pacing_percentage is not None:
            warnings_by_customer.setdefault(customer_id, []).append(
                {
                    "budgetId": row.get("budgetId"),
                    "accountCode": row.get("accountCode"),
                    "campaignNames": campaign_names,
                    "currentAmount": current_google_budget,
                    "newAmount": None,
                    "spent": float(spend),
                    "pacing": float(pacing_percentage),
                    "warningCode": "PACING_OVER_100",
                    "error": (
                        "Pacing is more than 100% "
                        f"({_format_percent(pacing_percentage)})."
                    ),
                }
            )

        if has_spend_pct_over_100 and spend_percentage is not None:
            warnings_by_customer.setdefault(customer_id, []).append(
                {
                    "budgetId": row.get("budgetId"),
                    "accountCode": row.get("accountCode"),
                    "campaignNames": campaign_names,
                    "currentAmount": current_google_budget,
                    "newAmount": None,
                    "spent": float(spend),
                    "spendPercent": float(spend_percentage),
                    "warningCode": "SPEND_PERCENT_OVER_100",
                    "error": (
                        "%Spend is more than 100% "
                        f"({_format_percent(spend_percentage)})."
                    ),
                }
            )

    return warnings_by_customer, failures_by_customer


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
        get_ggad_budget_adtype_candidates,
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
            if (standardize_account_code(acc.get("accountCode")) or "")
            in account_code_filter
        ]

    campaigns, budgets, costs, fallback_ad_types_by_budget = run_parallel(
        tasks=[
            (get_ggad_campaigns, (accounts,)),
            (get_ggad_budgets, (accounts,)),
            (get_ggad_spents, (accounts,)),
            (get_ggad_budget_adtype_candidates, (accounts,)),
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
        fallback_ad_types_by_budget=fallback_ad_types_by_budget,
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

    _filter_existing_mutation_warnings(
        mutation_results=mutation_results,
    )

    budget_warning_threshold = get_budget_warning_threshold()
    threshold_warning_count = 0
    if budget_warning_threshold is not None:
        threshold_warnings_by_customer = _collect_budget_threshold_warnings(
            budget_payloads=budget_payloads,
            threshold=budget_warning_threshold,
        )
        threshold_warnings_by_customer = filter_cached_google_ads_warnings(
            threshold_warnings_by_customer
        )
        threshold_warning_count = _inject_budget_warnings(
            mutation_results=mutation_results,
            warnings_by_customer=threshold_warnings_by_customer,
        )
        if threshold_warning_count > 0:
            logger.warning(
                "Google Ads budget threshold warnings",
                extra={
                    "extra_fields": {
                        "threshold": float(budget_warning_threshold),
                        "warning_count": threshold_warning_count,
                    }
                },
            )

    budget_warnings, budget_failures = _collect_budget_allocation_and_spend_issues(
        rows=results,
    )
    budget_warnings = filter_cached_google_ads_warnings(
        budget_warnings
    )
    _inject_budget_warnings(
        mutation_results=mutation_results,
        warnings_by_customer=budget_warnings,
    )
    _inject_budget_failures(
        mutation_results=mutation_results,
        failures_by_customer=budget_failures,
    )

    # =====================================================
    # 5. Aggregate results
    # =====================================================
    overall_summary = {"total": 0, "succeeded": 0, "failed": 0, "warnings": 0}

    for r in mutation_results:
        overall_summary["total"] += r["summary"]["total"]
        overall_summary["succeeded"] += r["summary"]["succeeded"]
        overall_summary["failed"] += r["summary"]["failed"]
        overall_summary["warnings"] += r["summary"].get("warnings", 0)

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
    # 6. Log failures summary (single entry)
    # =====================================================
    if overall_summary.get("failed", 0) > 0:
        failure_rows: list[dict] = []
        for r in mutation_results:
            failures = r.get("failures") or []
            if not failures:
                continue
            failure_rows.append(
                {
                    "customerId": r.get("customerId"),
                    "accountCode": r.get("accountCode"),
                    "operation": r.get("operation"),
                    "failures": failures,
                }
            )
        if failure_rows:
            logger.error(
                "Google Ads pipeline failures",
                extra={
                    "extra_fields": {
                        "failed_count": overall_summary.get("failed", 0),
                        "failure_rows": failure_rows,
                    }
                },
            )

    # =====================================================
    # 7. Log warnings summary (single entry)
    # =====================================================
    warning_rows: list[dict] = []
    for r in mutation_results:
        warnings = r.get("warnings") or []
        if not warnings:
            continue
        warning_rows.append(
            {
                "customerId": r.get("customerId"),
                "accountCode": r.get("accountCode"),
                "operation": r.get("operation"),
                "warnings": warnings,
            }
        )
    if warning_rows:
        logger.warning(
            "Google Ads pipeline warnings",
            extra={
                "extra_fields": {
                    "warning_count": sum(len(r["warnings"]) for r in warning_rows),
                    "warning_rows": warning_rows,
                }
            },
        )

    logger.debug(
        "Google Ads pipeline completed",
        extra={
            "extra_fields": {
                "dry_run": dry_run,
                "account_codes": (
                    sorted(account_code_filter) if account_code_filter else "ALL"
                ),
                "overall_summary": overall_summary,
                "mutation_results_count": len(mutation_results),
            }
        },
    )

    has_failures = overall_summary.get("failed", 0) > 0
    has_warnings = overall_summary.get("warnings", 0) > 0 or bool(warning_rows)
    if has_failures or has_warnings:
        try:
            subject, text_body, html_body = build_google_ads_alert_email(
                full_report=pipeline_result,
            )
            send_google_ads_result_email(
                subject,
                text_body,
                html=html_body,
            )
        except Exception as exc:
            logger.error(
                "Failed to send Google Ads alert email",
                extra={"extra_fields": {"error": str(exc)}},
            )

    return pipeline_result
