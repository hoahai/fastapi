from __future__ import annotations

import calendar
from datetime import date, datetime
from decimal import InvalidOperation
from decimal import Decimal

import pytz

from apps.spendsphere.api.v1.helpers.accountCodes import (
    standardize_account_code,
    standardize_account_code_set,
)
from apps.spendsphere.api.v1.helpers.campaignRules import (
    DEFAULT_INACTIVE_PREFIXES,
    has_any_active_campaign,
    should_filter_row,
)
from apps.spendsphere.api.v1.helpers.dataTransform import (
    build_update_payloads_from_inputs,
    transform_google_ads_data,
)
from apps.spendsphere.api.v1.helpers.config import (
    get_budget_warning_threshold,
    get_google_ads_inactive_prefixes,
    is_google_ads_inactive_name,
)
from apps.spendsphere.api.v1.helpers.email import build_google_ads_alert_email
from apps.spendsphere.api.v1.helpers.dbQueries import (
    get_allocations,
    get_accelerations,
    get_masterbudgets,
    get_rollbreakdowns,
)
from apps.spendsphere.api.v1.helpers.ggSheet import get_active_period
from apps.spendsphere.api.v1.helpers.spendsphereHelpers import (
    filter_cached_google_ads_failures,
    filter_cached_google_ads_warnings,
    get_google_ads_budgets_cache_entries,
    get_google_ads_campaigns_cache_entries,
    get_google_ads_clients_cache_entry,
    sync_google_ads_warning_states,
)
from shared.constants import (
    ADTYPE_ALLOCATION_TOTAL_TOLERANCE_PERCENT,
    BUDGET_LESS_THAN_SPEND_TOLERANCE,
)
from shared.email import send_google_ads_result_email
from shared.logger import get_logger
from shared.tenant import get_timezone
from shared.utils import get_current_period, run_parallel

# =========================================================
# LOGGER
# =========================================================

logger = get_logger("SpendSphere")
BUDGET_LESS_THAN_SPEND_TOLERANCE_DECIMAL = Decimal(
    str(BUDGET_LESS_THAN_SPEND_TOLERANCE)
)
ADTYPE_ALLOCATION_TOTAL_TOLERANCE_DECIMAL = Decimal(
    str(ADTYPE_ALLOCATION_TOTAL_TOLERANCE_PERCENT)
)

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
            new_amount_display = f"${float(new_amount):,.2f}"
            message = (
                f"New budget amount ({new_amount_display}) exceeds configured "
                f"threshold ({threshold_display}). Budget update is still applied."
            )

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
    use_cache: bool = True,
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

    if use_cache:
        filtered_by_customer = filter_cached_google_ads_warnings(warnings_by_customer)
    else:
        filtered_by_customer = warnings_by_customer
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


def _filter_existing_mutation_failures(
    *,
    mutation_results: list[dict],
    use_cache: bool = True,
) -> int:
    total_failures = 0

    for result in mutation_results:
        existing_failures = [
            failure
            for failure in (result.get("failures") or [])
            if isinstance(failure, dict)
        ]
        if not existing_failures:
            continue

        customer_id = str(result.get("customerId", "")).strip()
        if not customer_id:
            continue

        if use_cache:
            filtered_by_customer = filter_cached_google_ads_failures(
                {customer_id: existing_failures}
            )
            failures = filtered_by_customer.get(customer_id, [])
        else:
            failures = existing_failures

        result["failures"] = failures
        summary = result.setdefault("summary", {})
        succeeded = int(summary.get("succeeded", 0) or 0)
        warnings_count = int(summary.get("warnings", 0) or 0)
        summary["failed"] = len(failures)
        summary["total"] = succeeded + len(failures) + warnings_count
        total_failures += summary["failed"]

    return total_failures


def _maybe_filter_google_ads_warnings(
    warnings_by_customer: dict[str, list[dict]],
    *,
    use_cache: bool,
) -> dict[str, list[dict]]:
    if not warnings_by_customer:
        return {}
    if use_cache:
        return filter_cached_google_ads_warnings(warnings_by_customer)
    return warnings_by_customer


def _merge_warnings_by_customer(
    destination: dict[str, list[dict]],
    source: dict[str, list[dict]],
) -> None:
    for raw_customer_id, warnings in source.items():
        customer_id = str(raw_customer_id).strip()
        if not customer_id or not isinstance(warnings, list):
            continue
        for warning in warnings:
            if isinstance(warning, dict):
                destination.setdefault(customer_id, []).append(warning)


def _extract_warnings_from_mutation_results(
    mutation_results: list[dict],
) -> dict[str, list[dict]]:
    warnings_by_customer: dict[str, list[dict]] = {}
    for result in mutation_results:
        customer_id = str(result.get("customerId", "")).strip()
        if not customer_id:
            continue
        warnings = result.get("warnings") or []
        if not isinstance(warnings, list):
            continue
        for warning in warnings:
            if isinstance(warning, dict):
                warnings_by_customer.setdefault(customer_id, []).append(warning)
    return warnings_by_customer


def _maybe_filter_google_ads_failures(
    failures_by_customer: dict[str, list[dict]],
    *,
    use_cache: bool,
) -> dict[str, list[dict]]:
    if not failures_by_customer:
        return {}
    if use_cache:
        return filter_cached_google_ads_failures(failures_by_customer)
    return failures_by_customer


def _collect_budget_allocation_and_spend_issues(
    *,
    rows: list[dict],
    planned_new_amounts: dict[tuple[str, str], float] | None = None,
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

        shortfall_amount: Decimal | None = None
        if allocated_budget_amount is not None:
            shortfall_amount = spend - allocated_budget_amount
        has_budget_less_than_spend = (
            shortfall_amount is not None
            and shortfall_amount > BUDGET_LESS_THAN_SPEND_TOLERANCE_DECIMAL
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

        budget_id = str(row.get("budgetId", "")).strip()
        planned_new_amount = (
            planned_new_amounts.get((customer_id, budget_id))
            if planned_new_amounts and budget_id
            else None
        )

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
                "budgetId": budget_id or row.get("budgetId"),
                "accountCode": row.get("accountCode"),
                "campaignNames": campaign_names,
                "currentAmount": current_google_budget,
                "newAmount": planned_new_amount,
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
                        "error": (
                            f"Spend detected ({spend_display}) with 0 allocation; "
                            "budget update skipped."
                        ),
                    }
                )

        if has_budget_less_than_spend:
            budget_display = f"${float(allocated_budget_amount):,.2f}"
            warnings_by_customer.setdefault(customer_id, []).append(
                {
                    "budgetId": budget_id or row.get("budgetId"),
                    "accountCode": row.get("accountCode"),
                    "campaignNames": campaign_names,
                    "currentAmount": current_google_budget,
                    "newAmount": planned_new_amount,
                    "spent": float(spend),
                    "warningCode": "BUDGET_LESS_THAN_SPEND",
                    "error": (
                        "Allocated budget (before acceleration) is lower than spend "
                        f"by more than $0.50 ({budget_display} < {spend_display})."
                    ),
                }
            )

        if has_pacing_over_100 and pacing_percentage is not None:
            pacing_display = f"{float(pacing_percentage):,.2f}"
            budget_id_display = str(budget_id or row.get("budgetId") or "Unknown")
            warnings_by_customer.setdefault(customer_id, []).append(
                {
                    "budgetId": budget_id or row.get("budgetId"),
                    "accountCode": row.get("accountCode"),
                    "campaignNames": campaign_names,
                    "currentAmount": current_google_budget,
                    "newAmount": planned_new_amount,
                    "spent": float(spend),
                    "pacing": float(pacing_percentage),
                    "warningCode": "PACING_OVER_100",
                    "error": f"Budget Id ({budget_id_display}) has pacing ({pacing_display}%) more than 100%.",
                }
            )

        if has_spend_pct_over_100 and spend_percentage is not None:
            spend_percent_display = f"{float(spend_percentage):,.2f}"
            warnings_by_customer.setdefault(customer_id, []).append(
                {
                    "budgetId": budget_id or row.get("budgetId"),
                    "accountCode": row.get("accountCode"),
                    "campaignNames": campaign_names,
                    "currentAmount": current_google_budget,
                    "newAmount": planned_new_amount,
                    "spent": float(spend),
                    "spendPercent": float(spend_percentage),
                    "warningCode": "SPEND_PERCENT_OVER_100",
                    "error": f"Percent Spent ({spend_percent_display}%) is more than 100%.",
                }
            )

    return warnings_by_customer, failures_by_customer


def _collect_ad_type_allocation_total_warnings(
    *,
    rows: list[dict],
    month: int,
    year: int,
) -> dict[str, list[dict]]:
    grouped: dict[tuple[str, str, str], dict[str, object]] = {}
    inactive_prefixes = DEFAULT_INACTIVE_PREFIXES

    for row in rows:
        if not isinstance(row, dict):
            continue

        allocation_value = _to_decimal(row.get("allocation"))
        no_allocation = allocation_value is None or allocation_value == Decimal("0")
        spent_value = _to_decimal(row.get("totalCost"))
        no_spent = spent_value is None or spent_value == Decimal("0")
        no_active_campaigns = not has_any_active_campaign(
            row.get("campaigns"),
            inactive_prefixes=inactive_prefixes,
        )
        # Align with tableData row filtering: filter rows before grouping/checking totals.
        if should_filter_row(
            no_allocation=no_allocation,
            no_active_campaigns=no_active_campaigns,
            no_spent=no_spent,
        ):
            continue

        customer_id = str(row.get("ggAccountId", "")).strip()
        account_code = standardize_account_code(row.get("accountCode")) or ""
        ad_type_code = str(row.get("adTypeCode", "")).strip().upper()
        budget_id = str(row.get("budgetId", "")).strip()
        if not customer_id or not account_code or not ad_type_code or not budget_id:
            continue

        key = (customer_id, account_code, ad_type_code)
        entry = grouped.setdefault(
            key,
            {
                "totalAllocation": Decimal("0"),
                "budgetIds": set(),
                "hasMasterBudgetValue": False,
                "masterBudgetSum": Decimal("0"),
            },
        )
        budget_ids = entry["budgetIds"]
        if not isinstance(budget_ids, set):
            budget_ids = set()
            entry["budgetIds"] = budget_ids
        if budget_id in budget_ids:
            continue
        budget_ids.add(budget_id)

        allocation = allocation_value if allocation_value is not None else Decimal("0")
        entry["totalAllocation"] = (
            entry.get("totalAllocation", Decimal("0")) + allocation
        )

        master_budget_amount = _to_decimal(row.get("netAmount"))
        if master_budget_amount is not None:
            entry["hasMasterBudgetValue"] = True
            entry["masterBudgetSum"] = (
                entry.get("masterBudgetSum", Decimal("0")) + master_budget_amount
            )

    warnings_by_customer: dict[str, list[dict]] = {}
    expected_total = Decimal("100.00")
    quantize_factor = Decimal("0.01")

    for (customer_id, account_code, ad_type_code), entry in grouped.items():
        has_master_budget_value = bool(entry.get("hasMasterBudgetValue"))
        if has_master_budget_value:
            master_budget_sum = entry.get("masterBudgetSum", Decimal("0"))
            if not isinstance(master_budget_sum, Decimal):
                master_budget_sum = _to_decimal(master_budget_sum) or Decimal("0")
            # Treat summed $0 master budget as "no budgets" for this allocation-total check.
            if master_budget_sum.quantize(quantize_factor) == Decimal("0.00"):
                continue

        total_allocation = entry.get("totalAllocation", Decimal("0"))
        if not isinstance(total_allocation, Decimal):
            total_allocation = _to_decimal(total_allocation) or Decimal("0")
        normalized_total = total_allocation.quantize(quantize_factor)
        total_delta = (normalized_total - expected_total).copy_abs()
        if total_delta <= ADTYPE_ALLOCATION_TOTAL_TOLERANCE_DECIMAL:
            continue

        period_key = f"{year:04d}-{month:02d}"
        warnings_by_customer.setdefault(customer_id, []).append(
            {
                "budgetId": f"ALLOC_TOTAL:{period_key}:{ad_type_code}",
                "accountCode": account_code,
                "campaignNames": [f"adTypeCode={ad_type_code}"],
                "adTypeCode": ad_type_code,
                "month": month,
                "year": year,
                "actualAllocationTotal": float(normalized_total),
                "expectedAllocationTotal": float(expected_total),
                "warningCode": "ADTYPE_ALLOCATION_TOTAL_NOT_100",
                "error": (
                    f"Total allocation ({float(normalized_total):,.2f}%) for "
                    f"{ad_type_code} must be within "
                    f"{float(expected_total - ADTYPE_ALLOCATION_TOTAL_TOLERANCE_DECIMAL):,.2f}% "
                    f"to {float(expected_total + ADTYPE_ALLOCATION_TOTAL_TOLERANCE_DECIMAL):,.2f}%."
                ),
            }
        )

    return warnings_by_customer


def _build_planned_budget_amount_lookup(
    budget_payloads: list[dict],
) -> dict[tuple[str, str], float]:
    lookup: dict[tuple[str, str], float] = {}
    for payload in budget_payloads:
        customer_id = str(payload.get("customer_id", "")).strip()
        if not customer_id:
            continue
        for update in payload.get("updates") or []:
            if not isinstance(update, dict):
                continue
            budget_id = str(update.get("budgetId", "")).strip()
            if not budget_id:
                continue
            new_amount = update.get("newAmount")
            if new_amount is None:
                continue
            try:
                lookup[(customer_id, budget_id)] = float(new_amount)
            except (TypeError, ValueError):
                continue
    return lookup


# =========================================================
# TRANSFORM BUILDER
# =========================================================


def _resolve_period(month: int | None, year: int | None) -> tuple[int, int]:
    if month is not None and year is not None:
        return month, year
    current = get_current_period()
    return current["month"], current["year"]


def _resolve_period_date(month: int, year: int) -> date:
    tz = pytz.timezone(get_timezone())
    today = datetime.now(tz).date()
    current = get_current_period()
    current_key = (current["year"], current["month"])
    target_key = (year, month)
    if target_key == current_key:
        return today
    if target_key < current_key:
        return date(year, month, calendar.monthrange(year, month)[1])
    return date(year, month, 1)


def _filter_accelerations_for_date(
    accelerations: list[dict],
    period_date: date,
) -> list[dict]:
    if not accelerations:
        return []

    filtered: list[dict] = []
    for row in accelerations:
        if not isinstance(row, dict):
            continue
        start_date = row.get("startDate")
        end_date = row.get("endDate")
        try:
            parsed_start = (
                datetime.fromisoformat(str(start_date)).date()
                if start_date
                else None
            )
        except ValueError:
            parsed_start = None
        try:
            parsed_end = datetime.fromisoformat(str(end_date)).date() if end_date else None
        except ValueError:
            parsed_end = None

        if parsed_start and period_date < parsed_start:
            continue
        if parsed_end and period_date > parsed_end:
            continue
        filtered.append(row)
    return filtered


def _build_fallback_ad_types_by_budget(
    campaigns: list[dict],
) -> dict[tuple[str, str], str | None]:
    result: dict[tuple[str, str], str | None] = {}
    for row in campaigns:
        if not isinstance(row, dict):
            continue
        customer_id = str(row.get("customerId", "")).strip()
        budget_id = str(row.get("budgetId", "")).strip()
        if not customer_id or not budget_id:
            continue
        key = (customer_id, budget_id)
        if key in result:
            continue
        ad_type = str(row.get("adTypeCode", "")).strip().upper()
        result[key] = ad_type or None
    return result


def _get_cached_or_fetch_accounts(
    *,
    refresh_google_ads_caches: bool,
    get_ggad_accounts,
) -> list[dict]:
    if refresh_google_ads_caches:
        return get_ggad_accounts(refresh_cache=True)

    cached, _ = get_google_ads_clients_cache_entry()
    if isinstance(cached, list) and cached:
        return cached
    return get_ggad_accounts(refresh_cache=False)


def _get_cached_or_fetch_campaigns_and_budgets(
    accounts: list[dict],
    *,
    refresh_google_ads_caches: bool,
    get_ggad_campaigns,
    get_ggad_budgets,
) -> tuple[list[dict], list[dict]]:
    if not accounts:
        return [], []

    if refresh_google_ads_caches:
        campaigns = get_ggad_campaigns(accounts, refresh_cache=True)
        budgets = get_ggad_budgets(accounts, refresh_cache=True)
        return campaigns, budgets

    account_by_code: dict[str, dict] = {}
    account_codes: list[str] = []
    for account in accounts:
        account_code = standardize_account_code(account.get("accountCode")) or ""
        if not account_code or account_code in account_by_code:
            continue
        account_by_code[account_code] = account
        account_codes.append(account_code)

    cached_campaigns, missing_campaign_codes = get_google_ads_campaigns_cache_entries(
        account_codes
    )
    cached_budgets, missing_budget_codes = get_google_ads_budgets_cache_entries(
        account_codes
    )

    campaigns: list[dict] = []
    budgets: list[dict] = []
    for account_code in account_codes:
        campaigns.extend(cached_campaigns.get(account_code, []))
        budgets.extend(cached_budgets.get(account_code, []))

    if missing_campaign_codes:
        missing_accounts = [
            account_by_code[code]
            for code in account_codes
            if code in missing_campaign_codes and code in account_by_code
        ]
        if missing_accounts:
            campaigns.extend(get_ggad_campaigns(missing_accounts, refresh_cache=False))

    if missing_budget_codes:
        missing_accounts = [
            account_by_code[code]
            for code in account_codes
            if code in missing_budget_codes and code in account_by_code
        ]
        if missing_accounts:
            budgets.extend(get_ggad_budgets(missing_accounts, refresh_cache=False))

    return campaigns, budgets


def build_transform_rows_for_period(
    *,
    account_codes: list[str] | str | None = None,
    month: int | None = None,
    year: int | None = None,
    refresh_google_ads_caches: bool = False,
    cache_first: bool = False,
    include_costs: bool = True,
) -> dict[str, object]:
    """
    Build transformed rows using the same core transform rules with period control.
    """
    from apps.spendsphere.api.v1.helpers.ggAd import (
        get_ggad_accounts,
        get_ggad_budgets,
        get_ggad_campaigns,
        get_ggad_spents,
    )

    account_code_filter = normalize_account_codes(account_codes)
    resolved_month, resolved_year = _resolve_period(month, year)
    period_date = _resolve_period_date(resolved_month, resolved_year)
    month_start = date(resolved_year, resolved_month, 1)
    month_end = date(
        resolved_year,
        resolved_month,
        calendar.monthrange(resolved_year, resolved_month)[1],
    )

    def _get_accelerations_for_month(rows: list[str] | None) -> list[dict]:
        return get_accelerations(
            rows,
            start_date=month_start,
            end_date=month_end,
        )

    master_budgets, allocations, rollbreakdowns, accelerations = run_parallel(
        tasks=[
            (get_masterbudgets, (account_code_filter, resolved_month, resolved_year)),
            (get_allocations, (account_code_filter, resolved_month, resolved_year)),
            (get_rollbreakdowns, (account_code_filter, resolved_month, resolved_year)),
            (_get_accelerations_for_month, (account_code_filter,)),
        ],
        api_name="spendsphere_transform_db",
    )
    active_accelerations = _filter_accelerations_for_date(accelerations, period_date)

    if cache_first:
        accounts = _get_cached_or_fetch_accounts(
            refresh_google_ads_caches=refresh_google_ads_caches,
            get_ggad_accounts=get_ggad_accounts,
        )
    else:
        accounts = get_ggad_accounts(refresh_cache=refresh_google_ads_caches)

    if account_code_filter:
        allowed = set(account_code_filter)
        accounts = [
            account
            for account in accounts
            if (standardize_account_code(account.get("accountCode")) or "") in allowed
        ]

    if cache_first:
        campaigns, budgets = _get_cached_or_fetch_campaigns_and_budgets(
            accounts,
            refresh_google_ads_caches=refresh_google_ads_caches,
            get_ggad_campaigns=get_ggad_campaigns,
            get_ggad_budgets=get_ggad_budgets,
        )
    else:
        campaigns = get_ggad_campaigns(
            accounts,
            refresh_cache=refresh_google_ads_caches,
        )
        budgets = get_ggad_budgets(
            accounts,
            refresh_cache=refresh_google_ads_caches,
        )

    costs = (
        get_ggad_spents(
            accounts,
            resolved_month,
            resolved_year,
            refresh_cache=refresh_google_ads_caches,
        )
        if include_costs
        else []
    )
    fallback_ad_types_by_budget = _build_fallback_ad_types_by_budget(campaigns)
    active_period = get_active_period(
        account_code_filter,
        resolved_month,
        resolved_year,
        as_of=period_date,
    )

    rows = transform_google_ads_data(
        master_budgets=master_budgets,
        campaigns=campaigns,
        budgets=budgets,
        costs=costs,
        allocations=allocations,
        rollovers=rollbreakdowns,
        accelerations=active_accelerations,
        activePeriod=active_period,
        fallback_ad_types_by_budget=fallback_ad_types_by_budget,
        today=period_date,
        include_transform_results=True,
    )

    return {
        "period": {
            "month": resolved_month,
            "year": resolved_year,
        },
        "rows": rows,
        "allocations": allocations,
    }


# =========================================================
# PIPELINE
# =========================================================


def run_google_ads_budget_pipeline(
    *,
    account_codes: list[str] | str | None = None,
    dry_run: bool = False,
    include_transform_results: bool = False,
    refresh_google_ads_caches: bool = False,
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
    accounts = get_ggad_accounts(refresh_cache=refresh_google_ads_caches)

    if account_code_filter:
        accounts = [
            acc
            for acc in accounts
            if (standardize_account_code(acc.get("accountCode")) or "")
            in account_code_filter
        ]

    def _get_campaigns(rows: list[dict]) -> list[dict]:
        return get_ggad_campaigns(
            rows,
            refresh_cache=refresh_google_ads_caches,
        )

    def _get_budgets(rows: list[dict]) -> list[dict]:
        return get_ggad_budgets(
            rows,
            refresh_cache=refresh_google_ads_caches,
        )

    def _get_spends(rows: list[dict]) -> list[dict]:
        return get_ggad_spents(
            rows,
            refresh_cache=refresh_google_ads_caches,
        )

    campaigns, budgets, costs, fallback_ad_types_by_budget = run_parallel(
        tasks=[
            (_get_campaigns, (accounts,)),
            (_get_budgets, (accounts,)),
            (_get_spends, (accounts,)),
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

    current_warnings_by_customer: dict[str, list[dict]] = (
        _extract_warnings_from_mutation_results(mutation_results)
    )

    _filter_existing_mutation_warnings(
        mutation_results=mutation_results,
        use_cache=not refresh_google_ads_caches,
    )
    _filter_existing_mutation_failures(
        mutation_results=mutation_results,
        use_cache=not refresh_google_ads_caches,
    )

    budget_warning_threshold = get_budget_warning_threshold()
    if budget_warning_threshold is not None:
        threshold_warnings_by_customer = _collect_budget_threshold_warnings(
            budget_payloads=budget_payloads,
            threshold=budget_warning_threshold,
        )
        _merge_warnings_by_customer(
            current_warnings_by_customer, threshold_warnings_by_customer
        )
        threshold_warnings_by_customer = _maybe_filter_google_ads_warnings(
            threshold_warnings_by_customer,
            use_cache=not refresh_google_ads_caches,
        )
        _inject_budget_warnings(
            mutation_results=mutation_results,
            warnings_by_customer=threshold_warnings_by_customer,
        )

    planned_budget_amounts = _build_planned_budget_amount_lookup(budget_payloads)
    budget_warnings, budget_failures = _collect_budget_allocation_and_spend_issues(
        rows=results,
        planned_new_amounts=planned_budget_amounts,
    )
    _merge_warnings_by_customer(current_warnings_by_customer, budget_warnings)
    budget_warnings = _maybe_filter_google_ads_warnings(
        budget_warnings,
        use_cache=not refresh_google_ads_caches,
    )
    budget_failures = _maybe_filter_google_ads_failures(
        budget_failures,
        use_cache=not refresh_google_ads_caches,
    )
    _inject_budget_warnings(
        mutation_results=mutation_results,
        warnings_by_customer=budget_warnings,
    )
    _inject_budget_failures(
        mutation_results=mutation_results,
        failures_by_customer=budget_failures,
    )

    current_period = get_current_period()
    ad_type_allocation_warnings = _collect_ad_type_allocation_total_warnings(
        rows=results,
        month=current_period["month"],
        year=current_period["year"],
    )
    _merge_warnings_by_customer(current_warnings_by_customer, ad_type_allocation_warnings)
    ad_type_allocation_warnings = _maybe_filter_google_ads_warnings(
        ad_type_allocation_warnings,
        use_cache=not refresh_google_ads_caches,
    )
    _inject_budget_warnings(
        mutation_results=mutation_results,
        warnings_by_customer=ad_type_allocation_warnings,
    )
    sync_google_ads_warning_states(current_warnings_by_customer)

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
