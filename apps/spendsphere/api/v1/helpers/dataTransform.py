# api/v1/helpers/dataTransform.py

from decimal import Decimal
from collections import defaultdict
from datetime import datetime, date, time
import calendar
import pytz

from shared.constants import GGADS_MIN_BUDGET_DELTA
from shared.logger import get_logger
from apps.spendsphere.api.v1.helpers.config import (
    get_google_ads_inactive_prefixes,
    get_service_mapping,
    is_google_ads_inactive_name,
)
from shared.tenant import get_timezone

logger = get_logger("Data Transform")


def _is_zzz_name(
    name: str | None,
    inactive_prefixes: tuple[str, ...] | None = None,
) -> bool:
    """
    Return True when a name matches configured inactive prefixes.

    Example:
        _is_zzz_name("zzz. old campaign") -> True
        _is_zzz_name("Brand Search") -> False
    """
    return is_google_ads_inactive_name(
        name,
        inactive_prefixes=inactive_prefixes,
    )


def _normalize_account_code(value: object) -> str | None:
    """
    Normalize account code values to uppercase trimmed tokens.

    Example:
        _normalize_account_code(" taaa ") -> "TAAA"
        _normalize_account_code("") -> None
    """
    normalized = str(value or "").strip().upper()
    return normalized or None

# ============================================================
# 1. MASTER BUDGET → AD TYPE MAPPING
# ============================================================


def master_budget_ad_type_mapping(master_budgets: list[dict]) -> list[dict]:
    """
    Aggregate master budgets by (accountCode, adTypeCode).

    Example:
        Input:
            master_budgets = [
                {"accountCode": "TAAA", "serviceId": "svc-1", "netAmount": 100},
                {"accountCode": "TAAA", "serviceId": "svc-2", "netAmount": 50},
            ]
        Output:
            [
                {
                    "accountCode": "TAAA",
                    "adTypeCode": "...",
                    "netAmount": Decimal("150"),
                    "services": [...],
                }
            ]
    """
    grouped = defaultdict(
        lambda: {
            "netAmount": Decimal("0"),
            "services": [],
        }
    )

    service_mapping = get_service_mapping()

    for mb in master_budgets:
        service_id = mb.get("serviceId")
        mapping = service_mapping.get(service_id)
        if not mapping:
            continue

        account_code = _normalize_account_code(mb.get("accountCode"))
        key = (account_code, mapping["adTypeCode"])
        net_amount = Decimal(str(mb.get("netAmount", 0)))

        grouped[key]["netAmount"] += net_amount
        grouped[key]["services"].append(
            {
                "serviceId": service_id,
                "serviceName": mapping["serviceName"],
                "netAmount": net_amount,
            }
        )

    return [
        {
            "accountCode": account,
            "adTypeCode": ad_type,
            "netAmount": values["netAmount"],
            "services": values["services"],
        }
        for (account, ad_type), values in grouped.items()
    ]


# ============================================================
# 2. JOIN CAMPAIGNS (FLAT)
# ============================================================


def master_budget_campaigns_join(
    master_budget_data: list[dict],
    campaigns: list[dict],
) -> list[dict]:
    """
    Join master budget data with campaigns by (accountCode, adTypeCode).

    Example:
        Input:
            master_budget_data = [{"accountCode": "TAAA", "adTypeCode": "SEM"}]
            campaigns = [{"accountCode": "TAAA", "adTypeCode": "SEM", "campaignId": "1"}]
        Output:
            [{"accountCode": "TAAA", "adTypeCode": "SEM", "campaignId": "1", ...}]
    """
    lookup = defaultdict(list)

    for c in campaigns:
        key = (_normalize_account_code(c.get("accountCode")), c.get("adTypeCode"))
        lookup[key].append(c)

    rows: list[dict] = []

    for mb in master_budget_data:
        key = (_normalize_account_code(mb.get("accountCode")), mb.get("adTypeCode"))
        for c in lookup.get(key, []):
            rows.append(
                {
                    **mb,
                    "customerId": c.get("customerId"),
                    "accountName": c.get("accountName"),
                    "campaignId": c.get("campaignId"),
                    "campaignName": c.get("campaignName"),
                    "budgetId": c.get("budgetId"),
                    "campaignStatus": c.get("status"),
                }
            )

    return rows


# ============================================================
# 3. JOIN GOOGLE BUDGETS (FLAT)
# ============================================================


def master_budget_google_budgets_join(
    rows: list[dict],
    budgets: list[dict],
) -> list[dict]:
    """
    Join Google Ads budget metadata by budgetId.

    Example:
        Input:
            rows = [{"budgetId": "123"}]
            budgets = [{"budgetId": "123", "budgetName": "Main", "amount": 200}]
        Output:
            [{"budgetId": "123", "budgetName": "Main", "budgetAmount": Decimal("200")}]
    """
    lookup = {b.get("budgetId"): b for b in budgets}

    return [
        {
            **r,
            "budgetName": lookup.get(r.get("budgetId"), {}).get("budgetName"),
            "budgetStatus": lookup.get(r.get("budgetId"), {}).get("status"),
            "budgetAmount": Decimal(
                str(lookup.get(r.get("budgetId"), {}).get("amount", 0))
            ),
        }
        for r in rows
    ]


# ============================================================
# 4. GROUP BY BUDGET (CORE CHANGE)
# ============================================================


def group_campaigns_by_budget(
    master_budget_data: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    allocations: list[dict],
    *,
    include_transform_results: bool,
    fallback_ad_types_by_budget: dict[tuple[str, str], str | None] | None = None,
) -> list[dict]:
    """
    Build budget-centric rows from campaign-level records.

    Behavior:
    - Primary rows are produced by matching campaign records to master budget rows
      using `(accountCode, adTypeCode)`.
    - Campaign-level costs are aggregated into one budget-level `totalCost`.
    - Budget metadata (`budgetName`, `budgetStatus`, `budgetAmount`) is filled
      from `budgets` using `budgetId`.
    - Fallback rows are also created for budgets with no matched campaigns when:
      1) the budget has an allocation, or
      2) its account exists in master budget data.
    - When `include_transform_results=True`, each row includes extra debugging
      fields (`services`, campaign-level `cost`, and joined `campaignNames` text).

    Example (normal grouped row):
        master_budget_data = [
            {
                "accountCode": "TAAA",
                "adTypeCode": "SEM",
                "netAmount": Decimal("1000"),
                "services": [{"serviceId": "svc-sem", "serviceName": "SEM", "netAmount": Decimal("1000")}],
            }
        ]
        campaigns = [
            {
                "customerId": "111",
                "accountCode": "TAAA",
                "adTypeCode": "SEM",
                "campaignId": "C1",
                "campaignName": "TAAA | SEM | Brand",
                "budgetId": "B1",
                "status": "ENABLED",
            },
            {
                "customerId": "111",
                "accountCode": "TAAA",
                "adTypeCode": "SEM",
                "campaignId": "C2",
                "campaignName": "TAAA | SEM | NonBrand",
                "budgetId": "B1",
                "status": "PAUSED",
            },
        ]
        budgets = [{"customerId": "111", "accountCode": "TAAA", "budgetId": "B1", "budgetName": "Main SEM", "status": "ENABLED", "amount": 80}]
        costs = [
            {"customerId": "111", "campaignId": "C1", "budgetId": "B1", "cost": 30},
            {"customerId": "111", "campaignId": "C2", "budgetId": "B1", "cost": 20},
        ]
        allocations = [{"accountCode": "TAAA", "ggBudgetId": "B1", "allocation": 60}]

        group_campaigns_by_budget(..., include_transform_results=True) returns a row like:
            {
                "ggAccountId": "111",
                "accountCode": "TAAA",
                "adTypeCode": "SEM",
                "netAmount": Decimal("1000"),
                "budgetId": "B1",
                "budgetName": "Main SEM",
                "budgetStatus": "ENABLED",
                "budgetAmount": Decimal("80"),
                "campaigns": [
                    {"campaignId": "C1", "campaignName": "TAAA | SEM | Brand", "status": "ENABLED", "cost": Decimal("30")},
                    {"campaignId": "C2", "campaignName": "TAAA | SEM | NonBrand", "status": "PAUSED", "cost": Decimal("20")},
                ],
                "totalCost": Decimal("50"),
                "services": [...],
                "campaignNames": "TAAA | SEM | Brand\\nTAAA | SEM | NonBrand",
            }

    Example (fallback row when no campaign matches):
        Suppose Google Ads has budget `B2` for account `TAAA`, but no campaign in
        `campaigns` matched `(accountCode="TAAA", adTypeCode=...)`.
        This function still emits a row for `B2` when either condition is true:
        1) Allocations contain the exact key `(accountCode="TAAA", ggBudgetId="B2")`
        2) Account `TAAA` appears in `master_budget_data` for the period
        This keeps important "orphan" budgets visible in downstream calculations.

        The emitted fallback row has:
        - `campaigns: []`
        - `adTypeCode` inferred from the first campaign found for
          `(customerId, budgetId)` when available (including optional
          raw-campaign fallback map); otherwise `None`
        - `netAmount: Decimal("0")`
        - `totalCost` derived from `costs` by `(customerId, budgetId)` when present.
    """
    cost_lookup: dict[tuple[str | None, str | None], Decimal] = defaultdict(
        lambda: Decimal("0")
    )
    budget_cost_lookup: dict[tuple[str | None, str | None], Decimal] = defaultdict(
        lambda: Decimal("0")
    )
    for c in costs:
        key = (c.get("customerId"), c.get("campaignId"))
        cost_lookup[key] += Decimal(str(c.get("cost", 0)))
        budget_key = (c.get("customerId"), c.get("budgetId"))
        budget_cost_lookup[budget_key] += Decimal(str(c.get("cost", 0)))

    budget_lookup = {b.get("budgetId"): b for b in budgets}
    master_lookup = {
        (_normalize_account_code(mb.get("accountCode")), mb.get("adTypeCode")): mb
        for mb in master_budget_data
    }
    accounts_with_master = {
        _normalize_account_code(mb.get("accountCode"))
        for mb in master_budget_data
        if _normalize_account_code(mb.get("accountCode"))
    }
    allocation_lookup = {
        (
            _normalize_account_code(a.get("accountCode")),
            str(a.get("ggBudgetId", "")).strip(),
        )
        for a in allocations
        if _normalize_account_code(a.get("accountCode"))
        and str(a.get("ggBudgetId", "")).strip()
    }
    # Preserve the first adType seen per (customerId, budgetId) so fallback rows
    # can still expose adTypeCode even when campaign rows are skipped later.
    first_ad_type_by_budget: dict[tuple[str, str], str | None] = {}
    for c in campaigns:
        customer_id = str(c.get("customerId", "")).strip()
        budget_id = str(c.get("budgetId", "")).strip()
        if not customer_id or not budget_id:
            continue
        key = (customer_id, budget_id)
        if key in first_ad_type_by_budget:
            continue
        ad_type = str(c.get("adTypeCode", "")).strip()
        first_ad_type_by_budget[key] = ad_type or None
    if fallback_ad_types_by_budget:
        for raw_key, raw_ad_type in fallback_ad_types_by_budget.items():
            try:
                customer_id_raw, budget_id_raw = raw_key
            except Exception:
                continue
            customer_id = str(customer_id_raw or "").strip()
            budget_id = str(budget_id_raw or "").strip()
            if not customer_id or not budget_id:
                continue
            key = (customer_id, budget_id)
            if key in first_ad_type_by_budget:
                continue
            ad_type = str(raw_ad_type or "").strip()
            first_ad_type_by_budget[key] = ad_type or None

    grouped: dict[tuple[str, str], dict] = {}

    for c in campaigns:
        account_code = _normalize_account_code(c.get("accountCode"))
        ad_type = c.get("adTypeCode")
        master = master_lookup.get((account_code, ad_type))
        if not master:
            continue

        customer_id = c.get("customerId")
        budget_id = c.get("budgetId")
        if not budget_id:
            continue

        group_key = (customer_id, budget_id)

        if group_key not in grouped:
            budget_meta = budget_lookup.get(budget_id, {})
            group = {
                "ggAccountId": customer_id,
                "accountCode": master.get("accountCode"),
                "adTypeCode": master.get("adTypeCode"),
                "netAmount": master.get("netAmount"),
                "budgetId": budget_id,
                "budgetName": budget_meta.get("budgetName"),
                "budgetStatus": budget_meta.get("status"),
                "budgetAmount": Decimal(
                    str(budget_meta.get("amount", 0))
                ),
                "campaigns": [],
                "totalCost": Decimal("0"),
            }
            if include_transform_results:
                group["services"] = master.get("services", [])
                group["campaignNames"] = ""
                group["_campaign_names"] = []  # internal helper
            grouped[group_key] = group

        campaign_id = c.get("campaignId")
        campaign_name = c.get("campaignName")

        grouped[group_key]["campaigns"].append(
            {
                "campaignId": campaign_id,
                "campaignName": campaign_name,
                "status": c.get("status"),
                **(
                    {
                        "cost": cost_lookup.get(
                            (customer_id, campaign_id), Decimal("0")
                        )
                    }
                    if include_transform_results
                    else {}
                ),
            }
        )

        cost_value = cost_lookup.get((customer_id, campaign_id), Decimal("0"))
        grouped[group_key]["totalCost"] += cost_value

        if include_transform_results and campaign_name:
            grouped[group_key]["_campaign_names"].append(campaign_name)

    # finalize campaignNames
    for budget in budgets:
        customer_id = budget.get("customerId")
        budget_id = str(budget.get("budgetId", "")).strip()
        if not customer_id or not budget_id:
            continue

        group_key = (customer_id, budget_id)
        if group_key in grouped:
            continue

        account_code = _normalize_account_code(budget.get("accountCode"))
        has_allocation = (account_code, budget_id) in allocation_lookup
        has_masterbudget = account_code in accounts_with_master
        if not has_allocation and not has_masterbudget:
            continue

        inferred_ad_type = first_ad_type_by_budget.get(group_key)
        fallback_group = {
            "ggAccountId": customer_id,
            "accountCode": account_code or budget.get("accountCode"),
            "accountName": budget.get("accountName"),
            "adTypeCode": inferred_ad_type,
            "netAmount": Decimal("0"),
            "budgetId": budget_id,
            "budgetName": budget.get("budgetName"),
            "budgetStatus": budget.get("status"),
            "budgetAmount": Decimal(str(budget.get("amount", 0))),
            "campaigns": [],
            "totalCost": budget_cost_lookup.get(group_key, Decimal("0")),
        }
        if include_transform_results:
            fallback_group["services"] = []
            fallback_group["campaignNames"] = ""
            fallback_group["_campaign_names"] = []
        grouped[group_key] = fallback_group

    if include_transform_results:
        for b in grouped.values():
            names = b.pop("_campaign_names", [])
            b["campaignNames"] = "\n".join(sorted(set(names)))

    return list(grouped.values())


# ============================================================
# 6. JOIN ALLOCATIONS & ROLLOVERS (BUDGET LEVEL)
# ============================================================


def budget_allocation_join(
    budgets: list[dict],
    allocations: list[dict],
) -> list[dict]:
    """
    Attach `allocation` percentage to each budget row.

    Example:
        budgets = [{"accountCode": "TAAA", "budgetId": "123"}]
        allocations = [{"accountCode": "TAAA", "ggBudgetId": "123", "allocation": 60}]
        -> [{"accountCode": "TAAA", "budgetId": "123", "allocation": Decimal("60")}]
    """
    lookup = {
        (
            _normalize_account_code(a.get("accountCode")),
            str(a.get("ggBudgetId", "")).strip(),
        ): Decimal(
            str(a.get("allocation", 0))
        )
        for a in allocations
        if _normalize_account_code(a.get("accountCode"))
        and str(a.get("ggBudgetId", "")).strip()
    }

    for b in budgets:
        b["allocation"] = lookup.get(
            (
                _normalize_account_code(b.get("accountCode")),
                str(b.get("budgetId", "")).strip(),
            ),
            None,
        )

    return budgets


def budget_rollover_join(
    budgets: list[dict],
    rollovers: list[dict],
) -> list[dict]:
    """
    Attach rollover amount by `(accountCode, adTypeCode)` to budget rows.

    Example:
        budgets = [{"accountCode": "TAAA", "adTypeCode": "SEM"}]
        rollovers = [{"accountCode": "TAAA", "adTypeCode": "SEM", "amount": 120}]
        -> [{"accountCode": "TAAA", "adTypeCode": "SEM", "rolloverAmount": Decimal("120")}]
    """
    lookup = {
        (
            _normalize_account_code(r.get("accountCode")),
            str(r.get("adTypeCode", "")).strip(),
        ): Decimal(
            str(r.get("amount", 0))
        )
        for r in rollovers
        if _normalize_account_code(r.get("accountCode"))
    }

    for b in budgets:
        b["rolloverAmount"] = lookup.get(
            (
                _normalize_account_code(b.get("accountCode")),
                str(b.get("adTypeCode", "")).strip(),
            ),
            Decimal("0"),
        )

    return budgets


# ============================================================
# 7. JOIN ACTIVE PERIOD
# ============================================================


def _coerce_date(value) -> date | None:
    """
    Parse supported date-like inputs into `date`.

    Example:
        _coerce_date("2026-02-01") -> date(2026, 2, 1)
        _coerce_date("02/01/26") -> date(2026, 2, 1)
        _coerce_date("bad") -> None
    """
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(value.strip(), fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(value.strip()).date()
        except ValueError:
            return None
    return None


def _coerce_datetime(value) -> datetime | None:
    """
    Parse supported datetime-like inputs into `datetime`.

    Example:
        _coerce_datetime("2026-02-01 08:30:00") -> datetime(...)
        _coerce_datetime(date(2026, 2, 1)) -> datetime(2026, 2, 1, 0, 0)
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, str) and value.strip():
        value = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def budget_activePeriod_join(
    budgets: list[dict],
    activePeriod: list[dict] | None,
    today: date | None = None,
) -> list[dict]:
    """
    Attach active-period fields (`startDate`, `endDate`, `isActive`) to budget rows.

    Example:
        activePeriod = [{"accountCode": "TAAA", "startDate": "2026-02-01", "endDate": "2026-02-28"}]
        budgets = [{"accountCode": "TAAA", "budgetId": "123"}]
        -> [{"accountCode": "TAAA", "budgetId": "123", "isActive": True, ...}]
    """
    tz = pytz.timezone(get_timezone())
    now = datetime.now(tz)
    if not today:
        today = now.date()

    lookup: dict[str, dict] = {}

    for ap in activePeriod or []:
        account_code = _normalize_account_code(ap.get("accountCode"))
        if not account_code:
            continue
        lookup[account_code] = ap

    for b in budgets:
        account_code = _normalize_account_code(b.get("accountCode")) or ""
        ap = lookup.get(account_code, {})

        start_date_raw = ap.get("startDate")
        end_date_raw = ap.get("endDate")
        start_date = _coerce_date(start_date_raw)
        end_date = _coerce_date(end_date_raw)

        if "isActive" in ap:
            is_active = bool(ap.get("isActive"))
        else:
            if start_date is None:
                start_ok = True
            else:
                start_dt = tz.localize(datetime.combine(start_date, time.min))
                start_ok = now >= start_dt

            if end_date is None:
                end_ok = True
            else:
                end_dt = tz.localize(datetime.combine(end_date, time.max))
                end_ok = now <= end_dt

            is_active = start_ok and end_ok

        b["startDate"] = start_date_raw
        b["endDate"] = end_date_raw
        b["isActive"] = is_active

    return budgets


# ============================================================
# 8. CALCULATE DAILY BUDGET (BUDGET LEVEL)
# ============================================================


def calculate_daily_budget(
    budgets: list[dict],
    today: date | None = None,
) -> list[dict]:
    """
    Calculate budget math fields at budget level.

    Adds/updates:
    - `daysLeft`
    - `allocatedBudgetBeforeAcceleration`
    - `remainingBudget`
    - `dailyBudget` (0 when inactive)

    Example:
        Input row has `netAmount=1000`, `rolloverAmount=100`, `allocation=50`, `totalCost=300`
        Output row includes computed `dailyBudget` and `remainingBudget`.
    """

    if not today:
        tz = pytz.timezone(get_timezone())
        today = datetime.now(tz).date()

    days_in_month = calendar.monthrange(today.year, today.month)[1]
    month_days_left = days_in_month - today.day + 1

    for b in budgets:
        days_left_value = month_days_left
        end_date = _coerce_date(b.get("endDate"))
        if end_date and end_date.year == today.year and end_date.month == today.month:
            days_left_value = (end_date - today).days + 1
            if days_left_value < 0:
                days_left_value = 0
        b["daysLeft"] = int(days_left_value)
        days_left = Decimal(str(days_left_value))

        total_cost = b.get("totalCost")
        if total_cost is None:
            total_cost = sum(c.get("cost", 0) for c in b.get("campaigns", []))
        net = Decimal(str(b.get("netAmount", 0)))
        rollover = Decimal(str(b.get("rolloverAmount", 0)))
        allocation = b.get("allocation")

        total_cost_decimal = Decimal(str(total_cost))
        b["totalCost"] = total_cost_decimal.quantize(Decimal("0.01"))

        # 🔹 Handle missing allocation
        if allocation is None:
            b["allocatedBudgetBeforeAcceleration"] = None
            b["remainingBudget"] = None
            b["dailyBudget"] = None
            continue

        allocation_pct = Decimal(str(allocation)) / Decimal("100")
        allocated_budget_base_raw = (net + rollover) * allocation_pct
        b["allocatedBudgetBeforeAcceleration"] = allocated_budget_base_raw.quantize(
            Decimal("0.01")
        )
        remaining_base = allocated_budget_base_raw - total_cost_decimal
        daily_base = remaining_base / days_left if days_left > 0 else Decimal("0")

        accel_multiplier = Decimal(str(b.get("accelerationMultiplier", 100)))
        accel_ratio = accel_multiplier / Decimal("100")

        remaining = allocated_budget_base_raw * accel_ratio - total_cost_decimal
        daily = remaining / days_left if days_left > 0 else Decimal("0")

        b["remainingBudget"] = remaining.quantize(Decimal("0.01"))

        if accel_multiplier != Decimal("100"):
            b["dailyBudgetBase"] = daily_base.quantize(Decimal("0.01"))

        if b.get("isActive") is False:
            b["dailyBudget"] = Decimal("0.00")
        else:
            b["dailyBudget"] = daily.quantize(Decimal("0.01"))

    return budgets


# ============================================================
# 9. APPLY ACCELERATIONS
# ============================================================


def apply_budget_accelerations(
    budgets: list[dict],
    accelerations: list[dict] | None = None,
) -> list[dict]:
    """
    Apply the best matching acceleration to each budget row.

    Precedence:
        BUDGET scope > AD_TYPE scope > ACCOUNT scope
    Tie-break:
        latest `dateUpdated/dateCreated`, then highest id

    Example:
        If account has ACCOUNT=120 and budget has BUDGET=150,
        the row receives `accelerationMultiplier=Decimal("150")`.
    """
    if not accelerations:
        return budgets

    account_accels: dict[str, list[dict]] = defaultdict(list)
    ad_type_accels: dict[tuple[str, str], list[dict]] = defaultdict(list)
    budget_accels: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for accel in accelerations:
        account_code = str(accel.get("accountCode", "")).upper()
        scope_type = str(accel.get("scopeLevel", "")).upper()
        scope_value = str(accel.get("scopeValue", "")).strip()

        if not account_code or not scope_type:
            continue

        if scope_type == "ACCOUNT":
            account_accels[account_code].append(accel)
        elif scope_type == "AD_TYPE" and scope_value:
            ad_type_accels[(account_code, scope_value)].append(accel)
        elif scope_type == "BUDGET" and scope_value:
            budget_accels[(account_code, scope_value)].append(accel)

    def _accel_sort_key(a: dict) -> tuple[datetime, int]:
        updated = _coerce_datetime(a.get("dateUpdated") or a.get("dateCreated"))
        try:
            accel_id = int(a.get("id") or 0)
        except (TypeError, ValueError):
            accel_id = 0
        return (updated or datetime.min, accel_id)

    def _best(accels: list[dict] | None) -> dict | None:
        if not accels:
            return None
        return max(accels, key=_accel_sort_key)

    for b in budgets:
        account_code = str(b.get("accountCode", "")).upper()
        budget_id = str(b.get("budgetId", "")).strip()
        ad_type = str(b.get("adTypeCode", "")).strip()

        accel = None
        if budget_id:
            accel = _best(budget_accels.get((account_code, budget_id)))
        if accel is None and ad_type:
            accel = _best(ad_type_accels.get((account_code, ad_type)))
        if accel is None:
            accel = _best(account_accels.get(account_code))

        if not accel:
            continue

        multiplier = Decimal(str(accel.get("multiplier", 0)))
        if multiplier <= 0:
            continue

        b["accelerationId"] = accel.get("id")
        b["accelerationMultiplier"] = multiplier

    return budgets


# ============================================================
# 10. UPDATE PAYLOADS
# ============================================================


def generate_update_payloads(data: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Convert transformed rows into Google Ads mutation payloads.

    Returns:
    - budget_payloads: grouped by customer_id for budget amount updates
    - campaign_payloads: grouped by customer_id for campaign status updates

    Example:
        Input:
            [{"ggAccountId": "1", "budgetId": "B1", "dailyBudget": Decimal("25.00"), ...}]
        Output:
            ([{"customer_id": "1", "updates": [...]}], [{"customer_id": "1", "updates": [...]}])
    """
    budget_updates: dict[str, list[dict]] = {}
    campaign_updates: dict[str, dict[str, dict]] = {}
    inactive_prefixes = get_google_ads_inactive_prefixes()

    for row in data:
        customer_id = row["ggAccountId"]
        allocation = row.get("allocation")
        daily_budget_raw = row.get("dailyBudget")
        budget_amount_raw = row.get("budgetAmount")

        is_inactive = row.get("isActive") is False

        if allocation is None and not is_inactive:
            continue

        if allocation is None and is_inactive:
            expected_status = "PAUSED"
            daily_budget = None
        else:
            if daily_budget_raw is None:
                continue

            daily_budget = Decimal(daily_budget_raw)

            # Inline expected status logic
            if is_inactive:
                expected_status = "PAUSED"
            else:
                expected_status = (
                    "ENABLED" if daily_budget >= Decimal("0.01") else "PAUSED"
                )

        campaigns = row.get("campaigns", [])
        campaign_names = [
            c.get("campaignName")
            for c in campaigns
            if c.get("campaignName")
        ]

        # -------------------------
        # Campaign status updates (independent)
        # -------------------------
        for campaign in campaigns:
            if _is_zzz_name(
                campaign.get("campaignName"),
                inactive_prefixes=inactive_prefixes,
            ):
                continue
            if campaign["status"] != expected_status:
                customer_updates = campaign_updates.setdefault(customer_id, {})
                customer_updates[str(campaign["campaignId"])] = {
                    "campaignId": campaign["campaignId"],
                    "campaignName": campaign.get("campaignName"),
                    "oldStatus": campaign["status"],
                    "newStatus": expected_status,
                    "accountCode": row.get("accountCode"),
                }

        # -------------------------
        # Budget updates (stricter rules)
        # -------------------------
        if not campaigns:
            continue
        if daily_budget is None:
            continue
        if budget_amount_raw is None:
            continue

        budget_amount = Decimal(budget_amount_raw)

        # Enforce Google Ads minimum
        amount_to_set = (
            Decimal("0.01") if daily_budget <= Decimal("0") else daily_budget
        )

        # Skip small changes unless targeting 0.00/0.01
        if amount_to_set not in (Decimal("0"), Decimal("0.01")):
            if abs(amount_to_set - budget_amount) <= Decimal(
                str(GGADS_MIN_BUDGET_DELTA)
            ):
                continue

        # Only update when values differ (after min floor)
        if amount_to_set == budget_amount:
            continue

        budget_updates.setdefault(customer_id, []).append(
            {
                "budgetId": row["budgetId"],
                "accountCode": row.get("accountCode"),
                "customerName": row.get("accountName"),
                "campaignNames": campaign_names,
                "currentAmount": float(budget_amount),
                "newAmount": float(amount_to_set),
                "remainingBudget": (
                    float(row.get("remainingBudget"))
                    if row.get("remainingBudget") is not None
                    else None
                ),
                "daysLeft": row.get("daysLeft"),
            }
        )

    budget_payloads = [
        {"customer_id": cid, "updates": updates}
        for cid, updates in budget_updates.items()
    ]

    campaign_payloads = [
        {"customer_id": cid, "updates": list(updates.values())}
        for cid, updates in campaign_updates.items()
    ]

    logger.debug(
        "Payload Data",
        extra={
            "extra_fields": {
                "operation": "generate_update_payloads",
                "budget_payloads": budget_payloads,
                "campaign_payloads": campaign_payloads,
            }
        },
    )

    return budget_payloads, campaign_payloads


# ============================================================
# 10. PIPELINE ORCHESTRATOR
# ============================================================


def _build_budget_rows(
    master_budgets: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    allocations: list[dict],
    rollovers: list[dict],
    accelerations: list[dict] | None = None,
    activePeriod: list[dict] | None = None,
    fallback_ad_types_by_budget: dict[tuple[str, str], str | None] | None = None,
    *,
    today: date | None = None,
    include_transform_results: bool = True,
) -> list[dict]:
    """
    Run the full in-memory transformation pipeline and return budget rows.

    Pipeline stages:
    1. Master budget aggregation by ad type
    2. Campaign grouping by budget
    3. Allocation join
    4. Rollover join
    5. Active period join
    6. Acceleration application
    7. Daily budget calculation

    Example:
        rows = _build_budget_rows(
            master_budgets=[...],
            campaigns=[...],
            budgets=[...],
            costs=[...],
            allocations=[...],
            rollovers=[...],
        )
    """

    step1 = master_budget_ad_type_mapping(master_budgets)
    step2 = group_campaigns_by_budget(
        step1,
        campaigns,
        budgets,
        costs,
        allocations,
        include_transform_results=include_transform_results,
        fallback_ad_types_by_budget=fallback_ad_types_by_budget,
    )
    step3 = budget_allocation_join(step2, allocations)
    step4 = budget_rollover_join(step3, rollovers)
    step5 = budget_activePeriod_join(step4, activePeriod)
    step6 = apply_budget_accelerations(step5, accelerations)
    step7 = calculate_daily_budget(
        step6,
        today=today,
    )

    if not include_transform_results:
        return list(step7)

    # --------------------------------------------------
    # SORT RESULTS (accountCode ASC, adTypeCode DESC)
    # --------------------------------------------------
    result = list(step7)
    result.sort(key=lambda r: (r.get("adTypeCode") or ""), reverse=True)
    result.sort(
        key=lambda r: (r.get("accountCode") is None, r.get("accountCode") or "")
    )

    return result


def transform_google_ads_data(
    master_budgets: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    allocations: list[dict],
    rollovers: list[dict],
    accelerations: list[dict] | None = None,
    activePeriod: list[dict] | None = None,
    fallback_ad_types_by_budget: dict[tuple[str, str], str | None] | None = None,
    *,
    today: date | None = None,
    include_transform_results: bool = True,
) -> list[dict]:
    """
    Public wrapper to produce transformed budget rows.

    Example:
        rows = transform_google_ads_data(
            master_budgets=[...],
            campaigns=[...],
            budgets=[...],
            costs=[...],
            allocations=[...],
            rollovers=[...],
            activePeriod=[...],
        )
    """
    return _build_budget_rows(
        master_budgets,
        campaigns,
        budgets,
        costs,
        allocations,
        rollovers,
        accelerations=accelerations,
        activePeriod=activePeriod,
        fallback_ad_types_by_budget=fallback_ad_types_by_budget,
        today=today,
        include_transform_results=include_transform_results,
    )


def build_update_payloads_from_inputs(
    master_budgets: list[dict],
    campaigns: list[dict],
    budgets: list[dict],
    costs: list[dict],
    allocations: list[dict],
    rollovers: list[dict],
    accelerations: list[dict] | None = None,
    activePeriod: list[dict] | None = None,
    fallback_ad_types_by_budget: dict[tuple[str, str], str | None] | None = None,
    *,
    include_transform_results: bool = False,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Build mutation payloads directly from raw input collections.

    Returns:
    - budget_payloads
    - campaign_payloads
    - rows (transformed rows; included always for downstream inspection)

    Example:
        budget_payloads, campaign_payloads, rows = build_update_payloads_from_inputs(
            master_budgets=[...],
            campaigns=[...],
            budgets=[...],
            costs=[...],
            allocations=[...],
            rollovers=[...],
            include_transform_results=True,
        )
    """
    rows = _build_budget_rows(
        master_budgets,
        campaigns,
        budgets,
        costs,
        allocations,
        rollovers,
        accelerations=accelerations,
        activePeriod=activePeriod,
        fallback_ad_types_by_budget=fallback_ad_types_by_budget,
        include_transform_results=include_transform_results,
    )
    budget_payloads, campaign_payloads = generate_update_payloads(rows)
    return budget_payloads, campaign_payloads, rows
