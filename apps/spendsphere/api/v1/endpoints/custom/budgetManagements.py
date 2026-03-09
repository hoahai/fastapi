from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Callable, Literal

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from apps.spendsphere.api.v1.deps import require_feature
from apps.spendsphere.api.v1.endpoints.core.periods import (
    build_periods_data,
    validate_month_offsets,
)
from apps.spendsphere.api.v1.endpoints.custom.spreadsheetParser_nucar import (
    calculate_nucar_spreadsheet_budgets,
    get_nucar_recommended_budgets,
    sync_nucar_master_budget_sheet,
)
from apps.spendsphere.api.v1.helpers.account_codes import standardize_account_code
from apps.spendsphere.api.v1.helpers.config import get_service_mapping
from apps.spendsphere.api.v1.helpers.db_queries import (
    get_masterbudgets,
    soft_delete_masterbudget,
    upsert_masterbudgets,
)
from apps.spendsphere.api.v1.helpers.ggAd import (
    get_ggad_accounts,
    get_ggad_campaigns,
    get_ggad_spents,
)
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    get_google_sheet_cache_entry,
    get_services_cache_entry,
    normalize_account_codes,
    refresh_services_cache,
    set_google_sheet_cache,
    validate_account_codes,
)
from shared.tenant import get_tenant_id
from shared.utils import get_current_period

_budget_managements_feature_dependency = require_feature("budget_managements")
_BUDGET_OVERVIEW_CACHE_KEY_PREFIX = "budget_management_overview"
_BUDGET_OVERVIEW_CACHE_HASH = "budget_management_overview::v13"
_ADTYPE_PRIORITY_ORDER = ("SEM", "PM", "DIS", "VID", "DM")
_ADTYPE_PRIORITY_RANK = {
    code: index for index, code in enumerate(_ADTYPE_PRIORITY_ORDER)
}


class BudgetManagementUpsertItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str | None = None
    accountCode: str = Field(min_length=1)
    serviceId: str = Field(min_length=1)
    subService: str | None = None
    netAmount: float


class BudgetManagementUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    month: int | None = None
    year: int | None = None
    rows: list[BudgetManagementUpsertItem] = Field(min_length=1)


class BudgetManagementChangeItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    op: Literal["create", "update", "delete"]
    id: str | None = None
    accountCode: str | None = None
    serviceId: str | None = None
    subService: str | None = None
    note: str | None = None
    amount: float | None = None


class BudgetManagementChangesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    month: int | None = None
    year: int | None = None
    changes: list[BudgetManagementChangeItem] = Field(min_length=1)


def _resolve_period(month: int | None, year: int | None) -> tuple[int, int]:
    if (month is None) != (year is None):
        raise HTTPException(
            status_code=400,
            detail="month and year must be provided together",
        )
    if month is None and year is None:
        period = get_current_period()
        month = period["month"]
        year = period["year"]
    if month is None or year is None:
        raise HTTPException(status_code=400, detail="Invalid period")
    if not 1 <= month <= 12:
        raise HTTPException(status_code=400, detail="month must be between 1 and 12")
    if not 2000 <= year <= 2100:
        raise HTTPException(status_code=400, detail="year must be between 2000 and 2100")
    return month, year


def _resolve_previous_period(month: int, year: int) -> tuple[int, int]:
    if month == 1:
        return 12, year - 1
    return month - 1, year


def _get_service_mapping_entry(
    service_mapping: dict,
    service_id: str,
) -> dict[str, object]:
    if not isinstance(service_mapping, dict):
        return {}
    for key in (service_id, service_id.upper(), service_id.lower()):
        entry = service_mapping.get(key)
        if isinstance(entry, dict):
            return entry
    return {}


def _to_decimal(value: object, *, fallback: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return fallback


def _build_budget_overview_cache_key(month: int, year: int) -> str:
    return f"{_BUDGET_OVERVIEW_CACHE_KEY_PREFIX}::{year:04d}-{month:02d}"


def _format_decimal_2(value: object) -> str:
    amount = _to_decimal(value)
    return f"{amount.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP):.2f}"


def _adtype_priority_rank(ad_type_code: str) -> tuple[int, str]:
    normalized = str(ad_type_code or "").strip().upper()
    return (_ADTYPE_PRIORITY_RANK.get(normalized, len(_ADTYPE_PRIORITY_ORDER)), normalized)


def _pick_preferred_adtype(current: str | None, candidate: str | None) -> str | None:
    current_normalized = str(current or "").strip().upper() or None
    candidate_normalized = str(candidate or "").strip().upper() or None
    if candidate_normalized is None:
        return current_normalized
    if current_normalized is None:
        return candidate_normalized
    return (
        candidate_normalized
        if _adtype_priority_rank(candidate_normalized) < _adtype_priority_rank(current_normalized)
        else current_normalized
    )


def _sum_db_budget_by_account_adtype(
    budget_rows: list[dict],
    *,
    service_mapping: dict,
) -> dict[tuple[str, str], Decimal]:
    totals: dict[tuple[str, str], Decimal] = {}
    for row in budget_rows:
        account_code = standardize_account_code(row.get("accountCode"))
        service_id = str(row.get("serviceId") or "").strip()
        mapping = _get_service_mapping_entry(service_mapping, service_id)
        ad_type_code = str(mapping.get("adTypeCode") or "").strip().upper()
        if not account_code or not ad_type_code:
            continue
        key = (account_code, ad_type_code)
        totals[key] = totals.get(key, Decimal("0")) + _to_decimal(row.get("netAmount"))
    return totals


def _sum_google_spend_by_account_adtype(
    account_codes: list[str],
    *,
    month: int,
    year: int,
) -> dict[tuple[str, str], Decimal]:
    if not account_codes:
        return {}

    requested_codes = set(account_codes)
    google_accounts = get_ggad_accounts(refresh_cache=False)
    target_accounts = [
        account
        for account in google_accounts
        if (standardize_account_code(account.get("accountCode")) or "") in requested_codes
    ]
    if not target_accounts:
        return {}

    campaigns = get_ggad_campaigns(target_accounts, refresh_cache=False)
    campaign_adtype_by_campaign: dict[tuple[str, str], str] = {}
    campaign_adtype_by_budget: dict[tuple[str, str], str] = {}
    for campaign in campaigns:
        customer_id = str(campaign.get("customerId") or "").strip()
        campaign_id = str(campaign.get("campaignId") or "").strip()
        budget_id = str(campaign.get("budgetId") or "").strip()
        ad_type = str(campaign.get("adTypeCode") or "").strip().upper()
        if not customer_id or not ad_type:
            continue
        if campaign_id:
            campaign_adtype_by_campaign[(customer_id, campaign_id)] = ad_type
        if budget_id:
            key = (customer_id, budget_id)
            campaign_adtype_by_budget[key] = _pick_preferred_adtype(
                campaign_adtype_by_budget.get(key),
                ad_type,
            )

    spends = get_ggad_spents(
        target_accounts,
        month=month,
        year=year,
    )
    totals: dict[tuple[str, str], Decimal] = {}
    for spend_row in spends:
        account_code = standardize_account_code(spend_row.get("accountCode"))
        customer_id = str(spend_row.get("customerId") or "").strip()
        campaign_id = str(spend_row.get("campaignId") or "").strip()
        budget_id = str(spend_row.get("budgetId") or "").strip()
        ad_type = campaign_adtype_by_campaign.get((customer_id, campaign_id))
        if ad_type is None and budget_id:
            ad_type = campaign_adtype_by_budget.get((customer_id, budget_id))
        if not account_code or not ad_type:
            continue
        key = (account_code, ad_type)
        totals[key] = totals.get(key, Decimal("0")) + _to_decimal(spend_row.get("cost"))

    return totals


def _resolve_spreadsheet_parser(
    tenant_id: str | None,
) -> Callable[[list[str], int, int], list[dict[str, object]]] | None:
    parsers = {
        "nucar": calculate_nucar_spreadsheet_budgets,
    }
    key = str(tenant_id or "").strip().lower()
    return parsers.get(key)


def _resolve_recommended_budget_parser(
    tenant_id: str | None,
) -> Callable[[str, str | None, int, int], list[dict[str, object]]] | None:
    parsers = {
        "nucar": get_nucar_recommended_budgets,
    }
    key = str(tenant_id or "").strip().lower()
    return parsers.get(key)


def _resolve_master_budget_sheet_syncer(
    tenant_id: str | None,
) -> Callable[..., dict[str, object]] | None:
    parsers = {
        "nucar": sync_nucar_master_budget_sheet,
    }
    key = str(tenant_id or "").strip().lower()
    return parsers.get(key)


def ensure_budget_managements_access() -> None:
    _budget_managements_feature_dependency()


def _filter_and_sort_services(rows: list[dict]) -> list[dict]:
    filtered: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ad_type_code = str(row.get("adTypeCode", "")).strip()
        if not ad_type_code:
            continue
        filtered.append(
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "adTypeCode": ad_type_code,
            }
        )
    filtered.sort(
        key=lambda item: (
            str(item.get("adTypeCode", "")).strip(),
            str(item.get("name", "")).strip().lower(),
        )
    )
    return filtered


def get_budget_management_selections_data(
    *,
    months_before: int = 2,
    months_after: int = 1,
    refresh_service_cache: bool = False,
) -> dict[str, object]:
    """
    Get budget-management UI selection data.

    Returns period options, active services, and active accounts.
    """
    validate_month_offsets(months_before, months_after)

    periods = build_periods_data(months_before, months_after)

    services = get_services_cache_entry()
    if refresh_service_cache or services is None:
        services = refresh_services_cache(department_code="DIGM")
    services_payload = _filter_and_sort_services(services)

    accounts = validate_account_codes(None)
    accounts_payload = sorted(
        [
            {
                "id": account.get("id"),
                "descriptiveName": account.get("descriptiveName"),
                "accountCode": standardize_account_code(
                    account.get("accountCode") or account.get("code")
                ),
                "accountName": str(
                    account.get("accountName")
                    or account.get("name")
                    or account.get("descriptiveName")
                    or ""
                ).strip(),
            }
            for account in accounts
        ],
        key=lambda item: (
            str(item.get("accountCode") or "").casefold(),
            str(item.get("accountName") or "").casefold(),
        ),
    )
    accounts_payload = [
        account for account in accounts_payload if str(account.get("accountCode") or "").strip()
    ]

    return {
        "periods": periods,
        "services": services_payload,
        "accounts": accounts_payload,
    }


def get_budget_managements(
    account_codes: list[str] | None = None,
    month: int | None = None,
    year: int | None = None,
):
    """
    Get budgets and optional tenant-calculated spreadsheet budgets.

    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament?accountCodes=NUCAR&month=1&year=2026

    Example response:
        {
          "budgets": [],
          "calculatedBudgets": [
            {
              "accountCode": "NUCAR",
              "calculatedBudget": 2500.0,
              "source": "spreadsheet",
              "sourceRows": 3
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
    """
    requested_codes = normalize_account_codes(account_codes)
    if not requested_codes:
        raise HTTPException(status_code=400, detail="accountCodes is required")

    month, year = _resolve_period(month, year)
    validate_account_codes(requested_codes, month=month, year=year)

    budgets = get_masterbudgets(requested_codes, month, year)
    payload: dict[str, object] = {"budgets": budgets}

    parser = _resolve_spreadsheet_parser(get_tenant_id())
    if parser is not None:
        payload["calculatedBudgets"] = parser(requested_codes, month, year)

    return payload


def get_budget_management_db_rows(
    month: int | None = None,
    year: int | None = None,
):
    """
    Get DB budget rows for all active accounts in a period.

    Response includes:
    - tableData: account/service budget rows with previousMonthUnderspent
    - spentData: current-period Google spend rows with accountCode/adTypeCode/spent
    `previousMonthUnderspent` calculated as:
    previous-month DB budget minus previous-month Google spend, grouped by
    (accountCode, adType).
    Rows are sorted by accountCode, then adType priority:
    SEM > PM > DIS > VID > DM.
    """
    period_month, period_year = _resolve_period(month, year)
    previous_month, previous_year = _resolve_previous_period(period_month, period_year)
    cache_key = _build_budget_overview_cache_key(period_month, period_year)

    cached_payload, is_stale = get_google_sheet_cache_entry(
        cache_key,
        config_hash=_BUDGET_OVERVIEW_CACHE_HASH,
    )
    if cached_payload is not None and not is_stale:
        table_data: list[dict[str, object]] = []
        spent_data: list[dict[str, object]] = []
        if isinstance(cached_payload, list) and cached_payload:
            first_item = cached_payload[0]
            if (
                len(cached_payload) == 1
                and isinstance(first_item, dict)
                and (
                    "tableData" in first_item
                    or "spentData" in first_item
                )
            ):
                cached_table_data = first_item.get("tableData")
                cached_spent_data = first_item.get("spentData")
                if isinstance(cached_table_data, list):
                    table_data = cached_table_data
                if isinstance(cached_spent_data, list):
                    spent_data = cached_spent_data
            else:
                table_data = cached_payload
        elif isinstance(cached_payload, dict):
            cached_table_data = cached_payload.get("tableData")
            cached_spent_data = cached_payload.get("spentData")
            if isinstance(cached_table_data, list):
                table_data = cached_table_data
            if isinstance(cached_spent_data, list):
                spent_data = cached_spent_data
        return {
            "period": {"month": period_month, "year": period_year},
            "previousPeriod": {"month": previous_month, "year": previous_year},
            "tableData": table_data,
            "spentData": spent_data,
        }

    accounts = validate_account_codes(
        None,
        month=period_month,
        year=period_year,
    )
    account_name_by_code: dict[str, str] = {}
    account_codes: list[str] = []
    for account in accounts:
        account_code = standardize_account_code(
            account.get("accountCode") or account.get("code")
        )
        if not account_code:
            continue
        if account_code not in account_name_by_code:
            account_codes.append(account_code)
        account_name = str(
            account.get("accountName")
            or account.get("name")
            or account.get("descriptiveName")
            or account_code
        ).strip()
        account_name_by_code[account_code] = account_name or account_code

    if not account_codes:
        set_google_sheet_cache(
            cache_key,
            [{"tableData": [], "spentData": []}],
            config_hash=_BUDGET_OVERVIEW_CACHE_HASH,
        )
        return {
            "period": {"month": period_month, "year": period_year},
            "previousPeriod": {"month": previous_month, "year": previous_year},
            "tableData": [],
            "spentData": [],
        }

    budgets = get_masterbudgets(account_codes, period_month, period_year)
    previous_month_budgets = get_masterbudgets(
        account_codes,
        previous_month,
        previous_year,
    )
    service_mapping = get_service_mapping()
    previous_month_budget_lookup = _sum_db_budget_by_account_adtype(
        previous_month_budgets,
        service_mapping=service_mapping,
    )
    current_period_spend_lookup = _sum_google_spend_by_account_adtype(
        account_codes,
        month=period_month,
        year=period_year,
    )
    previous_month_spend_lookup = _sum_google_spend_by_account_adtype(
        account_codes,
        month=previous_month,
        year=previous_year,
    )

    payload_rows: list[dict[str, object]] = []
    for row in budgets:
        account_code = standardize_account_code(row.get("accountCode")) or ""
        service_id = str(row.get("serviceId") or "").strip()
        mapping = _get_service_mapping_entry(service_mapping, service_id)
        ad_type_code = str(mapping.get("adTypeCode") or "").strip().upper()
        service_name = str(
            row.get("serviceName")
            or mapping.get("serviceName")
            or service_id
        ).strip()

        previous_month_underspent = Decimal("0")
        if account_code and ad_type_code:
            previous_month_underspent = (
                previous_month_budget_lookup.get((account_code, ad_type_code), Decimal("0"))
                - previous_month_spend_lookup.get((account_code, ad_type_code), Decimal("0"))
            )
        payload_rows.append(
            {
                "budgetId": row.get("id"),
                "accountCode": account_code,
                "accountName": account_name_by_code.get(account_code, account_code),
                "_sortAdTypeCode": ad_type_code,
                "adTypeCode": ad_type_code,
                "serviceId": service_id,
                "service": service_name,
                "amount": _format_decimal_2(row.get("netAmount")),
                "note": row.get("note"),
                "previousMonthUnderspent": _format_decimal_2(previous_month_underspent),
            }
        )

    payload_rows.sort(
        key=lambda item: (
            str(item.get("accountCode") or ""),
            _adtype_priority_rank(str(item.get("_sortAdTypeCode") or "")),
            str(item.get("service") or "").lower(),
            str(item.get("budgetId") or ""),
        )
    )
    for index, item in enumerate(payload_rows):
        item.pop("_sortAdTypeCode", None)
        item["dataNo"] = index

    spent_data_map: dict[tuple[str, str], Decimal] = {}
    for row in payload_rows:
        account_code = str(row.get("accountCode") or "").strip()
        ad_type_code = str(row.get("adTypeCode") or "").strip().upper()
        if not account_code or not ad_type_code:
            continue
        key = (account_code, ad_type_code)
        if key in spent_data_map:
            continue
        spent_data_map[key] = current_period_spend_lookup.get(
            (account_code, ad_type_code),
            Decimal("0"),
        )

    spent_data = [
        {
            "accountCode": account_code,
            "adTypeCode": ad_type_code,
            "spent": _format_decimal_2(spent_amount),
        }
        for (account_code, ad_type_code), spent_amount in sorted(
            spent_data_map.items(),
            key=lambda item: (
                item[0][0],
                _adtype_priority_rank(item[0][1]),
            ),
        )
    ]

    set_google_sheet_cache(
        cache_key,
        [{"tableData": payload_rows, "spentData": spent_data}],
        config_hash=_BUDGET_OVERVIEW_CACHE_HASH,
    )

    return {
        "period": {"month": period_month, "year": period_year},
        "previousPeriod": {"month": previous_month, "year": previous_year},
        "tableData": payload_rows,
        "spentData": spent_data,
    }


def get_recommended_budget_managements(
    account_code: str | None,
    month: int,
    year: int,
    service_id: str | None = None,
):
    """
    Get recommended budgets from tenant-specific spreadsheet parser.

    Example request:
        GET /api/spendsphere/v1/uis/budgetManagament/recommended?accountCode=ALAM&serviceId=SEM&month=2&year=2026

    Example request (all SERVICE_BUDGETS):
        GET /api/spendsphere/v1/uis/budgetManagament/recommended?accountCode=ALAM&month=2&year=2026

    Example response:
        {
          "accountCode": "ALAM",
          "serviceId": "SEM",
          "serviceName": "Google Search",
          "amount": 800.0
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
    """
    month, year = _resolve_period(month, year)

    parser = _resolve_recommended_budget_parser(get_tenant_id())
    if parser is None:
        raise HTTPException(
            status_code=404,
            detail="Recommended budget parser not configured for this tenant",
        )

    normalized_requested_codes = normalize_account_codes([account_code]) if account_code else []
    if normalized_requested_codes:
        normalized_account_code = normalized_requested_codes[0]
        validate_account_codes([normalized_account_code], month=month, year=year)
        try:
            results = parser(normalized_account_code, service_id, month, year)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if service_id is not None:
            if results:
                return results[0]
            return {
                "accountCode": normalized_account_code,
                "serviceId": service_id,
                "serviceName": service_id,
                "amount": None,
            }
        return results

    accounts = validate_account_codes(None, month=month, year=year)
    account_codes: list[str] = []
    seen_codes: set[str] = set()
    for account in accounts:
        candidate = standardize_account_code(
            account.get("accountCode") or account.get("code")
        )
        if not candidate or candidate in seen_codes:
            continue
        seen_codes.add(candidate)
        account_codes.append(candidate)

    payload: list[dict[str, object]] = []
    for code in account_codes:
        try:
            payload.extend(parser(code, service_id, month, year))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    payload.sort(
        key=lambda item: (
            str(item.get("accountCode") or "").casefold(),
            str(item.get("serviceId") or "").casefold(),
        )
    )
    return payload


def sync_budget_management_master_budget_sheet(
    month: int | None = None,
    year: int | None = None,
    refresh_google_ads_caches: bool = False,
):
    """
    Build NuCar master-budget pivot rows and refresh the target Google Sheet tab.

    Example request:
        POST /api/spendsphere/v1/uis/budgetManagament/masterBudgetDataSync?month=3&year=2026

    Example request (default current month/year):
        POST /api/spendsphere/v1/uis/budgetManagament/masterBudgetDataSync

    Example response:
        {
          "period": {"month": 3, "year": 2026},
          "spreadsheetId": "1heDhjHoLYjsoM9fOazW3KQisaCYOG6zPZs-_X3PUCs8",
          "sheetName": "2.3 Master Budget Data",
          "startRow": 9,
          "rowCount": 2,
          "rows": [
            {
              "budgetId": "14644368953",
              "amount": 1794.0,
              "scheduleStatus": "-"
            },
            {
              "budgetId": "14650372785",
              "amount": 1121.25,
              "scheduleStatus": "-"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Requires FEATURE_FLAGS.budget_managements=true for this tenant
        - month/year are optional and default to current tenant period
        - month/year must be provided together when specified
        - Route is available only for tenants with a configured sync handler
    """
    month, year = _resolve_period(month, year)

    sync_handler = _resolve_master_budget_sheet_syncer(get_tenant_id())
    if sync_handler is None:
        raise HTTPException(
            status_code=404,
            detail="Master budget sheet sync is not configured for this tenant",
        )

    try:
        result = sync_handler(
            month,
            year,
            refresh_google_ads_caches=refresh_google_ads_caches,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"period": {"month": month, "year": year}, **result}


def create_budget_managements(payload: BudgetManagementUpsertRequest):
    """
    Create budget rows for a period.

    Example request:
        POST /api/spendsphere/v1/uis/budgetManagament
        {
          "month": 1,
          "year": 2026,
          "rows": [
            {
              "accountCode": "NUCAR",
              "serviceId": "SEM",
              "subService": null,
              "netAmount": 1000
            }
          ]
        }

    Example response:
        {
          "period": {"month": 1, "year": 2026},
          "updated": 0,
          "inserted": 1
        }
    """
    month, year = _resolve_period(payload.month, payload.year)

    rows: list[dict[str, object]] = []
    account_codes: list[str] = []
    for item in payload.rows:
        if item.id:
            raise HTTPException(
                status_code=400,
                detail="id must be empty for create",
            )
        account_codes.append(item.accountCode)
        rows.append(item.model_dump())

    validate_account_codes(account_codes, month=month, year=year)
    result = upsert_masterbudgets(rows, month=month, year=year)
    return {"period": {"month": month, "year": year}, **result}


def update_budget_managements(payload: BudgetManagementUpsertRequest):
    """
    Update budget rows for a period.

    Example request:
        PUT /api/spendsphere/v1/uis/budgetManagament
        {
          "month": 1,
          "year": 2026,
          "rows": [
            {
              "id": "65c8d225-9f8f-4d13-8558-d6698f239a45",
              "accountCode": "NUCAR",
              "serviceId": "SEM",
              "subService": null,
              "netAmount": 1200
            }
          ]
        }

    Example response:
        {
          "period": {"month": 1, "year": 2026},
          "updated": 1,
          "inserted": 0
        }
    """
    month, year = _resolve_period(payload.month, payload.year)

    rows: list[dict[str, object]] = []
    account_codes: list[str] = []
    for item in payload.rows:
        if not item.id:
            raise HTTPException(
                status_code=400,
                detail="id is required for update",
            )
        account_codes.append(item.accountCode)
        rows.append(item.model_dump())

    validate_account_codes(account_codes, month=month, year=year)
    result = upsert_masterbudgets(rows, month=month, year=year)
    return {"period": {"month": month, "year": year}, **result}


def soft_delete_budget_management(
    budget_id: str,
    account_code: str,
    month: int | None = None,
    year: int | None = None,
):
    """
    Soft delete a budget by setting `netAmount` to 0.

    Example request:
        DELETE /api/spendsphere/v1/uis/budgetManagament/65c8d225-9f8f-4d13-8558-d6698f239a45?accountCode=NUCAR&month=1&year=2026

    Example response:
        {
          "budgetId": "65c8d225-9f8f-4d13-8558-d6698f239a45",
          "accountCode": "NUCAR",
          "month": 1,
          "year": 2026,
          "softDeleted": true
        }
    """
    period_month, period_year = _resolve_period(month, year)
    validate_account_codes([account_code], month=period_month, year=period_year)

    affected = soft_delete_masterbudget(
        budget_id=budget_id,
        account_code=account_code,
        month=period_month,
        year=period_year,
    )
    if affected <= 0:
        raise HTTPException(status_code=404, detail="Budget not found")

    return {
        "budgetId": budget_id,
        "accountCode": account_code,
        "month": period_month,
        "year": period_year,
        "softDeleted": True,
    }


def apply_budget_management_changes(
    payload: BudgetManagementChangesRequest,
):
    """
    Apply create/update/delete budget changes in one request.
    """
    month, year = _resolve_period(payload.month, payload.year)

    existing_rows = get_masterbudgets(None, month, year)
    existing_by_id: dict[str, dict] = {}
    for row in existing_rows:
        row_id = str(row.get("id") or "").strip()
        if row_id:
            existing_by_id[row_id] = row

    upsert_rows: list[dict[str, object]] = []
    delete_rows: list[tuple[str, str]] = []
    delete_keys: set[tuple[str, str]] = set()
    account_codes_to_validate: set[str] = set()

    for index, change in enumerate(payload.changes):
        op = str(change.op).strip().lower()
        budget_id = str(change.id or "").strip()
        explicit_account_code = (
            standardize_account_code(change.accountCode)
            if change.accountCode is not None
            else None
        )
        explicit_service_id = (
            str(change.serviceId).strip()
            if change.serviceId is not None
            else None
        )
        existing_row = existing_by_id.get(budget_id) if budget_id else None

        if op == "create":
            if not explicit_account_code:
                raise HTTPException(
                    status_code=400,
                    detail=f"changes[{index}].accountCode is required for create",
                )
            if not explicit_service_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"changes[{index}].serviceId is required for create",
                )
            upsert_rows.append(
                {
                    "accountCode": explicit_account_code,
                    "serviceId": explicit_service_id,
                    "subService": change.subService,
                    "note": change.note,
                    "netAmount": change.amount if change.amount is not None else 0,
                }
            )
            account_codes_to_validate.add(explicit_account_code)
            continue

        if op == "update":
            if not budget_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"changes[{index}].id is required for update",
                )
            if existing_row is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Budget not found for update: {budget_id}",
                )

            account_code = explicit_account_code or standardize_account_code(
                existing_row.get("accountCode")
            )
            service_id = explicit_service_id or str(
                existing_row.get("serviceId") or ""
            ).strip()
            if not account_code or not service_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"changes[{index}] has invalid accountCode/serviceId",
                )

            amount = (
                change.amount
                if change.amount is not None
                else float(existing_row.get("netAmount") or 0)
            )
            sub_service = (
                change.subService
                if "subService" in change.model_fields_set
                else existing_row.get("subService")
            )
            note = (
                change.note
                if "note" in change.model_fields_set
                else existing_row.get("note")
            )

            upsert_rows.append(
                {
                    "id": budget_id,
                    "accountCode": account_code,
                    "serviceId": service_id,
                    "subService": sub_service,
                    "note": note,
                    "netAmount": amount,
                }
            )
            account_codes_to_validate.add(account_code)
            continue

        if op == "delete":
            if not budget_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"changes[{index}].id is required for delete",
                )
            if existing_row is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Budget not found for delete: {budget_id}",
                )
            account_code = explicit_account_code or standardize_account_code(
                existing_row.get("accountCode")
            )
            if not account_code:
                raise HTTPException(
                    status_code=400,
                    detail=f"changes[{index}] has invalid accountCode",
                )
            delete_key = (budget_id, account_code)
            if delete_key in delete_keys:
                continue
            delete_keys.add(delete_key)
            delete_rows.append(delete_key)
            account_codes_to_validate.add(account_code)
            continue

        raise HTTPException(
            status_code=400,
            detail=f"changes[{index}].op must be one of create/update/delete",
        )

    if account_codes_to_validate:
        validate_account_codes(
            sorted(account_codes_to_validate),
            month=month,
            year=year,
        )

    updated = 0
    inserted = 0
    if upsert_rows:
        upsert_result = upsert_masterbudgets(
            upsert_rows,
            month=month,
            year=year,
        )
        updated = int(upsert_result.get("updated") or 0)
        inserted = int(upsert_result.get("inserted") or 0)

    deleted = 0
    for budget_id, account_code in delete_rows:
        affected = soft_delete_masterbudget(
            budget_id=budget_id,
            account_code=account_code,
            month=month,
            year=year,
        )
        deleted += max(int(affected or 0), 0)

    return {
        "period": {"month": month, "year": year},
        "updated": updated,
        "inserted": inserted,
        "deleted": deleted,
        "appliedChanges": len(payload.changes),
    }
