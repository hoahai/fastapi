from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from apps.spendsphere.api.v1.endpoints.core.ui.main import _build_table_data_payload
from apps.spendsphere.api.v1.helpers.config import get_adtypes
from apps.spendsphere.api.v1.helpers.budgetReportPdf import build_budget_report_pdf
from apps.spendsphere.api.v1.helpers.pipeline import build_transform_rows_for_period
from apps.spendsphere.api.v1.helpers.spendsphereHelpers import (
    get_budget_management_cache_entry,
    set_budget_management_cache,
)
from shared.tenant import get_timezone
from shared.utils import get_current_period

router = APIRouter()

_REPORT_CACHE_KEY_PREFIX = "budget_managements::budgets_report_pdf::"
_REPORT_CACHE_SCHEMA_HASH = "budgets-report-pdf-data-v1"


def _sanitize_filename_token(value: object) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip())
    cleaned = cleaned.strip("-.")
    return cleaned or "tenant"


def _build_report_cache_key(*, month: int, year: int) -> str:
    return f"{_REPORT_CACHE_KEY_PREFIX}{year:04d}-{month:02d}"


def _to_cache_safe(value: object) -> object:
    if isinstance(value, dict):
        return {str(k): _to_cache_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_cache_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_to_cache_safe(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _to_cache_safe_rows(rows: list[dict]) -> list[dict]:
    normalized: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        safe_row = _to_cache_safe(row)
        if isinstance(safe_row, dict):
            normalized.append(safe_row)
    return normalized


@router.get(
    "/reports/budgetOverview",
    summary="Generate budget PDF report by period",
    description=(
        "Generates a portrait PDF report grouped by account code using SpendSphere "
        "period transform data."
    ),
)
def get_budgets_report_pdf(
    request: Request,
    month: int = Query(..., ge=1, le=12),
    year: int = Query(..., ge=2000, le=2100),
    fresh_data: bool = Query(False, alias="fresh_data"),
):
    """
    Generates a budget PDF report for all available budget IDs in a selected month/year.
    The report is grouped by account and includes spend, allocation, acceleration, and pacing metrics.

    Example request:
        GET /api/spendsphere/v1/reports/budgetOverview?month=1&year=2026

    Example request (cache bypass):
        GET /api/spendsphere/v1/reports/budgetOverview?month=1&year=2026&fresh_data=true

    Example response:
        Content-Type: application/pdf
        Content-Disposition: attachment; filename="SpendSphere Budget Overview - nucar2601 - 2604010912.pdf"
        <binary pdf content>

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - month must be between 1 and 12
        - year must be between 2000 and 2100
        - fresh_data=true bypasses report data cache for the requested period
    """
    ad_type_order: dict[str, int] = {}
    for code, meta in get_adtypes().items():
        code_key = str(code or "").strip().upper()
        if not code_key or not isinstance(meta, dict):
            continue
        try:
            ad_type_order[code_key] = int(meta.get("order"))
        except (TypeError, ValueError):
            continue

    tenant_id = getattr(request.state, "tenant_id", None) or request.headers.get(
        "x-tenant-id"
    )
    tenant_token = _sanitize_filename_token(tenant_id)
    resolved_month = month
    resolved_year = year
    cache_key = _build_report_cache_key(month=resolved_month, year=resolved_year)

    rows: list[dict] | None = None
    if not fresh_data:
        cached_rows, _is_stale = get_budget_management_cache_entry(
            cache_key,
            config_hash=_REPORT_CACHE_SCHEMA_HASH,
            tenant_id=tenant_id,
        )
        if isinstance(cached_rows, list):
            rows = cached_rows

    if rows is None:
        try:
            transform_payload = build_transform_rows_for_period(
                account_codes=None,
                month=resolved_month,
                year=resolved_year,
                refresh_google_ads_caches=False,
                cache_first=True,
                include_costs=True,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        rows = transform_payload.get("rows")
        budgets = transform_payload.get("budgets")
        campaigns = transform_payload.get("campaigns")
        costs = transform_payload.get("costs")
        accelerations = transform_payload.get("accelerations")
        allocations = transform_payload.get("allocations")

        period = (
            transform_payload.get("period")
            if isinstance(transform_payload, dict)
            else {}
        )
        if isinstance(period, dict):
            resolved_month = int(period.get("month", resolved_month))
            resolved_year = int(period.get("year", resolved_year))
            cache_key = _build_report_cache_key(month=resolved_month, year=resolved_year)

        rows = rows if isinstance(rows, list) else []
        budgets = budgets if isinstance(budgets, list) else []
        allocations = allocations if isinstance(allocations, list) else []
        campaigns = campaigns if isinstance(campaigns, list) else []
        costs = costs if isinstance(costs, list) else []
        accelerations = accelerations if isinstance(accelerations, list) else []

        # Keep budget visibility aligned with UI load filtering rules.
        table_data_payload = _build_table_data_payload(
            rows,
            budgets=budgets,
            allocations=allocations,
            campaigns=campaigns,
            costs=costs,
            accelerations=accelerations,
        )
        table_rows = (
            table_data_payload.get("data", [])
            if isinstance(table_data_payload, dict)
            else []
        )
        allowed_budget_keys = {
            (
                str(item.get("accountId", "")).strip(),
                str(item.get("budgetId", "")).strip(),
            )
            for item in table_rows
            if isinstance(item, dict)
            and str(item.get("accountId", "")).strip()
            and str(item.get("budgetId", "")).strip()
        }
        rows = [
            row
            for row in rows
            if (
                str(row.get("ggAccountId", "")).strip(),
                str(row.get("budgetId", "")).strip(),
            )
            in allowed_budget_keys
        ]

        cache_rows = _to_cache_safe_rows(rows)
        try:
            set_budget_management_cache(
                cache_key,
                cache_rows,
                config_hash=_REPORT_CACHE_SCHEMA_HASH,
                tenant_id=tenant_id,
            )
        except Exception:
            # Cache write failures should not block report generation.
            pass

    rows = rows if isinstance(rows, list) else []

    current_period = get_current_period()
    is_current_period = (
        resolved_month == int(current_period.get("month"))
        and resolved_year == int(current_period.get("year"))
    )

    pdf_bytes = build_budget_report_pdf(
        rows=rows,
        budgets=[],
        tenant_id=str(tenant_id or "unknown"),
        month=resolved_month,
        year=resolved_year,
        ad_type_order=ad_type_order,
        is_current_period=is_current_period,
    )

    period_token = f"{tenant_token}{resolved_year % 100:02d}{resolved_month:02d}"
    timestamp_token = datetime.now(ZoneInfo(get_timezone())).strftime("%y%m%d%H%M")
    filename = (
        f"SpendSphere Budget Overview - {period_token} - {timestamp_token}.pdf"
    )
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
