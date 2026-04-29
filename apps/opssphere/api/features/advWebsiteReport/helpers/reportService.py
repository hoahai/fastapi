from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from apps.opssphere.api.features.advWebsiteReport.helpers.advWebsiteReport import (
    build_cta_sections,
    build_menu_sections,
    build_srp_sort_categories_section,
    fetch_adv_website_events_for_date_range,
    fetch_adv_website_events_for_month,
)
from apps.opssphere.api.features.advWebsiteReport.helpers.advWebsiteReportPdf import (
    build_adv_website_cta_report_pdf,
)
from apps.opssphere.api.features.advWebsiteReport.helpers.routeUtils import (
    resolve_account_property_config,
    sanitize_filename_token,
)
from shared.tenant import get_timezone


def generate_adv_website_report_pdf(
    *,
    tenant_id: str,
    account_code: str,
    month: int | None = None,
    year: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    period_label: str = "",
) -> tuple[bytes, str]:
    try:
        ga4_config, account_config = resolve_account_property_config(account_code)
    except HTTPException as exc:
        raise ValueError(str(exc.detail or "Invalid accountCode configuration.")) from exc

    property_id = str(account_config.get("property_id") or "").strip()
    if not property_id:
        raise ValueError(
            "GA4 property id is missing for accountCode in opssphere.ga4.properties."
        )

    timezone_for_report = str(account_config.get("timezone") or "").strip() or str(
        ga4_config.get("timezone") or get_timezone()
    )
    start_date_floor = str(account_config.get("start_date") or "").strip() or str(
        ga4_config.get("start_date") or ""
    ).strip()

    start_date_text = str(start_date or "").strip()
    end_date_text = str(end_date or "").strip()
    if start_date_text or end_date_text:
        fetched = fetch_adv_website_events_for_date_range(
            property_id=property_id,
            start_date=start_date_text,
            end_date=end_date_text,
            min_start_date=start_date_floor or None,
        )
    else:
        if month is None or year is None:
            raise ValueError("month/year is required when start_date/end_date is not provided.")
        fetched = fetch_adv_website_events_for_month(
            property_id=property_id,
            month=month,
            year=year,
            min_start_date=start_date_floor or None,
        )
    rows = fetched.get("rows")
    menu_rows = fetched.get("menu_rows")
    srp_sort_rows = fetched.get("srp_sort_rows")
    sections = build_cta_sections(rows if isinstance(rows, list) else [])
    menu_sections = build_menu_sections(menu_rows if isinstance(menu_rows, list) else [])
    srp_sort_section = build_srp_sort_categories_section(
        srp_sort_rows if isinstance(srp_sort_rows, list) else []
    )

    has_meaningful_values = any(
        str(item.get("buttonText") or "").strip().lower()
        not in {"", "(blank)", "(not set)", "not set"}
        for section in sections
        if isinstance(section, dict)
        for item in (
            section.get("rows") if isinstance(section.get("rows"), list) else []
        )
        if isinstance(item, dict)
    )
    if not has_meaningful_values:
        raise ValueError(
            "CTA click-text values are empty or not set for the fixed dimension "
            f"'{str(fetched.get('resolved_click_text_dimension') or '')}'. "
            "Verify that GA4 custom dimension customEvent:CTA contains values "
            "for the selected period."
        )

    pdf_bytes = build_adv_website_cta_report_pdf(
        tenant_id=tenant_id,
        account_code=account_code,
        month=int(month or 1),
        year=int(year or 2000),
        period_label=period_label or "",
        timezone=timezone_for_report,
        sections=sections,
        menu_sections=menu_sections,
        srp_sort_section=srp_sort_section,
    )
    return pdf_bytes, timezone_for_report


def build_report_filename(
    *,
    tenant_id: str,
    account_code: str,
    month: int | None = None,
    year: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    timezone_for_report: str,
) -> str:
    try:
        timestamp_token = datetime.now(ZoneInfo(timezone_for_report)).strftime("%y%m%d%H%M")
    except Exception:
        timestamp_token = datetime.now(ZoneInfo(get_timezone())).strftime("%y%m%d%H%M")

    account_token = sanitize_filename_token(account_code).lower()
    tenant_token = sanitize_filename_token(tenant_id).lower()
    start_date_text = str(start_date or "").strip()
    end_date_text = str(end_date or "").strip()
    if start_date_text and end_date_text:
        period_token = f"{start_date_text.replace('-', '')}-{end_date_text.replace('-', '')}"
    else:
        if month is None or year is None:
            raise ValueError("month/year is required when start_date/end_date is not provided.")
        period_token = f"{int(year) % 100:02d}{int(month):02d}"
    return (
        "OpsSphere-AdvWebsite-CTA-"
        f"{tenant_token}-{account_token}-{period_token}-{timestamp_token}.pdf"
    )
