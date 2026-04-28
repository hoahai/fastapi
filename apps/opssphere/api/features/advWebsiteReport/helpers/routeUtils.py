from __future__ import annotations

import calendar
from datetime import datetime
import re

from fastapi import HTTPException

from apps.opssphere.api.helpers.config import get_ga4_config


def sanitize_filename_token(value: object) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip())
    cleaned = cleaned.strip("-.")
    return cleaned or "tenant"


def resolve_account_property_config(account_code: str) -> tuple[dict[str, object], dict[str, object]]:
    ga4_config = get_ga4_config()
    properties_config_by_account_code = {
        str(key).strip().upper(): value
        for key, value in dict(
            ga4_config.get("properties_config_by_account_code", {})
        ).items()
        if str(key).strip()
    }
    account_config = properties_config_by_account_code.get(account_code)
    if not isinstance(account_config, dict):
        raise HTTPException(
            status_code=400,
            detail=(
                "accountCode is not configured for this tenant in "
                "opssphere.ga4.properties."
            ),
        )
    return ga4_config, account_config


def resolve_period_params(
    *,
    month: int | None,
    year: int | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, object]:
    def _fmt_mdy(dt: datetime) -> str:
        return f"{dt.month}/{dt.day}/{dt.year}"

    start_text = str(start_date or "").strip()
    end_text = str(end_date or "").strip()
    has_range = bool(start_text or end_text)
    if has_range:
        if month is not None or year is not None:
            raise HTTPException(
                status_code=400,
                detail="Use either start_date/end_date or month/year, not both.",
            )
        if not start_text or not end_text:
            raise HTTPException(
                status_code=400,
                detail="start_date and end_date must be provided together.",
            )
        try:
            start_parsed = datetime.strptime(start_text, "%Y-%m-%d")
            end_parsed = datetime.strptime(end_text, "%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="start_date and end_date must be in YYYY-MM-DD format.",
            ) from exc
        if start_parsed.year < 2000 or start_parsed.year > 2100:
            raise HTTPException(
                status_code=400,
                detail="start_date year must be between 2000 and 2100.",
            )
        if end_parsed.year < 2000 or end_parsed.year > 2100:
            raise HTTPException(
                status_code=400,
                detail="end_date year must be between 2000 and 2100.",
            )
        if start_parsed.date() > end_parsed.date():
            raise HTTPException(
                status_code=400,
                detail="start_date must be on or before end_date.",
            )

        start_iso = start_parsed.strftime("%Y-%m-%d")
        end_iso = end_parsed.strftime("%Y-%m-%d")
        if start_iso == end_iso:
            label = _fmt_mdy(start_parsed)
        else:
            label = f"{_fmt_mdy(start_parsed)} to {_fmt_mdy(end_parsed)}"
        return {
            "mode": "range",
            "start_date": start_iso,
            "end_date": end_iso,
            "month": int(start_parsed.month),
            "year": int(start_parsed.year),
            "period_label": label,
        }

    if month is None and year is None:
        raise HTTPException(
            status_code=400,
            detail="Either start_date/end_date or month/year is required.",
        )
    if month is None or year is None:
        raise HTTPException(
            status_code=400,
            detail="month and year must be provided together.",
        )

    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="month must be between 1 and 12.")
    if year < 2000 or year > 2100:
        raise HTTPException(status_code=400, detail="year must be between 2000 and 2100.")
    month_start = datetime(int(year), int(month), 1)
    month_end = datetime(int(year), int(month), calendar.monthrange(int(year), int(month))[1])

    return {
        "mode": "month",
        "start_date": "",
        "end_date": "",
        "month": int(month),
        "year": int(year),
        "period_label": f"{_fmt_mdy(month_start)} to {_fmt_mdy(month_end)}",
    }
