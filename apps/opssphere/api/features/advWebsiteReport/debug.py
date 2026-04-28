from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from apps.opssphere.api.features.advWebsiteReport.helpers.advWebsiteReport import (
    fetch_adv_website_events_for_date_range,
    fetch_adv_website_events_for_month,
)
from apps.opssphere.api.features.advWebsiteReport.helpers.routeUtils import (
    resolve_period_params,
    resolve_account_property_config,
)
from apps.opssphere.api.helpers.ga4 import list_ga4_dimensions

router = APIRouter(prefix="/advWebsiteReport/debug")


@router.get("/clickTextDimension")
def debug_adv_website_click_text_dimension(
    accountCode: str = Query(
        ...,
        min_length=1,
        description="Tenant account code configured in opssphere.ga4.properties.",
    ),
    month: int | None = Query(
        None,
        ge=1,
        le=12,
        description="Target report month in range 1..12.",
    ),
    year: int | None = Query(
        None,
        ge=2000,
        le=2100,
        description="Target report year in range 2000..2100.",
    ),
    start_date: str | None = Query(
        None,
        description="Target report start date in YYYY-MM-DD format.",
    ),
    end_date: str | None = Query(
        None,
        description="Target report end date in YYYY-MM-DD format.",
    ),
):
    """
    Debug helper for CTA GA4 schema resolution. It runs the same monthly CTA
    query path for the selected period and returns which GA4 event/click/metric
    fields were resolved.

    Example request:
        GET /api/opssphere/advWebsiteReport/debug/clickTextDimension?accountCode=TAAA&month=4&year=2026

    Example request (date range):
        GET /api/opssphere/advWebsiteReport/debug/clickTextDimension?accountCode=TAAA&start_date=2026-04-01&end_date=2026-04-15

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 18,
            "timestamp": "2026-04-27T09:00:00.000Z"
          },
          "data": {
            "accountCode": "TAAA",
            "propertyId": "353247313",
            "dateRange": {"startDate": "2026-04-01", "endDate": "2026-04-30"},
            "resolved": {
              "eventDimension": "eventName",
              "clickTextDimension": "customEvent:CTA",
              "eventCountMetric": "eventCount",
              "subMenuParentDimension": "customEvent:mega_menu_parent"
            },
            "rowCount": 135
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Tenant config must include opssphere.ga4.properties mapping for accountCode
        - Use either start_date/end_date OR month/year (not both)
        - Optional GA4 start-date floor:
          opssphere.ga4.start_date or opssphere.ga4.properties.<accountCode>.start_date
        - This endpoint does not return report data rows; it returns resolved field keys and row count
    """
    account_code = str(accountCode or "").strip().upper()
    if not account_code:
        raise HTTPException(status_code=400, detail="accountCode is required.")
    period = resolve_period_params(
        month=month,
        year=year,
        start_date=start_date,
        end_date=end_date,
    )

    ga4_config, account_config = resolve_account_property_config(account_code)
    property_id = str(account_config.get("property_id") or "").strip()
    if not property_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "GA4 property id is missing for accountCode in "
                "opssphere.ga4.properties."
            ),
        )
    start_date_floor = str(account_config.get("start_date") or "").strip() or str(
        ga4_config.get("start_date") or ""
    ).strip()

    try:
        if str(period["start_date"] or "").strip():
            fetched = fetch_adv_website_events_for_date_range(
                property_id=property_id,
                start_date=str(period["start_date"]),
                end_date=str(period["end_date"]),
                min_start_date=start_date_floor or None,
            )
        else:
            fetched = fetch_adv_website_events_for_month(
                property_id=property_id,
                month=int(period["month"]),
                year=int(period["year"]),
                min_start_date=start_date_floor or None,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to resolve CTA GA4 dimensions: {exc}",
        ) from exc

    rows = fetched.get("rows")
    row_count = len(rows) if isinstance(rows, list) else 0
    sample_values: list[str] = []
    if isinstance(rows, list):
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            value = str(row.get("click_text") or "").strip()
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            sample_values.append(value)
            if len(sample_values) >= 10:
                break
    return {
        "accountCode": account_code,
        "propertyId": property_id,
        "dateRange": {
            "startDate": str(fetched.get("start_date") or ""),
            "endDate": str(fetched.get("end_date") or ""),
        },
        "resolved": {
            "eventDimension": str(fetched.get("resolved_event_dimension") or ""),
            "clickTextDimension": str(fetched.get("resolved_click_text_dimension") or ""),
            "eventCountMetric": str(fetched.get("resolved_event_count_metric") or ""),
            "subMenuParentDimension": str(
                fetched.get("resolved_sub_menu_parent_dimension") or ""
            ),
        },
        "rowCount": row_count,
        "sampleClickTextValues": sample_values,
        "candidateStats": fetched.get("candidate_stats", []),
        "subMenuParentCandidateStats": fetched.get(
            "sub_menu_parent_candidate_stats", []
        ),
    }


@router.get("/dimensions")
def debug_adv_website_dimensions(
    accountCode: str = Query(
        ...,
        min_length=1,
        description="Tenant account code configured in opssphere.ga4.properties.",
    ),
    contains: str | None = Query(
        None,
        description=(
            "Optional case-insensitive text filter applied to apiName/uiName/description."
        ),
    ),
    custom_only: bool = Query(
        False,
        description="When true, return custom dimensions only.",
    ),
    limit: int = Query(
        500,
        ge=1,
        le=5000,
        description="Maximum number of dimensions returned after filters.",
    ),
):
    """
    List GA4 dimensions for the account's mapped property using GA4 metadata.
    Use this to discover the correct click-text custom dimension name.

    Example request:
        GET /api/opssphere/advWebsiteReport/debug/dimensions?accountCode=DCM

    Example request (filter):
        GET /api/opssphere/advWebsiteReport/debug/dimensions?accountCode=DCM&contains=click

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 25,
            "timestamp": "2026-04-27T09:00:00.000Z"
          },
          "data": {
            "accountCode": "DCM",
            "propertyId": "353247313",
            "totalDimensions": 312,
            "filteredCount": 3,
            "clickLikeDimensions": ["customEvent:click_text", "customEvent:cta_text"],
            "dimensions": [
              {
                "apiName": "customEvent:click_text",
                "uiName": "click_text",
                "description": "...",
                "customDefinition": true,
                "deprecatedApiNames": []
              }
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Tenant config must include opssphere.ga4.properties mapping for accountCode
    """
    account_code = str(accountCode or "").strip().upper()
    if not account_code:
        raise HTTPException(status_code=400, detail="accountCode is required.")

    _, account_config = resolve_account_property_config(account_code)
    property_id = str(account_config.get("property_id") or "").strip()
    if not property_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "GA4 property id is missing for accountCode in "
                "opssphere.ga4.properties."
            ),
        )

    try:
        all_dimensions = list_ga4_dimensions(property_id=property_id)
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to load GA4 metadata dimensions: {exc}",
        ) from exc

    filter_text = str(contains or "").strip().lower()
    filtered: list[dict[str, object]] = []
    for item in all_dimensions:
        if custom_only and not bool(item.get("customDefinition")):
            continue
        if filter_text:
            haystack = " ".join(
                [
                    str(item.get("apiName") or ""),
                    str(item.get("uiName") or ""),
                    str(item.get("description") or ""),
                ]
            ).lower()
            if filter_text not in haystack:
                continue
        filtered.append(item)

    limited = filtered[:limit]
    click_like_dimensions = [
        str(item.get("apiName") or "")
        for item in filtered
        if any(
            token in str(item.get("apiName") or "").lower()
            for token in ("click", "cta", "button", "comm")
        )
    ]

    return {
        "accountCode": account_code,
        "propertyId": property_id,
        "totalDimensions": len(all_dimensions),
        "filteredCount": len(filtered),
        "clickLikeDimensions": click_like_dimensions[:100],
        "dimensions": limited,
    }
