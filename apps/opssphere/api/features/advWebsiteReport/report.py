from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from apps.opssphere.api.features.advWebsiteReport.helpers.publicLink import (
    create_signed_report_token,
    get_signed_report_ttl_days,
    resolve_public_signing_secret,
)
from apps.opssphere.api.features.advWebsiteReport.helpers.reportService import (
    build_report_filename,
    generate_adv_website_report_pdf,
)
from apps.opssphere.api.features.advWebsiteReport.helpers.routeUtils import (
    resolve_period_params,
)

router = APIRouter(prefix="/advWebsiteReport")


@router.post("/public-link")
def create_adv_website_public_link(
    request: Request,
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
    Mint a signed public URL for Advanced Website report PDF. The URL is valid
    without API key or tenant header and expires based on tenant-configured TTL.

    Example request:
        POST /api/opssphere/advWebsiteReport/public-link?accountCode=DCM&month=4&year=2026

    Example request (date range):
        POST /api/opssphere/advWebsiteReport/public-link?accountCode=DCM&start_date=2026-04-01&end_date=2026-04-15

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 13,
            "timestamp": "2026-04-28T10:00:00.000Z"
          },
          "data": {
            "url": "https://api.example.com/public/opssphere/advWebsiteReport/reports/cta?token=...",
            "expiresAt": "2026-05-28T10:00:00+00:00",
            "ttlDays": 30
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Use either start_date/end_date OR month/year (not both)
        - Tenant config may set opssphere.public.signed_report_ttl_days (default 30)
        - Signing secret required via opssphere.public.signing_secret or OPSSPHERE_PUBLIC_SIGNING_SECRET
    """
    tenant_id = str(
        getattr(request.state, "tenant_id", None)
        or request.headers.get("x-tenant-id")
        or ""
    ).strip()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing X-Tenant-Id header.")

    account_code = str(accountCode or "").strip().upper()
    if not account_code:
        raise HTTPException(status_code=400, detail="accountCode is required.")
    period = resolve_period_params(
        month=month,
        year=year,
        start_date=start_date,
        end_date=end_date,
    )

    try:
        ttl_days = get_signed_report_ttl_days()
        secret = resolve_public_signing_secret()
        token, payload = create_signed_report_token(
            tenant_id=tenant_id,
            account_code=account_code,
            month=int(period["month"]),
            year=int(period["year"]),
            start_date=str(period["start_date"] or ""),
            end_date=str(period["end_date"] or ""),
            ttl_days=ttl_days,
            secret=secret,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to create signed public link: {exc}",
        ) from exc

    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme or "https"
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
    path = f"/public/opssphere/advWebsiteReport/reports/cta?token={quote(token, safe='')}"
    url = f"{scheme}://{host}{path}" if host else path

    expires_at = datetime.fromtimestamp(int(payload["exp"]), tz=timezone.utc).isoformat()
    return {
        "url": url,
        "expiresAt": expires_at,
        "ttlDays": int(ttl_days),
    }


@router.get("/reports/cta")
def get_adv_website_cta_report_pdf(
    request: Request,
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
    Generate Advanced Website PDF report from GA4 events for a tenant account code
    and selected period, including Menu and CTA sections.

    Example request:
        GET /api/opssphere/advWebsiteReport/reports/cta?accountCode=TAAA&month=4&year=2026

    Example request (date range):
        GET /api/opssphere/advWebsiteReport/reports/cta?accountCode=TAAA&start_date=2026-04-01&end_date=2026-04-15

    Example response:
        Content-Type: application/pdf
        Content-Disposition: attachment; filename="OpsSphere-AdvWebsite-CTA-taaa-dcm-2604-2604281000.pdf"
        <binary pdf content>

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Tenant config must include opssphere.ga4.properties mapping for accountCode
        - Use either start_date/end_date OR month/year (not both)
        - Optional GA4 start-date floor:
          opssphere.ga4.start_date or opssphere.ga4.properties.<accountCode>.start_date
        - GA4 pulls dimensions: eventName + customEvent:CTA
        - GA4 pulls SRP sort dimension: customEvent:filter_group (event: srp_filter_select)
        - GA4 menu parent dimension: customEvent:mega_menu_parent
        - GA4 pulls metric: eventCount
        - Menu sections in report:
          Megamenus, Submenus
        - CTA sections in report:
          SRPs- New, SRPs- Used, VDPs- New, VDPs- Used
        - SRP filters section in report:
          SRP Sort Categories
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

    tenant_id = str(
        getattr(request.state, "tenant_id", None)
        or request.headers.get("x-tenant-id")
        or "tenant"
    ).strip()
    try:
        pdf_bytes, timezone_for_report = generate_adv_website_report_pdf(
            tenant_id=tenant_id,
            account_code=account_code,
            month=int(period["month"]),
            year=int(period["year"]),
            start_date=str(period["start_date"] or ""),
            end_date=str(period["end_date"] or ""),
            period_label=str(period["period_label"] or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to generate CTA report from GA4: {exc}",
        ) from exc

    filename = build_report_filename(
        tenant_id=tenant_id,
        account_code=account_code,
        month=int(period["month"]),
        year=int(period["year"]),
        start_date=str(period["start_date"] or ""),
        end_date=str(period["end_date"] or ""),
        timezone_for_report=timezone_for_report,
    )
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
