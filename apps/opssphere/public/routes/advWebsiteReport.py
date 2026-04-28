from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from apps.opssphere.api.features.advWebsiteReport.helpers.publicLink import (
    decode_signed_report_payload_without_verify,
    resolve_public_signing_secret,
    verify_signed_report_token,
)
from apps.opssphere.api.features.advWebsiteReport.helpers.reportService import (
    build_report_filename,
    generate_adv_website_report_pdf,
)
from shared.tenant import (
    TenantConfigError,
    reset_tenant_context,
    set_tenant_context,
)

router = APIRouter(prefix="/public/opssphere/advWebsiteReport")


@router.get("/reports/cta")
def get_public_adv_website_cta_report_pdf(
    request: Request,
    token: str = Query(
        ...,
        min_length=10,
        description="Signed public report token.",
    ),
):
    """
    Resolve a signed public URL token and return Advanced Website report PDF inline.

    Example request:
        GET /public/opssphere/advWebsiteReport/reports/cta?token=eyJ2Ijox...

    Example response:
        Content-Type: application/pdf
        Content-Disposition: inline; filename="OpsSphere-AdvWebsite-CTA-taaa-dcm-2604-2604281000.pdf"
        <binary pdf content>

    Requirements:
        - No API key required
        - No X-Tenant-Id required
        - token must be valid and not expired
        - Signing secret required via opssphere.public.signing_secret or OPSSPHERE_PUBLIC_SIGNING_SECRET
    """
    try:
        unverified_payload = decode_signed_report_payload_without_verify(token)
        tenant_id = str(unverified_payload.get("tenantId") or "").strip().lower()
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    if not tenant_id:
        raise HTTPException(status_code=401, detail="Invalid token payload.")

    tenant_ctx_token = None
    try:
        tenant_ctx_token = set_tenant_context(tenant_id)
        request.state.tenant_id = tenant_id
        try:
            secret = resolve_public_signing_secret()
            payload = verify_signed_report_token(token, secret=secret)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc

        account_code = str(payload.get("accountCode") or "").strip().upper()
        start_date = str(payload.get("startDate") or "").strip()
        end_date = str(payload.get("endDate") or "").strip()
        try:
            month = int(payload.get("month") or 0)
            year = int(payload.get("year") or 0)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=401, detail="Invalid token claims.") from exc
        if not account_code or month < 1 or month > 12 or year < 2000 or year > 2100:
            raise HTTPException(status_code=401, detail="Invalid token claims.")

        try:
            pdf_bytes, timezone_for_report = generate_adv_website_report_pdf(
                tenant_id=tenant_id,
                account_code=account_code,
                month=month,
                year=year,
                start_date=start_date,
                end_date=end_date,
                period_label=(
                    start_date if (start_date and start_date == end_date)
                    else f"{start_date} to {end_date}" if (start_date and end_date)
                    else ""
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to generate CTA report from GA4: {exc}",
            ) from exc
    except TenantConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if tenant_ctx_token is not None:
            reset_tenant_context(tenant_ctx_token)

    filename = build_report_filename(
        tenant_id=tenant_id,
        account_code=account_code,
        month=month,
        year=year,
        start_date=start_date,
        end_date=end_date,
        timezone_for_report=timezone_for_report,
    )
    headers = {"Content-Disposition": f'inline; filename="{filename}"'}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
