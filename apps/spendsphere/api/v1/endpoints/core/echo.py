from typing import Literal

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel

from apps.spendsphere.api.v1.helpers.email import build_google_ads_alert_email
from shared.email import send_google_ads_result_email
from shared.logger import send_axiom_test_log

router = APIRouter()


@router.post("/echo", summary="Echo request body")
def echo_route(request_payload: object = Body(...)):
    """
    Return the same JSON payload that was posted to this endpoint.

    Example request:
        POST /api/spendsphere/v1/echo
        {
          "ping": "pong"
        }

    Example response:
        {
          "meta": {
            "success": true,
            "request_id": "8d0f4de2-5a72-4ea2-9f2f-6f60f6769a20"
          },
          "data": {
            "ping": "pong"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
    """
    return request_payload


class TestEmailRequest(BaseModel):
    subject: str | None = None
    body: str | None = None
    html: str | None = None
    app_name: str | None = None
    to_addresses: list[str] | None = None
    simulate_alert: bool = False
    force_api_error: bool = False


class TestAxiomRequest(BaseModel):
    message: str | None = None
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    force_error: bool = False


@router.post("/echo/test-axiom", summary="Send a synchronous test log to Axiom")
def test_axiom_route(request_payload: TestAxiomRequest):
    """
    Send a single synchronous Axiom test log for the current tenant/app scope.

    Example request:
        POST /api/spendsphere/v1/echo/test-axiom
        {
          "message": "SpendSphere Axiom test log",
          "level": "INFO"
        }

    Example request (force fallback email):
        POST /api/spendsphere/v1/echo/test-axiom
        {
          "message": "Force Axiom failure and trigger backup email",
          "level": "ERROR",
          "force_error": true
        }

    Example response:
        {
          "meta": {
            "success": true,
            "request_id": "6b8b0c40-5bdf-4a77-8a0c-6d3d0e90f6a9"
          },
          "data": {
            "status": "sent",
            "axiom": {
              "sent": true,
              "dataset": "spendsphere-logs",
              "apiUrl": "https://api.axiom.co",
              "forcedError": false
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - If Axiom ingest fails, fallback email is sent to truonghoahai@gmail.com
    """
    message = request_payload.message or "SpendSphere Axiom test log"
    try:
        result = send_axiom_test_log(
            message=message,
            level=request_payload.level,
            force_error=request_payload.force_error,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {"status": "sent", "axiom": result}


@router.post("/echo/test-email", summary="Send a test email via Zoho Mail")
def test_email_route(request_payload: TestEmailRequest):
    """
    Send a test email through Zoho Mail for the current tenant context.

    Example request:
        POST /api/spendsphere/v1/echo/test-email
        {
          "subject": "SpendSphere test email",
          "body": "This is a test",
          "app_name": "spendsphere",
          "to_addresses": ["truonghoahai@gmail.com"]
        }

    Example request (simulate email API failure):
        POST /api/spendsphere/v1/echo/test-email
        {
          "subject": "Force email error",
          "body": "Expect Axiom error log",
          "force_api_error": true
        }

    Example response:
        {
          "meta": {
            "success": true,
            "request_id": "c5d8ad08-9f77-49b3-8a7d-0d3db3a9b7ca"
          },
          "data": {
            "status": "sent",
            "mode": "basic",
            "zoho": {
              "status": 200,
              "body": "{\"status\":{\"code\":200,\"description\":\"mail sent\"}}"
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - If email API fails, the error is logged to Axiom
        - Simulated/real email API failures return HTTP 502 from this test route
    """
    def _send_email_for_test(
        *,
        subject: str,
        body: str,
        html: str | None = None,
    ) -> dict:
        try:
            return send_google_ads_result_email(
                subject,
                body,
                html=html,
                app_name=request_payload.app_name,
                to_addresses=request_payload.to_addresses,
                force_api_error=request_payload.force_api_error,
                return_response=True,
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    if request_payload.simulate_alert:
        sample_report = {
            "dry_run": True,
            "account_codes": ["TEST"],
            "overall_summary": {
                "total": 6,
                "succeeded": 3,
                "failed": 2,
                "warnings": 1,
            },
            "mutation_results": [
                {
                    "customerId": "1234567890",
                    "accountCode": "TEST",
                    "operation": "update_budgets",
                    "summary": {"total": 3, "succeeded": 2, "failed": 1},
                    "failures": [
                        {
                            "budgetId": "BUDG-001",
                            "accountCode": "TEST",
                            "campaignNames": ["Campaign A", "Campaign B"],
                            "currentAmount": 100.0,
                            "newAmount": 150.0,
                            "error": "Budget update failed for campaign A",
                        }
                    ],
                    "warnings": [
                        {
                            "budgetId": "BUDG-002",
                            "accountCode": "TEST",
                            "campaignNames": ["Campaign C"],
                            "currentAmount": 80.0,
                            "newAmount": 60.0,
                            "error": "Campaign C already paused",
                        }
                    ],
                },
                {
                    "customerId": "1234567890",
                    "accountCode": "TEST",
                    "operation": "update_campaign_statuses",
                    "summary": {
                        "total": 3,
                        "succeeded": 1,
                        "failed": 1,
                        "warnings": 1,
                    },
                    "failures": [
                        {
                            "campaignId": "CMP-001",
                            "oldStatus": "ENABLED",
                            "newStatus": "PAUSED",
                            "error": "Campaign status update failed for campaign B",
                        }
                    ],
                    "warnings": [],
                },
            ],
        }
        subject, text_body, html_body = build_google_ads_alert_email(
            full_report=sample_report,
        )
        response = _send_email_for_test(
            subject=subject,
            body=text_body,
            html=html_body,
        )
        return {"status": "sent", "mode": "alert", "zoho": response}

    subject = request_payload.subject or "Zoho Mail test"
    body = (
        request_payload.body
        or "This is a test email sent from the SpendSphere API."
    )
    response = _send_email_for_test(
        subject=subject,
        body=body,
        html=request_payload.html,
    )

    return {"status": "sent", "mode": "basic", "zoho": response}
