from fastapi import APIRouter, Body
from pydantic import BaseModel

from apps.spendsphere.api.v1.helpers.email import build_google_ads_alert_email
from shared.email import send_google_ads_result_email

router = APIRouter()


@router.post("/echo", summary="Echo request body")
def echo_route(payload: object = Body(...)):
    return payload


class TestEmailRequest(BaseModel):
    subject: str | None = None
    body: str | None = None
    html: str | None = None
    app_name: str | None = None
    simulate_alert: bool = False


@router.post("/echo/test-email", summary="Send a test email via Zoho Mail")
def test_email_route(payload: TestEmailRequest):
    if payload.simulate_alert:
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
        response = send_google_ads_result_email(
            subject,
            text_body,
            html=html_body,
            return_response=True,
        )
        return {"status": "sent", "mode": "alert", "zoho": response}

    subject = payload.subject or "Zoho Mail test"
    body = payload.body or "This is a test email sent from the SpendSphere API."
    response = send_google_ads_result_email(
        subject,
        body,
        html=payload.html,
        app_name=payload.app_name,
        return_response=True,
    )

    return {"status": "sent", "mode": "basic", "zoho": response}
