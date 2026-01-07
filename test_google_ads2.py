from __future__ import annotations

from functions.ggAd import (
    update_budgets,
    update_campaign_statuses,
)
from functions.utils import run_parallel
from functions.logger import (
    get_logger,
    enable_console_logging,
    log_run_start,
    log_run_end,
)

from functions.email import (
    build_google_ads_result_email,
    send_google_ads_result_email,
)

# =========================================================
# LOGGER
# =========================================================
file_logger = get_logger("job")
logger = get_logger(__name__)
enable_console_logging(logger)

# =========================================================
# MAIN
# =========================================================


def run_budget_update(customer_id: str, updates: list[dict]) -> dict:
    """
    Wrapper for parallel execution (args-only).
    """
    return update_budgets(
        customer_id=customer_id,
        updates=updates,
    )


def run_campaign_update(customer_id: str, updates: list[dict]) -> dict:
    """
    Wrapper for parallel execution (args-only).
    """
    return update_campaign_statuses(
        customer_id=customer_id,
        updates=updates,
    )


if __name__ == "__main__":
    log_run_start()

    try:
        # =====================================================
        # Example payloads (normally produced by transform step)
        # =====================================================

        budget_payloads = [
            {
                "customer_id": "1244599695",
                "updates": [
                    {
                        "budgetId": "13908992865",
                        "currentAmount": 9.01,
                        "newAmount": 9.0,
                    },
                    {
                        "budgetId": "13908993015",
                        "currentAmount": 17.49,
                        "newAmount": 17.48,
                    },
                    {
                        "budgetId": "14325382782",
                        "currentAmount": 24.3,
                        "newAmount": 23.83,
                    },
                ],
            },
            {
                "customer_id": "8048510771",
                "updates": [
                    {
                        "budgetId": "15264548297",
                        "currentAmount": 29.0,
                        "newAmount": 29.65,
                    },
                    {
                        "budgetId": "13542945751",
                        "currentAmount": 19.0,
                        "newAmount": 19.03,
                    },
                ],
            },
        ]

        campaign_payloads = []

        # =====================================================
        # Build parallel tasks
        # =====================================================

        tasks = []

        for payload in budget_payloads:
            tasks.append(
                (
                    run_budget_update,
                    (payload["customer_id"], payload["updates"]),
                )
            )

        for payload in campaign_payloads:
            tasks.append(
                (
                    run_campaign_update,
                    (payload["customer_id"], payload["updates"]),
                )
            )

        # =====================================================
        # Execute Google Ads updates in parallel
        # =====================================================

        results = run_parallel(
            tasks=tasks,
            api_name="google_ads_mutation",
        )

        # =====================================================
        # Log summary results
        # =====================================================
        aggregated_results = []
        total_summary = {
            "total": 0,
            "succeeded": 0,
            "failed": 0,
        }

        for r in results:
            aggregated_results.append(
                {
                    "customerId": r["customerId"],
                    "operation": r["operation"],
                    "summary": r["summary"],
                    "successes": r["successes"],
                    "failures": r["failures"],
                }
            )

            total_summary["total"] += r["summary"]["total"]
            total_summary["succeeded"] += r["summary"]["succeeded"]
            total_summary["failed"] += r["summary"]["failed"]

        logger.info(
            "Google Ads mutation completed",
            extra={
                "extra_fields": {
                    "operation": "update_campaign_statuses",
                    "customers_processed": len(results),
                    "overall_summary": total_summary,
                    "results": aggregated_results,
                }
            },
        )

        email_body = build_google_ads_result_email(
            overall_summary=total_summary,
            aggregated_results=aggregated_results,
        )

        send_google_ads_result_email(
            subject="Google Ads mutation completed",
            body=email_body,
        )

    except Exception as exc:
        logger.error(
            "Job failed",
            extra={
                "extra_fields": {
                    "event": "job_failed",
                    "job": "test_mysql_with_rollovers",
                    "error": str(exc),
                }
            },
        )
        raise

    finally:
        log_run_end()
