# test_mysql_with_rollovers.py

from __future__ import annotations

import os
import pandas as pd

from functions.db_queries import (
    get_masterbudgets,
    get_allocations,
    get_rollbreakdowns,
)

from functions.ggSheet import get_rollovers

from functions.ggAd import (
    get_ggad_accounts,
    get_ggad_campaigns,
    get_ggad_budgets,
    get_ggad_spents,
    update_budgets,
    update_campaign_statuses,
)

from functions.dataTransform import transform_google_ads_data

from functions.utils import run_parallel
from functions.logger import (
    get_logger,
    enable_console_logging,
    log_run_start,
    log_run_end,
)

# =========================================================
# LOGGER
# =========================================================
# File-only logger (default)
file_logger = get_logger("job")

# Logger
logger = get_logger(__name__)
enable_console_logging(logger)

# =========================================================
# CONFIG
# =========================================================

# Can be:
# - None or ""        → all accounts
# - "TAC"             → single account
# - ["TAC", "TAAA"]   → multiple accounts
ACCOUNT_CODE = []

OUTPUT_DIR = "output"

# =========================================================
# HELPERS
# =========================================================


def normalize_account_codes(account_code):
    """
    Normalize account_code into a set[str] (uppercase),
    or None meaning "all accounts".
    """
    if account_code is None:
        return None

    # Empty string or whitespace → all accounts
    if isinstance(account_code, str):
        code = account_code.strip()
        return {code.upper()} if code else None

    # Empty list → all accounts
    if isinstance(account_code, list):
        cleaned = {
            code.strip().upper()
            for code in account_code
            if isinstance(code, str) and code.strip()
        }
        return cleaned if cleaned else None

    raise TypeError("ACCOUNT_CODE must be None, str, or list[str]")


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


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    log_run_start()

    try:
        # ---------------------------------
        # Normalize account filter once
        # ---------------------------------
        account_code_filter = normalize_account_codes(ACCOUNT_CODE)

        # =====================================================
        # 1. Google Sheets (SYNC — NO THREADS)
        # =====================================================
        rollovers = get_rollovers(ACCOUNT_CODE)

        # =====================================================
        # 2. MySQL — Parallel
        # =====================================================
        master_budgets, allocations, rollbreakdowns = run_parallel(
            tasks=[
                (get_masterbudgets, (ACCOUNT_CODE,)),
                (get_allocations, (ACCOUNT_CODE,)),
                (get_rollbreakdowns, (ACCOUNT_CODE,)),
            ],
            api_name="database",
        )

        # =====================================================
        # 3. Google Ads — Parallel
        # =====================================================
        accounts = get_ggad_accounts()
        account_code_filter = normalize_account_codes(ACCOUNT_CODE)

        if account_code_filter is not None:
            accounts = [
                acc
                for acc in accounts
                if acc.get("accountCode", "").upper() in account_code_filter
            ]

        campaigns, budgets, costs = run_parallel(
            tasks=[
                (get_ggad_campaigns, (accounts,)),
                (get_ggad_budgets, (accounts,)),
                (get_ggad_spents, (accounts,)),
            ],
            api_name="google_ads",
        )

        # =====================================================
        # 4. Transform Data
        # =====================================================
        results = transform_google_ads_data(
            master_budgets=master_budgets,
            campaigns=campaigns,
            budgets=budgets,
            costs=costs,
            allocations=allocations,
            rollovers=rollovers,
        )

        # =====================================================
        # 5. Export Results to Excel (overwrite)
        # =====================================================
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        output_file = os.path.join(
            OUTPUT_DIR,
            "results.xlsx",  # fixed name → overwrite
        )

        df = pd.DataFrame(results)
        df.to_excel(
            output_file,
            index=False,
            engine="openpyxl",
        )

        file_logger.info(
            "Results exported to Excel",
            extra={
                "extra_fields": {
                    "event": "export_excel",
                    "file_path": output_file,
                    "rows": len(results),
                }
            },
        )

        # =====================================================
        # JOB SUCCESS
        # =====================================================
        logger.info(
            "Job completed successfully",
            extra={
                "extra_fields": {
                    "event": "job_success",
                    "job": "test_mysql_with_rollovers",
                    "account_codes": (
                        sorted(account_code_filter) if account_code_filter else "ALL"
                    ),
                }
            },
        )

    except Exception as exc:
        # =====================================================
        # JOB FAILURE
        # =====================================================
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
        # =====================================================
        # RUN END (ALWAYS)
        # =====================================================
        log_run_end()
