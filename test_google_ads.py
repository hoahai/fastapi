# test_mysql_with_rollovers.py

from functions.db_queries import (
    get_masterbudgets,
    get_allocations,
    get_rollbreakdowns,
)

from functions.ggAd import (
    get_ggad_accounts,
    get_ggad_campaigns,
    get_ggad_budgets,
    get_ggad_spents,
)

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

logger = get_logger(__name__)
enable_console_logging(logger)

# =========================================================
# CONFIG
# =========================================================

# Can be:
# - None or ""        → all accounts
# - "TAC"             → single account
# - ["TAC", "TAAA"]   → multiple accounts
ACCOUNT_CODE = ["TAC", "TAAA"]

# =========================================================
# HELPERS
# =========================================================

def normalize_account_codes(account_code):
    """
    Normalize account_code into a set[str] (uppercase),
    or None meaning "all accounts".
    """
    if not account_code:
        return None

    if isinstance(account_code, str):
        return {account_code.strip().upper()}

    if isinstance(account_code, list):
        return {
            code.strip().upper()
            for code in account_code
            if isinstance(code, str) and code.strip()
        }

    raise TypeError("ACCOUNT_CODE must be None, str, or list[str]")

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
        # 1. MySQL — Parallel
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
        # 2. Google Ads — Parallel
        # =====================================================
        accounts = get_ggad_accounts()

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
        # JOB SUCCESS
        # =====================================================
        logger.info(
            "Job completed successfully",
            extra={
                "extra_fields": {
                    "event": "job_success",
                    "job": "test_mysql_with_rollovers",
                    "account_codes": (
                        sorted(account_code_filter)
                        if account_code_filter
                        else "ALL"
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
