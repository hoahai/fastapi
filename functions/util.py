# functions/utils.py
from datetime import datetime, date
import calendar
import pytz
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Callable, Dict, List

from functions.constants import (
    TIMEZONE,
    GGAD_MAX_WORKERS,
    GGAD_MAX_RETRIES,
    GGAD_INITIAL_BACKOFF,
    GGAD_MAX_BACKOFF,
    GGAD_ACCOUNT_TIMEOUT,
    GGAD_JITTER_MIN,
    GGAD_JITTER_MAX,
)

def get_current_period():
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)

    year = now.year
    month = now.month

    # First day of month
    start_date = date(year, month, 1)

    # Last day of month
    last_day = calendar.monthrange(year, month)[1]
    end_date = date(year, month, last_day)

    return {
        "year": year,
        "month": month,
        "start_date": start_date.isoformat(),  # YYYY-MM-DD
        "end_date": end_date.isoformat(),      # YYYY-MM-DD
    }

def _run_with_retry(
    func: Callable[[Dict], List[Dict]],
    account: Dict
) -> List[Dict]:
    """
    Run a per-account function with retry + exponential backoff + jitter
    """
    for attempt in range(1, GGAD_MAX_RETRIES + 1):
        try:
            # Jitter before execution
            time.sleep(random.uniform(GGAD_JITTER_MIN, GGAD_JITTER_MAX))

            return func(account)

        except Exception as e:
            if attempt == GGAD_MAX_RETRIES:
                raise RuntimeError(
                    f"Account {account.get('id')} failed after "
                    f"{GGAD_MAX_RETRIES} attempts: {e}"
                )

            backoff = min(
                GGAD_INITIAL_BACKOFF * (2 ** (attempt - 1)),
                GGAD_MAX_BACKOFF
            )
            time.sleep(backoff)


def run_parallel_accounts(
    accounts: List[Dict],
    per_account_func: Callable[[Dict], List[Dict]],
) -> List[Dict]:
    """
    Run a function in parallel across Google Ads accounts with:
    - limited concurrency
    - retry + exponential backoff
    - jitter
    - per-account timeout
    """
    if not accounts:
        return []

    results: List[Dict] = []

    with ThreadPoolExecutor(max_workers=GGAD_MAX_WORKERS) as executor:
        future_map = {
            executor.submit(_run_with_retry, per_account_func, acc): acc
            for acc in accounts
        }

        for future in as_completed(future_map):
            account = future_map[future]
            customer_id = account.get("id")

            try:
                data = future.result(timeout=GGAD_ACCOUNT_TIMEOUT)
                results.extend(data)

            except TimeoutError:
                results.append({
                    "customerId": customer_id,
                    "accountCode": account.get("accountCode"),
                    "error": f"Timeout after {GGAD_ACCOUNT_TIMEOUT}s"
                })

            except Exception as e:
                results.append({
                    "customerId": customer_id,
                    "accountCode": account.get("accountCode"),
                    "error": str(e)
                })

    return results