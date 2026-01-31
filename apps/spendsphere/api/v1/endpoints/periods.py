from fastapi import APIRouter, HTTPException, Query

from apps.spendsphere.api.v1.helpers.ggSheet import get_active_period
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import require_account_code
from shared.utils import get_current_period

router = APIRouter()
current_period_router = APIRouter()


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    total = year * 12 + (month - 1) + delta
    new_year = total // 12
    new_month = (total % 12) + 1
    return new_year, new_month


def build_periods_data(months_before: int, months_after: int) -> dict:
    period = get_current_period()
    current_year = period["year"]
    current_month = period["month"]

    months_array: list[dict] = []
    for offset in range(-months_before, months_after + 1):
        year, month = _shift_month(current_year, current_month, offset)
        months_array.append(
            {
                "month": month,
                "year": year,
                "period": f"{month}/{year}",
            }
        )

    return {
        "currentPeriod": f"{current_month}/{current_year}",
        "monthsArray": months_array,
    }


def validate_month_offsets(months_before: int, months_after: int) -> None:
    if months_before < 0 or months_after < 0:
        raise HTTPException(
            status_code=400,
            detail="months_before and months_after must be >= 0",
        )


# ============================================================
# PERIODS
# ============================================================


@router.get(
    "/periods",
    summary="Get current period and surrounding months",
    description="Returns the current period plus a window of months around it.",
)
def get_periods_route(
    months_before: int = Query(
        2, description="Number of months before the current month to include."
    ),
    months_after: int = Query(
        1, description="Number of months after the current month to include."
    ),
):
    """
    Example request:
    GET /spendsphere/api/v1/periods

    Example request (custom window):
    GET /spendsphere/api/v1/periods?months_before=3&months_after=2

    Example response:
    {
      "currentPeriod": "1/2026",
      "monthsArray": [
        {
          "month": 11,
          "year": 2025,
          "period": "11/2025"
        },
        {
          "month": 12,
          "year": 2025,
          "period": "12/2025"
        },
        {
          "month": 1,
          "year": 2026,
          "period": "1/2026"
        },
        {
          "month": 2,
          "year": 2026,
          "period": "2/2026"
        }
      ]
    }
    """
    validate_month_offsets(months_before, months_after)
    return build_periods_data(months_before, months_after)



@current_period_router.get("/current-period")
def get_current_period_route():
    """
    Example request:
    GET /spendsphere/api/v1/current-period

    Example response:
    {
      "year": 2026,
      "month": 1,
      "start_date": "2026-01-01",
      "end_date": "2026-01-31"
    }
    """
    return get_current_period()


@router.get(
    "/active-period/{account_code}",
    summary="Get active period for an account",
    description="Returns the active period metadata from the active period sheet.",
)
def get_active_period_route(account_code: str):
    """
    Example request:
    GET /spendsphere/api/v1/active-period/TAAA

    Example response:
    {
      "accountCode": "TAAA",
      "startDate": "2026-01-01",
      "endDate": "2026-01-31",
      "isActive": true
    }
    """
    account_code = require_account_code(account_code)
    data = get_active_period([account_code])
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No active period found for account_code '{account_code}'",
        )
    return data[0]
