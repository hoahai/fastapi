from fastapi import APIRouter, Query

from apps.spendsphere.api.v1.endpoints.periods import (
    build_periods_data,
    validate_month_offsets,
)
from apps.spendsphere.api.v1.helpers.ggAd import get_ggad_accounts
from shared.utils import run_parallel

router = APIRouter()


# ============================================================
# UI
# ============================================================


@router.get(
    "/ui/selections",
    summary="Get Google Ads clients and period metadata",
    description="Returns Google Ads clients and period data in a single response.",
)
def get_ui_selections_route(
    months_before: int = Query(
        2, description="Number of months before the current month to include."
    ),
    months_after: int = Query(
        1, description="Number of months after the current month to include."
    ),
):
    """
    Example request:
    GET /spendsphere/api/v1/ui/selections

    Example response:
    {
      "googleAdsClients": [
        {
          "id": "6563107233",
          "descriptiveName": "AUC_Autocity Credit",
          "accountCode": "AUC",
          "accountName": "Autocity Credit"
        }
      ],
      "periods": {
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
    }
    """
    validate_month_offsets(months_before, months_after)

    tasks = [
        (get_ggad_accounts, ()),
        (build_periods_data, (months_before, months_after)),
    ]
    clients, periods = run_parallel(
        tasks=tasks,
        api_name="spendsphere_v1_ui_selections",
    )
    return {
        "googleAdsClients": clients,
        "periods": periods,
    }
