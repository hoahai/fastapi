from fastapi import APIRouter

from apps.spendsphere.api.v1.helpers.ggAd import get_ggad_accounts

router = APIRouter()


# ============================================================
# GOOGLE ADS CLIENTS
# ============================================================


@router.get(
    "/google-ads",
    summary="List Google Ads clients",
    description="Returns Google Ads clients that match the SpendSphere naming convention.",
)
def get_google_ads_clients_route():
    """
    Example request:
    GET /spendsphere/api/v1/google-ads

    Example response:
    [
      {
        "id": "6563107233",
        "descriptiveName": "AUC_Autocity Credit",
        "accountCode": "AUC",
        "accountName": "Autocity Credit"
      }
    ]
    """
    return get_ggad_accounts()
