from fastapi import APIRouter, Query

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
def get_google_ads_clients_route(
    refresh_cache: bool = Query(
        False, description="When true, refreshes the Google Ads client cache."
    ),
):
    """
    Example request:
    GET /api/spendsphere/v1/google-ads
    Header: X-Tenant-Id: acme

    Example request (force refresh):
    GET /api/spendsphere/v1/google-ads?refresh_cache=true
    Header: X-Tenant-Id: acme

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
    return get_ggad_accounts(refresh_cache=refresh_cache)


@router.post(
    "/google-ads/refresh",
    summary="Refresh Google Ads client cache",
    description="Forces a refresh of the Google Ads client cache for the current tenant.",
)
def refresh_google_ads_clients_route():
    """
    Example request:
    POST /api/spendsphere/v1/google-ads/refresh
    Header: X-Tenant-Id: acme

    Example response:
    {
      "googleAdsClients": [
        {
          "id": "6563107233",
          "descriptiveName": "AUC_Autocity Credit",
          "accountCode": "AUC",
          "accountName": "Autocity Credit"
        }
      ]
    }
    """
    return {"googleAdsClients": get_ggad_accounts(refresh_cache=True)}
