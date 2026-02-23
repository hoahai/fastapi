from fastapi import APIRouter, Query

from apps.spendsphere.api.v1.helpers.ggAd import (
    get_ggad_accounts,
    get_ggad_accounts_with_summary,
)

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
        {
          "summary": {
            "total": 3,
            "valid": 1,
            "invalid": 2
          },
          "validAccounts": [
            {
              "id": "6563107233",
              "descriptiveName": "AUC_Autocity Credit",
              "accountCode": "AUC",
              "accountName": "Autocity Credit"
            }
          ],
          "invalidAccounts": [
            {
              "id": "1234567890",
              "descriptiveName": "Legacy Account",
              "reason": "invalid_name_format"
            },
            {
              "id": "9999999999",
              "descriptiveName": "NoCode_Account",
              "reason": "account_code_not_extractable",
              "accountCode": null
            }
          ]
        }
    """
    return get_ggad_accounts_with_summary(refresh_cache=refresh_cache)


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
