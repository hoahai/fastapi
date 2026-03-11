from fastapi import APIRouter

router = APIRouter()


@router.get("/bootstrap", summary="FundSphere bootstrap payload")
def bootstrap():
    """
    Return a starter bootstrap payload for FundSphere clients.

    Example request:
        GET /api/fundsphere/v1/bootstrap

    Example response:
        {
          "status": "ok",
          "app": "fundsphere",
          "version": "v1"
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
    """
    return {"status": "ok", "app": "fundsphere", "version": "v1"}
