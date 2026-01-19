from fastapi import APIRouter

from apps.spendsphere.api.v2.endpoints import spendsphere

router = APIRouter(prefix="/v2")
router.include_router(spendsphere.router, tags=["spendsphere"])
