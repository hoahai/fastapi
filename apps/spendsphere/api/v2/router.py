from fastapi import APIRouter

from apps.spendsphere.api.v2.endpoints import spendsphere

router = APIRouter(prefix="/api/v2")
router.include_router(spendsphere.router, tags=["spendsphere"])
