from fastapi import APIRouter

from apps.spendsphere.api.v1.endpoints import spendsphere

router = APIRouter(prefix="/v1")
router.include_router(spendsphere.router, tags=["spendsphere"])
