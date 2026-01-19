from fastapi import APIRouter

from api.v1.endpoints import spendsphere

router = APIRouter(prefix="/api/v1")
router.include_router(spendsphere.router, tags=["spendsphere"])
