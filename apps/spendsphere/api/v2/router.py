from fastapi import APIRouter

from apps.spendsphere.api.v1.endpoints import spendsphere as v1_spendsphere
from apps.spendsphere.api.v2.endpoints import spendsphere as v2_spendsphere

router = APIRouter(prefix="/v2")
router.include_router(v2_spendsphere.router, tags=["spendsphere"])
router.include_router(
    v1_spendsphere.router,
    tags=["spendsphere"],
    include_in_schema=False,
)
