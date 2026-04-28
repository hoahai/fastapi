from fastapi import APIRouter

from apps.opssphere.api.features.advWebsiteReport.debug import (
    router as debug_router,
)
from apps.opssphere.api.features.advWebsiteReport.report import (
    router as report_router,
)

router = APIRouter()
router.include_router(debug_router)
router.include_router(report_router)

__all__ = ["router"]
