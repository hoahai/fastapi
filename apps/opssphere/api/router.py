from fastapi import APIRouter

from apps.opssphere.api.features.advWebsiteReport import (
    router as adv_website_report_router,
)

router = APIRouter()
router.include_router(adv_website_report_router, tags=["opssphere"])
