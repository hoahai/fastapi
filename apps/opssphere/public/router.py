from fastapi import APIRouter

from apps.opssphere.public.routes.advWebsiteReport import (
    router as adv_website_report_public_router,
)

router = APIRouter()
router.include_router(adv_website_report_public_router, tags=["opssphere-public"])

__all__ = ["router"]
