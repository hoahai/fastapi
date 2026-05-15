from fastapi import APIRouter, Depends

from apps.opssphere.api.features.advWebsiteReport import (
    router as adv_website_report_router,
)
from shared.auth.dependencies import enforce_opssphere_permission

router = APIRouter(dependencies=[Depends(enforce_opssphere_permission)])
router.include_router(adv_website_report_router, tags=["opssphere"])
