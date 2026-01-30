from fastapi import APIRouter

from apps.spendsphere.api.v1.endpoints import (
    accelerations,
    allocations,
    budgets,
    googleAds,
    periods,
    rollovers,
    ui,
    update,
)

router = APIRouter(prefix="/v1")
router.include_router(periods.current_period_router, tags=["spendsphere"])
router.include_router(googleAds.router, tags=["spendsphere"])
router.include_router(periods.router, tags=["spendsphere"])
router.include_router(ui.router, tags=["spendsphere"])
router.include_router(budgets.router, tags=["spendsphere"])
router.include_router(allocations.router, tags=["spendsphere"])
router.include_router(accelerations.router, tags=["spendsphere"])
router.include_router(rollovers.router, tags=["spendsphere"])
router.include_router(update.router, tags=["spendsphere"])
