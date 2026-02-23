from fastapi import APIRouter

from apps.spendsphere.api.v1.endpoints.core import (
    accelerations,
    allocations,
    budgets,
    caches,
    echo,
    googleAds,
    periods,
    rollovers,
    uis,
    updates,
)
from apps.spendsphere.api.v1.endpoints.custom import budgetManagements

router = APIRouter(prefix="/v1")
router.include_router(periods.current_period_router, tags=["spendsphere"])
router.include_router(googleAds.router, tags=["spendsphere"])
router.include_router(periods.router, tags=["spendsphere"])
router.include_router(uis.router, tags=["spendsphere"])
router.include_router(budgets.router, tags=["spendsphere"])
router.include_router(allocations.router, tags=["spendsphere"])
router.include_router(accelerations.router, tags=["spendsphere"])
router.include_router(rollovers.router, tags=["spendsphere"])
router.include_router(echo.router, tags=["spendsphere"])
router.include_router(updates.router, tags=["spendsphere"])
router.include_router(caches.router, tags=["spendsphere"])
router.include_router(budgetManagements.router, tags=["spendsphere"])
