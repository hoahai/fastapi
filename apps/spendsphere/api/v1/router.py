from fastapi import APIRouter, Depends

from apps.spendsphere.api.v1.endpoints.core import (
    accelerations,
    allocations,
    budgetReports,
    budgets,
    caches,
    echo,
    googleAds,
    periods,
    rollovers,
    uis,
    updates,
)
from shared.auth.dependencies import enforce_spendsphere_permission

router = APIRouter(
    prefix="/v1",
    dependencies=[Depends(enforce_spendsphere_permission)],
)
router.include_router(periods.current_period_router, tags=["spendsphere"])
router.include_router(googleAds.router, tags=["spendsphere"])
router.include_router(periods.router, tags=["spendsphere"])
router.include_router(uis.router, tags=["spendsphere"])
router.include_router(budgets.router, tags=["spendsphere"])
router.include_router(allocations.router, tags=["spendsphere"])
router.include_router(budgetReports.router, tags=["spendsphere"])
router.include_router(accelerations.router, tags=["spendsphere"])
router.include_router(rollovers.router, tags=["spendsphere"])
router.include_router(echo.router, tags=["spendsphere"])
router.include_router(updates.router, tags=["spendsphere"])
router.include_router(caches.router, tags=["spendsphere"])
