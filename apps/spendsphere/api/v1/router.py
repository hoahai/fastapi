from fastapi import APIRouter

from apps.spendsphere.api.v1.endpoints import (
    accelerations,
    allocations,
    budgets,
    current_period,
    rollovers,
    update,
)

router = APIRouter(prefix="/v1")
router.include_router(current_period.router, tags=["spendsphere"])
router.include_router(budgets.router, tags=["spendsphere"])
router.include_router(allocations.router, tags=["spendsphere"])
router.include_router(accelerations.router, tags=["spendsphere"])
router.include_router(rollovers.router, tags=["spendsphere"])
router.include_router(update.router, tags=["spendsphere"])
