from fastapi import APIRouter

from apps.spendsphere.api.v1.endpoints import (
    allocations as v1_allocations,
    budgets as v1_budgets,
    current_period as v1_current_period,
    rollovers as v1_rollovers,
    update as v1_update,
)
from apps.spendsphere.api.v2.endpoints import (
    allocations,
    budgets,
    rollovers,
    update,
)

router = APIRouter(prefix="/v2")
router.include_router(budgets.router, tags=["spendsphere"])
router.include_router(allocations.router, tags=["spendsphere"])
router.include_router(rollovers.router, tags=["spendsphere"])
router.include_router(update.router, tags=["spendsphere"])

router.include_router(v1_current_period.router, tags=["spendsphere"], include_in_schema=False)
router.include_router(v1_budgets.router, tags=["spendsphere"], include_in_schema=False)
router.include_router(v1_allocations.router, tags=["spendsphere"], include_in_schema=False)
router.include_router(v1_rollovers.router, tags=["spendsphere"], include_in_schema=False)
router.include_router(v1_update.router, tags=["spendsphere"], include_in_schema=False)
