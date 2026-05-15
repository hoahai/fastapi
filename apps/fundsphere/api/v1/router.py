from fastapi import APIRouter, Depends

from apps.fundsphere.api.v1.endpoints.masterBudgetControl import (
    budgetData,
    masterBudget,
    netSpend,
    settings,
)
from shared.auth.dependencies import enforce_fundsphere_permission


# ============================================================
# ROUTER
# ============================================================

router = APIRouter(
    prefix="/v1",
    dependencies=[Depends(enforce_fundsphere_permission)],
)
router.include_router(
    settings.router,
    prefix="/masterBudgetControl",
    tags=["fundsphere"],
)
router.include_router(
    budgetData.router,
    prefix="/masterBudgetControl",
    tags=["fundsphere"],
)
router.include_router(
    masterBudget.router,
    prefix="/masterBudgetControl",
    tags=["fundsphere"],
)
router.include_router(
    netSpend.router,
    prefix="/masterBudgetControl",
    tags=["fundsphere"],
)
