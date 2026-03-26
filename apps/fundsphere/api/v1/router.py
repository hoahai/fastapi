from fastapi import APIRouter

from apps.fundsphere.api.v1.endpoints.masterBudgetControl import budgetData, settings


# ============================================================
# ROUTER
# ============================================================

router = APIRouter(prefix="/v1")
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
