from fastapi import APIRouter

from apps.spendsphere.api.v1.endpoints.core.ui import (
    budget_ui_router,
    main_ui_router,
)

# Backward-compatible aggregator for all UI routers.
router = APIRouter()
router.include_router(main_ui_router)
router.include_router(budget_ui_router)

__all__ = ["router", "main_ui_router", "budget_ui_router"]
