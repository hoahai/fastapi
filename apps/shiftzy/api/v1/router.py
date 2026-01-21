from fastapi import APIRouter

from apps.shiftzy.api.v1.endpoints import health, weeks

router = APIRouter(prefix="/v1")
router.include_router(health.router, tags=["health"])
router.include_router(weeks.router, tags=["weeks"])
