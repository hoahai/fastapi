from fastapi import APIRouter

from apps.shiftzy.api.v1.endpoints import health

router = APIRouter(prefix="/v1")
router.include_router(health.router, tags=["health"])
