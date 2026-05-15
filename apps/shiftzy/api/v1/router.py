from fastapi import APIRouter, Depends

from apps.shiftzy.api.v1.endpoints import (
    bootstrap,
    employees,
    positions,
    schedules,
    shifts,
    weeks,
)
from shared.auth.dependencies import enforce_shiftzy_permission

router = APIRouter(
    prefix="/v1",
    dependencies=[Depends(enforce_shiftzy_permission)],
)
router.include_router(bootstrap.router, tags=["bootstrap"])
router.include_router(weeks.router, tags=["weeks"])
router.include_router(positions.router, tags=["positions"])
router.include_router(employees.router, tags=["employees"])
router.include_router(shifts.router, tags=["shifts"])
router.include_router(schedules.router, tags=["schedules"])
