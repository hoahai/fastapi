from fastapi import APIRouter

from apps.shiftzy.api.v1.endpoints import (
    bootstrap,
    employees,
    positions,
    schedules,
    shifts,
    weeks,
)

router = APIRouter(prefix="/v1")
router.include_router(bootstrap.router, tags=["bootstrap"])
router.include_router(weeks.router, tags=["weeks"])
router.include_router(positions.router, tags=["positions"])
router.include_router(employees.router, tags=["employees"])
router.include_router(shifts.router, tags=["shifts"])
router.include_router(schedules.router, tags=["schedules"])
