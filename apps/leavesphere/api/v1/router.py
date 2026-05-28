from fastapi import APIRouter, Depends

from apps.leavesphere.api.v1.endpoints import (
    employeeManagers,
    employees,
    ptoActions,
    ptoTransactions,
    ptoTypes,
)
from shared.auth.dependencies import enforce_leavesphere_permission

router = APIRouter(
    prefix="/v1",
    dependencies=[Depends(enforce_leavesphere_permission)],
)
router.include_router(employees.router, tags=["leavesphere"])
router.include_router(employeeManagers.router, tags=["leavesphere"])
router.include_router(ptoTypes.router, tags=["leavesphere"])
router.include_router(ptoActions.router, tags=["leavesphere"])
router.include_router(ptoTransactions.router, tags=["leavesphere"])
