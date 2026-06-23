from fastapi import APIRouter, Depends

from apps.leavesphere.api.v1.endpoints import (
    leaveManagement,
    employeeManagers,
    employees,
    googleCalendar,
    ptoActions,
    ptoTransactions,
    ptoTypes,
)
from apps.leavesphere.api.v1.endpoints.ui.myPto import router as my_pto_ui_router
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
router.include_router(leaveManagement.router, tags=["leavesphere"])
router.include_router(googleCalendar.router, tags=["leavesphere"])
router.include_router(my_pto_ui_router, tags=["leavesphere"])
