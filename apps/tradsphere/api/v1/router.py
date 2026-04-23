from fastapi import APIRouter

from apps.tradsphere.api.v1.endpoints import (
    accounts,
    broadcastCalendar,
    contacts,
    deliveryMethods,
    estNums,
    schedules,
    schedulesImport,
    schedulesWeeks,
    stations,
    stationsContacts,
)

router = APIRouter(prefix="/v1")
router.include_router(accounts.router, tags=["tradsphere"])
router.include_router(estNums.router, tags=["tradsphere"])
router.include_router(stations.router, tags=["tradsphere"])
router.include_router(deliveryMethods.router, tags=["tradsphere"])
router.include_router(schedules.router, tags=["tradsphere"])
router.include_router(schedulesImport.router, tags=["tradsphere"])
router.include_router(schedulesWeeks.router, tags=["tradsphere"])
router.include_router(contacts.router, tags=["tradsphere"])
router.include_router(stationsContacts.router, tags=["tradsphere"])
router.include_router(broadcastCalendar.router, tags=["tradsphere"])
