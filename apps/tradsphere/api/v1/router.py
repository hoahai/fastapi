from fastapi import APIRouter

from apps.tradsphere.api.v1.endpoints import (
    accounts,
    broadcastCalendar,
    contacts,
    deliveryMethods,
    estNums,
    stations,
    stationsContacts,
)

router = APIRouter(prefix="/v1")
router.include_router(accounts.router, tags=["tradsphere"])
router.include_router(estNums.router, tags=["tradsphere"])
router.include_router(stations.router, tags=["tradsphere"])
router.include_router(deliveryMethods.router, tags=["tradsphere"])
router.include_router(contacts.router, tags=["tradsphere"])
router.include_router(stationsContacts.router, tags=["tradsphere"])
router.include_router(broadcastCalendar.router, tags=["tradsphere"])
