from fastapi import APIRouter, Depends

from apps.tradsphere.api.v1.endpoints.core import (
    accounts,
    broadcastCalendar,
    contacts,
    deliveryMethods,
    estNums,
    invoiceChecklistNotes,
    invoiceChecklistStations,
    invoiceChecklists,
    invoiceNoteAttachments,
    schedules,
    schedulesImport,
    schedulesWeeks,
    stations,
    stationsContacts,
)
from apps.tradsphere.api.v1.endpoints.core.ui import main as uiMain
from shared.auth.dependencies import enforce_tradsphere_permission

router = APIRouter(
    prefix="/v1",
    dependencies=[Depends(enforce_tradsphere_permission)],
)
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
router.include_router(invoiceChecklists.router, tags=["tradsphere"])
router.include_router(invoiceChecklistStations.router, tags=["tradsphere"])
router.include_router(invoiceChecklistNotes.router, tags=["tradsphere"])
router.include_router(invoiceNoteAttachments.router, tags=["tradsphere"])
router.include_router(uiMain.router, tags=["tradsphere"])
