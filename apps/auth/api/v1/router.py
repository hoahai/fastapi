from fastapi import APIRouter

from apps.auth.api.v1.endpoints import admin, invitations, session

router = APIRouter(prefix="/v1")
router.include_router(session.router, tags=["auth"])
router.include_router(session.debug_router, tags=["auth"])
router.include_router(invitations.router, tags=["auth"])
router.include_router(admin.router, tags=["auth-admin"])
