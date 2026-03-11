from fastapi import APIRouter

from apps.fundsphere.api.v1.endpoints.core import accounts, bootstrap

router = APIRouter(prefix="/v1")
router.include_router(accounts.router, tags=["fundsphere"])
router.include_router(bootstrap.router, tags=["fundsphere"])
