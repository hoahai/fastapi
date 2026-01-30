from fastapi import APIRouter, Body

router = APIRouter()


@router.post("/echo", summary="Echo request body")
def echo_route(payload: object = Body(...)):
    return payload
