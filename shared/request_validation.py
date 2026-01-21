from __future__ import annotations

from fastapi import HTTPException, Request
from fastapi.routing import APIRoute


def validate_query_params(request: Request) -> None:
    route = request.scope.get("route")
    if not isinstance(route, APIRoute):
        return

    allowed = {param.alias or param.name for param in route.dependant.query_params}
    unknown = set(request.query_params.keys()) - allowed
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown query params: {', '.join(sorted(unknown))}",
        )
