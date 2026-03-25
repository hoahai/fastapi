from __future__ import annotations

from fastapi import HTTPException, Request
from fastapi.routing import APIRoute


_ALLOW_UNKNOWN_QUERY_PARAMS_ATTR = "__allow_unknown_query_params__"


def allow_unknown_query_params(func):
    setattr(func, _ALLOW_UNKNOWN_QUERY_PARAMS_ATTR, True)
    return func


def validate_query_params(request: Request) -> None:
    route = request.scope.get("route")
    if not isinstance(route, APIRoute):
        return

    if getattr(route.endpoint, _ALLOW_UNKNOWN_QUERY_PARAMS_ATTR, False):
        return

    allowed = {param.alias or param.name for param in route.dependant.query_params}
    unknown = set(request.query_params.keys()) - allowed
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown query params: {', '.join(sorted(unknown))}",
        )
