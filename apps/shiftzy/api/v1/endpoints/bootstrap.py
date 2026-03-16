from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from apps.shiftzy.api.v1.helpers.db_queries import (
    get_employees,
    get_positions,
    get_shifts,
)
from apps.shiftzy.api.v1.helpers.weeks import list_weeks
from shared.utils import run_parallel

router = APIRouter()

_TABLE_LOADERS = {
    "shifts": get_shifts,
    "employees": get_employees,
    "weeks": list_weeks,
    "positions": get_positions,
}


def _normalize_tables(tables: list[str] | None) -> list[str]:
    if not tables:
        return []
    normalized: list[str] = []
    for item in tables:
        for part in item.split(","):
            name = part.strip().lower()
            if name and name not in normalized:
                normalized.append(name)
    return normalized


@router.get("/bootstrap")
def get_bootstrap(
    tables: list[str] | None = Query(None),
    include_all: bool = Query(False, alias="all"),
):
    """
    Fetch multiple Shiftzy bootstrap datasets in one request.

    Example request:
        GET /api/shiftzy/v1/bootstrap

    Example request (selected tables + include inactive):
        GET /api/shiftzy/v1/bootstrap?tables=shifts,employees&all=true

    Example request (single table):
        GET /api/shiftzy/v1/bootstrap?tables=positions&all=true

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 6},
          "data": {
            "shifts": [{"id": 3, "name": "Morning", "start_time": "08:00", "end_time": "12:00", "active": 1, "duration": "04:00"}],
            "employees": [{"id": "948f09c9-f6b9-11f0-b7f6-5a4783e25118", "name": "Taylor Reed", "schedule_section": "Front", "note": null, "ref_positionCode": "FR-CASH", "active": 1}]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Unknown `tables` values return 400
        - Query param `all` maps to `include_all` for shifts/employees/positions
        - `all` does not apply to `weeks`
    """
    selected = _normalize_tables(tables)
    if not selected:
        selected = list(_TABLE_LOADERS.keys())

    unknown = [name for name in selected if name not in _TABLE_LOADERS]
    if unknown:
        unknown_list = ", ".join(sorted(unknown))
        raise HTTPException(status_code=400, detail=f"Unknown tables: {unknown_list}")

    tasks = []
    for name in selected:
        if name == "weeks":
            tasks.append((_TABLE_LOADERS[name], ()))
        else:
            tasks.append((_TABLE_LOADERS[name], (None, include_all)))
    results = run_parallel(tasks=tasks, api_name="shiftzy.bootstrap")
    data = {name: result for name, result in zip(selected, results)}

    return data

