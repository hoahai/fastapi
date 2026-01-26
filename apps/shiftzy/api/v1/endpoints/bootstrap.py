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
