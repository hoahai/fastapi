from __future__ import annotations

from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from apps.fundsphere.api.v1.helpers.dbQueries import (
    BudgetSegmentValidationError,
    BudgetSegmentsBudgetNotFoundError,
    get_budget_segments,
    replace_budget_segments,
)


# ============================================================
# ROUTER
# ============================================================

router = APIRouter(prefix="/budgetSegments")


# ============================================================
# REQUEST MODELS
# ============================================================


class BudgetSegmentInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    objective: Literal["New", "Used", "Service"]
    objectivePercent: Decimal = Field(
        ge=Decimal("0"),
        le=Decimal("100"),
        max_digits=5,
        decimal_places=2,
    )
    language: Literal["English", "Spanish"]
    segmentPercent: Decimal = Field(
        ge=Decimal("0"),
        le=Decimal("100"),
        max_digits=5,
        decimal_places=2,
    )


class BudgetSegmentsReplaceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    segments: list[BudgetSegmentInput] = Field(default_factory=list)


# ============================================================
# ERRORS
# ============================================================


def _raise_budget_segment_error(exc: Exception) -> None:
    if isinstance(exc, BudgetSegmentValidationError):
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Invalid budget segments",
                "errors": exc.errors,
            },
        ) from exc
    if isinstance(exc, BudgetSegmentsBudgetNotFoundError):
        raise HTTPException(
            status_code=404,
            detail={"message": str(exc)},
        ) from exc
    raise exc


# ============================================================
# ROUTES
# ============================================================


@router.get("", summary="List segments for a FundSphere budget")
def list_budget_segments_route(
    budget_id: str = Query(..., alias="budgetId"),
):
    """
    List the configured objective/language segments for one budget.

    Example request:
        GET /api/fundsphere/v1/masterBudgetControl/budgetSegments?budgetId=9d8c7b6a-5e4f-4321-9abc-1234567890ab
        Header: X-Tenant-Id: taaa

    Example response:
        {
          "meta": {"requestId": "...", "durationMs": 12, "timestamp": "..."},
          "data": {
            "budgetId": "9d8c7b6a-5e4f-4321-9abc-1234567890ab",
            "segments": [
              {
                "id": "11111111-2222-3333-4444-555555555555",
                "budgetId": "9d8c7b6a-5e4f-4321-9abc-1234567890ab",
                "objective": "New",
                "objectivePercent": 60.00,
                "language": "English",
                "segmentPercent": 40.00
              },
              {
                "id": "22222222-3333-4444-5555-666666666666",
                "budgetId": "9d8c7b6a-5e4f-4321-9abc-1234567890ab",
                "objective": "New",
                "objectivePercent": 60.00,
                "language": "Spanish",
                "segmentPercent": 60.00
              },
              {
                "id": "33333333-4444-5555-6666-777777777777",
                "budgetId": "9d8c7b6a-5e4f-4321-9abc-1234567890ab",
                "objective": "Used",
                "objectivePercent": 40.00,
                "language": "English",
                "segmentPercent": 60.00
              },
              {
                "id": "44444444-5555-6666-7777-888888888888",
                "budgetId": "9d8c7b6a-5e4f-4321-9abc-1234567890ab",
                "objective": "Used",
                "objectivePercent": 40.00,
                "language": "Spanish",
                "segmentPercent": 40.00
              }
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id and a valid API key
        - Tenant config must include DB_TABLES.budgetSegmentObjectives and
          DB_TABLES.budgetSegmentLanguages
        - budgetId must identify a budget in DB_TABLES.budgets
        - Segment percentages are stored with at most two decimal places
        - Objective percentages must total 100% across objectives present
        - For each objective with segment rows, English and Spanish
          percentages together must total 100%
    """
    try:
        return {
            "budgetId": str(budget_id).strip(),
            "segments": get_budget_segments(budget_id=budget_id),
        }
    except Exception as exc:
        _raise_budget_segment_error(exc)
        raise


@router.put("/{budget_id}", summary="Replace all segments for a FundSphere budget")
def replace_budget_segments_route(
    budget_id: str,
    body: BudgetSegmentsReplaceRequest = Body(...),
):
    """
    Atomically replace the complete segment collection for one budget.

    Example request:
        PUT /api/fundsphere/v1/masterBudgetControl/budgetSegments/9d8c7b6a-5e4f-4321-9abc-1234567890ab
        Header: X-Tenant-Id: taaa
        Body:
          {
            "segments": [
              {"objective": "New", "objectivePercent": 60.00, "language": "English", "segmentPercent": 70.00},
              {"objective": "New", "objectivePercent": 60.00, "language": "Spanish", "segmentPercent": 30.00},
              {"objective": "Used", "objectivePercent": 40.00, "language": "English", "segmentPercent": 50.00},
              {"objective": "Used", "objectivePercent": 40.00, "language": "Spanish", "segmentPercent": 50.00}
            ]
          }

    Example request (remove all segments):
        PUT /api/fundsphere/v1/masterBudgetControl/budgetSegments/9d8c7b6a-5e4f-4321-9abc-1234567890ab
        Body: {"segments": []}

    Example response:
        {
          "meta": {"requestId": "...", "durationMs": 24, "timestamp": "..."},
          "data": {
            "budgetId": "9d8c7b6a-5e4f-4321-9abc-1234567890ab",
            "deletedCount": 1,
            "writtenRowCount": 4,
            "segmentIds": ["11111111-2222-3333-4444-555555555555", "..."],
            "segments": [
              {
                "id": "11111111-2222-3333-4444-555555555555",
                "budgetId": "9d8c7b6a-5e4f-4321-9abc-1234567890ab",
                "objective": "New",
                "objectivePercent": 60.00,
                "language": "English",
                "segmentPercent": 40.00
              }
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id and a valid API key
        - Tenant config must include DB_TABLES.budgetSegmentObjectives and
          DB_TABLES.budgetSegmentLanguages
        - objective must be New, Used, or Service
        - objectivePercent must be between 0 and 100 with at most two decimals
        - language must be English or Spanish
        - segmentPercent must be between 0 and 100 with at most two decimals
        - Each objective/language combination may appear only once
        - Objective percentages must total 100% across objectives present
        - Each objective's English and Spanish percentages must total 100%
        - The replacement is transactional and respects the budget foreign key
    """
    try:
        segments = [segment.model_dump() for segment in body.segments]
        return replace_budget_segments(
            budget_id=budget_id,
            segments=segments,
        )
    except Exception as exc:
        _raise_budget_segment_error(exc)
        raise


@router.delete("/{budget_id}", summary="Delete all segments for a FundSphere budget")
def delete_budget_segments_route(budget_id: str):
    """
    Delete all segment rows belonging to one budget without deleting the budget.

    Example request:
        DELETE /api/fundsphere/v1/masterBudgetControl/budgetSegments/9d8c7b6a-5e4f-4321-9abc-1234567890ab
        Header: X-Tenant-Id: taaa

    Example response:
        {
          "meta": {"requestId": "...", "durationMs": 18, "timestamp": "..."},
          "data": {
            "budgetId": "9d8c7b6a-5e4f-4321-9abc-1234567890ab",
            "deletedCount": 2
          }
        }

    Requirements:
        - Requires X-Tenant-Id and a valid API key
        - Tenant config must include DB_TABLES.budgetSegmentObjectives and
          DB_TABLES.budgetSegmentLanguages
        - budgetId must identify an existing budget
        - Deleting a budget itself also removes segments through ON DELETE CASCADE
    """
    try:
        result = replace_budget_segments(budget_id=budget_id, segments=[])
        return {
            "budgetId": result["budgetId"],
            "deletedCount": result["deletedCount"],
        }
    except Exception as exc:
        _raise_budget_segment_error(exc)
        raise
