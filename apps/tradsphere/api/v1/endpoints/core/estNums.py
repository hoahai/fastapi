from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.estNums import (
    create_est_nums_data,
    list_est_nums_data,
    modify_est_nums_data,
    search_est_nums_data,
)
from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values, parse_int_list

router = APIRouter(prefix="/estNums")


@router.get("/search")
def search_est_nums_route(
    q: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int | None = Query(None, ge=0),
    cursor: str | None = Query(None),
    created_from: str | None = Query(None, alias="createdFrom"),
    created_to: str | None = Query(None, alias="createdTo"),
    timezone: str | None = Query(None, alias="timezone"),
):
    """
    Search TradSphere estimate numbers without loading the full dataset.

    Example request:
        GET /api/tradsphere/v1/estNums/search?q=2600&limit=50

    Example request (created date range):
        GET /api/tradsphere/v1/estNums/search?createdFrom=2026-05-07&createdTo=2026-05-07&limit=25

    Example request (special keyword + timezone):
        GET /api/tradsphere/v1/estNums/search?q=today&timezone=America/Chicago&limit=50

    Example response:
        {
          "meta": {"timestamp": "2026-05-07T10:00:00+07:00", "duration_ms": 6},
          "data": {
            "items": [
              {
                "estNum": 26001,
                "accountCode": "TAAA",
                "accountName": "Alpha Motors",
                "flightStart": "2026-04-01",
                "flightEnd": "2026-04-30",
                "mediaType": "TV",
                "buyer": "Elyse",
                "note": "Prime time package",
                "hasSchedule": true,
                "month": null,
                "quarter": 2,
                "year": 2026,
                "periodLabel": "APR,MAY'26",
                "broadcastMonths": [4, 5],
                "broadcastYears": [2026]
              }
            ],
            "total": 123,
            "limit": 50,
            "nextOffset": 50,
            "nextCursor": "50"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - At least one search filter is required: q, createdFrom, or createdTo
        - q search matches estNum/accountCode/accountName/buyer/mediaType/note/flight dates
        - q=today (case-insensitive) applies created-date filter for current date in timezone (default America/Chicago)
        - createdFrom/createdTo must be ISO date YYYY-MM-DD when provided
        - createdFrom must be on or before createdTo when both are provided
        - limit must be between 1 and 200
        - cursor/offset must be non-negative integers; cursor takes precedence when both are provided
        - response includes hasSchedule plus derived period metadata:
          month/quarter/year (when uniquely resolvable), periodLabel, and broadcastMonths/broadcastYears
    """
    try:
        return search_est_nums_data(
            query=q,
            limit=limit,
            offset=offset,
            cursor=cursor,
            created_from=created_from,
            created_to=created_to,
            timezone=timezone,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("")
def get_est_nums_route(
    est_nums: list[str] | None = Query(None, alias="estNums"),
    est_num: list[str] | None = Query(None, alias="estNum"),
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    account_code: list[str] | None = Query(None, alias="accountCode"),
    year: int | None = Query(None),
    month: int | None = Query(None),
    quarter: int | None = Query(None, alias="quarter"),
):
    """
    Return TradSphere estimate-number rows with filters and deterministic sorting.

    Example request:
        GET /api/tradsphere/v1/estNums?year=2026

    Example request (estNum + accountCode):
        GET /api/tradsphere/v1/estNums?estNum=101&accountCode=TAAA

    Example request (comma-separated filters):
        GET /api/tradsphere/v1/estNums?estNums=101,102&accountCodes=TAAA,TBBB

    Example request (period filters):
        GET /api/tradsphere/v1/estNums?year=2026&month=4&quarter=2

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 3},
          "data": [
            {
              "estNum": 101,
              "accountCode": "TAAA",
              "flightStart": "2026-04-01",
              "flightEnd": "2026-04-30",
              "mediaType": "TV",
              "buyer": "Elyse",
              "note": "Prime time package",
              "hasSchedule": true,
              "month": null,
              "quarter": 2,
              "year": 2026,
              "periodLabel": "APR,MAY'26",
              "broadcastMonths": [4, 5],
              "broadcastYears": [2026]
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - At least one filter is required: accountCodes/accountCode, estNums/estNum, year, month+year, or quarter+year
        - estNums/estNum values must be unsigned integers
        - estNums/estNum and accountCodes/accountCode accept comma-separated values
        - mediaTypes/mediaType filter is not supported on this route
        - month requires year
        - quarter requires year
        - response includes hasSchedule (true when at least one schedule exists for estNum)
        - response includes derived period metadata:
          month/quarter/year (when uniquely resolvable), periodLabel, and broadcastMonths/broadcastYears
        - for Calendar billing accounts, trailing overlap week (when flightEnd is a cross-month week end) is excluded from broadcastMonths/broadcastYears
        - response is sorted by: primary broadcast year DESC (uses lowest year when row spans multiple years), accountCode ASC, flightStart month ASC, estNum ASC
        - year/month/quarter filters use broadcast calendar semantics (broadcast week is Monday-Sunday, month/year from week-ending Sunday)
        - year must be 1901-2155 when provided
        - month must be 1-12 when provided
        - quarter must be 1-4 when provided
        - when month and quarter are both provided, month must belong to quarter
        - when year is provided with month/quarter, filtering matches broadcast month/quarter within that broadcast year
        - Blank list filters are treated as no filter for that specific list
    """
    normalized_est_nums = parse_int_list(est_nums, est_num)
    normalized_account_codes = parse_csv_values(
        account_codes,
        account_code,
        uppercase=True,
    )
    try:
        return list_est_nums_data(
            est_nums=normalized_est_nums,
            account_codes=normalized_account_codes,
            year=year,
            month=month,
            quarter=quarter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_est_nums_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or many TradSphere estimate-number rows.

    Example request:
        POST /api/tradsphere/v1/estNums
        [
          {
            "estNum": 101,
            "accountCode": "TAAA",
            "flightStart": "2026-04-01",
            "flightEnd": "2026-04-30",
            "mediaType": "TV",
            "buyer": "Elyse",
            "note": "Prime time package"
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "data": {"inserted": 1}
        }

    Example error response (duplicate estNum):
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "error": {
            "message": "Bad Request",
            "detail": "estNum values already exist in TradSphere estNums: 101"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts object or array of objects
        - estNum, accountCode, flightStart, flightEnd, mediaType, buyer are required for each item
        - estNum must be unsigned integer
        - Duplicate estNum values in payload are rejected with HTTP 400
        - estNum must not already exist in TradSphere_EstNums (no upsert on POST)
        - flightStart/flightEnd must be ISO date YYYY-MM-DD
        - flightStart must be on or before flightEnd
        - buyer max length is 36
        - note max length is 2048
        - accountCode must exist in TradSphere_Accounts
        - mediaType must match tenant enum tradsphere.ENUMS.mediaType
    """
    try:
        return create_est_nums_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_est_nums_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update TradSphere estimate-number rows by estNum.

    Example request:
        PUT /api/tradsphere/v1/estNums
        [
          {
            "estNum": 101,
            "accountCode": "TBBB",
            "flightStart": "2026-05-01",
            "flightEnd": "2026-05-31",
            "mediaType": "OTT",
            "buyer": "9d81fd9b-3d49-42d8-9f6a-362f4c47ec36",
            "note": "Updated placement"
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "data": {"updated": 1}
        }

    Example error response (accountCode not in TradSphere_Accounts):
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "error": {
            "message": "Bad Request",
            "detail": "Unknown TradSphere accountCode values: TCCC"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts object or array of objects
        - estNum is required per item
        - accountCode updates must exist in TradSphere_Accounts when provided; otherwise returns HTTP 400
        - mediaType updates are validated when provided
        - flightStart/flightEnd must be ISO date YYYY-MM-DD when provided
        - when both flightStart and flightEnd are provided, flightStart must be on or before flightEnd
        - buyer max length is 36
        - note max length is 2048
    """
    try:
        return modify_est_nums_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
