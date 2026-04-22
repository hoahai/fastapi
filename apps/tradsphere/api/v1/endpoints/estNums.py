from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.estNums import (
    create_est_nums_data,
    list_est_nums_data,
    modify_est_nums_data,
)
from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values, parse_int_list

router = APIRouter(prefix="/estNums")


@router.get("")
def get_est_nums_route(
    est_nums: list[str] | None = Query(None, alias="estNums"),
    est_num: list[str] | None = Query(None, alias="estNum"),
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    account_code: list[str] | None = Query(None, alias="accountCode"),
    media_types: list[str] | None = Query(None, alias="mediaTypes"),
    media_type: list[str] | None = Query(None, alias="mediaType"),
):
    """
    Return TradSphere estimate-number rows with optional est/account/media filters.

    Example request:
        GET /api/tradsphere/v1/estNums

    Example request (estNum + accountCode):
        GET /api/tradsphere/v1/estNums?estNum=101&accountCode=TAAA

    Example request (comma-separated filters):
        GET /api/tradsphere/v1/estNums?estNums=101,102&accountCodes=TAAA,TBBB&mediaTypes=TV,OTT

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
              "note": "Prime time package"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - estNums/estNum values must be unsigned integers
        - estNums/estNum, accountCodes/accountCode, mediaTypes/mediaType accept comma-separated values
        - Blank filter lists are treated as no filter (returns all for that filter)
    """
    normalized_est_nums = parse_int_list(est_nums, est_num)
    normalized_account_codes = parse_csv_values(account_codes, account_code, uppercase=True)
    normalized_media_types = parse_csv_values(media_types, media_type, uppercase=True)
    try:
        return list_est_nums_data(
            est_nums=normalized_est_nums,
            account_codes=normalized_account_codes,
            media_types=normalized_media_types,
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

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts object or array of objects
        - estNum, accountCode, flightStart, flightEnd, mediaType, buyer are required for each item
        - estNum must be unsigned integer
        - flightStart/flightEnd must be ISO date YYYY-MM-DD
        - flightStart must be on or before flightEnd
        - buyer max length is 36
        - note max length is 2048
        - accountCode must exist
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

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts object or array of objects
        - estNum is required per item
        - accountCode/mediaType updates are validated when provided
        - flightStart/flightEnd must be ISO date YYYY-MM-DD when provided
        - when both flightStart and flightEnd are provided, flightStart must be on or before flightEnd
        - buyer max length is 36
        - note max length is 2048
    """
    try:
        return modify_est_nums_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
