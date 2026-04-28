from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.accounts import (
    create_accounts,
    list_accounts,
    modify_accounts,
)
from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values

router = APIRouter(prefix="/accounts")


@router.get("")
def get_accounts_route(
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    account_code: list[str] | None = Query(None, alias="accountCode"),
    active: bool = Query(True),
):
    """
    Return TradSphere account-code mappings joined with master account metadata.

    Example request:
        GET /api/tradsphere/v1/accounts

    Example request (single alias filter):
        GET /api/tradsphere/v1/accounts?accountCode=TAAA

    Example request (multiple filters):
        GET /api/tradsphere/v1/accounts?accountCodes=TAAA,TBBB&active=true

    Example request (include inactive TradSphere accounts):
        GET /api/tradsphere/v1/accounts?active=false

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 3},
          "data": [
            {
              "accountCode": "TAAA",
              "billingType": "Calendar",
              "market": "Los Angeles",
              "note": "Primary west-coast account",
              "name": "Alpha Motors",
              "logoUrl": "https://cdn.example.com/logos/taaa.png",
              "active": 1
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - accountCodes/accountCode accept comma-separated values
        - Blank accountCodes/accountCode values return all rows
        - active defaults to true
        - active=true filters by master Accounts.active = 1
        - active=false disables the active filter and returns all TradSphere accounts
    """
    normalized_codes = parse_csv_values(account_codes, account_code, uppercase=True)
    try:
        return list_accounts(account_codes=normalized_codes, active=active)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_accounts_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or many TradSphere account-code mapping rows.

    Example request:
        POST /api/tradsphere/v1/accounts
        [
          {
            "accountCode": "TAAA",
            "billingType": "Calendar",
            "market": "Los Angeles",
            "note": "Primary west-coast account"
          },
          {
            "accountCode": "TBBB",
            "billingType": "Broadcast",
            "market": "New York"
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "data": {"inserted": 2}
        }

    Example error response (duplicate accountCode):
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "error": {
            "message": "Bad Request",
            "detail": "accountCode values already exist in TradSphere accounts: TAAA"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts object or array of objects
        - accountCode is required for each item
        - accountCode must exist in master Accounts
        - accountCode must not already exist in TradSphere_Accounts (no upsert on POST)
        - Duplicate accountCode values in payload are rejected with HTTP 400
        - billingType accepts Broadcast or Calendar (default Calendar)
        - market max length is 255
        - note max length is 2048
    """
    try:
        return create_accounts(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_accounts_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update TradSphere account-code mapping rows by accountCode.

    Example request:
        PUT /api/tradsphere/v1/accounts
        [
          {
            "accountCode": "TAAA",
            "billingType": "Broadcast",
            "market": "San Diego",
            "note": "Moved to regional team"
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
        - accountCode is required per item
        - accountCode must already exist in TradSphere_Accounts
        - At least one updatable field is required: billingType, market, note
        - billingType accepts Broadcast or Calendar
        - market max length is 255
        - note max length is 2048
    """
    try:
        return modify_accounts(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
