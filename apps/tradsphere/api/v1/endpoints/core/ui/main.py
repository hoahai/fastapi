from __future__ import annotations

from collections import OrderedDict
from datetime import date

from fastapi import APIRouter, HTTPException, Query

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_tradsphere_account_codes_exist,
    require_account_code,
)
from apps.tradsphere.api.v1.helpers.accounts import list_accounts
from apps.tradsphere.api.v1.helpers.dbQueries import get_stations
from apps.tradsphere.api.v1.helpers.estNums import (
    get_est_num_broadcast_weeks,
    list_est_nums_data,
)
from apps.tradsphere.api.v1.helpers.schedules import list_schedules_data

router = APIRouter(prefix="/ui/main")
_MONTH_ABBR = (
    "",
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
)
_STATION_MEDIA_TYPE_ORDER = {"TV": 0, "RA": 1, "CA": 2}


def _coerce_iso_date(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _format_period_segment(months: list[int], year: int) -> str:
    valid_months = [month for month in months if 1 <= month <= 12]
    if not valid_months:
        return f"Q?'{year % 100:02d}"

    unique_months = sorted(set(valid_months))
    month_set = set(unique_months)
    quarter_numbers = sorted({((month - 1) // 3) + 1 for month in unique_months})
    full_quarters = all(
        {quarter * 3 - 2, quarter * 3 - 1, quarter * 3}.issubset(month_set)
        for quarter in quarter_numbers
    )
    if full_quarters and len(unique_months) == (len(quarter_numbers) * 3):
        quarter_label = "Q" + ",".join(str(quarter) for quarter in quarter_numbers)
        return f"{quarter_label}'{year % 100:02d}"

    months_label = ",".join(_MONTH_ABBR[month] for month in unique_months)
    return f"{months_label}'{year % 100:02d}"


def _build_est_num_name_and_sort_date(
    row: dict,
    *,
    billing_type: str | None,
) -> tuple[str, date]:
    media_type = str(row.get("mediaType") or "").strip().upper()
    flight_start = _coerce_iso_date(row.get("flightStart"))
    flight_end = _coerce_iso_date(row.get("flightEnd"))
    if flight_start is None or flight_end is None or flight_start > flight_end:
        return media_type, date.min

    weeks = get_est_num_broadcast_weeks(
        flight_start=flight_start,
        flight_end=flight_end,
        billing_type=billing_type,
    )
    latest_week_end = date.min
    months_by_year: OrderedDict[int, list[int]] = OrderedDict()
    for week in weeks:
        week_end = week["weekEnd"]
        if week_end > latest_week_end:
            latest_week_end = week_end
        year = int(week_end.year)
        month = int(week_end.month)
        months = months_by_year.setdefault(year, [])
        if month not in months:
            months.append(month)

    period_segments = [
        _format_period_segment(months, year)
        for year, months in months_by_year.items()
    ]
    period_label = ", ".join(segment for segment in period_segments if segment).strip()
    name = f"{period_label} {media_type}".strip()
    return (name or media_type), latest_week_end


def _to_int(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _station_sort_key(item: dict) -> tuple[int, int, str]:
    latest_end_date = item.get("_latestEndDate")
    latest_ordinal = date.min.toordinal()
    if isinstance(latest_end_date, date):
        latest_ordinal = latest_end_date.toordinal()
    media_type = str(item.get("_mediaType") or "").strip().upper()
    media_rank = _STATION_MEDIA_TYPE_ORDER.get(media_type, 3)
    code = str(item.get("code") or "").strip().upper()
    return (-latest_ordinal, media_rank, code)


@router.get("/selections")
def get_ui_main_selections_route():
    """
    Return TradSphere account selections for UI dropdowns.

    Example request:
        GET /api/tradsphere/v1/ui/main/selections

    Example response:
        {
          "meta": {"timestamp": "2026-04-29T09:00:00+07:00", "duration_ms": 2},
          "data": [
            {"code": "TAAA", "name": "Alpha Motors"},
            {"code": "TBBB", "name": "Beta Auto Group"}
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Returns TradSphere accounts only
        - Uses active master accounts only
    """
    try:
        rows = list_accounts(active=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return [
        {
            "code": str(row.get("accountCode") or "").strip(),
            "name": str(row.get("name") or "").strip(),
        }
        for row in rows
        if str(row.get("accountCode") or "").strip()
    ]


@router.get("/load")
def get_ui_main_load_route(
    account_code: str = Query(..., alias="accountCode"),
):
    """
    Return UI load payload for a TradSphere account, including account, estnums, and stations.

    Example request:
        GET /api/tradsphere/v1/ui/main/load?accountCode=TAAA

    Example response:
        {
          "meta": {"timestamp": "2026-04-29T09:00:00+07:00", "duration_ms": 5},
          "data": {
            "account": {
              "code": "TAAA",
              "name": "Alpha Motors",
              "logoUrl": "https://cdn.example.com/logos/taaa.png",
              "billingType": "Calendar",
              "market": "Los Angeles",
              "note": "Primary west-coast account"
            },
            "esnums": [
              {"estnum": 26001, "name": "Q3'26 TV", "hasSchedule": true, "note": "Prime time package"}
            ],
            "stations": [
              {"code": "KABC", "name": "ABC Los Angeles"}
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - accountCode is required and must exist in TradSphere accounts
        - estnums are sorted by latest period descending
        - for Calendar billing accounts, trailing overlap week at flight end is excluded when building estnum period labels
        - stations are sorted by most recent related schedule descending, then mediaType order (TV, RA, CA, else)
    """
    try:
        normalized_account_code = require_account_code(account_code)
        ensure_tradsphere_account_codes_exist([normalized_account_code])

        account_rows = list_accounts(
            account_codes=[normalized_account_code],
            active=False,
        )
        account_row = next(
            (
                row
                for row in account_rows
                if str(row.get("accountCode") or "").strip().upper()
                == normalized_account_code
            ),
            None,
        )
        if not isinstance(account_row, dict):
            raise ValueError(
                f"Unknown TradSphere accountCode values: {normalized_account_code}"
            )

        account_billing_type = str(account_row.get("billingType") or "").strip()

        est_num_rows = list_est_nums_data(account_codes=[normalized_account_code])
        estnum_items_with_sort: list[tuple[dict, date]] = []
        est_nums: list[int] = []
        for row in est_num_rows:
            est_num = _to_int(row.get("estNum"))
            if est_num is None:
                continue
            name, latest_sort_date = _build_est_num_name_and_sort_date(
                row,
                billing_type=account_billing_type,
            )
            estnum_items_with_sort.append(
                (
                    {
                        "estnum": est_num,
                        "name": name,
                        "hasSchedule": bool(row.get("hasSchedule")),
                        "note": str(row.get("note") or "").strip() or None,
                    },
                    latest_sort_date,
                )
            )
            est_nums.append(est_num)

        estnum_items_with_sort.sort(
            key=lambda item: (
                -item[1].toordinal(),
                -int(item[0]["estnum"]),
            )
        )
        estnums = [item[0] for item in estnum_items_with_sort]

        schedules = list_schedules_data(est_nums=est_nums) if est_nums else []
        station_latest_schedule_end: dict[str, date] = {}
        for row in schedules:
            station_code = str(row.get("stationCode") or "").strip().upper()
            if not station_code:
                continue
            end_date = _coerce_iso_date(row.get("endDate"))
            if end_date is None:
                end_date = _coerce_iso_date(row.get("startDate"))
            if end_date is None:
                continue
            current = station_latest_schedule_end.get(station_code)
            if current is None or end_date > current:
                station_latest_schedule_end[station_code] = end_date

        stations_rows = get_stations(
            codes=[],
            account_codes=[normalized_account_code],
            est_nums=[],
            station_name=None,
            delivery_method_detail=False,
        )
        station_items_index: dict[str, dict] = {}
        for row in stations_rows:
            station_code = str(row.get("code") or "").strip().upper()
            if not station_code:
                continue
            station_items_index[station_code] = {
                "code": station_code,
                "name": str(row.get("name") or "").strip(),
                "_latestEndDate": station_latest_schedule_end.get(
                    station_code,
                    date.min,
                ),
                "_mediaType": str(row.get("mediaType") or "").strip().upper(),
            }

        for station_code, latest_end_date in station_latest_schedule_end.items():
            if station_code in station_items_index:
                continue
            station_items_index[station_code] = {
                "code": station_code,
                "name": "",
                "_latestEndDate": latest_end_date,
                "_mediaType": "",
            }

        station_items = sorted(station_items_index.values(), key=_station_sort_key)
        stations = [
            {
                "code": str(item.get("code") or "").strip(),
                "name": str(item.get("name") or "").strip(),
            }
            for item in station_items
            if str(item.get("code") or "").strip()
        ]

        return {
            "account": {
                "code": str(account_row.get("accountCode") or "").strip().upper(),
                "name": str(account_row.get("name") or "").strip(),
                "logoUrl": account_row.get("logoUrl"),
                "billingType": account_row.get("billingType"),
                "market": account_row.get("market"),
                "note": account_row.get("note"),
            },
            "esnums": estnums,
            "stations": stations,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
