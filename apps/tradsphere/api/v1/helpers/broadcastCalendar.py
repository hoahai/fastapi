from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Any


def _coerce_to_date(given_date: date | datetime | str) -> date:
    if isinstance(given_date, datetime):
        return given_date.date()
    if isinstance(given_date, date):
        return given_date
    if isinstance(given_date, str):
        text = given_date.strip()
        if not text:
            raise ValueError("givenDate cannot be empty")
        try:
            return date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError("givenDate must be ISO date format YYYY-MM-DD") from exc
    raise TypeError("givenDate must be date, datetime, or ISO date string")


def _monday_of_week(target_date: date) -> date:
    return target_date - timedelta(days=target_date.weekday())


def get_broadcast_calendar_info(given_date: date | datetime | str) -> dict[str, Any]:
    target_date = _coerce_to_date(given_date)

    first_day_of_week = _monday_of_week(target_date)
    last_date_of_week = first_day_of_week + timedelta(days=6)

    broadcast_month = last_date_of_week.month
    broadcast_year = last_date_of_week.year

    gregorian_month_day_1 = date(broadcast_year, broadcast_month, 1)
    begin_broadcast_month = _monday_of_week(gregorian_month_day_1)

    last_day_of_month = date(
        broadcast_year,
        broadcast_month,
        monthrange(broadcast_year, broadcast_month)[1],
    )
    number_of_broadcast_week = ((last_day_of_month - begin_broadcast_month).days + 1) // 7
    end_broadcast_month = begin_broadcast_month + timedelta(
        days=number_of_broadcast_week * 7 - 1
    )

    week_num_of_month = ((last_date_of_week - begin_broadcast_month).days + 1) // 7

    broadcast_year_day_1 = date(broadcast_year, 1, 1)
    begin_broadcast_year = _monday_of_week(broadcast_year_day_1)
    week_num_of_year = ((last_date_of_week - begin_broadcast_year).days + 1) // 7

    return {
        "broadcastMonth": broadcast_month,
        "broadcastYear": broadcast_year,
        "beginBroadcastMonth": begin_broadcast_month,
        "endBroadcastMonth": end_broadcast_month,
        "numberOfBroadcastWeek": number_of_broadcast_week,
        "firstDayOfWeek": first_day_of_week,
        "lastDateOfWeek": last_date_of_week,
        "weekNumofMonth": week_num_of_month,
        "weekNumofYear": week_num_of_year,
    }


def get_broadcast_calendar_value(
    given_date: date | datetime | str,
    result_type: str,
) -> Any:
    info = get_broadcast_calendar_info(given_date)
    normalized = str(result_type or "").strip().lower()
    result_type_map = {
        "month": "broadcastMonth",
        "year": "broadcastYear",
        "start_date": "beginBroadcastMonth",
        "end_date": "endBroadcastMonth",
        "num_of_week": "numberOfBroadcastWeek",
        "firstdate_of_week": "firstDayOfWeek",
        "lastdate_of_week": "lastDateOfWeek",
        "week_num_of_month": "weekNumofMonth",
        "week_num_of_year": "weekNumofYear",
    }

    mapped_key = result_type_map.get(normalized)
    if not mapped_key:
        supported = ", ".join(result_type_map.keys())
        raise ValueError(
            f"Unsupported resultType '{result_type}'. Use one of: {supported}"
        )
    return info[mapped_key]
