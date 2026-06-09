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


# Broadcast buckets anchor on the week-ending Sunday.
def _calendar_month_start(month: int, year: int) -> date:
    month_start = date(year, month, 1)
    if month_start.weekday() == 0:
        return month_start
    return month_start + timedelta(days=7 - month_start.weekday())


def _calendar_month_end(month: int, year: int) -> date:
    month_end = date(year, month, monthrange(year, month)[1])
    return _monday_of_week(month_end) + timedelta(days=6)


def get_broadcast_month_range(month: int, year: int) -> tuple[date, date]:
    month_start = date(year, month, 1)
    month_end = date(year, month, monthrange(year, month)[1])
    begin_broadcast_month = _monday_of_week(month_start)

    number_of_broadcast_week = ((month_end - begin_broadcast_month).days + 1) // 7
    if number_of_broadcast_week < 1:
        raise ValueError("Unable to compute broadcast month range")

    end_broadcast_month = begin_broadcast_month + timedelta(
        days=number_of_broadcast_week * 7 - 1
    )
    return begin_broadcast_month, end_broadcast_month


def get_calendar_month_range(month: int, year: int) -> tuple[date, date]:
    begin_calendar_month = _calendar_month_start(month, year)
    end_calendar_month = _calendar_month_end(month, year)
    return begin_calendar_month, end_calendar_month


def get_broadcast_month_bucket(given_date: date | datetime | str) -> tuple[int, int]:
    target_date = _coerce_to_date(given_date)
    first_day_of_week = _monday_of_week(target_date)
    last_date_of_week = first_day_of_week + timedelta(days=6)
    return last_date_of_week.year, last_date_of_week.month


# Calendar buckets anchor on the Monday that starts the week.
def get_calendar_month_bucket(given_date: date | datetime | str) -> tuple[int, int]:
    target_date = _coerce_to_date(given_date)
    first_day_of_week = _monday_of_week(target_date)
    return first_day_of_week.year, first_day_of_week.month


def get_broadcast_weeks_in_range(
    start_date: date | datetime | str,
    end_date: date | datetime | str,
) -> list[dict[str, date]]:
    start_value = _coerce_to_date(start_date)
    end_value = _coerce_to_date(end_date)
    if start_value > end_value:
        raise ValueError("startDate must be on or before endDate")

    start_week = _monday_of_week(start_value)
    end_week = _monday_of_week(end_value)

    weeks: list[dict[str, date]] = []
    cursor = start_week
    while cursor <= end_week:
        weeks.append(
            {
                "weekStart": cursor,
                "weekEnd": cursor + timedelta(days=6),
            }
        )
        cursor += timedelta(days=7)
    return weeks


def get_broadcast_calendar_info(given_date: date | datetime | str) -> dict[str, Any]:
    target_date = _coerce_to_date(given_date)

    first_day_of_week = _monday_of_week(target_date)
    last_date_of_week = first_day_of_week + timedelta(days=6)

    broadcast_year, broadcast_month = get_broadcast_month_bucket(target_date)
    calendar_year, calendar_month = get_calendar_month_bucket(target_date)

    begin_broadcast_month, end_broadcast_month = get_broadcast_month_range(
        broadcast_month,
        broadcast_year,
    )
    begin_calendar_month, end_calendar_month = get_calendar_month_range(
        calendar_month,
        calendar_year,
    )

    week_num_of_month = ((last_date_of_week - begin_broadcast_month).days + 1) // 7
    calendar_week_num_of_month = ((first_day_of_week - begin_calendar_month).days // 7) + 1

    broadcast_year_day_1 = date(broadcast_year, 1, 1)
    begin_broadcast_year = _monday_of_week(broadcast_year_day_1)
    week_num_of_year = ((last_date_of_week - begin_broadcast_year).days + 1) // 7

    calendar_year_day_1 = date(calendar_year, 1, 1)
    begin_calendar_year = _monday_of_week(calendar_year_day_1)
    calendar_week_num_of_year = ((first_day_of_week - begin_calendar_year).days // 7) + 1

    number_of_broadcast_week = ((end_broadcast_month - begin_broadcast_month).days + 1) // 7
    number_of_calendar_week = ((end_calendar_month - begin_calendar_month).days + 1) // 7

    return {
        "broadcastMonth": broadcast_month,
        "broadcastYear": broadcast_year,
        "beginBroadcastMonth": begin_broadcast_month,
        "endBroadcastMonth": end_broadcast_month,
        "numberOfBroadcastWeek": number_of_broadcast_week,
        "calendarMonth": calendar_month,
        "calendarYear": calendar_year,
        "beginCalendarMonth": begin_calendar_month,
        "endCalendarMonth": end_calendar_month,
        "numberOfCalendarWeek": number_of_calendar_week,
        "firstDayOfWeek": first_day_of_week,
        "lastDateOfWeek": last_date_of_week,
        "weekNumofMonth": week_num_of_month,
        "weekNumofYear": week_num_of_year,
        "calendarWeekNumofMonth": calendar_week_num_of_month,
        "calendarWeekNumofYear": calendar_week_num_of_year,
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
        "calendar_month": "calendarMonth",
        "calendar_year": "calendarYear",
        "calendar_start_date": "beginCalendarMonth",
        "calendar_end_date": "endCalendarMonth",
        "calendar_num_of_week": "numberOfCalendarWeek",
        "calendar_firstdate_of_week": "firstDayOfWeek",
        "calendar_lastdate_of_week": "lastDateOfWeek",
        "calendar_week_num_of_month": "calendarWeekNumofMonth",
        "calendar_week_num_of_year": "calendarWeekNumofYear",
    }

    mapped_key = result_type_map.get(normalized)
    if not mapped_key:
        supported = ", ".join(result_type_map.keys())
        raise ValueError(
            f"Unsupported resultType '{result_type}'. Use one of: {supported}"
        )
    return info[mapped_key]
