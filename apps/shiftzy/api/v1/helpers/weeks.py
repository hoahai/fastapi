from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from shared.tenant import (
    TenantConfigError,
    TenantConfigValidationError,
    get_env,
    get_timezone,
)

APP_NAME = "Shiftzy"


# ============================================================
# HELPERS
# ============================================================

def _require_env_value(key: str) -> str:
    raw = get_env(key)
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=[key])
    return str(raw).strip()


def _parse_int(key: str) -> int:
    raw = _require_env_value(key)
    try:
        return int(raw)
    except ValueError as exc:
        raise TenantConfigValidationError(app_name=APP_NAME, invalid=[key]) from exc


def _parse_start_date() -> date:
    raw = _require_env_value("START_DATE")
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=["START_DATE"],
        ) from exc
    if parsed.weekday() != 0:
        raise TenantConfigValidationError(app_name=APP_NAME, invalid=["START_DATE"])
    return parsed


def _get_week_config() -> tuple[date, int]:
    start_date = _parse_start_date()
    start_week_no = _parse_int("START_WEEK_NO")
    return start_date, start_week_no


def _get_today_date() -> date:
    tz = ZoneInfo(get_timezone())
    return datetime.now(tz).date()


# ============================================================
# PUBLIC API
# ============================================================

def get_default_week_window() -> tuple[int, int]:
    before = _parse_int("WEEK_BEFORE")
    after = _parse_int("WEEK_AFTER")
    if before < 0:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=["WEEK_BEFORE"],
        )
    if after < 0:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=["WEEK_AFTER"],
        )
    return before, after


def get_week_no_for_date(target_date: date) -> int:
    start_date, start_week_no = _get_week_config()
    monday = target_date - timedelta(days=target_date.weekday())
    weeks_delta = (monday - start_date).days // 7
    return start_week_no + weeks_delta


def build_week_info(
    week_no: int,
    *,
    start_date: date | None = None,
    start_week_no: int | None = None,
    today: date | None = None,
) -> dict:
    if start_date is None or start_week_no is None:
        start_date, start_week_no = _get_week_config()

    today = today or _get_today_date()
    offset = week_no - start_week_no
    week_start = start_date + timedelta(days=offset * 7)
    week_end = week_start + timedelta(days=6)

    return {
        "week_no": week_no,
        "start_date": week_start.isoformat(),
        "end_date": week_end.isoformat(),
        "is_today_week": week_start <= today <= week_end,
    }


def list_weeks(
    *,
    week_before: int | None = None,
    week_after: int | None = None,
) -> list[dict]:
    start_date, start_week_no = _get_week_config()
    today = _get_today_date()
    monday = today - timedelta(days=today.weekday())
    weeks_delta = (monday - start_date).days // 7
    current_week_no = start_week_no + weeks_delta

    if week_before is None or week_after is None:
        default_before, default_after = get_default_week_window()
        if week_before is None:
            week_before = default_before
        if week_after is None:
            week_after = default_after

    if week_before < 0 or week_after < 0:
        raise TenantConfigError("Shiftzy week_before/week_after must be >= 0")

    start_week = current_week_no - week_before
    end_week = current_week_no + week_after

    return [
        build_week_info(
            week_no,
            start_date=start_date,
            start_week_no=start_week_no,
            today=today,
        )
        for week_no in range(start_week, end_week + 1)
    ]
