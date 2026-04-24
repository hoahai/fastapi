from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from fpdf import FPDF

from apps.tradsphere.api.v1.helpers.broadcastCalendar import get_broadcast_calendar_info


_MODE_COMPACT = "compact"
_MODE_DETAIL = "detail"
_ALLOWED_MODES = {_MODE_COMPACT, _MODE_DETAIL}
_BILLING_CALENDAR = "calendar"
_BILLING_BROADCAST = "broadcast"
_LETTER_LANDSCAPE_WIDTH_MM = 279.4
_LETTER_LANDSCAPE_HEIGHT_MM = 215.9
_DEFAULT_MARGIN_MM = 8.0
_COMPACT_WEEK_COL_WIDTH_MM = 11.0
_TOTAL_SPOT_WIDTH_MM = 16.0
_TOTAL_GROSS_WIDTH_MM = 20.0
_ZERO_SPOT_WEEK_FILL_RGB = (217, 217, 217)
_TABLE_BORDER_RGB = (214, 214, 214)
_TABLE_HEADER_BG_RGB = (224, 235, 255)
_TABLE_SUMMARY_BG_RGB = (239, 245, 255)
_SECTION_BG_RGB = (51, 119, 255)
_SECTION_TEXT_RGB = (255, 255, 255)
_COMPACT_STATIC_COLUMNS: list[tuple[str, float]] = [
    ("Vendor", 36.0),
    ("Start Date", 16.0),
    ("End Date", 16.0),
]
_DETAIL_STATIC_COLUMNS: list[tuple[str, float]] = [
    ("Vendor", 36.0),
    ("Days", 12.0),
    ("Start Date", 16.0),
    ("End Date", 16.0),
    ("DP", 8.0),
    ("Program Name", 50.0),
    ("Rtg", 8.0),
    ("Rate", 14.0),
]


class _SchedulesPDF(FPDF):
    def footer(self):
        self.set_y(-8)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(90, 90, 90)
        self.cell(0, 4, f"Page {self.page_no()}/{{nb}}", align="R")


def _normalize_mode(mode: str) -> str:
    normalized = str(mode or "").strip().lower() or _MODE_COMPACT
    if normalized not in _ALLOWED_MODES:
        raise ValueError("mode must be one of: compact, detail")
    return normalized


def _normalize_billing_type(billing_type: str | None) -> str:
    normalized = str(billing_type or "").strip().lower() or _BILLING_CALENDAR
    if normalized not in {_BILLING_CALENDAR, _BILLING_BROADCAST}:
        raise ValueError("billingType must be one of: Calendar, Broadcast")
    return normalized


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = str(value or "").strip()
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _to_int(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return 0


def _safe_text(value: object) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    return " ".join(text.split())


def _format_money(value: Decimal) -> str:
    return f"{value:,.2f}"


def _clip_text(pdf: FPDF, text: str, width: float) -> str:
    safe = _safe_text(text)
    if not safe:
        return ""
    if pdf.get_string_width(safe) <= width:
        return safe
    ellipsis = "..."
    max_width = max(0.0, width - pdf.get_string_width(ellipsis))
    out = ""
    for char in safe:
        candidate = out + char
        if pdf.get_string_width(candidate) > max_width:
            break
        out = candidate
    return f"{out}{ellipsis}" if out else ellipsis


def _format_money_short_scale(value: Decimal) -> str:
    amount = _to_decimal(value)
    sign = "-" if amount < 0 else ""
    absolute = abs(amount)
    scales: list[tuple[Decimal, str]] = [
        (Decimal("1000000000"), "B"),
        (Decimal("1000000"), "M"),
        (Decimal("1000"), "K"),
    ]
    for scale_value, suffix in scales:
        if absolute >= scale_value:
            scaled = absolute / scale_value
            text = f"{scaled:.1f}" if scaled < 100 else f"{scaled:.0f}"
            text = text.rstrip("0").rstrip(".")
            return f"{sign}${text}{suffix}"
    return f"{sign}${absolute:.0f}"


def _format_money_for_cell(pdf: FPDF, value: Decimal, width: float) -> str:
    amount = _to_decimal(value)
    candidates = [
        f"${_format_money(amount)}",
        f"${amount:,.0f}",
        _format_money_short_scale(amount),
        f"${int(amount):d}",
    ]
    usable_width = max(1.0, float(width) - 1.0)
    for text in candidates:
        if pdf.get_string_width(text) <= usable_width:
            return text
    return candidates[-1]


def _is_cable_media_type(value: object) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return False
    tokens = [part.strip() for part in text.replace("|", ",").split(",") if part.strip()]
    if not tokens:
        return text in {"CA", "CABLE"}
    return any(token in {"CA", "CABLE"} for token in tokens)


def _resolve_vendor_label(
    *,
    station_code: str,
    station_name: str,
    media_type: object,
) -> str:
    code = str(station_code or "").strip().upper()
    name = str(station_name or "").strip()
    if _is_cable_media_type(media_type):
        return f"Cable ({code})" if code else "Cable"
    if name and code:
        return f"{name} ({code})"
    if name:
        return name
    return code


def _draw_filled_rounded_rect(
    pdf: FPDF,
    *,
    x: float,
    y: float,
    w: float,
    h: float,
    radius: float,
    color: tuple[int, int, int],
) -> None:
    pdf.set_fill_color(*color)
    if hasattr(pdf, "rounded_rect"):
        try:
            pdf.rounded_rect(x, y, w, h, radius, style="F")
            return
        except Exception:
            pass
    pdf.rect(x, y, w, h, style="F")


def _effective_page_width(pdf: FPDF) -> float:
    return float(pdf.w - pdf.l_margin - pdf.r_margin)


def _parse_iso_date(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _format_date_us(value: object) -> str:
    parsed = _parse_iso_date(value)
    if not parsed:
        return _safe_text(value)
    return f"{parsed.month}/{parsed.day}/{parsed.year}"


def _format_week_label(value: object, *, billing_type: str) -> str:
    parsed = _parse_iso_date(value)
    if not parsed:
        return _safe_text(value)
    if billing_type == _BILLING_BROADCAST:
        try:
            info = get_broadcast_calendar_info(parsed)
            week_start = info.get("firstDayOfWeek")
            if isinstance(week_start, date):
                return f"{week_start.month}/{week_start.day}"
        except Exception:
            pass
    return f"{parsed.month}/{parsed.day}"


def _format_month_label(value: object, *, billing_type: str) -> str:
    parsed = _parse_iso_date(value)
    if not parsed:
        return "Weeks"
    if billing_type == _BILLING_CALENDAR:
        return parsed.strftime("%B'%y")
    try:
        info = get_broadcast_calendar_info(parsed)
        month = int(info.get("broadcastMonth"))
        year = int(info.get("broadcastYear"))
        return date(year, month, 1).strftime("%B'%y")
    except Exception:
        return parsed.strftime("%B'%y")


def _build_week_defs(
    schedule_weeks: list[dict],
    *,
    billing_type: str,
) -> list[dict[str, str]]:
    unique: dict[str, str] = {}
    for row in schedule_weeks:
        week_start = str(row.get("weekStart") or "").strip()
        parsed = _parse_iso_date(week_start)
        if not parsed:
            continue
        key = parsed.isoformat()
        unique[key] = key

    sorted_keys = sorted(unique.keys())
    out: list[dict[str, str]] = []
    for key in sorted_keys:
        out.append(
            {
                "key": key,
                "label": _format_week_label(key, billing_type=billing_type),
                "monthLabel": _format_month_label(key, billing_type=billing_type),
            }
        )

    if not out:
        return [{"key": "__none__", "label": "-", "monthLabel": "Weeks"}]
    return out


def _build_month_groups(week_defs: list[dict[str, str]]) -> list[tuple[str, int]]:
    groups: list[tuple[str, int]] = []
    for week in week_defs:
        month_label = str(week.get("monthLabel") or "Weeks")
        if groups and groups[-1][0] == month_label:
            groups[-1] = (month_label, groups[-1][1] + 1)
        else:
            groups.append((month_label, 1))
    return groups


def _compute_zero_spot_week_keys(
    *,
    week_defs: list[dict[str, str]],
    row_week_spots: list[dict[str, int]],
) -> set[str]:
    zero_weeks: set[str] = set()
    for week in week_defs:
        week_key = str(week.get("key") or "")
        if not week_key or week_key.startswith("__"):
            continue
        has_non_zero = False
        for spots_map in row_week_spots:
            if int(spots_map.get(week_key, 0)) != 0:
                has_non_zero = True
                break
        if not has_non_zero:
            zero_weeks.add(week_key)
    return zero_weeks


def _resolve_pdf_dimensions(
    *,
    schedule_weeks: list[dict],
    billing_type: str,
) -> tuple[float, float]:
    week_defs = _build_week_defs(schedule_weeks, billing_type=billing_type)
    week_count = max(1, len(week_defs))

    compact_static_total_width = sum(
        width for _label, width in _COMPACT_STATIC_COLUMNS
    )
    compact_content_width = (
        compact_static_total_width
        + _TOTAL_SPOT_WIDTH_MM
        + _TOTAL_GROSS_WIDTH_MM
        + (float(week_count) * _COMPACT_WEEK_COL_WIDTH_MM)
    )

    detail_static_total_width = sum(
        width for _label, width in _DETAIL_STATIC_COLUMNS
    )
    detail_content_width = (
        detail_static_total_width
        + _TOTAL_SPOT_WIDTH_MM
        + _TOTAL_GROSS_WIDTH_MM
        + (float(week_count) * _COMPACT_WEEK_COL_WIDTH_MM)
    )

    content_width = max(compact_content_width, detail_content_width)
    page_width = max(
        _LETTER_LANDSCAPE_WIDTH_MM,
        content_width + (_DEFAULT_MARGIN_MM * 2.0),
    )
    return (page_width, _LETTER_LANDSCAPE_HEIGHT_MM)


def _build_week_spots_map(schedule_weeks: list[dict]) -> dict[str, dict[str, int]]:
    mapped: dict[str, dict[str, int]] = {}
    for row in schedule_weeks:
        schedule_id = str(row.get("scheduleId") or "").strip()
        week_start = str(row.get("weekStart") or "").strip()
        if not schedule_id:
            continue
        parsed = _parse_iso_date(week_start)
        if not parsed:
            continue
        week_key = parsed.isoformat()
        mapped.setdefault(schedule_id, {})
        mapped[schedule_id][week_key] = int(mapped[schedule_id].get(week_key, 0)) + _to_int(
            row.get("spots")
        )
    return mapped


def _build_compact_pivot_rows(
    schedules: list[dict],
    week_spots_by_schedule: dict[str, dict[str, int]],
) -> list[dict]:
    grouped: dict[tuple[str, str], dict[str, object]] = {}

    for row in schedules:
        est_num = str(row.get("estNum") or "").strip()
        station_code = str(row.get("stationCode") or "").strip().upper()
        if not est_num or not station_code:
            continue

        key = (est_num, station_code)
        start_date = _parse_iso_date(row.get("startDate"))
        end_date = _parse_iso_date(row.get("endDate"))
        billing_code = str(row.get("billingCode") or "").strip()
        media_type = str(row.get("mediaType") or "").strip().upper()
        schedule_id = str(row.get("id") or "").strip()
        fallback_total_spot = _to_int(row.get("totalSpot"))
        fallback_total_gross = _to_decimal(row.get("totalGross"))
        rate_gross = _to_decimal(row.get("rateGross"))
        row_week_spots = week_spots_by_schedule.get(schedule_id, {})

        if key not in grouped:
            grouped[key] = {
                "estNum": est_num,
                "stationCode": station_code,
                "billingCodes": set(),
                "mediaTypes": set(),
                "startDate": start_date,
                "endDate": end_date,
                "weekSpots": {},
                "weekGross": {},
                "fallbackTotalSpot": 0,
                "fallbackTotalGross": Decimal("0"),
            }

        current = grouped[key]
        if billing_code:
            current["billingCodes"].add(billing_code)
        if media_type:
            current["mediaTypes"].add(media_type)

        current_start = current.get("startDate")
        if start_date and (current_start is None or start_date < current_start):
            current["startDate"] = start_date

        current_end = current.get("endDate")
        if end_date and (current_end is None or end_date > current_end):
            current["endDate"] = end_date

        current["fallbackTotalSpot"] = int(current.get("fallbackTotalSpot", 0)) + int(
            fallback_total_spot
        )
        current["fallbackTotalGross"] = _to_decimal(
            current.get("fallbackTotalGross")
        ) + fallback_total_gross

        week_spots = current["weekSpots"]
        week_gross = current["weekGross"]
        for week_key, spots in row_week_spots.items():
            week_spots[week_key] = int(week_spots.get(week_key, 0)) + int(spots)
            week_gross[week_key] = _to_decimal(week_gross.get(week_key)) + (
                rate_gross * Decimal(int(spots))
            )

    out: list[dict] = []
    for key in sorted(
        grouped.keys(),
        key=lambda item: (
            int(item[0]) if str(item[0]).isdigit() else 999999999,
            item[1],
        ),
    ):
        row = grouped[key]
        billing_codes = sorted(list(row.get("billingCodes") or []))
        media_types = sorted(list(row.get("mediaTypes") or []))
        out.append(
            {
                "estNum": row.get("estNum"),
                "stationCode": row.get("stationCode"),
                "billingCode": ", ".join(billing_codes),
                "mediaType": ", ".join(media_types),
                "startDate": row.get("startDate").isoformat()
                if row.get("startDate")
                else "",
                "endDate": row.get("endDate").isoformat() if row.get("endDate") else "",
                "weekSpots": dict(row.get("weekSpots") or {}),
                "weekGross": dict(row.get("weekGross") or {}),
                "fallbackTotalSpot": int(row.get("fallbackTotalSpot", 0)),
                "fallbackTotalGross": _to_decimal(row.get("fallbackTotalGross")),
            }
        )

    return out


def _draw_report_header(
    pdf: FPDF,
    *,
    mode: str,
    billing_type: str,
) -> None:
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 9, "TradSphere Schedule Report", border=0, ln=1, align="C")

    pdf.set_font("Helvetica", "", 9)
    subtitle = f"Mode: {mode.title()} | Billing Type: {billing_type.title()}"
    pdf.cell(0, 6, subtitle, border=0, ln=1, align="C")
    pdf.ln(4)


def _draw_table_context_header(
    pdf: FPDF,
    *,
    est_num: int,
    est_num_note: str | None,
    total_spots: int,
    total_gross: Decimal,
) -> None:
    est_num_text = f"EstNum: {est_num}"
    note_text = _safe_text(est_num_note)
    if note_text:
        est_num_text = f"{est_num_text} | Note: {note_text}"

    # Keep visual breathing room between the report header and table context.
    pdf.ln(2)

    # Section header (SpendSphere style): blue rounded bar with white text.
    x = pdf.l_margin
    y = pdf.get_y()
    w = pdf.w - pdf.l_margin - pdf.r_margin
    h = 7.0
    _draw_filled_rounded_rect(
        pdf,
        x=x,
        y=y,
        w=w,
        h=h,
        radius=1.2,
        color=_SECTION_BG_RGB,
    )
    pdf.set_text_color(*_SECTION_TEXT_RGB)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_xy(x + 1.2, y)
    pdf.cell(w - 2.4, h, _clip_text(pdf, est_num_text, w - 2.4), border=0, ln=1, align="L")
    pdf.set_y(y + h + 1.0)

    # Context sub-header row using the same table visual language.
    pdf.set_draw_color(*_TABLE_BORDER_RGB)
    pdf.set_fill_color(*_TABLE_SUMMARY_BG_RGB)
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(
        0,
        6,
        f"Gross: ${_format_money(total_gross)} | Spots: {total_spots}",
        border=1,
        ln=1,
        fill=True,
    )
    pdf.ln(2)


def _draw_compact_table_header(
    pdf: FPDF,
    *,
    static_columns: list[tuple[str, float]],
    week_defs: list[dict[str, str]],
    week_col_width: float,
    total_spot_width: float,
    total_gross_width: float,
) -> None:
    month_groups = _build_month_groups(week_defs)
    row_height = 6
    header_total_height = row_height * 2
    start_x = pdf.get_x()
    start_y = pdf.get_y()

    pdf.set_draw_color(*_TABLE_BORDER_RGB)
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(*_TABLE_HEADER_BG_RGB)
    pdf.set_text_color(0, 0, 0)
    current_x = start_x
    for label, width in static_columns:
        pdf.set_xy(current_x, start_y)
        pdf.cell(width, header_total_height, label, border=1, align="C", fill=True)
        current_x += width

    pdf.set_xy(current_x, start_y)
    for month_label, month_count in month_groups:
        pdf.cell(
            week_col_width * float(month_count),
            row_height,
            month_label,
            border=1,
            align="C",
            fill=True,
        )

    week_block_width = week_col_width * float(len(week_defs))
    pdf.set_xy(current_x + week_block_width, start_y)
    pdf.cell(total_spot_width, header_total_height, "Total Spot", border=1, align="C", fill=True)
    pdf.set_xy(current_x + week_block_width + total_spot_width, start_y)
    pdf.cell(
        total_gross_width,
        header_total_height,
        "Total Gross",
        border=1,
        align="C",
        fill=True,
    )

    pdf.set_xy(current_x, start_y + row_height)
    for week in week_defs:
        pdf.cell(
            week_col_width,
            row_height,
            _clip_text(pdf, week.get("label") or "", week_col_width - 1),
            border=1,
            align="C",
            fill=True,
        )

    pdf.set_xy(start_x, start_y + header_total_height)


def _new_page_for_compact(
    pdf: FPDF,
    *,
    est_num: int,
    mode: str,
    billing_type: str,
    est_num_note: str | None,
    total_spots: int,
    total_gross: Decimal,
    static_columns: list[tuple[str, float]],
    week_defs: list[dict[str, str]],
    week_col_width: float,
    total_spot_width: float,
    total_gross_width: float,
) -> None:
    pdf.add_page()
    _draw_report_header(
        pdf,
        mode=mode,
        billing_type=billing_type,
    )
    _draw_table_context_header(
        pdf,
        est_num=est_num,
        est_num_note=est_num_note,
        total_spots=total_spots,
        total_gross=total_gross,
    )
    _draw_compact_table_header(
        pdf,
        static_columns=static_columns,
        week_defs=week_defs,
        week_col_width=week_col_width,
        total_spot_width=total_spot_width,
        total_gross_width=total_gross_width,
    )


def _draw_compact_mode(
    pdf: FPDF,
    *,
    schedules: list[dict],
    schedule_weeks: list[dict],
    station_names: dict[str, str] | None,
    billing_type: str,
    est_num_note: str | None,
    est_num: int,
    mode: str,
    total_spots: int,
    total_gross: Decimal,
    tenant_id: str | None,
) -> None:
    static_columns = _COMPACT_STATIC_COLUMNS
    total_spot_width = _TOTAL_SPOT_WIDTH_MM
    total_gross_width = _TOTAL_GROSS_WIDTH_MM
    week_defs = _build_week_defs(schedule_weeks, billing_type=billing_type)
    week_count = max(1, len(week_defs))
    static_total_width = sum(width for _label, width in static_columns)
    available_week_width = (
        _effective_page_width(pdf) - static_total_width - total_spot_width - total_gross_width
    )
    if available_week_width <= 0:
        week_col_width = 10.0
    else:
        week_col_width = available_week_width / float(week_count)
    week_spots_by_schedule = _build_week_spots_map(schedule_weeks)
    compact_rows = _build_compact_pivot_rows(schedules, week_spots_by_schedule)
    zero_spot_week_keys = _compute_zero_spot_week_keys(
        week_defs=week_defs,
        row_week_spots=[dict(row.get("weekSpots") or {}) for row in compact_rows],
    )
    station_name_map: dict[str, str] = {}
    for key, value in (station_names or {}).items():
        station_name_map[str(key or "").strip().upper()] = str(value or "").strip()

    _draw_table_context_header(
        pdf,
        est_num=est_num,
        est_num_note=est_num_note,
        total_spots=total_spots,
        total_gross=total_gross,
    )
    _draw_compact_table_header(
        pdf,
        static_columns=static_columns,
        week_defs=week_defs,
        week_col_width=week_col_width,
        total_spot_width=total_spot_width,
        total_gross_width=total_gross_width,
    )
    pdf.set_draw_color(*_TABLE_BORDER_RGB)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(20, 20, 20)
    row_height = 6
    month_groups = _build_month_groups(week_defs)

    summary_total_spot = 0
    summary_total_gross = Decimal("0")
    weekly_spot_totals: dict[str, int] = {}
    weekly_gross_totals: dict[str, Decimal] = {}

    for row in compact_rows:
        if pdf.get_y() + row_height > pdf.h - pdf.b_margin:
            _new_page_for_compact(
                pdf,
                est_num=est_num,
                mode=mode,
                billing_type=billing_type,
                est_num_note=est_num_note,
                total_spots=total_spots,
                total_gross=total_gross,
                static_columns=static_columns,
                week_defs=week_defs,
                week_col_width=week_col_width,
                total_spot_width=total_spot_width,
                total_gross_width=total_gross_width,
            )
            pdf.set_draw_color(*_TABLE_BORDER_RGB)
            pdf.set_font("Helvetica", "", 8)

        station_code = str(row.get("stationCode") or "").strip().upper()
        station_name = station_name_map.get(station_code, "")
        vendor = _resolve_vendor_label(
            station_code=station_code,
            station_name=station_name,
            media_type=row.get("mediaType"),
        )

        static_values = [
            vendor,
            _format_date_us(row.get("startDate")),
            _format_date_us(row.get("endDate")),
        ]
        for index, (_label, width) in enumerate(static_columns):
            align = "L" if index == 0 else "C"
            text = _clip_text(pdf, static_values[index], width - 1)
            pdf.cell(width, row_height, text, border=1, align=align)

        row_week_spots = dict(row.get("weekSpots") or {})
        row_total_spot = 0
        for week in week_defs:
            week_key = str(week.get("key") or "")
            spots = int(row_week_spots.get(week_key, 0))
            row_total_spot += spots
            if week_key in zero_spot_week_keys:
                pdf.set_fill_color(*_ZERO_SPOT_WEEK_FILL_RGB)
                pdf.cell(week_col_width, row_height, str(spots), border=1, align="C", fill=True)
            else:
                pdf.cell(week_col_width, row_height, str(spots), border=1, align="C")
            weekly_spot_totals[week_key] = int(weekly_spot_totals.get(week_key, 0)) + int(spots)

        if row_total_spot <= 0:
            row_total_spot = _to_int(row.get("fallbackTotalSpot"))
        pdf.cell(total_spot_width, row_height, str(row_total_spot), border=1, align="C")

        row_week_gross = dict(row.get("weekGross") or {})
        row_total_gross = Decimal("0")
        for week in week_defs:
            week_key = str(week.get("key") or "")
            value = _to_decimal(row_week_gross.get(week_key))
            row_total_gross += value
            weekly_gross_totals[week_key] = _to_decimal(weekly_gross_totals.get(week_key)) + value

        if row_total_gross <= 0:
            row_total_gross = _to_decimal(row.get("fallbackTotalGross"))
        pdf.cell(
            total_gross_width,
            row_height,
            _format_money(row_total_gross),
            border=1,
            align="R",
        )
        pdf.ln(row_height)

        summary_total_spot += int(row_total_spot)
        summary_total_gross += row_total_gross

    summary_rows_height = row_height * 3
    if pdf.get_y() + summary_rows_height > pdf.h - pdf.b_margin:
        _new_page_for_compact(
            pdf,
            est_num=est_num,
            mode=mode,
            billing_type=billing_type,
            est_num_note=est_num_note,
            total_spots=total_spots,
            total_gross=total_gross,
            static_columns=static_columns,
            week_defs=week_defs,
            week_col_width=week_col_width,
            total_spot_width=total_spot_width,
            total_gross_width=total_gross_width,
        )

    summary_start_y = pdf.get_y()
    summary_label_width = static_total_width
    week_block_width = week_col_width * float(len(week_defs))
    right_block_x = pdf.get_x() + summary_label_width + week_block_width

    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(*_TABLE_SUMMARY_BG_RGB)
    pdf.set_text_color(40, 40, 40)

    # Row 1: Total Spots
    pdf.cell(summary_label_width, row_height, "Total Spots", border=1, align="L", fill=True)
    for week in week_defs:
        week_key = str(week.get("key") or "")
        spot_value = int(weekly_spot_totals.get(week_key, 0))
        pdf.cell(
            week_col_width,
            row_height,
            str(spot_value),
            border=1,
            align="C",
            fill=True,
        )

    pdf.set_xy(right_block_x, summary_start_y)
    pdf.cell(
        total_spot_width,
        summary_rows_height,
        str(summary_total_spot),
        border=1,
        align="C",
        fill=True,
    )
    pdf.cell(
        total_gross_width,
        summary_rows_height,
        f"${_format_money(summary_total_gross)}",
        border=1,
        align="R",
        fill=True,
    )
    pdf.set_xy(pdf.l_margin, summary_start_y + row_height)

    # Row 2: Total Gross
    pdf.cell(summary_label_width, row_height, "Total Gross", border=1, align="L", fill=True)
    for week in week_defs:
        week_key = str(week.get("key") or "")
        gross_value = _to_decimal(weekly_gross_totals.get(week_key))
        gross_text = _format_money_for_cell(pdf, gross_value, week_col_width)
        pdf.cell(
            week_col_width,
            row_height,
            gross_text,
            border=1,
            align="C",
            fill=True,
        )
    pdf.set_xy(pdf.l_margin, summary_start_y + (row_height * 2))

    # Row 3: Total Gross by Month
    pdf.cell(summary_label_width, row_height, "Total Gross by Month", border=1, align="L", fill=True)
    month_week_index = 0
    for _month_label, month_count in month_groups:
        month_width = week_col_width * float(month_count)
        month_total = Decimal("0")
        for _ in range(month_count):
            if month_week_index >= len(week_defs):
                break
            week_key = str(week_defs[month_week_index].get("key") or "")
            month_total += _to_decimal(weekly_gross_totals.get(week_key))
            month_week_index += 1
        pdf.cell(
            month_width,
            row_height,
            f"${_format_money(month_total)}",
            border=1,
            align="C",
            fill=True,
        )
    pdf.set_y(summary_start_y + summary_rows_height)


def _draw_detail_mode(
    pdf: FPDF,
    *,
    schedules: list[dict],
    schedule_weeks: list[dict],
    station_names: dict[str, str] | None,
    billing_type: str,
    est_num_note: str | None,
    est_num: int,
    mode: str,
    total_spots: int,
    total_gross: Decimal,
    tenant_id: str | None,
) -> None:
    static_columns = _DETAIL_STATIC_COLUMNS
    total_spot_width = _TOTAL_SPOT_WIDTH_MM
    total_gross_width = _TOTAL_GROSS_WIDTH_MM
    week_defs = _build_week_defs(schedule_weeks, billing_type=billing_type)
    week_count = max(1, len(week_defs))
    static_total_width = sum(width for _label, width in static_columns)
    available_week_width = (
        _effective_page_width(pdf) - static_total_width - total_spot_width - total_gross_width
    )
    if available_week_width <= 0:
        week_col_width = 10.0
    else:
        week_col_width = available_week_width / float(week_count)

    week_spots_by_schedule = _build_week_spots_map(schedule_weeks)
    zero_spot_week_keys = _compute_zero_spot_week_keys(
        week_defs=week_defs,
        row_week_spots=[
            dict(week_spots_by_schedule.get(str(row.get("id") or "").strip(), {}))
            for row in schedules
        ],
    )
    station_name_map: dict[str, str] = {}
    for key, value in (station_names or {}).items():
        station_name_map[str(key or "").strip().upper()] = str(value or "").strip()

    _draw_table_context_header(
        pdf,
        est_num=est_num,
        est_num_note=est_num_note,
        total_spots=total_spots,
        total_gross=total_gross,
    )
    _draw_compact_table_header(
        pdf,
        static_columns=static_columns,
        week_defs=week_defs,
        week_col_width=week_col_width,
        total_spot_width=total_spot_width,
        total_gross_width=total_gross_width,
    )
    pdf.set_draw_color(*_TABLE_BORDER_RGB)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(20, 20, 20)
    row_height = 6
    month_groups = _build_month_groups(week_defs)

    summary_total_spot = 0
    summary_total_gross = Decimal("0")
    weekly_spot_totals: dict[str, int] = {}
    weekly_gross_totals: dict[str, Decimal] = {}
    current_station_key: str | None = None
    current_station_label = ""
    station_week_spot_totals: dict[str, int] = {}
    station_week_gross_totals: dict[str, Decimal] = {}
    station_total_spot = 0
    station_total_gross = Decimal("0")

    def _flush_station_subtotal_row() -> None:
        nonlocal station_week_spot_totals
        nonlocal station_week_gross_totals
        nonlocal station_total_spot
        nonlocal station_total_gross
        nonlocal current_station_label
        if current_station_key is None:
            return
        summary_rows_height = row_height * 3
        if pdf.get_y() + summary_rows_height > pdf.h - pdf.b_margin:
            _new_page_for_compact(
                pdf,
                est_num=est_num,
                mode=mode,
                billing_type=billing_type,
                est_num_note=est_num_note,
                total_spots=total_spots,
                total_gross=total_gross,
                static_columns=static_columns,
                week_defs=week_defs,
                week_col_width=week_col_width,
                total_spot_width=total_spot_width,
                total_gross_width=total_gross_width,
            )
            pdf.set_draw_color(*_TABLE_BORDER_RGB)

        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(*_TABLE_SUMMARY_BG_RGB)
        pdf.set_text_color(35, 35, 35)
        summary_start_y = pdf.get_y()
        summary_label_width = static_total_width
        week_block_width = week_col_width * float(len(week_defs))
        right_block_x = pdf.get_x() + summary_label_width + week_block_width

        # Row 1: Total Spots
        title_1 = _clip_text(
            pdf,
            f"{current_station_label} - Total Spots",
            summary_label_width - 1,
        )
        pdf.cell(summary_label_width, row_height, title_1, border=1, align="R", fill=True)
        for week in week_defs:
            week_key = str(week.get("key") or "")
            spot_value = int(station_week_spot_totals.get(week_key, 0))
            pdf.cell(
                week_col_width,
                row_height,
                str(spot_value),
                border=1,
                align="C",
                fill=True,
            )

        pdf.set_xy(right_block_x, summary_start_y)
        pdf.cell(
            total_spot_width,
            summary_rows_height,
            str(station_total_spot),
            border=1,
            align="C",
            fill=True,
        )
        pdf.cell(
            total_gross_width,
            summary_rows_height,
            f"${_format_money(station_total_gross)}",
            border=1,
            align="R",
            fill=True,
        )

        # Row 2: Total Gross
        pdf.set_xy(pdf.l_margin, summary_start_y + row_height)
        title_2 = _clip_text(
            pdf,
            f"{current_station_label} - Total Gross",
            summary_label_width - 1,
        )
        pdf.cell(summary_label_width, row_height, title_2, border=1, align="R", fill=True)
        for week in week_defs:
            week_key = str(week.get("key") or "")
            gross_value = _to_decimal(station_week_gross_totals.get(week_key))
            gross_text = _format_money_for_cell(pdf, gross_value, week_col_width)
            pdf.cell(
                week_col_width,
                row_height,
                gross_text,
                border=1,
                align="C",
                fill=True,
            )

        # Row 3: Total Gross by Month
        pdf.set_xy(pdf.l_margin, summary_start_y + (row_height * 2))
        title_3 = _clip_text(
            pdf,
            f"{current_station_label} - Total Gross by Month",
            summary_label_width - 1,
        )
        pdf.cell(summary_label_width, row_height, title_3, border=1, align="R", fill=True)
        month_week_index = 0
        for _month_label, month_count in month_groups:
            month_width = week_col_width * float(month_count)
            month_total = Decimal("0")
            for _ in range(month_count):
                if month_week_index >= len(week_defs):
                    break
                week_key = str(week_defs[month_week_index].get("key") or "")
                month_total += _to_decimal(station_week_gross_totals.get(week_key))
                month_week_index += 1
            pdf.cell(
                month_width,
                row_height,
                f"${_format_money(month_total)}",
                border=1,
                align="C",
                fill=True,
            )

        pdf.set_y(summary_start_y + summary_rows_height)
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(20, 20, 20)

    for row in schedules:
        station_code = str(row.get("stationCode") or "").strip().upper()
        station_name = station_name_map.get(station_code, "")
        vendor = _resolve_vendor_label(
            station_code=station_code,
            station_name=station_name,
            media_type=row.get("mediaType"),
        )
        station_key = station_code or vendor.upper()

        if current_station_key is None:
            current_station_key = station_key
            current_station_label = vendor
        elif station_key != current_station_key:
            _flush_station_subtotal_row()
            current_station_key = station_key
            current_station_label = vendor
            station_week_spot_totals = {}
            station_week_gross_totals = {}
            station_total_spot = 0
            station_total_gross = Decimal("0")

        if pdf.get_y() + row_height > pdf.h - pdf.b_margin:
            _new_page_for_compact(
                pdf,
                est_num=est_num,
                mode=mode,
                billing_type=billing_type,
                est_num_note=est_num_note,
                total_spots=total_spots,
                total_gross=total_gross,
                static_columns=static_columns,
                week_defs=week_defs,
                week_col_width=week_col_width,
                total_spot_width=total_spot_width,
                total_gross_width=total_gross_width,
            )
            pdf.set_draw_color(*_TABLE_BORDER_RGB)
            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(20, 20, 20)

        static_values = [
            vendor,
            _safe_text(row.get("days")),
            _format_date_us(row.get("startDate")),
            _format_date_us(row.get("endDate")),
            _safe_text(row.get("daypart")),
            _safe_text(row.get("programName")),
            _safe_text(row.get("rtg")),
            _format_money(_to_decimal(row.get("rateGross"))),
        ]
        for index, (_label, width) in enumerate(static_columns):
            if index in {0, 5}:
                align = "L"
            elif index == 7:
                align = "R"
            else:
                align = "C"
            text = _clip_text(pdf, static_values[index], width - 1)
            pdf.cell(width, row_height, text, border=1, align=align)

        schedule_row_id = str(row.get("id") or "").strip()
        row_week_spots = week_spots_by_schedule.get(schedule_row_id, {})
        row_rate_gross = _to_decimal(row.get("rateGross"))
        row_total_spot = 0
        row_total_gross_by_weeks = Decimal("0")
        for week in week_defs:
            week_key = str(week.get("key") or "")
            spots = int(row_week_spots.get(week_key, 0))
            row_total_spot += spots
            if week_key in zero_spot_week_keys:
                pdf.set_fill_color(*_ZERO_SPOT_WEEK_FILL_RGB)
                pdf.cell(week_col_width, row_height, str(spots), border=1, align="C", fill=True)
            else:
                pdf.cell(week_col_width, row_height, str(spots), border=1, align="C")
            weekly_spot_totals[week_key] = int(weekly_spot_totals.get(week_key, 0)) + int(spots)
            station_week_spot_totals[week_key] = int(station_week_spot_totals.get(week_key, 0)) + int(
                spots
            )
            gross_value = row_rate_gross * Decimal(int(spots))
            row_total_gross_by_weeks += gross_value
            weekly_gross_totals[week_key] = _to_decimal(weekly_gross_totals.get(week_key)) + gross_value
            station_week_gross_totals[week_key] = (
                _to_decimal(station_week_gross_totals.get(week_key)) + gross_value
            )

        if row_total_spot <= 0:
            row_total_spot = _to_int(row.get("totalSpot"))
        pdf.cell(total_spot_width, row_height, str(row_total_spot), border=1, align="C")

        row_total_gross = _to_decimal(row.get("totalGross"))
        if row_total_gross <= 0:
            if row_total_gross_by_weeks > 0:
                row_total_gross = row_total_gross_by_weeks
            else:
                row_total_gross = row_rate_gross * Decimal(row_total_spot)
        pdf.cell(
            total_gross_width,
            row_height,
            _format_money(row_total_gross),
            border=1,
            align="R",
        )
        pdf.ln(row_height)

        summary_total_spot += int(row_total_spot)
        summary_total_gross += row_total_gross
        station_total_spot += int(row_total_spot)
        station_total_gross += row_total_gross

    _flush_station_subtotal_row()

    summary_rows_height = row_height * 3
    if pdf.get_y() + summary_rows_height > pdf.h - pdf.b_margin:
        _new_page_for_compact(
            pdf,
            est_num=est_num,
            mode=mode,
            billing_type=billing_type,
            est_num_note=est_num_note,
            total_spots=total_spots,
            total_gross=total_gross,
            static_columns=static_columns,
            week_defs=week_defs,
            week_col_width=week_col_width,
            total_spot_width=total_spot_width,
            total_gross_width=total_gross_width,
        )

    summary_start_y = pdf.get_y()
    summary_label_width = static_total_width
    week_block_width = week_col_width * float(len(week_defs))
    right_block_x = pdf.get_x() + summary_label_width + week_block_width

    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(*_TABLE_SUMMARY_BG_RGB)
    pdf.set_text_color(40, 40, 40)

    # Row 1: Total Spots
    pdf.cell(summary_label_width, row_height, "Total Spots", border=1, align="L", fill=True)
    for week in week_defs:
        week_key = str(week.get("key") or "")
        spot_value = int(weekly_spot_totals.get(week_key, 0))
        pdf.cell(
            week_col_width,
            row_height,
            str(spot_value),
            border=1,
            align="C",
            fill=True,
        )

    pdf.set_xy(right_block_x, summary_start_y)
    pdf.cell(
        total_spot_width,
        summary_rows_height,
        str(summary_total_spot),
        border=1,
        align="C",
        fill=True,
    )
    pdf.cell(
        total_gross_width,
        summary_rows_height,
        f"${_format_money(summary_total_gross)}",
        border=1,
        align="R",
        fill=True,
    )
    pdf.set_xy(pdf.l_margin, summary_start_y + row_height)

    # Row 2: Total Gross
    pdf.cell(summary_label_width, row_height, "Total Gross", border=1, align="L", fill=True)
    for week in week_defs:
        week_key = str(week.get("key") or "")
        gross_value = _to_decimal(weekly_gross_totals.get(week_key))
        gross_text = _format_money_for_cell(pdf, gross_value, week_col_width)
        pdf.cell(
            week_col_width,
            row_height,
            gross_text,
            border=1,
            align="C",
            fill=True,
        )
    pdf.set_xy(pdf.l_margin, summary_start_y + (row_height * 2))

    # Row 3: Total Gross by Month
    pdf.cell(summary_label_width, row_height, "Total Gross by Month", border=1, align="L", fill=True)
    month_week_index = 0
    for _month_label, month_count in month_groups:
        month_width = week_col_width * float(month_count)
        month_total = Decimal("0")
        for _ in range(month_count):
            if month_week_index >= len(week_defs):
                break
            week_key = str(week_defs[month_week_index].get("key") or "")
            month_total += _to_decimal(weekly_gross_totals.get(week_key))
            month_week_index += 1
        pdf.cell(
            month_width,
            row_height,
            f"${_format_money(month_total)}",
            border=1,
            align="C",
            fill=True,
        )
    pdf.set_y(summary_start_y + summary_rows_height)


def build_schedules_pdf(
    *,
    est_num: int,
    est_num_note: str | None = None,
    schedules: list[dict],
    mode: str = _MODE_COMPACT,
    schedule_weeks: list[dict] | None = None,
    station_names: dict[str, str] | None = None,
    billing_type: str = "Calendar",
    tenant_id: str | None = None,
) -> bytes:
    normalized_mode = _normalize_mode(mode)
    normalized_billing_type = _normalize_billing_type(billing_type)
    normalized_schedules = [row for row in schedules if isinstance(row, dict)]
    normalized_schedule_weeks = [
        row for row in (schedule_weeks or []) if isinstance(row, dict)
    ]

    total_spots = sum(_to_int(row.get("totalSpot")) for row in normalized_schedules)
    total_gross = sum((_to_decimal(row.get("totalGross")) for row in normalized_schedules), Decimal("0"))

    page_width_mm, page_height_mm = _resolve_pdf_dimensions(
        schedule_weeks=normalized_schedule_weeks,
        billing_type=normalized_billing_type,
    )
    pdf = _SchedulesPDF(
        orientation="L",
        unit="mm",
        format=(page_height_mm, page_width_mm),
    )
    pdf.alias_nb_pages()
    pdf.set_margins(8, 10, 8)
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    _draw_report_header(
        pdf,
        mode=normalized_mode,
        billing_type=normalized_billing_type,
    )

    if not normalized_schedules:
        pdf.set_text_color(80, 80, 80)
        pdf.set_font("Helvetica", "I", 11)
        pdf.multi_cell(0, 7, "No schedules found for the provided estnum.", border=1)
    elif normalized_mode == _MODE_COMPACT:
        _draw_compact_mode(
            pdf,
            schedules=normalized_schedules,
            schedule_weeks=normalized_schedule_weeks,
            station_names=station_names,
            billing_type=normalized_billing_type,
            est_num_note=est_num_note,
            est_num=est_num,
            mode=normalized_mode,
            total_spots=total_spots,
            total_gross=total_gross,
            tenant_id=tenant_id,
        )
    else:
        _draw_detail_mode(
            pdf,
            schedules=normalized_schedules,
            schedule_weeks=normalized_schedule_weeks,
            station_names=station_names,
            billing_type=normalized_billing_type,
            est_num_note=est_num_note,
            est_num=est_num,
            mode=normalized_mode,
            total_spots=total_spots,
            total_gross=total_gross,
            tenant_id=tenant_id,
        )

    rendered = pdf.output(dest="S")
    if isinstance(rendered, str):
        return rendered.encode("latin-1")
    return bytes(rendered)
