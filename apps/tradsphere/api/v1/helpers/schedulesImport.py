from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from apps.tradsphere.api.v1.helpers.schedules import create_schedules_data


_MAX_LINE_NUM = 9999
_MAX_NUM_OF_WEEK = 5


def _slice(line: str, start: int, length: int) -> str:
    index = max(start - 1, 0)
    return line[index : index + length]


def _trim_text(
    raw: str,
    *,
    field: str,
    line_no: int,
    required: bool,
    max_length: int,
    uppercase: bool = False,
) -> str:
    text = str(raw or "").strip()
    if uppercase:
        text = text.upper()
    if required and not text:
        raise ValueError(f"line {line_no}: {field} is required")
    if text and len(text) > max_length:
        raise ValueError(
            f"line {line_no}: {field} must be <= {max_length} characters"
        )
    return text


def _parse_non_negative_int(
    raw: str,
    *,
    field: str,
    line_no: int,
    required: bool,
    default: int | None = None,
    maximum: int | None = None,
) -> int:
    text = str(raw or "").strip()
    if not text:
        if required:
            raise ValueError(f"line {line_no}: {field} is required")
        if default is not None:
            return default
        raise ValueError(f"line {line_no}: {field} cannot be empty")
    try:
        parsed = int(text)
    except ValueError as exc:
        raise ValueError(f"line {line_no}: {field} must be an integer") from exc
    if parsed < 0:
        raise ValueError(f"line {line_no}: {field} must be >= 0")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"line {line_no}: {field} must be <= {maximum}")
    return parsed


def _parse_decimal_text(
    raw: str,
    *,
    field: str,
    line_no: int,
    required: bool,
    default: str | None = None,
) -> str | None:
    text = str(raw or "").strip().replace(",", "")
    if not text:
        if required:
            raise ValueError(f"line {line_no}: {field} is required")
        return default
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"line {line_no}: {field} must be numeric") from exc
    if parsed < 0:
        raise ValueError(f"line {line_no}: {field} must be >= 0")
    return str(parsed)


def _parse_yymmdd(
    raw: str,
    *,
    field: str,
    line_no: int,
) -> tuple[str, date]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError(f"line {line_no}: {field} is required")
    if len(text) != 6 or not text.isdigit():
        raise ValueError(f"line {line_no}: {field} must be YYMMDD")
    year = 2000 + int(text[0:2])
    month = int(text[2:4])
    day = int(text[4:6])
    try:
        parsed = date(year, month, day)
    except ValueError as exc:
        raise ValueError(f"line {line_no}: {field} is invalid date") from exc
    return parsed.isoformat(), parsed


def _normalize_schedule_id(raw: str, *, line_no: int) -> str:
    text = str(raw or "").strip()
    if not text:
        return f"M{line_no + 99999}"
    try:
        parsed = int(text)
    except ValueError:
        parsed_text = _trim_text(
            text,
            field="ScheduleID",
            line_no=line_no,
            required=True,
            max_length=20,
        )
        return parsed_text
    if parsed < 0:
        raise ValueError(f"line {line_no}: ScheduleID must be >= 0")
    return str(parsed)


def _parse_media_type(raw: str) -> str:
    text = str(raw or "").strip().replace(" ", "")
    if text == "T":
        return "TV"
    return "RA"


def _parse_lines(payload: dict | str) -> list[tuple[int, str]]:
    skip_blank_lines = True
    lines: list[str]

    if isinstance(payload, str):
        lines = payload.splitlines()
    elif isinstance(payload, dict):
        if "skipBlankLines" in payload:
            skip_blank_lines = bool(payload.get("skipBlankLines"))
        raw_content = (
            payload.get("content")
            or payload.get("rawText")
            or payload.get("text")
        )
        if isinstance(raw_content, str):
            lines = raw_content.splitlines()
        elif isinstance(payload.get("lines"), list):
            lines = []
            for idx, item in enumerate(payload.get("lines") or [], start=1):
                if item is None:
                    raise ValueError(f"lines[{idx}] must be a string")
                lines.append(str(item))
        else:
            raise ValueError(
                "Payload must include content/rawText/text (string) or lines (array)"
            )
    else:
        raise ValueError("Payload must be an object or a raw text string")

    out: list[tuple[int, str]] = []
    for idx, line in enumerate(lines, start=1):
        current = str(line or "")
        if skip_blank_lines and not current.strip():
            continue
        out.append((idx, current.rstrip("\r")))

    if not out:
        raise ValueError("No schedule lines provided")
    return out


def _parse_schedule_line(
    *,
    line_no: int,
    line: str,
) -> dict:
    est_num = _parse_non_negative_int(
        _slice(line, 1, 10),
        field="EstNum",
        line_no=line_no,
        required=True,
    )
    billing_code = _trim_text(
        _slice(line, 17, 15),
        field="ClientBillingCode",
        line_no=line_no,
        required=True,
        max_length=20,
    )
    station_code = _trim_text(
        _slice(line, 34, 10),
        field="VendorCode",
        line_no=line_no,
        required=True,
        max_length=10,
        uppercase=True,
    )
    line_num = _parse_non_negative_int(
        _slice(line, 44, 4),
        field="LineNum",
        line_no=line_no,
        required=True,
        maximum=_MAX_LINE_NUM,
    )
    start_date, start_date_obj = _parse_yymmdd(
        _slice(line, 56, 6),
        field="StartDate",
        line_no=line_no,
    )
    end_date, end_date_obj = _parse_yymmdd(
        _slice(line, 62, 6),
        field="EndDate",
        line_no=line_no,
    )
    if start_date_obj > end_date_obj:
        raise ValueError(f"line {line_no}: StartDate must be on or before EndDate")

    total_spot = _parse_non_negative_int(
        _slice(line, 68, 5),
        field="TotalSpot",
        line_no=line_no,
        required=False,
        default=0,
    )
    rate_gross = _parse_decimal_text(
        _slice(line, 73, 12),
        field="RateGross",
        line_no=line_no,
        required=False,
        default="0",
    )
    length = _parse_non_negative_int(
        _slice(line, 97, 4),
        field="Length",
        line_no=line_no,
        required=True,
        maximum=255,
    )
    num_of_week = _parse_non_negative_int(
        _slice(line, 101, 2),
        field="NumofWeek",
        line_no=line_no,
        required=False,
        default=0,
        maximum=_MAX_NUM_OF_WEEK,
    )
    week_spots = [
        _parse_non_negative_int(
            _slice(line, 103, 3),
            field="W1",
            line_no=line_no,
            required=False,
            default=0,
        ),
        _parse_non_negative_int(
            _slice(line, 106, 3),
            field="W2",
            line_no=line_no,
            required=False,
            default=0,
        ),
        _parse_non_negative_int(
            _slice(line, 109, 3),
            field="W3",
            line_no=line_no,
            required=False,
            default=0,
        ),
        _parse_non_negative_int(
            _slice(line, 112, 3),
            field="W4",
            line_no=line_no,
            required=False,
            default=0,
        ),
        _parse_non_negative_int(
            _slice(line, 115, 3),
            field="W5",
            line_no=line_no,
            required=False,
            default=0,
        ),
    ]

    year_two_digit = _parse_non_negative_int(
        _slice(line, 118, 2),
        field="Year",
        line_no=line_no,
        required=True,
        maximum=99,
    )
    broadcast_month = _parse_non_negative_int(
        _slice(line, 120, 2),
        field="Month",
        line_no=line_no,
        required=True,
        maximum=12,
    )
    if broadcast_month < 1:
        raise ValueError(f"line {line_no}: Month must be >= 1")
    broadcast_year = 2000 + year_two_digit

    runtime = _trim_text(
        _slice(line, 122, 30),
        field="Runtime",
        line_no=line_no,
        required=True,
        max_length=50,
    )
    program_name = _trim_text(
        _slice(line, 152, 30),
        field="ProgramName",
        line_no=line_no,
        required=False,
        max_length=255,
    )
    days = _trim_text(
        _slice(line, 182, 20),
        field="Days",
        line_no=line_no,
        required=True,
        max_length=20,
    )
    total_gross = _parse_decimal_text(
        _slice(line, 331, 12),
        field="TotalGross",
        line_no=line_no,
        required=False,
        default="0",
    )
    daypart = _trim_text(
        _slice(line, 343, 23),
        field="DayPart",
        line_no=line_no,
        required=True,
        max_length=10,
    )
    rating = _parse_decimal_text(
        _slice(line, 436, 10),
        field="Rating",
        line_no=line_no,
        required=False,
        default=None,
    )

    schedule_id = _normalize_schedule_id(_slice(line, 446, 10), line_no=line_no)
    schedule_payload = {
        "scheduleId": schedule_id,
        "lineNum": line_num,
        "estNum": est_num,
        "billingCode": billing_code,
        "mediaType": _parse_media_type(_slice(line, 32, 2)),
        "stationCode": station_code,
        "broadcastMonth": broadcast_month,
        "broadcastYear": broadcast_year,
        "startDate": start_date,
        "endDate": end_date,
        "totalSpot": total_spot,
        "totalGross": total_gross,
        "rateGross": rate_gross,
        "length": length,
        "runtime": runtime,
        "programName": program_name or None,
        "days": days,
        "daypart": daypart,
        "rtg": rating,
    }
    for week_index in range(min(num_of_week, _MAX_NUM_OF_WEEK)):
        schedule_payload[f"w{week_index + 1}"] = int(week_spots[week_index])

    return {
        "lineNo": line_no,
        "schedule": schedule_payload,
    }


def import_schedules_data(payload: dict | str) -> dict:
    raw_lines = _parse_lines(payload)
    parsed_rows = [
        _parse_schedule_line(line_no=line_no, line=line)
        for line_no, line in raw_lines
    ]
    schedules_payload: list[dict] = []
    for item in parsed_rows:
        schedule_payload = dict(item["schedule"])
        schedule_payload["_sourceLine"] = int(item["lineNo"])
        schedules_payload.append(schedule_payload)

    schedules_result = create_schedules_data(schedules_payload)

    return {
        "summary": {
            "totalLines": len(raw_lines),
            "parsedLines": len(parsed_rows),
            "schedulesUpserted": int(schedules_result.get("inserted") or 0),
            "scheduleWeeksUpserted": int(
                schedules_result.get("scheduleWeeksUpserted") or 0
            ),
        }
    }
