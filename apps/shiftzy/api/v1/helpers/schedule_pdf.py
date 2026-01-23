from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
import ast
import json
from pathlib import Path

from fpdf import FPDF

from apps.shiftzy.api.v1.helpers.config import get_schedule_sections
from shared.tenant import get_env


_LINE_HEIGHT = 4
_CELL_PADDING_X = 0.3
_CELL_PADDING_TOP = 2.0
_CELL_PADDING_BOTTOM = 2.0
_FOOTER_HEIGHT = 6
_FOOTER_TEXT = "Staffs are expected to come on time. Internal used only"
_NOTE_COLOR = (130, 130, 130)
_SECTION_FONT_SIZE = 14
_SECTION_HEIGHT = 14
_NAME_COL_WIDTH = 20
_ICON_SIZE = 3.2
_ICON_GAP = 0.6
_ICON_EXTS = (".png", ".jpg", ".jpeg")


def _coerce_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value.strip())
    return None


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    return text.encode("latin-1", "replace").decode("latin-1")


def _limit_lines(lines: list[str], max_lines: int) -> list[str]:
    if len(lines) <= max_lines:
        return lines
    return lines[: max_lines - 1] + ["..."]


def _normalize_orientation(value: str | None) -> str:
    if value is None:
        return "L"
    text = str(value).strip().lower()
    if text in {"l", "landscape", "lanscape"}:
        return "L"
    if text in {"p", "portrait"}:
        return "P"
    raise ValueError("orientation must be 'portrait' or 'landscape'")


def _parse_env_float(key: str, default: float) -> float:
    raw = get_env(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(str(raw).strip())
    except ValueError as exc:
        raise ValueError(f"Invalid {key} value") from exc


def _parse_env_text(key: str, default: str) -> str:
    raw = get_env(key)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw)


def _parse_env_color(key: str, default: tuple[int, int, int]) -> tuple[int, int, int]:
    raw = get_env(key)
    if raw is None or str(raw).strip() == "":
        return default
    text = str(raw).strip()
    parsed = None
    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(text)
        except Exception:
            parsed = None
        if isinstance(parsed, (list, tuple)) and len(parsed) == 3:
            break
    if parsed is None:
        parts = [p for p in text.replace(",", " ").split() if p]
        if len(parts) == 3:
            parsed = parts

    if not isinstance(parsed, (list, tuple)) or len(parsed) != 3:
        raise ValueError(f"Invalid {key} value")

    try:
        values = tuple(int(float(item)) for item in parsed)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {key} value") from exc

    if any(v < 0 or v > 255 for v in values):
        raise ValueError(f"Invalid {key} value")
    return values


def _parse_env_json_map(key: str, default: dict[str, str]) -> dict[str, str]:
    raw = get_env(key)
    if raw is None or str(raw).strip() == "":
        return default
    text = str(raw).strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(text)
        except (SyntaxError, ValueError) as exc:
            raise ValueError(f"Invalid {key} value") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"Invalid {key} value")
    return {str(k): str(v) for k, v in parsed.items()}


def _load_pdf_config() -> dict[str, object]:
    return {
        "position_icon_map": _parse_env_json_map("_POSITION_ICON_MAP", {}),
        "position_icon_base_path": _parse_env_text("_POSITION_ICON_BASE_PATH", ""),
        "icon_size": _parse_env_float("_ICON_SIZE", _ICON_SIZE),
        "icon_gap": _parse_env_float("_ICON_GAP", _ICON_GAP),
        "line_height": _parse_env_float("_LINE_HEIGHT", _LINE_HEIGHT),
        "cell_padding_x": _parse_env_float("_CELL_PADDING_X", _CELL_PADDING_X),
        "cell_padding_top": _parse_env_float("_CELL_PADDING_TOP", _CELL_PADDING_TOP),
        "cell_padding_bottom": _parse_env_float(
            "_CELL_PADDING_BOTTOM",
            _CELL_PADDING_BOTTOM,
        ),
        "footer_height": _parse_env_float("_FOOTER_HEIGHT", _FOOTER_HEIGHT),
        "footer_text": _parse_env_text("_FOOTER_TEXT", _FOOTER_TEXT),
        "note_color": _parse_env_color("_NOTE_COLOR", _NOTE_COLOR),
        "section_font_size": _parse_env_float("_SECTION_FONT_SIZE", _SECTION_FONT_SIZE),
        "section_height": _parse_env_float("_SECTION_HEIGHT", _SECTION_HEIGHT),
        "name_col_width": _parse_env_float("_NAME_COL_WIDTH", _NAME_COL_WIDTH),
    }


def _resolve_base_path(value: str | None) -> Path | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path
    repo_root = _find_repo_root(Path(__file__).resolve())
    candidates = [Path.cwd() / path]
    if repo_root is not None:
        candidates.append(repo_root / path)
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return candidates[0]


def _find_repo_root(start: Path) -> Path | None:
    current = start
    for parent in [current] + list(current.parents):
        if (parent / "fastapi").is_dir():
            return parent
    return None


def _resolve_icon_path(
    code: str | None,
    *,
    icon_map: dict[str, str],
    icon_base_path: Path | None,
) -> str | None:
    if not code:
        return None
    key = str(code).strip()
    if not key:
        return None

    if key in icon_map:
        raw = icon_map.get(key)
        if raw is None:
            return None
        path = Path(str(raw))
        if not path.is_absolute():
            base = icon_base_path or Path.cwd()
            path = base / path
        return str(path) if path.is_file() else None

    if icon_base_path:
        for ext in _ICON_EXTS:
            candidate = icon_base_path / f"{key}{ext}"
            if candidate.is_file():
                return str(candidate)

    return None


def _build_cell_lines(
    rows: list[dict],
    *,
    icon_resolver,
) -> list[tuple[str, str, str | None]]:
    lines: list[tuple[str, str, str | None]] = []
    for idx, row in enumerate(rows):
        if idx > 0:
            lines.append(("", "DIVIDER", None))
        shift_name = row.get("shift_name")
        if shift_name:
            lines.append((str(shift_name), "SHIFT", None))
        time_line = ""
        start = row.get("start_time")
        end = row.get("end_time")
        if start and end:
            time_line = f"{start}-{end}"
        elif start:
            time_line = str(start)
        elif end:
            time_line = str(end)
        if time_line:
            lines.append((time_line, "TIME", None))

        position_name = row.get("position_name")
        if position_name:
            icon_path = icon_resolver(row.get("position_code"))
            lines.append((str(position_name), "POSITION", icon_path))

        note = row.get("note")
        if note:
            lines.append((str(note), "NOTE", None))

    return lines


def _split_text_lines(
    pdf: FPDF,
    text: str,
    width: float,
    line_height: float,
) -> list[str]:
    if text == "":
        return [""]

    lines: list[str] = []
    for raw_line in text.splitlines() or [""]:
        if raw_line == "":
            lines.append("")
            continue
        try:
            x, y = pdf.get_x(), pdf.get_y()
            wrapped = pdf.multi_cell(
                width,
                line_height,
                raw_line,
                border=0,
                align="L",
                split_only=True,
            )
            pdf.set_xy(x, y)
        except TypeError:
            wrapped = _wrap_text_manual(pdf, raw_line, width)
        lines.extend(wrapped or [""])
    return lines


def _wrap_text_manual(pdf: FPDF, text: str, width: float) -> list[str]:
    words = text.split()
    if not words:
        return [""]
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = word if not current else f"{current} {word}"
        if pdf.get_string_width(candidate) <= width:
            current = candidate
            continue
        if current:
            lines.append(current)
            current = word
        else:
            lines.append(word)
            current = ""
    if current:
        lines.append(current)
    return lines


def _wrap_plain_lines(
    pdf: FPDF,
    text: str,
    width: float,
    line_height: float,
) -> list[str]:
    safe_text = _safe_text(text)
    return _split_text_lines(pdf, safe_text, width, line_height)


def _truncate_text(pdf: FPDF, text: str, width: float) -> str:
    if text == "":
        return ""
    if pdf.get_string_width(text) <= width:
        return text
    ellipsis = "..."
    max_width = width - pdf.get_string_width(ellipsis)
    if max_width <= 0:
        return ellipsis
    truncated = ""
    for char in text:
        if pdf.get_string_width(truncated + char) > max_width:
            break
        truncated += char
    return f"{truncated}{ellipsis}" if truncated else ellipsis


def _wrap_styled_lines(
    pdf: FPDF,
    lines: list[tuple[str, str, str | None]],
    width: float,
    line_height: float,
    note_color: tuple[int, int, int],
    icon_size: float,
    icon_gap: float,
) -> list[tuple[str, str, str | None]]:
    wrapped: list[tuple[str, str, str | None]] = []
    base_family = pdf.font_family
    base_style = pdf.font_style
    base_size = pdf.font_size_pt

    for text, style, icon_path in lines:
        if style == "DIVIDER":
            wrapped.append(("", "DIVIDER", None))
            continue
        if style == "TIME":
            safe_text = _safe_text(text)
            wrapped.append((safe_text, style, None))
            continue
        if style == "SHIFT":
            safe_text = _safe_text(text).replace("\n", " ")
            font_style, font_size, _ = _style_settings(base_size, style, note_color)
            pdf.set_font(base_family, font_style, font_size)
            wrapped.append((_truncate_text(pdf, safe_text, width), style, None))
            continue
        if style == "POSITION":
            safe_text = _safe_text(text).replace("\n", " ")
            font_style, font_size, _ = _style_settings(base_size, style, note_color)
            pdf.set_font(base_family, font_style, font_size)
            usable_width = width - (icon_size + icon_gap) if icon_path else width
            wrapped_lines = _split_text_lines(pdf, safe_text, usable_width, line_height)
            for idx, line in enumerate(wrapped_lines):
                if icon_path:
                    token = icon_path if idx == 0 else ""
                else:
                    token = None
                wrapped.append((line, style, token))
            continue
        if text is None:
            continue
        safe_text = _safe_text(text)
        font_style, font_size, _ = _style_settings(base_size, style, note_color)
        pdf.set_font(base_family, font_style, font_size)
        for segment in safe_text.splitlines() or [""]:
            if segment == "":
                wrapped.append(("", style, None))
                continue
            for line in _split_text_lines(pdf, segment, width, line_height):
                wrapped.append((line, style, None))

    pdf.set_font(base_family, base_style, base_size)
    return wrapped


def _draw_plain_cell(
    pdf: FPDF,
    x: float,
    y: float,
    width: float,
    height: float,
    lines: list[str],
    *,
    align: str = "C",
    fill: bool = False,
    border: bool = True,
    line_height: float = _LINE_HEIGHT,
    padding_x: float = _CELL_PADDING_X,
    padding_top: float = _CELL_PADDING_TOP,
    padding_bottom: float = _CELL_PADDING_BOTTOM,
) -> None:
    style = None
    if border and fill:
        style = "DF"
    elif border:
        style = "D"
    elif fill:
        style = "F"
    if style:
        pdf.rect(x, y, width, height, style=style)
    if not lines:
        return

    text_height = len(lines) * line_height
    available = height - padding_top - padding_bottom
    if available <= text_height:
        start_y = y + padding_top
    else:
        start_y = y + padding_top + (available - text_height) / 2

    current_y = start_y
    for line in lines:
        pdf.set_xy(x + padding_x, current_y)
        pdf.cell(width - 2 * padding_x, line_height, line, border=0, align=align)
        current_y += line_height


def _draw_styled_cell(
    pdf: FPDF,
    x: float,
    y: float,
    width: float,
    height: float,
    lines: list[tuple[str, str, str | None]],
    *,
    align: str = "C",
    fill: bool = False,
    line_height: float = _LINE_HEIGHT,
    padding_x: float = _CELL_PADDING_X,
    padding_top: float = _CELL_PADDING_TOP,
    padding_bottom: float = _CELL_PADDING_BOTTOM,
    note_color: tuple[int, int, int] = _NOTE_COLOR,
    icon_size: float = _ICON_SIZE,
    icon_gap: float = _ICON_GAP,
) -> None:
    style = "DF" if fill else "D"
    pdf.rect(x, y, width, height, style=style)
    if not lines:
        return

    text_height = len(lines) * line_height
    available = height - padding_top - padding_bottom
    if available <= text_height:
        start_y = y + padding_top
    else:
        start_y = y + padding_top + (available - text_height) / 2

    base_family = pdf.font_family
    base_style = pdf.font_style
    base_size = pdf.font_size_pt
    base_color = (0, 0, 0)
    current_y = start_y
    for text, style, icon_path in lines:
        if style == "DIVIDER":
            line_y = current_y + line_height / 2
            pdf.set_draw_color(140, 140, 140)
            pdf.line(x + padding_x, line_y, x + width - padding_x, line_y)
            pdf.set_draw_color(0, 0, 0)
            current_y += line_height
            continue

        font_style, font_size, color = _style_settings(base_size, style, note_color)
        pdf.set_font(base_family, font_style, font_size)
        pdf.set_text_color(*color)
        if style == "POSITION":
            reserve_icon = icon_path is not None
            extra = (icon_size + icon_gap) if reserve_icon else 0
            text_width = pdf.get_string_width(text)
            available = width - 2 * padding_x
            total_width = extra + text_width
            if align == "C":
                start_x = x + padding_x + max(0, (available - total_width) / 2)
            elif align == "R":
                start_x = x + width - padding_x - total_width
            else:
                start_x = x + padding_x

            if icon_path:
                icon_y = current_y + max(0, (line_height - icon_size) / 2)
                try:
                    pdf.image(icon_path, start_x, icon_y, w=icon_size, h=icon_size)
                except Exception:
                    pass

            text_x = start_x + extra
            pdf.set_xy(text_x, current_y)
            pdf.cell(width - (text_x - x) - padding_x, line_height, text, border=0, align="L")
        else:
            pdf.set_xy(x + padding_x, current_y)
            pdf.cell(width - 2 * padding_x, line_height, text, border=0, align=align)
        current_y += line_height
    pdf.set_font(base_family, base_style, base_size)
    pdf.set_text_color(*base_color)


def _style_settings(
    base_size: float,
    style: str,
    note_color: tuple[int, int, int],
) -> tuple[str, float, tuple[int, int, int]]:
    if style == "TIME":
        size = max(8, base_size - 1)
        return "B", size, (0, 0, 0)
    if style == "SHIFT":
        return "B", base_size, (0, 0, 0)
    if style == "NOTE":
        size = max(7, base_size - 1)
        return "I", size, note_color
    if style == "B":
        return "B", base_size, (0, 0, 0)
    return "", base_size, (0, 0, 0)


def _get_section_order(sections: dict[str, object]) -> list[str]:
    try:
        preferred = get_schedule_sections()
    except Exception:
        preferred = []

    ordered: list[str] = []
    seen: set[str] = set()
    for name in preferred:
        if name in sections:
            ordered.append(name)
            seen.add(name)

    for name in sorted(sections.keys()):
        if name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


class _SchedulePDF(FPDF):
    def __init__(self, *args, footer_text: str, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._footer_text = footer_text

    def footer(self) -> None:
        config = getattr(self, "_cfg", {}) or {}
        footer_height = float(config.get("footer_height", _FOOTER_HEIGHT))
        footer_text = str(config.get("footer_text", self._footer_text))
        self.set_y(-(self.b_margin - 2))
        self.set_font("Helvetica", "", 8)
        self.set_text_color(90, 90, 90)
        name_col_width = getattr(self, "_name_col_width", 0)
        day_col_width = getattr(self, "_day_col_width", 0)
        if name_col_width and day_col_width:
            self.set_x(self.l_margin + name_col_width)
            self.cell(
                day_col_width * 7,
                footer_height,
                _safe_text(footer_text),
                align="C",
            )
        else:
            self.cell(0, footer_height, _safe_text(footer_text), align="C")


def build_schedule_pdf(
    *,
    schedules: list[dict],
    week_info: dict,
    orientation: str | None = None,
) -> bytes:
    config = _load_pdf_config()
    line_height = float(config["line_height"])
    padding_x = float(config["cell_padding_x"])
    padding_top = float(config["cell_padding_top"])
    padding_bottom = float(config["cell_padding_bottom"])
    footer_height = float(config["footer_height"])
    note_color = config["note_color"]
    icon_map = config["position_icon_map"]
    icon_base_path = _resolve_base_path(config.get("position_icon_base_path"))
    icon_size = float(config["icon_size"])
    icon_gap = float(config["icon_gap"])

    week_start = _coerce_date(week_info.get("start_date"))
    week_end = _coerce_date(week_info.get("end_date"))
    week_no = week_info.get("week_no")
    if week_start is None or week_end is None:
        raise ValueError("week_info must include start_date and end_date")

    week_dates = [week_start + timedelta(days=i) for i in range(7)]
    week_date_set = {day for day in week_dates}

    section_map: dict[str, dict[str, dict[date, list[dict]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for row in schedules:
        row_date = _coerce_date(row.get("date"))
        if row_date is None or row_date not in week_date_set:
            continue
        section = row.get("schedule_section") or "Other"
        employee = row.get("employee_name") or row.get("employee_id") or "Unknown"
        section_map[str(section)][str(employee)][row_date].append(row)

    pdf_orientation = _normalize_orientation(orientation)
    pdf = _SchedulePDF(
        orientation=pdf_orientation,
        unit="mm",
        format="Letter",
        footer_text=str(config["footer_text"]),
    )
    pdf._cfg = config
    margin = 6
    pdf.set_margins(margin, margin, margin)
    pdf.set_auto_page_break(False, margin + footer_height)
    pdf.add_page()

    page_width = getattr(pdf, "epw", pdf.w - margin * 2)
    name_col_width = float(config["name_col_width"])
    day_col_width = (page_width - name_col_width) / 7
    pdf._name_col_width = name_col_width
    pdf._day_col_width = day_col_width

    def resolve_icon(code: object) -> str | None:
        if code is None:
            return None
        return _resolve_icon_path(
            str(code),
            icon_map=icon_map,
            icon_base_path=icon_base_path,
        )

    def draw_title() -> None:
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "B", 22)
        pdf.set_x(margin + name_col_width)
        pdf.cell(
            day_col_width * 7,
            9,
            _safe_text("Schedule"),
            align="C",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        pdf.set_font("Helvetica", "", 10)
        week_label = (
            f"Week {week_no} "
            f"{week_start.month}/{week_start.day}/{week_start.year}"
            f" - {week_end.month}/{week_end.day}/{week_end.year}"
        )
        pdf.set_x(margin + name_col_width)
        pdf.cell(
            day_col_width * 7,
            6,
            _safe_text(week_label),
            align="C",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        pdf.ln(2)

    def draw_table_header() -> None:
        header_height = 12
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(74, 144, 226)
        pdf.set_text_color(255, 255, 255)
        y = pdf.get_y()
        x = margin
        _draw_plain_cell(
            pdf,
            x,
            y,
            name_col_width,
            header_height,
            [],
            align="C",
            fill=False,
            border=False,
            line_height=line_height,
            padding_x=padding_x,
            padding_top=padding_top,
            padding_bottom=padding_bottom,
        )
        x += name_col_width
        for day in week_dates:
            label = f"{day.strftime('%A')}\n{day.month}/{day.day}"
            header_lines = _wrap_plain_lines(
                pdf,
                label,
                day_col_width - 2 * padding_x,
                line_height,
            )
            _draw_plain_cell(
                pdf,
                x,
                y,
                day_col_width,
                header_height,
                header_lines,
                align="C",
                fill=True,
                line_height=line_height,
                padding_x=padding_x,
                padding_top=padding_top,
                padding_bottom=padding_bottom,
            )
            x += day_col_width
        pdf.set_y(y + header_height)
        pdf.set_text_color(0, 0, 0)

    def ensure_space(height: float) -> None:
        if pdf.get_y() + height > pdf.h - pdf.b_margin:
            pdf.add_page()
            draw_title()
            draw_table_header()

    draw_title()
    draw_table_header()

    if not section_map:
        ensure_space(12)
        pdf.set_font("Helvetica", "", 10)
        pdf.set_font("Helvetica", "", 10)
        empty_lines = _wrap_plain_lines(
            pdf,
            "No schedules found for this week.",
            day_col_width * 7 - 2 * padding_x,
            line_height,
        )
        row_y = pdf.get_y()
        _draw_plain_cell(
            pdf,
            margin,
            row_y,
            name_col_width,
            12,
            [],
            align="C",
            border=False,
            line_height=line_height,
            padding_x=padding_x,
            padding_top=padding_top,
            padding_bottom=padding_bottom,
        )
        _draw_plain_cell(
            pdf,
            margin + name_col_width,
            row_y,
            day_col_width * 7,
            12,
            empty_lines,
            align="C",
            line_height=line_height,
            padding_x=padding_x,
            padding_top=padding_top,
            padding_bottom=padding_bottom,
        )
        pdf.set_y(row_y + 12)
    else:
        ordered_sections = _get_section_order(section_map)
        for section in ordered_sections:
            section_height = float(config["section_height"])
            ensure_space(section_height)
            pdf.set_font("Helvetica", "B", float(config["section_font_size"]))
            section_lines = _wrap_plain_lines(
                pdf,
                str(section),
                day_col_width * 7 - 2 * padding_x,
                line_height,
            )
            row_y = pdf.get_y()
            _draw_plain_cell(
                pdf,
                margin,
                row_y,
                name_col_width,
                section_height,
                [],
                align="C",
                fill=False,
                border=False,
                line_height=line_height,
                padding_x=padding_x,
                padding_top=padding_top,
                padding_bottom=padding_bottom,
            )
            _draw_plain_cell(
                pdf,
                margin + name_col_width,
                row_y,
                day_col_width * 7,
                section_height,
                section_lines,
                align="C",
                fill=False,
                border=False,
                line_height=line_height,
                padding_x=padding_x,
                padding_top=padding_top,
                padding_bottom=padding_bottom,
            )
            pdf.set_y(row_y + section_height)

            employees = sorted(
                section_map[section].keys(),
                key=lambda name: str(name).lower(),
            )
            for employee in employees:
                entries_by_day = section_map[section][employee]
                cell_lines: list[list[tuple[str, str, str | None]]] = []
                line_counts: list[int] = []
                pdf.set_font("Helvetica", "", 9)
                for day in week_dates:
                    raw_lines = _build_cell_lines(
                        entries_by_day.get(day, []),
                        icon_resolver=resolve_icon,
                    )
                    wrapped = _wrap_styled_lines(
                        pdf,
                        raw_lines,
                        day_col_width - 2 * padding_x,
                        line_height,
                        note_color,
                        icon_size,
                        icon_gap,
                    )
                    cell_lines.append(wrapped)
                    line_counts.append(max(1, len(wrapped)))

                pdf.set_font("Helvetica", "B", 9)
                name_lines = _wrap_plain_lines(
                    pdf,
                    str(employee),
                    name_col_width - 2 * padding_x,
                    line_height,
                )
                line_counts.append(max(1, len(name_lines)))

                max_lines = max(line_counts + [1])
                row_height = max(
                    12,
                    line_height * max_lines + padding_top + padding_bottom
                    + 2,
                )

                ensure_space(row_height)
                row_y = pdf.get_y()

                pdf.set_font("Helvetica", "B", 9)
                _draw_plain_cell(
                    pdf,
                    margin,
                    row_y,
                    name_col_width,
                    row_height,
                    name_lines,
                    align="L",
                    border=False,
                    line_height=line_height,
                    padding_x=padding_x,
                    padding_top=padding_top,
                    padding_bottom=padding_bottom,
                )
                x = margin + name_col_width
                pdf.set_font("Helvetica", "", 9)
                for lines in cell_lines:
                    _draw_styled_cell(
                        pdf,
                        x,
                        row_y,
                        day_col_width,
                        row_height,
                        lines,
                        align="C",
                        line_height=line_height,
                        padding_x=padding_x,
                        padding_top=padding_top,
                        padding_bottom=padding_bottom,
                        note_color=note_color,
                        icon_size=icon_size,
                        icon_gap=icon_gap,
                    )
                    x += day_col_width
                pdf.set_y(row_y + row_height)

            if section != ordered_sections[-1]:
                spacer_height = 4
                ensure_space(spacer_height)
                pdf.set_y(pdf.get_y() + spacer_height)

    pdf_bytes = pdf.output(dest="S")
    if isinstance(pdf_bytes, str):
        return pdf_bytes.encode("latin-1")
    return bytes(pdf_bytes)
