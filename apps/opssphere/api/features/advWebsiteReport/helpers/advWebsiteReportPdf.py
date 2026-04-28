from __future__ import annotations

import calendar
from pathlib import Path

from fpdf import FPDF

_FONT_FAMILY = "Helvetica"
_TITLE_TEXT = "Advanced Website Report"

_TEXT_COLOR = (0, 0, 0)
_TABLE_BORDER_COLOR = (214, 214, 214)
_TABLE_HEADER_BG_COLOR = (224, 235, 255)
_TABLE_CONTEXT_BG_COLOR = (239, 245, 255)
_SECTION_BG_COLOR = (51, 119, 255)
_SECTION_TEXT_COLOR = (255, 255, 255)

_PAGE_WIDTH_MM = 297.0
_PAGE_MIN_HEIGHT_MM = 210.0

_MARGIN_LEFT = 12.0
_MARGIN_TOP = 12.0
_MARGIN_RIGHT = 12.0
_MARGIN_BOTTOM = 12.0

_GRID_COLUMNS = 3
_GRID_COLUMN_GAP = 6.0
_LEFT_COLUMN_INDEX = 0
_MIDDLE_COLUMN_INDEX = 1
_GRID_ROW_GAP = 6.0

_REPORT_TITLE_HEIGHT = 7.0
_REPORT_META_HEIGHT = 5.0
_REPORT_AFTER_GAP = 8.0
_LOGO_WIDTH_MM = 30.49
_LOGO_HEIGHT_MM = 5.0
_LOGO_AFTER_GAP = 2.0
_LOGO_CANDIDATE_PATHS = (
    Path(__file__).resolve().parents[6] / "static" / "taaa_logo.png",
)

_COLUMN_HEADER_HEIGHT = 7.0
_COLUMN_HEADER_AFTER_GAP = 1.0
_COLUMN_CONTEXT_HEIGHT = 6.0
_COLUMN_CONTEXT_AFTER_GAP = 2.0

_SECTION_HEADER_HEIGHT = 6.8
_TABLE_HEADER_HEIGHT = 6.2
_TABLE_ROW_MIN_HEIGHT = 5.6
_CELL_TEXT_LINE_HEIGHT = 3.8
_CELL_PADDING_X = 0.8
_CELL_PADDING_Y = 0.7


class _AdvWebsiteReportPDF(FPDF):
    def footer(self) -> None:
        self.set_y(-8)
        self.set_font(_FONT_FAMILY, "", 7)
        self.set_text_color(*_TEXT_COLOR)
        self.cell(0, 4, f"{self.page_no()}/{{nb}}", border=0, ln=0, align="R")


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).encode("latin-1", "replace").decode("latin-1")


def _get_logo_path() -> Path | None:
    for path in _LOGO_CANDIDATE_PATHS:
        if path.exists():
            return path
    return None


def _header_logo_block_height() -> float:
    return (_LOGO_HEIGHT_MM + _LOGO_AFTER_GAP) if _get_logo_path() else 0.0


def _split_long_token(pdf: FPDF, token: str, max_width: float) -> list[str]:
    if token == "":
        return [""]

    pieces: list[str] = []
    current = ""
    for char in token:
        candidate = f"{current}{char}"
        if current and pdf.get_string_width(candidate) > max_width:
            pieces.append(current)
            current = char
        else:
            current = candidate
    if current:
        pieces.append(current)
    return pieces or [""]


def _wrap_text_manual(pdf: FPDF, text: str, max_width: float) -> list[str]:
    if text == "":
        return [""]

    wrapped: list[str] = []
    for raw_line in text.splitlines() or [""]:
        if raw_line == "":
            wrapped.append("")
            continue
        words = raw_line.split(" ")
        current = ""
        for word in words:
            candidate = word if not current else f"{current} {word}"
            if pdf.get_string_width(candidate) <= max_width:
                current = candidate
                continue
            if current:
                wrapped.append(current)
                current = ""
            if pdf.get_string_width(word) <= max_width:
                current = word
                continue
            wrapped.extend(_split_long_token(pdf, word, max_width))
        if current or not words:
            wrapped.append(current)
    return wrapped or [""]


def _split_text_lines(pdf: FPDF, text: str, width: float, line_height: float) -> list[str]:
    safe = _safe_text(text)
    if safe == "":
        return [""]
    try:
        x, y = pdf.get_x(), pdf.get_y()
        lines = pdf.multi_cell(
            width,
            line_height,
            safe,
            border=0,
            align="L",
            split_only=True,
        )
        pdf.set_xy(x, y)
        if isinstance(lines, list) and lines:
            return [str(line) for line in lines]
    except TypeError:
        pass
    return _wrap_text_manual(pdf, safe, width)


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


def _column_geometry(pdf: FPDF, *, column_index: int) -> tuple[float, float]:
    total_width = pdf.w - pdf.l_margin - pdf.r_margin
    column_width = (total_width - ((_GRID_COLUMNS - 1) * _GRID_COLUMN_GAP)) / _GRID_COLUMNS
    column_x = pdf.l_margin + (column_index * (column_width + _GRID_COLUMN_GAP))
    return column_x, column_width


def _build_wrapped_row_layouts(
    pdf: FPDF,
    *,
    rows: list[dict[str, object]],
    text_key: str,
    text_col_width: float,
) -> list[dict[str, object]]:
    layouts: list[dict[str, object]] = []
    if not rows:
        return layouts

    text_width = max(4.0, text_col_width - (_CELL_PADDING_X * 2))
    pdf.set_font(_FONT_FAMILY, "", 9)

    for row in rows:
        lines = _split_text_lines(
            pdf,
            str(row.get(text_key) or ""),
            text_width,
            _CELL_TEXT_LINE_HEIGHT,
        )
        lines = lines or [""]
        row_height = max(
            _TABLE_ROW_MIN_HEIGHT,
            (len(lines) * _CELL_TEXT_LINE_HEIGHT) + (_CELL_PADDING_Y * 2),
        )
        layouts.append(
            {
                "lines": lines,
                "clicks": int(row.get("clicks") or 0),
                "height": float(row_height),
            }
        )
    return layouts


def _build_submenu_group_layouts(
    pdf: FPDF,
    *,
    groups: list[dict[str, object]],
    submenu_col_width: float,
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    text_width = max(4.0, submenu_col_width - (_CELL_PADDING_X * 2))
    pdf.set_font(_FONT_FAMILY, "", 9)

    for group in groups:
        mega_menu = str(group.get("megaMenu") or "").strip() or "(Unknown)"
        raw_rows = group.get("rows")
        rows = raw_rows if isinstance(raw_rows, list) else []
        row_layouts: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            lines = _split_text_lines(
                pdf,
                str(row.get("subMenu") or ""),
                text_width,
                _CELL_TEXT_LINE_HEIGHT,
            )
            lines = lines or [""]
            row_height = max(
                _TABLE_ROW_MIN_HEIGHT,
                (len(lines) * _CELL_TEXT_LINE_HEIGHT) + (_CELL_PADDING_Y * 2),
            )
            row_layouts.append(
                {
                    "lines": lines,
                    "clicks": int(row.get("clicks") or 0),
                    "height": float(row_height),
                }
            )
        if not row_layouts:
            row_layouts.append(
                {
                    "lines": ["No data"],
                    "clicks": 0,
                    "height": _TABLE_ROW_MIN_HEIGHT,
                }
            )
        out.append(
            {
                "megaMenu": mega_menu,
                "rows": row_layouts,
                "groupHeight": sum(float(item.get("height") or 0.0) for item in row_layouts),
            }
        )
    return out


def _estimate_two_col_card_height(
    pdf: FPDF,
    *,
    width: float,
    rows: list[dict[str, object]],
    text_key: str,
) -> float:
    text_col_width = width * 0.74
    layouts = _build_wrapped_row_layouts(
        pdf,
        rows=rows,
        text_key=text_key,
        text_col_width=text_col_width,
    )
    rows_height = (
        sum(float(item.get("height") or 0.0) for item in layouts)
        if layouts
        else _TABLE_ROW_MIN_HEIGHT
    )
    return _SECTION_HEADER_HEIGHT + _TABLE_HEADER_HEIGHT + rows_height


def _estimate_submenu_card_height(
    pdf: FPDF,
    *,
    width: float,
    groups: list[dict[str, object]],
) -> float:
    mega_col = width * 0.25
    submenu_col = width * 0.55
    _ = mega_col
    layouts = _build_submenu_group_layouts(
        pdf,
        groups=groups,
        submenu_col_width=submenu_col,
    )
    rows_height = (
        sum(float(group.get("groupHeight") or 0.0) for group in layouts)
        if layouts
        else _TABLE_ROW_MIN_HEIGHT
    )
    return _SECTION_HEADER_HEIGHT + _TABLE_HEADER_HEIGHT + rows_height


def _column_header_block_height() -> float:
    return (
        _COLUMN_HEADER_HEIGHT
        + _COLUMN_HEADER_AFTER_GAP
        + _COLUMN_CONTEXT_HEIGHT
        + _COLUMN_CONTEXT_AFTER_GAP
    )


def _resolve_dynamic_page_height(
    *,
    sections: list[dict[str, object]],
    menu_sections: dict[str, object],
) -> float:
    probe = _AdvWebsiteReportPDF(
        orientation="L",
        unit="mm",
        format=(_PAGE_MIN_HEIGHT_MM, _PAGE_WIDTH_MM),
    )
    probe.set_margins(_MARGIN_LEFT, _MARGIN_TOP, _MARGIN_RIGHT)
    probe.add_page()

    _, cta_width = _column_geometry(probe, column_index=_MIDDLE_COLUMN_INDEX)
    _, menu_width = _column_geometry(probe, column_index=_LEFT_COLUMN_INDEX)

    valid_cta_sections = [
        item
        for item in sections
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]
    cta_height = _column_header_block_height()
    for index, section in enumerate(valid_cta_sections):
        raw_rows = section.get("rows")
        cta_rows = raw_rows if isinstance(raw_rows, list) else []
        cta_height += _estimate_two_col_card_height(
            probe,
            width=cta_width,
            rows=cta_rows,
            text_key="buttonText",
        )
        if index < len(valid_cta_sections) - 1:
            cta_height += _GRID_ROW_GAP

    mega_rows_raw = menu_sections.get("megaMenuRows")
    menu_mega_rows = mega_rows_raw if isinstance(mega_rows_raw, list) else []
    sub_groups_raw = menu_sections.get("subMenuGroups")
    menu_sub_groups = sub_groups_raw if isinstance(sub_groups_raw, list) else []
    menu_height = _column_header_block_height()
    menu_height += _estimate_two_col_card_height(
        probe,
        width=menu_width,
        rows=menu_mega_rows,
        text_key="megaMenu",
    )
    menu_height += _GRID_ROW_GAP
    menu_height += _estimate_submenu_card_height(
        probe,
        width=menu_width,
        groups=menu_sub_groups,
    )

    content_height = (
        _header_logo_block_height()
        + _REPORT_TITLE_HEIGHT
        + _REPORT_META_HEIGHT
        + _REPORT_AFTER_GAP
        + max(menu_height, cta_height)
    )
    resolved = _MARGIN_TOP + content_height + _MARGIN_BOTTOM + 2.0
    return max(_PAGE_MIN_HEIGHT_MM, resolved)


def _draw_report_header(
    pdf: FPDF,
    *,
    account_code: str,
    month: int,
    year: int,
    period_label: str,
    timezone: str,
) -> None:
    logo_path = _get_logo_path()
    if logo_path is not None:
        logo_y = pdf.get_y()
        logo_x = (pdf.w - _LOGO_WIDTH_MM) / 2.0
        try:
            pdf.image(
                str(logo_path),
                x=logo_x,
                y=logo_y,
                w=_LOGO_WIDTH_MM,
                h=_LOGO_HEIGHT_MM,
            )
            pdf.set_y(logo_y + _LOGO_HEIGHT_MM + _LOGO_AFTER_GAP)
        except Exception:
            pdf.set_y(logo_y)

    pdf.set_font(_FONT_FAMILY, "B", 14)
    pdf.set_text_color(*_TEXT_COLOR)
    pdf.cell(0, _REPORT_TITLE_HEIGHT, _safe_text(_TITLE_TEXT), ln=1, align="C")

    resolved_period_label = str(period_label or "").strip() or f"{calendar.month_name[month]} {year}"
    meta_line = (
        f"Account: {_safe_text(account_code)} | "
        f"Period: {_safe_text(resolved_period_label)} | Timezone: {_safe_text(timezone)}"
    )
    pdf.set_font(_FONT_FAMILY, "", 9)
    pdf.cell(0, _REPORT_META_HEIGHT, _safe_text(meta_line), ln=1, align="C")
    pdf.ln(_REPORT_AFTER_GAP)


def _draw_column_header(
    pdf: FPDF,
    *,
    x: float,
    y: float,
    width: float,
    title: str,
    context_text: str,
) -> float:
    _draw_filled_rounded_rect(
        pdf,
        x=x,
        y=y,
        w=width,
        h=_COLUMN_HEADER_HEIGHT,
        radius=1.2,
        color=_SECTION_BG_COLOR,
    )
    pdf.set_text_color(*_SECTION_TEXT_COLOR)
    pdf.set_font(_FONT_FAMILY, "B", 11)
    pdf.set_xy(x + 1.2, y)
    pdf.cell(width - 2.4, _COLUMN_HEADER_HEIGHT, _safe_text(title), border=0, ln=1, align="L")

    context_y = y + _COLUMN_HEADER_HEIGHT + _COLUMN_HEADER_AFTER_GAP
    pdf.set_xy(x, context_y)
    pdf.set_draw_color(*_TABLE_BORDER_COLOR)
    pdf.set_fill_color(*_TABLE_CONTEXT_BG_COLOR)
    pdf.set_text_color(*_TEXT_COLOR)
    pdf.set_font(_FONT_FAMILY, "", 9)
    pdf.cell(
        width,
        _COLUMN_CONTEXT_HEIGHT,
        _safe_text(context_text),
        border=1,
        ln=1,
        align="L",
        fill=True,
    )
    return context_y + _COLUMN_CONTEXT_HEIGHT + _COLUMN_CONTEXT_AFTER_GAP


def _draw_two_col_card(
    pdf: FPDF,
    *,
    x: float,
    y: float,
    width: float,
    section_name: str,
    left_header: str,
    text_key: str,
    rows: list[dict[str, object]],
) -> float:
    text_col_width = width * 0.74
    clicks_col_width = width - text_col_width
    row_layouts = _build_wrapped_row_layouts(
        pdf,
        rows=rows,
        text_key=text_key,
        text_col_width=text_col_width,
    )

    _draw_filled_rounded_rect(
        pdf,
        x=x,
        y=y,
        w=width,
        h=_SECTION_HEADER_HEIGHT,
        radius=1.0,
        color=_SECTION_BG_COLOR,
    )
    pdf.set_text_color(*_SECTION_TEXT_COLOR)
    pdf.set_font(_FONT_FAMILY, "B", 10)
    pdf.set_xy(x + 1.0, y)
    pdf.cell(width - 2.0, _SECTION_HEADER_HEIGHT, _safe_text(section_name), border=0, ln=1, align="L")

    header_y = y + _SECTION_HEADER_HEIGHT
    pdf.set_xy(x, header_y)
    pdf.set_draw_color(*_TABLE_BORDER_COLOR)
    pdf.set_fill_color(*_TABLE_HEADER_BG_COLOR)
    pdf.set_text_color(*_TEXT_COLOR)
    pdf.set_font(_FONT_FAMILY, "B", 9)
    pdf.cell(text_col_width, _TABLE_HEADER_HEIGHT, _safe_text(left_header), border=1, align="L", fill=True)
    pdf.cell(
        clicks_col_width,
        _TABLE_HEADER_HEIGHT,
        "Clicks",
        border=1,
        ln=1,
        align="R",
        fill=True,
    )

    current_y = header_y + _TABLE_HEADER_HEIGHT
    pdf.set_font(_FONT_FAMILY, "", 9)
    if not row_layouts:
        pdf.set_xy(x, current_y)
        pdf.cell(text_col_width, _TABLE_ROW_MIN_HEIGHT, "No data", border=1, align="L")
        pdf.cell(clicks_col_width, _TABLE_ROW_MIN_HEIGHT, "0", border=1, ln=1, align="R")
        return _SECTION_HEADER_HEIGHT + _TABLE_HEADER_HEIGHT + _TABLE_ROW_MIN_HEIGHT

    for row in row_layouts:
        lines = [str(line) for line in (row.get("lines") or [""])]
        row_height = float(row.get("height") or _TABLE_ROW_MIN_HEIGHT)
        clicks_value = int(row.get("clicks") or 0)

        pdf.rect(x, current_y, text_col_width, row_height, style="D")
        pdf.rect(x + text_col_width, current_y, clicks_col_width, row_height, style="D")

        for line_index, line_text in enumerate(lines):
            line_y = current_y + _CELL_PADDING_Y + (line_index * _CELL_TEXT_LINE_HEIGHT)
            if line_y + _CELL_TEXT_LINE_HEIGHT > current_y + row_height:
                break
            pdf.set_xy(x + _CELL_PADDING_X, line_y)
            pdf.cell(
                text_col_width - (_CELL_PADDING_X * 2),
                _CELL_TEXT_LINE_HEIGHT,
                _safe_text(line_text),
                border=0,
                ln=0,
                align="L",
            )

        click_y = current_y + ((row_height - _CELL_TEXT_LINE_HEIGHT) / 2.0)
        pdf.set_xy(x + text_col_width + _CELL_PADDING_X, click_y)
        pdf.cell(
            clicks_col_width - (_CELL_PADDING_X * 2),
            _CELL_TEXT_LINE_HEIGHT,
            f"{clicks_value:,}",
            border=0,
            ln=0,
            align="R",
        )
        current_y += row_height

    return current_y - y


def _draw_submenu_card(
    pdf: FPDF,
    *,
    x: float,
    y: float,
    width: float,
    section_name: str,
    groups: list[dict[str, object]],
) -> float:
    mega_col_width = width * 0.25
    submenu_col_width = width * 0.55
    clicks_col_width = width - mega_col_width - submenu_col_width
    group_layouts = _build_submenu_group_layouts(
        pdf,
        groups=groups,
        submenu_col_width=submenu_col_width,
    )

    _draw_filled_rounded_rect(
        pdf,
        x=x,
        y=y,
        w=width,
        h=_SECTION_HEADER_HEIGHT,
        radius=1.0,
        color=_SECTION_BG_COLOR,
    )
    pdf.set_text_color(*_SECTION_TEXT_COLOR)
    pdf.set_font(_FONT_FAMILY, "B", 10)
    pdf.set_xy(x + 1.0, y)
    pdf.cell(width - 2.0, _SECTION_HEADER_HEIGHT, _safe_text(section_name), border=0, ln=1, align="L")

    header_y = y + _SECTION_HEADER_HEIGHT
    pdf.set_xy(x, header_y)
    pdf.set_draw_color(*_TABLE_BORDER_COLOR)
    pdf.set_fill_color(*_TABLE_HEADER_BG_COLOR)
    pdf.set_text_color(*_TEXT_COLOR)
    pdf.set_font(_FONT_FAMILY, "B", 9)
    pdf.cell(mega_col_width, _TABLE_HEADER_HEIGHT, "Megamenu", border=1, align="L", fill=True)
    pdf.cell(submenu_col_width, _TABLE_HEADER_HEIGHT, "Submenus", border=1, align="L", fill=True)
    pdf.cell(clicks_col_width, _TABLE_HEADER_HEIGHT, "Clicks", border=1, ln=1, align="R", fill=True)

    current_y = header_y + _TABLE_HEADER_HEIGHT
    pdf.set_font(_FONT_FAMILY, "", 9)
    if not group_layouts:
        pdf.set_xy(x, current_y)
        pdf.cell(mega_col_width, _TABLE_ROW_MIN_HEIGHT, "(Unknown)", border=1, align="L")
        pdf.cell(submenu_col_width, _TABLE_ROW_MIN_HEIGHT, "No data", border=1, align="L")
        pdf.cell(clicks_col_width, _TABLE_ROW_MIN_HEIGHT, "0", border=1, ln=1, align="R")
        return _SECTION_HEADER_HEIGHT + _TABLE_HEADER_HEIGHT + _TABLE_ROW_MIN_HEIGHT

    for group in group_layouts:
        mega_name = str(group.get("megaMenu") or "(Unknown)")
        rows = group.get("rows")
        row_layouts = rows if isinstance(rows, list) else []
        group_height = float(group.get("groupHeight") or _TABLE_ROW_MIN_HEIGHT)

        pdf.rect(x, current_y, mega_col_width, group_height, style="D")
        pdf.set_xy(x + _CELL_PADDING_X, current_y + ((group_height - _CELL_TEXT_LINE_HEIGHT) / 2.0))
        pdf.set_font(_FONT_FAMILY, "B", 9)
        pdf.cell(
            mega_col_width - (_CELL_PADDING_X * 2),
            _CELL_TEXT_LINE_HEIGHT,
            _safe_text(mega_name),
            border=0,
            ln=0,
            align="L",
        )
        pdf.set_font(_FONT_FAMILY, "", 9)

        row_y = current_y
        for row in row_layouts:
            lines = [str(line) for line in (row.get("lines") or [""])]
            row_height = float(row.get("height") or _TABLE_ROW_MIN_HEIGHT)
            clicks_value = int(row.get("clicks") or 0)

            pdf.rect(x + mega_col_width, row_y, submenu_col_width, row_height, style="D")
            pdf.rect(
                x + mega_col_width + submenu_col_width,
                row_y,
                clicks_col_width,
                row_height,
                style="D",
            )

            for line_index, line_text in enumerate(lines):
                line_y = row_y + _CELL_PADDING_Y + (line_index * _CELL_TEXT_LINE_HEIGHT)
                if line_y + _CELL_TEXT_LINE_HEIGHT > row_y + row_height:
                    break
                pdf.set_xy(x + mega_col_width + _CELL_PADDING_X, line_y)
                pdf.cell(
                    submenu_col_width - (_CELL_PADDING_X * 2),
                    _CELL_TEXT_LINE_HEIGHT,
                    _safe_text(line_text),
                    border=0,
                    ln=0,
                    align="L",
                )

            click_y = row_y + ((row_height - _CELL_TEXT_LINE_HEIGHT) / 2.0)
            pdf.set_xy(
                x + mega_col_width + submenu_col_width + _CELL_PADDING_X,
                click_y,
            )
            pdf.cell(
                clicks_col_width - (_CELL_PADDING_X * 2),
                _CELL_TEXT_LINE_HEIGHT,
                f"{clicks_value:,}",
                border=0,
                ln=0,
                align="R",
            )
            row_y += row_height

        current_y += group_height

    return current_y - y


def _draw_menu_column(
    pdf: FPDF,
    *,
    start_y: float,
    menu_sections: dict[str, object],
) -> float:
    x, width = _column_geometry(pdf, column_index=_LEFT_COLUMN_INDEX)
    y = _draw_column_header(
        pdf,
        x=x,
        y=start_y,
        width=width,
        title="Menu",
        context_text="Mega menu and submenu clicks",
    )

    mega_rows_raw = menu_sections.get("megaMenuRows")
    mega_rows = mega_rows_raw if isinstance(mega_rows_raw, list) else []
    mega_height = _draw_two_col_card(
        pdf,
        x=x,
        y=y,
        width=width,
        section_name="Megamenus",
        left_header="Megamenu",
        text_key="megaMenu",
        rows=mega_rows,
    )
    y += mega_height + _GRID_ROW_GAP

    sub_groups_raw = menu_sections.get("subMenuGroups")
    sub_groups = sub_groups_raw if isinstance(sub_groups_raw, list) else []
    submenu_height = _draw_submenu_card(
        pdf,
        x=x,
        y=y,
        width=width,
        section_name="Submenus",
        groups=sub_groups,
    )
    y += submenu_height
    return y


def _draw_cta_column(
    pdf: FPDF,
    *,
    start_y: float,
    sections: list[dict[str, object]],
) -> float:
    x, width = _column_geometry(pdf, column_index=_MIDDLE_COLUMN_INDEX)
    y = _draw_column_header(
        pdf,
        x=x,
        y=start_y,
        width=width,
        title="CTA",
        context_text="CTA interactions grouped by button text and click count",
    )

    valid_sections = [
        item
        for item in sections
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]
    for index, section in enumerate(valid_sections):
        rows_raw = section.get("rows")
        rows = rows_raw if isinstance(rows_raw, list) else []
        name = str(section.get("name") or "").strip()
        if not name:
            continue
        card_height = _draw_two_col_card(
            pdf,
            x=x,
            y=y,
            width=width,
            section_name=name,
            left_header="CTAs",
            text_key="buttonText",
            rows=rows,
        )
        y += card_height
        if index < len(valid_sections) - 1:
            y += _GRID_ROW_GAP
    return y


def build_adv_website_cta_report_pdf(
    *,
    tenant_id: str,
    account_code: str,
    month: int,
    year: int,
    period_label: str = "",
    timezone: str,
    sections: list[dict[str, object]],
    menu_sections: dict[str, object] | None = None,
) -> bytes:
    _ = tenant_id
    normalized_menu_sections = menu_sections if isinstance(menu_sections, dict) else {}
    page_height = _resolve_dynamic_page_height(
        sections=sections,
        menu_sections=normalized_menu_sections,
    )
    pdf = _AdvWebsiteReportPDF(
        orientation="L",
        unit="mm",
        format=(page_height, _PAGE_WIDTH_MM),
    )
    pdf.alias_nb_pages()
    pdf.set_margins(_MARGIN_LEFT, _MARGIN_TOP, _MARGIN_RIGHT)
    pdf.set_auto_page_break(auto=False, margin=_MARGIN_BOTTOM)
    pdf.add_page()

    _draw_report_header(
        pdf,
        account_code=account_code,
        month=month,
        year=year,
        period_label=period_label,
        timezone=timezone,
    )
    start_y = pdf.get_y()
    menu_end_y = _draw_menu_column(
        pdf,
        start_y=start_y,
        menu_sections=normalized_menu_sections,
    )
    cta_end_y = _draw_cta_column(
        pdf,
        start_y=start_y,
        sections=sections,
    )
    pdf.set_y(max(menu_end_y, cta_end_y))

    rendered = pdf.output(dest="S")
    if isinstance(rendered, bytes):
        return rendered
    if isinstance(rendered, bytearray):
        return bytes(rendered)
    return str(rendered).encode("latin-1", "replace")
