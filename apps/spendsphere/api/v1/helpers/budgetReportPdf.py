from __future__ import annotations

import calendar
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from fpdf import FPDF

from shared.constants import GGADS_MIN_BUDGET, GGADS_MIN_BUDGET_DELTA
from shared.tenant import get_timezone

_FONT_FAMILY = "Helvetica"
_TITLE_TEXT = "Budget OverView"
_LARGE_RANK = 10**9
_COLOR_DEFAULT = (0, 0, 0)
_COLOR_YELLOW = (194, 137, 0)
_COLOR_GREEN = (0, 128, 0)
_COLOR_RED = (196, 0, 0)
_COLOR_BRIGHT_YELLOW = (255, 255, 0)
_BORDER_COLOR = (214, 214, 214)
# Approximation of rgba(51, 119, 255, 0.15) on white background.
_HEADER_BG_COLOR = (224, 235, 255)
_TYPE_SUMMARY_BG_COLOR = (239, 245, 255)
_SECTION_BG_COLOR = (51, 119, 255)
_SECTION_TEXT_COLOR = (255, 255, 255)
_DAILY_BUDGET_ALERT_BG_COLOR = (153, 0, 255)  # #9900ff
_DAILY_BUDGET_ALERT_THRESHOLD = Decimal("500")
_PERCENT_SPENT_BENCHMARK_BG_COLOR = (246, 171, 31)  # #f6ab1f
_PERCENT_SPENT_BENCHMARK_TEXT_COLOR = (226, 82, 30)  # #e2521e
_TABLE_COLUMNS: list[tuple[str, str, float, str]] = [
    ("type", "Type", 11.0, "L"),
    ("adTypeBudget", "Master Budget", 18.0, "L"),
    ("budgetId", "BudgetId", 20.0, "L"),
    ("campaigns", "Campaigns", 46.0, "L"),
    ("spent", "Spent", 19.0, "R"),
    ("allocation", "Allocation", 16.0, "R"),
    ("allocatedBudget", "Allocated Budget", 22.0, "R"),
    ("dailyBudget", "Daily Budget", 20.0, "R"),
    ("percentSpent", "% Spent", 19.0, "R"),
    ("pacing", "Pacing", 20.0, "R"),
]
_SECTION_HEADER_BOX_HEIGHT = 7.0
_SECTION_HEADER_AFTER_SPACING = 2.5


class _BudgetReportPDF(FPDF):
    def footer(self) -> None:
        self.set_y(-8)
        self.set_font(_FONT_FAMILY, "", 6.5)
        self.set_text_color(*_COLOR_DEFAULT)
        self.cell(0, 4, f"{self.page_no()}/{{nb}}", border=0, ln=0, align="R")


def _get_table_columns(pdf: FPDF) -> list[tuple[str, str, float, str]]:
    available_width = max(0.0, float(pdf.w - pdf.l_margin - pdf.r_margin))
    base_total = sum(width for _key, _label, width, _align in _TABLE_COLUMNS)
    if available_width <= 0 or base_total <= 0:
        return list(_TABLE_COLUMNS)
    scale = available_width / float(base_total)
    return [
        (key, label, width * scale, align)
        for key, label, width, align in _TABLE_COLUMNS
    ]


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).encode("latin-1", "replace").decode("latin-1")


def _to_decimal(value: object, *, default: Decimal | None = None) -> Decimal | None:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if not text:
        return default
    text = text.replace(",", "")
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError, TypeError):
        return default


def _format_currency(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"${float(value):,.2f}"


def _format_percent(value: Decimal | None, *, signed: bool = False) -> str:
    if value is None:
        return ""
    if signed:
        return f"{float(value):+,.2f}%"
    return f"{float(value):,.2f}%"


def _format_allocation(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"{float(value):,.2f}%"


def _should_mark_daily_budget_mismatch(
    daily_budget: Decimal | None,
    current_budget: Decimal | None,
) -> bool:
    """
    Mirror Google Ads budget-update delta logic so report markers match
    real update behavior.
    """
    if daily_budget is None or current_budget is None:
        return False

    min_budget = Decimal(str(GGADS_MIN_BUDGET))
    min_delta = Decimal(str(GGADS_MIN_BUDGET_DELTA))
    amount_to_set = min_budget if daily_budget <= Decimal("0") else daily_budget

    if amount_to_set == current_budget:
        return False

    if amount_to_set not in (Decimal("0"), min_budget):
        if (amount_to_set - current_budget).copy_abs() <= min_delta:
            return False

    return True


def _metric_text_color(value: Decimal | None) -> tuple[int, int, int]:
    if value is None or value < 0:
        return _COLOR_DEFAULT
    if value == Decimal("0"):
        return _COLOR_DEFAULT
    if value <= Decimal("85"):
        return _COLOR_YELLOW
    if value <= Decimal("100"):
        return _COLOR_GREEN
    return _COLOR_RED


def _should_highlight_daily_budget_cell(row_data: dict[str, object]) -> bool:
    daily_budget = _to_decimal(row_data.get("_dailyBudgetValue"), default=None)
    current_budget = _to_decimal(row_data.get("_currentBudgetValue"), default=None)
    return bool(
        (daily_budget is not None and daily_budget > _DAILY_BUDGET_ALERT_THRESHOLD)
        or (current_budget is not None and current_budget > _DAILY_BUDGET_ALERT_THRESHOLD)
    )


def _cell_text_color(key: str, row_data: dict[str, object], *, is_header: bool) -> tuple[int, int, int]:
    if is_header:
        return _SECTION_BG_COLOR
    if bool(row_data.get("_isSummaryRow")):
        return _COLOR_DEFAULT
    if key == "dailyBudget":
        if _should_highlight_daily_budget_cell(row_data):
            return _COLOR_BRIGHT_YELLOW
        daily_budget = _to_decimal(row_data.get("_dailyBudgetValue"), default=None)
        if daily_budget is not None and daily_budget <= Decimal("0"):
            return _COLOR_RED
        return _COLOR_DEFAULT
    if key == "percentSpent":
        if _is_percent_spent_over_benchmark(row_data):
            return _PERCENT_SPENT_BENCHMARK_TEXT_COLOR
        return _metric_text_color(
            _to_decimal(row_data.get("_percentSpentValue"), default=None)
        )
    if key == "pacing":
        return _metric_text_color(_to_decimal(row_data.get("_pacingValue"), default=None))
    return _COLOR_DEFAULT


def _append_bold(style: str) -> str:
    return style if "B" in style else f"{style}B"


def _is_percent_spent_over_benchmark(row_data: dict[str, object]) -> bool:
    percent_value = _to_decimal(row_data.get("_percentSpentValue"), default=None)
    benchmark = _to_decimal(row_data.get("_percentSpentBenchmark"), default=None)
    if percent_value is None or benchmark is None:
        return False
    return percent_value > benchmark


def _cell_base_style(
    key: str,
    row_data: dict[str, object],
    *,
    is_header: bool,
    default_style: str,
) -> str:
    style = default_style
    if is_header:
        return style
    if bool(row_data.get("_isSummaryRow")):
        return _append_bold(style)
    if key == "dailyBudget" and _should_highlight_daily_budget_cell(row_data):
        return _append_bold(style)
    if key in {"percentSpent", "pacing"}:
        if key == "percentSpent" and _is_percent_spent_over_benchmark(row_data):
            return _append_bold(style)
        metric_value = _to_decimal(
            row_data.get("_percentSpentValue" if key == "percentSpent" else "_pacingValue"),
            default=None,
        )
        if metric_value is None or metric_value == Decimal("0"):
            return style
        return _append_bold(style)
    if key == "dailyBudget" and isinstance(row_data.get("_dailyBudgetStyledLines"), list):
        return style
    if key == "dailyBudget" and bool(row_data.get("_dailyBudgetMismatch")):
        return _append_bold(style)
    return style


def _desc_rank(value: float | None) -> float:
    if value is None:
        return float("inf")
    return -value


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


def _is_enabled_status(status: object) -> bool:
    return str(status or "").strip().upper() == "ENABLED"


def _derive_campaign_status(campaigns: object) -> str:
    if not isinstance(campaigns, list) or not campaigns:
        return "PAUSED"
    for campaign in campaigns:
        if not isinstance(campaign, dict):
            continue
        status = campaign.get("status")
        if status is None:
            status = campaign.get("campaignStatus")
        if _is_enabled_status(status):
            return "ENABLED"
    return "PAUSED"


def _build_campaign_lines(
    campaigns: object,
    *,
    max_lines: int = 6,
) -> list[tuple[str, str]]:
    if not isinstance(campaigns, list) or not campaigns:
        return [("-", "")]

    lines: list[tuple[str, str]] = []
    for campaign in campaigns:
        if not isinstance(campaign, dict):
            continue
        campaign_name = str(campaign.get("campaignName", "")).strip() or "-"
        campaign_id = str(campaign.get("campaignId", "")).strip() or "-"
        status = campaign.get("status")
        if status is None:
            status = campaign.get("campaignStatus")
        style = "" if _is_enabled_status(status) else "I"
        lines.append((f"{campaign_name} ({campaign_id})", style))

    if not lines:
        return [("-", "")]
    if len(lines) <= max_lines:
        return lines

    hidden = len(lines) - max_lines
    return lines[:max_lines] + [(f"... (+{hidden} more)", "")]


def _ad_type_rank(code: object, ad_type_order: dict[str, int]) -> int:
    key = str(code or "").strip().upper()
    if not key:
        return _LARGE_RANK
    return ad_type_order.get(key, _LARGE_RANK)


def _resolve_as_of_day(*, month: int, year: int) -> tuple[int, int]:
    days_in_month = calendar.monthrange(year, month)[1]
    now = datetime.now(ZoneInfo(get_timezone()))
    if year == now.year and month == now.month:
        as_of_day = now.day
    elif (year, month) < (now.year, now.month):
        as_of_day = days_in_month
    else:
        as_of_day = 1
    as_of_day = max(1, min(days_in_month, as_of_day))
    return as_of_day, days_in_month


def _calculate_mtd_max_spendable(
    allocated_before: Decimal | None,
    *,
    as_of_day: int,
    days_in_month: int,
) -> Decimal | None:
    if allocated_before is None or allocated_before <= 0:
        return None
    if as_of_day <= 0 or days_in_month <= 0:
        return None
    return allocated_before * Decimal(str(as_of_day)) / Decimal(str(days_in_month))


def _build_master_budget_styled_lines(
    *,
    net_amount: Decimal | None,
    rollover_amount: Decimal | None,
    services: object,
) -> list[tuple[str, str, float | None]] | None:
    total: Decimal | None = None
    if net_amount is not None and rollover_amount is not None:
        total = (net_amount + rollover_amount).quantize(Decimal("0.01"))
    elif net_amount is not None:
        total = net_amount.quantize(Decimal("0.01"))
    elif rollover_amount is not None:
        total = rollover_amount.quantize(Decimal("0.01"))

    if total is None:
        return None

    breakdown_lines: list[tuple[str, str, float | None]] = []
    service_line_count = 0
    if isinstance(services, list):
        for service in services:
            if not isinstance(service, dict):
                continue
            name = str(
                service.get("serviceName")
                or service.get("name")
                or service.get("serviceId")
                or "service"
            ).strip()
            amount = _to_decimal(service.get("netAmount"), default=None)
            if not name or amount is None:
                continue
            breakdown_lines.append(
                (f"{name}: {_format_currency(amount)}", "", 5.8)
            )
            service_line_count += 1

    show_rollover = rollover_amount is not None and rollover_amount != Decimal("0")
    if show_rollover:
        breakdown_lines.append(
            (f"Rollover: {_format_currency(rollover_amount)}", "", 5.8)
        )

    # Treat an all-zero/no-budget payload as blank for this column.
    if total == Decimal("0.00") and service_line_count == 0 and not show_rollover:
        return None

    lines: list[tuple[str, str, float | None]] = [
        (_format_currency(total), "B", None),
    ]

    if breakdown_lines:
        if service_line_count > 0:
            lines.append(("------", "", 5.8))
        lines.extend(breakdown_lines)

    return lines


def _build_summary_row(
    rows: list[dict[str, object]],
    *,
    row_kind: str,
    type_value: str | None = None,
    as_of_day: int,
    days_in_month: int,
) -> dict[str, object]:
    spent_total = Decimal("0")
    allocation_total = Decimal("0")
    allocated_before_total = Decimal("0")
    allocated_after_total = Decimal("0")
    daily_budget_total = Decimal("0")
    daily_budget_count = 0
    has_allocation = False
    has_allocated_before = False
    has_allocated_after = False

    for row in rows:
        if not isinstance(row, dict):
            continue
        spent_value = _to_decimal(row.get("_spentValue"), default=Decimal("0")) or Decimal("0")
        spent_total += spent_value

        allocation_value = _to_decimal(row.get("_allocationValue"), default=None)
        if allocation_value is not None:
            allocation_total += allocation_value
            has_allocation = True

        allocated_before_value = _to_decimal(row.get("_allocatedBeforeValue"), default=None)
        if allocated_before_value is not None:
            allocated_before_total += allocated_before_value
            has_allocated_before = True

        allocated_after_value = _to_decimal(row.get("_allocatedAfterValue"), default=None)
        if allocated_after_value is not None:
            allocated_after_total += allocated_after_value
            has_allocated_after = True

        daily_budget_value = _to_decimal(row.get("_dailyBudgetValue"), default=None)
        if daily_budget_value is not None:
            daily_budget_total += daily_budget_value
            daily_budget_count += 1

    allocated_before_total_value = (
        allocated_before_total.quantize(Decimal("0.01"))
        if has_allocated_before
        else None
    )
    allocated_after_total_value = (
        allocated_after_total.quantize(Decimal("0.01"))
        if has_allocated_after
        else None
    )
    allocation_total_value = (
        allocation_total.quantize(Decimal("0.01"))
        if has_allocation
        else Decimal("0.00")
    )
    daily_budget_avg_value: Decimal | None = None
    if daily_budget_count > 0:
        daily_budget_avg_value = (
            daily_budget_total / Decimal(str(daily_budget_count))
        ).quantize(Decimal("0.01"))
    percent_spent = Decimal("0")
    if allocated_before_total_value is not None and allocated_before_total_value > 0:
        percent_spent = (
            spent_total / allocated_before_total_value * Decimal("100")
        ).quantize(Decimal("0.01"))

    pacing = Decimal("0")
    total_mtd_max_spendable = _calculate_mtd_max_spendable(
        allocated_before_total_value,
        as_of_day=as_of_day,
        days_in_month=days_in_month,
    )
    if total_mtd_max_spendable is not None and total_mtd_max_spendable > 0:
        pacing = (spent_total / total_mtd_max_spendable * Decimal("100")).quantize(
            Decimal("0.01")
        )
    percent_spent_benchmark = (
        (Decimal(str(as_of_day)) / Decimal(str(days_in_month))) * Decimal("100")
    ).quantize(Decimal("0.01"))

    allocated_display = _format_currency(allocated_after_total_value)
    allocation_display = (
        _format_allocation(allocation_total_value)
        if row_kind == "type"
        else ""
    )
    daily_budget_display = _format_currency(daily_budget_avg_value)
    summary_type = "" if row_kind == "account" else str(type_value or "")

    return {
        "type": summary_type,
        "adTypeBudget": "",
        "budgetId": "",
        "campaigns": "",
        "allocation": allocation_display,
        "spent": _format_currency(spent_total.quantize(Decimal("0.01"))),
        "allocatedBudget": allocated_display,
        "dailyBudget": daily_budget_display,
        "percentSpent": _format_percent(percent_spent),
        "pacing": _format_percent(pacing),
        "_campaignStyledLines": [("", "")],
        "_allocatedBold": False,
        "_allocatedStyledLines": None,
        "_adTypeBudgetStyledLines": None,
        "_dailyBudgetValue": daily_budget_avg_value,
        "_currentBudgetValue": None,
        "_dailyBudgetMismatch": False,
        "_percentSpentValue": percent_spent,
        "_percentSpentBenchmark": percent_spent_benchmark,
        "_pacingValue": pacing,
        "_campaignStatusRank": 2,
        "_allocatedSort": float(allocated_after_total_value) if allocated_after_total_value is not None else None,
        "_spentSort": float(spent_total),
        "_spentValue": spent_total.quantize(Decimal("0.01")),
        "_allocatedBeforeValue": allocated_before_total_value,
        "_allocatedAfterValue": allocated_after_total_value,
        "_allocationValue": allocation_total_value if row_kind == "type" else None,
        "_adTypeBudgetValue": None,
        "_accelerationDeltaValue": None,
        "_isSummaryRow": True,
        "_summaryKind": "account" if row_kind == "account" else "type",
    }


def _build_report_groups(
    rows: list[dict] | None,
    ad_type_order: dict[str, int],
    *,
    month: int,
    year: int,
    is_current_period: bool,
) -> dict[str, list[dict[str, object]]]:
    groups: dict[str, list[dict[str, object]]] = {}
    if not isinstance(rows, list):
        return groups
    as_of_day, days_in_month = _resolve_as_of_day(month=month, year=year)
    percent_spent_benchmark = (
        (Decimal(str(as_of_day)) / Decimal(str(days_in_month))) * Decimal("100")
    ).quantize(Decimal("0.01"))

    for row in rows:
        if not isinstance(row, dict):
            continue
        budget_id = str(row.get("budgetId", "")).strip()
        if not budget_id:
            continue

        account_code = str(row.get("accountCode", "")).strip().upper() or "UNKNOWN"
        ad_type_code = str(row.get("adTypeCode", "")).strip().upper() or "-"

        campaigns = row.get("campaigns")
        campaign_status = _derive_campaign_status(campaigns)
        campaign_status_rank = 0 if campaign_status == "ENABLED" else 1

        spend = _to_decimal(row.get("totalCost"), default=Decimal("0")) or Decimal("0")
        allocation = _to_decimal(row.get("allocation"), default=None)
        net_amount = _to_decimal(row.get("netAmount"), default=None)
        rollover_amount = _to_decimal(row.get("rolloverAmount"), default=None)
        ad_type_budget_value: Decimal | None = None
        if net_amount is not None and rollover_amount is not None:
            ad_type_budget_value = (net_amount + rollover_amount).quantize(
                Decimal("0.01")
            )
        elif net_amount is not None:
            ad_type_budget_value = net_amount.quantize(Decimal("0.01"))
        elif rollover_amount is not None:
            ad_type_budget_value = rollover_amount.quantize(Decimal("0.01"))
        ad_type_budget_styled_lines = _build_master_budget_styled_lines(
            net_amount=net_amount,
            rollover_amount=rollover_amount,
            services=row.get("services"),
        )
        allocated_before = _to_decimal(
            row.get("allocatedBudgetBeforeAcceleration"),
            default=None,
        )
        multiplier = _to_decimal(row.get("accelerationMultiplier"), default=None)
        acceleration_present = (
            row.get("accelerationId") is not None or multiplier is not None
        )
        if multiplier is None or multiplier <= 0:
            multiplier = Decimal("100")

        allocated_after: Decimal | None = None
        if allocated_before is not None:
            allocated_after = (
                allocated_before * (multiplier / Decimal("100"))
            ).quantize(Decimal("0.01"))

        daily_budget = _to_decimal(row.get("dailyBudget"), default=None)
        current_budget = _to_decimal(row.get("budgetAmount"), default=None)
        daily_budget_display = _format_currency(daily_budget)
        daily_budget_mismatch = (
            _should_mark_daily_budget_mismatch(daily_budget, current_budget)
            if is_current_period
            else False
        )
        if daily_budget_mismatch:
            daily_budget_display = f"{daily_budget_display}*"
        daily_budget_styled_lines: list[tuple[str, str, float | None]] | None = None
        if daily_budget_mismatch and current_budget is not None:
            daily_budget_styled_lines = [
                (daily_budget_display, "B", None),
                (f"Current: {_format_currency(current_budget)}", "", 5.8),
            ]

        percent_spent: Decimal | None = None
        if allocated_before is not None and allocated_before > 0:
            percent_spent = (
                spend / allocated_before * Decimal("100")
            ).quantize(Decimal("0.01"))
        percent_spent_display = _format_percent(percent_spent) if percent_spent is not None else "0.00%"
        percent_spent_value = percent_spent if percent_spent is not None else Decimal("0")

        pacing: Decimal | None = None
        mtd_max_spendable = _calculate_mtd_max_spendable(
            allocated_before,
            as_of_day=as_of_day,
            days_in_month=days_in_month,
        )
        if mtd_max_spendable is not None and mtd_max_spendable > 0:
            pacing = (spend / mtd_max_spendable * Decimal("100")).quantize(
                Decimal("0.01")
            )
        pacing_display = _format_percent(pacing) if pacing is not None else "0.00%"
        pacing_value = pacing if pacing is not None else Decimal("0")

        acceleration_value = ""
        acceleration_delta: Decimal | None = None
        if acceleration_present:
            acceleration_delta = (multiplier - Decimal("100")).quantize(Decimal("0.01"))
            acceleration_value = _format_percent(acceleration_delta, signed=True)

        allocated_budget_display = _format_currency(allocated_after)
        allocated_styled_lines: list[tuple[str, str, float | None]] | None = None
        if allocated_after is not None and acceleration_value:
            allocated_styled_lines = [
                (allocated_budget_display, "B", None),
                (f"Accel. {acceleration_value}", "I", 5.8),
            ]

        ad_type_budget_display = (
            _format_currency(ad_type_budget_value)
            if ad_type_budget_styled_lines
            else "-"
        )

        groups.setdefault(account_code, []).append(
            {
                "type": ad_type_code,
                "adTypeBudget": ad_type_budget_display,
                "budgetId": budget_id,
                "campaigns": "",
                "allocation": _format_allocation(allocation),
                "spent": _format_currency(spend),
                "allocatedBudget": allocated_budget_display,
                "dailyBudget": daily_budget_display,
                "percentSpent": percent_spent_display,
                "pacing": pacing_display,
                "_campaignStyledLines": _build_campaign_lines(campaigns),
                "_allocatedBold": bool(allocated_styled_lines),
                "_allocatedStyledLines": allocated_styled_lines,
                "_adTypeBudgetStyledLines": ad_type_budget_styled_lines,
                "_dailyBudgetStyledLines": daily_budget_styled_lines,
                "_dailyBudgetValue": daily_budget,
                "_currentBudgetValue": current_budget,
                "_dailyBudgetMismatch": daily_budget_mismatch,
                "_percentSpentValue": percent_spent_value,
                "_percentSpentBenchmark": percent_spent_benchmark,
                "_pacingValue": pacing_value,
                "_campaignStatusRank": campaign_status_rank,
                "_allocatedSort": float(allocated_after) if allocated_after is not None else None,
                "_spentSort": float(spend),
                "_spentValue": spend,
                "_allocationValue": allocation,
                "_allocatedBeforeValue": allocated_before,
                "_allocatedAfterValue": allocated_after,
                "_adTypeBudgetValue": ad_type_budget_value,
                "_accelerationDeltaValue": acceleration_delta,
            }
        )

    for account_code, account_rows in groups.items():
        account_rows.sort(
            key=lambda item: (
                _ad_type_rank(item.get("type"), ad_type_order),
                str(item.get("type", "")),
                int(item.get("_campaignStatusRank", 1)),
                _desc_rank(item.get("_allocatedSort")),
                -float(item.get("_spentSort", 0.0)),
                str(item.get("budgetId", "")),
            )
        )
        account_rows_with_summaries: list[dict[str, object]] = []
        idx = 0
        while idx < len(account_rows):
            current_type = str(account_rows[idx].get("type", ""))
            end = idx + 1
            while end < len(account_rows) and str(account_rows[end].get("type", "")) == current_type:
                end += 1
            type_rows = account_rows[idx:end]
            account_rows_with_summaries.extend(type_rows)
            account_rows_with_summaries.append(
                _build_summary_row(
                    type_rows,
                    row_kind="type",
                    type_value=current_type,
                    as_of_day=as_of_day,
                    days_in_month=days_in_month,
                )
            )
            idx = end

        if account_rows:
            account_rows_with_summaries.append(
                _build_summary_row(
                    account_rows,
                    row_kind="account",
                    as_of_day=as_of_day,
                    days_in_month=days_in_month,
                )
            )

        groups[account_code] = account_rows_with_summaries

    return dict(sorted(groups.items(), key=lambda item: item[0]))


def _draw_title_block(
    pdf: FPDF,
    *,
    tenant_id: str,
    month: int,
    year: int,
) -> None:
    now = datetime.now(ZoneInfo(get_timezone()))
    generated_at = f"{now.month}/{now.day}/{now.year} {now.strftime('%H:%M:%S')}"
    subtitle = (
        f"Company: {tenant_id} | Period: {month}/{year} | "
        f"Generated: {generated_at}"
    )

    pdf.set_font(_FONT_FAMILY, "B", 16)
    pdf.cell(0, 9, _safe_text(_TITLE_TEXT), ln=1, align="C")
    pdf.set_font(_FONT_FAMILY, "", 9)
    pdf.cell(0, 6, _safe_text(subtitle), ln=1, align="C")
    pdf.ln(4)


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


def _draw_section_header(pdf: FPDF, account_code: str, *, continued: bool = False) -> None:
    title = f"Account: {account_code}"
    if continued:
        title = f"{title} (continued)"
    x = pdf.l_margin
    y = pdf.get_y()
    w = pdf.w - pdf.l_margin - pdf.r_margin
    h = _SECTION_HEADER_BOX_HEIGHT

    _draw_filled_rounded_rect(
        pdf,
        x=x,
        y=y,
        w=w,
        h=h,
        radius=1.2,
        color=_SECTION_BG_COLOR,
    )

    pdf.set_text_color(*_SECTION_TEXT_COLOR)
    pdf.set_font(_FONT_FAMILY, "B", 11)
    pdf.set_xy(x + 1.2, y)
    pdf.cell(w - 2.4, h, _safe_text(title), ln=1, align="L")
    pdf.set_text_color(*_COLOR_DEFAULT)
    pdf.ln(_SECTION_HEADER_AFTER_SPACING)


def _compute_table_row_layout(
    pdf: FPDF,
    row_data: dict[str, object],
    *,
    font_size: float,
    line_height: float,
    padding: float,
    ignore_height_keys: set[str] | None = None,
) -> tuple[float, list[list[tuple[str, str, float | None]]]]:
    pdf.set_font(_FONT_FAMILY, "", font_size)
    wrapped_columns: list[list[tuple[str, str, float | None]]] = []
    max_lines = 1
    table_columns = _get_table_columns(pdf)
    ignored = ignore_height_keys or set()

    for key, _label, width, _align in table_columns:
        text_width = width - (padding * 2)
        styled_lines: list[tuple[str, str, float | None]] = []
        if key == "campaigns":
            campaign_lines = row_data.get("_campaignStyledLines")
            if isinstance(campaign_lines, list) and campaign_lines:
                for line in campaign_lines:
                    if not isinstance(line, tuple) or len(line) != 2:
                        continue
                    line_text, line_style = line
                    wrapped = _split_text_lines(
                        pdf,
                        str(line_text or ""),
                        text_width,
                        line_height,
                    )
                    for wrapped_line in wrapped:
                        styled_lines.append((wrapped_line, str(line_style or ""), None))
            else:
                wrapped = _split_text_lines(
                    pdf,
                    str(row_data.get(key, "") or ""),
                    text_width,
                    line_height,
                )
                styled_lines = [(line, "", None) for line in wrapped]
        elif key == "allocatedBudget":
            allocated_lines = row_data.get("_allocatedStyledLines")
            if isinstance(allocated_lines, list) and allocated_lines:
                for entry in allocated_lines:
                    if not isinstance(entry, tuple) or len(entry) < 2:
                        continue
                    line_text = entry[0]
                    line_style = entry[1]
                    line_size = entry[2] if len(entry) > 2 else None
                    render_size = (
                        float(line_size)
                        if isinstance(line_size, (int, float))
                        else font_size
                    )
                    pdf.set_font(_FONT_FAMILY, "", render_size)
                    wrapped = _split_text_lines(
                        pdf,
                        str(line_text or ""),
                        text_width,
                        line_height,
                    )
                    for wrapped_line in wrapped:
                        styled_lines.append((wrapped_line, str(line_style or ""), line_size if isinstance(line_size, (int, float)) else None))
                pdf.set_font(_FONT_FAMILY, "", font_size)
            else:
                wrapped = _split_text_lines(
                    pdf,
                    str(row_data.get(key, "") or ""),
                    text_width,
                    line_height,
                )
                styled_lines = [(line, "", None) for line in wrapped]
        elif key == "adTypeBudget":
            ad_type_budget_lines = row_data.get("_adTypeBudgetStyledLines")
            if isinstance(ad_type_budget_lines, list) and ad_type_budget_lines:
                for entry in ad_type_budget_lines:
                    if not isinstance(entry, tuple) or len(entry) < 2:
                        continue
                    line_text = entry[0]
                    line_style = entry[1]
                    line_size = entry[2] if len(entry) > 2 else None
                    render_size = (
                        float(line_size)
                        if isinstance(line_size, (int, float))
                        else font_size
                    )
                    pdf.set_font(_FONT_FAMILY, "", render_size)
                    wrapped = _split_text_lines(
                        pdf,
                        str(line_text or ""),
                        text_width,
                        line_height,
                    )
                    for wrapped_line in wrapped:
                        styled_lines.append(
                            (
                                wrapped_line,
                                str(line_style or ""),
                                line_size if isinstance(line_size, (int, float)) else None,
                            )
                        )
                pdf.set_font(_FONT_FAMILY, "", font_size)
            else:
                wrapped = _split_text_lines(
                    pdf,
                    str(row_data.get(key, "") or ""),
                    text_width,
                    line_height,
                )
                styled_lines = [(line, "", None) for line in wrapped]
        elif key == "dailyBudget":
            daily_lines = row_data.get("_dailyBudgetStyledLines")
            if isinstance(daily_lines, list) and daily_lines:
                for entry in daily_lines:
                    if not isinstance(entry, tuple) or len(entry) < 2:
                        continue
                    line_text = entry[0]
                    line_style = entry[1]
                    line_size = entry[2] if len(entry) > 2 else None
                    render_size = (
                        float(line_size)
                        if isinstance(line_size, (int, float))
                        else font_size
                    )
                    pdf.set_font(_FONT_FAMILY, "", render_size)
                    wrapped = _split_text_lines(
                        pdf,
                        str(line_text or ""),
                        text_width,
                        line_height,
                    )
                    for wrapped_line in wrapped:
                        styled_lines.append(
                            (
                                wrapped_line,
                                str(line_style or ""),
                                line_size if isinstance(line_size, (int, float)) else None,
                            )
                        )
                pdf.set_font(_FONT_FAMILY, "", font_size)
            else:
                wrapped = _split_text_lines(
                    pdf,
                    str(row_data.get(key, "") or ""),
                    text_width,
                    line_height,
                )
                styled_lines = [(line, "", None) for line in wrapped]
        else:
            wrapped = _split_text_lines(
                pdf,
                str(row_data.get(key, "") or ""),
                text_width,
                line_height,
            )
            styled_lines = [(line, "", None) for line in wrapped]

        if not styled_lines:
            styled_lines = [("", "", None)]
        wrapped_columns.append(styled_lines)
        if key not in ignored and len(styled_lines) > max_lines:
            max_lines = len(styled_lines)

    row_height = max((line_height * max_lines) + (padding * 2), 6.0)
    return row_height, wrapped_columns


def _merge_style(base_style: str, line_style: str) -> str:
    style = base_style or ""
    line = line_style or ""
    if "B" in line and "B" not in style:
        style += "B"
    if "I" in line and "I" not in style:
        style += "I"
    return style


def _draw_table_row(
    pdf: FPDF,
    row_data: dict[str, object],
    *,
    font_size: float,
    line_height: float,
    padding: float,
    is_header: bool = False,
    precomputed: tuple[float, list[list[tuple[str, str, float | None]]]] | None = None,
    row_height_override: float | None = None,
    draw_type_cell: bool = True,
    type_cell_height: float | None = None,
    suppress_type_text: bool = False,
    draw_ad_type_budget_cell: bool = True,
    ad_type_budget_cell_height: float | None = None,
) -> float:
    if precomputed is None:
        precomputed = _compute_table_row_layout(
            pdf,
            row_data,
            font_size=font_size,
            line_height=line_height,
            padding=padding,
        )
    row_height, wrapped_columns = precomputed
    if row_height_override is not None and row_height_override > 0:
        row_height = row_height_override

    y = pdf.get_y()
    x = pdf.l_margin
    table_columns = _get_table_columns(pdf)
    is_summary = bool(row_data.get("_isSummaryRow")) and not is_header
    row_fill_color: tuple[int, int, int] | None = None
    if is_header:
        row_fill_color = _HEADER_BG_COLOR
    elif is_summary:
        summary_kind = str(row_data.get("_summaryKind", "")).strip().lower()
        if summary_kind == "type":
            row_fill_color = _TYPE_SUMMARY_BG_COLOR
        else:
            row_fill_color = _HEADER_BG_COLOR
    pdf.set_draw_color(*_BORDER_COLOR)

    for idx, (key, _label, width, align) in enumerate(table_columns):
        lines = wrapped_columns[idx]

        if key == "type" and not is_header and not draw_type_cell:
            x += width
            continue
        if key == "adTypeBudget" and not is_header and not draw_ad_type_budget_cell:
            x += width
            continue

        cell_height = row_height
        if key == "type" and not is_header and type_cell_height is not None:
            cell_height = type_cell_height
        if key == "adTypeBudget" and not is_header and ad_type_budget_cell_height is not None:
            cell_height = ad_type_budget_cell_height

        cell_fill_color = row_fill_color
        if (
            key == "dailyBudget"
            and not is_header
            and not is_summary
            and _should_highlight_daily_budget_cell(row_data)
        ):
            cell_fill_color = _DAILY_BUDGET_ALERT_BG_COLOR
        if (
            key == "percentSpent"
            and not is_header
            and not is_summary
            and _is_percent_spent_over_benchmark(row_data)
        ):
            cell_fill_color = _PERCENT_SPENT_BENCHMARK_BG_COLOR
        fill = cell_fill_color is not None
        if cell_fill_color is not None:
            pdf.set_fill_color(*cell_fill_color)
        pdf.rect(x, y, width, cell_height, style="DF" if fill else "D")

        base_style = "B" if is_header else ""
        if key == "allocatedBudget" and not is_header and bool(row_data.get("_allocatedBold")):
            base_style = "B"
        base_style = _cell_base_style(
            key,
            row_data,
            is_header=is_header,
            default_style=base_style,
        )

        text_width = width - (padding * 2)
        text_color = _cell_text_color(key, row_data, is_header=is_header)
        render_lines = lines
        if key == "type" and suppress_type_text and not is_header:
            render_lines = [("", "", None)]

        for line_idx, line_data in enumerate(render_lines):
            line_text, line_style, line_size = line_data
            effective_base_style = base_style
            if key == "allocatedBudget" and isinstance(row_data.get("_allocatedStyledLines"), list):
                # For acceleration annotation rows, keep bold only on the amount line.
                effective_base_style = ""
            if key == "dailyBudget" and isinstance(row_data.get("_dailyBudgetStyledLines"), list):
                # For mismatch annotation rows, keep bold only on the amount line.
                effective_base_style = ""
            merged_style = _merge_style(effective_base_style, line_style)
            render_size = (
                float(line_size)
                if isinstance(line_size, (int, float))
                else font_size
            )
            pdf.set_font(_FONT_FAMILY, merged_style, render_size)
            pdf.set_text_color(*text_color)

            line_y = y + padding + (line_idx * line_height)
            if line_y + line_height > y + cell_height:
                break
            pdf.set_xy(x + padding, line_y)
            pdf.cell(
                text_width,
                line_height,
                _safe_text(line_text),
                border=0,
                ln=0,
                align=align,
            )

        x += width

    pdf.set_text_color(*_COLOR_DEFAULT)
    pdf.set_draw_color(*_COLOR_DEFAULT)
    pdf.set_y(y + row_height)
    return row_height


def _draw_table_header(pdf: FPDF) -> None:
    header_row = {key: label for key, label, _width, _align in _TABLE_COLUMNS}
    _draw_table_row(
        pdf,
        header_row,
        font_size=7.3,
        line_height=3.4,
        padding=0.8,
        is_header=True,
    )


def _estimate_table_header_height(pdf: FPDF) -> float:
    header_row = {key: label for key, label, _width, _align in _TABLE_COLUMNS}
    row_height, _ = _compute_table_row_layout(
        pdf,
        header_row,
        font_size=7.3,
        line_height=3.4,
        padding=0.8,
    )
    return row_height


def _ensure_page_room_for_account_start(
    pdf: FPDF,
    *,
    first_row: dict[str, object] | None,
    body_font_size: float,
    body_line_height: float,
    body_padding: float,
) -> None:
    header_height = _estimate_table_header_height(pdf)
    first_row_height = 6.0
    if isinstance(first_row, dict):
        first_row_height, _ = _compute_table_row_layout(
            pdf,
            first_row,
            font_size=body_font_size,
            line_height=body_line_height,
            padding=body_padding,
        )
    required_height = (
        _SECTION_HEADER_BOX_HEIGHT
        + _SECTION_HEADER_AFTER_SPACING
        + header_height
        + first_row_height
    )
    if pdf.get_y() + required_height > _bottom_y(pdf):
        pdf.add_page()


def _bottom_y(pdf: FPDF) -> float:
    return pdf.h - pdf.b_margin


def _render_type_group(
    pdf: FPDF,
    *,
    account_code: str,
    rows: list[dict[str, object]],
    font_size: float,
    line_height: float,
    padding: float,
) -> None:
    cursor = 0
    while cursor < len(rows):
        available = _bottom_y(pdf) - pdf.get_y()
        segment_rows: list[dict[str, object]] = []
        segment_layouts: list[tuple[float, list[list[tuple[str, str, float | None]]]]] = []
        segment_height = 0.0
        idx = cursor

        while idx < len(rows):
            is_summary_row = bool(rows[idx].get("_isSummaryRow"))
            is_first_row_in_segment = not segment_rows
            ignore_height_keys: set[str]
            if is_summary_row:
                ignore_height_keys = set()
            elif is_first_row_in_segment:
                # Keep merged master-budget content fully visible by allowing
                # the first detail row in each segment to size for this column.
                ignore_height_keys = {"type"}
            else:
                ignore_height_keys = {"type", "adTypeBudget"}
            layout = _compute_table_row_layout(
                pdf,
                rows[idx],
                font_size=font_size,
                line_height=line_height,
                padding=padding,
                ignore_height_keys=ignore_height_keys,
            )
            row_height = layout[0]
            if not segment_rows:
                if row_height > available and available < 20:
                    pdf.add_page()
                    _draw_table_header(pdf)
                    available = _bottom_y(pdf) - pdf.get_y()
                    continue
                segment_rows.append(rows[idx])
                segment_layouts.append(layout)
                segment_height += row_height
                idx += 1
                continue

            if segment_height + row_height > available:
                break
            segment_rows.append(rows[idx])
            segment_layouts.append(layout)
            segment_height += row_height
            idx += 1

        if not segment_rows:
            break

        detail_indices = [
            i
            for i, segment_row in enumerate(segment_rows)
            if not bool(segment_row.get("_isSummaryRow"))
        ]
        first_detail_idx = detail_indices[0] if detail_indices else None
        row_height_overrides: dict[int, float] = {}
        if first_detail_idx is not None and detail_indices:
            # Base detail heights ignore merged columns; then only add height if
            # merged Master Budget content would otherwise be clipped.
            for detail_idx in detail_indices:
                base_layout = _compute_table_row_layout(
                    pdf,
                    segment_rows[detail_idx],
                    font_size=font_size,
                    line_height=line_height,
                    padding=padding,
                    ignore_height_keys={"type", "adTypeBudget"},
                )
                row_height_overrides[detail_idx] = base_layout[0]

            detail_base_total = sum(row_height_overrides[i] for i in detail_indices)
            first_detail_row = segment_rows[first_detail_idx]
            ad_type_only_ignore_keys = {
                key
                for key, _label, _width, _align in _get_table_columns(pdf)
                if key != "adTypeBudget"
            }
            ad_type_layout = _compute_table_row_layout(
                pdf,
                first_detail_row,
                font_size=font_size,
                line_height=line_height,
                padding=padding,
                ignore_height_keys=ad_type_only_ignore_keys,
            )
            required_merged_height = ad_type_layout[0]
            shortfall = required_merged_height - detail_base_total
            if shortfall > 0:
                detail_count = len(detail_indices)
                per_row_extra = shortfall / float(detail_count)
                distributed = 0.0
                for pos, detail_idx in enumerate(detail_indices):
                    if pos == detail_count - 1:
                        row_extra = shortfall - distributed
                    else:
                        row_extra = per_row_extra
                        distributed += row_extra
                    row_height_overrides[detail_idx] += row_extra

        def _render_row_height(i: int) -> float:
            return row_height_overrides.get(i, segment_layouts[i][0])

        detail_rows_height = (
            sum(_render_row_height(i) for i in detail_indices)
            if detail_indices
            else 0.0
        )
        segment_render_height = sum(
            _render_row_height(i) for i in range(len(segment_rows))
        )

        for seg_idx, row in enumerate(segment_rows):
            draw_ad_type_budget_cell = True
            ad_type_budget_cell_height: float | None = None
            if first_detail_idx is not None:
                if seg_idx == first_detail_idx:
                    ad_type_budget_cell_height = detail_rows_height
                elif seg_idx in detail_indices:
                    draw_ad_type_budget_cell = False
            is_summary_row = bool(row.get("_isSummaryRow"))
            draw_type_cell = seg_idx == 0
            suppress_type_text = bool(is_summary_row and cursor > 0 and seg_idx == 0)
            _draw_table_row(
                pdf,
                row,
                font_size=font_size,
                line_height=line_height,
                padding=padding,
                is_header=False,
                precomputed=segment_layouts[seg_idx],
                row_height_override=row_height_overrides.get(seg_idx),
                draw_type_cell=draw_type_cell,
                type_cell_height=segment_render_height if seg_idx == 0 else None,
                suppress_type_text=suppress_type_text,
                draw_ad_type_budget_cell=draw_ad_type_budget_cell,
                ad_type_budget_cell_height=ad_type_budget_cell_height,
            )

        cursor += len(segment_rows)
        if cursor < len(rows):
            pdf.add_page()
            _draw_table_header(pdf)


def _render_account_rows(
    pdf: FPDF,
    *,
    account_code: str,
    rows: list[dict[str, object]],
    font_size: float,
    line_height: float,
    padding: float,
) -> None:
    idx = 0
    while idx < len(rows):
        row_type = str(rows[idx].get("type", ""))
        group_end = idx + 1
        while group_end < len(rows) and str(rows[group_end].get("type", "")) == row_type:
            group_end += 1

        _render_type_group(
            pdf,
            account_code=account_code,
            rows=rows[idx:group_end],
            font_size=font_size,
            line_height=line_height,
            padding=padding,
        )
        idx = group_end


def build_budget_report_pdf(
    *,
    rows: list[dict] | None,
    budgets: list[dict] | None,
    tenant_id: str,
    month: int,
    year: int,
    ad_type_order: dict[str, int] | None = None,
    is_current_period: bool = True,
) -> bytes:
    del budgets  # reserved

    pdf = _BudgetReportPDF(orientation="P", unit="mm", format="Letter")
    pdf.alias_nb_pages()
    pdf.set_margins(6, 10, 6)
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    _draw_title_block(pdf, tenant_id=tenant_id, month=month, year=year)
    grouped_rows = _build_report_groups(
        rows,
        ad_type_order or {},
        month=month,
        year=year,
        is_current_period=is_current_period,
    )

    if not grouped_rows:
        pdf.set_font(_FONT_FAMILY, "", 10)
        pdf.cell(
            0,
            6,
            _safe_text("No budget rows found for the selected month/year."),
            ln=1,
        )
    else:
        body_font_size = 7.0
        body_line_height = 3.3
        body_padding = 0.8

        for account_index, (account_code, account_rows) in enumerate(grouped_rows.items()):
            if account_index > 0:
                pdf.ln(8)

            _ensure_page_room_for_account_start(
                pdf,
                first_row=account_rows[0] if account_rows else None,
                body_font_size=body_font_size,
                body_line_height=body_line_height,
                body_padding=body_padding,
            )

            _draw_section_header(pdf, account_code)
            _draw_table_header(pdf)
            _render_account_rows(
                pdf,
                account_code=account_code,
                rows=account_rows,
                font_size=body_font_size,
                line_height=body_line_height,
                padding=body_padding,
            )

    rendered = pdf.output(dest="S")
    if isinstance(rendered, bytes):
        return rendered
    if isinstance(rendered, bytearray):
        return bytes(rendered)
    return str(rendered).encode("latin-1", "replace")
