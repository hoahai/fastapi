from __future__ import annotations

import calendar
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from apps.opssphere.api.helpers.ga4 import run_ga4_report

# GA4 Data API field names are camelCase (eventName/eventCount).
GA4_EVENT_NAME_DIMENSION_CANDIDATES: tuple[str, ...] = ("eventName",)
GA4_CLICK_TEXT_DIMENSION = "customEvent:CTA"
GA4_SUB_MENU_PARENT_DIMENSION = "customEvent:mega_menu_parent"
GA4_EVENT_COUNT_METRIC_CANDIDATES: tuple[str, ...] = ("eventCount",)

MENU_MEGA_EVENT_NAME = "mega_menu_interaction"
MENU_SUB_EVENT_NAME = "sub_menu_interaction"

ADV_WEBSITE_EVENT_FILTERS: tuple[str, ...] = (
    "srp_filter_select",
    "new_vdp_cta_interaction",
    "used_vdp_cta_interaction",
    "new_srp_cta_interaction",
    "used_srp_cta_interaction",
    MENU_MEGA_EVENT_NAME,
    MENU_SUB_EVENT_NAME,
)

CTA_EVENT_SECTION_MAP: dict[str, str] = {
    "new_srp_cta_interaction": "SRPs- New",
    "used_srp_cta_interaction": "SRPs- Used",
    "new_vdp_cta_interaction": "VDPs- New",
    "used_vdp_cta_interaction": "VDPs- Used",
}

CTA_SECTION_ORDER: tuple[str, ...] = (
    "SRPs- New",
    "SRPs- Used",
    "VDPs- New",
    "VDPs- Used",
)
_NON_MEANINGFUL_CLICK_TEXT_VALUES = {
    "",
    "(blank)",
    "(not set)",
    "not set",
    "null",
    "none",
    "(none)",
}


def get_month_date_range(*, month: int, year: int) -> tuple[str, str]:
    last_day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


def _parse_iso_date(value: str) -> datetime.date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None
    return parsed.date()


def _resolve_effective_date_range(
    *,
    month: int,
    year: int,
    min_start_date: str | None,
) -> tuple[str, str]:
    month_start, month_end = get_month_date_range(month=month, year=year)
    month_start_date = _parse_iso_date(month_start)
    month_end_date = _parse_iso_date(month_end)
    if month_start_date is None or month_end_date is None:
        return month_start, month_end

    configured_min_date = _parse_iso_date(str(min_start_date or "").strip())
    if configured_min_date is None:
        return month_start, month_end

    effective_start_date = max(month_start_date, configured_min_date)
    if effective_start_date > month_end_date:
        raise ValueError(
            "Selected month is earlier than configured GA4 start_date "
            f"({configured_min_date.isoformat()})."
        )

    return effective_start_date.isoformat(), month_end_date.isoformat()


def _to_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return 0
    try:
        return int(Decimal(text))
    except (InvalidOperation, ValueError, TypeError):
        return 0


def _normalize_click_text(value: object) -> str:
    return str(value or "").strip()


def _is_meaningful_click_text(value: object) -> bool:
    normalized = _normalize_click_text(value)
    if not normalized:
        return False
    return normalized.lower() not in _NON_MEANINGFUL_CLICK_TEXT_VALUES


def _build_menu_rows_from_base_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    menu_rows: list[dict[str, object]] = []
    for row in rows:
        event_name = str(row.get("event_name") or "").strip()
        if event_name not in {MENU_MEGA_EVENT_NAME, MENU_SUB_EVENT_NAME}:
            continue
        menu_rows.append(
            {
                "event_name": event_name,
                "click_text": _normalize_click_text(row.get("click_text")),
                "event_count": max(0, _to_int(row.get("event_count"))),
                "menu_parent_text": "",
            }
        )
    return menu_rows


def _fetch_menu_rows_with_parent_dimension(
    *,
    property_id: str,
    start_date: str,
    end_date: str,
    event_dimension: str,
    click_dimension: str,
    event_count_metric: str,
    base_rows: list[dict[str, object]],
) -> tuple[list[dict[str, object]], str, list[dict[str, object]]]:
    parent_dimension = GA4_SUB_MENU_PARENT_DIMENSION
    try:
        report = run_ga4_report(
            property_id=property_id,
            start_date=start_date,
            end_date=end_date,
            dimensions=[event_dimension, click_dimension, parent_dimension],
            metrics=[event_count_metric],
            limit=250000,
            dimension_filter={
                "filter": {
                    "fieldName": event_dimension,
                    "inListFilter": {
                        "values": [MENU_MEGA_EVENT_NAME, MENU_SUB_EVENT_NAME],
                    },
                }
            },
        )
    except Exception:
        # Keep parity with CTA behavior: strict fixed-dimension query.
        raise

    rows: list[dict[str, object]] = []
    meaningful_parents: list[str] = []
    for row in report.get("rows", []):
        if not isinstance(row, dict):
            continue
        dimensions = row.get("dimensions")
        metrics = row.get("metrics")
        if not isinstance(dimensions, dict):
            dimensions = {}
        if not isinstance(metrics, dict):
            metrics = {}

        event_name = str(dimensions.get(event_dimension) or "").strip()
        if event_name not in {MENU_MEGA_EVENT_NAME, MENU_SUB_EVENT_NAME}:
            continue

        click_text = _normalize_click_text(dimensions.get(click_dimension))
        menu_parent_text = _normalize_click_text(dimensions.get(parent_dimension))
        event_count = max(0, _to_int(metrics.get(event_count_metric)))
        rows.append(
            {
                "event_name": event_name,
                "click_text": click_text,
                "event_count": event_count,
                "menu_parent_text": menu_parent_text,
            }
        )
        if event_name == MENU_SUB_EVENT_NAME and _is_meaningful_click_text(menu_parent_text):
            meaningful_parents.append(menu_parent_text)

    unique_meaningful = {value.lower() for value in meaningful_parents if str(value).strip()}
    stats = [
        {
            "subMenuParentDimension": parent_dimension,
            "rowCount": len(rows),
            "meaningfulParentValueCount": len(meaningful_parents),
            "uniqueMeaningfulParentValueCount": len(unique_meaningful),
        }
    ]

    if not rows:
        return _build_menu_rows_from_base_rows(base_rows), parent_dimension, stats
    return rows, parent_dimension, stats


def fetch_adv_website_events_for_month(
    *,
    property_id: str,
    month: int,
    year: int,
    min_start_date: str | None = None,
) -> dict[str, object]:
    start_date, end_date = _resolve_effective_date_range(
        month=month,
        year=year,
        min_start_date=min_start_date,
    )
    return fetch_adv_website_events_for_date_range(
        property_id=property_id,
        start_date=start_date,
        end_date=end_date,
        min_start_date=min_start_date,
    )


def fetch_adv_website_events_for_date_range(
    *,
    property_id: str,
    start_date: str,
    end_date: str,
    min_start_date: str | None = None,
) -> dict[str, object]:
    start_date = str(start_date or "").strip()
    end_date = str(end_date or "").strip()
    start_date_obj = _parse_iso_date(start_date)
    end_date_obj = _parse_iso_date(end_date)
    if start_date_obj is None or end_date_obj is None:
        raise ValueError("Invalid GA4 date range. Expected YYYY-MM-DD.")
    if start_date_obj > end_date_obj:
        raise ValueError("Invalid GA4 date range: start_date is after end_date.")

    configured_min_date = _parse_iso_date(str(min_start_date or "").strip())
    if configured_min_date is not None:
        effective_start = max(start_date_obj, configured_min_date)
        if effective_start > end_date_obj:
            raise ValueError(
                "Selected period is earlier than configured GA4 start_date "
                f"({configured_min_date.isoformat()})."
            )
        start_date = effective_start.isoformat()
        end_date = end_date_obj.isoformat()

    best_rows: list[dict[str, object]] | None = None
    best_report: dict[str, object] | None = None
    best_event_dimension = ""
    best_click_dimension = GA4_CLICK_TEXT_DIMENSION
    best_count_metric = ""
    best_score = (-1, -1)
    last_error: Exception | None = None
    candidate_stats: list[dict[str, object]] = []
    click_dimension = GA4_CLICK_TEXT_DIMENSION

    for event_dimension in GA4_EVENT_NAME_DIMENSION_CANDIDATES:
        for event_count_metric in GA4_EVENT_COUNT_METRIC_CANDIDATES:
            try:
                report = run_ga4_report(
                    property_id=property_id,
                    start_date=start_date,
                    end_date=end_date,
                    dimensions=[event_dimension, click_dimension],
                    metrics=[event_count_metric],
                    limit=250000,
                    dimension_filter={
                        "filter": {
                            "fieldName": event_dimension,
                            "inListFilter": {
                                "values": list(ADV_WEBSITE_EVENT_FILTERS),
                            },
                        }
                    },
                )
                normalized_rows: list[dict[str, object]] = []
                meaningful_values: list[str] = []
                for row in report.get("rows", []):
                    if not isinstance(row, dict):
                        continue

                    dimensions = row.get("dimensions")
                    metrics = row.get("metrics")
                    if not isinstance(dimensions, dict):
                        dimensions = {}
                    if not isinstance(metrics, dict):
                        metrics = {}

                    event_name = str(dimensions.get(event_dimension) or "").strip()
                    click_text = _normalize_click_text(dimensions.get(click_dimension))
                    event_count = _to_int(metrics.get(event_count_metric))
                    if not event_name:
                        continue

                    normalized_rows.append(
                        {
                            "event_name": event_name,
                            "click_text": click_text,
                            "event_count": event_count,
                        }
                    )
                    if _is_meaningful_click_text(click_text):
                        meaningful_values.append(click_text)

                unique_meaningful = {
                    value.lower() for value in meaningful_values if value
                }
                score = (len(meaningful_values), len(unique_meaningful))
                candidate_stats.append(
                    {
                        "eventDimension": event_dimension,
                        "clickTextDimension": click_dimension,
                        "eventCountMetric": event_count_metric,
                        "rowCount": len(normalized_rows),
                        "meaningfulValueCount": score[0],
                        "uniqueMeaningfulValueCount": score[1],
                    }
                )

                if score > best_score:
                    best_score = score
                    best_rows = normalized_rows
                    best_report = report
                    best_event_dimension = event_dimension
                    best_click_dimension = click_dimension
                    best_count_metric = event_count_metric
                last_error = None
            except Exception as exc:
                last_error = exc
                candidate_stats.append(
                    {
                        "eventDimension": event_dimension,
                        "clickTextDimension": click_dimension,
                        "eventCountMetric": event_count_metric,
                        "error": str(exc),
                    }
                )

    if best_rows is None or best_report is None:
        if last_error is not None:
            raise last_error
        raise RuntimeError("Failed to fetch GA4 event rows for CTA report.")

    (
        menu_rows,
        resolved_sub_menu_parent_dimension,
        sub_menu_parent_candidate_stats,
    ) = _fetch_menu_rows_with_parent_dimension(
        property_id=property_id,
        start_date=start_date,
        end_date=end_date,
        event_dimension=best_event_dimension,
        click_dimension=best_click_dimension,
        event_count_metric=best_count_metric,
        base_rows=best_rows,
    )

    return {
        "start_date": start_date,
        "end_date": end_date,
        "resolved_event_dimension": best_event_dimension,
        "resolved_click_text_dimension": best_click_dimension,
        "resolved_event_count_metric": best_count_metric,
        "resolved_sub_menu_parent_dimension": resolved_sub_menu_parent_dimension,
        "candidate_stats": candidate_stats,
        "sub_menu_parent_candidate_stats": sub_menu_parent_candidate_stats,
        "rows": best_rows,
        "menu_rows": menu_rows,
    }


def _split_menu_path(value: str) -> tuple[str, str]:
    text = _normalize_click_text(value)
    if not text:
        return "", ""
    parts = [
        item.strip()
        for item in re.split(r"\s*(?:>|>>|\||::|->)\s*", text)
        if item.strip()
    ]
    if len(parts) < 2:
        return "", ""
    return parts[0], " - ".join(parts[1:])


def build_cta_sections(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, int]] = {section: {} for section in CTA_SECTION_ORDER}

    for row in rows:
        event_name = str(row.get("event_name") or "").strip()
        section = CTA_EVENT_SECTION_MAP.get(event_name)
        if not section:
            continue

        button_text = _normalize_click_text(row.get("click_text")) or "(blank)"
        event_count = max(0, _to_int(row.get("event_count")))
        grouped[section][button_text] = grouped[section].get(button_text, 0) + event_count

    sections: list[dict[str, object]] = []
    for section in CTA_SECTION_ORDER:
        section_rows = grouped.get(section, {})
        section_items = [
            {"buttonText": button_text, "clicks": clicks}
            for button_text, clicks in sorted(
                section_rows.items(),
                key=lambda item: (-int(item[1]), item[0].lower(), item[0]),
            )
        ]
        sections.append(
            {
                "name": section,
                "rows": section_items,
                "totalClicks": sum(item["clicks"] for item in section_items),
            }
        )
    return sections


def build_menu_sections(
    rows: list[dict[str, object]],
) -> dict[str, object]:
    mega_menu_clicks: dict[str, int] = {}
    sub_menu_grouped: dict[str, dict[str, int]] = {}

    for row in rows:
        event_name = str(row.get("event_name") or "").strip()
        click_text = _normalize_click_text(row.get("click_text"))
        event_count = max(0, _to_int(row.get("event_count")))
        if event_count <= 0:
            continue

        if event_name == MENU_MEGA_EVENT_NAME:
            mega_name = click_text or "(blank)"
            mega_menu_clicks[mega_name] = mega_menu_clicks.get(mega_name, 0) + event_count
            continue

        if event_name != MENU_SUB_EVENT_NAME:
            continue

        parent_name = _normalize_click_text(row.get("menu_parent_text"))
        submenu_name = click_text or "(blank)"

        parsed_parent, parsed_submenu = _split_menu_path(submenu_name)
        if not _is_meaningful_click_text(parent_name) and parsed_parent:
            parent_name = parsed_parent
            submenu_name = parsed_submenu or submenu_name

        if not _is_meaningful_click_text(parent_name):
            parent_name = "(Unknown)"

        sub_menu_grouped.setdefault(parent_name, {})
        sub_menu_grouped[parent_name][submenu_name] = (
            sub_menu_grouped[parent_name].get(submenu_name, 0) + event_count
        )

    mega_menu_rows = [
        {"megaMenu": mega_name, "clicks": clicks}
        for mega_name, clicks in sorted(
            mega_menu_clicks.items(),
            key=lambda item: (-int(item[1]), item[0].lower(), item[0]),
        )
    ]

    sub_menu_groups: list[dict[str, object]] = []
    for mega_name, grouped_rows in sub_menu_grouped.items():
        rows_list = [
            {"subMenu": sub_name, "clicks": clicks}
            for sub_name, clicks in sorted(
                grouped_rows.items(),
                key=lambda item: (-int(item[1]), item[0].lower(), item[0]),
            )
        ]
        total_clicks = sum(int(item["clicks"]) for item in rows_list)
        sub_menu_groups.append(
            {
                "megaMenu": mega_name,
                "rows": rows_list,
                "totalClicks": total_clicks,
            }
        )

    sub_menu_groups.sort(
        key=lambda item: (
            -int(item.get("totalClicks") or 0),
            str(item.get("megaMenu") or "").lower(),
            str(item.get("megaMenu") or ""),
        )
    )

    return {
        "megaMenuRows": mega_menu_rows,
        "subMenuGroups": sub_menu_groups,
        "totalMegaMenuClicks": sum(int(item.get("clicks") or 0) for item in mega_menu_rows),
        "totalSubMenuClicks": sum(
            int(item.get("totalClicks") or 0) for item in sub_menu_groups
        ),
    }
