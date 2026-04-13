from __future__ import annotations

import threading
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from apps.spendsphere.api.v1.helpers.accountCodes import standardize_account_code
from apps.spendsphere.api.v1.helpers.config import get_spendsphere_sheets
from apps.spendsphere.api.v1.helpers.spendsphereHelpers import (
    clear_google_ads_campaigns_cache_entries,
    list_pending_video_campaign_status_requests,
    resolve_video_campaign_status_requests,
    upsert_video_campaign_status_requests,
)
from shared.ggSheet import _read_sheet_values, _write_sheet_values
from shared.logger import get_logger
from shared.tenant import get_timezone

logger = get_logger("SpendSphere")

_VIDEO_STATUS_SHEET_KEY = "video_campaign_status_update"
_VIDEO_WARNING_CODE = "CAMPAIGN_STATUS_MUTATE_NOT_ALLOWED"
_VIDEO_CHANNEL_TYPE = "VIDEO"
_VIDEO_STATUS_HEADERS = [
    "createdAt",
    "source",
    "customerId",
    "accountCode",
    "campaignId",
    "campaignName",
    "oldStatus",
    "newStatus",
    "channelType",
    "warningCode",
    "message",
]
_SCRIPT_PROCESSED_HEADER = "processed"
_SCRIPT_RESULT_HEADER = "result"
_REQUEST_ID_MARKER = "::rid="
_VIDEO_STATUS_SHEET_LOCK = threading.Lock()


def _column_label_from_index(index: int) -> str:
    if index < 1:
        raise ValueError("index must be >= 1")
    result = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(ord("A") + remainder) + result
    return result


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_status(value: object) -> str:
    raw = _normalize_text(value)
    return raw.upper() if raw else ""


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    normalized = _normalize_text(value).lower()
    return normalized in {"true", "1", "yes", "y"}


def _extract_campaign_name(warning: dict) -> str:
    names = warning.get("campaignNames")
    if isinstance(names, list):
        for value in names:
            name = _normalize_text(value)
            if name:
                return name
    return _normalize_text(warning.get("campaignName"))


def _with_request_id_in_source(source: str, request_id: str) -> str:
    base = _normalize_text(source)
    return f"{base}{_REQUEST_ID_MARKER}{request_id}" if base else f"{_REQUEST_ID_MARKER}{request_id}"


def _extract_request_id_from_source(source_value: object) -> str:
    source = _normalize_text(source_value)
    if not source:
        return ""
    marker_index = source.rfind(_REQUEST_ID_MARKER)
    if marker_index < 0:
        return ""
    return source[marker_index + len(_REQUEST_ID_MARKER) :].strip()


def _is_video_status_warning(warning: dict) -> bool:
    warning_code = _normalize_text(warning.get("warningCode")).upper()
    if warning_code != _VIDEO_WARNING_CODE:
        return False
    channel_type = _normalize_text(
        warning.get("channelType") or warning.get("trigger")
    ).upper()
    return channel_type == _VIDEO_CHANNEL_TYPE


def _append_video_status_rows(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    rows: list[list[object]],
) -> None:
    if not rows:
        return

    end_col = _column_label_from_index(len(_VIDEO_STATUS_HEADERS))
    header_range = f"'{sheet_name}'!A1:{end_col}1"

    with _VIDEO_STATUS_SHEET_LOCK:
        existing_header_rows = _read_sheet_values(
            spreadsheet_id=spreadsheet_id,
            range_name=header_range,
            app_name="SpendSphere",
        )
        existing_header = (
            [_normalize_text(value) for value in existing_header_rows[0]]
            if existing_header_rows
            else []
        )
        if existing_header != _VIDEO_STATUS_HEADERS:
            _write_sheet_values(
                spreadsheet_id=spreadsheet_id,
                range_name=header_range,
                values=[_VIDEO_STATUS_HEADERS],
                app_name="SpendSphere",
            )

        first_column_values = _read_sheet_values(
            spreadsheet_id=spreadsheet_id,
            range_name=f"'{sheet_name}'!A:A",
            app_name="SpendSphere",
        )
        start_row = max(len(first_column_values) + 1, 2)
        end_row = start_row + len(rows) - 1
        write_range = f"'{sheet_name}'!A{start_row}:{end_col}{end_row}"
        _write_sheet_values(
            spreadsheet_id=spreadsheet_id,
            range_name=write_range,
            values=rows,
            app_name="SpendSphere",
        )


def _column_index(headers: list[object], header_name: str) -> int:
    target = str(header_name).strip().lower()
    for index, raw in enumerate(headers):
        if _normalize_text(raw).lower() == target:
            return index
    return -1


def _sync_pending_request_statuses_from_sheet(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    pending_by_request_id: dict[str, dict[str, object]],
) -> tuple[int, int]:
    if not pending_by_request_id:
        return 0, 0

    values = _read_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=f"'{sheet_name}'!A:ZZ",
        app_name="SpendSphere",
    )
    if not values:
        return 0, 0

    headers = values[0]
    source_index = _column_index(headers, "source")
    processed_index = _column_index(headers, _SCRIPT_PROCESSED_HEADER)
    result_index = _column_index(headers, _SCRIPT_RESULT_HEADER)
    if source_index < 0 or processed_index < 0:
        return 0, 0

    now_iso = datetime.now(ZoneInfo(get_timezone())).isoformat(timespec="seconds")
    resolved_entries: list[dict[str, object]] = []
    resolved_account_codes: set[str] = set()
    seen_request_ids: set[str] = set()

    for row in values[1:]:
        if source_index >= len(row):
            continue
        request_id = _extract_request_id_from_source(row[source_index])
        if not request_id or request_id in seen_request_ids:
            continue
        if request_id not in pending_by_request_id:
            continue

        processed_value = row[processed_index] if processed_index < len(row) else ""
        if not _to_bool(processed_value):
            continue

        result_value = row[result_index] if result_index < len(row) else ""
        resolved_entries.append(
            {
                "requestId": request_id,
                "resolvedAt": now_iso,
                "result": _normalize_text(result_value),
            }
        )
        entry = pending_by_request_id.get(request_id) or {}
        account_code = standardize_account_code(entry.get("accountCode")) or ""
        if account_code:
            resolved_account_codes.add(account_code)
        seen_request_ids.add(request_id)

    if not resolved_entries:
        return 0, 0

    resolved_count = resolve_video_campaign_status_requests(resolved_entries)
    cleared_count = (
        clear_google_ads_campaigns_cache_entries(sorted(resolved_account_codes))
        if resolved_account_codes
        else 0
    )
    return resolved_count, cleared_count


def _collect_new_requests_and_rows(
    *,
    mutation_results: list[dict],
    source: str,
    created_at: str,
    existing_pending_keys: set[tuple[str, str, str]],
) -> tuple[list[dict[str, object]], list[list[object]], int, int, int]:
    new_requests: list[dict[str, object]] = []
    new_rows: list[list[object]] = []
    deferred_warnings = 0
    duplicate_suppressed_warnings = 0
    unresolved_pending_warnings = 0
    local_pending_keys: set[tuple[str, str, str]] = set()

    for result in mutation_results:
        if not isinstance(result, dict):
            continue
        if _normalize_text(result.get("operation")) != "update_campaign_statuses":
            continue

        customer_id = _normalize_text(result.get("customerId"))
        warnings = result.get("warnings")
        if not customer_id or not isinstance(warnings, list):
            continue

        filtered_warnings: list[dict] = []
        for warning in warnings:
            if not isinstance(warning, dict) or not _is_video_status_warning(warning):
                if isinstance(warning, dict):
                    filtered_warnings.append(warning)
                continue

            campaign_id = _normalize_text(warning.get("campaignId"))
            new_status = _normalize_status(warning.get("newStatus"))
            if not campaign_id or not new_status:
                filtered_warnings.append(warning)
                continue

            pending_key = (customer_id, campaign_id, new_status)
            if pending_key in existing_pending_keys:
                # Warning is emitted only when the same pending request
                # remains unresolved by the script on a later run.
                unresolved_pending_warnings += 1
                filtered_warnings.append(warning)
                continue

            if pending_key in local_pending_keys:
                duplicate_suppressed_warnings += 1
                continue

            request_id = uuid.uuid4().hex
            source_with_request_id = _with_request_id_in_source(source, request_id)
            account_code = standardize_account_code(warning.get("accountCode")) or ""
            new_requests.append(
                {
                    "requestId": request_id,
                    "status": "pending",
                    "customerId": customer_id,
                    "accountCode": account_code,
                    "campaignId": campaign_id,
                    "newStatus": new_status,
                    "source": source_with_request_id,
                    "createdAt": created_at,
                }
            )
            new_rows.append(
                [
                    created_at,
                    source_with_request_id,
                    customer_id,
                    account_code,
                    campaign_id,
                    _extract_campaign_name(warning),
                    _normalize_status(warning.get("oldStatus")),
                    new_status,
                    _VIDEO_CHANNEL_TYPE,
                    _VIDEO_WARNING_CODE,
                    _normalize_text(warning.get("error")),
                ]
            )
            local_pending_keys.add(pending_key)
            deferred_warnings += 1
            # First-seen blocked VIDEO warnings are deferred to the next run.
            # They are tracked in sheet/cache now and emitted only if unresolved.

        result["warnings"] = filtered_warnings
        summary = result.setdefault("summary", {})
        summary["warnings"] = len(filtered_warnings)

    return (
        new_requests,
        new_rows,
        deferred_warnings,
        duplicate_suppressed_warnings,
        unresolved_pending_warnings,
    )


def sync_video_campaign_status_updates(
    *,
    mutation_results: list[dict],
    source: str,
) -> None:
    """
    Sync blocked VIDEO campaign status updates with request tracking.

    Behavior:
    - Reads sheet status only when pending requests exist.
    - Resolves pending requests from sheet rows marked processed.
    - Clears affected campaign cache entries on resolve.
    - Defers first-seen blocked VIDEO warnings to sheet/cache.
    - Emits VIDEO warning only when matching pending request remains unresolved.
    - Appends new request rows for newly blocked VIDEO warnings.
    """
    if not mutation_results:
        return

    try:
        sheets = get_spendsphere_sheets()
        target = sheets.get(_VIDEO_STATUS_SHEET_KEY, {})
        spreadsheet_id = _normalize_text(target.get("spreadsheet_id"))
        sheet_name = _normalize_text(target.get("range_name"))

        if not spreadsheet_id or not sheet_name:
            return

        tz = ZoneInfo(get_timezone())
        created_at = datetime.now(tz).isoformat(timespec="seconds")

        pending_entries = list_pending_video_campaign_status_requests()
        pending_by_request_id = {
            str(entry.get("requestId", "")).strip(): entry for entry in pending_entries
        }
        pending_by_request_id = {
            request_id: entry
            for request_id, entry in pending_by_request_id.items()
            if request_id
        }

        resolved_count, cleared_campaign_cache_count = (
            _sync_pending_request_statuses_from_sheet(
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                pending_by_request_id=pending_by_request_id,
            )
            if pending_by_request_id
            else (0, 0)
        )

        if resolved_count > 0:
            pending_entries = list_pending_video_campaign_status_requests()

        existing_pending_keys = {
            (
                _normalize_text(entry.get("customerId")),
                _normalize_text(entry.get("campaignId")),
                _normalize_status(entry.get("newStatus")),
            )
            for entry in pending_entries
            if _normalize_text(entry.get("customerId"))
            and _normalize_text(entry.get("campaignId"))
            and _normalize_status(entry.get("newStatus"))
        }

        (
            requests,
            rows,
            deferred_warnings,
            duplicate_suppressed_warnings,
            unresolved_pending_warnings,
        ) = _collect_new_requests_and_rows(
            mutation_results=mutation_results,
            source=source,
            created_at=created_at,
            existing_pending_keys=existing_pending_keys,
        )

        if requests:
            upsert_video_campaign_status_requests(requests)
        if rows:
            _append_video_status_rows(
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                rows=rows,
            )

    except Exception as exc:
        logger.error(
            "Failed to sync video campaign status updates to sheet",
            extra={
                "extra_fields": {
                    "source": source,
                    "error": str(exc),
                }
            },
        )
