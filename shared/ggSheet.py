import re

from google.oauth2 import service_account
from googleapiclient.discovery import build

from shared.utils import resolve_secret_path

# =====================================================
# CONFIG
# =====================================================

READONLY_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
READWRITE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_ROW_BOUNDED_A1_RANGE_RE = re.compile(
    r"^\s*([A-Za-z]+)(\d+):([A-Za-z]+)(?:([0-9]+))?\s*$"
)

# =====================================================
# INTERNAL CLIENT (DO NOT USE THREADS)
# =====================================================

def _get_sheets_service(
    *,
    scopes: list[str] | None = None,
):
    """
    Create Google Sheets service.

    IMPORTANT:
    - Must be called in a process that does NOT create threads
    """
    cred_path = resolve_secret_path(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "service-account.json",
        fallback_env_vars=("json_key_file_path",),
    )

    credentials = service_account.Credentials.from_service_account_file(
        cred_path,
        scopes=scopes or READONLY_SCOPES,
    )

    return build(
        "sheets",
        "v4",
        credentials=credentials,
        cache_discovery=False,  # critical on macOS
    )


def _read_sheet_raw(
    spreadsheet_id: str,
    range_name: str,
) -> list[dict]:
    """
    Low-level sheet reader.
    Returns raw rows as list[dict].
    """
    service = _get_sheets_service()

    result = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
        )
        .execute()
    )

    rows = result.get("values", [])
    if not rows:
        return []

    headers = rows[0]
    data_rows = rows[1:]

    return [dict(zip(headers, row)) for row in data_rows]


def _read_sheet_values(
    spreadsheet_id: str,
    range_name: str,
    *,
    value_render_option: str | None = None,
) -> list[list[object]]:
    """
    Read raw values from a sheet range without header mapping.
    """
    service = _get_sheets_service()
    get_kwargs: dict[str, object] = {
        "spreadsheetId": spreadsheet_id,
        "range": range_name,
    }
    if value_render_option:
        get_kwargs["valueRenderOption"] = value_render_option

    result = service.spreadsheets().values().get(**get_kwargs).execute()
    values = result.get("values", [])
    if not isinstance(values, list):
        return []
    return values


def _column_label_to_index(label: str) -> int:
    cleaned = str(label or "").strip().upper()
    if not cleaned or not cleaned.isalpha():
        raise ValueError(f"Invalid column label: {label}")

    index = 0
    for char in cleaned:
        index = (index * 26) + (ord(char) - ord("A") + 1)
    return index - 1


def _parse_row_bounded_a1_range(value: str) -> tuple[str, int, str]:
    """
    Parse ranges shaped like `A1:D` or `A1:D999`.
    Returns `(start_col, start_row, end_col)`.
    """
    match = _ROW_BOUNDED_A1_RANGE_RE.fullmatch(str(value or ""))
    if not match:
        raise ValueError(f"Invalid row-bounded A1 range: {value}")

    return match.group(1).upper(), int(match.group(2)), match.group(3).upper()


def _clear_sheet_notes(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    start_col: str,
    start_row: int,
    end_col: str,
) -> dict:
    """
    Clear cell notes for a bounded column range from start_row to sheet end.
    """
    service = _get_sheets_service(scopes=READWRITE_SCOPES)
    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title,gridProperties(rowCount)))",
        )
        .execute()
    )

    target_props = None
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if str(props.get("title") or "") == str(sheet_name or ""):
            target_props = props
            break

    if not target_props:
        raise ValueError(f"Sheet not found: {sheet_name}")

    sheet_id = target_props.get("sheetId")
    row_count = int(target_props.get("gridProperties", {}).get("rowCount") or 0)
    if sheet_id is None:
        raise ValueError(f"Sheet id not found: {sheet_name}")

    start_row_index = max(int(start_row) - 1, 0)
    if row_count <= start_row_index:
        return {}

    start_col_index = _column_label_to_index(start_col)
    end_col_index = _column_label_to_index(end_col) + 1

    return (
        service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": int(sheet_id),
                                "startRowIndex": start_row_index,
                                "endRowIndex": row_count,
                                "startColumnIndex": start_col_index,
                                "endColumnIndex": end_col_index,
                            },
                            "cell": {"note": ""},
                            "fields": "note",
                        }
                    }
                ]
            },
        )
        .execute()
    )


def _clear_sheet_values(
    spreadsheet_id: str,
    range_name: str,
) -> dict:
    """
    Clear values in the target range.
    """
    service = _get_sheets_service(scopes=READWRITE_SCOPES)
    return (
        service.spreadsheets()
        .values()
        .clear(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            body={},
        )
        .execute()
    )


def _set_checkbox_validation(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    start_col: str,
    start_row: int,
    end_col: str | None = None,
    end_row: int | None = None,
) -> dict:
    """
    Apply checkbox data validation to the target range.
    """
    service = _get_sheets_service(scopes=READWRITE_SCOPES)
    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title,gridProperties(rowCount)))",
        )
        .execute()
    )

    target_props = None
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if str(props.get("title") or "") == str(sheet_name or ""):
            target_props = props
            break

    if not target_props:
        raise ValueError(f"Sheet not found: {sheet_name}")

    sheet_id = target_props.get("sheetId")
    row_count = int(target_props.get("gridProperties", {}).get("rowCount") or 0)
    if sheet_id is None:
        raise ValueError(f"Sheet id not found: {sheet_name}")
    if row_count <= 0:
        return {}

    start_row_index = max(int(start_row) - 1, 0)
    if start_row_index >= row_count:
        return {}
    end_row_index = (
        row_count
        if end_row is None
        else min(max(int(end_row), int(start_row)), row_count)
    )
    if end_row_index <= start_row_index:
        return {}

    end_col_label = str(end_col or start_col).strip().upper()
    start_col_index = _column_label_to_index(start_col)
    end_col_index = _column_label_to_index(end_col_label) + 1

    return (
        service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": int(sheet_id),
                                "startRowIndex": start_row_index,
                                "endRowIndex": end_row_index,
                                "startColumnIndex": start_col_index,
                                "endColumnIndex": end_col_index,
                            },
                            "cell": {
                                "dataValidation": {
                                    "condition": {"type": "BOOLEAN"},
                                    "strict": True,
                                    "showCustomUi": True,
                                }
                            },
                            "fields": "dataValidation",
                        }
                    }
                ]
            },
        )
        .execute()
    )


def _write_sheet_values(
    spreadsheet_id: str,
    range_name: str,
    values: list[list[object]],
    *,
    value_input_option: str = "USER_ENTERED",
) -> dict:
    """
    Write values to the target range.
    """
    service = _get_sheets_service(scopes=READWRITE_SCOPES)
    return (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption=value_input_option,
            body={"values": values},
        )
        .execute()
    )
