from google.oauth2 import service_account
from googleapiclient.discovery import build

from shared.utils import resolve_secret_path

# =====================================================
# CONFIG
# =====================================================

READONLY_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
READWRITE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

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
