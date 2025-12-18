# functions/ggSheet.py
from typing import List
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def read_spreadsheet(
    spreadsheet_id: str,
    range_name: str
) -> List[dict]:
    """
    Read data from Google Spreadsheet using service account
    Returns rows as list of dicts
    """

    credentials = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=SCOPES,
    )

    service = build("sheets", "v4", credentials=credentials)

    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )

    rows = result.get("values", [])

    if not rows:
        return []

    headers = rows[0]
    data_rows = rows[1:]

    return [dict(zip(headers, row)) for row in data_rows]
