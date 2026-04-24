from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from apps.tradsphere.api.v1.helpers.schedulesImport import import_schedules_data

router = APIRouter(prefix="/schedules/import")
_ALLOWED_IMPORT_FILE_EXTENSIONS = {".txt"}


def _decode_import_file_content(raw_content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw_content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("Unable to decode import file content")


def _ensure_import_file_extension(filename: str | None) -> None:
    file_name = str(filename or "").strip()
    if not file_name:
        raise ValueError("Import file name is required")

    file_extension = Path(file_name).suffix.lower()
    if file_extension not in _ALLOWED_IMPORT_FILE_EXTENSIONS:
        allowed = ", ".join(sorted(_ALLOWED_IMPORT_FILE_EXTENSIONS))
        raise ValueError(f"Import file extension must be one of: {allowed}")


@router.post("")
async def import_schedules_file_route(
    import_files: list[UploadFile] = File(..., alias="file"),
    skip_blank_lines: bool = Query(True, alias="skipBlankLines"),
):
    """
    Import schedules from an uploaded fixed-width text file.

    Example request:
        POST /api/tradsphere/v1/schedules/import
        Content-Type: multipart/form-data
        form-data:
          file=@"/path/to/strata-export.txt"

    Example request (keep blank lines):
        POST /api/tradsphere/v1/schedules/import?skipBlankLines=false
        Content-Type: multipart/form-data
        form-data:
          file=@"/path/to/strata-export.txt"

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 12},
          "data": {
            "summary": {
              "linesSent": 3,
              "importedNew": 2,
              "updated": 1,
              "totalLines": 3,
              "parsedLines": 3,
              "schedulesUpserted": 3,
              "scheduleWeeksUpserted": 10
            }
          }
        }

    Example error response (line-level validation):
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 2},
          "error": {
            "message": "Bad Request",
            "detail": "Missing required fields: EstNum (lines: 2, 4); DayPart (lines: 4)"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Accepts multipart/form-data with file field name `file`
        - Exactly one file is allowed; multiple `file` uploads return HTTP 400
        - File extension must be `.txt`
        - Uploaded file must be plain text in STRATA fixed-width format
        - Import is rejected when any required fields are missing on any row; response includes missing field names and line numbers
        - ClientBillingCode must match `YYQQ-ACCOUNTCODE-MARKETCODE` with quarter `01-04` (example: `2602-TAAA-AUS`)
        - ClientBillingCode max length is 20 characters
        - Summary includes `linesSent`, `importedNew`, and `updated` counts (based on deduped schedule matchKey rows)
        - Empty files are rejected with HTTP 400
        - skipBlankLines defaults to true
        - Uses the same parsing/upsert behavior as POST /api/tradsphere/v1/schedules/import
    """
    if len(import_files) != 1:
        raise HTTPException(
            status_code=400,
            detail="Exactly one file is allowed for import",
        )
    import_file = import_files[0]

    try:
        _ensure_import_file_extension(import_file.filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    raw_content = await import_file.read()
    if not raw_content:
        raise HTTPException(status_code=400, detail="Import file is empty")

    try:
        decoded_content = _decode_import_file_content(raw_content)
        return import_schedules_data(
            {
                "content": decoded_content,
                "skipBlankLines": skip_blank_lines,
            }
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
