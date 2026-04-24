from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Body, File, HTTPException, Query, UploadFile

from apps.tradsphere.api.v1.helpers.schedulesImport import import_schedules_data

router = APIRouter(prefix="/schedules/import")
_ALLOWED_IMPORT_FILE_EXTENSIONS = {".txt"}


@router.post("")
def import_schedules_route(
    payload: dict | str = Body(...),
):
    """
    Import fixed-width schedule lines and upsert schedules + schedule-weeks.

    Example request:
        POST /api/tradsphere/v1/schedules/import
        {
          "content": "1904            2504-RL-MID    T KMID       110        250929251026    1    435.0000      369.75  30 4  0  1  0  0   25107:00PM-10:15PM                NFL: 10/6 CHIEFS V JAGUARS    MO                  O            ODM                                                                                                      1904             435.00SP                     SPAdults 35-64                                                              17.1    126238   0                      110"
        }

    Example request (lines array):
        POST /api/tradsphere/v1/schedules/import
        {
          "lines": [
            "1904            2504-RL-MID    T KMID       110        250929251026    1    435.0000      369.75  30 4  0  1  0  0   25107:00PM-10:15PM                NFL: 10/6 CHIEFS V JAGUARS    MO                  O            ODM                                                                                                      1904             435.00SP                     SPAdults 35-64                                                              17.1    126238   0                      110"
          ]
        }

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 10},
          "data": {
            "summary": {
              "totalLines": 1,
              "parsedLines": 1,
              "schedulesUpserted": 1,
              "scheduleWeeksUpserted": 4
            }
          }
        }

    Example error response (line-level week validation):
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 2},
          "error": {
            "message": "Bad Request",
            "detail": "line 7: w fields must be consecutive and complete for this date range: requires 2 week field(s): w1, w2 (out of range: w3, w4)"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts raw text string, or object with content/rawText/text, or object with lines array
        - Blank lines are skipped by default (set skipBlankLines=false to keep them)
        - File format must match legacy fixed-width STRATA export positions
        - Schedule id is taken from fixed-width ScheduleID; when blank it is generated as M(lineNumber+99999)
        - matchKey uses SHA-256(scheduleId|lineNum|estNum|startDate|endDate)
        - Route upserts schedules first, then upserts schedule-weeks derived from NumofWeek + W1..W5
        - Duplicate matchKey rows in the same file are deduped with last line values
        - Week fields follow the same consecutive/complete week rule as POST /schedules based on startDate/endDate
        - Validation errors include source line context (line N: <detail>)
    """
    try:
        return import_schedules_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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


@router.post("/file")
async def import_schedules_file_route(
    import_file: UploadFile = File(..., alias="file"),
    skip_blank_lines: bool = Query(True, alias="skipBlankLines"),
):
    """
    Import schedules from an uploaded fixed-width text file.

    Example request:
        POST /api/tradsphere/v1/schedules/import/file
        Content-Type: multipart/form-data
        form-data:
          file=@"/path/to/strata-export.txt"

    Example request (keep blank lines):
        POST /api/tradsphere/v1/schedules/import/file?skipBlankLines=false
        Content-Type: multipart/form-data
        form-data:
          file=@"/path/to/strata-export.txt"

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 12},
          "data": {
            "summary": {
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
            "detail": "line 2: EstNum is required"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Accepts multipart/form-data with file field name `file`
        - File extension must be `.txt`
        - Uploaded file must be plain text in STRATA fixed-width format
        - Empty files are rejected with HTTP 400
        - skipBlankLines defaults to true
        - Uses the same parsing/upsert behavior as POST /api/tradsphere/v1/schedules/import
    """
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
