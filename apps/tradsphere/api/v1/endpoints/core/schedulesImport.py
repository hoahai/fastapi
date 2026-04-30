from __future__ import annotations

import base64
import binascii
from pathlib import Path

from fastapi import APIRouter, Body, File, HTTPException, Query, UploadFile
from pydantic import BaseModel, ConfigDict, Field

from apps.tradsphere.api.v1.helpers.schedulesImport import import_schedules_data

router = APIRouter(prefix="/schedules/import")
_ALLOWED_IMPORT_FILE_EXTENSIONS = {".txt"}
_ALLOWED_IMPORT_MIME_TYPES = {"text/plain"}


class ScheduleImportBase64Payload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    type: str
    content_base64: str = Field(alias="contentBase64")
    skip_blank_lines: bool = Field(default=True, alias="skipBlankLines")


def _decode_import_file_content(raw_content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw_content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("Unable to decode import file content")


def _decode_import_base64_content(encoded_content: str) -> bytes:
    text = str(encoded_content or "").strip()
    if not text:
        raise ValueError("contentBase64 is required")

    if "," in text:
        raise ValueError(
            "contentBase64 must contain only base64 data; do not include data URL prefix"
        )

    try:
        return base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("contentBase64 must be a valid base64 string") from exc


def _ensure_import_file_extension(filename: str | None) -> None:
    file_name = str(filename or "").strip()
    if not file_name:
        raise ValueError("Import file name is required")

    file_extension = Path(file_name).suffix.lower()
    if file_extension not in _ALLOWED_IMPORT_FILE_EXTENSIONS:
        allowed = ", ".join(sorted(_ALLOWED_IMPORT_FILE_EXTENSIONS))
        raise ValueError(f"Import file extension must be one of: {allowed}")


def _parse_skip_blank_lines(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    raise ValueError("skipBlankLines must be a boolean")


def _ensure_import_file_type(content_type: str | None) -> None:
    normalized = str(content_type or "").strip().lower()
    if not normalized:
        raise ValueError("type is required")
    normalized = normalized.split(";", 1)[0].strip()
    if normalized not in _ALLOWED_IMPORT_MIME_TYPES:
        allowed = ", ".join(sorted(_ALLOWED_IMPORT_MIME_TYPES))
        raise ValueError(f"type must be one of: {allowed}")


@router.post("/base64")
def import_schedules_base64_route(
    payload: ScheduleImportBase64Payload = Body(...),
):
    """
    Import schedules from a base64-encoded fixed-width text payload.

    Example request:
        POST /api/tradsphere/v1/schedules/import/base64
        Content-Type: application/json
        {
          "name": "SPSch_202.txt",
          "type": "text/plain",
          "contentBase64": "MDAwMDAwMTAwMSAgICAyNjAyLVRBQUEtQVVTICAgS0FCQyAgMDAwMSAgMjYwNDAxMjYwNDMw...",
          "skipBlankLines": true
        }

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 13},
          "data": {
            "summary": {
              "linesSent": 3,
              "importedNew": 2,
              "updated": 1,
              "importedEstNums": [1001, 1002],
              "updatedEstNums": [1003],
              "totalLines": 3,
              "parsedLines": 3,
              "schedulesUpserted": 3,
              "scheduleWeeksUpserted": 10
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Accepts application/json payload
        - `name` is required and must have `.txt` extension
        - `type` is required and must be `text/plain`
        - `contentBase64` is required and must be valid base64 text
        - `contentBase64` must be base64 bytes only (do not send `data:*;base64,` prefix)
        - Base64 content must decode into plain text STRATA fixed-width format
        - Import is rejected when any required fields are missing on any row; response includes missing field names and line numbers
        - ClientBillingCode must match `YYQQ-ACCOUNTCODE-MARKETCODE` with quarter `01-04` (example: `2602-TAAA-AUS`)
        - ClientBillingCode max length is 20 characters
        - Summary includes `importedEstNums` and `updatedEstNums` as unique estNum lists
        - skipBlankLines defaults to true when not provided
        - Uses the same parsing/upsert behavior as POST /api/tradsphere/v1/schedules/import/file
    """
    try:
        _ensure_import_file_extension(payload.name)
        _ensure_import_file_type(payload.type)
        encoded_content = payload.content_base64
        raw_content = _decode_import_base64_content(encoded_content)
        if not raw_content:
            raise HTTPException(status_code=400, detail="Import file is empty")

        decoded_content = _decode_import_file_content(raw_content)
        return import_schedules_data(
            {
                "content": decoded_content,
                "skipBlankLines": _parse_skip_blank_lines(payload.skip_blank_lines),
            }
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/file")
async def import_schedules_file_route(
    import_files: list[UploadFile] = File(..., alias="file"),
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
              "linesSent": 3,
              "importedNew": 2,
              "updated": 1,
              "importedEstNums": [1001, 1002],
              "updatedEstNums": [1003],
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
        - Summary includes `importedEstNums` and `updatedEstNums` as unique estNum lists
        - Empty files are rejected with HTTP 400
        - skipBlankLines defaults to true
        - Uses the same parsing/upsert behavior as POST /api/tradsphere/v1/schedules/import/file
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
