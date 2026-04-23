from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

from apps.tradsphere.api.v1.helpers.schedulesImport import import_schedules_data

router = APIRouter(prefix="/schedules/import")


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
