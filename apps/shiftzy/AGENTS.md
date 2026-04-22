# AGENTS.md (Shiftzy)

## Scope

- Applies to `apps/shiftzy/**`.
- Root-level `AGENTS.md` still applies; this file adds Shiftzy-specific rules.

## App Overview

- Mounted at `/api/shiftzy`.
- Active version: `/api/shiftzy/v1`.
- App entrypoint: `apps/shiftzy/api/main.py`.
- Router: `apps/shiftzy/api/v1/router.py`.

## Architecture Rules

- Keep route handlers under `api/v1/endpoints`.
- Keep DB access logic in `api/v1/helpers/dbQueries.py`.
- Keep config/tenant validation in `api/v1/helpers/config.py`.
- Keep week/date helper logic in dedicated helper modules (for example `helpers/weeks.py`).

## Tenant + Config Rules

- Shiftzy tenant config must be validated via:
  - `apps/shiftzy/api/v1/helpers/config.py`
- Required tenant values include:
  - `START_WEEK_NO`
  - `START_DATE` (must be Monday)
  - `WEEK_BEFORE`
  - `WEEK_AFTER`
  - `POSITION_AREAS_ENUM`
  - `SCHEDULE_SECTIONS_ENUM`
  - `DB_TABLES`
- `PDF` config is optional.

## Scheduling/Data Rules

- Preserve week-index and date-window behavior driven by tenant config.
- Keep schedule/employee/shift flows transaction-safe where currently implemented.
- Keep payload normalization and conflict validation in helper layer; keep endpoints thin.

## PDF Rules

- PDF generation uses `fpdf2`.
- Assets are under `apps/shiftzy/api/assets`.
- Do not move or replace PDF stack unless explicitly requested.

## Route Standards

- Keep response envelope-compatible responses.
- Unknown query params must remain rejected unless explicitly opted out.
- Route docstrings are mandatory and must include:
  - short description
  - example request(s)
  - example response
  - requirements/notes

## Safety Constraints

- No cross-tenant shared state without tenant keys.
- No cross-app refactors from Shiftzy changes unless explicitly requested.
