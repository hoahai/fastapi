# AGENTS.md (FundSphere)

## Scope

- Applies to `apps/fundsphere/**`.
- Root-level `AGENTS.md` still applies; this file adds FundSphere-specific rules.

## App Overview

- Mounted at `/api/fundsphere`.
- Active version: `/api/fundsphere/v1`.
- App entrypoint: `apps/fundsphere/api/main.py`.
- Router: `apps/fundsphere/api/v1/router.py`.

## Architecture Rules

- Keep route handlers under `api/v1/endpoints/masterBudgetControl`.
- Keep tenant/config parsing and validation in `api/v1/helpers/config.py`.
- Keep DB logic in `api/v1/helpers/dbQueries.py`.
- Keep sheet-read/write orchestration in endpoint layer; low-level sheet calls via `shared/ggSheet.py`.

## Tenant + Config Rules

- FundSphere tenant config must pass validator in:
  - `apps/fundsphere/api/v1/helpers/config.py`
- Required config families:
  - `SPREADSHEETS`
  - `DB_TABLES`
  - `fundsphere.google_accounts`
- Keep alias support behavior for spreadsheet keys where already implemented.

## Master Budget Control Rules

- Preserve endpoint namespace under `/masterBudgetControl`.
- Current settings sync flows:
  - accounts sync
  - services sync
- Preserve existing range/header conventions for sheet writes.
- Keep account/service normalization behavior unchanged unless explicitly requested.

## Cache Rules

- Use tenant-scoped shared cache bucket behavior (`db_reads`) for DB read caches.
- Respect TTL override precedence:
  - app-scoped cache key
  - app-scoped fallback TTL
  - global fallback TTL
  - default 300s
- Keep `fresh_data=true` semantics for cache bypass routes.

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
- No cross-app refactors from FundSphere changes unless explicitly requested.
