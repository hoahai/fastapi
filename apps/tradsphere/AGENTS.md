# TradSphere App Guide for AI Agents

This file applies to `apps/tradsphere/**`.
Root-level guidance in `/Users/haitruongh/Developer/fastapi/AGENTS.md` still applies.

## 1. App Overview

TradSphere is a tenant-isolated FastAPI sub-app mounted at:
- `/api/tradsphere/v1/*`

Main capabilities:
- Manage TradSphere account mappings (`TradSphere_Accounts`) linked to master accounts (`Accounts`).
- Manage estimate numbers (`estNums`) and broadcast-aware period filtering.
- Manage stations and delivery methods.
- Manage contacts and station-contact links.
- Manage schedules + weekly spot rows, including import from fixed-width `.txt`.
- Generate schedule PDF reports (compact/detail modes).
- Provide UI helper endpoints (`/ui/main/selections`, `/ui/main/load`) for frontend dropdown/load payloads.
- Compute broadcast calendar values.

Core flow (high-level):
1. Root app validates tenant context + API key.
2. TradSphere app validates query params + wraps responses in envelope.
3. Endpoint handlers call helper layer for validation/business logic.
4. Helper layer calls `dbQueries.py` for SQL reads/writes.
5. Shared tenant-scoped file cache (`caches.json`) is used for validation/read caches and PDF payload cache.

## 2. Tech Stack

- Backend framework: FastAPI (`fastapi==0.128.0`)
- ASGI server: Uvicorn (`uvicorn==0.40.0`)
- Database: MySQL (`mysql-connector-python==9.5.0`)
- PDF generation: `fpdf2==2.7.9`
- Config files: YAML tenant files via PyYAML (`pyyaml==6.0.3`)
- Env loading: python-dotenv (`python-dotenv==1.2.1`)
- Request file upload parsing: `python-multipart==0.0.26`
- Logging: shared logger with optional Axiom sink (app-scoped tenant config)

Primary references:
- `apps/tradsphere/api/main.py`
- `apps/tradsphere/api/v1/router.py`
- `apps/tradsphere/api/v1/helpers/*.py`
- `shared/db.py`, `shared/middleware.py`, `shared/tenant.py`, `shared/tenantDataCache.py`

## 3. Repository Structure

Key TradSphere folders/files:

- `apps/tradsphere/api/main.py`: TradSphere FastAPI app bootstrap (response envelope + timing middleware, query param validation dependency).
- `apps/tradsphere/api/v1/router.py`: v1 router registration.
- `apps/tradsphere/api/v1/endpoints/core/*.py`: route handlers.
- `apps/tradsphere/api/v1/endpoints/core/ui/main.py`: UI support endpoints.
- `apps/tradsphere/api/v1/helpers/config.py`: tenant config parsing/validation for TradSphere.
- `apps/tradsphere/api/v1/helpers/accountValidation.py`: TradSphere-scoped existence validation + cache invalidation.
- `apps/tradsphere/api/v1/helpers/dbQueries.py`: all SQL operations and DB-read caching.
- `apps/tradsphere/api/v1/helpers/*.py`: domain logic (`accounts`, `estNums`, `stations`, `contacts`, `schedules`, `schedulesImport`, `schedulesPdf`, `broadcastCalendar`).

Cross-app files directly affecting TradSphere:
- `/Users/haitruongh/Developer/fastapi/main.py`: root mount + shared middleware chain.
- `shared/middleware.py`: tenant context, API-key auth, request/response logging, response envelope wrapping.
- `shared/requestValidation.py`: unknown query param rejection.
- `shared/exceptionHandlers.py`: global error shape.
- `shared/response.py`: `meta/data` and `meta/error` envelope.

## 4. How to Run the App

No TradSphere-only run script is defined; run from repo root.

1. Create and activate virtualenv:
```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Provide env + tenant config:
- `.env` at `/etc/.env` or `etc/.env`
- tenant YAML at `/etc/secrets/<tenant>.yaml` or `etc/secrets/<tenant>.yaml`

4. Run API:
```bash
uvicorn main:app --reload --port 8000
```

5. TradSphere base URL:
- `http://localhost:8000/api/tradsphere/v1`

Required runtime services:
- MySQL reachable with configured credentials

## 5. Environment Variables

Only key variables used by TradSphere path are listed.

| Variable | Where Used | Purpose |
|---|---|---|
| `API_KEY_REGISTRY` | `shared/middleware.py` | Required API key registry (`client_id:api_key,...`) for API auth. |
| `DB_HOST` `DB_PORT` `DB_USER` `DB_PASSWORD` `DB_NAME` | `shared/db.py` | MySQL connection config. |
| `DB_POOL_ENABLED` `DB_POOL_SIZE` `DB_POOL_RESET_SESSION` `DB_POOL_ACQUIRE_TIMEOUT_MS` `DB_POOL_ACQUIRE_BACKOFF_MS` `DB_POOL_ACQUIRE_MAX_BACKOFF_MS` | `shared/db.py` | MySQL pooling behavior. |
| `TIMEZONE` | `shared/tenant.py`, `shared/response.py`, cache timestamps | Response timestamp and cache TTL time comparisons. |
| `APP_ENV` | `shared/exceptionHandlers.py`, `shared/logger.py` | Controls traceback inclusion (`local/dev/development`) and logging context. |
| `TRADSPHERE_DB_TABLES` (or tenant `tradsphere.DB_TABLES`) | `helpers/config.py` | Table name overrides. |
| `TRADSPHERE_ENUMS` (or tenant `tradsphere.ENUMS`) | `helpers/config.py` | `mediaType` and `contactType` enum overrides. |
| `TRADSPHERE_CACHE` (or tenant `tradsphere.CACHE`) | `helpers/config.py`, `dbQueries.py`, `accountValidation.py` | TTL settings for validation/read caches. |
| `CACHE` / `cache` | `shared/tenantDataCache.py` | Fallback shared cache TTL config. |
| `TRADSPHERE_AXIOM_API_TOKEN` `TRADSPHERE_AXIOM_DATASET` (+ optional `TRADSPHERE_AXIOM_API_URL`, `TRADSPHERE_AXIOM_BATCH_SIZE`, `TRADSPHERE_AXIOM_FLUSH_SECONDS`) | `shared/logger.py` via app-scoped env | Optional Axiom logging for TradSphere scope. |
| `SPENDSPHERE_ACCOUNT_CODE_CACHE_PATH` | `shared/tenantDataCache.py` | Path override for shared `caches.json` store (used by TradSphere caches too). |

Do not hardcode secrets. Use placeholders in docs/config examples.

## 6. Architecture and Request Flow

Request path (for `/api/tradsphere/*`):

1. Root app middleware (`/Users/haitruongh/Developer/fastapi/main.py` + `shared/middleware.py`):
- `tenant_context_middleware`: requires `X-Tenant-Id`, loads tenant YAML, validates TradSphere config via validator registry.
- `request_response_logger_middleware`: logs request/response body + metadata.
- `api_key_auth_middleware`: requires API key (`X-API-Key` or `Authorization: Bearer ...`) for API paths.
- `timing_middleware`: start timer.

2. TradSphere app middleware (`apps/tradsphere/api/main.py`):
- `response_envelope_middleware`: wraps success/error payloads into envelope.
- `timing_middleware`: local timing.

3. Route dependency:
- `validate_query_params` rejects unknown query params with HTTP 400 unless explicitly allowed (none in TradSphere currently).

4. Endpoint -> helper -> DB:
- Endpoint files in `endpoints/core`.
- Business validation in helper files.
- SQL only in `helpers/dbQueries.py` with parameterized queries (`%s` placeholders).

5. Response:
- Success: `{ "meta": ..., "data": ... }`
- Error: `{ "meta": ..., "error": ... }`

## 7. Authentication and Authorization

Authentication:
- API key auth enforced globally for `/api/*` (`shared/middleware.py`).
- Accepted headers:
  - `X-API-Key: <key>`
  - `Authorization: Bearer <key>`
- Missing/invalid key => `401`.

Tenant gate:
- `X-Tenant-Id` required for API paths.
- TradSphere tenant config must exist; missing/invalid config => `400`.

Authorization:
- No role/permission framework found in TradSphere.
- Access control is tenant isolation + resource existence validation.

## 8. Core Business Logic

### 8.1 Accounts
- Source of truth for TradSphere membership: `TradSphere_Accounts`.
- Master `Accounts` table is used for metadata and onboarding validation.
- `GET /accounts?active=true` filters by `Accounts.active=1` while still returning only TradSphere rows.
- `POST /accounts`: insert-only (no upsert); rejects payload duplicates and existing TradSphere codes.
- `PUT /accounts`: update-only; account code must already exist in TradSphere.

### 8.2 Broadcast Calendar Semantics
- Broadcast week = Monday-Sunday.
- Broadcast month/year derived from week-ending Sunday.
- Used by:
  - `GET /broadcastCalendar`
  - `GET /estNums` period filters
  - schedule date validation against `broadcastMonth/broadcastYear`

### 8.3 estNums
- Requires at least one filter for `GET /estNums`.
- `month`/`quarter` require `year`; month-quarter consistency enforced.
- `hasSchedule` computed from `TradSphere_Schedules` existence (cached).
- `broadcastMonths`/`broadcastYears` computed from broadcast-week overlap.
- Calendar billing accounts exclude trailing overlap week when `flightEnd` equals cross-month week-end.

### 8.4 Stations
- `GET /stations` requires one of: `codes`, `accountCode`, `estNum`, `name`.
- `syscode` rule:
  - required for mediaType `CA`
  - forbidden for non-`CA`
  - response includes `syscode` only for `CA`.
- `deliveryMethod` inline object is forbidden on station create/update; use `/stations/deliveryMethods`.
- Contacts are attached to station response grouped by `contactType`.

### 8.5 Contacts + Station Contacts
- Duplicate email detection on create/update checks payload collisions + DB collisions (case-insensitive).
- Contact name parsing:
  - `name` can be split to `firstName`/`lastName`.
  - if both `firstName` and `lastName` provided, `name` is ignored.
- Phone validation:
  - supports US-style formats / 10-11 digits.
  - `office` may have extension (`x1234`), `cell` may not.
- `PUT /contacts/stationsContacts` with identity change (`stationCode`/`contactId`/`contactType`) performs:
  - deactivate old row (`active=0`)
  - insert/reactivate new row
  - update non-identity fields as needed.

### 8.6 Schedules + Weeks
- `billingCode` must match `YYQQ-ACCOUNTCODE-MARKETCODE`, quarter `01-04`, max length 20.
- `POST /schedules`:
  - required fields strictly validated across all rows.
  - `id` auto-generated UUID if missing.
  - `matchKey = sha256(scheduleId|lineNum|estNum|startDate|endDate)`.
  - rows deduped by `matchKey` within payload (last wins).
  - validates `estNum` and `stationCode` existence.
  - validates start/end dates resolve to provided broadcast month/year.
  - optional `w1..w5` must be complete/consecutive for covered week count.
  - recomputes `totalSpot` from weeks and `totalGross = totalSpot * rateGross`.
  - upserts schedule weeks derived from broadcast weeks.
- `PUT /schedules`:
  - immutable fields: `scheduleId`, `lineNum`, `estNum`, `startDate`, `endDate`, `matchKey`.
  - recomputes `totalSpot` from existing schedule-weeks rows.
  - recomputes `totalGross` from recomputed `totalSpot` and effective `rateGross`.
- `POST/PUT /schedules/weeks` validate week date range and schedule existence.

### 8.7 Schedule Import + PDF
- `POST /schedules/import` accepts exactly one `.txt` file.
- Parses fixed-width STRATA export format and delegates to `create_schedules_data`.
- Rejects import if any required field missing; includes line numbers.
- `GET /schedules/pdf`:
  - mode: `compact` or `detail`
  - billingType: `Calendar` or `Broadcast`
  - PDF payload cached per tenant+estNum for 90 days.
  - PDF cache invalidated on schedule/schedule-week writes.

## 9. MySQL Database Schema

No migration files were found for TradSphere. The table definitions below are inferred from query usage in `apps/tradsphere/api/v1/helpers/dbQueries.py`.

| Table | Purpose | Columns used in code | PK/FK (inferred) | Notes |
|---|---|---|---|---|
| `TradSphere_Accounts` | TradSphere account membership + settings | `accountCode`, `billingType`, `market`, `note` | PK likely `accountCode`; FK likely to `Accounts.code` (not enforced in code) | Source of truth for TradSphere account membership. |
| `Accounts` (master) | Master account metadata | `code`, `name`, `logoUrl`, `active` | PK likely `code` | Joined for metadata and active filter only. |
| `TradSphere_EstNums` | Estimate-number records | `estNum`, `accountCode`, `flightStart`, `flightEnd`, `mediaType`, `buyer`, `note` | PK likely `estNum`; FK likely `accountCode -> TradSphere_Accounts.accountCode` | `estNum` must be unsigned int. |
| `TradSphere_DeliveryMethods` | Delivery credentials/config for stations | `id`, `name`, `url`, `username`, `password`, `deadline`, `note` | PK likely `id` | Upserted on duplicate key (exact unique key in DB not visible). |
| `TradSphere_Stations` | Station metadata | `code`, `name`, `affiliation`, `mediaType`, `syscode`, `language`, `ownership`, `deliveryMethodId`, `note` | PK likely `code`; FK likely `deliveryMethodId -> DeliveryMethods.id` | `syscode` business-valid only for `CA`. |
| `TradSphere_Schedules` | Schedule rows | `id`, `scheduleId`, `lineNum`, `estNum`, `billingCode`, `mediaType`, `stationCode`, `broadcastMonth`, `broadcastYear`, `startDate`, `endDate`, `totalSpot`, `totalGross`, `rateGross`, `length`, `runtime`, `programName`, `days`, `daypart`, `rtg`, `matchKey` | PK likely `id`; FKs likely `estNum -> EstNums`, `stationCode -> Stations` | Upsert with `ON DUPLICATE KEY UPDATE`; unique key(s) not fully visible (likely includes `matchKey`). |
| `TradSphere_ScheduleWeeks` | Per-week spots for schedule | `id`, `scheduleId`, `weekStart`, `weekEnd`, `spots` | PK likely `id`; FK likely `scheduleId -> Schedules.id` | Upsert on duplicate key, probably unique `(scheduleId, weekStart)` or similar. |
| `TradSphere_Contacts` | Contact directory | `id`, `firstName`, `lastName`, `company`, `jobTitle`, `office`, `cell`, `email`, `active`, `note` | PK likely `id`; unique likely on `email` | Duplicate email checks done in app + DB. |
| `TradSphere_StationsContacts` | Station-contact link table | `id`, `stationCode`, `contactId`, `contactType`, `primaryContact`, `note`, `active` | PK likely `id`; FKs likely `stationCode -> Stations.code`, `contactId -> Contacts.id` | Upsert on duplicate key; identity-change on update uses deactivate + reinsert behavior. |

Nullable/default details (inferred):
- Optional nullable fields include: `market`, `note`, `affiliation`, `ownership`, `programName`, `rtg`, phone/company/job fields.
- `contacts.active` defaults to true when omitted in create path.
- `stationsContacts.primaryContact` defaults to false; `active` defaults to true in create path.
- `deliveryMethods.deadline` defaults to `"10 AM"` when omitted.

## 10. MySQL Table Relationships

Relationship map (inferred from joins/validation):

- `Accounts.code -> TradSphere_Accounts.accountCode` (logical reference for onboarding + metadata join)
- `TradSphere_Accounts.accountCode -> TradSphere_EstNums.accountCode`
- `TradSphere_DeliveryMethods.id -> TradSphere_Stations.deliveryMethodId`
- `TradSphere_EstNums.estNum -> TradSphere_Schedules.estNum`
- `TradSphere_Stations.code -> TradSphere_Schedules.stationCode`
- `TradSphere_Schedules.id -> TradSphere_ScheduleWeeks.scheduleId`
- `TradSphere_Stations.code -> TradSphere_StationsContacts.stationCode`
- `TradSphere_Contacts.id -> TradSphere_StationsContacts.contactId`

Cardinality (inferred):
- One account -> many estNums.
- One estNum -> many schedules.
- One schedule -> many schedule weeks.
- One delivery method -> many stations.
- Many-to-many stations <-> contacts via `TradSphere_StationsContacts`.

Soft-status behavior:
- `TradSphere_Contacts.active` and `TradSphere_StationsContacts.active` are soft-active flags (not hard deletes).
- `/contacts/stationsContacts/deactivate` sets `active = 0`.

Cascade behavior:
- No cascade rules are visible in code. DB-level FK cascade behavior needs verification.

## 11. API Routes and Endpoints

All routes require `X-Tenant-Id` + valid API key unless route docs/state says otherwise (none in TradSphere v1 bypass this).

| Method | Path | Handler | Main tables | Side effects |
|---|---|---|---|---|
| GET | `/api/tradsphere/v1/accounts` | `endpoints/core/accounts.py:get_accounts_route` | `TradSphere_Accounts`, `Accounts` | Read only. |
| POST | `/api/tradsphere/v1/accounts` | `create_accounts_route` | `TradSphere_Accounts`, `Accounts` | Inserts account rows; invalidates validation/read caches. |
| PUT | `/api/tradsphere/v1/accounts` | `update_accounts_route` | `TradSphere_Accounts` | Updates account rows; invalidates caches. |
| GET | `/api/tradsphere/v1/estNums` | `get_est_nums_route` | `TradSphere_EstNums`, `TradSphere_Schedules`, `TradSphere_Accounts` | Read only; computes broadcast metadata + hasSchedule. |
| POST | `/api/tradsphere/v1/estNums` | `create_est_nums_route` | `TradSphere_EstNums` | Inserts estNums; invalidates caches. |
| PUT | `/api/tradsphere/v1/estNums` | `update_est_nums_route` | `TradSphere_EstNums` | Updates estNums; invalidates caches. |
| GET | `/api/tradsphere/v1/stations` | `get_stations_route` | `TradSphere_Stations`, `TradSphere_DeliveryMethods`, `TradSphere_Schedules`, `TradSphere_EstNums`, `TradSphere_StationsContacts`, `TradSphere_Contacts` | Read only; attaches grouped contacts + optional delivery method detail. |
| POST | `/api/tradsphere/v1/stations` | `create_stations_route` | `TradSphere_Stations` | Upserts stations by DB duplicate key behavior; invalidates caches. |
| PUT | `/api/tradsphere/v1/stations` | `update_stations_route` | `TradSphere_Stations` | Updates station fields; invalidates caches. |
| GET | `/api/tradsphere/v1/stations/deliveryMethods` | `get_delivery_methods_route` | `TradSphere_DeliveryMethods` | Read only. |
| POST | `/api/tradsphere/v1/stations/deliveryMethods` | `create_delivery_methods_route` | `TradSphere_DeliveryMethods` | Upserts delivery methods; invalidates caches. |
| PUT | `/api/tradsphere/v1/stations/deliveryMethods` | `update_delivery_methods_route` | `TradSphere_DeliveryMethods` | Updates delivery methods; invalidates caches. |
| GET | `/api/tradsphere/v1/schedules` | `get_schedules_route` | `TradSphere_Schedules` | Read only. |
| GET | `/api/tradsphere/v1/schedules/pdf` | `get_schedules_pdf_route` | `TradSphere_Schedules`, `TradSphere_ScheduleWeeks`, `TradSphere_Stations`, `TradSphere_EstNums` | Generates PDF bytes; uses/refreshes tenant-scoped PDF cache. |
| POST | `/api/tradsphere/v1/schedules` | `create_schedules_route` | `TradSphere_Schedules`, `TradSphere_ScheduleWeeks` | Upserts schedules + weeks; invalidates validation + PDF caches. |
| PUT | `/api/tradsphere/v1/schedules` | `update_schedules_route` | `TradSphere_Schedules`, `TradSphere_ScheduleWeeks` | Updates schedules; recomputes totals; invalidates validation + PDF caches. |
| POST | `/api/tradsphere/v1/schedules/import` | `import_schedules_file_route` | `TradSphere_Schedules`, `TradSphere_ScheduleWeeks` | Parses uploaded `.txt`, then calls schedule create flow. |
| GET | `/api/tradsphere/v1/schedules/weeks` | `get_schedule_weeks_route` | `TradSphere_ScheduleWeeks` | Read only. |
| POST | `/api/tradsphere/v1/schedules/weeks` | `create_schedule_weeks_route` | `TradSphere_ScheduleWeeks` | Upserts week rows; invalidates PDF cache. |
| PUT | `/api/tradsphere/v1/schedules/weeks` | `update_schedule_weeks_route` | `TradSphere_ScheduleWeeks` | Updates week rows; invalidates PDF cache. |
| GET | `/api/tradsphere/v1/contacts` | `get_contacts_route` | `TradSphere_Contacts`, `TradSphere_StationsContacts` | Read only. |
| POST | `/api/tradsphere/v1/contacts` | `create_contacts_route` | `TradSphere_Contacts` | Inserts contacts; duplicate-email errors return structured details; invalidates caches. |
| PUT | `/api/tradsphere/v1/contacts` | `update_contacts_route` | `TradSphere_Contacts` | Updates contacts; duplicate-email errors; invalidates caches. |
| GET | `/api/tradsphere/v1/contacts/stationsContacts` | `get_stations_contacts_route` | `TradSphere_StationsContacts` | Read only. |
| POST | `/api/tradsphere/v1/contacts/stationsContacts` | `create_stations_contacts_route` | `TradSphere_StationsContacts` | Upserts links; invalidates caches. |
| PUT | `/api/tradsphere/v1/contacts/stationsContacts` | `update_stations_contacts_route` | `TradSphere_StationsContacts` | May deactivate old identity row + insert/reactivate new row; invalidates caches when changed. |
| PUT | `/api/tradsphere/v1/contacts/stationsContacts/deactivate` | `deactivate_stations_contacts_route` | `TradSphere_StationsContacts` | Sets `active=0` for given IDs; invalidates caches when changed. |
| GET | `/api/tradsphere/v1/broadcastCalendar` | `get_broadcast_calendar_route` | none | Broadcast date computation only. |
| GET | `/api/tradsphere/v1/ui/main/selections` | `get_ui_main_selections_route` | `TradSphere_Accounts`, `Accounts` | Read only, active account selections. |
| GET | `/api/tradsphere/v1/ui/main/load` | `get_ui_main_load_route` | multiple (`Accounts`, `EstNums`, `Schedules`, `Stations`) | Read-only aggregate payload for frontend load. |

Notes:
- All route handlers include structured docstrings with examples and requirements.
- No `DELETE` routes currently exist.

## 12. Frontend Behavior

No frontend application code was found under `apps/tradsphere`.

What exists:
- Backend UI-support endpoints:
  - `/ui/main/selections`: account dropdown list.
  - `/ui/main/load`: account + estnum + station payload for a selected account.

Implication:
- TradSphere UI is likely external or in another repository/module.
- API response contract in these endpoints should be treated as frontend-facing and stable.

## 13. Background Jobs, Cron, Queues, and Scheduled Logic

Inside TradSphere app code:
- No cron jobs, queue workers, or async background task processors were found.

Asynchronous-like behavior:
- PDF cache entries are stored in shared file cache and lazily refreshed.
- Axiom logging internals use a queue in shared logger, but that is infrastructure-level logging, not TradSphere business job logic.

## 14. External Integrations

- MySQL: primary datastore (`shared/db.py`).
- File upload handling for schedule import (`python-multipart`).
- PDF generation via `fpdf2` for schedules report.
- Optional Axiom log ingestion via shared logger (app-scoped env).
- Tenant config via YAML files (`/etc/secrets` or `etc/secrets`).

No payment providers, broker APIs, SMS providers, or webhook handlers were found in TradSphere code.

## 15. Validation Rules and Constraints

Key validation sources:
- Endpoint-level query/body checks in `endpoints/core/*.py`.
- Business validation in helpers.
- SQL-level duplicate handling (`ON DUPLICATE KEY UPDATE` on several tables).

Examples:
- Unknown query params rejected globally.
- `accountCode`, `stationCode` normalized uppercase.
- `email` normalized lowercase and validated by regex.
- `contactType` and `mediaType` enforced against tenant enums.
- `billingCode` format parsed/validated centrally (`clientBillingCode.py`).
- Date range checks (`start <= end`) across estNums/schedules/weeks/import.
- Schedule `w1..w5` must match broadcast-week count contiguously if any provided.

## 16. Security Rules

- Tenant isolation:
  - `X-Tenant-Id` required and tenant config loaded per request.
  - TradSphere validator gate blocks access when tenant lacks TradSphere config.
- API key authentication required for `/api/*`.
- Input validation:
  - strict query param whitelist
  - body validation and business rules per helper.
- SQL safety:
  - queries parameterized with `%s` placeholders.
  - dynamic table/identifier quoting guarded by regex in `dbQueries.py`.
- Secrets:
  - loaded from env and tenant configs; avoid hardcoding.
- CORS/rate-limiting:
  - No explicit CORS middleware or route-level rate limiter found in TradSphere path.
- Data envelope:
  - response/error wrapped to consistent shape via middleware.

## 17. Testing

No TradSphere test suite was found (`tests/`, `pytest` config, and app-specific test files not present).

Practical current state:
- Validation is strongly encoded in runtime helper code.
- Manual/API-level regression checks are required before merge.

Gaps:
- Missing automated coverage for schedule import parsing, broadcast filtering, and identity-changing station-contact updates.

## 18. Coding Conventions

Observed patterns:
- Keep route functions thin; push logic into helpers.
- Keep SQL in `helpers/dbQueries.py`.
- Normalize identifiers early (uppercase codes, lowercase emails).
- Raise `ValueError` in helpers for business validation, convert to `HTTP 400` in endpoints.
- Use tenant-scoped shared cache keys with TradSphere prefixes:
  - `tradsphere_validation::...`
  - `tradsphere_db_reads::...`
  - `tradsphere_pdf::schedules_data::...`
- Route docstrings are mandatory and detailed (description, requests, responses, requirements).

## 19. Safe Change Guidelines for Future Agents

1. Read this file and root `AGENTS.md` before editing code.
2. Do not change table names/schema assumptions without checking all helper + query usage.
3. Do not bypass tenant validation or API-key checks.
4. Do not hardcode secrets, tokens, passwords, or tenant IDs.
5. Preserve account source-of-truth rules:
   - TradSphere membership from `TradSphere_Accounts`.
   - master `Accounts` for metadata/onboarding checks.
6. Preserve broadcast-calendar semantics for period logic.
7. Preserve schedule invariants:
   - immutable matchKey source fields on PUT
   - recomputed totals from week spots
   - billingCode format enforcement.
8. When adding/changing endpoints:
   - update route docstrings with realistic examples
   - keep unknown-query-param behavior in mind.
9. Invalidate relevant caches when write paths change.
10. Update this `AGENTS.md` whenever adding/changing:
   - routes
   - tables/columns
   - business rules
   - cache keys/TTL behavior.
11. Run relevant manual/API checks since no automated test suite is present.

## 20. Unknowns / Needs Verification

- Exact MySQL DDL (types, nullable/default definitions, FK constraints, index names) is not in repo; schema here is inferred from query usage.
- Exact unique keys that trigger `ON DUPLICATE KEY UPDATE` for:
  - `TradSphere_DeliveryMethods`
  - `TradSphere_Schedules`
  - `TradSphere_ScheduleWeeks`
  - `TradSphere_StationsContacts`
  need DB verification.
- No dedicated frontend code for TradSphere exists in this repo; external UI integration points should be confirmed with product/client teams.
- No automated tests are present; expected regression protocol (manual vs CI) needs confirmation.
- Existing legacy docs may mention routes not currently implemented (example: `/contacts/byStationCodes` appears in older docs but route is not registered in current `router.py`).
