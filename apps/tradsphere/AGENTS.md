# AGENTS.md (TradSphere)

## Scope

- Applies to `apps/tradsphere/**`.
- Root-level `AGENTS.md` still applies; this file adds TradSphere-specific rules.

## App Overview

- TradSphere is mounted at `/api/tradsphere`.
- Current version path is `/api/tradsphere/v1`.
- App entrypoint: `apps/tradsphere/api/main.py`.
- Router entrypoint: `apps/tradsphere/api/v1/router.py`.

## Architecture Rules

- Keep SpendSphere-style separation:
  - `endpoints/` for route handlers
  - `helpers/config.py` for tenant/app config parsing + validation
  - `helpers/dbQueries.py` for SQL access
  - `helpers/queryParsing.py` for shared query parsing
  - endpoint helpers (`accounts.py`, `estNums.py`, `stations.py`, `contacts.py`)
  - `helpers/accountValidation.py` for TradSphere-specific validation/cache checks
- Do not move TradSphere account-code validation into `shared/`.

## Tenant Safety (Mandatory)

- TradSphere routes must be inaccessible when tenant lacks TradSphere config.
- App-level tenant validation must fail with `400` (via root tenant middleware validator registry).
- Never store tenant-specific state in globals.
- All validation caches must be tenant-scoped.

## Tenant Config Expectations

- TradSphere section is required (scoped env via `TRADSPHERE_*` flattening).
- Config parser source of truth: `apps/tradsphere/api/v1/helpers/config.py`.
- DB table defaults:
  - `accounts: TradSphere_Accounts`
  - `masterAccounts: Accounts`
  - `estnums: TradSphere_EstNums`
  - `deliveryMethods: TradSphere_DeliveryMethods`
  - `stations: TradSphere_Stations`
  - `schedules: TradSphere_Schedules`
  - `schedulesWeeks: TradSphere_ScheduleWeeks`
  - `contacts: TradSphere_Contacts`
  - `stationsContacts: TradSphere_StationsContacts`
- Enum defaults:
  - `mediaType: ["TV","RA","CA","OD","NP","CINE","OTT"]`
  - `contactType: ["REP","TRAFFIC","BILLING"]`
- Cache TTL resolution:
  - `tradsphere.CACHE.db_validation_ttl_time`
  - fallback `tradsphere.CACHE.ttl_time`
  - default `300`

## Validation + Cache Rules

- Use tenant-scoped shared cache infra for DB existence checks.
- Cache keys must stay TradSphere-namespaced.
- Cached existence checks include:
  - account codes (TradSphere + master accounts)
  - station codes
  - delivery method ids
  - contact ids
  - stationsContacts ids

## AccountCode Behavior Rules

- Account-code normalization/validation remains local to TradSphere helpers (`helpers/accountValidation.py`).
- `accountCode` values are normalized to uppercase for all reads/writes.
- `TradSphere_Accounts` is the source of truth for TradSphere membership.
- Master `Accounts` is only used for:
  - onboarding validation in `POST /accounts` (code must exist in master)
  - metadata join in `GET /accounts` (`name`, `logoUrl`, `active`)
  - active filter in `GET /accounts` (`active=true` uses `Accounts.active = 1`)
- `GET /accounts` must return only rows that exist in `TradSphere_Accounts` (never all master accounts).
- `POST /accounts` is insert-only for TradSphere rows:
  - duplicates in payload are `400`
  - existing TradSphere rows are `400`
  - no upsert
- `PUT /accounts` is update-only for existing TradSphere rows:
  - unknown `accountCode` in `TradSphere_Accounts` is `400`
  - no implicit create/upsert
- Non-accounts endpoints that validate `accountCode` (for example estNums/schedules) must validate against `TradSphere_Accounts`, not master `Accounts`.

## Stations `syscode` Rules

- `GET /stations` response masking rule:
  - return `syscode` only when `mediaType = "CA"`
  - for all other media types, omit `syscode` from the response payload
  - supports query toggles:
    - `deliveryMethodDetail` (default `false`) for delivery method detail level
      - `true`: full deliveryMethod object (`id`, `name`, `url`, `username`, `deadline`, `note`)
      - `false`: deliveryMethod returns only `id` and `name`
    - `contactDetail` (default `false`) for REP contact detail level
      - contacts are always grouped by `contactType`
      - `REP`:
        - `true`: full REP contact objects
        - `false`: short REP objects `{id, name, email}`
      - non-`REP`: always list of emails
  - contacts come from active `stationsContacts` + active `contacts`
- `POST /stations` validation rule:
  - `syscode` must be an unsigned integer when provided
  - when `mediaType = "CA"`, `syscode` is required
  - when `mediaType != "CA"`, `syscode` is not allowed
- `PUT /stations` validation rule:
  - `syscode` must be an unsigned integer when provided
  - when an item sets `mediaType = "CA"`, the same item must include `syscode`
  - when an item sets `mediaType != "CA"`, the same item must not include `syscode`
  - when `syscode` is updated without `mediaType`, existing station media type must be `CA`

## API Behavior Rules

- Keep response envelope behavior consistent with app middleware stack.
- Unknown query params must be rejected by shared request validation.
- Provide clear `400` validation errors.
- No `DELETE` routes.
- Route docstrings are mandatory and must include:
  - short description
  - example request(s)
  - example response (envelope shape)
  - requirements/notes

## Endpoint Coverage (v1)

- Accounts:
  - `GET /accounts`
  - `POST /accounts`
  - `PUT /accounts`
- EstNums:
  - `GET /estNums`
  - `POST /estNums`
  - `PUT /estNums`
- Stations + DeliveryMethods:
  - `GET /stations`
  - `GET /stations/deliveryMethods`
  - `POST /stations`
  - `PUT /stations`
  - `POST /stations/deliveryMethods`
  - `PUT /stations/deliveryMethods`
- Schedules + ScheduleWeeks:
  - `GET /schedules`
  - `POST /schedules`
  - `PUT /schedules`
  - `POST /schedules/import`
  - `GET /schedules/weeks`
  - `POST /schedules/weeks`
  - `PUT /schedules/weeks`
- Contacts + StationsContacts:
  - `GET /contacts`
  - `GET /contacts/byStationCodes`
  - `POST /contacts`
  - `PUT /contacts`
  - `GET /contacts/stationsContacts`
  - `POST /contacts/stationsContacts`
  - `PUT /contacts/stationsContacts`
- Broadcast Calendar:
  - `GET /broadcastCalendar`

## Broadcast Calendar Rules

- Broadcast week is Monday-Sunday.
- Broadcast month/year is assigned by the week-ending Sunday.
- First week of broadcast month is the week containing Gregorian day 1.
- Period filtering that depends on month/year/quarter (for example `GET /estNums`) must use broadcast-week overlap via `helpers/broadcastCalendar.py`, not Gregorian month-boundary checks.
- For `GET /estNums`, when both `month` and `quarter` are provided, month-quarter consistency must be validated.
- Supported `resultType` values:
  - `month`
  - `year`
  - `start_date`
  - `end_date`
  - `num_of_week`
  - `firstdate_of_week`
  - `lastdate_of_week`
  - `week_num_of_month`
  - `week_num_of_year`
