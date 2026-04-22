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
  - `PUT /stations/deliveryMethods`
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
- Supported `resultType` values:
  - `Month`
  - `Year`
  - `Start Date`
  - `End Date`
  - `Num of Week`
  - `FirstDate of Week`
  - `LastDate of Week`
  - `Week Num of Month`
  - `Week Num of Year`
