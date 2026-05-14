# Project Knowledge

## Executive Summary
Checkpoint created before analysis: `git stash` snapshot at `stash@{0}` with label `checkpoint-knowledge-doc-2026-05-13`. Working tree was restored after snapshot.

This repository is a multi-tenant FastAPI monorepo for internal business apps under “TheSphereWorks.”  
Core active user experience is currently TradSphere + shared Auth. Other backend domains (SpendSphere, Shiftzy, FundSphere, OpsSphere) are implemented as API sub-apps. Frontend app rollout is partial: TradSphere is production-facing, while other app UIs are mostly placeholders.

Assumptions:
- “Frontend Backend Repo Structure” conversation content was not available in the workspace, so this report is based on code and docs in the repo.
- Status is assessed from current files on May 13, 2026.

## Repository Overview
High-level layout:
- Backend entrypoint and API gateway at [`main.py`](/Users/haitruongh/Developer/fastapi/main.py).
- Domain apps under `apps/`:
  - `spendsphere`, `shiftzy`, `fundsphere`, `tradsphere`, `opssphere`, plus `auth`.
- Shared backend infrastructure under `shared/` (middleware, tenancy, auth, DB, logging, cache helpers).
- Frontend workspace under `frontend/`:
  - App: `frontend/apps/tradsphere`
  - Shared frontend modules: `frontend/shared`
  - Home/portal pages: `frontend/apps/home`
- Operational docs and SQL under `docs/`.

Architectural pattern:
- API gateway mounts sub-apps by prefix.
- Each app has `api/main.py` + `v1/router.py` + endpoint/helper modules.
- Reuse-first policy via `shared/` backend modules.
- Frontend uses app-local code + shared auth/components/utilities.

## Tech Stack
Backend:
- Python + FastAPI (`fastapi`, `uvicorn`) from [`requirements.txt`](/Users/haitruongh/Developer/fastapi/requirements.txt)
- MySQL via `mysql-connector-python`
- Pydantic v2
- YAML tenant configs (`pyyaml`)
- Google integrations:
  - Google Ads SDK (`google-ads`)
  - Google Sheets/Google APIs
- PDF generation (`fpdf2`, `openpyxl`)
- Supabase REST/Auth integration (custom client in backend)

Frontend:
- React 18 + TypeScript + Vite in [`frontend/apps/tradsphere/package.json`](/Users/haitruongh/Developer/fastapi/frontend/apps/tradsphere/package.json)
- Tailwind CSS
- Radix UI (dialog), TanStack Table, Recharts, framer-motion, lucide-react
- Custom auth/session and API request hooks in `frontend/shared`

Build/deploy/tooling:
- Root npm scripts for frontend build/sync + backend serve in [`package.json`](/Users/haitruongh/Developer/fastapi/package.json)
- Render deployment config in [`render.yaml`](/Users/haitruongh/Developer/fastapi/render.yaml)
- No detected repo-level lint/format config (`eslint`, `prettier`, `ruff`, `black`, `pytest.ini` not found)

Testing:
- Python `unittest`-style tests mainly in `tests/auth` and minimal TradSphere permission tests.

## Architecture
Backend runtime architecture:
- Root app mounts:
  - `/api/spendsphere`
  - `/api/shiftzy`
  - `/api/fundsphere`
  - `/api/tradsphere`
  - `/api/opssphere`
  - `/api/auth`
- Middleware order in root is implemented in [`main.py`](/Users/haitruongh/Developer/fastapi/main.py) and [`shared/middleware.py`](/Users/haitruongh/Developer/fastapi/shared/middleware.py):
  1. tenant context
  2. request/response logger
  3. API key / bearer auth
  4. timing
- Sub-apps apply response envelope middleware.

Frontend architecture:
- Single-page app with manual history-based routing in [`frontend/apps/tradsphere/src/App.tsx`](/Users/haitruongh/Developer/fastapi/frontend/apps/tradsphere/src/App.tsx), not React Router.
- Route guards via shared auth state and permission checks.
- Backend called via relative `/api/...` endpoints.

Auth architecture:
- JWT bearer (Supabase) + legacy API key compatibility mode.
- Backend authorization and tenant/app role resolution via `shared/auth/*`.
- Frontend auth provider stores session in localStorage and refreshes with Supabase endpoints.

## Frontend Structure
Main frontend app:
- [`frontend/apps/tradsphere/src/App.tsx`](/Users/haitruongh/Developer/fastapi/frontend/apps/tradsphere/src/App.tsx): route switching, guards, auth pages, workspace pages.
- `src/pages/*`: main TradSphere pages (Estimate Numbers, Contacts, Stations, Admin Users, Profile).
- `src/components/dashboard/*`: heavy UI/domain components.
- `src/hooks/useApiRequest.ts`: shared request layer with toast/error handling.

Shared frontend modules:
- `frontend/shared/auth/*`: auth provider, guards, Supabase client, permission helpers.
- `frontend/shared/api/authHeaders.ts`: tenant + auth headers.
- `frontend/shared/components/*`: shared UI primitives.
- `frontend/shared/cache/*`: generic cache policy/store helpers.

Home pages:
- `frontend/apps/home/src/*` used by TradSphere app through alias `@home`.

## Backend Structure
Core backend layers:
- Entrypoint gateway: [`main.py`](/Users/haitruongh/Developer/fastapi/main.py)
- Shared infra:
  - [`shared/middleware.py`](/Users/haitruongh/Developer/fastapi/shared/middleware.py)
  - [`shared/tenant.py`](/Users/haitruongh/Developer/fastapi/shared/tenant.py)
  - [`shared/db.py`](/Users/haitruongh/Developer/fastapi/shared/db.py)
  - [`shared/response.py`](/Users/haitruongh/Developer/fastapi/shared/response.py)
  - [`shared/exceptionHandlers.py`](/Users/haitruongh/Developer/fastapi/shared/exceptionHandlers.py)

Domain apps:
- SpendSphere: budget/allocation/Google Ads/sheets/cache heavy backend.
- Shiftzy: workforce scheduling CRUD + bootstrap + PDF.
- FundSphere: master budget control sync/update with sheets + DB.
- TradSphere: accounts, estNums, schedules, contacts, stations, delivery methods, broadcast calendar, UI helper endpoints.
- OpsSphere: GA4-based report generation + signed public report links.
- Auth app: sessions, invitations, admin user/role scope management.

## Data Flow
Typical frontend-to-backend flow:
1. User signs in with email/password against Supabase auth API.
2. Frontend stores session tokens in localStorage.
3. API calls use `Authorization: Bearer <token>` + `X-Tenant-Id`.
4. Backend root middleware validates tenant, auth mode, and app permissions.
5. Endpoint executes helper/DB/third-party logic.
6. Response wrapped in `{ meta, data }` or `{ meta, error }`.

State management:
- Frontend uses React hooks and local component state.
- Shared cache utility uses localStorage or memory with TTL policies.
- Backend uses file-based cache (`caches.json`) and tenant-scoped cache helpers.

Error handling:
- Backend wraps errors via shared envelope and custom exception handlers.
- Frontend request hook parses envelope-style errors and raises toast notifications.

Config usage:
- Env loaded from `/etc/.env` then `etc/.env`.
- Tenant-specific config loaded per request from `/etc/secrets/<tenant>.yaml` or `etc/secrets/<tenant>.yaml`, with include support.
- TradSphere frontend Vite `envDir` points to `etc/` (important local-dev behavior).

## Important Files
| File | Purpose | Key Dependents | Edit Carefully |
|---|---|---|---|
| [`main.py`](/Users/haitruongh/Developer/fastapi/main.py) | Root gateway app, middleware wiring, app mounts, frontend static fallback | All API traffic and frontend serving | Yes |
| [`shared/middleware.py`](/Users/haitruongh/Developer/fastapi/shared/middleware.py) | Tenant/auth/logging/response wrapping pipeline | Every request | Yes |
| [`shared/tenant.py`](/Users/haitruongh/Developer/fastapi/shared/tenant.py) | Tenant config load/validation/context vars | All tenant-scoped behavior | Yes |
| [`shared/requestValidation.py`](/Users/haitruongh/Developer/fastapi/shared/requestValidation.py) | Unknown query param rejection | All route handlers | Yes |
| [`shared/db.py`](/Users/haitruongh/Developer/fastapi/shared/db.py) | MySQL connection pooling + transaction helpers | Most DB helpers | Yes |
| [`shared/auth/dependencies.py`](/Users/haitruongh/Developer/fastapi/shared/auth/dependencies.py) | Bearer auth + permission enforcement | Auth, TradSphere protected routes | Yes |
| [`shared/auth/permissions_repo.py`](/Users/haitruongh/Developer/fastapi/shared/auth/permissions_repo.py) | Tenant/app role resolution from Supabase tables | Authorization checks | Yes |
| [`shared/auth/supabase_client.py`](/Users/haitruongh/Developer/fastapi/shared/auth/supabase_client.py) | Supabase REST/Auth calls | Auth endpoints, permission repo | Yes |
| [`apps/tradsphere/api/v1/router.py`](/Users/haitruongh/Developer/fastapi/apps/tradsphere/api/v1/router.py) | TradSphere endpoint registration + permission dependency | TradSphere API | Yes |
| [`apps/auth/api/v1/endpoints/admin.py`](/Users/haitruongh/Developer/fastapi/apps/auth/api/v1/endpoints/admin.py) | Admin scope, invites, user access management | Auth admin UI and APIs | Yes |
| [`apps/tradsphere/api/v1/helpers/dbQueries.py`](/Users/haitruongh/Developer/fastapi/apps/tradsphere/api/v1/helpers/dbQueries.py) | Core TradSphere SQL query layer | TradSphere endpoints | Yes |
| [`apps/spendsphere/api/v1/helpers/spendsphereHelpers.py`](/Users/haitruongh/Developer/fastapi/apps/spendsphere/api/v1/helpers/spendsphereHelpers.py) | SpendSphere cache/data orchestration | SpendSphere endpoints | Yes |
| [`frontend/apps/tradsphere/src/App.tsx`](/Users/haitruongh/Developer/fastapi/frontend/apps/tradsphere/src/App.tsx) | Frontend route controller and guard composition | Entire frontend UX | Yes |
| [`frontend/apps/tradsphere/src/hooks/useApiRequest.ts`](/Users/haitruongh/Developer/fastapi/frontend/apps/tradsphere/src/hooks/useApiRequest.ts) | Standard request + toast/error behavior | Most frontend API calls | Yes |
| [`frontend/shared/auth/AuthProvider.tsx`](/Users/haitruongh/Developer/fastapi/frontend/shared/auth/AuthProvider.tsx) | Session storage, token refresh, access profile loading | Auth/permission UX | Yes |
| [`frontend/shared/api/authHeaders.ts`](/Users/haitruongh/Developer/fastapi/frontend/shared/api/authHeaders.ts) | Builds tenant/auth headers + legacy fallback | Frontend API requests | Yes |
| [`frontend/apps/tradsphere/vite.config.ts`](/Users/haitruongh/Developer/fastapi/frontend/apps/tradsphere/vite.config.ts) | Vite aliases/env location | Frontend build and local dev | Yes |
| [`render.yaml`](/Users/haitruongh/Developer/fastapi/render.yaml) | Deploy build/start pipeline | Production deployment | Yes |
| [`docs/auth_phase1.md`](/Users/haitruongh/Developer/fastapi/docs/auth_phase1.md) | Auth rollout policy and migration flags | Team implementation alignment | Yes |
| [`etc/.env`](/Users/haitruongh/Developer/fastapi/etc/.env) | Local env config (contains sensitive values) | Local runtime for backend/frontend env vars | Extremely |

## Setup Instructions
Dependencies:
- Python 3.11+ (project currently has `.venv` and compiled artifacts from 3.11)
- Node.js + npm
- MySQL database
- Access to tenant YAML files in `etc/secrets/` (or `/etc/secrets/`)
- Supabase project keys for auth features
- Google service account credentials for Sheets/Ads features

Install:
1. `python3 -m venv .venv`
2. `source .venv/bin/activate`
3. `pip install -r requirements.txt`
4. `npm --prefix frontend/apps/tradsphere install`

Run backend:
1. `uvicorn main:app --reload --host 0.0.0.0 --port 8000`
2. Health check: `GET /ping`

Run frontend (Vite):
1. `npm --prefix frontend/apps/tradsphere run dev`

Build frontend and sync into backend-served static:
1. `npm run build:tradsphere`
2. `npm run sync-ui:tradsphere`
3. `npm run serve:api`

Testing:
- No unified test command is defined in root scripts.
- Existing tests are Python `unittest` modules under `tests/auth` and `tests/tradsphere`.

Known local gotchas:
- `Vite envDir` is set to `etc/`, so `frontend/apps/tradsphere/.env.local` may not be the active env source.
- No Vite proxy config was found; `/api/*` calls from Vite dev origin can fail unless routed appropriately.
- `etc/.env` and some `.env.local` files appear tracked locally with sensitive values.
- `static/index.html` fallback path referenced in backend is absent; frontend serving expects `apps/tradsphere/ui_dist/index.html`.

## Development Conventions
Observed conventions:
- Backend route groups by app/version under `apps/<app>/api/v1`.
- Route functions generally include structured docstrings with examples and requirements.
- Shared reusable backend logic belongs in `shared/`.
- Query params are strict by default; unknown params return 400.
- API responses are envelope-wrapped (`meta` + `data/error`).
- Tenant is mandatory (`X-Tenant-Id`) on most API paths.
- Frontend uses:
  - `@`, `@home`, `@shared` path aliases
  - Functional React components
  - Tailwind utility styling
  - Shared auth/request/cache modules in `frontend/shared`.

Recommended boundary rules going forward:
1. Keep UI composition, client cache, optimistic UX, and route guards in frontend.
2. Keep tenant validation, authorization, DB access, third-party secrets, and business rules in backend.
3. Share contracts via generated schemas/types from backend OpenAPI or explicit shared DTO package to avoid drift.
4. Keep app-specific business logic in app helpers; move generic logic into `shared/` only.

## Current Status
Appears complete:
- Multi-app FastAPI gateway and middleware stack.
- TradSphere backend core endpoints.
- Auth Phase 1 (session, invites, admin scope APIs) with Supabase integration.
- TradSphere frontend with auth pages, workspace shell, core pages.
- OpsSphere signed public report flow.
- Deployment pipeline for building/syncing TradSphere UI.

Appears partially implemented:
- Frontend rollout for SpendSphere/FundSphere/Shiftzy/OpsSphere (marked “Coming soon” in nav).
- Auth migration still in compat mode design (legacy API key fallback retained).
- Cross-app frontend platform is present but only one app has full UI.
- Test coverage is focused on auth and a small TradSphere slice; broad domain coverage is limited.

Appears missing or inconsistent:
- SpendSphere v2 paths referenced in docs/instructions are not present in code tree.
- Root docs mention local frontend `.env.local`, but Vite config points env loading to `etc/`.
- No detected centralized lint/format/test automation configuration.
- `apps/tradsphere/ui` appears mostly stale (contains only lockfile), while `ui_dist` is actual served output.

TODO/placeholders/unfinished signs:
- “Coming soon” entries for non-TradSphere app cards.
- Some legacy/deprecated compatibility paths mentioned in app docs.
- Frontend contains fallback error text implying a missing estNum search endpoint even though backend route now exists (suggests stale messaging logic).

## Risks and Recommendations
Key risks:
1. Security: sensitive keys are present in local env files tracked in working tree; risk of accidental commit/leak.
2. Auth transition risk: compat mode + legacy API key fallback can prolong insecure access paths.
3. Runtime config drift: docs/env expectations do not fully match Vite envDir behavior.
4. Maintainability: several very large files (3k+ lines) in both frontend and backend are high-fragility.
5. Test gap: limited automated coverage outside auth; regression risk in heavy business logic areas.
6. Caching/storage risk: file-based cache (`caches.json`) can be fragile across multi-instance deployments.
7. Frontend dev UX risk: no API proxy in Vite config; local integration can be confusing/broken.

Recommended next improvements:
1. Move all secrets to untracked env templates + secret manager; rotate exposed keys.
2. Add CI baseline: lint, typecheck, unit tests, smoke integration tests.
3. Split oversized modules (especially `StationModal.tsx`, TradSphere DB helpers, SpendSphere helpers).
4. Standardize API contract generation and frontend typed clients.
5. Align env documentation with actual Vite `envDir` behavior.
6. Define explicit auth cutover plan/date from compat to `jwt_only`.
7. Add broader test suites for SpendSphere/FundSphere/Shiftzy/TradSphere data rules.
8. Evaluate Redis/shared cache for multi-instance-safe caching.

## Project Knowledge for ChatGPT
This repo is a multi-tenant FastAPI monorepo for TheSphereWorks business apps. The backend gateway (`main.py`) mounts app APIs under `/api/*` and enforces tenant context, auth, logging, timing, and response envelopes. Core backend domains are SpendSphere, Shiftzy, FundSphere, TradSphere, OpsSphere, plus a shared Auth app.

TradSphere is the most complete end-to-end product today: it has substantial backend APIs and a React+TypeScript frontend (`frontend/apps/tradsphere`) with auth, permissions, admin users, and data workflows (accounts, estimate numbers, schedules, contacts, stations). Authentication uses Supabase JWT with tenant/app role checks, but supports legacy API-key fallback in compat mode during migration.

Tenant config is loaded per request from YAML files (`/etc/secrets` or `etc/secrets`) and can include other YAML files. Most endpoints require `X-Tenant-Id`. DB access is MySQL via shared helpers. Third-party integrations include Google Ads/Sheets and GA4-based reporting (OpsSphere). Caching is file-based (`caches.json`) with tenant-scoped keys.

Frontend architecture is currently app-centric and mostly TradSphere-focused; other app UIs are placeholders. Frontend uses custom routing logic (not React Router), shared auth/provider modules, and a shared API request layer with envelope-aware error handling. Build/deploy syncs TradSphere static assets into `apps/tradsphere/ui_dist` so backend can serve them.

Known constraints: large modules in both frontend and backend, limited automated test breadth, and some config/doc mismatches (notably frontend env loading). Before adding major features, prioritize secret hygiene, CI quality gates, module decomposition, and stronger cross-app API contract typing.

## Open Questions
1. The “Frontend Backend Repo Structure” conversation content was not accessible in the workspace.  
Needed: pasted transcript or file so this document can merge its context explicitly.
2. Which app(s) are considered production-critical today besides TradSphere?  
Needed: current production traffic/ownership map.
3. Is SpendSphere v2 intentionally removed or still planned?  
Needed: authoritative versioning roadmap.
4. What is the intended local frontend dev flow for API calls (Vite proxy vs backend-served UI)?  
Needed: preferred workflow and canonical command set.
5. Are `etc/.env` and `frontend/apps/tradsphere/.env.local` intentionally tracked locally?  
Needed: security policy on secret handling and git hygiene.
6. What CI pipeline/tooling is expected (pytest/unittest, linting, typecheck, formatting)?  
Needed: target standards and required checks.
7. Should OpsSphere and Auth be included in official architecture docs (AGENTS currently emphasizes four apps)?  
Needed: updated product boundary definition.
