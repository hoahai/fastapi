# AGENTS.md

## Overview

-   This repo is a multi-tenant FastAPI service with two apps:
    **SpendSphere** and **Shiftzy**.
-   The root app mounts both under `/api` and applies shared middleware
    (auth, tenant, logging).
-   The system is strictly tenant-isolated and must maintain
    architectural consistency across apps.

------------------------------------------------------------------------

## Architecture

### Entrypoints

-   `main.py` is the root FastAPI app and mounts app-specific sub-apps.
-   `apps/spendsphere/api/main.py` bootstraps SpendSphere (v1 + v2).
-   `apps/shiftzy/api/main.py` bootstraps Shiftzy (v1).

### Example Full Routes

-   SpendSphere v1: `/api/spendsphere/v1/...`
-   SpendSphere v2: `/api/spendsphere/v2/...`
-   Shiftzy v1: `/api/shiftzy/v1/...`

------------------------------------------------------------------------

## Middleware

### Middleware Order (Root App)

1.  `tenant_context_middleware`
2.  `request_response_logger_middleware`
3.  `api_key_auth_middleware`
4.  `timing_middleware`

-   App-level `response_envelope_middleware` runs inside each sub-app
    stack (SpendSphere/Shiftzy).

### Response Envelope Format

Successful response:

``` json
{
  "meta": { "...": "..." },
  "data": { "...": "..." }
}
```

Error response:

``` json
{
  "meta": { "...": "..." },
  "error": {
    "message": "...",
    "detail": "...",
    "code": "OPTIONAL"
  }
}
```

All endpoints must respect this structure.

------------------------------------------------------------------------

## Environment

-   `shared/utils.load_env` loads `/etc/.env` first, then `etc/.env`.
-   API key auth reads `API_KEY_REGISTRY` and accepts:
    -   `X-API-Key`
    -   `Authorization: Bearer ...`
-   MySQL settings come from:
    -   `DB_HOST`
    -   `DB_PORT`
    -   `DB_USER`
    -   `DB_PASSWORD`
    -   `DB_NAME`

Do not introduce new environment loading mechanisms.

------------------------------------------------------------------------

## Tenancy

-   Every API request must include `X-Tenant-Id`.
-   Tenant configs live in:
    -   `/etc/secrets/<tenant>.yaml`
    -   `etc/secrets/<tenant>.yaml`
-   Tenant configs may `include` other YAML files.
-   The root app recognizes legacy prefixes like `/spendsphere/api` for
    tenant validation and API key checks.

### Tenant Config Requirements

**SpendSphere requires:** - `SERVICE_BUDGETS` - `SERVICE_MAPPING` -
`ADTYPES` - `DB_TABLES` - `SPREADSHEET` - `GOOGLE_ADS_NAMING`

**SpendSphere custom-route access control:** - `FEATURE_FLAGS` (object)
- custom routes must be gated by feature flag keys (for example:
`FEATURE_FLAGS.budget_managements: true`)

**Shiftzy requires:** - `START_WEEK_NO` - `START_DATE` (must be
Monday) - `WEEK_BEFORE` - `WEEK_AFTER` - `POSITION_AREAS_ENUM` -
`SCHEDULE_SECTIONS_ENUM` - `DB_TABLES` - `PDF` (optional)

------------------------------------------------------------------------

## Route Documentation Requirements (Mandatory)

All API route functions must include a clear and structured docstring.

Every route must include:

1.  A short description (1--2 sentences)
2.  Example request(s)
3.  Example response
4.  Requirements or notes (if applicable)

Required format:

``` python
"""
Short description of what this endpoint does.

Example request:
    GET /api/spendsphere/v1/allocations?accountCodes=TAAA&accountCodes=TBBB

Example request (specific period):
    GET /api/spendsphere/v1/allocations?accountCodes=TAAA&accountCodes=TBBB&month=1&year=2026

Example response:
    [
      {
        "id": 1,
        "accountCode": "TAAA",
        "ggBudgetId": "15264548297",
        "allocation": 60.0
      }
    ]

Requirements:
    - Requires X-Tenant-Id header
    - Requires valid API key
    - month/year must be provided together
    - Unknown query params are rejected (400)
"""
```

Documentation rules:

-   Examples must use realistic values.
-   Responses must reflect the actual response shape.
-   If the endpoint uses the standard response envelope, examples must
    include it.
-   If optional parameters exist, include at least one example using
    them.
-   If validation rules exist, document them under "Requirements".
-   When modifying route behavior, update its docstring examples.
-   Undocumented routes are not production-ready.

------------------------------------------------------------------------

## Multi-Tenant Safety Rules (Mandatory)

-   Never cache data across tenants without scoping by tenant ID.
-   Never access tenant config without validating `X-Tenant-Id`.
-   All DB queries must be tenant-scoped (when applicable).
-   Never store tenant-specific state in global variables.
-   Never introduce cross-tenant shared caches without explicit tenant
    keys.

------------------------------------------------------------------------

## App-Specific Notes

### SpendSphere

-   Routes live under:
    -   `apps/spendsphere/api/v1/router.py`
    -   `apps/spendsphere/api/v2/router.py`
-   SpendSphere v1 endpoint layout:
    -   Core shared routes:
        `apps/spendsphere/api/v1/endpoints/core`
    -   Custom tenant-specific routes/helpers:
        `apps/spendsphere/api/v1/endpoints/custom`
-   Access policy for SpendSphere v1:
    -   Core routes are shared and must not include tenant-specific
        conditional logic.
    -   Custom routes must be gated by `FEATURE_FLAGS` in tenant config.
    -   Do not hardcode tenant IDs in route authorization logic for
        custom features.
-   Cache file `caches.json` holds SpendSphere cache data.
-   TTL and refresh rules are documented in `CACHE.md`.
-   File-based caching may not be safe for multi-instance deployments
    unless shared storage is mounted.

### SpendSphere AccountCode Validation (Current)

-   Shared validator location:
    -   `apps/spendsphere/api/v1/helpers/spendsphere_helpers.py`
    -   `validate_account_codes(...)`
    -   `require_account_code(...)`
    -   `normalize_account_codes(...)`
-   Source of truth is **Google Ads accounts/clients**, not DB
    `accounts` table.
-   Validation uses tenant-scoped cache in `caches.json` first
    (`account_codes` key), then refreshes from Google Ads when:
    -   cache is missing/stale
    -   requested code is not present
    -   legacy/non-Google-Ads cache format is detected
-   A valid accountCode must satisfy:
    1.  Parsed from Google Ads `descriptive_name` via
        `GOOGLE_ADS_NAMING.account` format/regex.
    2.  Account name must not start with any
        `GOOGLE_ADS_NAMING.inactivePrefixes` value (defaults to `zzz.`).
    3.  `get_active_period` must mark the account active for the target
        date.
-   Active-period behavior:
    -   default: evaluate with today in tenant timezone
    -   when `month` and `year` are supplied: evaluate using
        period-aware `as_of` date logic
    -   optional explicit `as_of` may be passed by callers
-   `include_all=False` (default):
    -   explicit inactive/invalid requested codes return 400 with detail
        (`invalid_codes`, `inactive_by_name`, `inactive_by_period`,
        `valid_codes`, `active_codes`)
    -   when no codes are explicitly requested, returns active accounts
        only
-   `include_all=True` allows inactive-by-name/period entries to pass
    validation lookup (for inspection-style endpoints).
-   `refresh_account_codes_cache(include_all=False)` now stores accounts
    active by both naming and active-period rules; `include_all=True`
    stores all parsed Google Ads accounts.
-   `DB_TABLES.ACCOUNTS` is optional for SpendSphere account-code
    validation and update flows.
-   `/allocations/duplicate` must not rely on DB `accounts.active`; it
    filters by Google Ads + active-period account activity for the
    target period.

### SpendSphere Google Ads Update Rules (Current)

-   Applies to:
    -   `POST /api/spendsphere/v1/updates/budget`
    -   `POST /api/spendsphere/v1/updates/budgetAsync`
-   Pipeline builds update rows from:
    -   master budgets + campaigns + Google budgets + spend costs +
        allocations + rollovers + active period + accelerations.
-   Campaign status update rule:
    -   `expected_status` is `PAUSED` when account is inactive.
    -   `expected_status` is `ENABLED` when active and `dailyBudget >=
        0.01`; otherwise `PAUSED`.
    -   Campaigns with names prefixed by any
        `GOOGLE_ADS_NAMING.inactivePrefixes` value are never status-updated.
    -   Campaign is updated only when current status differs from
        `expected_status`.
-   Budget amount update rule (stricter than campaign status):
    -   Skip if there are no campaigns under that budget.
    -   Skip if `dailyBudget` is missing.
    -   Skip if current Google budget amount is missing.
    -   Compute target amount:
        -   `0.01` when `dailyBudget <= 0`
        -   otherwise `dailyBudget`
    -   Skip small changes when absolute delta is `<=
        GGADS_MIN_BUDGET_DELTA` (except target `0`/`0.01` flow).
    -   Skip when target amount equals current amount.
-   Allocation/active interaction:
    -   If `allocation` is missing and account is active, row is skipped.
    -   If `allocation` is missing and account is inactive, campaign
        status can still be forced to `PAUSED`, but budget amount update
        is skipped.
-   Warning behavior:
    -   `BUDGET_AMOUNT_THRESHOLD_EXCEEDED` warning:
        -   Enabled only when tenant config `BUDGET_WARNING_THRESHOLD` is set.
        -   Triggered when computed `newAmount` is greater than that threshold.
        -   Does not block updates; warning is added to mutation result.
    -   `SPEND_WITHOUT_ALLOCATION` warning:
        -   Triggered when transformed row has `totalCost > 0` and
            allocation is missing (`None`) or allocation equals `0`.
        -   Warning message differentiates missing allocation vs zero allocation.
        -   Does not block updates; warning is added to mutation result.
    -   `BUDGET_LESS_THAN_SPEND` warning:
        -   Triggered when transformed row has `totalCost > 0` and
            allocated master budget is lower than spend:
            `(netAmount + rollBreakdown) * (allocation / 100) < totalCost`.
        -   The allocated-budget value is computed in transform as
            `allocatedBudgetBeforeAcceleration` and reused by warning logic.
        -   Uses allocated budget derived from master budget + roll
            breakdown, before acceleration (not Google daily budget).
        -   Does not block updates; warning is added to mutation result.
    -   `PACING_OVER_100` warning:
        -   Triggered when pacing is greater than `100%`.
        -   Uses transformed pacing values when present; otherwise
            computes pacing from spend vs accelerated allocated budget.
        -   Does not block updates; warning is added to mutation result.
    -   `SPEND_PERCENT_OVER_100` warning:
        -   Triggered when `%Spend` is greater than `100%`.
        -   Uses transformed `%Spend` values when present; otherwise
            computes `%Spend` from spend vs allocated budget before acceleration.
        -   Does not block updates; warning is added to mutation result.
    -   Shared row-level skip conditions for the two warnings above:
        -   Skipped when budget has no linked campaigns.
        -   Skipped when all linked campaigns are `PAUSED`.
        -   Skipped when all linked campaigns are prefixed by
            `GOOGLE_ADS_NAMING.inactivePrefixes`.
    -   Multiple warnings can be produced for the same row:
        -   If both conditions match, both warning entries are added with
            different `warningCode` values/messages.
    -   Warning dedupe cache:
        -   Warning emission is tenant-scoped and cached in `caches.json`.
        -   Duplicate warnings are suppressed by fingerprint
            (`customerId` + `warningCode` + budget/campaign/account identity)
            until TTL expires, and always reset at the start of a new local day.
        -   Default TTL is 24 hours (`86400` seconds).
        -   Tenant override is under `CACHE` with
            `google_ads_warnings_ttl_time` (or `google_ads_warning_ttl_time`).
        -   TTL `<= 0` keeps same-day dedupe and still resets on new day.
    -   Warning logging policy:
        -   Row-detail warnings are logged in `Google Ads pipeline warnings`.
        -   Separate summary log `Google Ads spend without allocation warnings`
            is removed to avoid duplicate warning-count logs in Axiom.
-   Execution mode:
    -   `dryRun=true`: no Google Ads mutations; returns simulated
        mutation result structure.
    -   `dryRun=false`: executes budget and campaign mutations in
        parallel.

### Shiftzy

-   Routes live under `/api/shiftzy/v1`.
-   PDF generation uses `fpdf2`.
-   Assets live in `apps/shiftzy/api/assets`.

------------------------------------------------------------------------

## External Services

-   Google Sheets/Ads use a service account resolved by:
    -   `shared/utils.resolve_secret_path`
    -   `GOOGLE_APPLICATION_CREDENTIALS`
-   `shared/ggSheet.py` is **not thread-safe**.
    -   Do not use it inside thread pools or background threads.

------------------------------------------------------------------------

## Versioning Notes

-   SpendSphere v2 follows an inheritance model:
    -   Only changes are implemented in v2.
    -   Unchanged behavior must rely on v1 logic.
    -   Do not duplicate v1 logic into v2.
-   Avoid modifying older versions unless fixing bugs.

------------------------------------------------------------------------

# Code Quality & Reuse Rules (Strict)

The agent must prioritize clean architecture and minimal duplication.

## 1. Shared Folder Enforcement (Critical)

-   The `shared/` folder is the single source of truth for reusable
    logic.
-   Before creating any new function, search the repository for existing
    implementations.
-   If similar logic exists in `shared/`, reuse it instead of
    duplicating.
-   If a new function appears generic or reusable across multiple apps,
    it MUST be created inside `shared/`.
-   Do not duplicate shared logic inside `apps/spendsphere` or
    `apps/shiftzy`.
-   Shared functions must not depend on app-specific modules.
-   If duplicated logic is discovered, refactor it into `shared/`.

This rule is mandatory and takes precedence over convenience.

## 2. Prefer Reuse Over Duplication

-   Search the repository for similar logic before adding new code.
-   Reuse existing utilities from `shared/` whenever possible.
-   Do not duplicate logic across apps.
-   Shared logic must live in `shared/`.

## 3. Delete Unused Code

-   Remove unused functions, imports, variables, and dead branches.
-   Do not leave commented-out legacy code.

## 4. Keep Functions Focused

-   Functions should do one thing.
-   Prefer small composable helpers.

## 5. Avoid Introducing New Patterns Lightly

-   Follow existing architectural patterns.
-   Do not introduce new DB or middleware patterns.

## 6. Resource Efficiency Rule (Critical – Free Server Constraint)

This system runs on a resource-constrained environment (free-tier server).

All code must be written with explicit consideration for:

- Low RAM usage
- Low CPU usage
- Minimal disk I/O
- Minimal network bandwidth usage
- Minimal external API calls
- Minimal thread/process overhead

Efficiency is not optional. It is a core architectural requirement.

## Mandatory Efficiency Principles

### 1. Avoid Unnecessary Data Loading

- Never load full datasets when filtering can be done in SQL.
- Always select only required columns.
- Avoid `SELECT *` in production logic.
- Paginate large result sets when applicable.
- Do not materialize large lists unless strictly necessary.

### 2. Cache Carefully and Intentionally

- Cache must always be tenant-scoped.
- Avoid over-caching large objects.
- Use TTL-based invalidation.
- Avoid aggressive refresh strategies.
- Do not introduce memory-heavy in-process caches.

### 3. Minimize External API Calls

- Batch Google Ads / Sheets requests when possible.
- Avoid duplicate refresh calls inside loops.
- Never call external APIs inside tight iteration loops.
- Use cached account metadata when valid.

### 4. Control Memory Usage

- Avoid building large intermediate lists.
- Prefer generators when possible.
- Avoid deep copies of large objects.
- Reuse computed values instead of recalculating.
- Clean up temporary structures when no longer needed.

### 5. Avoid Heavy Background or Async Overhead

- Do not introduce background workers unless strictly required.
- Avoid spawning threads for simple synchronous operations.
- Prefer simple execution paths over complex concurrency patterns.

### 6. Logging Discipline

- Do not log full payloads for large responses.
- Avoid excessive debug logging in production.
- Log summaries instead of full datasets.

### 7. Dependency Discipline

- Do not introduce heavy libraries without strong justification.
- Avoid large frameworks for small utilities.
- Keep dependency footprint minimal.

## Prohibited Patterns

- In-memory cross-tenant data accumulation.
- Full-table scans when indexed filters are available.
- Recomputing full Google Ads state per request.
- Re-fetching tenant config multiple times in one request.
- Creating large nested dicts/lists for temporary transformations without need.

## Design Mindset Requirement

When implementing any new feature, the agent must explicitly consider:

- Can this be done with less memory?
- Can this be done with fewer queries?
- Can this avoid extra API calls?
- Can this reuse existing cached data?
- Can this reduce object allocation?

Performance and resource efficiency must be prioritized over convenience.


------------------------------------------------------------------------

# Documentation Consistency Rule (Critical)

The AI agent MUST keep this `AGENTS.md` file synchronized with system behavior.

This file is the single source of truth for architectural, configuration,
and behavioral contracts across SpendSphere and Shiftzy.

## Mandatory Update Requirement

The agent MUST update `AGENTS.md` whenever any of the following change:

- New configuration keys are introduced (tenant or global)
- Existing configuration behavior changes
- New environment variables are added
- Cache structure, keys, TTLs, or scoping rules change
- Naming rules or parsing logic change
- API request/response contracts change
- Validation rules change
- Warning or alert behavior changes
- Background job or async behavior changes
- Google Ads update rules change
- Budget calculation logic changes
- Account validation logic changes
- Middleware behavior changes
- Feature flags are introduced or modified
- Any new cross-tenant safety rules are added
- Any mutation-safety rule changes

## Required Actions When Behavior Changes

When implementing behavior or config changes, the agent must:

1. Update the relevant section inside `AGENTS.md`.
2. Ensure documentation reflects the exact runtime behavior.
3. Add or modify bullet rules under the correct domain section
   (SpendSphere, Shiftzy, Middleware, Tenancy, etc.).
4. Keep wording precise and implementation-aligned.
5. Remove outdated documentation when logic changes.
6. Never leave stale behavior documented.

## Pull Request Standard

A change affecting behavior is NOT complete unless:

- `AGENTS.md` is updated accordingly, OR
- The PR explicitly states:

  "No AGENTS.md update required — no behavior/config changes."

Failure to update documentation when required is considered a contract violation.

------------------------------------------------------------------------

## Code Section Structure (Mandatory -- Clean Vision Standard)

All Python source files must use clear visual section separators for
readability and consistency.

### Required Section Header Format

``` python
# ============================================================
# SECTION NAME
# ============================================================
```

Example:

``` python
# ============================================================
# IMPORTS
# ============================================================

# ============================================================
# CONSTANTS
# ============================================================

# ============================================================
# VALIDATION HELPERS
# ============================================================

# ============================================================
# BUSINESS LOGIC
# ============================================================

# ============================================================
# ROUTES
# ============================================================
```

### Section Rules

-   Section headers must be UPPERCASE.
-   Keep header width consistent.
-   Add a blank line after each section header.
-   Large files must be broken into logical sections.
-   Do not introduce new functions outside a defined section.
-   If a file grows too large, consider splitting it.

### Recommended Section Order (Routes Files)

1.  IMPORTS\
2.  CONSTANTS\
3.  REQUEST/RESPONSE MODELS\
4.  VALIDATION HELPERS\
5.  BUSINESS LOGIC\
6.  ROUTES

### Recommended Section Order (Helper/Service Files)

1.  IMPORTS\
2.  CONSTANTS\
3.  INTERNAL UTILITIES\
4.  PUBLIC FUNCTIONS

Clean visual structure is mandatory for maintainability.

------------------------------------------------------------------------

## Logging

-   JSON logs are emitted by `shared/logger.py`.
-   Optional Axiom export via `AXIOM_*` environment variables.
-   Request and tenant context must be included when available.

## Logging Policy (Mandatory)

-   All routes must be logged through
    `request_response_logger_middleware`.
-   Do not add ad-hoc logging inside route handlers unless logging
    domain-specific business events.
-   Axiom export must be handled centrally via `shared/logger.py`.
-   Route-level logging must not bypass middleware.
-   Logging must include:
    -   method
    -   path
    -   status_code
    -   duration
    -   tenant_id
    -   request_id (if available)

------------------------------------------------------------------------

## Conventions

-   Add endpoints under `apps/<app>/api/vX/endpoints`.
-   Wire them in the matching router.
-   Reuse `shared/` helpers instead of duplicating logic.
-   If adding a new app:
    -   Include tenant validation.
    -   Include response envelope middleware.
    -   Follow existing structure patterns.

------------------------------------------------------------------------

## Deployment Notes

-   The code reads `/etc/.env` first, then `etc/.env`.
-   Production deployments should mount secrets to `/etc/.env`.
-   Tenant configs are read from `/etc/secrets/<tenant>.yaml` or
    `etc/secrets/<tenant>.yaml`.
-   The app import path is `main:app`.

------------------------------------------------------------------------

## Local Setup Checklist

-   Create a virtualenv:

    ``` bash
    python -m venv .venv
    pip install -r requirements.txt
    ```

-   Create `etc/.env` with:

    -   `API_KEY_REGISTRY`
    -   MySQL credentials

-   Add `etc/secrets/<tenant>.yaml`.

-   Use `etc/secrets/tenant.template.yaml` as a starting point.

-   Configure `GOOGLE_APPLICATION_CREDENTIALS`.

-   Run:

    ``` bash
    uvicorn main:app --reload --port 8000
    ```

------------------------------------------------------------------------

## Tests

-   No test runner is currently configured.

-   Prefer `pytest`.

-   Place tests under `tests/`.

-   Suggested command:

    ``` bash
    pytest -q
    ```

------------------------------------------------------------------------

## Common Commands

Before working:

``` bash
git pull
```

After working:

``` bash
git add .
git commit -m "Describe what you changed"
git push
```

Reset to remote main (destructive):

``` bash
git fetch origin
git checkout main
git reset --hard origin/main
git clean -fd
```

Remove last commit and force push (destructive):

``` bash
git reset --soft HEAD~1
git push --force
```

Recreate virtualenv:

``` bash
rm -rf .venv
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run server:

``` bash
uvicorn main:app --reload
```
