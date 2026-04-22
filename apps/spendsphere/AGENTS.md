# AGENTS.md (SpendSphere)

## Scope

- Applies to `apps/spendsphere/**`.
- Root-level `AGENTS.md` still applies; this file adds SpendSphere-specific rules.

## App Overview

- Mounted at `/api/spendsphere`.
- Active versions:
  - `/api/spendsphere/v1`
  - `/api/spendsphere/v2`
- App entrypoint: `apps/spendsphere/api/main.py`.
- Routers:
  - `apps/spendsphere/api/v1/router.py`
  - `apps/spendsphere/api/v2/router.py`

## Architecture Rules

- Keep v1 split by responsibility:
  - Shared/core endpoints under `api/v1/endpoints/core`
  - Tenant-custom endpoints under `api/v1/endpoints/custom`
- Do not add tenant-specific conditionals to core routes.
- Custom routes must be feature-flag gated via tenant config.
- For v2:
  - Implement only changed behavior in v2.
  - Reuse/inherit unchanged behavior from v1.

## Tenant + Config Rules

- SpendSphere tenant config must pass app validator in:
  - `apps/spendsphere/api/v1/helpers/config.py`
- Do not bypass root tenant middleware validation.
- App-scoped tenant config keys must be preferred where available.

## Account-Code Validation Rules

- Source of truth is Google Ads accounts/clients.
- Keep account-code validation in SpendSphere helpers (current shared validator flow already exists in app helper layer).
- Respect active-period rules and inactive-prefix naming rules.
- Do not reintroduce DB `accounts.active` dependency for validation-critical flows.

## Caching Rules

- SpendSphere caches are tenant-scoped in `caches.json`.
- Use existing cache helpers and TTL configuration behavior.
- Prefer cache-first fetch behavior with explicit refresh controls where already supported.

## Google Ads / Mutation Rules

- Preserve current mutation policies:
  - campaign status eligibility rules
  - budget amount delta and threshold rules
  - blocked channel handling (warnings vs failures)
  - warning dedupe cache behavior
- Do not change dry-run/execute behavior without explicit request.

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
- No cross-app refactors from SpendSphere changes unless explicitly requested.
