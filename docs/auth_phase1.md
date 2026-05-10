# Phase 1 Auth Rollout (Tradsphere First)

## Scope

Phase 1 introduces Supabase JWT authentication and tenant/app authorization for Tradsphere first, while keeping legacy API-key compatibility.

Related setup docs:

- [auth_phase1_env_example.md](/Users/haitruongh/Developer/fastapi/docs/auth_phase1_env_example.md)
- [auth_phase1_local_setup.md](/Users/haitruongh/Developer/fastapi/docs/auth_phase1_local_setup.md)
- [supabase_phase1_seed_example.sql](/Users/haitruongh/Developer/fastapi/docs/supabase_phase1_seed_example.sql)

## Auth Flags

Local/dev defaults:

- `AUTH_MODE=compat`
- `AUTH_PROTECT_TRADSPHERE=false`
- `AUTH_ENABLE_LEGACY_API_KEY_FALLBACK=true`

Production initial rollout:

- `AUTH_MODE=compat`
- `AUTH_PROTECT_TRADSPHERE=true`
- `AUTH_ENABLE_LEGACY_API_KEY_FALLBACK=true`

Future cutover (not Phase 1):

- `AUTH_MODE=jwt_only`
- `AUTH_ENABLE_LEGACY_API_KEY_FALLBACK=false`

Optional:

- `AUTH_PERMISSION_CACHE_TTL_SECONDS=120`
- `AUTH_ENABLE_DEBUG_ENDPOINTS=false`
- `AUTH_INVITE_TTL_HOURS=72`
- `AUTH_INVITE_BASE_URL=https://<frontend-domain>`

## Supabase Env Vars

Backend:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`

Notes:

- `SUPABASE_ANON_KEY` may be legacy JWT `anon` or new `sb_publishable_*`.
- `SUPABASE_SERVICE_ROLE_KEY` may be legacy JWT `service_role` or new `sb_secret_*`.
- New `sb_*` keys are not JWTs and must not be sent as `Authorization: Bearer ...`.
- For backend Supabase REST table access with new keys, use `apikey` header with the service key.
- `Authorization: Bearer ...` is reserved for real user access tokens.

Frontend (`VITE_*`):

- `VITE_SUPABASE_URL`
- `VITE_SUPABASE_ANON_KEY`
- `VITE_DEFAULT_TENANT_SLUG`
- `VITE_AUTH_MODE` (default `compat`)
- `VITE_AUTH_ENABLE_LEGACY_API_KEY_FALLBACK` (default `true`)
- `VITE_AUTH_PROTECT_TRADSPHERE` (default `false`)
- `VITE_LEGACY_API_KEY` (optional compat fallback)
- `VITE_LEGACY_USER_NAME` (optional compat fallback)

## Tenant Slug Mapping Rule

Canonical rule:

- `X-Tenant-Id` must equal `tenants.slug` exactly.

Example:

- `X-Tenant-Id: taaa`
- `tenants.slug: taaa`

## Runtime Verification Rules

For protected Tradsphere APIs (`/api/tradsphere/v1/*`) when `AUTH_PROTECT_TRADSPHERE=true`:

1. Validate bearer JWT (`Authorization: Bearer <token>`).
2. Require `X-Tenant-Id`.
3. Verify tenant slug exists and is active.
4. Verify user is active member of tenant.
5. Verify tenant app (`tradsphere`) is enabled.
6. Verify user has required role/permission.

## Legacy Compatibility

In `AUTH_MODE=compat` with fallback enabled:

- If bearer JWT is missing on protected Tradsphere API, legacy `X-API-Key` path is allowed.
- Responses include `X-Auth-Deprecated: api-key-fallback` when legacy fallback is used.
- Legacy fallback usage is logged for migration tracking.

## Public Route Allowlist

Public routes remain limited to:

- `/`
- `/ping`
- frontend static assets (`/assets/*`, `/fe/assets/*`)
- frontend auth pages (`/auth/*`)
- signed OpsSphere public report (`/public/opssphere/advWebsiteReport/reports/cta`)

## Tradsphere-First Protection

Phase 1 enforces JWT/permission checks for Tradsphere first (behind `AUTH_PROTECT_TRADSPHERE`).
Other apps keep existing behavior until later phases.

## Invite Flow (API Only in Phase 1)

Phase 1 ships invite APIs (no admin UI yet):

- `POST /api/auth/v1/invitations`
- `GET /api/auth/v1/invitations/{token}`
- `POST /api/auth/v1/invitations/{token}/accept`
- `POST /api/auth/v1/invitations/{invitation_id}/revoke`

## Debug Endpoint (Dev-Only)

- `GET /api/auth/v1/debug/whoami`
- Only enabled when `AUTH_ENABLE_DEBUG_ENDPOINTS=true`
- Disabled by default
- Returns no sensitive secrets/tokens

## Supabase Tables (MVP)

- `profiles`
- `tenants`
- `apps`
- `tenant_users`
- `tenant_app_roles`
- `role_permissions`
- `invitations`

Schema file:

- [supabase_phase1_schema.sql](/Users/haitruongh/Developer/fastapi/docs/supabase_phase1_schema.sql)

Schema readiness notes (suggestions only, not applied yet):

- Add index for `tenant_users(user_id, tenant_id, status)` for membership checks.
- Add index for `tenant_app_roles(user_id, tenant_id, app_id)` for role lookup.
- Add index for `role_permissions(role)` for permission expansion.
- Add index for `invitations(token)` already covered by unique constraint.
- Optionally enforce lowercase policy for `tenants.slug` and `apps.code` via check constraints if desired.

## Future Cutover Plan

1. Monitor legacy API-key fallback logs.
2. Migrate frontend and external clients to JWT.
3. Switch to `AUTH_MODE=jwt_only`.
4. Disable legacy fallback.
