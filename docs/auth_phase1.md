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
- `AUTH_PROVIDER=supabase` (current provider module)

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
- Backend auth access is isolated through `shared/auth/providers/` so provider-specific calls stay out of route handlers.

Frontend (`VITE_*`):

- `VITE_SUPABASE_URL`
- `VITE_SUPABASE_ANON_KEY`
- `VITE_AUTH_PROVIDER` (default `supabase`)
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

## Frontend Auth UX (Phase 1)

Auth pages are enabled in frontend under `/auth/*` for Tradsphere-first rollout:

- `/auth/login`
- `/auth/callback`
- `/auth/update-password`
- `/auth/invite/{token}`
- `/auth/unauthorized`
- `/auth/invite/pending`

Login UX rules:

- Product name shown as `TheSphereWorks`.
- Auth pages are shared infrastructure under `frontend/shared/auth/pages/` (not app-local Home pages).
- Supported sign-in method:
  - Email/password only
- Google login and magic-link login are not included in this phase.
- Login copy remains invite-only and user-friendly.

My Profile + password rules:

- `/profile` supports basic profile updates (`firstName`, `lastName`, `fullName`) via `PATCH /api/auth/v1/session/me/profile`.
- `/profile` supports self password change through the frontend auth-provider abstraction and Supabase Auth user update.
- `/profile` account summary does not treat tenant as root-level identity; tenant is shown only in app/access scope context when needed.
- Password values are never persisted to browser storage and are never logged.
- Password policy for create/reset/change flows: `Password must be at least 8 characters and include at least one letter and one number.`
- Password reset recovery flow uses `/auth/update-password` and keeps email/password auth only (no magic link sign-in UX).

Workspace Home visibility rules:

- Signed-out users see sign-in CTA and should not be shown app access as available.
- Signed-in users see app availability based on `/api/auth/v1/session/me` (session permissions/access profile is the frontend source of truth).
- Workspace Home hides apps the user cannot access; unavailable app cards should not be shown as clickable actions.
- Sidebar navigation hides app/page links the user cannot access (including Admin/Users for non-admin scopes).
- Signed-in users with no app permissions see: `You do not have access to any apps yet. Contact your workspace administrator.`

Security/authorization rule:

- Frontend visibility and guards are UX controls only.
- Backend authorization remains the source of truth for protected API access.
- Direct URL access to unauthorized routes must still resolve to unauthorized behavior (route guard/backend enforcement).

## Tradsphere-First Protection

Phase 1 enforces JWT/permission checks for Tradsphere first (behind `AUTH_PROTECT_TRADSPHERE`).
Other apps keep existing behavior until later phases.

## Invite + Admin Flow (Phase 1 MVP)

Phase 1 includes invite APIs and admin management endpoints:

- `POST /api/auth/v1/invitations`
- `GET /api/auth/v1/invitations/{token}`
- `POST /api/auth/v1/invitations/{token}/accept`
- `POST /api/auth/v1/invitations/{invitation_id}/revoke`
- `GET /api/auth/v1/admin/users/load`
- `GET /api/auth/v1/admin/roles`
- `GET /api/auth/v1/admin/users`
- `PATCH /api/auth/v1/admin/users/{user_id}`
- `POST /api/auth/v1/admin/users/{user_id}/password-reset`
- `GET /api/auth/v1/admin/invitations`
- `POST /api/auth/v1/admin/invitations`
- `POST /api/auth/v1/admin/invitations/{invitation_id}/revoke`

Rules:

- Admin endpoints require bearer JWT + `X-Tenant-Id` + `tradsphere.admin`.
- Admin password reset endpoint sends Supabase recovery email for the target scoped user and never returns password/token values.
- Super Admin can reset any user; app-admin can reset only users in their managed tenant/app scope.
- Admin Users page should load core data through `GET /api/auth/v1/admin/users/load`:
  - Response includes `tenants`, `apps`, `roles`, `users`, `invitations` (pending by default), and `scope`.
  - Super Admin scope can use `includeAllTenants=true`; app-admin remains tenant/app scoped.
  - Existing granular admin endpoints remain available for targeted refresh/debug/action flows.
- Membership termination disables tenant/app access records; it does not delete Supabase auth users.
- Role/membership updates invalidate in-memory permission cache for affected users.
- Admin invitations support multiple tenant/app/role assignments in one invite package.
- Invite acceptance applies all assignment rows and invalidates the accepted user cache.

## Role Scope Model (Current)

- Global workspace role:
  - `super_admin`
- Tenant/app scoped roles:
  - `admin`
  - `editor`
  - `viewer`

Semantics:

- `viewer`: view-only app access
- `editor`: can mutate app data
- `admin`: can manage user access only in tenant/app scopes where they are admin
- `super_admin`: global highest access across tenants/apps

Notes:

- `super_admin` is global and is not stored as tenant/app role.
- Admin UI flow does not assign `super_admin`.
- Admin UI flow does not edit/disable `super_admin` members.
- Backend is source of truth for permission enforcement.

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
- `user_global_roles`
- `role_permissions`
- `invitations`
- `invitation_assignments`

Schema file:

- [supabase_phase1_schema.sql](/Users/haitruongh/Developer/fastapi/docs/supabase_phase1_schema.sql)
- [supabase_role_scope_migration.sql](/Users/haitruongh/Developer/fastapi/docs/supabase_role_scope_migration.sql)

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
