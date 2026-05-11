# Phase 1 Local Supabase Setup (Tradsphere First)

This validates existing Phase 1 auth/authz implementation with a real Supabase project.

## 1) Create Supabase Project

1. Create a Supabase project.
2. In Authentication -> Providers, enable:
   1. Email/password

## 2) Collect Supabase Keys

From Project Settings -> Data API:

1. Copy Project URL.
2. Copy `anon` public key.
3. Copy `service_role` secret key.

Rules:

- `service_role` key is backend-only.
- Never place `service_role` in frontend env.

## 3) Fill Local Env Files

Use [auth_phase1_env_example.md](/Users/haitruongh/Developer/fastapi/docs/auth_phase1_env_example.md).

1. Paste backend values into `etc/.env`.
2. Paste frontend values into `frontend/apps/tradsphere/.env.local`.
3. Keep local compatibility flags:
   1. `AUTH_MODE=compat`
   2. `AUTH_PROTECT_TRADSPHERE=true`
   3. `AUTH_ENABLE_LEGACY_API_KEY_FALLBACK=true`
   4. `AUTH_ENABLE_DEBUG_ENDPOINTS=true` (local only)

## 4) Apply Schema

Run SQL from:

- [supabase_phase1_schema.sql](/Users/haitruongh/Developer/fastapi/docs/supabase_phase1_schema.sql)

## 5) Create a Real Auth User

1. Sign up once via `/auth/login` (email/password), or create a user in Supabase dashboard.
2. Find user id:
   1. Supabase -> Authentication -> Users -> copy UID.

## 6) Apply Seed Template

1. Open [supabase_phase1_seed_example.sql](/Users/haitruongh/Developer/fastapi/docs/supabase_phase1_seed_example.sql).
2. Replace placeholders:
   1. `<PASTE_AUTH_USER_ID>`
   2. `<PASTE_TEST_USER_EMAIL>`
3. Run the SQL.

## 7) Start Local Services

1. Backend:
   1. `.venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000`
2. Frontend:
   1. `npm --prefix frontend/apps/tradsphere run dev`

## 8) Manual Validation Checklist

1. Public routes:
   1. `GET /` returns `200`.
   2. `GET /ping` returns `200`.
   3. `GET /auth/login` returns `200`.
2. Protected route without auth:
   1. `GET /api/tradsphere/v1/ui/main/selections` with only `X-Tenant-Id: taaa` returns `401`.
3. Compat fallback:
   1. Same route with valid `X-API-Key` returns non-`401` and includes `X-Auth-Deprecated: api-key-fallback`.
4. JWT auth:
   1. Login and call Tradsphere route with `Authorization: Bearer <token>` + `X-Tenant-Id: taaa`.
   2. Expect success for allowed role.
5. Wrong-tenant check:
   1. Valid JWT but wrong `X-Tenant-Id` should return `403`.
6. Frontend auth pages render:
   1. `/auth/login`
   2. `/auth/callback`
   3. `/auth/unauthorized`
   4. `/auth/invite/pending`

## 9) Notes

- If Tradsphere route fails with tenant/app membership errors, verify `tenant_users` and `tenant_app_roles` rows for the same `auth.users.id`.
- Keep legacy fallback enabled only during migration window.
