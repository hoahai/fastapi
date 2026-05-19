# Phase 1 Env Templates (Copy/Paste)

This repo ignores `.env.*` files. Use the templates below and paste values into your local env files.

Recommended local files:

- Backend: `etc/.env` (or `/etc/.env` in deployed environments)
- Frontend (Tradsphere app): `frontend/apps/tradsphere/.env.local`

Do not commit real secrets.

## Backend Template (`etc/.env`)

```dotenv
# Existing backend env values (DB, API_KEY_REGISTRY, etc.) remain required.

AUTH_MODE=compat
AUTH_PROTECT_TRADSPHERE=true
AUTH_ENABLE_LEGACY_API_KEY_FALLBACK=true
AUTH_PERMISSION_CACHE_TTL_SECONDS=120
AUTH_ENABLE_DEBUG_ENDPOINTS=true
AUTH_INVITE_TTL_HOURS=72
AUTH_INVITE_BASE_URL=http://localhost:8000

SUPABASE_URL=<PASTE_SUPABASE_URL>
SUPABASE_ANON_KEY=<PASTE_SUPABASE_ANON_KEY>
SUPABASE_SERVICE_ROLE_KEY=<PASTE_SUPABASE_SERVICE_ROLE_KEY>

# Global storage provider config (backend-only)
STORAGE_PROVIDER=cloudinary
CLOUDINARY_CLOUD_NAME=<PASTE_CLOUDINARY_CLOUD_NAME>
CLOUDINARY_API_KEY=<PASTE_CLOUDINARY_API_KEY>
CLOUDINARY_API_SECRET=<PASTE_CLOUDINARY_API_SECRET>
```

Where to find values in Supabase dashboard:

- `SUPABASE_URL`: Project Settings -> Data API -> Project URL
- `SUPABASE_ANON_KEY`: Project Settings -> API Keys -> `publishable` (`sb_publishable_*`) or legacy `anon`
- `SUPABASE_SERVICE_ROLE_KEY`: Project Settings -> API Keys -> `secret` (`sb_secret_*`) or legacy `service_role`

Backend request behavior:

- For Supabase REST permission/invitation table reads, backend uses `apikey: SUPABASE_SERVICE_ROLE_KEY`.
- If service key is `sb_secret_*`, backend does not send it in `Authorization: Bearer ...`.
- `Authorization: Bearer ...` is used only for real user JWTs (for example session access tokens).

## Frontend Template (`frontend/apps/tradsphere/.env.local`)

```dotenv
VITE_SUPABASE_URL=<PASTE_SUPABASE_URL>
VITE_SUPABASE_ANON_KEY=<PASTE_SUPABASE_ANON_KEY>
VITE_DEFAULT_TENANT_SLUG=taaa
VITE_AUTH_MODE=compat
VITE_AUTH_PROTECT_TRADSPHERE=true
VITE_AUTH_ENABLE_LEGACY_API_KEY_FALLBACK=true

# Compat-only fallback values. Keep while AUTH_MODE=compat.
VITE_LEGACY_API_KEY=<PASTE_EXISTING_DEV_API_KEY_IF_NEEDED>
VITE_LEGACY_USER_NAME=local-dev
```

## Security Rules

- `SUPABASE_SERVICE_ROLE_KEY` is backend-only. Never place it in frontend env files.
- `CLOUDINARY_API_SECRET` is backend-only. Never place it in frontend env files.
- `sb_secret_*` is not a JWT and must never be used as an Authorization bearer token.
- `VITE_*` values are bundled into browser code. Only use public/anon Supabase values there.
- Never commit real keys or tokens.
- `AUTH_INVITE_BASE_URL` can be either:
  - host root (example: `http://localhost:8000`)
  - full invite root (example: `http://localhost:8000/auth/invite`)
