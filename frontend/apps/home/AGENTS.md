# Home Frontend Agent Guide

## Scope
- This app owns the Workspace Home / portal UI.
- Home app source lives under `frontend/apps/home/src/`.

## Routing
- Workspace Home primary route is `/`.
- Tradsphere route is `/tradsphere/home`.
- `/fe` and `/fe/...` are compatibility-only paths and may redirect/alias to root-based routes.
- Keep route URLs stable and compatible with the shared frontend shell router.

## Home Page Structure
- Workspace Home should include:
  - workspace banner/header
  - Announcements section
  - Apps section with responsive app-card grid
- Apps grid should show only available apps; hide unavailable apps entirely.
- Workspace Home auth visibility:
  - If not signed in, show sign-in CTA and do not imply app access.
  - If signed in, app availability should follow `/api/auth/v1/session/me` permissions.
  - If signed in with no app permissions, show an explicit no-access empty state.
  - App-level mutation actions in downstream apps must stay permission-gated: users without edit access should get read-only forms for existing records, and create/add/save/delete/upload controls should remain disabled.

## Shared Auth Pages
- Auth routes used by the shared shell are:
  - `/auth/login`
  - `/auth/callback`
  - `/auth/invite/:token`
  - `/auth/unauthorized`
  - `/auth/invite/pending`
- Auth page source is shared in `frontend/shared/auth/pages/`.
- Login UX is TheSphereWorks-branded and supports only email/password.
- Do not add Google login or magic-link login in Phase 1.
- Keep auth messaging invite-only and friendly, with inline loading/error states (no browser-native alerts/confirms).

## Architecture
- Home app should render inside shared layout/shell components used by the frontend workspace.
- Shared shell sidebar preference is manual collapse/expand; collapsed hover/focus expansion is temporary and should not change saved preference.
- Keep Home-specific pages/components in `frontend/apps/home/`.
- Put reusable cross-app UI, hooks, styles, and cache utilities in `frontend/shared/`.

## Boundaries
- Do not move Home page-specific code into `frontend/src/`.
- Do not introduce Tradsphere-only UI patterns into Home unless intentionally shared.
