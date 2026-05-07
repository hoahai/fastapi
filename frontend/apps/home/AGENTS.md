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
- Apps grid should show current apps and mark unavailable apps as `Soon`.

## Architecture
- Home app should render inside shared layout/shell components used by the frontend workspace.
- Shared shell sidebar preference is manual collapse/expand; collapsed hover/focus expansion is temporary and should not change saved preference.
- Keep Home-specific pages/components in `frontend/apps/home/`.
- Put reusable cross-app UI, hooks, styles, and cache utilities in `frontend/shared/`.

## Boundaries
- Do not move Home page-specific code into `frontend/src/`.
- Do not introduce Tradsphere-only UI patterns into Home unless intentionally shared.
