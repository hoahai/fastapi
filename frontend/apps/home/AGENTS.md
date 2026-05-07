# Home Frontend Agent Guide

## Scope
- This app owns the Workspace Home / portal UI.
- Home app source lives under `frontend/apps/home/src/`.

## Routing
- Workspace Home route is `/fe` and `/fe/`.
- Keep route URLs stable and compatible with the shared frontend shell router.

## Architecture
- Home app should render inside shared layout/shell components used by the frontend workspace.
- Keep Home-specific pages/components in `frontend/apps/home/`.
- Put reusable cross-app UI, hooks, styles, and cache utilities in `frontend/shared/`.

## Boundaries
- Do not move Home page-specific code into `frontend/src/`.
- Do not introduce Tradsphere-only UI patterns into Home unless intentionally shared.
