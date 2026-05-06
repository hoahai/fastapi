# TradSphere Frontend Agent Guide

## Scope
- Modify only files under `frontend/apps/tradsphere/` for app-specific UI work.
- Move reusable patterns to `frontend/shared/` when they become cross-app.

## API and Routing
- Keep API requests relative to backend routes (for example: `/api/tradsphere/v1/...`).
- Keep production routing compatible with `/fe` base path.
- Do not hardcode hostnames.

## Consistency
- Follow `frontend/DESIGNS.md` and local `DESIGNS.md`.
- Reuse existing UI primitives before introducing new one-off components.

## Backend Boundaries
- Do not restructure backend folders.
- Touch backend only when required for `/fe` static serving integration.
