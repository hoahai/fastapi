# Frontend Workspace

## Ownership
- `frontend/shell` owns the Vite host, build toolchain, routing, global layout, and shell-level setup.
- `frontend/apps/<app>` owns app-specific pages, components, hooks, and business lib code.
- `frontend/shared` owns reusable auth, API, cache, components, hooks, and utilities used across apps.
- `frontend/components.json` is the shared component scaffolding config.

## Build and Sync
- Render/frontend build currently runs from `frontend/shell`.
- Shell output is written to `frontend/apps/tradsphere/dist`.
- Deployment sync target remains `apps/tradsphere/ui_dist`.
