# TradSphere Frontend Agent Guide

## Scope
- This folder is TradSphere app source only.
- Shell host/build/routing/toolchain ownership is in `frontend/shell/`.
- Shared cross-app auth/api/cache/components live in `frontend/shared/`.
- Workspace Home/Portal lives in `frontend/apps/home/`.

## What Belongs Here
- TradSphere pages and business UI in `src/pages` and `src/components`.
- TradSphere business hooks/lib in `src/hooks` and `src/lib`.
- TradSphere app-specific docs/config for contributor guidance.

## Compatibility Wrappers
- Legacy `@/...` imports still rely on these wrappers:
  - `src/components/layout/*`
  - `src/components/ui/toast.tsx`
  - `src/hooks/useRouteScrollRestoration.ts`
- Keep wrappers until references are migrated; remove only when `rg` shows zero usage.

## Boundaries
- Do not place shell bootstrap/toolchain files in this folder.
- Do not change backend static serving from this folder.
