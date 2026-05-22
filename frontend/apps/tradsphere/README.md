# TradSphere App Source

TradSphere business frontend source lives in this folder.

## Ownership
- Shell host/build/routing/toolchain: `frontend/shell`
- TradSphere business pages/components/hooks/lib: `frontend/apps/tradsphere/src`
- Shared cross-app primitives/auth/api/cache: `frontend/shared`

## Development
- Run via shell host:
  - `npm --prefix frontend/shell run dev`
  - `npm --prefix frontend/shell run build`

## Notes
- This app folder intentionally excludes Vite host/bootstrap ownership.
- Compatibility wrappers remain while legacy `@/...` imports are still used.
