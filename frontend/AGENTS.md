# Frontend Agent Guide

## Scope
- Frontend source lives under `frontend/`.
- App-specific code lives under `frontend/apps/<app-name>/`.
- Workspace Home is an app under `frontend/apps/home/`.
- Tradsphere is an app under `frontend/apps/tradsphere/`.
- New frontend apps should be added under `frontend/apps/<app-name>/`.
- Shared UI code lives under `frontend/shared/`.
- Shared UI/cache/hooks/styles/theme utilities belong in `frontend/shared/`.
- Keep `frontend/src/` minimal for root app entry/router wiring only.

## Routing and API Rules
- Primary production frontend route is `/`.
- Tradsphere frontend route is `/tradsphere/home`.
- Frontend assets must resolve under `/assets`.
- `/fe` and `/fe/...` are compatibility-only paths and should redirect/alias to root-based routes.
- API calls must stay relative (for example: `/api/...`).
- Do not hardcode localhost or deployment domains.

## Design Rules
- Follow `frontend/DESIGNS.md`.
- Reuse shared components/styles before adding one-off styles.
- Update design docs when introducing new reusable patterns.
- For shared shell sidebar behavior: persist manual collapse/expand preference, and keep hover/focus expansion temporary-only (no preference write).
- Prefer reusable layout/action primitives over per-page/per-modal one-off component structures.
- For app-agnostic UI building blocks, place code in `frontend/shared/components/`.
- Keep app wrappers in `frontend/apps/<app-name>/` when they bind business-specific data shapes.
- Avoid one-off modal shells/footers/form-row styling when existing shared primitives can be reused.
- Icon-only action controls must include tooltip and `aria-label`.
- Dirty-form close behavior must use shared guard/confirmation patterns, not ad-hoc close logic.
- Cache/status chips must use a shared chip component and stay in low-emphasis page/modal bottom placement.

## Backend Safety Rules
- Do not rename or restructure `apps/`.
- Do not modify backend files unless required for `/fe` serving.
- Keep backend API prefixes and logic unchanged.

## Delivery Rules
- Keep generated artifacts out of source commits.
- Ensure frontend builds before proposing backend integration changes.

## Shared Cache Rules
- Use shared cache utilities under `frontend/shared/cache/` for frontend data caching.
- Default data-loading policy is `stale-while-revalidate`.
- Manual refresh actions must use `network-only` and update cache on success.
- Mutations (`create/update/delete`) must patch relevant cache entries and invalidate or mark related cache keys stale.
- Never persist sensitive values (`passwords`, tokens, secrets) to `localStorage`/`sessionStorage`; use in-memory cache for sensitive data.
- Avoid app-specific duplicate cache implementations when shared cache utilities can be reused.
- Cache status UI should use low-emphasis floating chips; do not let cache notes/buttons occupy primary layout flow.
- Page cache chips should stay fixed near viewport bottom; modal chips should stay fixed/sticky near modal bottom.
- Cache refresh chips must use button semantics, keyboard focus styles, tooltip text on hover/focus, and context-specific `aria-label`.
- In report/table dialogs, anchor cache chips to the modal shell/footer (outside primary scroll content) so large tables never push chip placement.
- Preserve stale-while-revalidate UX: show cached data immediately and revalidate without blanking visible cached panels.
