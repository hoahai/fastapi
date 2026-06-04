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
- Tradsphere Stations route is `/tradsphere/stations`.
- Frontend assets must resolve under `/assets`.
- `/fe` and `/fe/...` are compatibility-only paths and should redirect/alias to root-based routes.
- API calls must stay relative (for example: `/api/...`).
- Do not hardcode localhost or deployment domains.

## Design Rules
- Follow `frontend/DESIGNS.md`.
- Reuse shared components/styles before adding one-off styles.
- Update design docs when introducing new reusable patterns.
- Auth UX standards:
  - Login supports only `email/password`.
  - Magic link and Google login are not part of this phase.
  - Do not show Google login UI in Phase 1.
  - Password recovery/update route is `/auth/update-password` (email/password only; no magic-link login UX).
  - Keep auth copy invite-only and user-friendly; avoid raw technical error dumps.
  - Do not use browser-native `alert`/`confirm` for auth flows.
  - Frontend auth guards are UX helpers only; backend permission checks remain source of truth.
  - Workspace Home and sidebar app visibility must be driven by `/api/auth/v1/session/me` access data already loaded in auth context.
  - Hide apps/pages the user cannot access instead of rendering disabled navigation by default.
  - Hide `Admin / Users` for non-admin scopes (`viewer`/`editor`); show it only for users with admin scope or `super_admin`.
  - Direct URL access must still be protected by route guards/backend checks even when links are hidden.
  - Keep Supabase-specific auth calls inside `frontend/shared/auth/provider/*` and call generic auth-context methods from pages/components.
  - `/profile` must keep profile and password forms split; save/update actions follow dirty + valid + loading guard rules.
  - `/profile` account summary must not show tenant as a root-level identity field; tenant belongs to app/access scope context.
  - Password policy for create/reset/change flows:
    - `Password must be at least 8 characters and include at least one letter and one number.`
  - Canonical role labels for auth/admin UX must be:
    - `Super Admin` (global/workspace role, non-assignable from normal admin page)
    - `Admin`, `Editor`, `Viewer` (tenant/app scoped)
  - Do not label view-only role as `User`; use `Viewer`.
  - Permission-gated mutation rule (global): if the signed-in user lacks edit permission for an app, keep mutation forms read-only and disable create/add/save/delete/upload actions.
  - Shared auth pages live under `frontend/shared/auth/pages/` and are routed at `/auth/*`.
- For shared shell sidebar behavior: persist manual collapse/expand preference, and keep hover/focus expansion temporary-only (no preference write).
- Prefer reusable layout/action primitives over per-page/per-modal one-off component structures.
- For app-agnostic UI building blocks, place code in `frontend/shared/components/`.
- Keep app wrappers in `frontend/apps/<app-name>/` when they bind business-specific data shapes.
- Avoid one-off modal shells/footers/form-row styling when existing shared primitives can be reused.
- Icon-only action controls must include tooltip and `aria-label`.
- Dirty-form close behavior must use shared guard/confirmation patterns, not ad-hoc close logic.
- Modal submit buttons should follow global dirty + valid rules:
  - hide submit action when form is pristine
  - disable submit while invalid/loading/submitting
  - use concise `Save` label in edit flows unless domain-specific wording is required
- Cache/status chips must use a shared chip component and stay in low-emphasis page/modal bottom placement.
- Free-text inputs must preserve raw typing while the user is editing.
  - Do not trim, title-case, or otherwise normalize free-text values in `onChange`.
  - Normalize on blur or on submit/save instead, and prefer the shared `useCommittedTextField` hook for text inputs/areas.
  - Live formatting is only appropriate for structured inputs that intentionally need it, such as phone fields, masked codes, and dates.

## Backend Safety Rules
- Do not rename or restructure `apps/`.
- Do not modify backend files unless required for `/fe` serving.
- Keep backend API prefixes and logic unchanged.

## Delivery Rules
- Keep generated artifacts out of source commits.
- Ensure frontend builds before proposing backend integration changes.

## Shared Cache Rules
- Use shared cache utilities under `frontend/shared/cache/` for frontend data caching.
- Use the global hybrid route/data-loading pattern across apps/pages:
  - one lightweight page/dashboard load route for core data required together at initial render
  - separate lazy routes for heavy sections, optional/collapsible sections, date-windowed data, search pages, modal/detail data, and write/transactional operations
  - avoid one giant "everything" route and avoid unnecessary small requests for data that is always needed together
  - avoid N+1 request patterns
  - scope cache keys by natural data boundary:
    - dashboard load route by entity/page key
    - timeline/date-window route by entity + date range
    - detail route by entity id
    - search route by submitted params
- Admin Users page rule:
  - initial load and refresh should use a single bundled route call: `GET /api/auth/v1/admin/users/load`
  - do not issue separate initial requests for `/admin/tenants`, `/admin/apps`, `/admin/roles`, `/admin/invitations`, and `/admin/users`
  - keep mutation routes separate (`create/revoke invite`, `update/disable user`) and reload bundled data once after mutation when local patching is insufficient
  - avoid duplicate load effects/request loops on page mount
- Default data-loading policy is `stale-while-revalidate`.
- Search-submit revalidation rule:
  - on valid search submit, render matching cached results immediately when available
  - always send a fresh network request in the background for the submitted params
  - keep cached results mounted while refreshing and update UI/cache when fresh data returns
  - ignore stale/out-of-order responses from older submissions
- Search form validity rule (global across all apps/pages):
  - do not run search while typing; search runs only on submit
  - do not show validation errors before user action on untouched empty search forms
  - search submit action must be hidden or disabled until the form is valid
  - clear action must stay hidden until at least one search field has a value
  - empty/invalid search form submits must not trigger backend requests
  - when no required fields exist, require at least one searchable field value before enabling search
  - page search state should be preserved when navigating away and back (draft, submitted params, current results, cache-status metadata)
- Offline-friendly search rule:
  - if refresh fails and cached results exist, keep cached results visible and show a non-blocking cached-data message
  - if refresh fails and no cached results exist, show normal error state
- Global loading-overlay rule:
  - page-level blocking busy overlays are allowed only when there is no cache-backed data to render
  - when cached data is visible and a background refresh is running, use non-blocking refresh indicators (for example cache-status chips), not blocking overlays
- Manual refresh actions must use `network-only` and update cache on success.
- Mutations (`create/update/delete`) must patch relevant cache entries and invalidate or mark related cache keys stale.
- Never persist sensitive values (`passwords`, tokens, secrets) to `localStorage`/`sessionStorage`; use in-memory cache for sensitive data.
- Avoid app-specific duplicate cache implementations when shared cache utilities can be reused.
- Cache status UI should use low-emphasis floating chips; do not let cache notes/buttons occupy primary layout flow.
- Page cache chips should stay fixed near viewport bottom; modal chips should stay fixed/sticky near modal bottom.
- Cache refresh chips must use button semantics, keyboard focus styles, tooltip text on hover/focus, and context-specific `aria-label`.
- In report/table dialogs, anchor cache chips to the modal shell/footer (outside primary scroll content) so large tables never push chip placement.
- Preserve stale-while-revalidate UX: show cached data immediately and revalidate without blanking visible cached panels.
