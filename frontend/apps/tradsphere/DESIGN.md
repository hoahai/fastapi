# TradSphere Page Foundation

This file defines the default page/layout baseline for new TradSphere pages.

## 1) Page Shell
- Use shared `AppPageLayout` for page-level structure.
- Keep a single banner at the top (`PageBanner` or app wrapper like `HeroBanner`).
- Keep main content in `SectionCard` blocks with consistent spacing (`gap-5` rhythm).

## 2) Header/Banner
- Use `PageBanner` styling for all top-level pages.
- Keep `eyebrow` label consistent (`TradSphere`).
- Keep one primary header action (if any) in the banner action slot.

## 3) Page Footer
- Use `PageCacheFooter` as the page footer.
- Footer must be viewport-fixed through `AppPageLayout` (in-app footer, not browser footer).
- Footer container stays transparent; only the chip is visually styled.
- Footer must stay above page content and below modal/dialog layers.
- `AppPageLayout` must reserve bottom spacer height so fixed footer never covers interactive content.
- Cache status chip must be clickable and run a network refresh action.

## 4) Load/Action Area
- Use `LoadActionArea` for the control row that combines selectors/filters and `Load` actions.
- Keep `Load` explicit; do not auto-fetch page data on mount.
- Disable load/sync actions while busy states are active.

## 5) Dropdown Pattern
- Use `AppDropdown` for selects.
- Do not use browser-native `<select>` in page or modal forms.
- Use `searchable={false}` for short fixed enums.

## 6) Modal Layout
- Use shared dialog primitives and keep modal structure as:
  - Header
  - Scrollable content body
  - Footer actions (`DialogFooter`)
  - Cache footer (`ModalCacheFooter`) when modal data is cache-backed
- Keep modal refresh chips in modal footer, outside primary scroll content.

## 7) Cache-First + Offline Pattern
- Render cached data first when available.
- Refresh in background unless user explicitly requests network-only refresh.
- If refresh fails and cache exists, keep cached UI visible and show non-blocking message.
- If no cache exists and fetch fails, show normal error state.

## 8) Empty/Loading States
- Use consistent empty cards (`border-dashed`, subtle background) for no-data states.
- Shared loading contract state keys:
  - `pageInitializing`
  - `pageRefreshing`
  - `cacheChipRefreshing`
  - `sectionLoading`
  - `searchLoading`
- Page-level overlay rules:
  - Use `PageLoadingLayer` for initial page hydration/checking cached state.
  - Use `PageLoadingLayer` when whole-page data is refreshing.
  - Use `PageLoadingLayer` when cache-chip force refresh is running.
  - Keep page layout visible behind the overlay.
- Section-level overlay rules:
  - Use `SectionLoadingLayer`/`SectionLoadingOverlay` only on the affected section.
  - Preserve other sections and existing cached content whenever possible.
  - Search pages should overlay results section instead of the full page when only results load.

## 9) Page + Section Messages
- Use shared `PageMessageStack` for page-wide state feedback and render it under the page banner/header.
- Use shared `SectionMessageStack` for section-specific state feedback and render it inside the affected `SectionCard`.
- Section messages must appear below section title/actions and above section content.
- Use variants (`info`, `success`, `warning`, `error`) instead of one-off alert bars.
- Cache/offline refresh fallbacks (for example `Showing cached results. Could not refresh.`) must use shared message stacks.

## 10) Page Stage Persistence
- Normal authenticated pages should restore practical stage when users navigate away and return.
- Use shared scoped persistence (`useScopedPersistentState` + shared page-state helpers), not one-off storage keys in page files.
- Scope persisted page state by user + tenant + app + page.
- Persist only safe stage: filters/search input, selected rows/ids, submitted criteria, cache references, and explicit loaded-stage markers.
- Do not persist sensitive/auth fields, destructive confirmation state, or unsafe unsaved modal drafts unless a page already has an explicit safe draft model.
