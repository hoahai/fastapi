# Frontend Design System

## Style Direction
- Clean modern SaaS/admin dashboard.
- Professional and uncluttered visual hierarchy.
- Rounded surfaces and controls.
- Consistent spacing and typography.

## Theme Principles
- Use a neutral base palette for structure.
- Use one primary accent color for key actions.
- Keep contrast high for readability.
- Avoid one-off visual styles when shared tokens/components can be used.

## Spacing Scale
- Base spacing unit: 4px.
- Preferred scale: 4, 8, 12, 16, 20, 24, 32, 40.
- Section spacing should be larger than component-internal spacing.

## Typography
- Use the app-selected sans-serif family consistently.
- Body text should prioritize readability over compactness.
- Headings should communicate hierarchy with size/weight, not excessive decoration.

## Color Usage
- Neutral colors: layout, backgrounds, borders, text.
- Primary accent: primary CTA, active/focus states, and highlights.
- Semantic colors: success/warning/error only for state feedback.

## Buttons
- Rounded corners, clear visual states (default/hover/focus/disabled).
- Primary button reserved for highest-priority action in a region.
- Secondary/ghost variants for lower-priority actions.
- Permission-gated mutations: if user lacks edit permission for the active app, allow opening detail/edit forms in read-only mode, and disable mutation actions (`Add`, `Create`, `Save`, `Delete`, `Upload`, etc.) while keeping read-only navigation/browse actions available.

## Forms
- Inputs use clear labels and visible focus states.
- Validation feedback should be concise and placed near fields.
- Keep form field spacing consistent.
- Search forms are submit-only and must never trigger backend search requests while typing.
- Do not show search validation errors on untouched empty forms.
- Search submit actions must stay hidden or disabled until search criteria are valid.
- Search clear actions should stay hidden until at least one field has a value.
- Empty/invalid search submits must not call backend APIs.
- If a search form has no required fields, default validity is `at least one searchable field is filled`.
- Search pages should preserve non-sensitive state across in-app navigation (draft, submitted criteria, results, cache metadata).

## Search Cache UX
- On valid submit, show matching cached results immediately when present.
- After showing cache, always run a background network refresh for the submitted params.
- Keep cached results visible while refreshing; do not blank result areas during revalidation.
- Enter/init cache-check should use page-level overlay so load state is consistent across pages.
- Manual cache-chip refresh should use page-level overlay for that page.
- Search/criteria submits should use section-level overlay when only result sections reload.
- Manual refresh actions (if present) should use network-only for the current submitted search context.
- Ignore stale/out-of-order responses from older submissions so results always match the latest valid submit.
- If refresh fails and cache exists, keep cached results visible and show a small non-blocking cached-data message.
- If refresh fails and no cache exists, show the normal error state.

## Cards and Panels
- Cards/panels should use subtle elevation or border separation.
- Keep title, actions, and content areas clearly separated.
- Avoid dense wall-of-text layouts.
- When a page stacks related sections (for example Tradsphere dashboard), keep new sections in the same content column rhythm unless a full-width requirement is explicit.
- For weekly schedule visualization, prefer timeline-style week buckets over month-grid calendars when business data is week-centric.
- Prefer bounded Gantt-style horizontal timelines (sticky label column + internal scroll) over unbounded full-history grids for schedule activity views.
- For grouped schedule timelines, prefer parent business grouping rows (for example EstNum) with child station lanes beneath.
- Timeline period expansion controls should append/prepend adjacent bounded windows and preserve already-rendered timeline content while loading.

## Layout Rules
- Prefer responsive grid/flex layouts.
- Keep content width bounded for readability.
- Use reusable layout primitives before custom page-level overrides.
- Normal authenticated workspace/app/admin pages should use shared `AppPageLayout` as the default page shell.
- For pages using shared shell layouts, keep cache/footer controls in a viewport-fixed in-app footer layer, not in normal document flow.
- Fixed page footer containers should be transparent; only chip controls carry visual styling.
- Reserve bottom spacer height in shared page layout so fixed footer chips never cover actionable content.
- If a page has no cache/data-refresh concept, omit the footer chip instead of forcing a placeholder cache footer.

## Loading Overlay Rules
- Use shared loading primitives (`PageLoadingLayer`, `SectionLoadingLayer`, `PageLoadingOverlay`, `SectionLoadingOverlay`) instead of page-local ad-hoc overlays.
- Shared loading contract keys:
  - `pageInitializing`
  - `pageRefreshing`
  - `cacheChipRefreshing`
  - `sectionLoading`
  - `searchLoading`
- Page-level overlays:
  - show during page hydration/initialization and whole-page refresh flows
  - show for cache-chip force refresh flows
  - keep content visible behind the overlay
  - stay above fixed footer controls and below modal/dialog layers
- Section-level overlays:
  - apply only to the affected section when data loading is section-scoped
  - preserve unaffected sections and cached content when possible
  - search pages should overlay result sections instead of blocking the full page when only results refresh

## Message Stack Rules
- Use shared message primitives (`PageMessageStack`, `SectionMessageStack`) for warning/info/error/success feedback.
- Page-level state messages must render under the page banner/header.
- Section-level state messages must render inside the affected section/card, below section title/actions and above section content.
- Replace ad-hoc alert bars (for example cached/offline refresh warnings) with shared message stacks.
- Message UI should remain soft and theme-consistent (subtle border, rounded corners, readable text), not browser-native alerts.

## Hybrid Route/Data Loading
- Use one lightweight page/dashboard load route for core data that must be rendered together initially.
- Use separate lazy routes for heavy sections, optional/collapsible sections, date-windowed sections, search pages, modal/detail data, and write/transactional operations.
- Avoid one giant aggregate endpoint that loads everything for a page.
- Avoid fragmented request patterns for data that is always required together.
- Avoid N+1 data-loading/request patterns.
- Admin Users page pattern:
  - use one bundled core-load request (`GET /api/auth/v1/admin/users/load`) for `tenants/apps/roles/users/invitations`
  - initial mount should issue one bundled request; refresh should issue one bundled request
  - keep invite/user mutations on separate endpoints and then update local state or run one bundled reload
  - avoid effect dependency loops that trigger duplicate fetches on the same page load
- Cache by natural scope:
  - dashboard load: entity/page key
  - timeline/date-window data: entity + date range
  - detail data: entity id
  - search data: submitted params

## Frontend Portal Pattern
- The frontend workspace root is `/` and should render the Workspace Home portal page.
- The frontend uses a shared app shell for workspace pages, not page-specific wrappers.
- Desktop uses a floating rounded left sidebar (fixed position) with spacing from viewport edges.
- The shell uses a mobile menu/drawer pattern on small screens.
- Desktop sidebar is collapsible and should persist locally.
- Manual collapse/expand remains the source of truth for saved sidebar preference.
- When manually collapsed, desktop sidebar may temporarily expand on hover/focus for navigation preview.
- Temporary hover/focus expansion must not update saved collapsed preference and must return to collapsed on mouse leave/blur.
- Expanded desktop sidebar should show section label, app icons, app labels, active state, and optional `Soon` badges.
- Collapsed desktop sidebar should be icon-only (no text labels and no leftover letter badges).
- Sidebar remains fixed while page content scrolls independently.
- Sidebar entries include `Home / Portal` and app links/status (for example `Tradsphere`, future app placeholders).
- Sidebar entries must be visibility-scoped by signed-in access from `/api/auth/v1/session/me`; hide unauthorized app/page items.
- App pages follow `/<app-name>/home` (for example `/tradsphere/home`).
- `/fe` is compatibility-only and should redirect/alias to root-based routes.
- Portal home should include a clear title, short guidance text, an Announcements section, and an Apps section with direct navigation cards.
- The Apps section should use responsive cards: mobile 1 column, tablet 2 columns, desktop 3+ columns when space allows.
- Portal app visibility must be auth-aware:
  - Signed-out users should see sign-in CTA and should not see app actions as available access.
  - Signed-in users should see only apps they can access, derived from `/api/auth/v1/session/me`.
  - Unauthorized/unavailable apps should be hidden from Workspace Home by default.
  - Signed-in users with no app permissions should see a clear empty state:
    - `You do not have access to any apps yet. Contact your workspace administrator.`
- Reuse a shared banner component so portal and app pages keep the same visual language.

## Auth UX Pages
- Auth pages under `/auth/*` should match TheSphereWorks visual language and remain responsive.
- Auth page source should live in shared auth (`frontend/shared/auth/pages/`), not app-local folders.
- Login page copy:
  - Title: `Welcome to TheSphereWorks`
  - Subtitle: `Sign in to access your workspace apps.`
  - Method: `Sign in with password`
- Login must support only `email/password`; do not show Google login or magic-link UI in Phase 1.
- Password recovery/update route is `/auth/update-password`; keep the UX email/password-only and do not reintroduce magic-link sign-in UI.
- Callback page should show friendly loading and error states without exposing tokens or raw secrets.
- Invite accept page should show invite summary state (email/workspace/app/role/status/expiration) and clear state messaging (pending/accepted/revoked/expired/mismatch).
- Unauthorized and pending-invite pages should show clear context + actions (`Back to Workspace Home`, `Sign out`, `Back to login` as applicable).
- Auth feedback should use styled inline cards/toasts, not browser-native `alert`/`confirm`.
- Never display or log tokens/service-role secrets in frontend UI.
- Profile security UX should keep profile fields and password fields in separate visual sections/cards.
- Profile account summary should use read-only summary cards and avoid root-level `Active Tenant` identity fields.
- Tenant context in profile UI should appear under app/access scope rows (for example App Access), not as a root identity value.
- Password forms should include inline strength/help text, friendly validation, and clear success/failure states.
- Password create/reset/change copy should use:
  - `Password must be at least 8 characters and include at least one letter and one number.`
- Admin password reset UI should be a secondary action in member edit flows and must never reveal generated tokens/password values.
- Backend remains source of truth for authorization; frontend guards are UX-level routing only.
- Hidden links are UX only; direct URL access must still be blocked by route guards/backend permission checks.
- Admin/member role display must use:
  - `Super Admin` (global role)
  - `Admin`, `Editor`, `Viewer` (tenant/app roles)
  - Never relabel `Viewer` as `User`.

## Page State Persistence
- Preserve useful UI state across frontend route navigation so returning users keep context.
- Normal authenticated workspace/app/admin pages should remember stage when users navigate away and back.
- Use shared `useScopedPersistentState` (and shared page-state utilities) for page stage persistence instead of one-off `localStorage`/`sessionStorage` logic in page files.
- Persist page stage with user + tenant + app + page scoping so one tenant/user context never restores into another context.
- Use `localStorage` for durable user preferences that should survive browser restarts.
- Use `sessionStorage` for page-scoped navigation state that should reset when the browser session ends.
- Persist compact primitives/IDs and simple UI preferences; avoid persisting full backend records when IDs are sufficient.
- Restore persisted state on page return and clear stale persisted selections gracefully when backing data no longer exists.
- Never persist sensitive values (`passwords`, secrets, tokens).
- Do not persist transient operational state by default (loading flags, error messages, temporary modal-open state, unsafe stale payloads).

## Banner Gradients
- Keep page banners consistent in typography, spacing, border radius, and button styling.
- Do not reuse one exact gradient for every page; each page/app banner should have a distinct but related gradient identity.
- Keep gradients professional, soft, and modern; avoid overly bright or clashing color stops.
- Recommended direction:
  - Portal / Workspace banner: soft blue-purple workspace gradient.
  - TradSphere banner: media/broadcast-inspired blue-indigo gradient.
  - Future apps: define unique but related gradients that match each app identity.

## Responsive Behavior
- Mobile-first styles with progressive enhancement.
- Ensure critical actions remain visible and reachable on small screens.
- Avoid horizontal overflow by default.

## Accessibility Expectations
- Keyboard navigable controls and dialogs.
- Visible focus rings.
- Sufficient color contrast.
- Meaningful labels and button text.

## Modal Form Pattern
- Reuse shared dialog primitives with centered layout, rounded panel, dark backdrop, and existing open/close animation.
- Prefer label-input row layouts on desktop (label column + control column) and stacked layout on mobile.
- Mark required fields with a red asterisk and keep validation feedback close to inputs.
- Include a top-right close button (`X`) and a primary submit action in the footer.
- Modal primary form action labels should be concise (`Save` in edit contexts unless domain wording requires otherwise).
- Modal primary submit actions should follow dirty + valid rules globally:
  - hide submit action when pristine
  - disable submit when dirty but invalid/loading/submitting
  - allow submit only when dirty + valid + not submitting
- When forms map to backend payloads, keep UI labels user-friendly but preserve exact payload keys in submit logic.
- When a modal mirrors an existing detail form (for example TradSphere Estimate Number vs Account Information), reuse the same label/value row styling and control proportions.
- Read-only fields should look non-editable without resembling disabled text inputs (avoid bordered disabled-input appearance when plain read-only value text is clearer).
- Dropdown/select controls should visually match the app's canonical dropdown treatment for consistency (for example Account Information `Billing Type` style).
- For Tradsphere scheduling inputs, prefer date-pickers configured for Monday-start weeks and America/Chicago timezone, with broadcast-calendar behavior when available.
- Estimate/flight forms may include a compact preset range helper (`Month`, `Quarter`, `Year`, `Custom`) near flight date fields to auto-fill start/end dates while keeping manual edits available.
- Keep helper controls visually secondary to persisted form fields: compact spacing, smaller controls, and subtle container treatment.
- Helper controls inside modals must remain fully contained in the field column and must not cause horizontal overflow.
- For business concepts represented by start/end dates (for example flight windows), prefer a single combined visible date-range field while preserving separate backend payload keys internally.
- Flight Range/Quick Range helper interactions should update the combined date-range display immediately.
- When possible, place date helper controls (like Quick Range) inside the related date-range popover to keep the main form concise.
- Inside date-range popovers, use a subtle divider between preset helper controls and manual start/end date fields.
- Preset range helpers must preserve backend payload date keys/format and must integrate with dirty-state detection so edit submit buttons only enable when effective values changed.
- If broadcast-calendar business rules are partially implemented on frontend, document assumptions inline and add TODO markers where backend confirmation is still needed.
- For edit modals that load existing data, either:
  - delay opening until data is ready, or
  - open modal shell with a clear loading state (spinner + loading text) and do not show incomplete form values.
- While an edit form is still in loading-shell mode (original values not yet resolved), close actions (outside click / `X`) should behave as clean close and must not trigger unsaved-change warnings.
- `Save Changes` actions in edit mode must be disabled until the user modifies at least one editable field compared to normalized original loaded values.
- `Save Changes` must stay disabled while form data is loading or submitting.
- Auto-seeded default values in create mode should be treated as initial baseline values (not unsaved changes by themselves).
- Auto-seeded values in edit mode should only count as unsaved changes when they are filling missing persisted fields after real detail load.
- Do not use `Cancel` buttons in modal footers.
- Outside click must not close a modal when the form has unsaved changes.
- Closing with unsaved changes should use a discard confirmation flow; avoid silently discarding user edits.
- Never use browser-native `alert`/`confirm` dialogs for app workflows.
- Use app-styled confirmation dialogs for unsaved/destructive decisions.
- Unsaved-change confirmation actions should use explicit labels: `Keep editing` and `Discard changes`.
- The safe action (`Keep editing`) should be visually emphasized; destructive discard should be clearly marked.
- Read-only/disabled display fields should render as regular readable values (no disabled opacity, no edit-style border), with layout signaling read-only intent.
- Related-record modals (for example TradSphere Station with Delivery Method + Contacts) must keep edits in a single local draft state and must not issue backend writes per field change.
- For Station modal behavior:
  - `Add Station` opens create mode.
  - Clicking a Station item opens edit mode.
  - Use one primary `Create Station` / `Save Changes` action to apply the modal change set.
  - Prefer one detail-read endpoint and one transactional create/update endpoint when backend support exists.
- Station modal visual structure should feel like one unified editor surface, not multiple heavy boxed cards.
- Separate sections in Station modal using heading rhythm, spacing, and subtle dividers instead of strong panel borders/backgrounds.
- Large station/related-record modals with three primary sections should use responsive grid columns that naturally resolve to 3/2/1 columns based on available width.
- Keep inter-column spacing breathable (for example larger desktop gaps like `gap-8` to `gap-10`) so dense form sections do not feel cramped.
- When width becomes constrained, columns must wrap/stack instead of squeezing controls into narrow unreadable field widths.
- Station modal form labels should match shared Tradsphere form label styling (size/weight/color/line-height/spacing) across all sections and nested station-related dialogs.
- Internal IDs (for example delivery method row ids) should remain hidden in user-facing station form sections unless operationally required for the task.
- Entity action controls inside dense station sections should prefer compact icon buttons with hover/focus tooltips over heavy text-button rows.
- Selector dialogs/lists (for example delivery-method selection) must provide enough vertical list space with scrollable results to avoid clipped options.
- Delivery-method selector lists must show enough identifying metadata when names are not unique (for example name + URL + username + deadline + password).
- Cache selector option data in memory only for faster reopen flows; do not persist selector option payloads to `localStorage`/`sessionStorage`.
- Delivery-method passwords may be shown in Station workflows when they are intentionally shared operational credentials.
- Keep delivery-method selector data in memory only; do not persist password-bearing selector payloads in browser storage.
- Selector search should match visible identifying fields (name, URL, username, deadline, password, and optional ID).
- Nested Station helper modals (`Select Delivery Method`, `Add/Edit Delivery Method`, `Add Existing Contact`, `Create/Edit Contact`) must use top-right `X` close only; do not add footer `Cancel`/`Close` buttons.
- Nested Station helper modals must follow dirty-close guards: if dirty, `X`/Escape uses unsaved-changes confirmation and outside-click does not silently close.
- Selector-style helper modals must keep primary action disabled until selection is valid and changed from current/baseline state.
- Delivery Method add/edit form rows must show red required `*` on required labels (name, URL, username, deadline) and keep optional fields unmarked.
- Delivery Method and Contact helper forms should use consistent label-column + input-column alignment on desktop and stack on small screens.
- Delivery Method edit workflows should show a compact `Also used by` section with other station usage when data is available.
- Station Delivery Method action icons should be ordered left-to-right as `Add`, `Edit`, `Select`, each with matching tooltip and `aria-label`.
- Station contact actions should use icon buttons (`add existing`, `create`, `copy`, `edit`, `remove`) and remain draft-based until Station `Save Changes`/`Create Station`.
- Station form rows should keep stable label/value column geometry so read-only values, text inputs, dropdowns, and textareas share the same value-column left edge.
- Dropdown menus used inside modals must not be clipped by scroll containers; render menu layers above modal content with sufficient z-index and scrollable option regions.
- All icon-only controls in Station modal workflows should include both tooltip text and matching accessible `aria-label`.
- Icon-only action controls must use clear, task-specific icons (not ambiguous glyphs) plus tooltip + `aria-label` parity.
- Tooltip layers should not be clipped by cards/scroll containers; prefer portal/fixed-position tooltip rendering for overlay reliability.
- Multi-column Station modal section headers should reserve consistent header height so section divider lines align on the same horizontal baseline.
- Traffic Delivery Method inside Station modal should use action-driven workflow (`Select`, `Add`, `Edit`) rather than direct inline field editing.
- Contact cards inside Station modal should stay compact and action-oriented (`Copy contact`, `Edit`, `Remove`) while remaining readable.
- `Copy contact` actions should stay easy to access on each station contact card and use Gmail-ready formatting.
- Prefer draft-based contact linking/editing flows (`add existing`, `create`, `update`, `remove`) and apply them through station save when backend bundled APIs support it.
- Large related-record modals should use responsive grid layouts that wrap/stack as space shrinks; avoid fixed breakpoint-only multi-column layouts that squeeze fields.
- Section cards in large modals should keep a minimum usable width (target around `360px+` when viewport allows).
- Form rows should stack label above input on narrow widths instead of compressing input controls to unreadable sizes.
- Text inputs and textareas must stay horizontally readable and must never collapse to widths where placeholder/content appears as vertically stacked characters.

## Image Preview Motion
- Logo/image zoom previews should reuse the shared dialog/modal motion pattern instead of abrupt portal-only rendering.
- Use animated backdrop fade and content fade/scale transitions for both open and close.
- Preferred motion profile for preview content: subtle fade + scale with slight vertical lift (`opacity`, `scale`, small `y` offset), consistent with modal interactions.

## Consistency Rules
- `frontend/apps/` contains app-level frontend code.
- Workspace Home lives in `frontend/apps/home/`.
- Tradsphere lives in `frontend/apps/tradsphere/`.
- Add new frontend apps under `frontend/apps/<app-name>/`.
- Shared UI belongs in `frontend/shared/`.
- App-specific UI belongs in `frontend/apps/<app-name>/`.
- Shared cache/hooks/styles/theme primitives belong in `frontend/shared/`.
- Keep `frontend/src/` minimal and focused on root entry/router wiring when needed.
- New patterns must be documented here before broad adoption.
- API request UX should use a shared request layer so success/failure toast behavior is consistent across pages and dialogs, not implemented ad hoc per form.
- Failed API requests should show a toast with the backend message/detail when available.
- Successful mutation requests (`POST`/`PUT`/`PATCH`/`DELETE`) should show a toast by default; callers may override title/message per action.
- After successful mutation requests, do not trigger broad page/dashboard reload endpoints by default.
- Prefer targeted local state/cache patching from mutation result data, plus narrow cache invalidation for only affected entities.
- Use full reload endpoints only when mutation responses are insufficient to keep client state correct, and document why in code.
- Detail-heavy views (for example entity detail forms and schedule/detail reports) should use browser cache with TTL for faster reopen/revisit behavior.
- Detail caches should use scoped keys (tenant/account/entity/context) to avoid collisions and must support targeted invalidation after related mutations.
- Frontend cache default policy is `stale-while-revalidate`: show cached data fast, then revalidate when cache is missing/expired.
- Manual refresh controls should use `network-only` and update both UI and cache after fresh responses.
- Shared cache metadata should include at least `fetchedAt`, `ttlMs`, and `source` (`cache` or `network`), with optional version/hash metadata.
- Mutation flows should patch immediate entity cache where possible and invalidate/mark stale dependent caches (lists/details/related options).
- Sensitive values must stay memory-only and must not be persisted to browser storage.
- Reuse `frontend/shared/cache` utilities across apps instead of creating per-app duplicate cache logic.
- Cache freshness notes should be low-emphasis UI (`text-xs`/smaller with muted color) and should not compete with primary actions.
- Prefer footer/bottom placement for `Last updated`/`Data source` notes in pages, panels, and modals.
- Avoid placing cache-status notes inline with primary control rows (load/filter/view-mode/action groups) unless layout constraints require it.
- Cache refresh chips should be floating secondary controls, not normal layout blocks that shift content.
- Page-level cache chips should stay fixed near the viewport bottom and remain visible during page scroll.
- Modal-level cache chips should stay fixed/sticky to the modal bottom while modal content scrolls.
- Cache refresh chips must be clickable buttons with tooltip text and focus-visible keyboard accessibility.
- For report/table modals, cache chips must be anchored to the modal shell/footer layer, never inside the main table/report scroll content.
- Cache revalidation should keep visible cached UI mounted (`stale-while-revalidate`) and must not clear loaded data before fresh data arrives.
- Long report detail views should use a viewport-constrained modal shell (`max-height` + bounded height in detail mode) so footer controls never get displaced by table height.
- In report modals, keep `Header -> Scroll Viewport -> Footer` structure, and apply `min-height: 0` to the scrollable flex child.

## Reusable Component Primitives
- Prefer these reusable UI primitives before creating app-level one-offs:
  - `Section`
  - `SectionHeader`
  - `SectionItem` / `EntityItem`
  - `ModalShell`
  - `ModalFooter`
  - `ModalFormLayout`
  - `FormRow`
  - `ReadOnlyField`
  - `IconActionButton`
  - `Tooltip`
  - `FloatingActionMenu`
  - `CacheStatusChip`
- `Section` should support: title, optional description, optional actions area, optional divider, and consistent spacing.
- `SectionItem`/entity items should support: primary action, secondary floating menu actions, selected state, disabled state, compact/normal variants, and accessible labels.
- `ModalShell` should centralize: title/description, close X, open/close animation, backdrop, scrollable body, dirty-close guard behavior, and shell size variants.
- `ModalFooter` should centralize footer behavior: no cancel button, submit-only when valid/changed, and no browser-native confirm/alert for close/discard flows.
- Form layouts should rely on shared rows and read-only display primitives; avoid one-off spacing offsets that break label/input alignment.
- Icon-only actions must use shared icon button + tooltip patterns and always include `aria-label`.
- Floating menus/tooltips must render as overlays so they do not change layout height and are not clipped by parent containers.
- Cache/status interactions should use one shared chip pattern for page-level and modal-level placement, refresh action, tooltip, and loading state.

## Entity Item Action Menus
- EstNum/entity cards should keep their primary click behavior on the card itself (for example: load/show schedule).
- Secondary item actions should be shown in a hover-triggered floating attached contextual menu.
- Station cards may use the same hover-triggered floating attached menu pattern for secondary actions (for example `Copy contact`).
- Non-clickable entity items must not use click cursor styles or hover motion affordances (no lift/scale hover effects).
- Do not render item action menus in normal layout flow; render as floating overlays so item, section, and container heights never change.
- Prefer reusable floating action menu patterns (portal/fixed positioning + anchored placement) so menus are not clipped by scroll/overflow containers.
- Menu interactions must not trigger the card primary click; stop event propagation for menu pointer/click actions.
- `Copy contact` must use already-loaded station/contact data only; do not add routes or make extra API calls only for clipboard actions.
- `Copy contact` output must be Gmail-ready (`Full Name <email@example.com>` when both values exist, fallback to email-only when name is unavailable).
- Clipboard success/failure feedback must use non-browser-native UI (for example app toast); do not use `alert`/`confirm`.
- For TradSphere EstNums, card click opens a native Schedule modal only when `hasSchedule` is true.
- EstNums without schedules must keep the same visual style (no gray/opacity reduction); only interaction/cursor changes.
- EstNum edit flows (for example `Update Estimate`) must remain in the contextual action menu, not on primary card click.
- Schedule reports in UI must render as native table UI (compact/detail), not embedded PDF viewers/images.
- Schedule viewer modals should size to report/table content width first (fit-content behavior) with viewport limits (`max-width` around `92-96vw`, `max-height` around `85-90vh`), and avoid fixed wide modal widths for compact content.
- Schedule loading states should use report-viewer-sized modals (not small form-style modals) to avoid jarring size jumps before report data appears.
- Compact schedule view should not leave large unused right-side modal space; summary/header bars should align to report/table width.
- Large report modals should use a constrained column-flex shell where header/controls stay outside the report scroll region when practical.
- Report scroll regions should use `overflow: auto` on a shrinkable flex child (`min-height: 0`) so oversized content scrolls vertically and horizontally inside the modal.
- Schedule tables should provide subtle row and column hover highlighting for readability on wide datasets; highlights should stay light and not overpower the base report palette.
- Schedule table data should be cached in memory per `EstNum` + view mode during the current page session and reused when toggling modes.
- Compact/detail table structure should follow backend report-generation rules; if structured backend table data is unavailable, show a clear limitation state instead of inventing inaccurate rows.

## TradSphere Estimate Numbers Search Page
- Route: `/tradsphere/estnums`.
- Sidebar hierarchy: `Tradsphere` is a parent app entry with child pages (`/tradsphere/home`, `/tradsphere/estnums`).
- The page is search-first: do not pre-load all EstNums on initial render.
- Do not run any default EstNum load on first render (no planning/current-quarter/current-year fallback fetches).
- Use a multi-field lazy-submit form (Enter/Search) with: `Estimate Number`, `Account`, `Buyer`, `Media Type`, `Month / Note`, `Year`, `Quarter`, `Created today`.
- Search is submit-only: no request-on-type and no debounced type-ahead search.
- Empty submit (all fields empty + `Created today` off) should clear results and return to empty-state guidance.
- `Created today` must resolve to backend created-date filtering in America/Chicago timezone, not client-side full-list filtering.
- Prefer backend search with query + limit (+ cursor/offset when available); avoid client-side full-dataset filtering.
- If current backend cannot express a multi-field combination efficiently, show a clear limitation and propose fielded backend search params instead of full-load fallback filtering.
- Cache results by submitted form params (all fields included), not draft text; use stale-while-revalidate for the last submitted search only.
- Clearing the form and submitting should clear search results (no default fallback load).
- Provide a floating page cache-status chip only when submitted search results exist; refresh runs network-only for the last submitted search context.
- Group search results by account/client first, then by period (for example year/quarter) when possible.
- Scheduled EstNums should open the existing Schedule modal; unscheduled rows must keep normal visual style and simply not open schedules.
- Add/Edit flows must reuse the existing EstimateNumberModal (create/edit modes), not a duplicated form implementation.
- Do not show `Copy EstNum` action in Estimate Numbers search results.
- Flight-date display in results should be `MM/DD/YYYY → MM/DD/YYYY` while backend payload remains ISO.

## TradSphere Contacts Page
- Route: `/tradsphere/contacts`.
- Purpose: search contacts, edit contact details, review station usage, and copy Gmail-ready contact values.
- Keep this page search-first with no default contact load on open.
- Search must be explicit-submit only (`Search` button or Enter), never request-on-type.
- Empty submit should not run a request and should show guidance/validation.
- Cache submitted contact-search results by full submitted form params.
- Use stale-while-revalidate behavior for repeated submitted searches and keep cached results mounted during refresh.
- Show loading as a results-area overlay while searching/refreshing.
- Show floating page cache-status chip only after a submitted search exists; chip refresh runs `network-only` on the current submitted search.
- Group results primarily by contact type; fallback grouping uses company when contact type is unavailable.
- Contact modal should include a read-only `Used by Stations` section with station relationship details.
- Contact modal should use a responsive two-section layout (`Contact` + `Used by Stations`) that renders as two columns on wide screens and stacked sections on narrower screens.
- Contact modal form spacing should stay clean and avoid nested padded containers that compress field width.

## TradSphere Stations Page
- Route: `/tradsphere/stations`.
- Purpose: search/manage stations and review delivery method + REP contact summaries.
- Keep page search-first with no default station load.
- Search is explicit-submit only (`Search` button/Enter); never search while typing.
- Search form supports multi-field filters (station code/name, media type, language, affiliation, contact).
- Search validity follows global rule: no required fields => enable search when at least one field has a value.
- Submitted searches are cache-first/offline-first:
  - show matching cached results immediately
  - refresh in background for submitted params
  - keep cached results visible during refresh
  - on refresh failure with cache, keep cached results and show non-blocking warning
- Show floating cache-status chip only after a submitted search context exists; chip refreshes current submitted search with `network-only`.
- Use grouped station results with collapsible groups (prefer Media Type grouping; fallback to name initial).
- Station result cards should mirror the compact read-only station relationship layout used in Contact modal `Used by Stations`, and include Delivery Method + REP contact rows.
- `Add Station` and `Edit Station` must reuse the existing `StationModal` (no duplicate station form implementation).
- Contact modal submit action label is `Save` and should follow dirty + valid modal rules (no submit action when pristine).
- Required indicators in contact forms must align with backend schema rules (`email` required + valid).
- Contact modal `Contact Type` should be editable via dropdown and persisted through existing stations-contact link update APIs when link rows exist.
- Contact form `Active` control should be an accessible toggle/switch.
- Contact phone fields should auto-format US display values while keeping backend-safe payload compatibility.
- Contact name fields should auto-case and support Full Name parsing/sync with First Name + Last Name.
- `Used by Stations` details should render as compact read-only cards with internal scroll for larger lists (not a table).
- Contact copy format should be `Full Name <email@example.com>` with email-only fallback.
- Avoid client-side full dataset scans and avoid per-contact request fan-out; use batched backend queries.
