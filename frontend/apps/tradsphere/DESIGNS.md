# TradSphere Frontend Design Rules

This app inherits global standards from `frontend/DESIGNS.md`.

## App-Specific Direction
- Maintain dashboard-centric information density with clear grouping.
- Use cards/panels for account, schedules, and station sections.
- Keep primary flow focused on account selection -> dashboard load -> edits.
- Accounts dashboard core payload should be loaded from `GET /api/tradsphere/v1/ui/accounts/load?accountCode=...`.
- Keep route loading hybrid:
  - lightweight core dashboard load on initial render
  - separate lazy/date-windowed route for Schedule Timeline
  - separate detail/search/transactional routes for modal/search/write workflows

## Component Guidance
- Prefer existing `src/components/ui` primitives and dashboard components.
- Keep table/list filtering interactions lightweight and responsive.
- Preserve existing visual hierarchy and spacing rhythm.
- Tradsphere Home `Schedule Timeline` must follow the same card rhythm and column alignment as `EstNums - Schedules` and `Stations` (no full-width calendar layout).

## Schedule Timeline
- Replace calendar-style schedule visualization with a bounded Gantt-style weekly timeline section.
- Place timeline below the `Stations` section in the same right-column dashboard stack.
- Keep section collapsible with default expanded behavior.
- Rows are grouped by `EstNum` with collapsible station child rows.
- Child row label: station code primary, station name secondary.
- Columns: broadcast weeks (Monday start) for the bounded visible period.
- Left label column should stay sticky while horizontal scrolling.
- Render all returned rows; if content is tall, keep the section height bounded and allow internal vertical scrolling.
- Render contiguous active weeks as one bar segment and non-contiguous weeks as separate segments.
- Bars are visual-only for activity ranges; do not display spot/gross text in bars.
- Previous/Next timeline controls must load only one adjacent bounded period per click and extend the currently loaded range (prepend/append), not replace it.
- Keep each request bounded (<= 13 weeks), cap combined loaded range to 26 weeks, and block further expansion with subtle guidance when capped.
- Cache timeline payloads by account + visible range + schema version (`v3`), keep cached timeline mounted while background refresh runs, and avoid unbounded cache growth.
- Merge adjacent window payloads by week/item keys and dedupe active-week arrays so already-loaded periods are not refetched or duplicated.

## Interaction Guidance
- Surface API/loading errors inline and close to related actions.
- Preserve optimistic/local cache behaviors unless requirements change.
- Keep profile editing simple and explicit:
  - `/profile` should expose first-name + last-name inputs
  - save writes combined `fullName` to auth profile APIs
  - keep email/tenant read-only
- Enforce role-gated mutation UX:
  - viewers stay read-only
  - only editors/admins can mutate
  - open existing-record forms in read-only mode and disable mutation actions (`Add`, `Create`, `Save`, `Delete`, `Upload`) when edit permission is missing
- Search flows are submit-only: never run backend search while typing.
- Do not show validation errors on untouched empty search forms.
- Search submit button/action should stay hidden or disabled until the form is valid.
- Search clear button/action should stay hidden until at least one field has a value.
- Empty/invalid search should never trigger backend requests.
- For forms without required fields, enable search only when at least one field is filled.
- Preserve non-sensitive search page state across in-app route switches (draft, submitted params, results, cache metadata).
- Submitted searches should be cache-first:
  - show matching cached results immediately when available
  - refresh in background from network for the same submitted params
  - keep cached results visible while refreshing
  - ignore stale/out-of-order responses from older submits
- Offline-friendly behavior:
  - cached results + refresh failure => keep cached results and show non-blocking cached-data message
  - no cache + refresh failure => show normal error state
- When cache-backed page content is visible, refresh should stay non-blocking; page-level blocking overlays are for no-cache loads only.

## Estimate Numbers Page
- Add and maintain a dedicated `/tradsphere/estnums` page focused on fast search.
- Treat Estimate Numbers as a child page under the Tradsphere sidebar parent.
- Use an explicit multi-field search form with these fields: `Estimate Number`, `Account`, `Buyer`, `Media Type`, `Month / Note`, `Year`, `Quarter`, `Created today`.
- Keep helper copy concise and practical.
- Do not fetch all EstNums on initial render.
- Do not run any default EstNum load on open (no current/previous year auto-fetch).
- Search should be lazy: run on Enter/submit, not on every keystroke.
- Do not run request-on-type and do not debounce typing.
- Empty submit should clear results and return to an empty-state prompt.
- Prefer backend search with `query + limit + pagination` and keep request count minimal.
- `Created today` should map to backend created-date filtering in America/Chicago timezone.
- Cache submitted-search results by full submitted form params and keep stale data visible while revalidating.
- Show page-level cache-status chip only after submitted search results exist; refresh must target the last submitted search only.
- Group result presentation by account/client and period sections for quick scanning.
- Scheduled rows should open existing schedule modal flow; unscheduled rows remain normal style.
- Add/Edit actions should use existing Estimate Number modal behavior.
- Do not show Copy EstNum action on Estimate Numbers results.
- Flight-date display should be `MM/DD/YYYY → MM/DD/YYYY` (display only).

## Contacts Page
- Add and maintain `/tradsphere/contacts` under the Tradsphere sidebar children.
- Keep Contacts page search-first with no default data load.
- Search must be submit-only (`Search` / Enter), not request-on-type.
- Contact-search cache keys must include all submitted fields and use stale-while-revalidate behavior.
- Keep cached result groups visible during refresh; show a results-area loading overlay.
- Show floating cache-status chip only after a submitted search exists; chip refreshes current submitted search with network-only behavior.
- Search requests should load lightweight contact-card data only; usage graph details should not load during search.
- Group results by contact type by default; fallback to company grouping when type data is absent.
- Contact modal supports add/edit with dirty-state guard, close by `X`, no cancel button, and read-only `Used by Stations` details.
- Contact modal open should load full usage graph cache-first, then refresh from network.
- Contact modal should use responsive two-section layout (`Contact` + `Used by Stations`): two columns on wide desktop, stacked on medium/small screens.
- Contact modal should avoid nested padded form boxes that narrow inputs; keep cleaner station-style spacing/alignment.
- Contact modal submit label is `Save` and should follow dirty + valid modal submit rules.
- Required indicators must follow backend contact schema rules (`email` required + valid for create/update contact payloads).
- `Contact Type` should be editable in the Contact modal via dropdown and save through existing stations-contact link APIs when link rows are present.
- `Active` should be an accessible toggle/switch control, not a checkbox.
- Phone inputs (`office`, `cell`) should auto-format US numbers while preserving backend-compatible values.
- Name inputs should auto-case (including common hyphenated names) and keep Full Name <-> First/Last sync behavior.
- `Used by Stations` should render compact read-only cards with internal scroll for larger usage sets (not a table).
- Contacts UI should also show read-only `Used by Accounts`:
  - no used-by summary on contact result cards (modal-focused detail)
  - dedicated section in Contact modal below `Used by Stations`
- Contacts modal should also show read-only `Used by EstNums` below `Used by Accounts`.
- Contact copy action should output Gmail-ready text (`Full Name <email@example.com>` with email-only fallback).

## Stations Page
- Add and maintain `/tradsphere/stations` under the Tradsphere sidebar children.
- Keep Stations page search-first with no default station load.
- Search must be explicit-submit only (Search/Enter), never request-on-type.
- Search form uses multi-field filters: Station Code, Station Name, Media Type, Language, Affiliation, Contact Name/Email.
- Media Type and Language should be dropdown controls.
- Market and Delivery Method are not part of Stations page search form.
- If no required fields exist, search is valid when at least one field has a value.
- Cache submitted station-search results by full submitted params and keep stale results visible during background refresh.
- Show page-level loading overlay only when searching with no visible cache-backed results.
- If refresh fails and cached results exist, keep cached results and show a non-blocking warning.
- Show floating cache-status chip after a submitted search exists; chip refreshes current submitted search with `network-only`.
- Group station results in collapsible sections (Media Type grouping first; fallback name initial).
- Add/Edit station actions must reuse existing `StationModal` create/edit modes.
- Station cards should follow the same compact read-only data presentation pattern as Contact modal `Used by Stations`, and include Delivery Method + REP contact rows.

## Station Modal
- Delivery Method linked-stations usage should render as compact cards (not plain text rows), with responsive 2-column layout when space allows.
- Linked-station cards should include subtle hover feedback and keep status metadata compact (for example media type and EstNum list when available).
