# TradSphere Frontend Design Rules

This app inherits global standards from `frontend/DESIGNS.md`.

## App-Specific Direction
- Maintain dashboard-centric information density with clear grouping.
- Use cards/panels for account, schedules, and station sections.
- Keep primary flow focused on account selection -> dashboard load -> edits.

## Component Guidance
- Prefer existing `src/components/ui` primitives and dashboard components.
- Keep table/list filtering interactions lightweight and responsive.
- Preserve existing visual hierarchy and spacing rhythm.

## Interaction Guidance
- Surface API/loading errors inline and close to related actions.
- Preserve optimistic/local cache behaviors unless requirements change.

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

## Station Modal
- Delivery Method linked-stations usage should render as compact cards (not plain text rows), with responsive 2-column layout when space allows.
- Linked-station cards should include subtle hover feedback and keep status metadata compact (for example media type and EstNum list when available).
