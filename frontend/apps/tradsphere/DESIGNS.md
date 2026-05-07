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
