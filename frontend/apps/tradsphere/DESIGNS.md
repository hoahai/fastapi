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
- Use a single prominent search bar; avoid introducing filter-heavy controls.
- Include a short helper line under search input with supported fields and tips.
- Do not fetch all EstNums on initial render.
- Default initial load should fetch only current year + previous year EstNums.
- Search should be lazy: run on Enter/submit, not on every keystroke.
- Ambiguous submissions must prompt for confirmation before request dispatch (no silent interpretation).
- For `YYYY` input, present `Search by year` and `Search by EstNum` choices.
- For month/year and quarter/year patterns (for example `6/26`, `Q1'26`), present `structured period search` and `text search` choices.
- Prefer backend search with `query + limit + pagination` and keep request count minimal.
- `today` keyword behavior should map to backend created-date search in America/Chicago timezone.
- Cache default/search results by confirmed search interpretation (mode + params) and keep stale data visible while revalidating.
- Provide page-level cache-status chip with refresh scoped to current submitted query/default result.
- Group result presentation by account/client and period sections for quick scanning.
- Scheduled rows should open existing schedule modal flow; unscheduled rows remain normal style.
- Add/Edit actions should use existing Estimate Number modal behavior.
- Do not show Copy EstNum action on Estimate Numbers results.
- Flight-date display should be `MM/DD/YYYY → MM/DD/YYYY` (display only).
