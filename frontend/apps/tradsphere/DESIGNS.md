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
