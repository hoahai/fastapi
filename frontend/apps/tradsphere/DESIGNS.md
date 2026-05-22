# TradSphere Frontend Design Rules

This file contains TradSphere-only UI conventions.
Global shell/shared rules are defined outside this app folder.

## Layout and Density
- Keep account-centric dashboard information dense but readable.
- Preserve section/card rhythm across Accounts, EstNums, Stations, Timeline, and modals.

## Search and Cache UX
- Use explicit submit-only search flows (no request-on-type).
- Keep cached results visible during refresh.
- Show non-blocking cache fallback messaging on network failure.

## Permission UX
- Preserve read-only behavior for non-edit roles.
- Hide or disable mutation actions when edit permission is missing.

## Modal UX
- Use app-styled unsaved-change confirmations.
- Keep scroll bounded inside large modal content areas.
