# Home Frontend Design Notes

## Workspace Home Identity
- The Workspace Home / portal page should keep a distinct banner and portal visual style.
- Home should use its own gradient identity appropriate for workspace-level navigation.
- Workspace Home is served at `/` as the primary frontend route.
- Home should include an Announcements section (secondary emphasis) and an Apps section.
- Apps should render in a responsive grid:
  - mobile: 1 column
  - tablet: 2 columns
  - desktop: 3+ columns when space allows

## System Alignment
- Follow the global design system in `frontend/DESIGNS.md`.
- Keep typography, spacing, and interaction patterns consistent with shared frontend standards.

## App Styling Boundaries
- Avoid Tradsphere-specific styling in Home views unless it is intentionally promoted as shared design.
- Shared reusable visual primitives should be moved to `frontend/shared/`.
