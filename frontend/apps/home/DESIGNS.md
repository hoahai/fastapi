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
- App cards must not imply access for signed-out users.
- Signed-out state should emphasize sign-in CTA.
- Signed-in state should reflect real app access from `/api/auth/v1/session/me`.
- Apps that are not yet ready should be hidden rather than shown as coming soon.
- If signed in with no app access, show a helpful no-access empty state.
- Cross-app permission pattern: downstream app forms open in read-only mode for users without edit permission, and create/add/save/delete/upload controls remain disabled.

## Auth UX Identity
- Auth pages should look consistent with TheSphereWorks/Workspace Home visual language.
- Auth page source belongs to shared auth (`frontend/shared/auth/pages/`), not Home-only folders.
- Login page must support only `Sign in with password` (email/password).
- Do not include Google login or magic-link UI.
- Keep invite-only context visible in login and no-access states.

## System Alignment
- Follow the global design system in `frontend/DESIGNS.md`.
- Keep typography, spacing, and interaction patterns consistent with shared frontend standards.

## App Styling Boundaries
- Avoid Tradsphere-specific styling in Home views unless it is intentionally promoted as shared design.
- Shared reusable visual primitives should be moved to `frontend/shared/`.
