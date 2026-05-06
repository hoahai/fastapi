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

## Forms
- Inputs use clear labels and visible focus states.
- Validation feedback should be concise and placed near fields.
- Keep form field spacing consistent.

## Cards and Panels
- Cards/panels should use subtle elevation or border separation.
- Keep title, actions, and content areas clearly separated.
- Avoid dense wall-of-text layouts.

## Layout Rules
- Prefer responsive grid/flex layouts.
- Keep content width bounded for readability.
- Use reusable layout primitives before custom page-level overrides.

## Responsive Behavior
- Mobile-first styles with progressive enhancement.
- Ensure critical actions remain visible and reachable on small screens.
- Avoid horizontal overflow by default.

## Accessibility Expectations
- Keyboard navigable controls and dialogs.
- Visible focus rings.
- Sufficient color contrast.
- Meaningful labels and button text.

## Consistency Rules
- Shared UI belongs in `frontend/shared/`.
- App-specific UI belongs in `frontend/apps/<app-name>/`.
- New patterns must be documented here before broad adoption.
