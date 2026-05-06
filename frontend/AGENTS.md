# Frontend Agent Guide

## Scope
- Frontend source lives under `frontend/`.
- App-specific code lives under `frontend/apps/<app-name>/`.
- Shared UI code lives under `frontend/shared/`.

## Routing and API Rules
- Production frontend route is `/fe`.
- Frontend assets must resolve under `/fe/assets`.
- API calls must stay relative (for example: `/api/...`).
- Do not hardcode localhost or deployment domains.

## Design Rules
- Follow `frontend/DESIGNS.md`.
- Reuse shared components/styles before adding one-off styles.
- Update design docs when introducing new reusable patterns.

## Backend Safety Rules
- Do not rename or restructure `apps/`.
- Do not modify backend files unless required for `/fe` serving.
- Keep backend API prefixes and logic unchanged.

## Delivery Rules
- Keep generated artifacts out of source commits.
- Ensure frontend builds before proposing backend integration changes.
