# Sphere Load Policy

This document defines the shared load behavior for Sphere pages that load data on demand, support local drafts, and need cache-aware reloads.

The goal is to standardize the interaction model across apps and pages so each page follows the same rules for:

- first load
- reload of the same criteria
- hard refresh
- offline fallback
- dirty-state confirmation
- baseline reset

## Terminology

- `criteria`: the current page selection that identifies the loaded dataset.
- `criteria key`: a stable string or structured fingerprint derived from the active criteria.
- `baseline snapshot`: the page state captured after the first successful load for a criteria.
- `dirty state`: any unsaved local edits or draft changes that differ from the baseline snapshot.
- `hard refresh`: a refresh that ignores cache and forces a backend fetch.
- `cache-first load`: show cached data immediately when available, then refresh in the background if needed.

## Core Rules

### 1. No auto-load on mount

- The page must not fetch the main dataset automatically on initial render.
- The page waits until the user clicks `Load`.
- The page may restore local UI state from browser storage, but that is not the same as loading the main dataset.

### 2. Load is criteria-driven

- A load action is always tied to the active criteria key.
- For LeaveSphere pages today, the criteria key is just `year`.
- For other Sphere pages later, the criteria key may contain multiple inputs.
- The page must compare the current criteria key against the last loaded criteria key to decide whether this is:
  - first load for that criteria
  - reload of the same criteria
  - load of a new criteria

### 3. First load uses cache-first behavior

- When the user loads a criteria for the first time:
  - show cached data immediately if it exists
  - if the cache is stale, still show it first and refresh in the background
  - if the cache is missing, fetch from the backend
  - if the network fails and cache exists, keep the cached data visible and report offline or fallback status
  - if there is no cache and no network, show a failure state

### 4. Reloading the same criteria resets the page

- If the user clicks `Load` again for the same criteria key, the action becomes a reload/reset action.
- If the page has dirty state, the UI must show a confirmation dialog before proceeding.
- After confirm, the page must restore the baseline snapshot from the first successful load for that criteria.
- The reload must clear all draft changes and return the screen to the original loaded state for that criteria.

### 5. Hard refresh is a separate action

- The cache status chip is a hard refresh control.
- It must ignore cache and force a backend fetch.
- It should refresh the underlying cached dataset and update the visible data.
- It should preserve the current working UI context as much as possible.
- It is not a reset action.
- It must not be used as the same mechanism as the `Load` reset flow.

### 6. Offline behavior

- Offline loading is allowed when cached data exists.
- Offline loading should prefer cached data over failure.
- If no cache exists, offline loading may fail with a clear message.

## Shared Page Contract

Every page that adopts this policy should provide the following pieces of state or equivalent abstractions:

- `criteriaKey`: stable identifier for the current selection
- `loadedCriteriaKey`: the criteria key for the currently loaded dataset
- `baselineSnapshot`: the state captured after the first successful load
- `currentDraftState`: the current mutable page state
- `isDirty`: whether the current draft state differs from the baseline snapshot
- `loadMode`:
  - `initial`
  - `reload`
  - `hardRefresh`
- `cacheState`:
  - `missing`
  - `fresh`
  - `stale`
  - `offlineFallback`

## Required Behavior by Action

### Initial `Load`

- Resolve the current criteria key.
- If there is a cached snapshot, render it immediately.
- If the cache is stale, keep the cached snapshot visible while a backend refresh runs.
- If the cache is missing, fetch from backend.
- After a successful response, store the response as the new baseline snapshot.

### Same-criteria `Load`

- Compare the current criteria key to the loaded criteria key.
- If they match, treat the click as reload/reset.
- If dirty, prompt the user before proceeding.
- If confirmed, restore the baseline snapshot and clear draft state.

### Cache Chip Refresh

- Force a backend refresh.
- Ignore cache for the request path.
- Keep the current UI selection and view context unless the page explicitly needs a different reset.
- Update cache with the fresh response.

## LeaveSphere Mapping

LeaveSphere currently uses only one criteria dimension for the Leave Management page:

- `year`

That means:

- first load for a year follows the cache-first rule
- same-year `Load` is the reload/reset action
- cache status chip is the hard refresh action

This page should still be implemented through the shared policy so the same behavior can later be applied to other Sphere pages with richer criteria sets.

## Shared Implementation Direction

When centralizing this later, the shared layer should own:

- criteria key normalization
- cache state detection
- cache-first vs hard-refresh policy selection
- baseline snapshot capture and restoration
- dirty-state comparison
- reload confirmation contract

Page-specific code should only provide:

- how to derive the criteria key
- how to capture and restore the page baseline
- how to determine whether current state is dirty
- how to render the page-specific confirmation copy
- how to render page-specific cache status text

## Current Shared Foundation

The first reusable frontend implementation lives in:

- `frontend/shared/hooks/useCriteriaLoadPolicy.ts`

It currently provides:

- `resolveCriteriaLoadPlan(...)`
- `useCriteriaBaselineStore(...)`
- `areCriteriaSnapshotsEqual(...)`

That file is the starting point for page-level adoption in LeaveSphere, TradSphere, Shiftzy, FundSphere, and future Sphere pages.

## Non-Goals

- This spec does not define the exact React hook or component API yet.
- This spec does not prescribe a storage backend for cached data or baseline snapshots.
- This spec does not define visual styling beyond the action semantics.

## Outcome

If a page follows this policy, users get:

- explicit control over when data loads
- immediate cached render on first load when available
- safe reloads that do not silently discard drafts
- a separate hard refresh path for cache bypass
- consistent offline fallback behavior
