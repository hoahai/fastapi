# Cache Behavior (SpendSphere)

This document describes the SpendSphere cache behavior for account codes and Google Ads clients.

## Cache file
- Location: `fastapi/caches.json`
- Override via env: `SPENDSPHERE_ACCOUNT_CODE_CACHE_PATH`
- Single shared file for all tenants and cache types.

## Tenant scoping
- Cache entries are stored per tenant key (derived from `X-Tenant-Id`).
- Each tenant has its own account code and Google Ads client data.

## TTL behavior
- Default TTL: 86400 seconds (24 hours).
- Tenant override: `spendsphere -> CACHE -> ttl_time` in `tenant.yaml`.
- Optional env override: `SPENDSPHERE_GOOGLE_ADS_CLIENTS_CACHE_TTL_SECONDS` (takes priority if set).

If a cache entry is stale, the API **blocks and refreshes** from the source before returning data.

## Account code cache
- Stored under `account_codes` per tenant.
- For account codes, two scopes are stored per tenant:
  - `active` for `include_all=false`
  - `all` for `include_all=true`
- Each scope has its own `updated_at` timestamp and TTL.

## Google Ads clients cache
- Stored under `google_ads_clients` per tenant.
- Reads return cached data when fresh.
- If stale or `refresh_cache=true`, data is fetched from Google Ads and the cache is updated.

## Refresh controls
- `GET /api/spendsphere/v1/google-ads?refresh_cache=true`
- `GET /api/spendsphere/v1/ui/selections?refresh_cache=true`
- `POST /api/spendsphere/v1/google-ads/refresh`
