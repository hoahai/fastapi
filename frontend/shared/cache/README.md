# Shared Frontend Cache

Reusable cache utilities for all frontend apps under `frontend/`.

## Default policy
- `stale-while-revalidate`
  - render cached data immediately when present
  - fetch network data when cache is missing/expired
  - write fresh network result back to cache

## Core API
- `readCacheSnapshot(key, options)`
- `getCacheData(key, options)`
- `setCacheData(key, data, options)`
- `refreshCache(key, loader, options)`
- `markCacheStale(key, options)`
- `removeCache(key, options)`
- `invalidateCache(matcher, options)`
- `shouldFetchNetwork(policy, snapshot)`
- `resolveCachePolicy(policy, snapshot)`

## Shared TTLs
- Shared TTL constants live in `frontend/shared/cache/ttl.ts`.
- Tradsphere uses `TRADSPHERE_CACHE_TTL_MS` to avoid duplicated per-file TTL literals.

## Policies
- `cache-first`
- `network-first`
- `stale-while-revalidate`
- `network-only`

## Metadata
Each entry stores:
- `fetchedAt`
- `ttlMs`
- `source` (`cache` or `network`)
- optional `version`/`hash`

## Sensitive data
- Use `storage: "memory"` for sensitive data.
- Do not persist passwords/tokens/secrets to browser storage.
