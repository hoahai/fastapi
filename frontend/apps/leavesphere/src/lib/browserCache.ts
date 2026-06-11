import {
  getCacheData,
  invalidateCache,
  markCacheStale,
  readCacheSnapshot,
  removeCache,
  setCacheData,
  type CacheSnapshot,
  type CacheSource,
} from "@shared/cache";

export type BrowserCacheSource = CacheSource;
export type BrowserCacheSnapshot<T> = CacheSnapshot<T>;
export type BrowserCacheSnapshotEntry<T> = {
  key: string;
  snapshot: BrowserCacheSnapshot<T>;
};

type WriteBrowserCacheOptions = {
  source?: BrowserCacheSource;
  fetchedAt?: number;
  version?: string;
  hash?: string;
};

const CACHE_NAMESPACE = "leavesphere:cache:";

const cacheBaseOptions = {
  namespace: CACHE_NAMESPACE,
  storage: "persistent" as const,
};

export function readBrowserCacheSnapshot<T>(key: string): BrowserCacheSnapshot<T> | null {
  return readCacheSnapshot<T>(key, {
    ...cacheBaseOptions,
    allowExpired: true,
  });
}

export function readBrowserCache<T>(key: string, options?: { allowExpired?: boolean }): T | null {
  return getCacheData<T>(key, {
    ...cacheBaseOptions,
    allowExpired: Boolean(options?.allowExpired),
  });
}

export function writeBrowserCache<T>(
  key: string,
  data: T,
  ttlMs: number,
  options?: WriteBrowserCacheOptions,
): void {
  setCacheData<T>(key, data, {
    ...cacheBaseOptions,
    ttlMs,
    source: options?.source,
    fetchedAt: options?.fetchedAt,
    version: options?.version,
    hash: options?.hash,
  });
}

export function markBrowserCacheStale(key: string): void {
  markCacheStale(key, cacheBaseOptions);
}

export function removeBrowserCache(key: string): void {
  removeCache(key, cacheBaseOptions);
}

export function removeBrowserCacheByPrefix(prefix: string): void {
  invalidateCache(prefix, {
    ...cacheBaseOptions,
    match: "prefix",
    mode: "remove",
  });
}
