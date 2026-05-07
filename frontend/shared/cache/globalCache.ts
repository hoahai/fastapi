export type CachePolicy = "cache-first" | "network-first" | "stale-while-revalidate" | "network-only";
export type CacheSource = "cache" | "network";
export type CacheStorage = "persistent" | "memory";

export type CacheEntry<T> = {
  fetchedAt: number;
  ttlMs: number;
  data: T;
  source: CacheSource;
  version?: string;
  hash?: string;
};

export type CacheSnapshot<T> = CacheEntry<T> & {
  ageMs: number;
  isExpired: boolean;
};

export type CacheNamespaceOptions = {
  namespace?: string;
  scope?: string;
  storage?: CacheStorage;
};

export type SetCacheOptions = CacheNamespaceOptions & {
  ttlMs: number;
  fetchedAt?: number;
  source?: CacheSource;
  version?: string;
  hash?: string;
};

export type ReadCacheOptions = CacheNamespaceOptions & {
  allowExpired?: boolean;
};

export type InvalidateCacheOptions = CacheNamespaceOptions & {
  mode?: "stale" | "remove";
  match?: "exact" | "prefix";
};

const DEFAULT_NAMESPACE = "frontend:cache:";

const memoryStore = new Map<string, string>();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function resolveNamespace(namespace?: string): string {
  return namespace || DEFAULT_NAMESPACE;
}

function buildStorageKey(key: string, options?: CacheNamespaceOptions): string {
  const namespace = resolveNamespace(options?.namespace);
  const scope = options?.scope?.trim();
  if (!scope) {
    return `${namespace}${key}`;
  }
  return `${namespace}${scope}:${key}`;
}

function parseStoredEntry<T>(raw: string): CacheEntry<T> | null {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!isRecord(parsed) || !("data" in parsed) || typeof parsed.ttlMs !== "number") {
      return null;
    }

    if ("fetchedAt" in parsed && typeof parsed.fetchedAt === "number") {
      return {
        fetchedAt: parsed.fetchedAt,
        ttlMs: parsed.ttlMs,
        data: parsed.data as T,
        source: parsed.source === "cache" ? "cache" : "network",
        version: typeof parsed.version === "string" ? parsed.version : undefined,
        hash: typeof parsed.hash === "string" ? parsed.hash : undefined,
      };
    }

    if ("timestamp" in parsed && typeof parsed.timestamp === "number") {
      return {
        fetchedAt: parsed.timestamp,
        ttlMs: parsed.ttlMs,
        data: parsed.data as T,
        source: "network",
      };
    }

    return null;
  } catch {
    return null;
  }
}

function computeIsExpired(fetchedAt: number, ttlMs: number): boolean {
  if (ttlMs <= 0) {
    return true;
  }
  return Date.now() - fetchedAt > ttlMs;
}

function readPersistent(key: string): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writePersistent(key: string, value: string): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // no-op when storage is unavailable
  }
}

function removePersistent(key: string): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.removeItem(key);
  } catch {
    // no-op when storage is unavailable
  }
}

function listPersistentKeys(prefix: string): string[] {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const keys: string[] = [];
    for (let index = 0; index < window.localStorage.length; index += 1) {
      const key = window.localStorage.key(index);
      if (key && key.startsWith(prefix)) {
        keys.push(key);
      }
    }
    return keys;
  } catch {
    return [];
  }
}

function readRawStorage(key: string, storage: CacheStorage): string | null {
  if (storage === "memory") {
    return memoryStore.get(key) ?? null;
  }
  return readPersistent(key);
}

function writeRawStorage(key: string, value: string, storage: CacheStorage): void {
  if (storage === "memory") {
    memoryStore.set(key, value);
    return;
  }
  writePersistent(key, value);
}

function removeRawStorage(key: string, storage: CacheStorage): void {
  if (storage === "memory") {
    memoryStore.delete(key);
    return;
  }
  removePersistent(key);
}

function listRawStorageKeys(prefix: string, storage: CacheStorage): string[] {
  if (storage === "memory") {
    return [...memoryStore.keys()].filter((key) => key.startsWith(prefix));
  }
  return listPersistentKeys(prefix);
}

export function readCacheSnapshot<T>(key: string, options?: ReadCacheOptions): CacheSnapshot<T> | null {
  const storage = options?.storage ?? "persistent";
  const storageKey = buildStorageKey(key, options);
  const raw = readRawStorage(storageKey, storage);
  if (!raw) {
    return null;
  }

  const parsed = parseStoredEntry<T>(raw);
  if (!parsed) {
    removeRawStorage(storageKey, storage);
    return null;
  }

  const ageMs = Math.max(0, Date.now() - parsed.fetchedAt);
  const snapshot: CacheSnapshot<T> = {
    ...parsed,
    ageMs,
    isExpired: computeIsExpired(parsed.fetchedAt, parsed.ttlMs),
    source: "cache",
  };

  if (!options?.allowExpired && snapshot.isExpired) {
    return null;
  }

  return snapshot;
}

export function getCacheData<T>(key: string, options?: ReadCacheOptions): T | null {
  return readCacheSnapshot<T>(key, options)?.data ?? null;
}

export function setCacheData<T>(key: string, data: T, options: SetCacheOptions): void {
  const storage = options.storage ?? "persistent";
  const storageKey = buildStorageKey(key, options);
  const payload: CacheEntry<T> = {
    fetchedAt: typeof options.fetchedAt === "number" ? options.fetchedAt : Date.now(),
    ttlMs: options.ttlMs,
    data,
    source: options.source === "cache" ? "cache" : "network",
    version: options.version,
    hash: options.hash,
  };
  writeRawStorage(storageKey, JSON.stringify(payload), storage);
}

export function removeCache(key: string, options?: CacheNamespaceOptions): void {
  const storage = options?.storage ?? "persistent";
  const storageKey = buildStorageKey(key, options);
  removeRawStorage(storageKey, storage);
}

export function markCacheStale(key: string, options?: CacheNamespaceOptions): void {
  const storage = options?.storage ?? "persistent";
  const storageKey = buildStorageKey(key, options);
  const raw = readRawStorage(storageKey, storage);
  if (!raw) {
    return;
  }

  const parsed = parseStoredEntry<unknown>(raw);
  if (!parsed) {
    removeRawStorage(storageKey, storage);
    return;
  }

  const stalePayload: CacheEntry<unknown> = {
    ...parsed,
    fetchedAt: Date.now() - Math.max(parsed.ttlMs, 1) - 1,
  };
  writeRawStorage(storageKey, JSON.stringify(stalePayload), storage);
}

export function invalidateCache(
  matcher: string | RegExp | ((logicalKey: string) => boolean),
  options?: InvalidateCacheOptions,
): void {
  const storage = options?.storage ?? "persistent";
  const namespace = resolveNamespace(options?.namespace);
  const scopePrefix = options?.scope?.trim() ? `${options.scope}:` : "";
  const storagePrefix = `${namespace}${scopePrefix}`;
  const mode = options?.mode ?? "stale";
  const match = options?.match ?? "exact";

  const matchesLogicalKey = (logicalKey: string): boolean => {
    if (typeof matcher === "string") {
      if (match === "prefix") {
        return logicalKey.startsWith(matcher);
      }
      return logicalKey === matcher;
    }
    if (matcher instanceof RegExp) {
      return matcher.test(logicalKey);
    }
    return matcher(logicalKey);
  };

  const keys = listRawStorageKeys(storagePrefix, storage);
  for (const storageKey of keys) {
    const logicalKey = storageKey.slice(storagePrefix.length);
    if (!matchesLogicalKey(logicalKey)) {
      continue;
    }

    if (mode === "remove") {
      removeRawStorage(storageKey, storage);
    } else {
      const raw = readRawStorage(storageKey, storage);
      if (!raw) {
        continue;
      }
      const parsed = parseStoredEntry<unknown>(raw);
      if (!parsed) {
        removeRawStorage(storageKey, storage);
        continue;
      }
      const stalePayload: CacheEntry<unknown> = {
        ...parsed,
        fetchedAt: Date.now() - Math.max(parsed.ttlMs, 1) - 1,
      };
      writeRawStorage(storageKey, JSON.stringify(stalePayload), storage);
    }
  }
}

export function shouldFetchNetwork(
  policy: CachePolicy,
  snapshot: CacheSnapshot<unknown> | null,
): boolean {
  if (policy === "network-only" || policy === "network-first") {
    return true;
  }

  if (!snapshot) {
    return true;
  }

  if (policy === "cache-first") {
    return snapshot.isExpired;
  }

  if (policy === "stale-while-revalidate") {
    return snapshot.isExpired;
  }

  return true;
}

export type ResolveCachePolicyResult<T> = {
  cachedSnapshot: CacheSnapshot<T> | null;
  shouldUseCache: boolean;
  shouldFetchNetwork: boolean;
};

export type RefreshCacheOptions = CacheNamespaceOptions & {
  ttlMs: number;
  version?: string;
  hash?: string;
};

export function resolveCachePolicy<T>(
  policy: CachePolicy,
  snapshot: CacheSnapshot<T> | null,
): ResolveCachePolicyResult<T> {
  return {
    cachedSnapshot: snapshot,
    shouldUseCache: policy !== "network-only" && !!snapshot,
    shouldFetchNetwork: shouldFetchNetwork(policy, snapshot),
  };
}

export async function refreshCache<T>(
  key: string,
  loader: () => Promise<T>,
  options: RefreshCacheOptions,
): Promise<CacheSnapshot<T>> {
  const data = await loader();
  const fetchedAt = Date.now();
  setCacheData<T>(key, data, {
    ttlMs: options.ttlMs,
    namespace: options.namespace,
    scope: options.scope,
    storage: options.storage,
    source: "network",
    fetchedAt,
    version: options.version,
    hash: options.hash,
  });
  const snapshot = readCacheSnapshot<T>(key, {
    namespace: options.namespace,
    scope: options.scope,
    storage: options.storage,
    allowExpired: true,
  });
  if (!snapshot) {
    throw new Error("Failed to refresh cache snapshot.");
  }
  return snapshot;
}
