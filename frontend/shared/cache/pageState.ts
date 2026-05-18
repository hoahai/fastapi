export type ScopedPageState = {
  userKey: string;
  tenantSlug: string;
  appCode: string;
  pageCode: string;
};

const PAGE_STATE_NAMESPACE = "workspace:page-state:v1:";

function normalizeKeySegment(value: string, fallback: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return fallback;
  }
  return normalized.replace(/[^a-z0-9._-]+/g, "_");
}

function buildUserPrefix(userKey: string): string {
  return `${PAGE_STATE_NAMESPACE}${normalizeKeySegment(userKey, "anonymous")}:`;
}

export function buildScopedPageStateStorageKey(scope: ScopedPageState): string {
  const userSegment = normalizeKeySegment(scope.userKey, "anonymous");
  const tenantSegment = normalizeKeySegment(scope.tenantSlug, "default");
  const appSegment = normalizeKeySegment(scope.appCode, "app");
  const pageSegment = normalizeKeySegment(scope.pageCode, "page");
  return `${PAGE_STATE_NAMESPACE}${userSegment}:${tenantSegment}:${appSegment}:${pageSegment}`;
}

export function readScopedPageState<T>(
  scope: ScopedPageState,
  validate?: (value: unknown) => value is T,
): T | null {
  if (typeof window === "undefined") {
    return null;
  }
  const storageKey = buildScopedPageStateStorageKey(scope);
  try {
    const raw = window.localStorage.getItem(storageKey);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as unknown;
    if (validate && !validate(parsed)) {
      window.localStorage.removeItem(storageKey);
      return null;
    }
    return parsed as T;
  } catch {
    return null;
  }
}

export function writeScopedPageState<T>(scope: ScopedPageState, value: T): void {
  if (typeof window === "undefined") {
    return;
  }
  const storageKey = buildScopedPageStateStorageKey(scope);
  try {
    window.localStorage.setItem(storageKey, JSON.stringify(value));
  } catch {
    // Ignore persistence write failures.
  }
}

export function removeScopedPageState(scope: ScopedPageState): void {
  if (typeof window === "undefined") {
    return;
  }
  const storageKey = buildScopedPageStateStorageKey(scope);
  try {
    window.localStorage.removeItem(storageKey);
  } catch {
    // Ignore persistence remove failures.
  }
}

export function clearScopedPageStatesForUser(userKey: string): void {
  if (typeof window === "undefined") {
    return;
  }
  const prefix = buildUserPrefix(userKey);
  try {
    const keys: string[] = [];
    for (let index = 0; index < window.localStorage.length; index += 1) {
      const key = window.localStorage.key(index);
      if (!key || !key.startsWith(prefix)) {
        continue;
      }
      keys.push(key);
    }
    for (const key of keys) {
      window.localStorage.removeItem(key);
    }
  } catch {
    // Ignore persistence remove failures.
  }
}
