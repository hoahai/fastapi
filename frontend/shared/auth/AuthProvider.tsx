import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

import { createAuthProvider } from "./provider";
import type { SignUpWithPasswordResult } from "./provider/types";
import type { AccessProfile, AuthStatus, AuthUser, SupabaseSession } from "./types";

const SESSION_STORAGE_KEY = "workspace.auth.session.v1";
const USER_STORAGE_KEY = "workspace.auth.user.v1";
const TENANT_STORAGE_KEY = "workspace.auth.tenantSlug.v1";
const ACCESS_PROFILE_CACHE_KEY = "workspace.auth.accessProfileCache.v1";
const ACCESS_PROFILE_CACHE_TTL_MS = 2 * 60 * 1000;

class AccessProfileRequestError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "AccessProfileRequestError";
    this.status = status;
  }
}

type AccessProfileCacheEntry = {
  cachedAt: number;
  profile: AccessProfile;
};

type AccessProfileCacheStore = Record<string, AccessProfileCacheEntry>;

type AuthContextValue = {
  status: AuthStatus;
  user: AuthUser | null;
  session: SupabaseSession | null;
  tenantSlug: string;
  accessProfile: AccessProfile | null;
  accessLoading: boolean;
  accessError: string | null;
  providerName: string;
  setTenantSlug: (tenantSlug: string) => void;
  signInWithPassword: (email: string, password: string) => Promise<void>;
  signUpWithPassword: (email: string, password: string) => Promise<{ status: "signed_in" | "confirm_email" }>;
  signOut: () => Promise<void>;
  ensureFreshSession: () => Promise<SupabaseSession | null>;
  getAccessToken: () => string | null;
  // Backward-compatible aliases for existing callers.
  signInPassword: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

function readJson<T>(key: string): T | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) {
      return null;
    }
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function writeJson(key: string, value: unknown): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Ignore storage write failures.
  }
}

function removeStorage(key: string): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.removeItem(key);
  } catch {
    // Ignore storage write failures.
  }
}

function getDefaultTenantSlug(): string {
  const envValue = String(import.meta.env.VITE_DEFAULT_TENANT_SLUG || "").trim().toLowerCase();
  return envValue;
}

async function fetchAccessProfile(session: SupabaseSession, tenantSlug: string): Promise<AccessProfile> {
  const response = await fetch("/api/auth/v1/session/me", {
    method: "GET",
    headers: {
      Authorization: `Bearer ${session.accessToken}`,
      "X-Tenant-Id": tenantSlug,
    },
  });

  const payload = await response.json().catch(() => null);
  const unwrapped = payload && typeof payload === "object" && "data" in (payload as Record<string, unknown>)
    ? (payload as { data?: unknown }).data
    : payload;

  if (!response.ok) {
    const message =
      typeof (unwrapped as { detail?: unknown })?.detail === "string"
        ? (unwrapped as { detail: string }).detail
        : typeof (unwrapped as { message?: unknown })?.message === "string"
          ? (unwrapped as { message: string }).message
          : `Session lookup failed (${response.status})`;
    throw new AccessProfileRequestError(message, response.status);
  }

  if (!unwrapped || typeof unwrapped !== "object") {
    throw new Error("Invalid session profile response");
  }

  return unwrapped as AccessProfile;
}

function accessProfileCacheEntryKey(userId: string, tenantSlug: string): string {
  return `${String(userId || "").trim().toLowerCase()}::${String(tenantSlug || "").trim().toLowerCase()}`;
}

function readAccessProfileCacheStore(): AccessProfileCacheStore {
  const store = readJson<AccessProfileCacheStore>(ACCESS_PROFILE_CACHE_KEY);
  if (!store || typeof store !== "object") {
    return {};
  }
  return store;
}

function writeAccessProfileCacheStore(store: AccessProfileCacheStore): void {
  writeJson(ACCESS_PROFILE_CACHE_KEY, store);
}

function pruneAccessProfileCacheStore(store: AccessProfileCacheStore, now: number): AccessProfileCacheStore {
  const next: AccessProfileCacheStore = {};
  for (const [key, entry] of Object.entries(store)) {
    const cachedAt = Number(entry?.cachedAt ?? NaN);
    if (!Number.isFinite(cachedAt) || cachedAt <= 0 || now - cachedAt > ACCESS_PROFILE_CACHE_TTL_MS) {
      continue;
    }
    if (!entry?.profile || typeof entry.profile !== "object") {
      continue;
    }
    next[key] = entry;
  }
  return next;
}

function readCachedAccessProfile(userId: string, tenantSlug: string): AccessProfile | null {
  const key = accessProfileCacheEntryKey(userId, tenantSlug);
  if (!key) {
    return null;
  }
  const now = Date.now();
  const store = pruneAccessProfileCacheStore(readAccessProfileCacheStore(), now);
  writeAccessProfileCacheStore(store);
  const entry = store[key];
  if (!entry || !entry.profile) {
    return null;
  }
  return entry.profile;
}

function writeCachedAccessProfile(userId: string, tenantSlug: string, profile: AccessProfile): void {
  const key = accessProfileCacheEntryKey(userId, tenantSlug);
  if (!key) {
    return;
  }
  const now = Date.now();
  const store = pruneAccessProfileCacheStore(readAccessProfileCacheStore(), now);
  store[key] = {
    cachedAt: now,
    profile,
  };
  writeAccessProfileCacheStore(store);
}

function removeCachedAccessProfile(userId: string, tenantSlug: string): void {
  const key = accessProfileCacheEntryKey(userId, tenantSlug);
  if (!key) {
    return;
  }
  const store = readAccessProfileCacheStore();
  if (!(key in store)) {
    return;
  }
  delete store[key];
  writeAccessProfileCacheStore(store);
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const provider = useMemo(() => createAuthProvider(), []);
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [session, setSession] = useState<SupabaseSession | null>(() => readJson<SupabaseSession>(SESSION_STORAGE_KEY));
  const [user, setUser] = useState<AuthUser | null>(() => readJson<AuthUser>(USER_STORAGE_KEY));
  const [tenantSlug, setTenantSlugState] = useState<string>(() => {
    const stored = readJson<string>(TENANT_STORAGE_KEY);
    if (typeof stored === "string" && stored.trim()) {
      return stored.trim().toLowerCase();
    }
    return getDefaultTenantSlug();
  });
  const [accessProfile, setAccessProfile] = useState<AccessProfile | null>(null);
  const [accessLoading, setAccessLoading] = useState(false);
  const [accessError, setAccessError] = useState<string | null>(null);

  const setTenantSlug = useCallback((value: string) => {
    const normalized = String(value || "").trim().toLowerCase();
    setTenantSlugState(normalized);
    writeJson(TENANT_STORAGE_KEY, normalized);
  }, []);

  const signOut = useCallback(async () => {
    await provider.signOut();
    setSession(null);
    setUser(null);
    setAccessProfile(null);
    setAccessLoading(false);
    setAccessError(null);
    setStatus("unauthenticated");
    removeStorage(SESSION_STORAGE_KEY);
    removeStorage(USER_STORAGE_KEY);
    removeStorage(ACCESS_PROFILE_CACHE_KEY);
  }, [provider]);

  const hydrateFromSession = useCallback(async (nextSession: SupabaseSession, nextUser?: AuthUser) => {
    const resolvedUser = nextUser ?? await provider.getUser(nextSession.accessToken);
    setUser(resolvedUser);
    setSession(nextSession);
    writeJson(USER_STORAGE_KEY, resolvedUser);
    writeJson(SESSION_STORAGE_KEY, nextSession);
    setStatus("authenticated");
  }, [provider]);

  const signInWithPassword = useCallback(async (email: string, password: string) => {
    const result = await provider.signInWithPassword(email, password);
    await hydrateFromSession(result.session, result.user);
  }, [provider, hydrateFromSession]);

  const signUpWithPassword = useCallback(async (email: string, password: string) => {
    const result: SignUpWithPasswordResult = await provider.signUpWithPassword(email, password);
    if (result.session) {
      await hydrateFromSession(result.session, result.user ?? undefined);
      return { status: "signed_in" as const };
    }

    setSession(null);
    setStatus("unauthenticated");
    removeStorage(SESSION_STORAGE_KEY);
    return { status: "confirm_email" as const };
  }, [provider, hydrateFromSession]);

  const getAccessToken = useCallback((): string | null => {
    return provider.getAccessToken(session);
  }, [provider, session]);

  const ensureFreshSession = useCallback(async (): Promise<SupabaseSession | null> => {
    if (!session?.accessToken) {
      return null;
    }

    const expiresAt = Number(session.expiresAt ?? NaN);
    if (!Number.isFinite(expiresAt) || expiresAt <= 0) {
      return session;
    }

    const now = Math.floor(Date.now() / 1000);
    if (now < expiresAt - 30) {
      return session;
    }

    try {
      const refreshed = await provider.refreshSession(session);
      if (!refreshed?.accessToken) {
        return session;
      }
      setSession(refreshed);
      writeJson(SESSION_STORAGE_KEY, refreshed);
      return refreshed;
    } catch {
      await signOut();
      return null;
    }
  }, [provider, session, signOut]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      if (!session?.accessToken) {
        setStatus("unauthenticated");
        return;
      }
      if (user) {
        setStatus("authenticated");
        return;
      }

      try {
        const resolvedUser = await provider.getUser(session.accessToken);
        if (cancelled) {
          return;
        }
        setUser(resolvedUser);
        writeJson(USER_STORAGE_KEY, resolvedUser);
        setStatus("authenticated");
      } catch {
        if (cancelled) {
          return;
        }
        await signOut();
      }
    }

    void run();
    return () => {
      cancelled = true;
    };
  }, [provider, session?.accessToken, signOut]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      if (status !== "authenticated" || !session?.accessToken) {
        setAccessProfile(null);
        setAccessLoading(false);
        setAccessError(null);
        return;
      }
      if (!tenantSlug) {
        setAccessProfile(null);
        setAccessLoading(false);
        setAccessError("Missing tenant selection");
        return;
      }

      const currentUserId = String(user?.id || "").trim();
      const cached = currentUserId ? readCachedAccessProfile(currentUserId, tenantSlug) : null;
      if (cached) {
        setAccessProfile(cached);
        setAccessLoading(false);
      } else {
        setAccessProfile(null);
        setAccessLoading(true);
      }

      try {
        const profile = await fetchAccessProfile(session, tenantSlug);
        if (cancelled) {
          return;
        }
        if (!user && profile.user) {
          setUser(profile.user);
          writeJson(USER_STORAGE_KEY, profile.user);
        }
        setAccessProfile(profile);
        setAccessLoading(false);
        setAccessError(null);
        writeCachedAccessProfile(profile.user.id, tenantSlug, profile);
      } catch (error) {
        if (cancelled) {
          return;
        }
        const message = error instanceof Error ? error.message : "Failed to load access profile";
        const statusCode = error instanceof AccessProfileRequestError ? error.status : 0;
        const shouldInvalidateCache = statusCode === 401 || statusCode === 403;

        if (shouldInvalidateCache && currentUserId) {
          removeCachedAccessProfile(currentUserId, tenantSlug);
          setAccessProfile(null);
        } else if (!cached) {
          setAccessProfile(null);
        }
        setAccessLoading(false);
        setAccessError(message);
      }
    }

    void run();
    return () => {
      cancelled = true;
    };
  }, [status, session, tenantSlug, user?.id]);

  const value = useMemo<AuthContextValue>(() => ({
    status,
    user,
    session,
    tenantSlug,
    accessProfile,
    accessLoading,
    accessError,
    providerName: provider.name,
    setTenantSlug,
    signInWithPassword,
    signUpWithPassword,
    signOut,
    ensureFreshSession,
    getAccessToken,
    signInPassword: signInWithPassword,
    logout: signOut,
  }), [
    status,
    user,
    session,
    tenantSlug,
    accessProfile,
    accessLoading,
    accessError,
    provider.name,
    setTenantSlug,
    signInWithPassword,
    signUpWithPassword,
    signOut,
    ensureFreshSession,
    getAccessToken,
  ]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuthContext(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuthContext must be used within AuthProvider");
  }
  return context;
}
