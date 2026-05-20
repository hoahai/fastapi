import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import { clearScopedPageStatesForUser } from "@shared/cache";
import { createAuthProvider } from "./provider";
import type { SignUpWithPasswordResult } from "./provider/types";
import type { AccessProfile, AuthStatus, AuthUser, SupabaseSession } from "./types";

const SESSION_STORAGE_KEY = "workspace.auth.session.v1";
const USER_STORAGE_KEY = "workspace.auth.user.v1";
const TENANT_STORAGE_KEY = "workspace.auth.tenantSlug.v1";
const ACCESS_PROFILE_CACHE_KEY = "workspace.auth.accessProfileCache.v1";
const AUTH_NOTICE_STORAGE_KEY = "workspace.auth.notice.v1";
const ACCESS_PROFILE_CACHE_TTL_MS = 30 * 60 * 1000;
const ACCESS_PROFILE_CACHE_STALE_RETAIN_MS = 7 * 24 * 60 * 60 * 1000;
const ACCESS_PROFILE_REVALIDATE_INTERVAL_MS = 60 * 60 * 1000;
const ACCESS_PROFILE_MANUAL_REFRESH_MIN_INTERVAL_MS = 15 * 1000;
const DISABLED_ACCOUNT_NOTICE = "Your account has been disabled. Contact your workspace administrator.";

class AccessProfileRequestError extends Error {
  status: number;
  code: string | null;

  constructor(message: string, status: number, code?: string | null) {
    super(message);
    this.name = "AccessProfileRequestError";
    this.status = status;
    const normalizedCode = String(code || "").trim().toLowerCase();
    this.code = normalizedCode || null;
  }
}

type AccessProfileCacheEntry = {
  cachedAt: number;
  profile: AccessProfile;
};

type AccessProfileCacheStore = Record<string, AccessProfileCacheEntry>;
type AccessProfileCacheStatus = { source: "cache" | "network"; fetchedAt: number };
type AccessProfileCacheReadResult = {
  entry: AccessProfileCacheEntry;
  isExpired: boolean;
};

type AuthContextValue = {
  status: AuthStatus;
  user: AuthUser | null;
  session: SupabaseSession | null;
  tenantSlug: string;
  authNotice: string | null;
  accessProfile: AccessProfile | null;
  initialAuthLoading: boolean;
  accessReadyFromCache: boolean;
  accessRefreshing: boolean;
  accessRefreshError: string | null;
  unauthorized: boolean;
  accessLoading: boolean;
  accessError: string | null;
  accessCacheStatus: AccessProfileCacheStatus | null;
  providerName: string;
  setTenantSlug: (tenantSlug: string) => void;
  clearAuthNotice: () => void;
  signInWithPassword: (email: string, password: string) => Promise<void>;
  signUpWithPassword: (email: string, password: string) => Promise<{ status: "signed_in" | "confirm_email" }>;
  updatePassword: (newPassword: string) => Promise<void>;
  sendPasswordResetEmail: (email: string, options?: { redirectTo?: string }) => Promise<void>;
  setSessionFromTokens: (tokens: { accessToken: string; refreshToken?: string | null; expiresInSeconds?: number | null }) => void;
  signOut: (options?: { notice?: string | null }) => Promise<void>;
  ensureFreshSession: () => Promise<SupabaseSession | null>;
  getAccessToken: () => string | null;
  refreshAccessProfile: (options?: { force?: boolean }) => void;
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

function getCurrentAppCodeFromLocation(): string {
  if (typeof window === "undefined") {
    return "workspace";
  }
  const pathname = String(window.location.pathname || "").trim().toLowerCase();
  if (!pathname) {
    return "workspace";
  }
  const segments = pathname.split("/").filter(Boolean);
  if (!segments.length) {
    return "workspace";
  }
  const first = segments[0];
  if (first === "tradsphere" || first === "shiftzy" || first === "spendsphere" || first === "fundsphere" || first === "opssphere") {
    return first;
  }
  return "workspace";
}

async function fetchAccessProfile(session: SupabaseSession, tenantSlug: string): Promise<AccessProfile> {
  const headers: Record<string, string> = {
    Authorization: `Bearer ${session.accessToken}`,
    "X-Tenant-Id": tenantSlug,
  };

  const response = await fetch("/api/auth/v1/session/me", {
    method: "GET",
    headers,
  });

  const payload = await response.json().catch(() => null);
  const envelope = payload && typeof payload === "object" ? (payload as Record<string, unknown>) : null;
  const unwrapped = envelope && "data" in envelope
    ? (payload as { data?: unknown }).data
    : payload;
  const errorPayload = envelope && "error" in envelope
    ? envelope.error
    : unwrapped;

  if (!response.ok) {
    let message = `Session lookup failed (${response.status})`;
    let code: string | null = null;

    if (errorPayload && typeof errorPayload === "object") {
      const errorRecord = errorPayload as Record<string, unknown>;
      const detail = errorRecord.detail;
      if (typeof errorRecord.message === "string" && errorRecord.message.trim()) {
        message = errorRecord.message.trim();
      } else if (typeof detail === "string" && detail.trim()) {
        message = detail.trim();
      } else if (detail && typeof detail === "object") {
        const detailRecord = detail as Record<string, unknown>;
        if (typeof detailRecord.message === "string" && detailRecord.message.trim()) {
          message = detailRecord.message.trim();
        }
      }

      if (typeof errorRecord.code === "string" && errorRecord.code.trim()) {
        code = errorRecord.code.trim().toLowerCase();
      } else if (detail && typeof detail === "object") {
        const detailRecord = detail as Record<string, unknown>;
        if (typeof detailRecord.code === "string" && detailRecord.code.trim()) {
          code = detailRecord.code.trim().toLowerCase();
        }
      }
    } else if (typeof errorPayload === "string" && errorPayload.trim()) {
      message = errorPayload.trim();
    }

    throw new AccessProfileRequestError(message, response.status, code);
  }

  if (!unwrapped || typeof unwrapped !== "object") {
    throw new Error("Invalid session profile response");
  }

  return unwrapped as AccessProfile;
}

function accessProfileCacheEntryKey(userId: string, tenantSlug: string, appCode: string): string {
  return `${String(userId || "").trim().toLowerCase()}::${String(tenantSlug || "").trim().toLowerCase()}::${String(appCode || "").trim().toLowerCase()}`;
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
    if (!Number.isFinite(cachedAt) || cachedAt <= 0 || now - cachedAt > ACCESS_PROFILE_CACHE_STALE_RETAIN_MS) {
      continue;
    }
    if (!entry?.profile || typeof entry.profile !== "object") {
      continue;
    }
    next[key] = entry;
  }
  return next;
}

function readCachedAccessProfileEntry(userId: string, tenantSlug: string, appCode: string): AccessProfileCacheReadResult | null {
  const key = accessProfileCacheEntryKey(userId, tenantSlug, appCode);
  if (!key) {
    return null;
  }
  const now = Date.now();
  const store = pruneAccessProfileCacheStore(readAccessProfileCacheStore(), now);
  writeAccessProfileCacheStore(store);
  const entry = store[key];
  if (!entry || !entry.profile || !Number.isFinite(Number(entry.cachedAt))) {
    return null;
  }
  return {
    entry,
    isExpired: now - entry.cachedAt > ACCESS_PROFILE_CACHE_TTL_MS,
  };
}

function writeCachedAccessProfile(userId: string, tenantSlug: string, appCode: string, profile: AccessProfile): void {
  const key = accessProfileCacheEntryKey(userId, tenantSlug, appCode);
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

function removeCachedAccessProfile(userId: string, tenantSlug: string, appCode: string): void {
  const key = accessProfileCacheEntryKey(userId, tenantSlug, appCode);
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

function isDisabledMembershipError(error: unknown): boolean {
  if (!(error instanceof AccessProfileRequestError)) {
    return false;
  }
  const code = String(error.code || "").trim().toLowerCase();
  if (code === "tenant_membership_disabled") {
    return true;
  }
  return String(error.message || "").toLowerCase().includes("account has been disabled");
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
  const [authNotice, setAuthNotice] = useState<string | null>(() => {
    const stored = readJson<string>(AUTH_NOTICE_STORAGE_KEY);
    if (typeof stored !== "string") {
      return null;
    }
    const normalized = stored.trim();
    return normalized || null;
  });
  const [accessLoading, setAccessLoading] = useState(false);
  const [accessRefreshing, setAccessRefreshing] = useState(false);
  const [accessError, setAccessError] = useState<string | null>(null);
  const [accessRefreshError, setAccessRefreshError] = useState<string | null>(null);
  const [accessCacheStatus, setAccessCacheStatus] = useState<AccessProfileCacheStatus | null>(null);
  const [accessRefreshVersion, setAccessRefreshVersion] = useState(0);
  const handledRefreshVersionRef = useRef(0);
  const lastManualRefreshAtRef = useRef(0);

  const setTenantSlug = useCallback((value: string) => {
    const normalized = String(value || "").trim().toLowerCase();
    setTenantSlugState(normalized);
    writeJson(TENANT_STORAGE_KEY, normalized);
  }, []);

  const clearAuthNotice = useCallback(() => {
    setAuthNotice(null);
    removeStorage(AUTH_NOTICE_STORAGE_KEY);
  }, []);

  const setSessionFromTokens = useCallback((tokens: { accessToken: string; refreshToken?: string | null; expiresInSeconds?: number | null }) => {
    const accessToken = String(tokens.accessToken || "").trim();
    if (!accessToken) {
      return;
    }
    const expiresInSeconds = Number(tokens.expiresInSeconds ?? NaN);
    const expiresAt = Number.isFinite(expiresInSeconds) ? Math.floor(Date.now() / 1000 + expiresInSeconds) : null;
    const nextSession: SupabaseSession = {
      accessToken,
      refreshToken: String(tokens.refreshToken || "").trim() || null,
      expiresAt,
    };
    setSession(nextSession);
    writeJson(SESSION_STORAGE_KEY, nextSession);
    setStatus("authenticated");
    clearAuthNotice();
    setAccessError(null);
    setAccessRefreshError(null);
  }, [clearAuthNotice]);

  const signOut = useCallback(async (options?: { notice?: string | null }) => {
    const notice = String(options?.notice || "").trim();
    const currentUserStateKey = String(user?.id || user?.email || "").trim().toLowerCase();
    await provider.signOut();
    setSession(null);
    setUser(null);
    setAccessProfile(null);
    setAccessLoading(false);
    setAccessRefreshing(false);
    setAccessError(null);
    setAccessRefreshError(null);
    setAccessCacheStatus(null);
    setStatus("unauthenticated");
    if (notice) {
      setAuthNotice(notice);
      writeJson(AUTH_NOTICE_STORAGE_KEY, notice);
    } else {
      clearAuthNotice();
    }
    removeStorage(SESSION_STORAGE_KEY);
    removeStorage(USER_STORAGE_KEY);
    removeStorage(ACCESS_PROFILE_CACHE_KEY);
    if (currentUserStateKey) {
      clearScopedPageStatesForUser(currentUserStateKey);
    }
  }, [clearAuthNotice, provider, user?.email, user?.id]);

  const hydrateFromSession = useCallback(async (nextSession: SupabaseSession, nextUser?: AuthUser) => {
    const resolvedUser = nextUser ?? await provider.getUser(nextSession.accessToken);
    setUser(resolvedUser);
    setSession(nextSession);
    writeJson(USER_STORAGE_KEY, resolvedUser);
    writeJson(SESSION_STORAGE_KEY, nextSession);
    setStatus("authenticated");
  }, [provider]);

  const signInWithPassword = useCallback(async (email: string, password: string) => {
    clearAuthNotice();
    const result = await provider.signInWithPassword(email, password);
    await hydrateFromSession(result.session, result.user);
  }, [clearAuthNotice, provider, hydrateFromSession]);

  const signUpWithPassword = useCallback(async (email: string, password: string) => {
    clearAuthNotice();
    const result: SignUpWithPasswordResult = await provider.signUpWithPassword(email, password);
    if (result.session) {
      await hydrateFromSession(result.session, result.user ?? undefined);
      return { status: "signed_in" as const };
    }

    setSession(null);
    setStatus("unauthenticated");
    removeStorage(SESSION_STORAGE_KEY);
    return { status: "confirm_email" as const };
  }, [clearAuthNotice, provider, hydrateFromSession]);

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

  const refreshAccessProfile = useCallback((options?: { force?: boolean }) => {
    const force = Boolean(options?.force);
    const now = Date.now();
    if (!force && now - lastManualRefreshAtRef.current < ACCESS_PROFILE_MANUAL_REFRESH_MIN_INTERVAL_MS) {
      return;
    }
    lastManualRefreshAtRef.current = now;
    setAccessRefreshVersion((value) => value + 1);
  }, []);

  const updatePassword = useCallback(async (newPassword: string) => {
    const normalizedPassword = String(newPassword || "");
    if (!normalizedPassword) {
      throw new Error("Password is required.");
    }
    const ensuredSession = await ensureFreshSession();
    const accessToken = provider.getAccessToken(ensuredSession);
    if (!accessToken) {
      throw new Error("Your session has expired. Please sign in again.");
    }
    await provider.updatePassword(accessToken, normalizedPassword);
  }, [ensureFreshSession, provider]);

  const sendPasswordResetEmail = useCallback(async (email: string, options?: { redirectTo?: string }) => {
    await provider.sendPasswordResetEmail(email, options);
  }, [provider]);

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
        const activeSession = await ensureFreshSession();
        if (cancelled) {
          return;
        }
        if (!activeSession?.accessToken) {
          setStatus("unauthenticated");
          return;
        }
        const resolvedUser = await provider.getUser(activeSession.accessToken);
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
  }, [provider, session?.accessToken, signOut, ensureFreshSession, user]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      if (status !== "authenticated" || !session?.accessToken) {
        setAccessProfile(null);
        setAccessLoading(false);
        setAccessRefreshing(false);
        setAccessError(null);
        setAccessRefreshError(null);
        setAccessCacheStatus(null);
        return;
      }
      if (!tenantSlug) {
        setAccessProfile(null);
        setAccessLoading(false);
        setAccessRefreshing(false);
        setAccessError("Missing tenant selection");
        setAccessRefreshError(null);
        setAccessCacheStatus(null);
        return;
      }

      const isManualRefresh = accessRefreshVersion > handledRefreshVersionRef.current;
      if (isManualRefresh) {
        handledRefreshVersionRef.current = accessRefreshVersion;
      }
      const currentUserId = String(user?.id || "").trim();
      const currentAppCode = getCurrentAppCodeFromLocation();
      const cachedEntry = currentUserId ? readCachedAccessProfileEntry(currentUserId, tenantSlug, currentAppCode) : null;
      const hasCachedAccess = Boolean(cachedEntry?.entry?.profile);
      const shouldWarmRefresh = Boolean(cachedEntry && !isManualRefresh && !accessProfile);
      const shouldRefreshFromNetwork = isManualRefresh || !cachedEntry || cachedEntry.isExpired || shouldWarmRefresh;

      if (cachedEntry && !isManualRefresh) {
        setAccessProfile(cachedEntry.entry.profile);
        setAccessCacheStatus({
          source: "cache",
          fetchedAt: cachedEntry.entry.cachedAt,
        });
        setAccessError(null);
      }

      if (hasCachedAccess) {
        setAccessLoading(false);
        setAccessRefreshing(shouldRefreshFromNetwork);
        setAccessRefreshError(null);
      } else {
        setAccessLoading(true);
        setAccessRefreshing(false);
      }

      if (!shouldRefreshFromNetwork) {
        setAccessLoading(false);
        setAccessRefreshing(false);
        return;
      }

      const activeSession = await ensureFreshSession();
      if (cancelled) {
        return;
      }
      if (!activeSession?.accessToken) {
        setAccessProfile(null);
        setAccessLoading(false);
        setAccessRefreshing(false);
        setAccessError("Your session has expired. Please sign in again.");
        setAccessRefreshError(null);
        setAccessCacheStatus(null);
        return;
      }

      try {
        const profile = await fetchAccessProfile(activeSession, tenantSlug);
        if (cancelled) {
          return;
        }
        if (!user && profile.user) {
          setUser(profile.user);
          writeJson(USER_STORAGE_KEY, profile.user);
        }
        setAccessProfile(profile);
        setAccessLoading(false);
        setAccessRefreshing(false);
        setAccessError(null);
        setAccessRefreshError(null);
        setAccessCacheStatus({
          source: "network",
          fetchedAt: Date.now(),
        });
        writeCachedAccessProfile(profile.user.id, tenantSlug, currentAppCode, profile);
      } catch (error) {
        const statusCode = error instanceof AccessProfileRequestError ? error.status : 0;
        if (statusCode === 401 && activeSession.refreshToken) {
          try {
            const refreshed = await provider.refreshSession(activeSession);
            if (cancelled) {
              return;
            }
            if (refreshed?.accessToken) {
              setSession(refreshed);
              writeJson(SESSION_STORAGE_KEY, refreshed);
              const retriedProfile = await fetchAccessProfile(refreshed, tenantSlug);
              if (cancelled) {
                return;
              }
              if (!user && retriedProfile.user) {
                setUser(retriedProfile.user);
                writeJson(USER_STORAGE_KEY, retriedProfile.user);
              }
              setAccessProfile(retriedProfile);
              setAccessLoading(false);
              setAccessRefreshing(false);
              setAccessError(null);
              setAccessRefreshError(null);
              setAccessCacheStatus({
                source: "network",
                fetchedAt: Date.now(),
              });
              writeCachedAccessProfile(retriedProfile.user.id, tenantSlug, currentAppCode, retriedProfile);
              return;
            }
          } catch (refreshError) {
            if (cancelled) {
              return;
            }
            if (isDisabledMembershipError(refreshError)) {
              await signOut({ notice: DISABLED_ACCOUNT_NOTICE });
              return;
            }
            // Fall through to normal unauthorized handling.
          }
        }

        if (cancelled) {
          return;
        }
        if (isDisabledMembershipError(error)) {
          await signOut({ notice: DISABLED_ACCOUNT_NOTICE });
          return;
        }
        if (statusCode === 401) {
          await signOut();
          return;
        }
        const message = error instanceof Error ? error.message : "Failed to load access profile";
        const shouldInvalidateCache = statusCode === 401 || statusCode === 403;

        if (shouldInvalidateCache && currentUserId) {
          removeCachedAccessProfile(currentUserId, tenantSlug, currentAppCode);
          setAccessProfile(null);
          setAccessCacheStatus(null);
          setAccessError(message);
          setAccessRefreshError(null);
        } else if (!cachedEntry) {
          setAccessProfile(null);
          setAccessCacheStatus(null);
          setAccessError(message);
          setAccessRefreshError(null);
        } else {
          setAccessCacheStatus({
            source: "cache",
            fetchedAt: cachedEntry.entry.cachedAt,
          });
          setAccessError(null);
          setAccessRefreshError(message);
        }
        setAccessLoading(false);
        setAccessRefreshing(false);
      }
    }

    void run();
    return () => {
      cancelled = true;
    };
  }, [status, session, tenantSlug, user, ensureFreshSession, provider, accessRefreshVersion]);

  useEffect(() => {
    if (status !== "authenticated" || !session?.accessToken) {
      return undefined;
    }

    const intervalId = window.setInterval(() => {
      refreshAccessProfile();
    }, ACCESS_PROFILE_REVALIDATE_INTERVAL_MS);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [refreshAccessProfile, session?.accessToken, status]);

  const initialAuthLoading = status === "loading" && !session?.accessToken && !user;
  const accessReadyFromCache = Boolean(accessProfile && accessCacheStatus?.source === "cache");
  const unauthorized = status === "unauthenticated"
    || (status === "authenticated" && !accessLoading && !accessRefreshing && !accessProfile && Boolean(accessError));

  const value = useMemo<AuthContextValue>(() => ({
    status,
    user,
    session,
    tenantSlug,
    authNotice,
    accessProfile,
    initialAuthLoading,
    accessReadyFromCache,
    accessRefreshing,
    accessRefreshError,
    unauthorized,
    accessLoading,
    accessError,
    accessCacheStatus,
    providerName: provider.name,
    setTenantSlug,
    clearAuthNotice,
    signInWithPassword,
    signUpWithPassword,
    updatePassword,
    sendPasswordResetEmail,
    setSessionFromTokens,
    signOut,
    ensureFreshSession,
    getAccessToken,
    refreshAccessProfile,
    signInPassword: signInWithPassword,
    logout: signOut,
  }), [
    status,
    user,
    session,
    tenantSlug,
    authNotice,
    accessProfile,
    initialAuthLoading,
    accessReadyFromCache,
    accessRefreshing,
    accessRefreshError,
    unauthorized,
    accessLoading,
    accessError,
    accessCacheStatus,
    provider.name,
    setTenantSlug,
    clearAuthNotice,
    signInWithPassword,
    signUpWithPassword,
    updatePassword,
    sendPasswordResetEmail,
    setSessionFromTokens,
    signOut,
    ensureFreshSession,
    getAccessToken,
    refreshAccessProfile,
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
