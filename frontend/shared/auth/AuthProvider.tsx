import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

import { createAuthProvider } from "./provider";
import type { SignUpWithPasswordResult } from "./provider/types";
import type { AccessProfile, AuthStatus, AuthUser, SupabaseSession } from "./types";

const SESSION_STORAGE_KEY = "workspace.auth.session.v1";
const USER_STORAGE_KEY = "workspace.auth.user.v1";
const TENANT_STORAGE_KEY = "workspace.auth.tenantSlug.v1";

type AuthContextValue = {
  status: AuthStatus;
  user: AuthUser | null;
  session: SupabaseSession | null;
  tenantSlug: string;
  accessProfile: AccessProfile | null;
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
    throw new Error(message);
  }

  if (!unwrapped || typeof unwrapped !== "object") {
    throw new Error("Invalid session profile response");
  }

  return unwrapped as AccessProfile;
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
    setAccessError(null);
    setStatus("unauthenticated");
    removeStorage(SESSION_STORAGE_KEY);
    removeStorage(USER_STORAGE_KEY);
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
        setAccessError(null);
        return;
      }
      if (!tenantSlug) {
        setAccessProfile(null);
        setAccessError("Missing tenant selection");
        return;
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
        setAccessError(null);
      } catch (error) {
        if (cancelled) {
          return;
        }
        setAccessProfile(null);
        setAccessError(error instanceof Error ? error.message : "Failed to load access profile");
      }
    }

    void run();
    return () => {
      cancelled = true;
    };
  }, [status, session, tenantSlug]);

  const value = useMemo<AuthContextValue>(() => ({
    status,
    user,
    session,
    tenantSlug,
    accessProfile,
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
