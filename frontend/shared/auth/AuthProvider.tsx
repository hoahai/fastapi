import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

import {
  extractSessionFromCallbackUrl,
  getUser,
  sendMagicLink,
  signInWithPassword,
} from "./supabaseClient";
import type { AccessProfile, AuthStatus, AuthUser, SupabaseSession } from "./types";

const SESSION_STORAGE_KEY = "workspace.auth.session.v1";
const TENANT_STORAGE_KEY = "workspace.auth.tenantSlug.v1";

type AuthContextValue = {
  status: AuthStatus;
  user: AuthUser | null;
  session: SupabaseSession | null;
  tenantSlug: string;
  accessProfile: AccessProfile | null;
  accessError: string | null;
  setTenantSlug: (tenantSlug: string) => void;
  signInPassword: (email: string, password: string) => Promise<void>;
  signInMagicLink: (email: string) => Promise<void>;
  completeCallbackFromUrl: (url: string) => Promise<void>;
  logout: () => void;
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

  const profile = unwrapped as AccessProfile;
  return profile;
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [session, setSession] = useState<SupabaseSession | null>(() => readJson<SupabaseSession>(SESSION_STORAGE_KEY));
  const [user, setUser] = useState<AuthUser | null>(null);
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

  const logout = useCallback(() => {
    setSession(null);
    setUser(null);
    setAccessProfile(null);
    setAccessError(null);
    setStatus("unauthenticated");
    removeStorage(SESSION_STORAGE_KEY);
  }, []);

  const hydrateFromSession = useCallback(async (nextSession: SupabaseSession) => {
    const nextUser = await getUser(nextSession.accessToken);
    setUser(nextUser);
    setSession(nextSession);
    writeJson(SESSION_STORAGE_KEY, nextSession);
    setStatus("authenticated");
  }, []);

  const signInPassword = useCallback(async (email: string, password: string) => {
    const result = await signInWithPassword(email, password);
    await hydrateFromSession(result.session);
  }, [hydrateFromSession]);

  const signInMagicLink = useCallback(async (email: string) => {
    const redirectTo = `${window.location.origin}/auth/callback`;
    await sendMagicLink(email, redirectTo);
  }, []);

  const completeCallbackFromUrl = useCallback(async (url: string) => {
    const callbackSession = extractSessionFromCallbackUrl(url);
    if (!callbackSession) {
      throw new Error("Auth callback token is missing.");
    }
    await hydrateFromSession(callbackSession);
  }, [hydrateFromSession]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      if (!session?.accessToken) {
        setStatus("unauthenticated");
        return;
      }

      try {
        const resolvedUser = await getUser(session.accessToken);
        if (cancelled) {
          return;
        }
        setUser(resolvedUser);
        setStatus("authenticated");
      } catch {
        if (cancelled) {
          return;
        }
        logout();
      }
    }

    void run();
    return () => {
      cancelled = true;
    };
  }, [session?.accessToken, logout]);

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
    setTenantSlug,
    signInPassword,
    signInMagicLink,
    completeCallbackFromUrl,
    logout,
  }), [
    status,
    user,
    session,
    tenantSlug,
    accessProfile,
    accessError,
    setTenantSlug,
    signInPassword,
    signInMagicLink,
    completeCallbackFromUrl,
    logout,
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
