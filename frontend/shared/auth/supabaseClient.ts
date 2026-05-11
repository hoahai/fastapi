import type { AuthUser, SupabaseSession } from "./types";

const SUPABASE_URL = String(import.meta.env.VITE_SUPABASE_URL || "").replace(/\/$/, "");
const SUPABASE_ANON_KEY = String(import.meta.env.VITE_SUPABASE_ANON_KEY || "");

function assertConfigured(): void {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error("Supabase frontend env vars are not configured.");
  }
}

async function requestJson(path: string, init: RequestInit): Promise<unknown> {
  assertConfigured();
  const response = await fetch(`${SUPABASE_URL}${path}`, {
    ...init,
    headers: {
      apikey: SUPABASE_ANON_KEY,
      ...(init.headers ?? {}),
    },
  });

  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const message =
      typeof (payload as { msg?: unknown })?.msg === "string"
        ? (payload as { msg: string }).msg
        : typeof (payload as { error_description?: unknown })?.error_description === "string"
          ? (payload as { error_description: string }).error_description
          : `Supabase auth request failed (${response.status})`;
    throw new Error(message);
  }

  return payload;
}

function parseUser(payload: unknown): AuthUser {
  const record = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
  const id = String(record.id || "").trim();
  if (!id) {
    throw new Error("Supabase user id is missing.");
  }
  const emailRaw = String(record.email || "").trim();
  return {
    id,
    email: emailRaw || null,
  };
}

function parseSession(payload: unknown): SupabaseSession {
  const record = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
  const accessToken = String(record.access_token || "").trim();
  if (!accessToken) {
    throw new Error("Supabase access token is missing.");
  }

  const refreshRaw = String(record.refresh_token || "").trim();
  const expiresIn = Number(record.expires_in ?? NaN);
  const expiresAt = Number.isFinite(expiresIn) ? Math.floor(Date.now() / 1000 + expiresIn) : null;

  return {
    accessToken,
    refreshToken: refreshRaw || null,
    expiresAt,
  };
}

export async function signInWithPassword(email: string, password: string): Promise<{ session: SupabaseSession; user: AuthUser }> {
  const payload = await requestJson("/auth/v1/token?grant_type=password", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ email, password }),
  });

  const record = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
  return {
    session: parseSession(record),
    user: parseUser(record.user),
  };
}

export async function refreshSession(refreshToken: string): Promise<SupabaseSession> {
  const normalizedRefreshToken = String(refreshToken || "").trim();
  if (!normalizedRefreshToken) {
    throw new Error("Supabase refresh token is missing.");
  }

  const payload = await requestJson("/auth/v1/token?grant_type=refresh_token", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ refresh_token: normalizedRefreshToken }),
  });

  const record = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
  return parseSession(record);
}

export async function signUpWithPassword(
  email: string,
  password: string,
): Promise<{ session: SupabaseSession | null; user: AuthUser | null }> {
  const payload = await requestJson("/auth/v1/signup", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ email, password }),
  });

  const record = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
  const user = record.user && typeof record.user === "object" ? parseUser(record.user) : null;
  const accessToken = String(record.access_token || "").trim();
  const session = accessToken ? parseSession(record) : null;

  return { session, user };
}

export async function getUser(accessToken: string): Promise<AuthUser> {
  const payload = await requestJson("/auth/v1/user", {
    method: "GET",
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  });
  return parseUser(payload);
}
