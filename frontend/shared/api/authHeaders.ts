import type { SupabaseSession } from "@shared/auth/types";

export function buildAuthHeaders(
  session: SupabaseSession | null,
  tenantSlug: string,
  includeJsonContentType: boolean,
): HeadersInit {
  const headers: Record<string, string> = {};
  if (session?.accessToken) {
    headers.Authorization = `Bearer ${session.accessToken}`;
  }
  const fallbackTenant = String(import.meta.env.VITE_DEFAULT_TENANT_SLUG || "").trim();
  const normalizedTenant = String(tenantSlug || fallbackTenant).trim();
  if (normalizedTenant) {
    headers["X-Tenant-Id"] = normalizedTenant;
  }
  if (!session?.accessToken) {
    const authMode = String(import.meta.env.VITE_AUTH_MODE || "compat").trim().toLowerCase();
    const legacyEnabled = String(import.meta.env.VITE_AUTH_ENABLE_LEGACY_API_KEY_FALLBACK || "true")
      .trim()
      .toLowerCase();
    const legacyApiKey = String(import.meta.env.VITE_LEGACY_API_KEY || "").trim();
    const legacyUserName = String(import.meta.env.VITE_LEGACY_USER_NAME || "").trim();
    if (authMode === "compat" && legacyEnabled !== "false" && legacyApiKey) {
      headers["X-API-Key"] = legacyApiKey;
      if (legacyUserName) {
        headers["X-User-Name"] = legacyUserName;
      }
    }
  }
  if (includeJsonContentType) {
    headers["Content-Type"] = "application/json";
  }
  return headers;
}
