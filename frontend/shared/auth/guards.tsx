import type { ReactNode } from "react";

import { useAuth } from "./useAuth";

export function shouldProtectTradsphereFrontend(): boolean {
  const value = String(import.meta.env.VITE_AUTH_PROTECT_TRADSPHERE || "false").trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes" || value === "on";
}

export function RequireAuth({ children, fallback }: { children: ReactNode; fallback: ReactNode }) {
  const auth = useAuth();
  if (!shouldProtectTradsphereFrontend()) {
    return <>{children}</>;
  }
  if (auth.status === "loading") {
    return <div className="p-6 text-sm text-slate-600">Loading session...</div>;
  }
  if (auth.status !== "authenticated" || !auth.user) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}

export function RequireTenantAccess({ children, fallback }: { children: ReactNode; fallback: ReactNode }) {
  const auth = useAuth();
  if (!shouldProtectTradsphereFrontend()) {
    return <>{children}</>;
  }
  if (auth.status !== "authenticated") {
    return <>{fallback}</>;
  }
  if (!auth.tenantSlug || !auth.accessProfile || auth.accessError) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}

export function RequirePermission({
  permission,
  children,
  fallback,
}: {
  permission: string;
  children: ReactNode;
  fallback: ReactNode;
}) {
  const auth = useAuth();
  if (!shouldProtectTradsphereFrontend()) {
    return <>{children}</>;
  }
  const permissions = new Set(auth.accessProfile?.permissions ?? []);
  if (!permissions.has(permission)) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}
