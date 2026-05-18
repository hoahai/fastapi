import type { ReactNode } from "react";
import { PageLoadingOverlay } from "@shared/components/status/LoadingOverlay";

import { useAuth } from "./useAuth";

export function AuthLoadingFallback({ message = "Loading access..." }: { message?: string }) {
  return <PageLoadingOverlay message={message} />;
}

function isAccessResolutionPending(
  status: string,
  accessLoading: boolean,
  accessProfile: unknown,
  accessError: string | null,
): boolean {
  if (status !== "authenticated") {
    return false;
  }
  return !accessProfile && (accessLoading || !accessError);
}

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
    return <AuthLoadingFallback message="Loading session..." />;
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
  if (auth.status === "loading") {
    return <AuthLoadingFallback />;
  }
  if (auth.status !== "authenticated") {
    return <>{fallback}</>;
  }
  if (isAccessResolutionPending(auth.status, auth.accessLoading, auth.accessProfile, auth.accessError)) {
    return <AuthLoadingFallback />;
  }
  if (!auth.tenantSlug || !auth.accessProfile) {
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
  if (auth.status === "loading") {
    return <AuthLoadingFallback />;
  }
  if (auth.status !== "authenticated") {
    return <>{fallback}</>;
  }
  if (isAccessResolutionPending(auth.status, auth.accessLoading, auth.accessProfile, auth.accessError)) {
    return <AuthLoadingFallback />;
  }
  if (!auth.accessProfile) {
    return <>{fallback}</>;
  }
  const permissions = new Set(auth.accessProfile?.permissions ?? []);
  if (!permissions.has(permission)) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}
