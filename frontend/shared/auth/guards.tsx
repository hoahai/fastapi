import type { ReactNode } from "react";
import { Loader2 } from "lucide-react";

import { useAuth } from "./useAuth";

export function AuthLoadingFallback({ message = "Loading access..." }: { message?: string }) {
  return (
    <div className="fixed inset-0 z-30 flex items-center justify-center bg-slate-950/25 backdrop-blur-[1.5px]">
      <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft">
        <Loader2 className="size-4 animate-spin text-blue-600" />
        <span>{message}</span>
      </div>
    </div>
  );
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
  if (auth.accessLoading && !auth.accessProfile) {
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
  if (auth.accessLoading && !auth.accessProfile) {
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
