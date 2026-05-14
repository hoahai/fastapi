import { useEffect, useMemo, useState, type ReactNode } from "react";

import TradsphereHomePage from "@/TradsphereHomePage";
import EstimateNumbersPage from "@/pages/EstimateNumbersPage";
import ContactsPage from "@/pages/ContactsPage";
import StationsPage from "@/pages/StationsPage";
import ShiftzySchedulePage from "@shiftzy/ShiftzySchedulePage";
import AdminUsersPage from "@/pages/AdminUsersPage";
import ProfilePage from "@/pages/ProfilePage";
import { WorkspaceNotFoundPage } from "@home/WorkspaceNotFoundPage";
import { WorkspacePortalPage } from "@home/WorkspacePortalPage";
import { AppShell } from "@/components/layout/AppShell";
import { APP_NAV_ITEMS, HOME_ROUTE } from "@/components/layout/navigation";
import { ToastProvider } from "@/components/ui/toast";
import { useRouteScrollRestoration } from "@/hooks/useRouteScrollRestoration";
import { AuthProvider } from "@shared/auth/AuthProvider";
import { AuthLoadingFallback, RequirePermission, RequireTenantAccess, shouldProtectTradsphereFrontend } from "@shared/auth/guards";
import { hasAnyAdminScope, hasAppViewAccess } from "@shared/auth/permissions";
import { AuthCallbackPage, InviteAcceptPage, LoginPage, PendingInvitePage, UnauthorizedPage, UpdatePasswordPage } from "@shared/auth/pages";
import { useAuth } from "@shared/auth/useAuth";

function getFrontendPath(pathname: string): string {
  const normalizedPath = pathname || "/";
  if (normalizedPath === "/fe" || normalizedPath === "/fe/") {
    return "/";
  }

  if (normalizedPath.startsWith("/fe/")) {
    const route = normalizedPath.slice(3);
    if (!route || route === "/") {
      return "/";
    }
    return stripTrailingSlash(route);
  }

  return stripTrailingSlash(normalizedPath);
}

function stripTrailingSlash(path: string): string {
  if (path.length > 1 && path.endsWith("/")) {
    return path.slice(0, -1);
  }
  return path || "/";
}

function toFrontendHref(route: string): string {
  return route;
}

function shouldRouteToUpdatePasswordFromHash(hash: string): boolean {
  const normalizedHash = String(hash || "").replace(/^#/, "");
  if (!normalizedHash) {
    return false;
  }
  const params = new URLSearchParams(normalizedHash);
  const flowType = String(params.get("type") || "").trim().toLowerCase();
  const hasAccessToken = String(params.get("access_token") || "").trim().length > 0;
  if (flowType === "recovery" && hasAccessToken) {
    return true;
  }

  const errorCode = String(params.get("error_code") || "").trim().toLowerCase();
  const hasSupabaseMarker = params.has("sb");
  if (!errorCode || !hasSupabaseMarker) {
    return false;
  }
  return errorCode === "otp_expired" || errorCode === "access_denied";
}

function toScrollStorageKey(route: string): string {
  if (route === HOME_ROUTE) {
    return "workspace.portal.scrollY";
  }
  if (route === "/tradsphere/home") {
    return "tradsphere.home.scrollY";
  }
  if (route === "/tradsphere/estnums") {
    return "tradsphere.estnums.scrollY";
  }
  if (route === "/tradsphere/contacts") {
    return "tradsphere.contacts.scrollY";
  }
  if (route === "/tradsphere/stations") {
    return "tradsphere.stations.scrollY";
  }
  if (route === "/shiftzy/home") {
    return "shiftzy.home.scrollY";
  }
  if (route === "/admin/users") {
    return "workspace.admin.users.scrollY";
  }
  if (route === "/profile") {
    return "workspace.profile.scrollY";
  }
  const normalized = route.replace(/[^a-zA-Z0-9]+/g, ".").replace(/^\.+|\.+$/g, "").toLowerCase();
  return `${normalized || "workspace"}.scrollY`;
}

function RedirectToLogin() {
  useEffect(() => {
    if (window.location.pathname === "/auth/login") {
      return;
    }
    window.location.replace("/auth/login");
  }, []);

  return <div className="p-6 text-sm text-slate-600">Redirecting to login...</div>;
}

function RequireSignedIn({ children }: { children: ReactNode }) {
  const auth = useAuth();

  if (auth.status === "loading") {
    return <AuthLoadingFallback message="Checking session..." />;
  }

  if (auth.status !== "authenticated" || !auth.user) {
    return <RedirectToLogin />;
  }

  return <>{children}</>;
}

function RequireAnyPermission({
  permissions,
  children,
  fallback,
}: {
  permissions: string[];
  children: ReactNode;
  fallback: ReactNode;
}) {
  const auth = useAuth();
  const granted = new Set(auth.accessProfile?.permissions ?? []);
  const allowed = permissions.some((permission) => granted.has(permission));
  if (!allowed) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}

function RequireAppView({
  appCode,
  children,
  fallback,
}: {
  appCode: string;
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
  if (!hasAppViewAccess(auth.accessProfile, appCode)) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}

function RequireAdminScope({ children, fallback }: { children: ReactNode; fallback: ReactNode }) {
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
  const appCodes = APP_NAV_ITEMS.map((item) => item.id);
  if (!hasAnyAdminScope(auth.accessProfile, appCodes)) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}

function App() {
  const [frontendPath, setFrontendPath] = useState(() => getFrontendPath(window.location.pathname));
  useRouteScrollRestoration(toScrollStorageKey(frontendPath));

  useEffect(() => {
    if (frontendPath === "/auth/update-password") {
      return;
    }
    if (!shouldRouteToUpdatePasswordFromHash(window.location.hash || "")) {
      return;
    }
    const targetPath = "/auth/update-password";
    const nextUrl = `${targetPath}${window.location.search || ""}${window.location.hash || ""}`;
    window.history.replaceState({}, "", nextUrl);
    setFrontendPath(targetPath);
  }, [frontendPath]);

  useEffect(() => {
    const handlePopState = () => {
      setFrontendPath(getFrontendPath(window.location.pathname));
    };

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  useEffect(() => {
    const canonicalPath = toFrontendHref(frontendPath);
    if (window.location.pathname === canonicalPath) {
      return;
    }

    window.history.replaceState({}, "", canonicalPath);
  }, [frontendPath]);

  const knownRoutes = useMemo(() => {
    return new Set<string>([
      HOME_ROUTE,
      "/auth/login",
      "/auth/callback",
      "/auth/update-password",
      "/auth/unauthorized",
      "/auth/invite/pending",
      "/profile",
      "/admin/users",
      "/tradsphere/home",
      "/tradsphere/estnums",
      "/tradsphere/contacts",
      "/tradsphere/stations",
      "/shiftzy/home",
    ]);
  }, []);

  function navigate(route: string) {
    const href = toFrontendHref(route);
    if (window.location.pathname === href || frontendPath === route) {
      return;
    }

    window.history.pushState({}, "", href);
    setFrontendPath(route);
  }

  const inviteToken = useMemo(() => {
    if (!frontendPath.startsWith("/auth/invite/")) {
      return null;
    }
    const token = frontendPath.replace("/auth/invite/", "").trim();
    return token || null;
  }, [frontendPath]);

  function renderTradsphereRoute() {
    return (
      <RequireSignedIn>
        <RequireTenantAccess fallback={<UnauthorizedPage />}>
          <RequirePermission permission="tradsphere.viewer" fallback={<UnauthorizedPage />}>
            {frontendPath === "/tradsphere/home" ? <TradsphereHomePage /> : null}
            {frontendPath === "/tradsphere/estnums" ? <EstimateNumbersPage /> : null}
            {frontendPath === "/tradsphere/contacts" ? <ContactsPage /> : null}
            {frontendPath === "/tradsphere/stations" ? <StationsPage /> : null}
          </RequirePermission>
        </RequireTenantAccess>
      </RequireSignedIn>
    );
  }

  function renderShiftzyRoute() {
    return (
      <RequireSignedIn>
        <RequireTenantAccess fallback={<UnauthorizedPage />}>
          <RequireAppView appCode="shiftzy" fallback={<UnauthorizedPage />}>
            {frontendPath === "/shiftzy/home" ? <ShiftzySchedulePage /> : null}
          </RequireAppView>
        </RequireTenantAccess>
      </RequireSignedIn>
    );
  }

  function renderProfileRoute() {
    return (
      <RequireSignedIn>
        <RequireTenantAccess fallback={<UnauthorizedPage />}>
          <RequireAnyPermission permissions={["workspace.super_admin", "tradsphere.viewer"]} fallback={<UnauthorizedPage />}>
            <ProfilePage />
          </RequireAnyPermission>
        </RequireTenantAccess>
      </RequireSignedIn>
    );
  }

  function renderAdminRoute() {
    return (
      <RequireSignedIn>
        <RequireTenantAccess fallback={<UnauthorizedPage />}>
          <RequireAdminScope fallback={<UnauthorizedPage />}>
            <AdminUsersPage />
          </RequireAdminScope>
        </RequireTenantAccess>
      </RequireSignedIn>
    );
  }

  return (
    <AuthProvider>
      <ToastProvider>
        <AppShell currentPath={frontendPath} onNavigate={navigate}>
          {frontendPath === "/auth/login" ? <LoginPage /> : null}
          {frontendPath === "/auth/callback" ? <AuthCallbackPage /> : null}
          {frontendPath === "/auth/update-password" ? <UpdatePasswordPage /> : null}
          {frontendPath === "/auth/unauthorized" ? <UnauthorizedPage /> : null}
          {frontendPath === "/auth/invite/pending" ? <PendingInvitePage /> : null}
          {inviteToken ? <InviteAcceptPage token={inviteToken} /> : null}
          {frontendPath === "/profile" ? renderProfileRoute() : null}
          {frontendPath === "/admin/users" ? renderAdminRoute() : null}
          {frontendPath.startsWith("/tradsphere/") ? renderTradsphereRoute() : null}
          {frontendPath.startsWith("/shiftzy/") ? renderShiftzyRoute() : null}
          {frontendPath === HOME_ROUTE ? (
            <RequireSignedIn>
              <WorkspacePortalPage onNavigate={navigate} />
            </RequireSignedIn>
          ) : null}
          {!knownRoutes.has(frontendPath) && !inviteToken ? <WorkspaceNotFoundPage onNavigate={navigate} /> : null}
        </AppShell>
      </ToastProvider>
    </AuthProvider>
  );
}

export default App;
