import { useEffect, useMemo, useState, type ReactNode } from "react";

import TradsphereHomePage from "@/TradsphereHomePage";
import EstimateNumbersPage from "@/pages/EstimateNumbersPage";
import ContactsPage from "@/pages/ContactsPage";
import StationsPage from "@/pages/StationsPage";
import AdminUsersPage from "@/pages/AdminUsersPage";
import { WorkspaceNotFoundPage } from "@home/WorkspaceNotFoundPage";
import { WorkspacePortalPage } from "@home/WorkspacePortalPage";
import { AppShell } from "@/components/layout/AppShell";
import { HOME_ROUTE } from "@/components/layout/navigation";
import { ToastProvider } from "@/components/ui/toast";
import { useRouteScrollRestoration } from "@/hooks/useRouteScrollRestoration";
import { AuthProvider } from "@shared/auth/AuthProvider";
import { RequirePermission, RequireTenantAccess } from "@shared/auth/guards";
import { AuthCallbackPage, InviteAcceptPage, LoginPage, PendingInvitePage, UnauthorizedPage } from "@shared/auth/pages";
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
  if (route === "/admin/users") {
    return "workspace.admin.users.scrollY";
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
    return <div className="p-6 text-sm text-slate-600">Checking session...</div>;
  }

  if (auth.status !== "authenticated" || !auth.user) {
    return <RedirectToLogin />;
  }

  return <>{children}</>;
}

function App() {
  const [frontendPath, setFrontendPath] = useState(() => getFrontendPath(window.location.pathname));
  useRouteScrollRestoration(toScrollStorageKey(frontendPath));

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
      "/auth/unauthorized",
      "/auth/invite/pending",
      "/admin/users",
      "/tradsphere/home",
      "/tradsphere/estnums",
      "/tradsphere/contacts",
      "/tradsphere/stations",
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

  function renderAdminRoute() {
    return (
      <RequireSignedIn>
        <RequireTenantAccess fallback={<UnauthorizedPage />}>
          <RequirePermission permission="tradsphere.admin" fallback={<UnauthorizedPage />}>
            <AdminUsersPage />
          </RequirePermission>
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
          {frontendPath === "/auth/unauthorized" ? <UnauthorizedPage /> : null}
          {frontendPath === "/auth/invite/pending" ? <PendingInvitePage /> : null}
          {inviteToken ? <InviteAcceptPage token={inviteToken} /> : null}
          {frontendPath === "/admin/users" ? renderAdminRoute() : null}
          {frontendPath.startsWith("/tradsphere/") ? renderTradsphereRoute() : null}
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
