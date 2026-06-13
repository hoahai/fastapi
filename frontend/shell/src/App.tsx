import { Suspense, lazy, useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import { AppShell } from "@shell/components/layout/AppShell";
import { APP_NAV_ITEMS, HOME_ROUTE } from "@shell/components/layout/navigation";
import { ToastProvider } from "@shell/components/ui/toast";
import { useRouteScrollRestoration } from "@shell/hooks/useRouteScrollRestoration";
import { AuthProvider } from "@shared/auth/AuthProvider";
import { AuthLoadingFallback, RequirePermission, RequireTenantAccess, shouldProtectFrontendAuth } from "@shared/auth/guards";
import { canAccessAppRoute } from "@shared/auth/pagePermissions";
import { hasAppAdminAccess, hasAppViewAccess, hasSuperAdminAccess } from "@shared/auth/permissions";
import { AuthCallbackPage, InviteAcceptPage, LoginPage, PendingInvitePage, UnauthorizedPage, UpdatePasswordPage } from "@shared/auth/pages";
import { useAuth } from "@shared/auth/useAuth";

const AccountsPage = lazy(() => import("@tradsphere/pages/AccountsPage"));
const EstimateNumbersPage = lazy(() => import("@tradsphere/pages/EstimateNumbersPage"));
const ContactsPage = lazy(() => import("@tradsphere/pages/ContactsPage"));
const StationsPage = lazy(() => import("@tradsphere/pages/StationsPage"));
const InvoiceChecklistPage = lazy(() => import("@tradsphere/pages/InvoiceChecklistPage"));
const TrafficPage = lazy(() => import("@tradsphere/pages/TrafficPage"));
const ShiftzyEmployeesPage = lazy(() => import("@shiftzy/pages/ShiftzyEmployeesPage"));
const ShiftzySchedulePage = lazy(() => import("@shiftzy/pages/ShiftzySchedulePage"));
const LeaveSphereMyPtoPage = lazy(() => import("@leavesphere/pages/MyPtoPage"));
const LeaveSphereLeaveManagementPage = lazy(() => import("@leavesphere/pages/LeaveManagementPage"));
const LeaveSphereQuickApprovalPage = lazy(() => import("@leavesphere/pages/QuickApprovalPage"));
const AdminUsersPage = lazy(() => import("@shell/pages/AdminUsersPage"));
const AppScopedAdminPage = lazy(() => import("@shell/pages/AppScopedAdminPage"));
const ProfilePage = lazy(() => import("@shell/pages/ProfilePage"));
const WorkspaceNotFoundPage = lazy(() =>
  import("@home/pages/WorkspaceNotFoundPage").then((module) => ({ default: module.WorkspaceNotFoundPage })),
);
const WorkspacePortalPage = lazy(() =>
  import("@home/pages/WorkspacePortalPage").then((module) => ({ default: module.WorkspacePortalPage })),
);

function getFrontendPath(pathname: string): string {
  const normalizedPath = pathname || "/";
  if (normalizedPath === "/tradsphere/users-access" || normalizedPath === "/tradsphere/users-access/") {
    return "/tradsphere/admin";
  }
  if (normalizedPath === "/fe" || normalizedPath === "/fe/") {
    return "/";
  }
  if (normalizedPath === "/leavesphere/admin-pto" || normalizedPath === "/leavesphere/admin-pto/") {
    return "/leavesphere/leave-management";
  }

  if (normalizedPath.startsWith("/fe/")) {
    const route = normalizedPath.slice(3);
    if (!route || route === "/") {
      return "/";
    }
    return normalizeLegacyLeaveSphereRoute(stripTrailingSlash(route));
  }

  return normalizeLegacyLeaveSphereRoute(stripTrailingSlash(normalizedPath));
}

function stripTrailingSlash(path: string): string {
  if (path.length > 1 && path.endsWith("/")) {
    return path.slice(0, -1);
  }
  return path || "/";
}

function normalizeLegacyLeaveSphereRoute(route: string): string {
  const normalized = stripTrailingSlash(route);
  if (normalized === "/leavesphere/admin-pto") {
    return "/leavesphere/leave-management";
  }
  return normalized;
}

function parseScopedAdminRoute(path: string): string | null {
  const match = String(path || "").match(/^\/([a-z0-9-_]+)\/admin$/);
  if (!match) {
    return null;
  }
  return String(match[1] || "").trim().toLowerCase() || null;
}

function parseLeaveSphereQuickApprovalToken(path: string): string | null {
  const match = String(path || "").match(/^\/leavesphere\/quick-approval\/([^/]+)$/);
  if (!match) {
    return null;
  }
  const encodedToken = String(match[1] || "").trim();
  if (!encodedToken) {
    return null;
  }
  try {
    return decodeURIComponent(encodedToken);
  } catch {
    return null;
  }
}

function formatAppLabel(appCode: string): string {
  const normalized = String(appCode || "").trim().toLowerCase();
  if (!normalized) {
    return "App";
  }
  const fromNav = APP_NAV_ITEMS.find((item) => String(item.id || "").trim().toLowerCase() === normalized);
  if (fromNav?.label) {
    return fromNav.label;
  }
  return normalized
    .split(/[-_]+/g)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

const BROWSER_TITLE_SUFFIX = "TheSphereWorks";

function formatBrowserTitle(pageTitle: string, appTitle?: string | null): string {
  const normalized = String(pageTitle || "").trim();
  if (!normalized) {
    return BROWSER_TITLE_SUFFIX;
  }
  const normalizedAppTitle = String(appTitle || "").trim();
  if (normalizedAppTitle) {
    return `${normalized} | ${normalizedAppTitle} | ${BROWSER_TITLE_SUFFIX}`;
  }
  return `${normalized} | ${BROWSER_TITLE_SUFFIX}`;
}

function resolveAppRouteTitle(route: string): { pageTitle: string; appTitle: string } | null {
  const normalizedRoute = stripTrailingSlash(route);
  const scopedAdminMatch = normalizedRoute.match(/^\/([a-z0-9-_]+)\/admin$/);
  if (scopedAdminMatch) {
    const appTitle = formatAppLabel(scopedAdminMatch[1] || "");
    return {
      pageTitle: "Admin",
      appTitle,
    };
  }

  for (const app of APP_NAV_ITEMS) {
    const child = app.children?.find((item) => item.available && item.route === normalizedRoute);
    if (!child) {
      continue;
    }
    return {
      pageTitle: child.label,
      appTitle: app.label,
    };
  }

  return null;
}

function resolveBrowserTitle(params: {
  frontendPath: string;
  inviteToken: string | null;
  leaveSphereQuickApprovalToken: string | null;
  scopedAdminAppCode: string | null;
}): string {
  const { frontendPath, inviteToken, leaveSphereQuickApprovalToken, scopedAdminAppCode } = params;

  if (frontendPath === HOME_ROUTE) {
    return formatBrowserTitle("Workspace Home");
  }
  if (frontendPath === "/auth/login") {
    return formatBrowserTitle("Sign In");
  }
  if (frontendPath === "/auth/callback") {
    return formatBrowserTitle("Signing In");
  }
  if (frontendPath === "/auth/update-password") {
    return formatBrowserTitle("Update Password");
  }
  if (frontendPath === "/auth/unauthorized") {
    return formatBrowserTitle("Access Not Authorized");
  }
  if (frontendPath === "/auth/invite/pending") {
    return formatBrowserTitle("Invitation Pending");
  }
  if (inviteToken) {
    return formatBrowserTitle("Accept Invitation");
  }
  if (leaveSphereQuickApprovalToken) {
    return formatBrowserTitle("LeaveSphere Quick Approval");
  }
  if (frontendPath === "/profile") {
    return formatBrowserTitle("My Profile");
  }
  if (frontendPath === "/admin/users") {
    return formatBrowserTitle("Admin Users");
  }
  if (scopedAdminAppCode) {
    return formatBrowserTitle("Admin", formatAppLabel(scopedAdminAppCode));
  }

  const appRouteTitle = resolveAppRouteTitle(frontendPath);
  if (appRouteTitle) {
    return formatBrowserTitle(appRouteTitle.pageTitle, appRouteTitle.appTitle);
  }

  return formatBrowserTitle("Page Not Found");
}

function toFrontendHref(route: string): string {
  return normalizeLegacyLeaveSphereRoute(route);
}

function getCurrentRelativeUrl(): string {
  if (typeof window === "undefined") {
    return "/";
  }
  const pathname = String(window.location.pathname || "").trim() || "/";
  const search = String(window.location.search || "");
  const hash = String(window.location.hash || "");
  return `${pathname}${search}${hash}`;
}

function buildLoginHref(returnTo: string): string {
  const normalizedReturnTo = String(returnTo || "").trim();
  if (!normalizedReturnTo || normalizedReturnTo === "/auth/login" || normalizedReturnTo.startsWith("/auth/login?")) {
    return "/auth/login";
  }
  return `/auth/login?return_to=${encodeURIComponent(normalizedReturnTo)}`;
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
  if (route === "/tradsphere/invoice-checklists") {
    return "tradsphere.invoice-checklists.scrollY";
  }
  if (route === "/tradsphere/traffic") {
    return "tradsphere.traffic.scrollY";
  }
  const scopedAdminMatch = route.match(/^\/([a-z0-9-_]+)\/admin$/);
  if (scopedAdminMatch) {
    return `${scopedAdminMatch[1]}.admin.scrollY`;
  }
  if (route === "/shiftzy/home") {
    return "shiftzy.home.scrollY";
  }
  if (route === "/shiftzy/employees") {
    return "shiftzy.employees.scrollY";
  }
  if (route === "/leavesphere/home") {
    return "leavesphere.home.scrollY";
  }
  if (route === "/leavesphere/leave-management") {
    return "leavesphere.leave-management.scrollY";
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
    window.location.replace(buildLoginHref(getCurrentRelativeUrl()));
  }, []);

  return <div className="p-6 text-sm text-slate-600">Redirecting to login...</div>;
}

function RedirectToHome() {
  useEffect(() => {
    if (window.location.pathname === "/") {
      return;
    }
    window.location.replace("/");
  }, []);

  return <div className="p-6 text-sm text-slate-600">Redirecting to workspace home...</div>;
}

function RouteChunkFallback() {
  return <AuthLoadingFallback message="Loading page..." />;
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
  if (isAccessResolutionPending(auth.status, auth.accessLoading, auth.accessProfile, auth.accessError)) {
    return <AuthLoadingFallback />;
  }
  if (!auth.accessProfile) {
    return <>{fallback}</>;
  }
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
  if (!shouldProtectFrontendAuth()) {
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
  if (!hasAppViewAccess(auth.accessProfile, appCode)) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}

function RequireAppPageRoute({
  appCode,
  route,
  children,
  fallback,
}: {
  appCode: string;
  route: string;
  children: ReactNode;
  fallback: ReactNode;
}) {
  const auth = useAuth();
  if (!shouldProtectFrontendAuth()) {
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
  const hasAccess = canAccessAppRoute({
    accessProfile: auth.accessProfile,
    appCode,
    tenantSlug: auth.tenantSlug,
    route,
  });
  if (!hasAccess) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}

function RequireAppAdmin({
  appCode,
  children,
  fallback,
}: {
  appCode: string;
  children: ReactNode;
  fallback: ReactNode;
}) {
  const auth = useAuth();
  if (!shouldProtectFrontendAuth()) {
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
  if (!hasAppAdminAccess(auth.accessProfile, appCode)) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}

function RequireAdminScope({ children, fallback }: { children: ReactNode; fallback: ReactNode }) {
  const auth = useAuth();
  if (!shouldProtectFrontendAuth()) {
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
  if (!hasSuperAdminAccess(auth.accessProfile)) {
    return <>{fallback}</>;
  }
  return <>{children}</>;
}

function App() {
  const [frontendPath, setFrontendPath] = useState(() => getFrontendPath(window.location.pathname));
  const frontendPathRef = useRef(frontendPath);
  useRouteScrollRestoration(toScrollStorageKey(frontendPath));

  useEffect(() => {
    frontendPathRef.current = frontendPath;
  }, [frontendPath]);

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
      const nextPath = getFrontendPath(window.location.pathname);
      const currentPath = frontendPathRef.current;
      if (nextPath === currentPath) {
        return;
      }

      let handled = false;
      const proceed = () => {
        if (handled) {
          return;
        }
        handled = true;
        window.history.pushState({}, "", toFrontendHref(nextPath));
        setFrontendPath(nextPath);
      };
      const beforeChangeEvent = new CustomEvent<{
        from: string;
        to: string;
        reason: "pop";
        proceed: () => void;
      }>("workspace:before-route-change", {
        cancelable: true,
        detail: {
          from: currentPath,
          to: nextPath,
          reason: "pop",
          proceed,
        },
      });
      window.dispatchEvent(beforeChangeEvent);
      if (beforeChangeEvent.defaultPrevented && !handled) {
        window.history.pushState({}, "", toFrontendHref(currentPath));
        return;
      }
      if (!beforeChangeEvent.defaultPrevented) {
        handled = true;
        setFrontendPath(nextPath);
      }
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
      "/tradsphere/traffic",
      "/tradsphere/invoice-checklists",
      "/shiftzy/home",
      "/shiftzy/employees",
      "/leavesphere/home",
      "/leavesphere/leave-management",
    ]);
  }, []);
  const scopedAdminAppCode = useMemo(() => parseScopedAdminRoute(frontendPath), [frontendPath]);

  function navigate(route: string) {
    const href = toFrontendHref(route);
    if (window.location.pathname === href || frontendPath === route) {
      return;
    }
    let handled = false;
    const proceed = () => {
      if (handled) {
        return;
      }
      handled = true;
      window.history.pushState({}, "", href);
      setFrontendPath(route);
    };
    const beforeChangeEvent = new CustomEvent<{
      from: string;
      to: string;
      reason: "push";
      proceed: () => void;
    }>("workspace:before-route-change", {
      cancelable: true,
      detail: {
        from: frontendPath,
        to: route,
        reason: "push",
        proceed,
      },
    });
    window.dispatchEvent(beforeChangeEvent);
    if (beforeChangeEvent.defaultPrevented && !handled) {
      return;
    }
    if (!beforeChangeEvent.defaultPrevented) {
      proceed();
    }
  }

  const inviteToken = useMemo(() => {
    if (!frontendPath.startsWith("/auth/invite/")) {
      return null;
    }
    if (frontendPath === "/auth/invite/pending") {
      return null;
    }
    const token = frontendPath.replace("/auth/invite/", "").trim();
    return token || null;
  }, [frontendPath]);
  const leaveSphereQuickApprovalToken = useMemo(() => parseLeaveSphereQuickApprovalToken(frontendPath), [frontendPath]);
  const browserTitle = useMemo(
    () =>
      resolveBrowserTitle({
        frontendPath,
        inviteToken,
        leaveSphereQuickApprovalToken,
        scopedAdminAppCode,
      }),
    [frontendPath, inviteToken, leaveSphereQuickApprovalToken, scopedAdminAppCode],
  );

  useEffect(() => {
    document.title = browserTitle;
  }, [browserTitle]);

  function renderTradsphereRoute() {
    return (
      <RequireSignedIn>
        <RequireTenantAccess fallback={<RedirectToHome />}>
          <RequirePermission permission="tradsphere.viewer" fallback={<RedirectToHome />}>
            {frontendPath === "/tradsphere/home" ? (
              <RequireAppPageRoute appCode="tradsphere" route="/tradsphere/home" fallback={<RedirectToHome />}>
                <Suspense fallback={<RouteChunkFallback />}>
                  <AccountsPage />
                </Suspense>
              </RequireAppPageRoute>
            ) : null}
            {frontendPath === "/tradsphere/estnums" ? (
              <RequireAppPageRoute appCode="tradsphere" route="/tradsphere/estnums" fallback={<RedirectToHome />}>
                <Suspense fallback={<RouteChunkFallback />}>
                  <EstimateNumbersPage />
                </Suspense>
              </RequireAppPageRoute>
            ) : null}
            {frontendPath === "/tradsphere/contacts" ? (
              <RequireAppPageRoute appCode="tradsphere" route="/tradsphere/contacts" fallback={<RedirectToHome />}>
                <Suspense fallback={<RouteChunkFallback />}>
                  <ContactsPage />
                </Suspense>
              </RequireAppPageRoute>
            ) : null}
            {frontendPath === "/tradsphere/stations" ? (
              <RequireAppPageRoute appCode="tradsphere" route="/tradsphere/stations" fallback={<RedirectToHome />}>
                <Suspense fallback={<RouteChunkFallback />}>
                  <StationsPage />
                </Suspense>
              </RequireAppPageRoute>
            ) : null}
            {frontendPath === "/tradsphere/traffic" ? (
              <RequireAppPageRoute appCode="tradsphere" route="/tradsphere/traffic" fallback={<RedirectToHome />}>
                <Suspense fallback={<RouteChunkFallback />}>
                  <TrafficPage />
                </Suspense>
              </RequireAppPageRoute>
            ) : null}
            {frontendPath === "/tradsphere/invoice-checklists" ? (
              <RequireAppPageRoute appCode="tradsphere" route="/tradsphere/invoice-checklists" fallback={<RedirectToHome />}>
                <Suspense fallback={<RouteChunkFallback />}>
                  <InvoiceChecklistPage />
                </Suspense>
              </RequireAppPageRoute>
            ) : null}
          </RequirePermission>
        </RequireTenantAccess>
      </RequireSignedIn>
    );
  }

  function renderShiftzyRoute() {
    return (
      <RequireSignedIn>
        <RequireTenantAccess fallback={<RedirectToHome />}>
          <RequireAppView appCode="shiftzy" fallback={<RedirectToHome />}>
            {frontendPath === "/shiftzy/home" ? (
              <RequireAppPageRoute appCode="shiftzy" route="/shiftzy/home" fallback={<RedirectToHome />}>
                <Suspense fallback={<RouteChunkFallback />}>
                  <ShiftzySchedulePage />
                </Suspense>
              </RequireAppPageRoute>
            ) : null}
            {frontendPath === "/shiftzy/employees" ? (
              <RequireAppPageRoute appCode="shiftzy" route="/shiftzy/employees" fallback={<RedirectToHome />}>
                <Suspense fallback={<RouteChunkFallback />}>
                  <ShiftzyEmployeesPage />
                </Suspense>
              </RequireAppPageRoute>
            ) : null}
          </RequireAppView>
        </RequireTenantAccess>
      </RequireSignedIn>
    );
  }

  function renderLeaveSphereRoute() {
    return (
      <RequireSignedIn>
        <RequireTenantAccess fallback={<RedirectToHome />}>
          <RequireAppView appCode="leavesphere" fallback={<RedirectToHome />}>
            {frontendPath === "/leavesphere/home" ? (
              <RequireAppPageRoute appCode="leavesphere" route="/leavesphere/home" fallback={<RedirectToHome />}>
                <Suspense fallback={<RouteChunkFallback />}>
                  <LeaveSphereMyPtoPage />
                </Suspense>
              </RequireAppPageRoute>
            ) : null}
            {frontendPath === "/leavesphere/leave-management" ? (
              <RequireAppAdmin appCode="leavesphere" fallback={<RedirectToHome />}>
                <RequireAppPageRoute appCode="leavesphere" route="/leavesphere/leave-management" fallback={<RedirectToHome />}>
                  <Suspense fallback={<RouteChunkFallback />}>
                    <LeaveSphereLeaveManagementPage />
                  </Suspense>
                </RequireAppPageRoute>
              </RequireAppAdmin>
            ) : null}
          </RequireAppView>
        </RequireTenantAccess>
      </RequireSignedIn>
    );
  }

  function renderScopedAppAdminRoute(appCode: string) {
    const normalizedAppCode = String(appCode || "").trim().toLowerCase();
    if (!normalizedAppCode) {
      return (
        <Suspense fallback={<RouteChunkFallback />}>
          <WorkspaceNotFoundPage onNavigate={navigate} />
        </Suspense>
      );
    }
    const appLabel = formatAppLabel(normalizedAppCode);
    return (
      <RequireSignedIn>
        <RequireTenantAccess fallback={<UnauthorizedPage />}>
          <RequireAppView appCode={normalizedAppCode} fallback={<UnauthorizedPage />}>
            <RequireAnyPermission permissions={["workspace.super_admin", `${normalizedAppCode}.admin`]} fallback={<UnauthorizedPage />}>
              <Suspense fallback={<RouteChunkFallback />}>
                <AppScopedAdminPage appCode={normalizedAppCode} appName={appLabel} />
              </Suspense>
            </RequireAnyPermission>
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
            <Suspense fallback={<RouteChunkFallback />}>
              <ProfilePage />
            </Suspense>
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
            <Suspense fallback={<RouteChunkFallback />}>
              <AdminUsersPage />
            </Suspense>
          </RequireAdminScope>
        </RequireTenantAccess>
      </RequireSignedIn>
    );
  }

  const routeContent = (
    <>
      {frontendPath === "/auth/login" ? <LoginPage /> : null}
      {frontendPath === "/auth/callback" ? <AuthCallbackPage /> : null}
      {frontendPath === "/auth/update-password" ? <UpdatePasswordPage /> : null}
      {frontendPath === "/auth/unauthorized" ? <UnauthorizedPage /> : null}
      {frontendPath === "/auth/invite/pending" ? <PendingInvitePage /> : null}
      {inviteToken ? <InviteAcceptPage token={inviteToken} /> : null}
      {leaveSphereQuickApprovalToken ? (
        <Suspense fallback={<RouteChunkFallback />}>
          <LeaveSphereQuickApprovalPage token={leaveSphereQuickApprovalToken} />
        </Suspense>
      ) : null}
      {frontendPath === "/profile" ? renderProfileRoute() : null}
      {frontendPath === "/admin/users" ? renderAdminRoute() : null}
      {frontendPath.startsWith("/tradsphere/") ? renderTradsphereRoute() : null}
      {frontendPath.startsWith("/shiftzy/") ? renderShiftzyRoute() : null}
      {frontendPath.startsWith("/leavesphere/") && !leaveSphereQuickApprovalToken ? renderLeaveSphereRoute() : null}
      {scopedAdminAppCode ? renderScopedAppAdminRoute(scopedAdminAppCode) : null}
      {frontendPath === HOME_ROUTE ? (
        <RequireSignedIn>
          <Suspense fallback={<RouteChunkFallback />}>
            <WorkspacePortalPage onNavigate={navigate} />
          </Suspense>
        </RequireSignedIn>
      ) : null}
      {!knownRoutes.has(frontendPath) && !inviteToken && !scopedAdminAppCode && !leaveSphereQuickApprovalToken ? (
        <Suspense fallback={<RouteChunkFallback />}>
          <WorkspaceNotFoundPage onNavigate={navigate} />
        </Suspense>
      ) : null}
    </>
  );

  return (
    <AuthProvider>
      <ToastProvider>
        {frontendPath === "/auth/login" ? (
          <div className="relative min-h-screen overflow-hidden bg-app-gradient text-foreground">
            <div className="pointer-events-none absolute -left-16 top-0 size-72 rounded-full bg-blue-200/40 blur-3xl" />
            <div className="pointer-events-none absolute right-0 top-16 size-72 rounded-full bg-cyan-200/30 blur-3xl" />
            <div className="pointer-events-none absolute left-[45%] top-8 size-64 rounded-full bg-violet-200/20 blur-3xl" />
            <main className="relative min-h-screen w-full space-y-6 px-4 py-5 sm:px-6 lg:px-8">
              {routeContent}
            </main>
          </div>
        ) : leaveSphereQuickApprovalToken ? (
          <>{routeContent}</>
        ) : (
          <AppShell currentPath={frontendPath} onNavigate={navigate}>
            {routeContent}
          </AppShell>
        )}
      </ToastProvider>
    </AuthProvider>
  );
}

export default App;
