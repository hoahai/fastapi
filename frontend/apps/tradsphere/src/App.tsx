import { useEffect, useMemo, useState } from "react";

import TradsphereHomePage from "@/TradsphereHomePage";
import { WorkspaceNotFoundPage } from "@home/WorkspaceNotFoundPage";
import { WorkspacePortalPage } from "@home/WorkspacePortalPage";
import { AppShell } from "@/components/layout/AppShell";
import { HOME_ROUTE } from "@/components/layout/navigation";
import { ToastProvider } from "@/components/ui/toast";
import { useRouteScrollRestoration } from "@/hooks/useRouteScrollRestoration";

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
  const normalized = route.replace(/[^a-zA-Z0-9]+/g, ".").replace(/^\.+|\.+$/g, "").toLowerCase();
  return `${normalized || "workspace"}.scrollY`;
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
    return new Set<string>([HOME_ROUTE, "/tradsphere/home"]);
  }, []);

  function navigate(route: string) {
    const href = toFrontendHref(route);
    if (window.location.pathname === href || frontendPath === route) {
      return;
    }

    window.history.pushState({}, "", href);
    setFrontendPath(route);
  }

  return (
    <ToastProvider>
      <AppShell currentPath={frontendPath} onNavigate={navigate}>
        {frontendPath === "/tradsphere/home" ? <TradsphereHomePage /> : null}
        {frontendPath === HOME_ROUTE ? <WorkspacePortalPage onNavigate={navigate} /> : null}
        {!knownRoutes.has(frontendPath) ? <WorkspaceNotFoundPage onNavigate={navigate} /> : null}
      </AppShell>
    </ToastProvider>
  );
}

export default App;
