import { ArrowRight, LayoutDashboard, LogIn, Lock, type LucideIcon } from "lucide-react";

import { APP_NAV_ITEMS } from "@/components/layout/navigation";
import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { Section, SectionHeader } from "@shared/components";
import { CacheStatusChip } from "@shared/components/status/CacheStatusChip";
import { getAccessAssignments, roleChipClass, roleLabel, rolePriority, tenantChipClass } from "@shared/auth/accessAssignments";
import { hasSuperAdminAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { useEffect, useMemo, useState } from "react";

type WorkspacePortalPageProps = {
  onNavigate: (route: string) => void;
};

const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";

export function WorkspacePortalPage({ onNavigate }: WorkspacePortalPageProps) {
  const auth = useAuth();
  const isSignedIn = auth.status === "authenticated" && Boolean(auth.user);
  const isSuperAdmin = hasSuperAdminAccess(auth.accessProfile);
  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());
  const appMetaByCode = useMemo(
    () => new Map(APP_NAV_ITEMS.map((item) => [item.id.toLowerCase(), item])),
    [],
  );

  const assignmentCards = useMemo(() => {
    if (!isSignedIn) {
      return [];
    }
    const assignments = getAccessAssignments(auth.accessProfile);
    const perTenantApp = new Map<string, {
      appCode: string;
      appName: string;
      appDescription: string;
      route: string;
      available: boolean;
      icon: LucideIcon;
      tenantSlug: string;
      tenantName: string | null;
      role: string;
    }>();

    for (const assignment of assignments) {
      const appCode = assignment.appCode.toLowerCase();
      const meta = appMetaByCode.get(appCode);
      const appName = meta?.label || assignment.appName || toTitleCase(appCode || "workspace");
      const appDescription = meta?.description || "Workspace app";
      const route = meta?.route || `/${appCode}/home`;
      const available = Boolean(meta?.available);
      const icon = (meta?.icon || LayoutDashboard) as LucideIcon;
      const dedupeKey = `${appCode}::${assignment.tenantSlug}`;
      const current = perTenantApp.get(dedupeKey);
      if (!current || rolePriority(assignment.role) < rolePriority(current.role)) {
        perTenantApp.set(dedupeKey, {
          appCode,
          appName,
          appDescription,
          route,
          available,
          icon,
          tenantSlug: assignment.tenantSlug,
          tenantName: assignment.tenantName,
          role: assignment.role,
        });
      }
    }

    const cards = Array.from(perTenantApp.values());
    if (cards.length === 0 && isSuperAdmin && auth.accessProfile?.app?.code && auth.accessProfile?.tenant?.slug) {
      const fallbackCode = String(auth.accessProfile.app.code || "").trim().toLowerCase();
      const meta = appMetaByCode.get(fallbackCode);
      cards.push({
        appCode: fallbackCode,
        appName: meta?.label || toTitleCase(fallbackCode || "workspace"),
        appDescription: meta?.description || "Workspace app",
        route: meta?.route || `/${fallbackCode}/home`,
        available: Boolean(meta?.available),
        icon: (meta?.icon || LayoutDashboard) as LucideIcon,
        tenantSlug: String(auth.accessProfile.tenant.slug || "").trim().toLowerCase(),
        tenantName: null,
        role: "super_admin",
      });
    }

    return cards.sort((a, b) => {
      const appCmp = a.appName.localeCompare(b.appName);
      if (appCmp !== 0) {
        return appCmp;
      }
      const tenantCmp = (a.tenantName || a.tenantSlug).localeCompare(b.tenantName || b.tenantSlug);
      if (tenantCmp !== 0) {
        return tenantCmp;
      }
      const roleCmp = rolePriority(a.role) - rolePriority(b.role);
      if (roleCmp !== 0) {
        return roleCmp;
      }
      return a.appCode.localeCompare(b.appCode);
    });
  }, [appMetaByCode, auth.accessProfile, isSignedIn, isSuperAdmin]);

  const launchableAssignments = assignmentCards.filter((item) => item.available);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleStorage = () => {
      const collapsed = readSidebarCollapsedState();
      setSidebarVisuallyExpanded(!collapsed);
    };
    const handleSidebarEvent = (event: Event) => {
      const customEvent = event as CustomEvent<{ collapsed?: boolean; visuallyExpanded?: boolean }>;
      if (typeof customEvent.detail?.visuallyExpanded === "boolean") {
        setSidebarVisuallyExpanded(customEvent.detail.visuallyExpanded);
        return;
      }
      if (typeof customEvent.detail?.collapsed === "boolean") {
        setSidebarVisuallyExpanded(!customEvent.detail.collapsed);
        return;
      }
      const collapsed = readSidebarCollapsedState();
      setSidebarVisuallyExpanded(!collapsed);
    };

    window.addEventListener("storage", handleStorage);
    window.addEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    return () => {
      window.removeEventListener("storage", handleStorage);
      window.removeEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    };
  }, []);

  const footerCacheStatusText = useMemo(() => {
    if (!isSignedIn) {
      return null;
    }
    if (auth.accessLoading && !auth.accessProfile) {
      return "Loading...";
    }
    if (auth.accessLoading) {
      return "Refreshing...";
    }
    if (auth.accessError) {
      return "Showing cached data. Could not refresh access profile.";
    }
    if (auth.accessCacheStatus) {
      return `Data source: ${auth.accessCacheStatus.source}. Last updated ${formatRelativeTime(auth.accessCacheStatus.fetchedAt)}.`;
    }
    return "Loading...";
  }, [auth.accessCacheStatus, auth.accessError, auth.accessLoading, auth.accessProfile, isSignedIn]);

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-6 pb-12">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="Workspace Home"
        description="Select an app to continue. Access is invite-only."
        gradientVariant="workspace"
        action={
          !isSignedIn ? (
            <Button onClick={() => onNavigate("/auth/login")}>
              <LogIn className="size-4" />
              Sign in
            </Button>
          ) : launchableAssignments.length > 0 ? (
            <Button
              onClick={() => {
                const next = launchableAssignments[0];
                auth.setTenantSlug(next.tenantSlug);
                onNavigate(next.route);
              }}
            >
              Open {launchableAssignments[0].appName}
            </Button>
          ) : undefined
        }
      />

      {!isSignedIn ? (
        <Section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
          <SectionHeader title="Sign-In Required" description="Sign in to view apps available to your account." />
          <p className="text-sm text-slate-600">Use your email and password on the login page.</p>
        </Section>
      ) : null}

      {isSignedIn && assignmentCards.length === 0 ? (
        <Section className="rounded-2xl border border-amber-200 bg-amber-50 p-5 shadow-soft">
          <SectionHeader
            title="No App Access Yet"
            description="You do not have access to any apps yet. Contact your workspace administrator."
          />
        </Section>
      ) : null}

      <Section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
        <SectionHeader
          title="Announcements"
          description="Important updates for all workspace users."
        />
        <p className="text-sm text-slate-600">
          No announcements right now.
        </p>
      </Section>

      {isSignedIn && assignmentCards.length > 0 ? (
        <Section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
          <SectionHeader title="Your App Access" description="Select a tenant-app workspace to continue." />

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 md:grid-cols-3">
            {assignmentCards.map((assignment) => {
              const Icon = assignment.icon;
              const tenantLabel = assignment.tenantName || assignment.tenantSlug;
              return (
                <article
                  key={`${assignment.appCode}::${assignment.tenantSlug}`}
                  className="group relative flex h-full flex-col overflow-hidden rounded-3xl border border-blue-100/95 bg-[linear-gradient(168deg,rgba(255,255,255,0.96)_0%,rgba(244,248,255,0.95)_52%,rgba(241,246,255,0.95)_100%)] p-4 shadow-soft transition hover:-translate-y-0.5 hover:border-blue-300 hover:shadow-md"
                >
                  <div className="pointer-events-none absolute inset-x-0 top-0 h-20 bg-gradient-to-r from-blue-500/8 via-cyan-400/6 to-violet-500/7" />
                  <div className="flex items-start justify-between gap-3">
                    <div className="relative z-10 flex items-center gap-3">
                      <div className="flex size-10 items-center justify-center rounded-xl border border-blue-100 bg-blue-50/90 text-blue-700">
                        <Icon className="size-5" />
                      </div>
                      <div>
                        <h2 className="text-base font-semibold text-slate-900">{assignment.appName}</h2>
                        <p className="text-sm text-slate-500">{assignment.appDescription}</p>
                      </div>
                    </div>
                    <span
                      className={`rounded-full px-2.5 py-1 text-xs font-medium ${
                        assignment.available ? "bg-emerald-50 text-emerald-700" : "bg-slate-100 text-slate-500"
                      }`}
                    >
                      {assignment.available ? "Available" : "Soon"}
                    </span>
                  </div>

                  <div className="relative z-10 mt-4 flex flex-wrap gap-2">
                    <span
                      className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold uppercase tracking-[0.08em] ${tenantChipClass(assignment.tenantSlug)}`}
                    >
                      {tenantLabel}
                    </span>
                    <span
                      className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold ${roleChipClass(assignment.role)}`}
                    >
                      {roleLabel(assignment.role)}
                    </span>
                  </div>

                  {assignment.available ? (
                    <Button
                      className="mt-4 w-full justify-between"
                      onClick={() => {
                        auth.setTenantSlug(assignment.tenantSlug);
                        onNavigate(assignment.route);
                      }}
                      aria-label={`Open ${assignment.appName} for tenant ${tenantLabel} as ${roleLabel(assignment.role)}`}
                    >
                      Open {assignment.appName}
                      <ArrowRight className="size-4" />
                    </Button>
                  ) : (
                    <Button className="mt-4 w-full" variant="secondary" disabled>
                      <Lock className="size-4" />
                      Coming Soon
                    </Button>
                  )}
                </article>
              );
            })}
          </div>
        </Section>
      ) : null}

      {footerCacheStatusText ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40">
          <div className={`mx-4 sm:mx-6 lg:mr-6 ${sidebarVisuallyExpanded ? "lg:ml-[18.75rem]" : "lg:ml-[6.5rem]"}`}>
            <div className="mx-auto w-full max-w-[1600px]">
              <CacheStatusChip
                text={footerCacheStatusText}
                onRefresh={auth.refreshAccessProfile}
                disabled={auth.accessLoading}
                refreshing={auth.accessLoading}
                refreshLabel="Refresh workspace access cache"
                tooltipText="Click to refresh workspace access profile"
                containerClassName="pointer-events-auto"
                className="max-w-[min(90vw,38rem)]"
              />
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function toTitleCase(value: string): string {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return "Workspace";
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function readSidebarCollapsedState(): boolean {
  if (typeof window === "undefined") {
    return false;
  }

  try {
    const nextValue = window.localStorage.getItem(SIDEBAR_COLLAPSED_STORAGE_KEY);
    if (nextValue !== null) {
      return nextValue === "1";
    }
    return window.localStorage.getItem(LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

function formatRelativeTime(timestamp: number): string {
  const deltaMs = Date.now() - timestamp;
  const minuteMs = 60 * 1000;
  const hourMs = 60 * minuteMs;
  const dayMs = 24 * hourMs;
  if (deltaMs < minuteMs) {
    return "just now";
  }
  if (deltaMs < hourMs) {
    const minutes = Math.max(1, Math.floor(deltaMs / minuteMs));
    return `${minutes}m ago`;
  }
  if (deltaMs < dayMs) {
    const hours = Math.max(1, Math.floor(deltaMs / hourMs));
    return `${hours}h ago`;
  }
  const days = Math.max(1, Math.floor(deltaMs / dayMs));
  return `${days}d ago`;
}
