import {
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  LayoutDashboard,
  LogIn,
  LogOut,
  Menu,
  ShieldCheck,
  UserRound,
} from "lucide-react";
import { useEffect, useMemo, useState, type ComponentType, type FocusEvent } from "react";

import { Button } from "@/components/ui/button";
import {
  APP_NAV_ITEMS,
  SHIFTZY_ACCOUNTS_CHILD_ICON,
  SHIFTZY_SCHEDULES_CHILD_ICON,
  TRADSPHERE_CONTACTS_CHILD_ICON,
  TRADSPHERE_ESTNUMS_CHILD_ICON,
  TRADSPHERE_HOME_CHILD_ICON,
  TRADSPHERE_STATIONS_CHILD_ICON,
  type AppNavChildItem,
  type AppNavItem,
} from "@/components/layout/navigation";
import { cn } from "@/lib/utils";
import {
  getAccessAssignments,
  roleChipClass,
  roleLabel,
  rolePriority,
  tenantChipClass,
  type NormalizedAccessAssignment,
} from "@shared/auth/accessAssignments";
import { shouldProtectTradsphereFrontend } from "@shared/auth/guards";
import { hasAnyAdminScope, hasSuperAdminAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";

type SidebarProps = {
  currentPath: string;
  collapsed: boolean;
  visuallyExpanded: boolean;
  onToggleCollapsed: () => void;
  onHoverExpandedChange: (nextValue: boolean) => void;
  onNavigate: (route: string) => void;
  mobileOpen: boolean;
  onToggleMobile: () => void;
  onCloseMobile: () => void;
};

type AppNavGroup = {
  item: AppNavItem;
  tenantAssignments: NormalizedAccessAssignment[];
};

export function Sidebar({
  currentPath,
  collapsed,
  visuallyExpanded,
  onToggleCollapsed,
  onHoverExpandedChange,
  onNavigate,
  mobileOpen,
  onToggleMobile,
  onCloseMobile,
}: SidebarProps) {
  const auth = useAuth();
  const [expandedById, setExpandedById] = useState<Record<string, boolean>>(() => ({}));
  const [accountMenuOpen, setAccountMenuOpen] = useState(false);
  const isCompact = !visuallyExpanded;
  const isSignedIn = auth.status === "authenticated" && Boolean(auth.user);
  const protectionEnabled = shouldProtectTradsphereFrontend();
  const hasAdminPermission =
    isSignedIn
    && (
      !protectionEnabled
      || hasAnyAdminScope(
        auth.accessProfile,
        APP_NAV_ITEMS.filter((item) => item.available).map((item) => item.id),
      )
    );
  const isSuperAdmin = protectionEnabled && hasSuperAdminAccess(auth.accessProfile);
  const navGroups = useMemo<AppNavGroup[]>(
    () => {
      if (!isSignedIn) {
        return [];
      }

      const assignments = getAccessAssignments(auth.accessProfile);
      const groups: AppNavGroup[] = [];

      for (const item of APP_NAV_ITEMS) {
        const appCode = String(item.id || "").trim().toLowerCase();
        let tenantAssignments = assignments
          .filter((entry) => entry.appCode === appCode)
          .sort((a, b) => {
            const tenantCmp = (a.tenantName || a.tenantSlug).localeCompare(b.tenantName || b.tenantSlug);
            if (tenantCmp !== 0) {
              return tenantCmp;
            }
            return rolePriority(a.role) - rolePriority(b.role);
          });

        if (!tenantAssignments.length && auth.accessProfile?.tenant?.slug) {
          const fallbackAppCode = String(auth.accessProfile.app?.code || "").trim().toLowerCase();
          if (fallbackAppCode === appCode) {
            const fallbackRole = String(auth.accessProfile.role || "viewer").trim().toLowerCase() || "viewer";
            tenantAssignments = [
              {
                tenantId: String(auth.accessProfile.tenant.id || "").trim(),
                tenantSlug: String(auth.accessProfile.tenant.slug || "").trim().toLowerCase(),
                tenantName: null,
                appId: String(auth.accessProfile.app?.id || "").trim(),
                appCode,
                appName: String(auth.accessProfile.app?.code || "").trim() || null,
                role: fallbackRole,
              },
            ];
          }
        }

        const shouldShow = isSuperAdmin || tenantAssignments.length > 0 || (!protectionEnabled && item.available);
        if (!shouldShow) {
          continue;
        }
        if (!isSuperAdmin && !item.available) {
          continue;
        }
        groups.push({
          item,
          tenantAssignments,
        });
      }

      return groups;
    },
    [auth.accessProfile, isSignedIn, isSuperAdmin, protectionEnabled],
  );
  const accountDisplayName = useMemo(() => {
    const fullName = String(auth.accessProfile?.user?.fullName || auth.user?.fullName || "").trim();
    if (fullName) {
      return fullName;
    }
    const email = String(auth.user?.email || "").trim();
    return email || "Signed in";
  }, [auth.accessProfile?.user?.fullName, auth.user?.fullName, auth.user?.email]);

  useEffect(() => {
    setExpandedById((current) => {
      const next = { ...current };
      for (const group of navGroups) {
        if (!group.item.children?.length) {
          continue;
        }
        if (isTopLevelActive(group.item, currentPath)) {
          next[group.item.id] = true;
        }
      }
      return next;
    });
  }, [currentPath, navGroups]);

  useEffect(() => {
    setAccountMenuOpen(false);
  }, [currentPath, mobileOpen, isCompact]);

  function toggleExpanded(itemId: string) {
    setExpandedById((current) => ({
      ...current,
      [itemId]: !current[itemId],
    }));
  }

  function setExpanded(itemId: string, value: boolean) {
    setExpandedById((current) => ({
      ...current,
      [itemId]: value,
    }));
  }

  function handleSidebarMouseEnter() {
    if (
      !collapsed ||
      typeof window === "undefined" ||
      !window.matchMedia("(hover: hover) and (pointer: fine)").matches
    ) {
      return;
    }
    onHoverExpandedChange(true);
  }

  function handleSidebarMouseLeave() {
    if (
      !collapsed ||
      typeof window === "undefined" ||
      !window.matchMedia("(hover: hover) and (pointer: fine)").matches
    ) {
      return;
    }
    onHoverExpandedChange(false);
  }

  function handleSidebarFocusCapture() {
    if (!collapsed) {
      return;
    }
    onHoverExpandedChange(true);
  }

  function handleSidebarBlurCapture(event: FocusEvent<HTMLElement>) {
    if (!collapsed) {
      return;
    }
    const nextFocusedElement = event.relatedTarget;
    if (nextFocusedElement instanceof Node && event.currentTarget.contains(nextFocusedElement)) {
      return;
    }
    onHoverExpandedChange(false);
  }

  function handleSignOut() {
    void auth.signOut();
    onCloseMobile();
    onNavigate("/auth/login");
  }

  function handleSelectTenantRoute(tenantSlug: string | null, route: string) {
    const normalizedTenant = String(tenantSlug || "").trim().toLowerCase();
    if (normalizedTenant) {
      auth.setTenantSlug(normalizedTenant);
    }
    onNavigate(route);
    onCloseMobile();
  }

  return (
    <>
      <header className="sticky top-0 z-30 rounded-2xl border border-blue-100/80 bg-white/90 p-3 shadow-soft backdrop-blur lg:hidden">
        <div className="flex items-center justify-between gap-3">
          <Button
            variant="outline"
            size="icon"
            className="border-blue-200/80 bg-white text-blue-700 hover:border-blue-300 hover:bg-blue-50"
            onClick={onToggleMobile}
            aria-label="Open navigation"
          >
            <Menu className="size-4" />
          </Button>
          <p className="text-sm font-semibold text-slate-800">Workspace Navigation</p>
          <div className="size-8" />
        </div>
      </header>

      {mobileOpen ? (
        <div className="fixed inset-0 z-40 bg-slate-950/35 backdrop-blur-[1px] lg:hidden" onClick={onCloseMobile} />
      ) : null}

      <aside
        className={cn(
          "fixed left-0 top-0 z-50 flex h-screen w-72 flex-col overflow-y-auto border-r border-blue-100/80 bg-[linear-gradient(186deg,rgba(255,255,255,0.96)_0%,rgba(242,248,255,0.96)_56%,rgba(236,248,255,0.96)_100%)] p-4 transition-[width,transform] duration-200 lg:left-4 lg:top-4 lg:bottom-4 lg:h-auto lg:w-[260px] lg:rounded-3xl lg:border lg:border-blue-100/90 lg:shadow-soft lg:backdrop-blur lg:translate-x-0",
          mobileOpen ? "translate-x-0" : "-translate-x-full lg:translate-x-0",
          isCompact ? "lg:w-[72px]" : "lg:w-[260px]",
        )}
        onMouseEnter={handleSidebarMouseEnter}
        onMouseLeave={handleSidebarMouseLeave}
        onFocusCapture={handleSidebarFocusCapture}
        onBlurCapture={handleSidebarBlurCapture}
      >
        <div className="pointer-events-none absolute left-3 right-3 top-3 h-20 rounded-2xl bg-gradient-to-r from-blue-500/9 via-cyan-400/8 to-emerald-300/7" />

        <div className="relative z-10 flex items-center justify-between gap-2 px-1 py-1">
          <div className={cn("min-w-0", isCompact && "lg:hidden")}>
            <p className="text-[10px] font-bold uppercase tracking-[0.26em] text-blue-700">TheSphereWorks</p>
            <h2 className="mt-1 text-base font-extrabold text-slate-900">Workspace</h2>
          </div>
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              className="hidden text-slate-500 hover:bg-blue-100/50 hover:text-blue-700 lg:inline-flex"
              onClick={onToggleCollapsed}
              aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            >
              {collapsed ? <ChevronRight className="size-4" /> : <ChevronLeft className="size-4" />}
            </Button>
            <Button
              variant="ghost"
              size="icon"
              className="text-slate-500 hover:bg-blue-100/50 hover:text-blue-700 lg:hidden"
              onClick={onCloseMobile}
              aria-label="Close navigation"
            >
              <ChevronLeft className="size-4" />
            </Button>
          </div>
        </div>

        {!isCompact ? (
          <div className="relative z-10 mt-4 flex items-center gap-2 px-1">
            <span className="h-px flex-1 bg-gradient-to-r from-blue-200/80 to-transparent" />
            <p className="text-[10px] font-semibold uppercase tracking-[0.26em] text-slate-500">Apps</p>
            <span className="h-px flex-1 bg-gradient-to-l from-blue-200/80 to-transparent" />
          </div>
        ) : null}

        <nav className="relative z-10 mt-3 flex-1 space-y-2">
          <SidebarItem
            icon={LayoutDashboard}
            label="Portal / Workspace Home"
            route="/"
            collapsed={isCompact}
            active={currentPath === "/"}
            available
            onNavigate={onNavigate}
            onCloseMobile={onCloseMobile}
          />
          {hasAdminPermission ? (
            <SidebarItem
              icon={ShieldCheck}
              label="Admin / Users"
              route="/admin/users"
              collapsed={isCompact}
              active={currentPath === "/admin/users"}
              available
              onNavigate={onNavigate}
              onCloseMobile={onCloseMobile}
            />
          ) : null}
          {navGroups.map((group) => {
            const topLevel = group.item;
            const tenantAssignments = group.tenantAssignments;
            const hasChildren = !isCompact && Boolean(topLevel.children?.length);
            const expanded = Boolean(expandedById[topLevel.id]);
            const parentActive = isTopLevelActive(topLevel, currentPath);
            const hasMultipleTenants = hasChildren && tenantAssignments.length > 1;
            const hasActiveTenantForApp = tenantAssignments.some((item) => item.tenantSlug === auth.tenantSlug);
            const defaultTenantSlug = hasActiveTenantForApp
              ? auth.tenantSlug
              : tenantAssignments[0]?.tenantSlug || auth.tenantSlug || null;

            return (
              <div
                key={topLevel.id}
                className={cn(
                  "space-y-1 rounded-2xl p-1",
                  hasChildren && expanded && "bg-blue-50/40",
                )}
              >
                <SidebarParentItem
                  icon={topLevel.icon}
                  label={topLevel.label}
                  collapsed={isCompact}
                  available={topLevel.available}
                  active={parentActive}
                  expanded={expanded}
                  hasChildren={hasChildren}
                  onNavigate={() => handleSelectTenantRoute(defaultTenantSlug, topLevel.route)}
                  onToggleExpand={() => toggleExpanded(topLevel.id)}
                  onExpand={() => setExpanded(topLevel.id, true)}
                />
                {hasChildren && expanded ? (
                  hasMultipleTenants ? (
                    <div className="space-y-2 border-l border-blue-100 pl-4">
                      {tenantAssignments.map((tenantAssignment) => (
                        <div
                          key={`${topLevel.id}::${tenantAssignment.tenantSlug}`}
                          className={cn(
                            "rounded-xl border border-blue-100/70 bg-white/65 p-2",
                            auth.tenantSlug === tenantAssignment.tenantSlug && "border-blue-200 bg-blue-50/65",
                          )}
                        >
                          <button
                            type="button"
                            onClick={() => handleSelectTenantRoute(tenantAssignment.tenantSlug, topLevel.route)}
                            className="flex w-full items-center justify-between gap-2 rounded-lg px-1.5 py-1 text-left transition hover:bg-blue-50/80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/65 focus-visible:ring-offset-1"
                            aria-current={auth.tenantSlug === tenantAssignment.tenantSlug && parentActive ? "page" : undefined}
                          >
                            <span
                              className={`inline-flex max-w-[72%] items-center truncate rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] ${tenantChipClass(tenantAssignment.tenantSlug)}`}
                            >
                              {tenantAssignment.tenantName || tenantAssignment.tenantSlug}
                            </span>
                            <span
                              className={`inline-flex shrink-0 items-center rounded-full border px-2 py-0.5 text-[10px] font-medium ${roleChipClass(tenantAssignment.role)}`}
                            >
                              {roleLabel(tenantAssignment.role)}
                            </span>
                          </button>
                          <div className="mt-1 space-y-1">
                            {topLevel.children!.map((child) => (
                              <SidebarChildItem
                                key={`${child.id}::${tenantAssignment.tenantSlug}`}
                                child={child}
                                currentPath={currentPath}
                                tenantSlug={tenantAssignment.tenantSlug}
                                activeTenantSlug={auth.tenantSlug}
                                onNavigate={handleSelectTenantRoute}
                              />
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="space-y-1 border-l border-blue-100 pl-4">
                      {topLevel.children!.map((child) => (
                        <SidebarChildItem
                          key={child.id}
                          child={child}
                          currentPath={currentPath}
                          tenantSlug={defaultTenantSlug}
                          activeTenantSlug={auth.tenantSlug}
                          onNavigate={handleSelectTenantRoute}
                        />
                      ))}
                    </div>
                  )
                ) : null}
              </div>
            );
          })}
        </nav>

        <div className="relative z-10 mt-4 rounded-2xl bg-gradient-to-r from-blue-100/35 via-cyan-100/25 to-transparent p-2">
          <div className="pt-3">
          {isSignedIn ? (
            <div
              className="relative"
              onMouseEnter={() => setAccountMenuOpen(true)}
              onMouseLeave={() => setAccountMenuOpen(false)}
              onFocusCapture={() => setAccountMenuOpen(true)}
              onBlurCapture={(event) => {
                const nextFocusedElement = event.relatedTarget;
                if (nextFocusedElement instanceof Node && event.currentTarget.contains(nextFocusedElement)) {
                  return;
                }
                setAccountMenuOpen(false);
              }}
            >
              <button
                type="button"
                aria-expanded={accountMenuOpen}
                aria-label="Account menu"
                className={cn(
                  "group flex w-full items-center gap-2 rounded-xl px-2 py-2 text-left transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/65 focus-visible:ring-offset-1",
                  "text-slate-700 hover:bg-white/70 hover:text-slate-900",
                  isCompact && "lg:justify-center",
                )}
                title={isCompact ? "Account" : undefined}
              >
                <UserRound className="size-4 shrink-0 text-blue-700 transition-colors group-hover:text-blue-800" />
                {!isCompact ? (
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-semibold text-slate-800">{accountDisplayName}</p>
                    <p className="truncate text-xs text-slate-500">{auth.user?.email || "-"}</p>
                  </div>
                ) : null}
              </button>

	              {accountMenuOpen ? (
	                <>
	                  <div
	                    aria-hidden="true"
	                    className={cn(
	                      "absolute",
	                      isCompact ? "left-full top-0 bottom-0 w-2" : "bottom-full left-0 right-0 h-2",
	                    )}
	                  />
	                  <div
	                    className={cn(
	                      "absolute z-20 rounded-xl border border-blue-100/85 bg-white/95 p-1 shadow-soft backdrop-blur",
	                      isCompact ? "bottom-0 left-full ml-2 w-44" : "bottom-full left-0 right-0 mb-2",
	                    )}
	                  >
	                    <button
	                      type="button"
	                      onClick={() => {
	                        setAccountMenuOpen(false);
	                        onNavigate("/profile");
	                        onCloseMobile();
	                      }}
	                      className="flex w-full items-center gap-2 rounded-lg px-2 py-2 text-left text-sm font-medium text-slate-700 transition hover:bg-blue-50/80 hover:text-blue-800"
	                    >
	                      <UserRound className="size-4" />
	                      <span>My profile</span>
	                    </button>
	                    <button
	                      type="button"
	                      onClick={() => {
	                        setAccountMenuOpen(false);
	                        handleSignOut();
	                      }}
	                      className="flex w-full items-center gap-2 rounded-lg px-2 py-2 text-left text-sm font-medium text-rose-700 transition hover:bg-rose-50/70 hover:text-rose-800"
	                    >
	                      <LogOut className="size-4" />
	                      <span>Sign out</span>
	                    </button>
	                  </div>
	                </>
	              ) : null}
	            </div>
          ) : (
            <button
              type="button"
              onClick={() => {
                onNavigate("/auth/login");
                onCloseMobile();
              }}
              className={cn(
                "flex w-full items-center gap-2 rounded-xl px-2 py-2 text-left text-sm font-medium text-slate-700 transition hover:bg-white/70 hover:text-blue-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/65 focus-visible:ring-offset-1",
                isCompact && "lg:justify-center",
              )}
              title={isCompact ? "Sign in" : undefined}
            >
              <LogIn className="size-4 shrink-0 text-blue-700" />
              {!isCompact ? <span>Sign in</span> : null}
            </button>
          )}
          </div>
        </div>
      </aside>
    </>
  );
}

type SidebarItemProps = {
  icon: ComponentType<{ className?: string }>;
  label: string;
  route: string;
  collapsed: boolean;
  active: boolean;
  available: boolean;
  onNavigate: (route: string) => void;
  onCloseMobile: () => void;
};

function SidebarItem({
  icon: Icon,
  label,
  route,
  collapsed,
  active,
  available,
  onNavigate,
  onCloseMobile,
}: SidebarItemProps) {
  const iconTone = active ? "active" : available ? "default" : "muted";

  return (
    <button
      type="button"
      onClick={() => {
        if (!available) {
          return;
        }
        onNavigate(route);
        onCloseMobile();
      }}
      disabled={!available}
      aria-current={active ? "page" : undefined}
      className={cn(
        "group flex w-full items-center gap-2.5 rounded-2xl px-2.5 py-2 text-left text-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/65 focus-visible:ring-offset-1",
        active
          ? "bg-gradient-to-r from-blue-100/85 via-cyan-50/70 to-white/80 text-blue-900"
          : "text-slate-700 hover:bg-white/70 hover:text-slate-900",
        !available && "cursor-not-allowed text-slate-500 opacity-70 hover:bg-transparent",
        collapsed && "lg:justify-center lg:px-1.5",
      )}
      title={collapsed ? label : undefined}
    >
      <NavIcon icon={Icon} tone={iconTone} />
      {!collapsed ? <span className="truncate font-medium">{label}</span> : null}
      {!available && !collapsed ? <SoonBadge className="ml-auto" /> : null}
    </button>
  );
}

type SidebarParentItemProps = {
  icon: ComponentType<{ className?: string }>;
  label: string;
  collapsed: boolean;
  available: boolean;
  active: boolean;
  expanded: boolean;
  hasChildren: boolean;
  onNavigate: () => void;
  onToggleExpand: () => void;
  onExpand: () => void;
};

function SidebarParentItem({
  icon: Icon,
  label,
  collapsed,
  available,
  active,
  expanded,
  hasChildren,
  onNavigate,
  onToggleExpand,
  onExpand,
}: SidebarParentItemProps) {
  const iconTone = active ? "active" : available ? "default" : "muted";

  return (
    <button
      type="button"
      onClick={() => {
        if (!available) {
          return;
        }
        if (hasChildren) {
          if (!expanded) {
            onExpand();
            return;
          }
          onToggleExpand();
          return;
        }
        onNavigate();
      }}
      disabled={!available}
      aria-current={active ? "page" : undefined}
      className={cn(
        "group flex w-full items-center gap-2.5 rounded-2xl px-2.5 py-2 text-left text-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/65 focus-visible:ring-offset-1",
        active
          ? "bg-gradient-to-r from-blue-100/85 via-cyan-50/70 to-white/80 text-blue-900"
          : "text-slate-700 hover:bg-white/70 hover:text-slate-900",
        !available && "cursor-not-allowed text-slate-500 opacity-70 hover:bg-transparent",
        collapsed && "lg:justify-center lg:px-1.5",
      )}
      title={collapsed ? label : undefined}
    >
      <NavIcon icon={Icon} tone={iconTone} />
      {!collapsed ? <span className="truncate font-medium">{label}</span> : null}
      {!collapsed && hasChildren ? (
        <ChevronDown
          className={cn(
            "ml-auto size-4 text-slate-400 transition-transform group-hover:text-blue-600",
            expanded && "rotate-180 text-blue-700",
          )}
        />
      ) : null}
      {!available && !collapsed ? <SoonBadge className="ml-auto" /> : null}
    </button>
  );
}

type SidebarChildItemProps = {
  child: AppNavChildItem;
  currentPath: string;
  tenantSlug: string | null;
  activeTenantSlug: string;
  onNavigate: (tenantSlug: string | null, route: string) => void;
};

function SidebarChildItem({ child, currentPath, tenantSlug, activeTenantSlug, onNavigate }: SidebarChildItemProps) {
  const normalizedTenantSlug = String(tenantSlug || "").trim().toLowerCase();
  const active = child.available && currentPath === child.route && (!normalizedTenantSlug || activeTenantSlug === normalizedTenantSlug);
  const ChildIcon =
    child.route === "/tradsphere/estnums"
      ? TRADSPHERE_ESTNUMS_CHILD_ICON
      : child.route === "/tradsphere/contacts"
        ? TRADSPHERE_CONTACTS_CHILD_ICON
        : child.route === "/tradsphere/stations"
          ? TRADSPHERE_STATIONS_CHILD_ICON
          : child.route === "/shiftzy/home"
            ? SHIFTZY_SCHEDULES_CHILD_ICON
            : child.route === "/shiftzy/employees"
              ? SHIFTZY_ACCOUNTS_CHILD_ICON
          : TRADSPHERE_HOME_CHILD_ICON;

  return (
    <button
      type="button"
      onClick={() => {
        if (!child.available) {
          return;
        }
        onNavigate(tenantSlug, child.route);
      }}
      disabled={!child.available}
      aria-current={active ? "page" : undefined}
      className={cn(
        "group flex w-full items-center gap-2 rounded-xl px-2.5 py-1.5 text-left text-[13px] transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/65 focus-visible:ring-offset-1",
        active
          ? "bg-blue-50/80 text-blue-900"
          : "text-slate-600 hover:bg-white/70 hover:text-slate-800",
        !child.available && "cursor-not-allowed text-slate-500 opacity-70 hover:bg-transparent",
      )}
    >
      <ChildIcon
        className={cn(
          "size-3.5 shrink-0 transition-colors",
          active ? "text-blue-700" : "text-blue-600 group-hover:text-blue-700",
          !child.available && "text-slate-400 group-hover:text-slate-400",
        )}
      />
      <span className="truncate font-medium">{child.label}</span>
      {!child.available ? <SoonBadge className="ml-auto" /> : null}
    </button>
  );
}

function NavIcon({
  icon: Icon,
  tone,
}: {
  icon: ComponentType<{ className?: string }>;
  tone: "active" | "default" | "muted";
}) {
  return (
    <Icon
      className={cn(
        "size-4 shrink-0 transition-colors",
        tone === "active" && "text-blue-700",
        tone === "default" && "text-blue-600 group-hover:text-blue-700",
        tone === "muted" && "text-slate-400 group-hover:text-slate-400",
      )}
    />
  );
}

function SoonBadge({ className }: { className?: string }) {
  return (
    <span
      className={cn(
        "rounded-full border border-slate-300 bg-slate-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-600",
        className,
      )}
    >
      Soon
    </span>
  );
}

function isTopLevelActive(item: AppNavItem, currentPath: string): boolean {
  if (!item.available) {
    return false;
  }
  if (item.activeMatchPrefix && currentPath.startsWith(item.activeMatchPrefix)) {
    return true;
  }
  return currentPath === item.route;
}
