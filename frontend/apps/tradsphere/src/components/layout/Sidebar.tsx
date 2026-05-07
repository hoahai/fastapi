import { ChevronDown, ChevronLeft, ChevronRight, LayoutDashboard, Menu } from "lucide-react";
import { useEffect, useState, type ComponentType } from "react";

import { Button } from "@/components/ui/button";
import {
  APP_NAV_ITEMS,
  TRADSPHERE_ESTNUMS_CHILD_ICON,
  TRADSPHERE_HOME_CHILD_ICON,
  type AppNavChildItem,
  type AppNavItem,
} from "@/components/layout/navigation";
import { cn } from "@/lib/utils";

type SidebarProps = {
  currentPath: string;
  collapsed: boolean;
  onToggleCollapsed: () => void;
  onNavigate: (route: string) => void;
  mobileOpen: boolean;
  onToggleMobile: () => void;
  onCloseMobile: () => void;
};

export function Sidebar({
  currentPath,
  collapsed,
  onToggleCollapsed,
  onNavigate,
  mobileOpen,
  onToggleMobile,
  onCloseMobile,
}: SidebarProps) {
  const [expandedById, setExpandedById] = useState<Record<string, boolean>>(() => ({}));

  useEffect(() => {
    setExpandedById((current) => {
      const next = { ...current };
      for (const item of APP_NAV_ITEMS) {
        if (!item.children?.length) {
          continue;
        }
        if (isTopLevelActive(item, currentPath)) {
          next[item.id] = true;
        }
      }
      return next;
    });
  }, [currentPath]);

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

  return (
    <>
      <header className="sticky top-0 z-30 rounded-2xl border border-border/80 bg-white/90 p-3 shadow-soft backdrop-blur lg:hidden">
        <div className="flex items-center justify-between gap-3">
          <Button variant="outline" size="icon" onClick={onToggleMobile} aria-label="Open navigation">
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
          "fixed left-0 top-0 z-50 h-screen w-72 overflow-y-auto border-r border-border bg-white p-4 transition-[width,transform] duration-200 lg:left-4 lg:top-4 lg:bottom-4 lg:h-auto lg:w-[260px] lg:rounded-3xl lg:border lg:border-border/80 lg:bg-white/95 lg:shadow-soft lg:backdrop-blur lg:translate-x-0",
          mobileOpen ? "translate-x-0" : "-translate-x-full lg:translate-x-0",
          collapsed ? "lg:w-[72px]" : "lg:w-[260px]",
        )}
      >
        <div className="flex items-center justify-between gap-2 px-1">
          <div className={cn("min-w-0", collapsed && "lg:hidden")}>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-blue-700">Portal</p>
            <h2 className="mt-1 text-lg font-bold text-slate-900">Workspace</h2>
          </div>
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              className="hidden lg:inline-flex"
              onClick={onToggleCollapsed}
              aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            >
              {collapsed ? <ChevronRight className="size-4" /> : <ChevronLeft className="size-4" />}
            </Button>
            <Button variant="ghost" size="icon" className="lg:hidden" onClick={onCloseMobile} aria-label="Close navigation">
              <ChevronLeft className="size-4" />
            </Button>
          </div>
        </div>

        {!collapsed ? (
          <p className="mt-4 px-2 text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">Apps</p>
        ) : null}

        <nav className="mt-3 space-y-2">
          <SidebarItem
            icon={LayoutDashboard}
            label="Portal / Workspace Home"
            route="/"
            collapsed={collapsed}
            active={currentPath === "/"}
            available
            onNavigate={onNavigate}
            onCloseMobile={onCloseMobile}
          />
          {APP_NAV_ITEMS.map((topLevel) => {
            const hasChildren = !collapsed && Boolean(topLevel.children?.length);
            const expanded = Boolean(expandedById[topLevel.id]);
            const parentActive = isTopLevelActive(topLevel, currentPath);

            return (
              <div key={topLevel.id} className="space-y-1">
                <SidebarParentItem
                  icon={topLevel.icon}
                  label={topLevel.label}
                  route={topLevel.route}
                  collapsed={collapsed}
                  available={topLevel.available}
                  active={parentActive}
                  expanded={expanded}
                  hasChildren={hasChildren}
                  onNavigate={onNavigate}
                  onCloseMobile={onCloseMobile}
                  onToggleExpand={() => toggleExpanded(topLevel.id)}
                  onExpand={() => setExpanded(topLevel.id, true)}
                />
                {hasChildren && expanded ? (
                  <div className="space-y-1 pl-9">
                    {topLevel.children!.map((child) => (
                      <SidebarChildItem
                        key={child.id}
                        child={child}
                        currentPath={currentPath}
                        onNavigate={onNavigate}
                        onCloseMobile={onCloseMobile}
                      />
                    ))}
                  </div>
                ) : null}
              </div>
            );
          })}
        </nav>
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
        "flex w-full items-center gap-3 rounded-xl border px-3 py-2 text-left text-sm transition",
        active
          ? "border-blue-300 bg-blue-50 text-blue-800"
          : "border-transparent text-slate-700 hover:border-slate-200 hover:bg-slate-50",
        !available && "cursor-not-allowed opacity-55",
        collapsed && "lg:justify-center lg:px-2",
      )}
      title={collapsed ? label : undefined}
    >
      <Icon className="size-4 shrink-0" />
      {!collapsed ? <span className="truncate">{label}</span> : null}
      {!available && !collapsed ? <span className="ml-auto text-[11px] text-slate-500">Soon</span> : null}
    </button>
  );
}

type SidebarParentItemProps = {
  icon: ComponentType<{ className?: string }>;
  label: string;
  route: string;
  collapsed: boolean;
  available: boolean;
  active: boolean;
  expanded: boolean;
  hasChildren: boolean;
  onNavigate: (route: string) => void;
  onCloseMobile: () => void;
  onToggleExpand: () => void;
  onExpand: () => void;
};

function SidebarParentItem({
  icon: Icon,
  label,
  route,
  collapsed,
  available,
  active,
  expanded,
  hasChildren,
  onNavigate,
  onCloseMobile,
  onToggleExpand,
  onExpand,
}: SidebarParentItemProps) {
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
            onNavigate(route);
            onCloseMobile();
            return;
          }
          onToggleExpand();
          return;
        }
        onNavigate(route);
        onCloseMobile();
      }}
      disabled={!available}
      aria-current={active ? "page" : undefined}
      className={cn(
        "flex w-full items-center gap-3 rounded-xl border px-3 py-2 text-left text-sm transition",
        active
          ? "border-blue-300 bg-blue-50 text-blue-800"
          : "border-transparent text-slate-700 hover:border-slate-200 hover:bg-slate-50",
        !available && "cursor-not-allowed opacity-55",
        collapsed && "lg:justify-center lg:px-2",
      )}
      title={collapsed ? label : undefined}
    >
      <Icon className="size-4 shrink-0" />
      {!collapsed ? <span className="truncate">{label}</span> : null}
      {!collapsed && hasChildren ? (
        <ChevronDown className={cn("ml-auto size-4 text-slate-500 transition-transform", expanded && "rotate-180")} />
      ) : null}
      {!available && !collapsed ? <span className="ml-auto text-[11px] text-slate-500">Soon</span> : null}
    </button>
  );
}

type SidebarChildItemProps = {
  child: AppNavChildItem;
  currentPath: string;
  onNavigate: (route: string) => void;
  onCloseMobile: () => void;
};

function SidebarChildItem({
  child,
  currentPath,
  onNavigate,
  onCloseMobile,
}: SidebarChildItemProps) {
  const active = child.available && currentPath === child.route;
  const ChildIcon = child.route === "/tradsphere/estnums" ? TRADSPHERE_ESTNUMS_CHILD_ICON : TRADSPHERE_HOME_CHILD_ICON;
  return (
    <button
      type="button"
      onClick={() => {
        if (!child.available) {
          return;
        }
        onNavigate(child.route);
        onCloseMobile();
      }}
      disabled={!child.available}
      aria-current={active ? "page" : undefined}
      className={cn(
        "flex w-full items-center gap-2 rounded-lg border px-3 py-1.5 text-left text-[13px] transition",
        active
          ? "border-blue-200 bg-blue-50 text-blue-800"
          : "border-transparent text-slate-600 hover:border-slate-200 hover:bg-slate-50",
        !child.available && "cursor-not-allowed opacity-55",
      )}
    >
      <ChildIcon className="size-3.5 shrink-0" />
      <span className="truncate">{child.label}</span>
      {!child.available ? <span className="ml-auto text-[11px] text-slate-500">Soon</span> : null}
    </button>
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
