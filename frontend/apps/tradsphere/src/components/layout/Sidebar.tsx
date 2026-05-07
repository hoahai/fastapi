import { ChevronLeft, ChevronRight, LayoutDashboard, Menu } from "lucide-react";
import type { ComponentType } from "react";

import { Button } from "@/components/ui/button";
import { APP_NAV_ITEMS, HOME_ROUTE } from "@/components/layout/navigation";
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
            route={HOME_ROUTE}
            collapsed={collapsed}
            currentPath={currentPath}
            available
            onNavigate={onNavigate}
            onCloseMobile={onCloseMobile}
          />
          {APP_NAV_ITEMS.map((app) => (
            <SidebarItem
              key={app.id}
              icon={app.icon}
              label={app.label}
              route={app.route}
              collapsed={collapsed}
              currentPath={currentPath}
              available={app.available}
              onNavigate={onNavigate}
              onCloseMobile={onCloseMobile}
            />
          ))}
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
  currentPath: string;
  available: boolean;
  onNavigate: (route: string) => void;
  onCloseMobile: () => void;
};

function SidebarItem({
  icon: Icon,
  label,
  route,
  collapsed,
  currentPath,
  available,
  onNavigate,
  onCloseMobile,
}: SidebarItemProps) {
  const isActive = available && currentPath === route;
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
      aria-current={isActive ? "page" : undefined}
      className={cn(
        "flex w-full items-center gap-3 rounded-xl border px-3 py-2 text-left text-sm transition",
        isActive
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
