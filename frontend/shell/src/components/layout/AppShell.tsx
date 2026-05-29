import { type ReactNode, useEffect, useState } from "react";

import { Sidebar } from "@shell/components/layout/Sidebar";
import { cn } from "@shared/components/utils/cn";

type AppShellProps = {
  currentPath: string;
  onNavigate: (route: string) => void;
  children: ReactNode;
};

const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";

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

export function AppShell({ currentPath, onNavigate, children }: AppShellProps) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(() => readSidebarCollapsedState());
  const [sidebarHoverExpanded, setSidebarHoverExpanded] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const sidebarVisuallyExpanded = !sidebarCollapsed || sidebarHoverExpanded;

  useEffect(() => {
    try {
      window.localStorage.setItem(SIDEBAR_COLLAPSED_STORAGE_KEY, sidebarCollapsed ? "1" : "0");
      window.localStorage.removeItem(LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY);
    } catch {
      // Ignore storage write failures.
    }
  }, [sidebarCollapsed]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.dispatchEvent(
      new CustomEvent<{ collapsed: boolean; hoverExpanded: boolean; visuallyExpanded: boolean }>(
        SIDEBAR_COLLAPSED_EVENT,
        {
          detail: {
            collapsed: sidebarCollapsed,
            hoverExpanded: sidebarHoverExpanded,
            visuallyExpanded: sidebarVisuallyExpanded,
          },
        },
      ),
    );
  }, [sidebarCollapsed, sidebarHoverExpanded, sidebarVisuallyExpanded]);

  useEffect(() => {
    if (!sidebarCollapsed) {
      setSidebarHoverExpanded(false);
    }
  }, [sidebarCollapsed]);

  useEffect(() => {
    setMobileOpen(false);
  }, [currentPath]);

  return (
    <div className="relative min-h-screen overflow-hidden bg-app-gradient text-foreground">
      <div className="pointer-events-none absolute -left-28 -top-12 size-[25rem] rounded-full bg-blue-300/38 blur-[110px]" />
      <div className="pointer-events-none absolute right-[-3rem] top-16 size-[24rem] rounded-full bg-indigo-300/30 blur-[110px]" />
      <div className="pointer-events-none absolute left-[42%] top-2 size-[20rem] rounded-full bg-sky-200/32 blur-[100px]" />

      <div className="relative min-h-screen w-full px-4 py-5 sm:px-6 lg:px-8 lg:py-6">
        <Sidebar
          currentPath={currentPath}
          collapsed={sidebarCollapsed}
          visuallyExpanded={sidebarVisuallyExpanded}
          onToggleCollapsed={() => {
            setSidebarHoverExpanded(false);
            setSidebarCollapsed((value) => !value);
          }}
          onHoverExpandedChange={(nextValue) => {
            if (!sidebarCollapsed) {
              return;
            }
            setSidebarHoverExpanded(nextValue);
          }}
          onNavigate={onNavigate}
          mobileOpen={mobileOpen}
          onToggleMobile={() => setMobileOpen((value) => !value)}
          onCloseMobile={() => setMobileOpen(false)}
        />

        <main
          className={cn(
            "relative space-y-7 pb-7 transition-[margin] duration-200",
            "lg:mr-6",
            sidebarVisuallyExpanded ? "lg:ml-[18.75rem]" : "lg:ml-[6.5rem]",
          )}
        >
          {children}
        </main>
      </div>
    </div>
  );
}
