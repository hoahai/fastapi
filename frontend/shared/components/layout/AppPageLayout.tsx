import { useEffect, useState, type ReactNode } from "react";

import { cn } from "../utils/cn";

export interface AppPageLayoutProps {
  pageMessages?: ReactNode;
  banner: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  className?: string;
  contentClassName?: string;
}

const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";
const PAGE_FOOTER_SPACER_CLASSNAME = "h-[calc(4.75rem+env(safe-area-inset-bottom))]";

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

export function AppPageLayout({
  pageMessages,
  banner,
  children,
  footer,
  className,
  contentClassName,
}: AppPageLayoutProps) {
  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());

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

  return (
    <div
      className={cn(
        "mx-auto flex w-full max-w-[1600px] flex-1 flex-col gap-5 pb-6 xl:min-h-[calc(100dvh-3.5rem)]",
        className,
      )}
    >
      {banner}
      {pageMessages}
      <div className={cn("flex min-h-0 flex-1 flex-col gap-5", contentClassName)}>
        {children}
        {footer ? <div aria-hidden className={cn("shrink-0", PAGE_FOOTER_SPACER_CLASSNAME)} /> : null}
      </div>
      {footer ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-[calc(0.75rem+env(safe-area-inset-bottom))] z-10">
          <div className={`mx-4 sm:mx-6 lg:mr-6 ${sidebarVisuallyExpanded ? "lg:ml-[18.75rem]" : "lg:ml-[6.5rem]"}`}>
            <div className="mx-auto w-full max-w-[1600px]">
              {footer}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
