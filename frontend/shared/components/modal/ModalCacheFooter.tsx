import { useEffect, useRef, useState, type ReactNode } from "react";

import { type CacheStatusChipProps, CacheStatusChip } from "../status/CacheStatusChip";
import { cn } from "../utils/cn";

export interface ModalCacheFooterProps extends CacheStatusChipProps {
  className?: string;
  actions?: ReactNode;
  actionsClassName?: string;
}

export function ModalCacheFooter({
  className,
  containerClassName,
  actions,
  actionsClassName,
  ...chipProps
}: ModalCacheFooterProps) {
  const footerRef = useRef<HTMLElement | null>(null);
  const [isCompact, setIsCompact] = useState(false);

  useEffect(() => {
    const footer = footerRef.current;
    if (!footer) {
      return;
    }

    const threshold = 720;
    const updateLayout = () => {
      setIsCompact(footer.getBoundingClientRect().width < threshold);
    };

    updateLayout();

    if (typeof ResizeObserver === "undefined") {
      return;
    }

    const observer = new ResizeObserver(() => {
      updateLayout();
    });
    observer.observe(footer);

    return () => {
      observer.disconnect();
    };
  }, []);

  return (
    <footer ref={footerRef} className={cn("mt-6 shrink-0 border-t border-slate-100 bg-white px-0 pt-4 pb-[calc(1rem+env(safe-area-inset-bottom))]", className)}>
      <div className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
        <div className="min-w-0">
          <CacheStatusChip
            {...chipProps}
            compact={isCompact}
            wrapText={isCompact}
            containerClassName={cn("pointer-events-auto", containerClassName)}
          />
        </div>
        {actions ? (
          <div className={cn("flex shrink-0 items-center justify-end gap-2", actionsClassName)}>
            {actions}
          </div>
        ) : null}
      </div>
    </footer>
  );
}
