import { type CSSProperties, type RefObject, useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";

import { cn } from "../utils/cn";

export interface TooltipProps {
  open: boolean;
  anchorRef: RefObject<HTMLElement | null>;
  text: string;
  offsetY?: number;
  className?: string;
}

export function Tooltip({
  open,
  anchorRef,
  text,
  offsetY = 6,
  className,
}: TooltipProps) {
  const [style, setStyle] = useState<CSSProperties>({
    left: 0,
    top: 0,
    visibility: "hidden",
  });

  useEffect(() => {
    if (!open) {
      return;
    }

    const updatePosition = () => {
      const anchor = anchorRef.current;
      if (!anchor) {
        return;
      }
      const rect = anchor.getBoundingClientRect();
      setStyle({
        left: rect.left + rect.width / 2,
        top: rect.top - offsetY,
        visibility: "visible",
      });
    };

    updatePosition();
    window.addEventListener("resize", updatePosition);
    window.addEventListener("scroll", updatePosition, true);
    return () => {
      window.removeEventListener("resize", updatePosition);
      window.removeEventListener("scroll", updatePosition, true);
    };
  }, [anchorRef, offsetY, open]);

  const tooltipNode = useMemo(() => {
    if (!open || typeof document === "undefined") {
      return null;
    }
    return createPortal(
      <span
        role="tooltip"
        className={cn(
          "pointer-events-none fixed z-[120] -translate-x-1/2 -translate-y-full whitespace-nowrap rounded-md bg-slate-900/75 px-2 py-1 text-xs font-medium text-slate-50 shadow-sm backdrop-blur-[1px]",
          className,
        )}
        style={style}
      >
        {text}
      </span>,
      document.body,
    );
  }, [className, open, style, text]);

  return tooltipNode;
}
