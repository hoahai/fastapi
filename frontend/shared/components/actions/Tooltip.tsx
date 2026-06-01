import { type CSSProperties, type RefObject, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

import { cn } from "../utils/cn";

export type TooltipPlacement = "top" | "bottom" | "auto";

export interface TooltipProps {
  open: boolean;
  anchorRef: RefObject<HTMLElement | null>;
  text: string;
  offsetY?: number;
  placement?: TooltipPlacement;
  viewportPadding?: number;
  followCursor?: boolean;
  cursorOffsetX?: number;
  cursorOffsetY?: number;
  className?: string;
}

export function Tooltip({
  open,
  anchorRef,
  text,
  offsetY = 8,
  placement = "auto",
  viewportPadding = 8,
  followCursor = true,
  cursorOffsetX = 14,
  cursorOffsetY = 12,
  className,
}: TooltipProps) {
  const tooltipRef = useRef<HTMLSpanElement | null>(null);
  const lastPointerRef = useRef<{ x: number; y: number } | null>(null);
  const [resolvedPlacement, setResolvedPlacement] = useState<Exclude<TooltipPlacement, "auto">>("top");
  const [style, setStyle] = useState<CSSProperties>({
    left: 0,
    top: 0,
    visibility: "hidden",
  });

  useEffect(() => {
    if (!open) {
      return;
    }
    const anchor = anchorRef.current;
    if (!anchor) {
      return;
    }

    const updatePosition = (pointer?: { x: number; y: number }) => {
      const tooltipNode = tooltipRef.current;
      const rect = anchor.getBoundingClientRect();
      const tooltipRect = tooltipNode?.getBoundingClientRect();
      const tooltipWidth = tooltipRect?.width ?? 0;
      const tooltipHeight = tooltipRect?.height ?? 0;

      let nextPlacement: Exclude<TooltipPlacement, "auto">;
      const referenceTop = followCursor && pointer
        ? pointer.y
        : rect.top;
      const referenceBottom = followCursor && pointer
        ? pointer.y
        : rect.bottom;
      const topReferenceOffset = followCursor ? cursorOffsetY : offsetY;
      const bottomReferenceOffset = followCursor ? cursorOffsetY : offsetY;

      if (placement === "auto") {
        const topRequiredSpace = tooltipHeight + topReferenceOffset + viewportPadding;
        const hasTopSpace = referenceTop >= topRequiredSpace;
        const bottomSpace = window.innerHeight - referenceBottom;
        nextPlacement = hasTopSpace || referenceTop >= bottomSpace ? "top" : "bottom";
      } else {
        nextPlacement = placement;
      }

      const anchorCenterX = followCursor && pointer
        ? pointer.x + cursorOffsetX
        : rect.left + rect.width / 2;
      const minLeft = viewportPadding + tooltipWidth / 2;
      const maxLeft = window.innerWidth - viewportPadding - tooltipWidth / 2;
      const clampedLeft = tooltipWidth > 0
        ? Math.min(Math.max(anchorCenterX, minLeft), Math.max(minLeft, maxLeft))
        : anchorCenterX;
      const top = nextPlacement === "top"
        ? referenceTop - topReferenceOffset
        : referenceBottom + bottomReferenceOffset;

      setResolvedPlacement(nextPlacement);
      setStyle({
        left: clampedLeft,
        top,
        visibility: "visible",
      });
    };

    const updateWithLastPointer = () => {
      updatePosition(lastPointerRef.current ?? undefined);
    };
    const handleMouseMove = (event: MouseEvent) => {
      lastPointerRef.current = { x: event.clientX, y: event.clientY };
      updatePosition(lastPointerRef.current);
    };

    updateWithLastPointer();
    const frameId = window.requestAnimationFrame(updateWithLastPointer);
    window.addEventListener("resize", updateWithLastPointer);
    window.addEventListener("scroll", updateWithLastPointer, true);
    if (followCursor) {
      anchor.addEventListener("mousemove", handleMouseMove);
    }
    return () => {
      window.cancelAnimationFrame(frameId);
      window.removeEventListener("resize", updateWithLastPointer);
      window.removeEventListener("scroll", updateWithLastPointer, true);
      if (followCursor) {
        anchor.removeEventListener("mousemove", handleMouseMove);
      }
    };
  }, [anchorRef, cursorOffsetX, cursorOffsetY, followCursor, offsetY, open, placement, viewportPadding]);

  const tooltipNode = useMemo(() => {
    if (!open || typeof document === "undefined") {
      return null;
    }
    return createPortal(
      <span
        ref={tooltipRef}
        role="tooltip"
        className={cn(
          "pointer-events-none fixed z-[120] max-w-[16rem] -translate-x-1/2 whitespace-normal break-words rounded-lg bg-slate-900/75 px-2.5 py-1.5 text-[11px] font-medium leading-snug text-slate-50 shadow-md backdrop-blur-sm",
          resolvedPlacement === "top" ? "-translate-y-full" : "translate-y-0",
          className,
        )}
        style={style}
      >
        {text}
      </span>,
      document.body,
    );
  }, [className, open, resolvedPlacement, style, text]);

  return tooltipNode;
}
