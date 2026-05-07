import { useEffect, useMemo, useRef, useState, type ButtonHTMLAttributes, type CSSProperties, type ReactNode } from "react";
import { createPortal } from "react-dom";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface ActionIconButtonProps extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children"> {
  icon: ReactNode;
  tooltip: string;
}

export function ActionIconButton({ icon, tooltip, className, ...props }: ActionIconButtonProps) {
  const anchorRef = useRef<HTMLSpanElement | null>(null);
  const [isTooltipOpen, setIsTooltipOpen] = useState(false);
  const [tooltipStyle, setTooltipStyle] = useState<CSSProperties>({
    left: 0,
    top: 0,
    visibility: "hidden",
  });

  useEffect(() => {
    if (!isTooltipOpen) {
      return;
    }

    const updatePosition = () => {
      const anchor = anchorRef.current;
      if (!anchor) {
        return;
      }
      const rect = anchor.getBoundingClientRect();
      setTooltipStyle({
        left: rect.left + rect.width / 2,
        top: rect.top - 6,
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
  }, [isTooltipOpen]);

  const tooltipNode = useMemo(() => {
    if (!isTooltipOpen || typeof document === "undefined") {
      return null;
    }
    return createPortal(
      <span
        role="tooltip"
        className="pointer-events-none fixed z-[120] -translate-x-1/2 -translate-y-full whitespace-nowrap rounded-md bg-slate-900 px-2 py-1 text-xs font-medium text-white opacity-100 shadow-sm"
        style={tooltipStyle}
      >
        {tooltip}
      </span>,
      document.body,
    );
  }, [isTooltipOpen, tooltip, tooltipStyle]);

  return (
    <>
      <span ref={anchorRef} className="inline-flex">
        <Button
          {...props}
          variant="ghost"
          size="icon"
          title={tooltip}
          aria-label={props["aria-label"] ?? tooltip}
          onMouseEnter={(event) => {
            props.onMouseEnter?.(event);
            if (!props.disabled) {
              setIsTooltipOpen(true);
            }
          }}
          onMouseLeave={(event) => {
            props.onMouseLeave?.(event);
            setIsTooltipOpen(false);
          }}
          onFocus={(event) => {
            props.onFocus?.(event);
            if (!props.disabled) {
              setIsTooltipOpen(true);
            }
          }}
          onBlur={(event) => {
            props.onBlur?.(event);
            setIsTooltipOpen(false);
          }}
          className={cn(
            "h-10 w-10 cursor-pointer transform-gpu bg-transparent text-slate-500 transition-transform duration-200 ease-out will-change-transform hover:scale-110 focus-visible:scale-110 motion-reduce:transform-none hover:!bg-transparent hover:text-blue-600 focus-visible:text-blue-600 focus-visible:ring-2 focus-visible:ring-ring [&_svg]:h-5 [&_svg]:w-5 [&_svg]:stroke-current [&_svg]:text-slate-500 [&_svg]:transition-transform [&_svg]:duration-200 [&_svg]:ease-out hover:[&_svg]:scale-125 hover:[&_svg]:text-blue-600 focus-visible:[&_svg]:scale-125 focus-visible:[&_svg]:text-blue-600",
            className,
          )}
        >
          {icon}
        </Button>
      </span>
      {tooltipNode}
    </>
  );
}
