import {
  type ButtonHTMLAttributes,
  type ReactNode,
  useRef,
  useState,
} from "react";

import { cn } from "../utils/cn";
import { Tooltip } from "./Tooltip";

export interface IconActionButtonProps
  extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children"> {
  icon: ReactNode;
  tooltip: string;
}

export function IconActionButton({
  icon,
  tooltip,
  className,
  ...props
}: IconActionButtonProps) {
  const { title: _nativeTitle, ...buttonProps } = props;
  void _nativeTitle;
  const anchorRef = useRef<HTMLSpanElement | null>(null);
  const [isTooltipOpen, setIsTooltipOpen] = useState(false);

  return (
    <>
      <span ref={anchorRef} className="inline-flex">
        <button
          {...buttonProps}
          type={buttonProps.type ?? "button"}
          aria-label={buttonProps["aria-label"] ?? tooltip}
          onMouseEnter={(event) => {
            buttonProps.onMouseEnter?.(event);
            if (!buttonProps.disabled) {
              setIsTooltipOpen(true);
            }
          }}
          onMouseLeave={(event) => {
            buttonProps.onMouseLeave?.(event);
            setIsTooltipOpen(false);
          }}
          onFocus={(event) => {
            buttonProps.onFocus?.(event);
            if (!buttonProps.disabled) {
              setIsTooltipOpen(true);
            }
          }}
          onBlur={(event) => {
            buttonProps.onBlur?.(event);
            setIsTooltipOpen(false);
          }}
          className={cn(
            "inline-flex items-center justify-center gap-2 rounded-md text-sm font-medium focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50",
            "h-10 w-10 cursor-pointer transform-gpu rounded-lg border border-transparent bg-transparent text-slate-500 transition-transform duration-200 ease-out will-change-transform hover:scale-105 focus-visible:scale-105 motion-reduce:transform-none",
            "hover:text-blue-600 focus-visible:text-blue-600",
            "[&_svg]:h-5 [&_svg]:w-5 [&_svg]:stroke-current [&_svg]:text-slate-500 [&_svg]:transition-transform [&_svg]:duration-200 [&_svg]:ease-out",
            "hover:[&_svg]:scale-115 hover:[&_svg]:text-blue-600 focus-visible:[&_svg]:scale-115 focus-visible:[&_svg]:text-blue-600",
            className,
          )}
        >
          {icon}
        </button>
      </span>
      <Tooltip open={isTooltipOpen} anchorRef={anchorRef} text={tooltip} />
    </>
  );
}
