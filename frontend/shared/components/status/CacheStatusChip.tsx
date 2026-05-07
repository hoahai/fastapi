import { useId } from "react";

import { cn } from "../utils/cn";

export type CacheStatusChipProps = {
  text: string;
  onRefresh: () => void;
  disabled?: boolean;
  refreshing?: boolean;
  refreshLabel: string;
  className?: string;
  containerClassName?: string;
  tooltipText?: string;
};

export function CacheStatusChip({
  text,
  onRefresh,
  disabled = false,
  refreshing = false,
  refreshLabel,
  className,
  containerClassName,
  tooltipText,
}: CacheStatusChipProps) {
  const tooltipId = useId();

  return (
    <div
      className={cn(
        "group relative inline-flex max-w-full pointer-events-auto",
        containerClassName,
      )}
    >
      <button
        type="button"
        onClick={onRefresh}
        disabled={disabled}
        aria-label={refreshLabel}
        aria-busy={refreshing}
        aria-describedby={tooltipId}
        className={cn(
          "inline-flex max-w-full items-center rounded-full border border-slate-200/90 bg-white/90 px-3 py-1.5 text-left shadow-sm backdrop-blur-sm transition-colors hover:border-slate-300 hover:bg-white disabled:cursor-not-allowed disabled:opacity-60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300 focus-visible:ring-offset-1",
          className,
        )}
      >
        <span className="truncate text-[11px] leading-4 text-slate-500">{text}</span>
      </button>
      <span
        id={tooltipId}
        role="tooltip"
        className="pointer-events-none absolute bottom-[calc(100%+0.45rem)] left-1/2 z-40 -translate-x-1/2 whitespace-nowrap rounded-md bg-slate-900/75 px-2 py-1 text-[11px] font-medium text-slate-50 opacity-0 shadow-sm backdrop-blur-[1px] transition-opacity group-hover:opacity-100 group-focus-within:opacity-100"
      >
        {tooltipText ?? refreshLabel}
      </span>
    </div>
  );
}
