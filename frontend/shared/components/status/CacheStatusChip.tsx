import { TooltipTarget } from "../actions/TooltipTarget";
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
  compact?: boolean;
  wrapText?: boolean;
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
  compact = false,
  wrapText = false,
}: CacheStatusChipProps) {
  return (
    <div
      className={cn(
        "inline-flex max-w-full min-w-0 pointer-events-auto",
        containerClassName,
      )}
    >
      <TooltipTarget text={tooltipText ?? refreshLabel}>
        <button
          type="button"
          onClick={onRefresh}
          disabled={disabled}
          aria-label={refreshLabel}
          aria-busy={refreshing}
          className={cn(
            "inline-flex max-w-full min-w-0 items-center rounded-full border border-blue-100/95 bg-white/94 text-left shadow-[0_16px_34px_-24px_rgba(59,130,246,0.5)] backdrop-blur-sm transition-all hover:-translate-y-0.5 hover:border-blue-200 hover:bg-white disabled:cursor-not-allowed disabled:opacity-60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300 focus-visible:ring-offset-1",
            compact ? "px-3 py-1" : "px-4 py-1.5",
            className,
          )}
        >
          <span
            className={cn(
              "block min-w-0 font-medium text-slate-600",
              compact ? "text-[10px] leading-4" : "text-[11px] leading-4",
              wrapText ? "whitespace-normal break-words" : "truncate",
            )}
          >
            {text}
          </span>
        </button>
      </TooltipTarget>
    </div>
  );
}
