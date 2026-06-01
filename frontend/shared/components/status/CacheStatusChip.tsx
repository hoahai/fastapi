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
  return (
    <div
      className={cn(
        "inline-flex max-w-full pointer-events-auto",
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
            "inline-flex max-w-full items-center rounded-full border border-blue-100/95 bg-white/94 px-4 py-1.5 text-left shadow-[0_16px_34px_-24px_rgba(59,130,246,0.5)] backdrop-blur-sm transition-all hover:-translate-y-0.5 hover:border-blue-200 hover:bg-white disabled:cursor-not-allowed disabled:opacity-60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300 focus-visible:ring-offset-1",
            className,
          )}
        >
          <span className="truncate text-[11px] font-medium leading-4 text-slate-600">{text}</span>
        </button>
      </TooltipTarget>
    </div>
  );
}
