import { cn } from "../utils/cn";

export interface ReadOnlyFieldProps {
  value?: string | null;
  className?: string;
}

export function ReadOnlyField({ value, className }: ReadOnlyFieldProps) {
  const displayValue = value?.trim() ? value : "-";
  return (
    <div
      className={cn(
        "min-h-10 cursor-default select-text rounded-lg border border-transparent bg-slate-50/85 px-3 py-2 text-sm text-slate-800",
        className,
      )}
    >
      {displayValue}
    </div>
  );
}
