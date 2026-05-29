import type { ReactNode } from "react";

import { cn } from "../utils/cn";

export interface FormRowProps {
  label: ReactNode;
  children: ReactNode;
  alignStart?: boolean;
  labelClassName?: string;
  valueClassName?: string;
}

export function FormRow({
  label,
  children,
  alignStart = false,
  labelClassName,
  valueClassName,
}: FormRowProps) {
  return (
    <div
      className={cn(
        "grid grid-cols-1 gap-2.5 sm:grid-cols-[124px_minmax(0,1fr)] sm:gap-[1.125rem]",
        alignStart ? "items-start" : "items-center",
      )}
    >
      <p className={cn("text-sm font-semibold tracking-[-0.01em] text-slate-600", alignStart && "sm:pt-2", labelClassName)}>{label}</p>
      <div className={cn("min-w-0", valueClassName)}>{children}</div>
    </div>
  );
}
