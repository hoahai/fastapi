import type { HTMLAttributes, ReactNode } from "react";

import { cn } from "../utils/cn";

export interface SectionHeaderProps extends Omit<HTMLAttributes<HTMLDivElement>, "title"> {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  divider?: boolean;
}

export function SectionHeader({
  title,
  description,
  actions,
  divider = true,
  className,
  ...props
}: SectionHeaderProps) {
  return (
    <div
      className={cn(
        "flex min-h-10 items-center justify-between gap-3",
        divider && "border-b border-slate-200 pb-2",
        className,
      )}
      {...props}
    >
      <div className="min-w-0">
        <h3 className="text-sm font-semibold text-slate-800">{title}</h3>
        {description ? <p className="mt-1 text-xs text-slate-500">{description}</p> : null}
      </div>
      {actions ? <div className="flex flex-wrap items-center justify-end gap-1">{actions}</div> : null}
    </div>
  );
}
