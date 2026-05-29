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
        "flex min-h-11 items-center justify-between gap-3",
        divider && "border-b border-blue-100/90 pb-3.5",
        className,
      )}
      {...props}
    >
      <div className="min-w-0">
        <h3 className="text-[15px] font-semibold tracking-[-0.014em] text-slate-800">{title}</h3>
        {description ? <p className="mt-1.5 text-xs leading-5 text-slate-500">{description}</p> : null}
      </div>
      {actions ? <div className="flex flex-wrap items-center justify-end gap-2">{actions}</div> : null}
    </div>
  );
}
