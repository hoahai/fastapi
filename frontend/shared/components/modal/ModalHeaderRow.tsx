import type { HTMLAttributes, ReactNode } from "react";

import { cn } from "../utils/cn";

export interface ModalHeaderRowProps extends HTMLAttributes<HTMLDivElement> {
  actions?: ReactNode;
  actionsClassName?: string;
}

export function ModalHeaderRow({
  className,
  actions,
  actionsClassName,
  children,
  ...props
}: ModalHeaderRowProps) {
  return (
    <div
      className={cn("flex min-h-11 items-start justify-between gap-3", className)}
      {...props}
    >
      <div className="min-w-0 flex-1">
        {children}
      </div>
      {actions ? (
        <div className={cn("shrink-0 pt-0.5", actionsClassName)}>
          {actions}
        </div>
      ) : null}
    </div>
  );
}
