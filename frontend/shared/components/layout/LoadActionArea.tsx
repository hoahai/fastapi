import type { ReactNode } from "react";

import { cn } from "../utils/cn";

export interface LoadActionAreaProps {
  controls: ReactNode;
  actions: ReactNode;
  className?: string;
  controlsClassName?: string;
  actionsClassName?: string;
}

export function LoadActionArea({
  controls,
  actions,
  className,
  controlsClassName,
  actionsClassName,
}: LoadActionAreaProps) {
  return (
    <div className={cn("grid gap-3 md:grid-cols-[minmax(0,260px)_auto] md:items-end", className)}>
      <div className={controlsClassName}>{controls}</div>
      <div className={cn("flex flex-wrap items-center gap-2", actionsClassName)}>{actions}</div>
    </div>
  );
}
