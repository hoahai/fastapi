import type { HTMLAttributes, ReactNode } from "react";

import { cn } from "../utils/cn";
import { SectionHeader } from "./SectionHeader";

export interface SectionCardProps extends Omit<HTMLAttributes<HTMLElement>, "title"> {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  contentClassName?: string;
  headerClassName?: string;
  divider?: boolean;
}

export function SectionCard({
  title,
  description,
  actions,
  className,
  contentClassName,
  headerClassName,
  divider = true,
  children,
  ...props
}: SectionCardProps) {
  return (
    <section
      className={cn(
        "flex min-h-0 flex-col rounded-2xl border border-blue-100 bg-white/95 p-4 shadow-soft",
        className,
      )}
      {...props}
    >
      <SectionHeader
        title={title}
        description={description}
        actions={actions}
        divider={divider}
        className={headerClassName}
      />
      <div className={cn("mt-3 min-h-0 flex-1", contentClassName)}>{children}</div>
    </section>
  );
}
