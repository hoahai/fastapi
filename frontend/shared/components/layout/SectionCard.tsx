import { forwardRef, type HTMLAttributes, type ReactNode } from "react";

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

export const SectionCard = forwardRef<HTMLElement, SectionCardProps>(function SectionCard({
  title,
  description,
  actions,
  className,
  contentClassName,
  headerClassName,
  divider = true,
  children,
  ...props
}, ref) {
  return (
    <section
      ref={ref}
      className={cn(
        "relative flex min-h-0 flex-col overflow-hidden rounded-[1.45rem] border border-blue-100/90 bg-white/95 p-5 shadow-soft",
        className,
      )}
      {...props}
    >
      <div className="pointer-events-none absolute inset-x-5 top-0 h-px bg-gradient-to-r from-transparent via-blue-200/80 to-transparent" />
      <SectionHeader
        title={title}
        description={description}
        actions={actions}
        divider={divider}
        className={headerClassName}
      />
      <div className={cn("mt-5 min-h-0 flex-1", contentClassName)}>{children}</div>
    </section>
  );
});

SectionCard.displayName = "SectionCard";
