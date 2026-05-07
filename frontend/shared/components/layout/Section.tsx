import type { HTMLAttributes } from "react";

import { cn } from "../utils/cn";

type SectionElement = "section" | "article" | "div";

export interface SectionProps extends HTMLAttributes<HTMLElement> {
  as?: SectionElement;
  compact?: boolean;
}

export function Section({
  as = "section",
  compact = false,
  className,
  children,
  ...props
}: SectionProps) {
  const Tag = as;
  return (
    <Tag
      className={cn("min-w-0", compact ? "space-y-2" : "space-y-3", className)}
      {...props}
    >
      {children}
    </Tag>
  );
}
