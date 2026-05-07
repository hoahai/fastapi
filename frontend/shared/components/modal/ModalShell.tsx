import type { HTMLAttributes } from "react";

import { cn } from "../utils/cn";

export interface ModalShellProps extends HTMLAttributes<HTMLDivElement> {}

export function ModalShell({ className, ...props }: ModalShellProps) {
  return (
    <div
      className={cn(
        "flex max-h-[90vh] flex-col overflow-hidden rounded-xl bg-white",
        className,
      )}
      {...props}
    />
  );
}
