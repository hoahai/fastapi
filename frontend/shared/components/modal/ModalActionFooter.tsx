import type { HTMLAttributes } from "react";

import { cn } from "../utils/cn";

export interface ModalActionFooterProps extends HTMLAttributes<HTMLDivElement> {}

export function ModalActionFooter({ className, ...props }: ModalActionFooterProps) {
  return (
    <div
      className={cn("flex flex-col-reverse gap-2 sm:flex-row sm:justify-end", className)}
      {...props}
    />
  );
}
