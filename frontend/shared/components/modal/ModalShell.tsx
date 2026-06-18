import type { HTMLAttributes, ReactNode } from "react";

import { SectionLoadingLayer } from "../status/LoadingOverlay";
import { cn } from "../utils/cn";

export interface ModalShellProps extends HTMLAttributes<HTMLDivElement> {
  busy?: boolean;
  busyMessage?: string;
  children?: ReactNode;
  closeButton?: ReactNode;
}

export function ModalShell({
  className,
  busy = false,
  busyMessage = "Saving...",
  children,
  closeButton,
  ...props
}: ModalShellProps) {
  return (
    <div className={cn("relative flex min-h-0 flex-1 flex-col", className)} {...props}>
      {closeButton ? (
        <div className="absolute right-0 top-1 z-20">
          {closeButton}
        </div>
      ) : null}
      <div className={cn("flex min-h-0 flex-1 flex-col transition-opacity duration-150", busy && "opacity-60")}>
        {children}
      </div>
      {busy ? <SectionLoadingLayer active message={busyMessage} className="z-10 rounded-xl" /> : null}
    </div>
  );
}
