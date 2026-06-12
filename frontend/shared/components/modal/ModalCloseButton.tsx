import type { ButtonHTMLAttributes, ReactNode } from "react";

import { cn } from "../utils/cn";

export interface ModalCloseButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  icon: ReactNode;
}

export function ModalCloseButton({
  icon,
  className,
  ...props
}: ModalCloseButtonProps) {
  const { title: _nativeTitle, ...buttonProps } = props;
  void _nativeTitle;

  return (
    <button
      {...buttonProps}
      type={buttonProps.type ?? "button"}
      aria-label={buttonProps["aria-label"] ?? "Close modal"}
      className={cn(
        "rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none",
        className,
      )}
    >
      {icon}
    </button>
  );
}
