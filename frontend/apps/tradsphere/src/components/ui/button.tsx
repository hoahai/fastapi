import type { ButtonHTMLAttributes } from "react";

import { cn } from "@/lib/utils";

type ButtonVariant = "default" | "secondary" | "outline" | "ghost";
type ButtonSize = "default" | "sm" | "icon";

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
}

const variantClasses: Record<ButtonVariant, string> = {
  default:
    "border border-primary/80 bg-primary text-primary-foreground shadow-soft hover:-translate-y-0.5 hover:bg-primary/95 hover:border-primary/90 active:translate-y-0",
  secondary:
    "border border-secondary bg-secondary text-secondary-foreground hover:-translate-y-0.5 hover:bg-secondary/90 active:translate-y-0",
  outline:
    "border border-border bg-white/96 text-foreground shadow-[0_10px_24px_-20px_rgba(37,99,235,0.55)] hover:-translate-y-0.5 hover:border-blue-200 hover:bg-blue-50/70 active:translate-y-0",
  ghost: "text-muted-foreground hover:bg-blue-50/70 hover:text-foreground",
};

const sizeClasses: Record<ButtonSize, string> = {
  default: "h-10 px-5 py-2",
  sm: "h-8 px-3 text-xs",
  icon: "h-8 w-8",
};

export function Button({
  className,
  variant = "default",
  size = "default",
  type = "button",
  ...props
}: ButtonProps) {
  return (
    <button
      type={type}
      className={cn(
        "inline-flex items-center justify-center gap-2 rounded-lg text-sm font-semibold tracking-[-0.01em] transition-all duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50",
        variantClasses[variant],
        sizeClasses[size],
        className,
      )}
      {...props}
    />
  );
}
