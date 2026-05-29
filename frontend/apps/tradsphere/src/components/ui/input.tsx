import { forwardRef, type InputHTMLAttributes } from "react";

import { cn } from "@/lib/utils";

export const Input = forwardRef<HTMLInputElement, InputHTMLAttributes<HTMLInputElement>>(
  ({ className, type = "text", ...props }, ref) => (
    <input
      ref={ref}
      type={type}
      className={cn(
        "flex h-10 w-full rounded-lg border border-input bg-white/96 px-3.5 py-2 text-sm text-slate-800 shadow-[0_8px_20px_-18px_rgba(30,64,175,0.45)] placeholder:text-muted-foreground/90 transition-[border-color,box-shadow,background-color] focus-visible:border-blue-300 focus-visible:bg-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1 disabled:cursor-not-allowed disabled:bg-slate-100/80 disabled:opacity-60",
        className,
      )}
      {...props}
    />
  ),
);

Input.displayName = "Input";
