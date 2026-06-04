import { type SelectHTMLAttributes } from "react";

import { cn } from "@/lib/utils";

export interface SelectOption {
  label: string;
  value: string;
}

interface SelectProps
  extends Omit<SelectHTMLAttributes<HTMLSelectElement>, "onChange"> {
  options: SelectOption[];
  onValueChange?: (value: string) => void;
}

export function Select({
  className,
  options,
  onValueChange,
  value,
  ...props
}: SelectProps) {
  return (
    <select
      className={cn(
        "flex h-10 w-full rounded-md border border-input bg-white px-3 py-2 text-sm text-foreground transition-[border-color,box-shadow,background-color] hover:border-blue-200 hover:bg-white focus-visible:outline-none focus-visible:border-blue-300 focus-visible:shadow-[inset_0_0_0_1px_rgba(59,130,246,0.12)] disabled:cursor-not-allowed disabled:opacity-50",
        className,
      )}
      value={value}
      onChange={(event) => onValueChange?.(event.target.value)}
      {...props}
    >
      {options.map((option) => (
        <option key={option.value} value={option.value}>
          {option.label}
        </option>
      ))}
    </select>
  );
}
