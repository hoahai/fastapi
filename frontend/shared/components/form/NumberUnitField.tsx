import { forwardRef, type ChangeEvent, type FocusEvent, type InputHTMLAttributes } from "react";

import { cn } from "../utils/cn";

type UnitPosition = "left" | "right";

export type NumberUnitFieldProps = Omit<
  InputHTMLAttributes<HTMLInputElement>,
  "className" | "onBlur" | "onChange" | "type" | "value"
> & {
  value: string;
  unit: string;
  unitPosition?: UnitPosition;
  onValueChange: (value: string) => void;
  onBlurValue?: (value: string) => void;
  shellClassName?: string;
  inputClassName?: string;
  unitClassName?: string;
};

const inputBaseClassName =
  "flex h-10 w-full rounded-lg border border-input bg-white/96 px-3.5 py-2 text-sm text-slate-800 shadow-[0_8px_20px_-18px_rgba(30,64,175,0.45)] placeholder:text-muted-foreground/90 transition-[border-color,box-shadow,background-color] hover:border-blue-200 hover:bg-white focus-visible:border-blue-300 focus-visible:bg-white focus-visible:outline-none focus-visible:shadow-[inset_0_0_0_1px_rgba(59,130,246,0.12)] disabled:cursor-not-allowed disabled:bg-slate-100/80 disabled:opacity-60";

function sanitizeDecimalInputValue(value: string): string {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) {
    return "";
  }
  const hasLeadingMinus = trimmed.startsWith("-");
  let next = trimmed.replace(/[^\d.-]/g, "");
  next = next.replace(/-/g, "");
  const firstDot = next.indexOf(".");
  if (firstDot >= 0) {
    next = `${next.slice(0, firstDot + 1)}${next.slice(firstDot + 1).replace(/\./g, "")}`;
  }
  return hasLeadingMinus ? `-${next}` : next;
}

function normalizeDecimalInputValue(value: string): string {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) {
    return "";
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed.toFixed(2) : trimmed;
}

export const NumberUnitField = forwardRef<HTMLInputElement, NumberUnitFieldProps>(
  (
    {
      value,
      unit,
      unitPosition = "left",
      onValueChange,
      onBlurValue,
      shellClassName,
      inputClassName,
      unitClassName,
      disabled,
      placeholder = "0.00",
      ...props
    },
    ref,
  ) => {
    const shellClasses = cn(
      "flex h-10 w-full items-stretch overflow-hidden rounded-lg border border-input bg-white/96 shadow-[0_8px_20px_-18px_rgba(30,64,175,0.45)] transition-[border-color,box-shadow,background-color] hover:border-blue-200 hover:bg-white focus-within:border-blue-300 focus-within:bg-white focus-within:shadow-[inset_0_0_0_1px_rgba(59,130,246,0.12)]",
      disabled && "cursor-not-allowed bg-slate-100/80 opacity-60",
      shellClassName,
    );
    const inputClasses = cn(
      "h-full flex-1 rounded-none border-0 bg-transparent px-4 shadow-none hover:border-0 hover:bg-transparent focus-visible:border-0 focus-visible:bg-transparent focus-visible:shadow-none",
      inputClassName,
    );
    const unitClasses = cn("pointer-events-none flex shrink-0 items-center px-3 text-sm text-slate-500", unitClassName);

    function handleChange(event: ChangeEvent<HTMLInputElement>) {
      onValueChange(sanitizeDecimalInputValue(event.currentTarget.value));
    }

    function handleBlur(event: FocusEvent<HTMLInputElement>) {
      const nextValue = normalizeDecimalInputValue(event.currentTarget.value);
      onValueChange(nextValue);
      onBlurValue?.(nextValue);
    }

    const input = (
      <input
        ref={ref}
        type="number"
        step="0.01"
        inputMode="decimal"
        value={value}
        onChange={handleChange}
        onBlur={handleBlur}
        disabled={disabled}
        placeholder={placeholder}
        className={cn(inputBaseClassName, inputClasses)}
        {...props}
      />
    );

    return (
      <div className={shellClasses}>
        {unitPosition === "left" ? <span className={unitClasses}>{unit}</span> : null}
        {input}
        {unitPosition === "right" ? <span className={unitClasses}>{unit}</span> : null}
      </div>
    );
  },
);

NumberUnitField.displayName = "NumberUnitField";
