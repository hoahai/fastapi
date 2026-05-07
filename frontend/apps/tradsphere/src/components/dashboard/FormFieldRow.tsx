import type { ReactNode } from "react";

interface LabeledFieldProps {
  label: ReactNode;
  children: ReactNode;
  alignStart?: boolean;
  labelClassName?: string;
  valueClassName?: string;
}

export function LabeledField({
  label,
  children,
  alignStart = false,
  labelClassName,
  valueClassName,
}: LabeledFieldProps) {
  return (
    <div
      className={`grid grid-cols-1 gap-2 sm:grid-cols-[110px_minmax(0,1fr)] sm:gap-4 ${
        alignStart ? "items-start" : "items-center"
      }`}
    >
      <p className={`text-sm text-slate-600 ${alignStart ? "sm:pt-2" : ""} ${labelClassName ?? ""}`}>{label}</p>
      <div className={`min-w-0 ${valueClassName ?? ""}`}>{children}</div>
    </div>
  );
}

export function ReadOnlyValue({ value }: { value?: string }) {
  const displayValue = value?.trim() ? value : "-";
  return <div className="min-h-10 px-3 py-2 text-sm text-slate-800 cursor-default select-text">{displayValue}</div>;
}
