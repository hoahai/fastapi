import type { ReactNode } from "react";

type LeaveSpherePtoMonthSeparatorProps = {
  label: ReactNode;
};

export function LeaveSpherePtoMonthSeparator({ label }: LeaveSpherePtoMonthSeparatorProps) {
  return (
    <div className="flex items-center py-[0.3rem] pb-[0.1rem]">
      <span className="h-px flex-1 bg-slate-200/80" />
      <p className="shrink-0 px-4 text-center text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-500">
        {label}
      </p>
      <span className="h-px flex-1 bg-slate-200/80" />
    </div>
  );
}
