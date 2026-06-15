import type { ReactNode } from "react";

import { LeaveSpherePtoEmployeeHeader } from "@leavesphere/components/LeaveSpherePtoEmployeeHeader";

type LeaveSpherePtoRequestCardProps = {
  employeeName: string;
  title: ReactNode;
  dateLabel: ReactNode;
  detailLabel?: ReactNode;
  hoursLabel: ReactNode;
  typeChip?: ReactNode;
  statusChip?: ReactNode;
  pictureUrl?: string | null;
  onClick: () => void;
  className?: string;
};

export function LeaveSpherePtoRequestCard({
  employeeName,
  title,
  dateLabel,
  detailLabel,
  hoursLabel,
  typeChip,
  statusChip,
  pictureUrl,
  onClick,
  className,
}: LeaveSpherePtoRequestCardProps) {
  return (
    <button
      type="button"
      className={`group w-full rounded-2xl border border-slate-200 bg-gradient-to-br from-white to-slate-50/80 px-3.5 py-2.5 text-left shadow-sm transition-all duration-200 hover:-translate-y-0.5 hover:border-blue-200 hover:bg-blue-50/50 hover:shadow-[0_14px_30px_-20px_rgba(37,99,235,0.45)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300 ${className ?? ""}`.trim()}
      onClick={onClick}
    >
      <div className="flex items-start gap-2">
        <LeaveSpherePtoEmployeeHeader
          employeeName={employeeName}
          pictureUrl={pictureUrl}
          title={title}
          className="min-w-0 flex-1"
          titleClassName="truncate pt-0.5 font-semibold"
        />
        {(typeChip || statusChip) ? (
          <div className="flex shrink-0 items-center gap-1.5 self-start">
            {typeChip}
            {statusChip}
          </div>
        ) : null}
      </div>
      <p className="mt-0.5 pl-0 text-xs text-slate-600">{dateLabel}</p>
      <div className="mt-1.5 flex min-w-0 items-center gap-1 text-[11px] text-slate-700">
        <span className="shrink-0">{hoursLabel}</span>
        {detailLabel ? (
          <>
            <span className="shrink-0">-</span>
            <span className="min-w-0 flex-1 truncate">{detailLabel}</span>
          </>
        ) : null}
      </div>
    </button>
  );
}
