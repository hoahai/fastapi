import type { ReactNode } from "react";

import { LeaveSpherePtoEmployeeHeader } from "@leavesphere/components/LeaveSpherePtoEmployeeHeader";
import { getLeaveSpherePtoRequestSurfaceClassName, type LeaveSpherePtoRequestTimelineTone } from "@leavesphere/lib/ptoStatus";

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
  tone?: LeaveSpherePtoRequestTimelineTone;
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
  tone = "future",
  className,
}: LeaveSpherePtoRequestCardProps) {
  return (
    <button
      type="button"
      className={`group relative w-full rounded-2xl border px-3.5 py-2.5 text-left shadow-sm transition-all duration-200 hover:z-10 hover:-translate-y-0.5 hover:scale-[0.99] focus-visible:outline-none focus-visible:ring-2 ${getLeaveSpherePtoRequestSurfaceClassName(tone)} ${className ?? ""}`.trim()}
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
