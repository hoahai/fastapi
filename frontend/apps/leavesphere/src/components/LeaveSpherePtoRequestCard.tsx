import type { ReactNode } from "react";

import { LeaveSpherePtoEmployeeHeader } from "@leavesphere/components/LeaveSpherePtoEmployeeHeader";

type LeaveSpherePtoRequestCardProps = {
  employeeName: string;
  title: ReactNode;
  dateLabel: ReactNode;
  detailLabel?: ReactNode;
  hoursLabel: ReactNode;
  submittedLabel: ReactNode;
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
  submittedLabel,
  typeChip,
  statusChip,
  pictureUrl,
  onClick,
  className,
}: LeaveSpherePtoRequestCardProps) {
  return (
    <button
      type="button"
      className={`w-full rounded-xl border border-blue-100 bg-white px-3.5 py-3 text-left transition-colors hover:border-blue-300 hover:bg-blue-50/35 ${className ?? ""}`.trim()}
      onClick={onClick}
    >
      <div className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-start gap-x-2 gap-y-1">
        <LeaveSpherePtoEmployeeHeader
          employeeName={employeeName}
          pictureUrl={pictureUrl}
          title={title}
          titleClassName="pt-0.5 font-semibold"
        />
        {(typeChip || statusChip) ? (
          <div className="flex shrink-0 items-center gap-1.5">
            {typeChip}
            {statusChip}
          </div>
        ) : null}
        <p className="col-span-3 pl-0 text-xs text-slate-600">{dateLabel}</p>
      </div>
      {detailLabel ? <p className="mt-2 truncate pl-0 text-xs text-slate-700">{detailLabel}</p> : null}
      <div className="mt-2 flex items-center justify-between text-[11px] text-slate-500">
        <span>{hoursLabel}</span>
        <span>{submittedLabel}</span>
      </div>
    </button>
  );
}
