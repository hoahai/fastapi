import type { ReactNode } from "react";

type LeaveSpherePtoRequestCardProps = {
  title: ReactNode;
  dateLabel: ReactNode;
  detailLabel?: ReactNode;
  hoursLabel: ReactNode;
  submittedLabel: ReactNode;
  statusChip?: ReactNode;
  onClick: () => void;
  className?: string;
};

export function LeaveSpherePtoRequestCard({
  title,
  dateLabel,
  detailLabel,
  hoursLabel,
  submittedLabel,
  statusChip,
  onClick,
  className,
}: LeaveSpherePtoRequestCardProps) {
  return (
    <button
      type="button"
      className={`w-full rounded-xl border border-blue-100 bg-white px-3.5 py-3 text-left transition-colors hover:border-blue-300 hover:bg-blue-50/35 ${className ?? ""}`.trim()}
      onClick={onClick}
    >
      <div className="grid grid-cols-[minmax(0,1fr)_auto] items-start gap-x-2 gap-y-1">
        <p className="min-w-0 text-sm font-semibold text-slate-900">{title}</p>
        {statusChip ? <div className="shrink-0">{statusChip}</div> : null}
        <p className="col-span-2 text-xs text-slate-600">{dateLabel}</p>
      </div>
      {detailLabel ? <p className="mt-2 text-xs text-slate-700">{detailLabel}</p> : null}
      <div className="mt-2 flex items-center justify-between text-[11px] text-slate-500">
        <span>{hoursLabel}</span>
        <span>{submittedLabel}</span>
      </div>
    </button>
  );
}
