import type { ReactNode } from "react";

import { LeaveSpherePtoEmployeeHeader } from "@leavesphere/components/LeaveSpherePtoEmployeeHeader";
import type { LeaveSpherePtoRequestCardTone } from "@leavesphere/lib/ptoDate";

const CARD_TONE_CLASS_NAMES: Record<LeaveSpherePtoRequestCardTone, string> = {
  past: "border-slate-300 bg-slate-100 hover:border-slate-400 hover:bg-slate-200/70 focus-visible:ring-slate-300",
  current: "border-emerald-300 bg-emerald-50 hover:border-emerald-400 hover:bg-emerald-100/80 focus-visible:ring-emerald-300",
  future: "border-slate-200 bg-white hover:border-blue-200 hover:bg-blue-50/55 focus-visible:ring-blue-300",
};

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
  tone?: LeaveSpherePtoRequestCardTone;
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
      className={`group w-full rounded-2xl border px-3.5 py-2.5 text-left shadow-sm transition-all duration-200 hover:-translate-y-0.5 focus-visible:outline-none focus-visible:ring-2 ${CARD_TONE_CLASS_NAMES[tone]} ${className ?? ""}`.trim()}
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
