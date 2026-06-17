import { useMemo, type ReactNode } from "react";

import { LeaveSpherePtoMonthSeparator } from "@leavesphere/components/LeaveSpherePtoMonthSeparator";
import { LeaveSpherePtoRequestCard } from "@leavesphere/components/LeaveSpherePtoRequestCard";
import { LeaveSpherePtoStatusChip } from "@leavesphere/components/PtoStatusChip";
import { LeaveSpherePtoTypeChip } from "@leavesphere/components/PtoTypeChip";
import { formatPtoRequestDateRangeLabel, groupLeaveSpherePtoRequestsByEndMonth } from "@leavesphere/lib/ptoDate";
import { formatLeaveSpherePtoStatusLabel, getLeaveSpherePtoRequestTimelineTone } from "@leavesphere/lib/ptoStatus";
import type { LeaveSpherePtoEmployeeDisplay } from "@leavesphere/lib/ptoEmployeeLookup";
import type { LeaveSpherePtoRequest } from "@leavesphere/lib/ptoTypes";

type LeaveSpherePtoRequestListProps = {
  requests: LeaveSpherePtoRequest[];
  emptyMessage: string;
  timeZone?: string | null;
  todayIsoDate: string;
  resolveEmployee: (request: LeaveSpherePtoRequest) => LeaveSpherePtoEmployeeDisplay;
  getTypeLabel: (request: LeaveSpherePtoRequest) => string;
  getHoursLabel: (request: LeaveSpherePtoRequest) => ReactNode;
  onRequestClick: (request: LeaveSpherePtoRequest) => void;
  className?: string;
};

export function LeaveSpherePtoRequestList({
  requests,
  emptyMessage,
  timeZone,
  todayIsoDate,
  resolveEmployee,
  getTypeLabel,
  getHoursLabel,
  onRequestClick,
  className,
}: LeaveSpherePtoRequestListProps) {
  const groupedRequests = useMemo(
    () => groupLeaveSpherePtoRequestsByEndMonth(requests, timeZone),
    [requests, timeZone],
  );

  return (
    <div className={`min-h-0 flex-1 space-y-4 overflow-y-auto pr-1 pt-1.5 ${className ?? ""}`.trim()}>
      {requests.length === 0 ? (
        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
          {emptyMessage}
        </div>
      ) : (
        groupedRequests.map((group) => (
          <section key={group.monthKey} className="space-y-3">
            <LeaveSpherePtoMonthSeparator label={group.monthLabel} />
            <div className="space-y-1.5">
              {group.requests.map((request) => {
                const employee = resolveEmployee(request);
                return (
                  <LeaveSpherePtoRequestCard
                    key={request.id}
                    onClick={() => onRequestClick(request)}
                    tone={getLeaveSpherePtoRequestTimelineTone({
                      status: request.status,
                      startDate: request.startDate,
                      endDate: request.endDate,
                      todayIsoDate,
                    })}
                    employeeName={employee.employeeName}
                    title={employee.employeeName}
                    pictureUrl={employee.pictureUrl}
                    typeChip={<LeaveSpherePtoTypeChip type={request.ptoTypeCode ?? request.type} label={getTypeLabel(request)} />}
                    statusChip={<LeaveSpherePtoStatusChip status={request.status} label={formatLeaveSpherePtoStatusLabel(request.status)} />}
                    dateLabel={formatPtoRequestDateRangeLabel(request.startDate, request.endDate, timeZone)}
                    detailLabel={request.description}
                    hoursLabel={getHoursLabel(request)}
                  />
                );
              })}
            </div>
          </section>
        ))
      )}
    </div>
  );
}
