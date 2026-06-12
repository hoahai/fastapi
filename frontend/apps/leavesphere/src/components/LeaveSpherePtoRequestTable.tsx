import type { ReactNode } from "react";

import { LeaveSpherePtoEmployeeHeader } from "@leavesphere/components/LeaveSpherePtoEmployeeHeader";
import { LeaveSpherePtoTypeChip } from "@leavesphere/components/PtoTypeChip";
import { LeaveSpherePtoStatusChip } from "@leavesphere/components/PtoStatusChip";
import type { LeaveSpherePtoEmployeeDisplay } from "@leavesphere/lib/ptoEmployeeLookup";
import type { LeaveSpherePtoRequest, LeaveSpherePtoStatus } from "@leavesphere/lib/ptoMocks";

type LeaveSpherePtoRequestTableProps = {
  requests: LeaveSpherePtoRequest[];
  emptyMessage: string;
  resolveEmployee: (request: LeaveSpherePtoRequest) => LeaveSpherePtoEmployeeDisplay;
  requestTypeLabel: (type: string) => string;
  statusLabel: (status: LeaveSpherePtoStatus) => string;
  formatSubmittedLabel: (request: LeaveSpherePtoRequest) => string;
  formatDateRangeLabel: (request: LeaveSpherePtoRequest) => string;
  formatHoursLabel: (hours: number) => string;
  onRequestClick: (request: LeaveSpherePtoRequest) => void;
  showDescription?: boolean;
  isRowHighlighted?: (request: LeaveSpherePtoRequest) => boolean;
  getDescriptionLabel?: (request: LeaveSpherePtoRequest) => ReactNode;
  employeeColumnClassName?: string;
  dateRangeColumnClassName?: string;
  descriptionColumnClassName?: string;
};

export function LeaveSpherePtoRequestTable({
  requests,
  emptyMessage,
  resolveEmployee,
  requestTypeLabel,
  statusLabel,
  formatSubmittedLabel,
  formatDateRangeLabel,
  formatHoursLabel,
  onRequestClick,
  showDescription = false,
  isRowHighlighted,
  getDescriptionLabel,
  employeeColumnClassName,
  dateRangeColumnClassName,
  descriptionColumnClassName,
}: LeaveSpherePtoRequestTableProps) {
  const colSpan = showDescription ? 6 : 5;

  return (
    <div className="overflow-x-auto rounded-xl border border-blue-100">
      <table className="min-w-full text-left text-sm">
        <thead className="bg-blue-50/70 text-xs uppercase tracking-[0.08em] text-slate-600">
          <tr>
            <th className={`${employeeColumnClassName ?? "px-3 py-2.5"}`}>Employee</th>
            <th className="px-3 py-2.5 text-center">Type</th>
            <th className={`${dateRangeColumnClassName ?? "px-3 py-2.5"}`}>Date range</th>
            {showDescription ? <th className={`${descriptionColumnClassName ?? "px-3 py-2.5"}`}>Description</th> : null}
            <th className="px-3 py-2.5 text-center">Status</th>
            <th className="px-3 py-2.5 text-center">Hours</th>
          </tr>
        </thead>
        <tbody>
          {requests.length === 0 ? (
            <tr>
              <td colSpan={colSpan} className="px-3 py-6 text-center text-sm text-slate-600">
                {emptyMessage}
              </td>
            </tr>
          ) : requests.map((request) => {
            const employee = resolveEmployee(request);
            const rowIsHighlighted = isRowHighlighted?.(request) ?? false;
            const rowClassName = rowIsHighlighted
              ? "cursor-pointer border-t border-emerald-200/80 bg-emerald-50 text-slate-700 transition-colors hover:bg-emerald-100/70"
              : "cursor-pointer border-t border-blue-100/80 bg-white text-slate-700 transition-colors hover:bg-blue-50/40";
            return (
              <tr
                key={request.id}
                className={rowClassName}
                onClick={() => onRequestClick(request)}
              >
                <td className={`${employeeColumnClassName ?? "px-3 py-2.5"}`}>
                  <LeaveSpherePtoEmployeeHeader
                    employeeName={employee.employeeName}
                    pictureUrl={employee.pictureUrl}
                    title={employee.employeeName}
                    subtitle={formatSubmittedLabel(request)}
                    titleClassName="font-medium"
                  />
                </td>
                <td className="px-3 py-2.5 text-center">
                  <LeaveSpherePtoTypeChip type={request.type} label={requestTypeLabel(request.type)} />
                </td>
                <td className={`${dateRangeColumnClassName ?? "px-3 py-2.5 whitespace-nowrap"}`}>{formatDateRangeLabel(request)}</td>
                {showDescription ? (
                  <td className={`${descriptionColumnClassName ?? "px-3 py-2.5 text-slate-600"}`}>
                    {getDescriptionLabel ? getDescriptionLabel(request) : request.description || "-"}
                  </td>
                ) : null}
                <td className="px-3 py-2.5 text-center">
                  <LeaveSpherePtoStatusChip status={request.status} label={statusLabel(request.status)} />
                </td>
                <td className="px-3 py-2.5 text-center">{formatHoursLabel(request.hours)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
